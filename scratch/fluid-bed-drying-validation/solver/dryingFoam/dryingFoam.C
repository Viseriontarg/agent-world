#include "fvCFD.H"
#include "OFstream.H"

using namespace Foam;

static scalar readRequiredScalar(const dictionary& dict, const word& key)
{
    scalar value = 0;
    dict.lookup(key) >> value;
    return value;
}

static scalar saturationPressure(const scalar temperature, const scalar pressure)
{
    // Buck equation below the normal boiling point, continued with
    // Clausius-Clapeyron above it.  The result is bounded because a
    // humidity-ratio closure is singular as psat approaches absolute pressure.
    const scalar T = max(temperature, 250.0);
    const scalar Tc = T - 273.15;
    scalar psat = 0.0;

    if (Tc <= 100.0)
    {
        psat = 611.21*exp((18.678 - Tc/234.5)*(Tc/(257.14 + Tc)));
    }
    else
    {
        const scalar p100 = 101325.0;
        const scalar LvMolar = 40650.0;
        const scalar Rmol = 8.314462618;
        psat = p100*exp(LvMolar/Rmol*(1.0/373.15 - 1.0/T));
    }

    return min(max(psat, 0.0), 0.95*pressure);
}

int main(int argc, char *argv[])
{
    argList::addNote
    (
        "Frozen-hydrodynamics two-carrier fluidised-bed drying validation solver"
    );

    #include "setRootCase.H"
    #include "createTime.H"
    #include "createMesh.H"

    Info<< "Reading dryingProperties" << nl << endl;
    IOdictionary dryingProperties
    (
        IOobject
        (
            "dryingProperties",
            runTime.constant(),
            mesh,
            IOobject::MUST_READ,
            IOobject::NO_WRITE
        )
    );

    const scalar rhoGas = readRequiredScalar(dryingProperties, "rhoGas");
    const scalar rhoSolid = readRequiredScalar(dryingProperties, "rhoSolid");
    const scalar cpGas = readRequiredScalar(dryingProperties, "cpGas");
    const scalar cpSolid = readRequiredScalar(dryingProperties, "cpSolid");
    const scalar kGas = readRequiredScalar(dryingProperties, "kGas");
    const scalar kSolid = readRequiredScalar(dryingProperties, "kSolid");
    const scalar muGas = readRequiredScalar(dryingProperties, "muGas");
    const scalar DvRef = readRequiredScalar(dryingProperties, "DvRef");
    const scalar particleDiameter = readRequiredScalar(dryingProperties, "particleDiameter");
    const scalar pressure = readRequiredScalar(dryingProperties, "pressure");
    const scalar latentHeat = readRequiredScalar(dryingProperties, "latentHeat");
    const scalar Xeq = readRequiredScalar(dryingProperties, "Xeq");
    const scalar Xcr = readRequiredScalar(dryingProperties, "Xcr");
    const scalar fallingExponent = readRequiredScalar(dryingProperties, "fallingExponent");
    const scalar DeffRef = readRequiredScalar(dryingProperties, "DeffRef");
    const scalar TrefDeff = readRequiredScalar(dryingProperties, "TrefDeff");
    const scalar activationEnergy = readRequiredScalar(dryingProperties, "activationEnergy");
    const scalar minimumTemperature = readRequiredScalar(dryingProperties, "minimumTemperature");
    const scalar maximumHumidityRatio = readRequiredScalar(dryingProperties, "maximumHumidityRatio");
    const scalar alphaFloor = readRequiredScalar(dryingProperties, "alphaFloor");
    const scalar sourceMultiplier = readRequiredScalar(dryingProperties, "sourceMultiplier");
    const scalar stopAtCritical = readRequiredScalar(dryingProperties, "stopAtCritical");

    Info<< "Reading fields" << nl << endl;

    volScalarField X
    (
        IOobject("X", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volScalarField Y
    (
        IOobject("Y", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volScalarField Tsolid
    (
        IOobject("Tsolid", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volScalarField Tgas
    (
        IOobject("Tgas", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volScalarField alphaSolid
    (
        IOobject("alphaSolid", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volVectorField Ugas
    (
        IOobject("Ugas", runTime.timeName(), mesh, IOobject::MUST_READ, IOobject::AUTO_WRITE),
        mesh
    );

    volScalarField alphaGas
    (
        IOobject("alphaGas", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        scalar(1) - alphaSolid
    );

    volScalarField alphaSolidEff
    (
        IOobject("alphaSolidEff", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
        max(alphaSolid, dimensionedScalar("alphaSolidFloor", dimless, alphaFloor))
    );

    volScalarField alphaGasEff
    (
        IOobject("alphaGasEff", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
        max(alphaGas, dimensionedScalar("alphaGasFloor", dimless, alphaFloor))
    );

    surfaceScalarField phi
    (
        IOobject("phi", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        fvc::flux(Ugas)
    );

    volScalarField mDot
    (
        IOobject("mDot", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroMdot", dimMass/dimVolume/dimTime, 0.0)
    );

    volScalarField mDotFilm
    (
        IOobject("mDotFilm", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroMdotFilm", dimMass/dimVolume/dimTime, 0.0)
    );

    volScalarField mDotInternal
    (
        IOobject("mDotInternal", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroMdotInternal", dimMass/dimVolume/dimTime, 0.0)
    );

    volScalarField Deffective
    (
        IOobject("Deffective", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroDeff", dimArea/dimTime, 0.0)
    );

    volScalarField fallingFactor
    (
        IOobject("fallingFactor", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroFalling", dimless, 0.0)
    );

    volScalarField qInterphase
    (
        IOobject("qInterphase", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroQInter", dimPower/dimVolume, 0.0)
    );

    mkDir(runTime.path()/"postProcessing");
    OFstream history(runTime.path()/"postProcessing"/"drying.csv");
    history
        << "time,solidWater,gasWater,totalWater,netWaterOutCum,sourceRate,"
        << "gasSourceIntegral,solidSourceIntegral,sourceCancellation,"
        << "openBalanceAbs,openBalanceRel,Xmean,Ymean,TsolidMean,TgasMean,"
        << "waterOutRate,filmLimitedFraction,energyLimitFraction" << nl;

    const scalar Rgas = 8.314462618;
    const scalar piValue = constant::mathematical::pi;
    const scalar radius = 0.5*particleDiameter;
    const scalar dtInitial = runTime.deltaTValue();

    scalar initialTotalWater = 0.0;
    scalar netWaterOutCum = 0.0;
    bool initialisedBalance = false;

    while (runTime.loop())
    {
        Info<< "Time = " << runTime.userTimeName() << nl << endl;

        alphaGas = scalar(1) - alphaSolid;
        alphaSolidEff = max(alphaSolid, dimensionedScalar("alphaSolidFloorNow", dimless, alphaFloor));
        alphaGasEff = max(alphaGas, dimensionedScalar("alphaGasFloorNow", dimless, alphaFloor));
        phi = fvc::flux(Ugas);

        const scalar dt = runTime.deltaTValue();
        scalar localFilmLimitedVolume = 0.0;
        scalar localEnergyLimitedVolume = 0.0;
        scalar localBedVolume = 0.0;

        forAll(mDot, celli)
        {
            const scalar aS = max(alphaSolid[celli], 0.0);
            const scalar aG = max(alphaGas[celli], alphaFloor);
            const scalar Xi = max(X[celli], 0.0);
            const scalar Yi = max(Y[celli], 0.0);
            const scalar Ts = max(Tsolid[celli], minimumTemperature);
            const scalar Tg = max(Tgas[celli], minimumTemperature);
            const scalar Urel = mag(Ugas[celli]);

            scalar fall = 0.0;
            if (Xi >= Xcr)
            {
                fall = 1.0;
            }
            else if (Xi > Xeq)
            {
                fall = pow((Xi - Xeq)/max(Xcr - Xeq, SMALL), fallingExponent);
            }
            fallingFactor[celli] = min(max(fall, 0.0), 1.0);

            if (aS <= alphaFloor || Xi <= Xeq + SMALL)
            {
                mDot[celli] = 0.0;
                mDotFilm[celli] = 0.0;
                mDotInternal[celli] = 0.0;
                Deffective[celli] = 0.0;
                qInterphase[celli] = 0.0;
                continue;
            }

            localBedVolume += mesh.V()[celli];

            const scalar Tfilm = max(0.5*(Ts + Tg), 250.0);
            const scalar Dv = DvRef*pow(Tfilm/298.15, 1.75)*(101325.0/pressure);
            const scalar Re = rhoGas*Urel*particleDiameter/max(muGas, SMALL);
            const scalar Sc = muGas/max(rhoGas*Dv, SMALL);
            const scalar Sh = 2.0 + 0.6*sqrt(max(Re, 0.0))*pow(max(Sc, SMALL), 1.0/3.0);
            const scalar km = Sh*Dv/particleDiameter;
            const scalar areaDensity = 6.0*aS/particleDiameter;

            const scalar psat = saturationPressure(Ts, pressure);
            const scalar waterActivity = fallingFactor[celli];
            const scalar YsurfaceRaw = waterActivity*0.622*psat/max(pressure - psat, 0.05*pressure);
            const scalar Ysurface = min(max(YsurfaceRaw, 0.0), maximumHumidityRatio);
            const scalar filmRate = rhoGas*km*areaDensity*max(Ysurface - Yi, 0.0);

            const scalar Deff = DeffRef*exp
            (
                -activationEnergy/Rgas*(1.0/Ts - 1.0/TrefDeff)
            );
            Deffective[celli] = max(Deff, 0.0);
            const scalar kLdf = piValue*piValue*Deffective[celli]/max(radius*radius, SMALL);
            const scalar internalRate =
                rhoSolid*aS*kLdf*max(Xi - Xeq, 0.0)*fallingFactor[celli];

            mDotFilm[celli] = max(filmRate, 0.0);
            mDotInternal[celli] = max(internalRate, 0.0);

            const scalar Pr = cpGas*muGas/max(kGas, SMALL);
            const scalar Nu = 2.0 + 0.6*sqrt(max(Re, 0.0))*pow(max(Pr, SMALL), 1.0/3.0);
            const scalar h = Nu*kGas/particleDiameter;
            scalar qH = h*areaDensity*(Tg - Ts);

            if (qH > 0.0)
            {
                const scalar gasNoOvershoot =
                    rhoGas*aG*cpGas*max(Tg - Ts, 0.0)/max(dt, SMALL);
                qH = min(qH, gasNoOvershoot);
            }
            else
            {
                const scalar solidNoOvershoot =
                    rhoSolid*aS*cpSolid*max(Ts - Tg, 0.0)/max(dt, SMALL);
                qH = max(qH, -solidNoOvershoot);
            }
            qInterphase[celli] = qH;

            const scalar smoothMinimum =
                (filmRate > SMALL && internalRate > SMALL)
              ? filmRate*internalRate/(filmRate + internalRate)
              : 0.0;

            const scalar inventoryLimit =
                rhoSolid*aS*max(Xi - Xeq, 0.0)/max(dt, SMALL);
            const scalar storedSensible =
                rhoSolid*aS*cpSolid*max(Ts - minimumTemperature, 0.0)/max(dt, SMALL);
            const scalar thermalLimit =
                (max(qH, 0.0) + storedSensible)/max(latentHeat, SMALL);

            scalar rate = sourceMultiplier*min(smoothMinimum, inventoryLimit);
            if (rate > thermalLimit)
            {
                rate = thermalLimit;
                localEnergyLimitedVolume += mesh.V()[celli];
            }
            if (filmRate <= internalRate)
            {
                localFilmLimitedVolume += mesh.V()[celli];
            }

            mDot[celli] = max(rate, 0.0);
        }

        mDot.correctBoundaryConditions();
        mDotFilm.correctBoundaryConditions();
        mDotInternal.correctBoundaryConditions();
        Deffective.correctBoundaryConditions();
        fallingFactor.correctBoundaryConditions();
        qInterphase.correctBoundaryConditions();

        volScalarField gasMoistureSource
        (
            IOobject("gasMoistureSource", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            mDot/rhoGas
        );

        volScalarField solidMoistureSource
        (
            IOobject("solidMoistureSource", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            -mDot/rhoSolid
        );

        fvScalarMatrix YEqn
        (
            fvm::ddt(alphaGasEff, Y)
          + fvm::div(phi, Y)
          - fvm::laplacian(alphaGasEff*DvRef, Y)
         == gasMoistureSource
        );
        YEqn.relax();
        YEqn.solve();
        Y.max(0.0);
        Y.min(maximumHumidityRatio);

        fvScalarMatrix XEqn
        (
            fvm::ddt(alphaSolidEff, X)
         == solidMoistureSource
        );
        XEqn.relax();
        XEqn.solve();
        X.max(Xeq);

        volScalarField qGas
        (
            IOobject("qGas", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            -qInterphase
        );

        volScalarField qSolid
        (
            IOobject("qSolid", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            qInterphase - mDot*latentHeat
        );

        fvScalarMatrix TgasEqn
        (
            fvm::ddt(alphaGasEff, Tgas)
          + fvm::div(phi, Tgas)
          - fvm::laplacian(alphaGasEff*kGas/(rhoGas*cpGas), Tgas)
         == qGas/(rhoGas*cpGas)
        );
        TgasEqn.relax();
        TgasEqn.solve();
        Tgas.max(minimumTemperature);

        fvScalarMatrix TsolidEqn
        (
            fvm::ddt(alphaSolidEff, Tsolid)
          - fvm::laplacian(alphaSolidEff*kSolid/(rhoSolid*cpSolid), Tsolid)
         == qSolid/(rhoSolid*cpSolid)
        );
        TsolidEqn.relax();
        TsolidEqn.solve();
        Tsolid.max(minimumTemperature);

        scalar localDrySolid = 0.0;
        scalar localDryGas = 0.0;
        scalar localSolidWater = 0.0;
        scalar localGasWater = 0.0;
        scalar localSolidTemperatureMass = 0.0;
        scalar localGasTemperatureMass = 0.0;
        scalar localSource = 0.0;

        forAll(mesh.V(), celli)
        {
            const scalar V = mesh.V()[celli];
            const scalar dryS = rhoSolid*max(alphaSolid[celli], 0.0)*V;
            const scalar dryG = rhoGas*max(alphaGas[celli], 0.0)*V;
            localDrySolid += dryS;
            localDryGas += dryG;
            localSolidWater += dryS*X[celli];
            localGasWater += dryG*Y[celli];
            localSolidTemperatureMass += dryS*Tsolid[celli];
            localGasTemperatureMass += dryG*Tgas[celli];
            localSource += mDot[celli]*V;
        }

        const scalar drySolid = returnReduce(localDrySolid, sumOp<scalar>());
        const scalar dryGas = returnReduce(localDryGas, sumOp<scalar>());
        const scalar solidWater = returnReduce(localSolidWater, sumOp<scalar>());
        const scalar gasWater = returnReduce(localGasWater, sumOp<scalar>());
        const scalar sourceRate = returnReduce(localSource, sumOp<scalar>());
        const scalar solidTemperatureMass = returnReduce(localSolidTemperatureMass, sumOp<scalar>());
        const scalar gasTemperatureMass = returnReduce(localGasTemperatureMass, sumOp<scalar>());
        const scalar bedVolume = returnReduce(localBedVolume, sumOp<scalar>());
        const scalar filmLimitedVolume = returnReduce(localFilmLimitedVolume, sumOp<scalar>());
        const scalar energyLimitedVolume = returnReduce(localEnergyLimitedVolume, sumOp<scalar>());

        scalar localNetWaterOutRate = 0.0;
        forAll(phi.boundaryField(), patchi)
        {
            const fvsPatchScalarField& phip = phi.boundaryField()[patchi];
            const fvPatchScalarField& Yp = Y.boundaryField()[patchi];
            forAll(phip, facei)
            {
                localNetWaterOutRate += rhoGas*phip[facei]*Yp[facei];
            }
        }
        const scalar netWaterOutRate = returnReduce(localNetWaterOutRate, sumOp<scalar>());
        netWaterOutCum += netWaterOutRate*dt;

        const scalar totalWater = solidWater + gasWater;
        if (!initialisedBalance)
        {
            initialTotalWater = totalWater + netWaterOutCum;
            initialisedBalance = true;
        }

        const scalar balanceAbs = initialTotalWater - totalWater - netWaterOutCum;
        const scalar balanceRel = mag(balanceAbs)/max(initialTotalWater, SMALL);
        const scalar Xmean = solidWater/max(drySolid, SMALL);
        const scalar Ymean = gasWater/max(dryGas, SMALL);
        const scalar TsMean = solidTemperatureMass/max(drySolid, SMALL);
        const scalar TgMean = gasTemperatureMass/max(dryGas, SMALL);
        const scalar gasSourceIntegral = sourceRate;
        const scalar solidSourceIntegral = -sourceRate;
        const scalar sourceCancellation = gasSourceIntegral + solidSourceIntegral;

        history
            << runTime.value() << ','
            << solidWater << ',' << gasWater << ',' << totalWater << ','
            << netWaterOutCum << ',' << sourceRate << ','
            << gasSourceIntegral << ',' << solidSourceIntegral << ','
            << sourceCancellation << ',' << balanceAbs << ',' << balanceRel << ','
            << Xmean << ',' << Ymean << ',' << TsMean << ',' << TgMean << ','
            << netWaterOutRate << ','
            << filmLimitedVolume/max(bedVolume, SMALL) << ','
            << energyLimitedVolume/max(bedVolume, SMALL) << nl;

        Info<< "drying: Xmean=" << Xmean
            << " sourceRate=" << sourceRate << " kg/s"
            << " waterOut=" << netWaterOutRate << " kg/s"
            << " balanceRel=" << balanceRel << nl << endl;

        runTime.write();

        if (stopAtCritical > 0.5 && Xmean <= Xcr)
        {
            Info<< "Reached critical moisture Xcr=" << Xcr
                << " at t=" << runTime.value() << " s" << nl << endl;
            break;
        }
    }

    Info<< "End" << nl << endl;
    return 0;
}
