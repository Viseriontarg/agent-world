#include "fvCFD.H"
#include "OFstream.H"

using namespace Foam;

static scalar requiredScalar(const dictionary& dict, const word& key)
{
    scalar value = 0.0;
    dict.lookup(key) >> value;
    return value;
}

static scalar boundedSaturationPressure(const scalar temperature, const scalar pressure)
{
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
        "Reduced frozen-hydrodynamics fluidised-bed drying validation solver"
    );

    #include "setRootCase.H"
    #include "createTime.H"
    #include "createMesh.H"

    IOdictionary properties
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

    const scalar rhoGas = requiredScalar(properties, "rhoGas");
    const scalar rhoSolid = requiredScalar(properties, "rhoSolid");
    const scalar cpGas = requiredScalar(properties, "cpGas");
    const scalar cpSolid = requiredScalar(properties, "cpSolid");
    const scalar kGas = requiredScalar(properties, "kGas");
    const scalar kSolid = requiredScalar(properties, "kSolid");
    const scalar muGas = requiredScalar(properties, "muGas");
    const scalar DvRef = requiredScalar(properties, "DvRef");
    const scalar particleDiameter = requiredScalar(properties, "particleDiameter");
    const scalar pressure = requiredScalar(properties, "pressure");
    const scalar latentHeat = requiredScalar(properties, "latentHeat");
    const scalar Xeq = requiredScalar(properties, "Xeq");
    const scalar Xcr = requiredScalar(properties, "Xcr");
    const scalar fallingExponent = requiredScalar(properties, "fallingExponent");
    const scalar DeffRef = requiredScalar(properties, "DeffRef");
    const scalar TrefDeff = requiredScalar(properties, "TrefDeff");
    const scalar activationEnergy = requiredScalar(properties, "activationEnergy");
    const scalar minimumTemperature = requiredScalar(properties, "minimumTemperature");
    const scalar maximumHumidityRatio = requiredScalar(properties, "maximumHumidityRatio");
    const scalar alphaFloor = requiredScalar(properties, "alphaFloor");
    const scalar sourceMultiplier = requiredScalar(properties, "sourceMultiplier");
    const scalar stopAtCritical = requiredScalar(properties, "stopAtCritical");

    const dimensionedScalar rhoGasDim("rhoGas", dimDensity, rhoGas);
    const dimensionedScalar rhoSolidDim("rhoSolid", dimDensity, rhoSolid);
    const dimensionedScalar cpGasDim
    (
        "cpGas",
        dimEnergy/dimMass/dimTemperature,
        cpGas
    );
    const dimensionedScalar cpSolidDim
    (
        "cpSolid",
        dimEnergy/dimMass/dimTemperature,
        cpSolid
    );
    const dimensionedScalar DvDim("Dv", dimArea/dimTime, DvRef);
    const dimensionedScalar alphaGasThermalDim
    (
        "alphaGasThermal",
        dimArea/dimTime,
        kGas/(rhoGas*cpGas)
    );
    const dimensionedScalar alphaSolidThermalDim
    (
        "alphaSolidThermal",
        dimArea/dimTime,
        kSolid/(rhoSolid*cpSolid)
    );
    const dimensionedScalar latentHeatDim
    (
        "latentHeat",
        dimEnergy/dimMass,
        latentHeat
    );

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
        dimensionedScalar("zeroFilm", dimMass/dimVolume/dimTime, 0.0)
    );
    volScalarField mDotInternal
    (
        IOobject("mDotInternal", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroInternal", dimMass/dimVolume/dimTime, 0.0)
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
        dimensionedScalar("zeroFall", dimless, 0.0)
    );
    volScalarField qInterphase
    (
        IOobject("qInterphase", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::AUTO_WRITE),
        mesh,
        dimensionedScalar("zeroQ", dimPower/dimVolume, 0.0)
    );

    mkDir(runTime.path()/"postProcessing");
    OFstream history(runTime.path()/"postProcessing"/"drying.csv");
    history
        << "time,solidWater,gasWater,totalWater,netWaterOutCum,sourceRate,"
        << "gasSourceIntegral,solidSourceIntegral,sourceCancellation,"
        << "openBalanceAbs,openBalanceRel,Xmean,Ymean,TsolidMean,TgasMean,"
        << "waterOutRate,filmLimitedFraction,energyLimitedFraction" << nl;

    const scalar universalGasConstant = 8.314462618;
    const scalar piValue = 3.14159265358979323846;
    const scalar radius = 0.5*particleDiameter;

    scalar initialTotalWater = 0.0;
    scalar netWaterOutCum = 0.0;
    bool balanceStarted = false;

    while (runTime.loop())
    {
        Info<< "Time = " << runTime.timeName() << nl << endl;

        alphaGas = scalar(1) - alphaSolid;
        alphaSolidEff = max
        (
            alphaSolid,
            dimensionedScalar("alphaSolidFloorNow", dimless, alphaFloor)
        );
        alphaGasEff = max
        (
            alphaGas,
            dimensionedScalar("alphaGasFloorNow", dimless, alphaFloor)
        );
        phi = fvc::flux(Ugas);

        const scalar dt = runTime.deltaTValue();
        scalar localFilmLimitedVolume = 0.0;
        scalar localEnergyLimitedVolume = 0.0;
        scalar localBedVolume = 0.0;

        forAll(mDot, celli)
        {
            const scalar aS = max(alphaSolid[celli], 0.0);
            const scalar aG = max(1.0 - aS, alphaFloor);
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
                fall = pow
                (
                    (Xi - Xeq)/max(Xcr - Xeq, SMALL),
                    fallingExponent
                );
            }
            fall = min(max(fall, 0.0), 1.0);
            fallingFactor[celli] = fall;

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
            const scalar Dv =
                DvRef*pow(Tfilm/298.15, 1.75)*(101325.0/pressure);
            const scalar Re =
                rhoGas*Urel*particleDiameter/max(muGas, SMALL);
            const scalar Sc =
                muGas/max(rhoGas*Dv, SMALL);
            const scalar Sh =
                2.0
              + 0.6*sqrt(max(Re, 0.0))*pow(max(Sc, SMALL), 1.0/3.0);
            const scalar km = Sh*Dv/particleDiameter;
            const scalar areaDensity = 6.0*aS/particleDiameter;

            const scalar psat = boundedSaturationPressure(Ts, pressure);
            const scalar YsurfaceRaw =
                fall*0.622*psat/max(pressure - psat, 0.05*pressure);
            const scalar Ysurface =
                min(max(YsurfaceRaw, 0.0), maximumHumidityRatio);
            const scalar filmRate =
                rhoGas*km*areaDensity*max(Ysurface - Yi, 0.0);

            const scalar Deff =
                DeffRef*exp
                (
                    -activationEnergy/universalGasConstant
                   *(1.0/Ts - 1.0/TrefDeff)
                );
            Deffective[celli] = max(Deff, 0.0);
            const scalar kLdf =
                piValue*piValue*Deffective[celli]/max(radius*radius, SMALL);
            const scalar internalRate =
                rhoSolid*aS*kLdf*max(Xi - Xeq, 0.0)*fall;

            mDotFilm[celli] = max(filmRate, 0.0);
            mDotInternal[celli] = max(internalRate, 0.0);

            const scalar Pr = cpGas*muGas/max(kGas, SMALL);
            const scalar Nu =
                2.0
              + 0.6*sqrt(max(Re, 0.0))*pow(max(Pr, SMALL), 1.0/3.0);
            const scalar h = Nu*kGas/particleDiameter;
            scalar qH = h*areaDensity*(Tg - Ts);

            if (qH > 0.0)
            {
                const scalar noGasOvershoot =
                    rhoGas*aG*cpGas*max(Tg - Ts, 0.0)/max(dt, SMALL);
                qH = min(qH, noGasOvershoot);
            }
            else
            {
                const scalar noSolidOvershoot =
                    rhoSolid*aS*cpSolid*max(Ts - Tg, 0.0)/max(dt, SMALL);
                qH = max(qH, -noSolidOvershoot);
            }
            qInterphase[celli] = qH;

            const scalar smoothMinimum =
                (filmRate > SMALL && internalRate > SMALL)
              ? filmRate*internalRate/(filmRate + internalRate)
              : 0.0;
            const scalar inventoryLimit =
                rhoSolid*aS*max(Xi - Xeq, 0.0)/max(dt, SMALL);
            const scalar storedSensible =
                rhoSolid*aS*cpSolid
               *max(Ts - minimumTemperature, 0.0)/max(dt, SMALL);
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

        fvScalarMatrix YEqn
        (
            fvm::ddt(alphaGasEff, Y)
          + fvm::div(phi, Y)
          - fvm::laplacian(alphaGasEff*DvDim, Y)
         == mDot/rhoGasDim
        );
        YEqn.relax();
        YEqn.solve();

        fvScalarMatrix XEqn
        (
            fvm::ddt(alphaSolidEff, X)
         == -mDot/rhoSolidDim
        );
        XEqn.relax();
        XEqn.solve();

        forAll(Y, celli)
        {
            Y[celli] = min(max(Y[celli], 0.0), maximumHumidityRatio);
            X[celli] = max(X[celli], Xeq);
        }
        Y.correctBoundaryConditions();
        X.correctBoundaryConditions();

        volScalarField qGas
        (
            IOobject("qGas", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            -qInterphase
        );
        volScalarField qSolid
        (
            IOobject("qSolid", runTime.timeName(), mesh, IOobject::NO_READ, IOobject::NO_WRITE),
            qInterphase - mDot*latentHeatDim
        );

        fvScalarMatrix TgasEqn
        (
            fvm::ddt(alphaGasEff, Tgas)
          + fvm::div(phi, Tgas)
          - fvm::laplacian(alphaGasEff*alphaGasThermalDim, Tgas)
         == qGas/(rhoGasDim*cpGasDim)
        );
        TgasEqn.relax();
        TgasEqn.solve();

        fvScalarMatrix TsolidEqn
        (
            fvm::ddt(alphaSolidEff, Tsolid)
          - fvm::laplacian(alphaSolidEff*alphaSolidThermalDim, Tsolid)
         == qSolid/(rhoSolidDim*cpSolidDim)
        );
        TsolidEqn.relax();
        TsolidEqn.solve();

        forAll(Tgas, celli)
        {
            Tgas[celli] = max(Tgas[celli], minimumTemperature);
            Tsolid[celli] = max(Tsolid[celli], minimumTemperature);
        }
        Tgas.correctBoundaryConditions();
        Tsolid.correctBoundaryConditions();

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
        const scalar solidTemperatureMass =
            returnReduce(localSolidTemperatureMass, sumOp<scalar>());
        const scalar gasTemperatureMass =
            returnReduce(localGasTemperatureMass, sumOp<scalar>());
        const scalar bedVolume =
            returnReduce(localBedVolume, sumOp<scalar>());
        const scalar filmLimitedVolume =
            returnReduce(localFilmLimitedVolume, sumOp<scalar>());
        const scalar energyLimitedVolume =
            returnReduce(localEnergyLimitedVolume, sumOp<scalar>());

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
        const scalar netWaterOutRate =
            returnReduce(localNetWaterOutRate, sumOp<scalar>());
        netWaterOutCum += netWaterOutRate*dt;

        const scalar totalWater = solidWater + gasWater;
        if (!balanceStarted)
        {
            initialTotalWater = totalWater + netWaterOutCum;
            balanceStarted = true;
        }

        const scalar balanceAbs =
            initialTotalWater - totalWater - netWaterOutCum;
        const scalar balanceRel =
            mag(balanceAbs)/max(initialTotalWater, SMALL);
        const scalar Xmean = solidWater/max(drySolid, SMALL);
        const scalar Ymean = gasWater/max(dryGas, SMALL);
        const scalar TsMean =
            solidTemperatureMass/max(drySolid, SMALL);
        const scalar TgMean =
            gasTemperatureMass/max(dryGas, SMALL);
        const scalar gasSourceIntegral = sourceRate;
        const scalar solidSourceIntegral = -sourceRate;
        const scalar sourceCancellation =
            gasSourceIntegral + solidSourceIntegral;

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
            << " sourceRate=" << sourceRate << " kg/s/m"
            << " waterOut=" << netWaterOutRate << " kg/s/m"
            << " balanceRel=" << balanceRel << nl << endl;

        runTime.write();

        if (stopAtCritical > 0.5 && Xmean <= Xcr)
        {
            Info<< "Reached Xcr=" << Xcr
                << " at t=" << runTime.value() << " s" << nl << endl;
            break;
        }
    }

    Info<< "End" << nl << endl;
    return 0;
}
