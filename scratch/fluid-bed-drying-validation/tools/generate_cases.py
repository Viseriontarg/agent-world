#!/usr/bin/env python3
"""Generate reproducible OpenFOAM cases for source-law validation.

No Fluent case/data file is embedded here.  The industrial cross-section is
reconstructed so that width * bedHeight * alphaSolid * rhoSolid = 3300 kg/m,
which is the dry-solid inventory established by the forensic audit.
"""

from __future__ import annotations

import json
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CASES = ROOT / "cases"

HEADER = """FoamFile
{{
    version     2.0;
    format      ascii;
    class       {klass};
    object      {obj};
}}
"""


@dataclass(frozen=True)
class Case:
    name: str
    width: float
    domain_height: float
    bed_height: float
    alpha_solid: float
    nx: int
    ny: int
    end_time: float
    delta_t: float
    write_interval: int
    rho_gas: float
    rho_solid: float
    cp_gas: float
    cp_solid: float
    k_gas: float
    k_solid: float
    mu_gas: float
    dv_ref: float
    particle_diameter: float
    pressure: float
    latent_heat: float
    x0: float
    xeq: float
    xcr: float
    falling_exponent: float
    deff_ref: float
    tref_deff: float
    activation_energy: float
    t_inlet: float
    t_initial: float
    u_inlet: float
    minimum_temperature: float = 273.15
    maximum_humidity_ratio: float = 10.0
    alpha_floor: float = 1.0e-8
    source_multiplier: float = 1.0
    stop_at_critical: float = 1.0

    @property
    def dry_solid_inventory(self) -> float:
        return (
            self.width
            * self.bed_height
            * self.alpha_solid
            * self.rho_solid
        )

    @property
    def initial_solid_water(self) -> float:
        return self.dry_solid_inventory * self.x0

    @property
    def water_to_xcr(self) -> float:
        return self.dry_solid_inventory * (self.x0 - self.xcr)

    @property
    def inlet_dry_air_mass_flow(self) -> float:
        return self.rho_gas * self.u_inlet * self.width


CASES_TO_BUILD = [
    Case(
        name="smoke",
        width=3.125,
        domain_height=1.0,
        bed_height=0.4,
        alpha_solid=0.60,
        nx=10,
        ny=10,
        end_time=0.01,
        delta_t=0.001,
        write_interval=5,
        rho_gas=101325.0 / (287.05 * 923.15),
        rho_solid=4400.0,
        cp_gas=1007.0,
        cp_solid=850.0,
        k_gas=0.067,
        k_solid=2.5,
        mu_gas=4.0e-5,
        dv_ref=8.0e-5,
        particle_diameter=1.89e-4,
        pressure=101325.0,
        latent_heat=2.257e6,
        x0=0.16,
        xeq=0.005,
        xcr=0.05,
        falling_exponent=1.5,
        deff_ref=1.0e-12,
        tref_deff=373.15,
        activation_energy=18000.0,
        t_inlet=923.15,
        t_initial=303.5,
        u_inlet=0.937,
        stop_at_critical=0.0,
    ),
    Case(
        name="semolina",
        width=0.10,
        domain_height=0.30,
        bed_height=0.10,
        alpha_solid=0.55,
        nx=12,
        ny=36,
        end_time=300.0,
        delta_t=0.10,
        write_interval=300,
        rho_gas=101325.0 / (287.05 * 353.15),
        rho_solid=1450.0,
        cp_gas=1007.0,
        cp_solid=1600.0,
        k_gas=0.030,
        k_solid=0.20,
        mu_gas=2.1e-5,
        dv_ref=3.0e-5,
        particle_diameter=1.0e-3,
        pressure=101325.0,
        latent_heat=2.40e6,
        x0=0.30,
        xeq=0.05,
        xcr=0.20,
        falling_exponent=1.0,
        deff_ref=3.0e-11,
        tref_deff=323.15,
        activation_energy=22000.0,
        t_inlet=353.15,
        t_initial=293.15,
        u_inlet=0.80,
        minimum_temperature=283.15,
        maximum_humidity_ratio=0.20,
        stop_at_critical=0.0,
    ),
    Case(
        name="industrial_low",
        width=3.125,
        domain_height=1.0,
        bed_height=0.4,
        alpha_solid=0.60,
        nx=40,
        ny=32,
        end_time=1800.0,
        delta_t=0.20,
        write_interval=1500,
        rho_gas=101325.0 / (287.05 * 923.15),
        rho_solid=4400.0,
        cp_gas=1007.0,
        cp_solid=850.0,
        k_gas=0.067,
        k_solid=2.5,
        mu_gas=4.0e-5,
        dv_ref=8.0e-5,
        particle_diameter=1.89e-4,
        pressure=101325.0,
        latent_heat=2.257e6,
        x0=0.16,
        xeq=0.005,
        xcr=0.05,
        falling_exponent=1.5,
        deff_ref=3.0e-13,
        tref_deff=373.15,
        activation_energy=18000.0,
        t_inlet=923.15,
        t_initial=303.5,
        u_inlet=0.937,
    ),
    Case(
        name="industrial_central",
        width=3.125,
        domain_height=1.0,
        bed_height=0.4,
        alpha_solid=0.60,
        nx=40,
        ny=32,
        end_time=1800.0,
        delta_t=0.20,
        write_interval=1500,
        rho_gas=101325.0 / (287.05 * 923.15),
        rho_solid=4400.0,
        cp_gas=1007.0,
        cp_solid=850.0,
        k_gas=0.067,
        k_solid=2.5,
        mu_gas=4.0e-5,
        dv_ref=8.0e-5,
        particle_diameter=1.89e-4,
        pressure=101325.0,
        latent_heat=2.257e6,
        x0=0.16,
        xeq=0.005,
        xcr=0.05,
        falling_exponent=1.5,
        deff_ref=1.0e-12,
        tref_deff=373.15,
        activation_energy=18000.0,
        t_inlet=923.15,
        t_initial=303.5,
        u_inlet=0.937,
    ),
    Case(
        name="industrial_high",
        width=3.125,
        domain_height=1.0,
        bed_height=0.4,
        alpha_solid=0.60,
        nx=40,
        ny=32,
        end_time=1800.0,
        delta_t=0.20,
        write_interval=1500,
        rho_gas=101325.0 / (287.05 * 923.15),
        rho_solid=4400.0,
        cp_gas=1007.0,
        cp_solid=850.0,
        k_gas=0.067,
        k_solid=2.5,
        mu_gas=4.0e-5,
        dv_ref=8.0e-5,
        particle_diameter=1.89e-4,
        pressure=101325.0,
        latent_heat=2.257e6,
        x0=0.16,
        xeq=0.005,
        xcr=0.05,
        falling_exponent=1.5,
        deff_ref=3.0e-12,
        tref_deff=373.15,
        activation_energy=18000.0,
        t_inlet=923.15,
        t_initial=303.5,
        u_inlet=0.937,
    ),
]


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def scalar_field(name: str, initial: float, case: Case, kind: str) -> str:
    if kind == "Y":
        patches = f"""
    inlet
    {{
        type fixedValue;
        value uniform 0;
    }}
    outlet
    {{
        type inletOutlet;
        inletValue uniform 0;
        value uniform 0;
    }}
    leftWall {{ type zeroGradient; }}
    rightWall {{ type zeroGradient; }}
    frontAndBack {{ type empty; }}
"""
    elif kind == "Tgas":
        patches = f"""
    inlet
    {{
        type fixedValue;
        value uniform {case.t_inlet:.12g};
    }}
    outlet
    {{
        type inletOutlet;
        inletValue uniform {case.t_inlet:.12g};
        value uniform {case.t_initial:.12g};
    }}
    leftWall {{ type zeroGradient; }}
    rightWall {{ type zeroGradient; }}
    frontAndBack {{ type empty; }}
"""
    else:
        patches = """
    inlet {{ type zeroGradient; }}
    outlet {{ type zeroGradient; }}
    leftWall {{ type zeroGradient; }}
    rightWall {{ type zeroGradient; }}
    frontAndBack {{ type empty; }}
"""

    dimensions = "[0 0 0 1 0 0 0]" if kind.startswith("T") else "[0 0 0 0 0 0 0]"
    return (
        HEADER.format(klass="volScalarField", obj=name)
        + f"dimensions {dimensions};\n"
        + f"internalField uniform {initial:.12g};\n"
        + "boundaryField\n{\n"
        + patches
        + "}\n"
    )


def vector_field(case: Case) -> str:
    u = case.u_inlet
    return (
        HEADER.format(klass="volVectorField", obj="Ugas")
        + "dimensions [0 1 -1 0 0 0 0];\n"
        + f"internalField uniform (0 {u:.12g} 0);\n"
        + """boundaryField
{
    inlet
    {
        type fixedValue;
        value uniform (0 UINLET 0);
    }
    outlet { type zeroGradient; }
    leftWall
    {
        type fixedValue;
        value uniform (0 0 0);
    }
    rightWall
    {
        type fixedValue;
        value uniform (0 0 0);
    }
    frontAndBack { type empty; }
}
""".replace("UINLET", f"{u:.12g}")
    )


def block_mesh(case: Case) -> str:
    w = case.width
    h = case.domain_height
    return HEADER.format(klass="dictionary", obj="blockMeshDict") + f"""
scale 1;

vertices
(
    (0 0 -0.5)
    ({w:.12g} 0 -0.5)
    ({w:.12g} {h:.12g} -0.5)
    (0 {h:.12g} -0.5)
    (0 0 0.5)
    ({w:.12g} 0 0.5)
    ({w:.12g} {h:.12g} 0.5)
    (0 {h:.12g} 0.5)
);

blocks
(
    hex (0 1 2 3 4 5 6 7) ({case.nx} {case.ny} 1)
    simpleGrading (1 1 1)
);

edges ();

boundary
(
    inlet
    {{
        type patch;
        faces ((0 4 5 1));
    }}
    outlet
    {{
        type patch;
        faces ((3 2 6 7));
    }}
    leftWall
    {{
        type wall;
        faces ((0 3 7 4));
    }}
    rightWall
    {{
        type wall;
        faces ((1 5 6 2));
    }}
    frontAndBack
    {{
        type empty;
        faces ((0 1 2 3) (4 7 6 5));
    }}
);

mergePatchPairs ();
"""


def control_dict(case: Case) -> str:
    return HEADER.format(klass="dictionary", obj="controlDict") + f"""
application dryingFoam;
startFrom startTime;
startTime 0;
stopAt endTime;
endTime {case.end_time:.12g};
deltaT {case.delta_t:.12g};
writeControl timeStep;
writeInterval {case.write_interval};
purgeWrite 0;
writeFormat ascii;
writePrecision 10;
writeCompression off;
timeFormat general;
timePrecision 10;
runTimeModifiable false;
"""


def fv_schemes() -> str:
    return HEADER.format(klass="dictionary", obj="fvSchemes") + """
ddtSchemes
{
    default Euler;
}
gradSchemes
{
    default Gauss linear;
}
divSchemes
{
    default none;
    div(phi,Y) Gauss upwind;
    div(phi,Tgas) Gauss upwind;
}
laplacianSchemes
{
    default Gauss linear corrected;
}
interpolationSchemes
{
    default linear;
}
snGradSchemes
{
    default corrected;
}
"""


def fv_solution() -> str:
    return HEADER.format(klass="dictionary", obj="fvSolution") + """
solvers
{
    "(X|Y|Tgas|Tsolid)"
    {
        solver smoothSolver;
        smoother symGaussSeidel;
        tolerance 1e-10;
        relTol 0;
        maxIter 200;
    }
}
relaxationFactors
{
    equations
    {
        "(X|Y|Tgas|Tsolid)" 0.9;
    }
}
"""


def set_fields(case: Case) -> str:
    return HEADER.format(klass="dictionary", obj="setFieldsDict") + f"""
defaultFieldValues
(
    volScalarFieldValue alphaSolid {case.alpha_floor:.12g}
);

regions
(
    boxToCell
    {{
        box (0 0 -0.5) ({case.width:.12g} {case.bed_height:.12g} 0.5);
        fieldValues
        (
            volScalarFieldValue alphaSolid {case.alpha_solid:.12g}
        );
    }}
);
"""


def properties(case: Case) -> str:
    rows = {
        "rhoGas": case.rho_gas,
        "rhoSolid": case.rho_solid,
        "cpGas": case.cp_gas,
        "cpSolid": case.cp_solid,
        "kGas": case.k_gas,
        "kSolid": case.k_solid,
        "muGas": case.mu_gas,
        "DvRef": case.dv_ref,
        "particleDiameter": case.particle_diameter,
        "pressure": case.pressure,
        "latentHeat": case.latent_heat,
        "Xeq": case.xeq,
        "Xcr": case.xcr,
        "fallingExponent": case.falling_exponent,
        "DeffRef": case.deff_ref,
        "TrefDeff": case.tref_deff,
        "activationEnergy": case.activation_energy,
        "minimumTemperature": case.minimum_temperature,
        "maximumHumidityRatio": case.maximum_humidity_ratio,
        "alphaFloor": case.alpha_floor,
        "sourceMultiplier": case.source_multiplier,
        "stopAtCritical": case.stop_at_critical,
    }
    body = "\n".join(f"{key:<24} {value:.12g};" for key, value in rows.items())
    return HEADER.format(klass="dictionary", obj="dryingProperties") + "\n" + body + "\n"


def build(case: Case) -> dict[str, float | str]:
    case_dir = CASES / case.name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    (case_dir / "0").mkdir(parents=True)
    (case_dir / "constant").mkdir()
    (case_dir / "system").mkdir()

    write(case_dir / "0" / "X", scalar_field("X", case.x0, case, "X"))
    write(case_dir / "0" / "Y", scalar_field("Y", 0.0, case, "Y"))
    write(case_dir / "0" / "Tsolid", scalar_field("Tsolid", case.t_initial, case, "Tsolid"))
    write(case_dir / "0" / "Tgas", scalar_field("Tgas", case.t_initial, case, "Tgas"))
    write(case_dir / "0" / "alphaSolid", scalar_field("alphaSolid", case.alpha_floor, case, "alphaSolid"))
    write(case_dir / "0" / "Ugas", vector_field(case))
    write(case_dir / "constant" / "dryingProperties", properties(case))
    write(case_dir / "system" / "blockMeshDict", block_mesh(case))
    write(case_dir / "system" / "controlDict", control_dict(case))
    write(case_dir / "system" / "fvSchemes", fv_schemes())
    write(case_dir / "system" / "fvSolution", fv_solution())
    write(case_dir / "system" / "setFieldsDict", set_fields(case))

    metadata = asdict(case)
    metadata.update(
        {
            "dry_solid_inventory_kg_per_m": case.dry_solid_inventory,
            "initial_solid_water_kg_per_m": case.initial_solid_water,
            "water_to_xcr_kg_per_m": case.water_to_xcr,
            "inlet_dry_air_mass_flow_kg_per_s_per_m": case.inlet_dry_air_mass_flow,
            "required_average_rate_to_xcr_600s": case.water_to_xcr / 600.0,
            "required_average_rate_to_xcr_1800s": case.water_to_xcr / 1800.0,
        }
    )
    write(case_dir / "case_metadata.json", json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return metadata


def main() -> None:
    CASES.mkdir(parents=True, exist_ok=True)
    manifest = {case.name: build(case) for case in CASES_TO_BUILD}
    write(ROOT / "cases" / "manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({name: data["dry_solid_inventory_kg_per_m"] for name, data in manifest.items()}, indent=2))


if __name__ == "__main__":
    main()
