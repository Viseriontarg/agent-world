#!/usr/bin/env python3
"""Fail-closed validation and engineering summary for dryingFoam runs."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_rows(case_name: str) -> tuple[dict, list[dict[str, float]]]:
    case = ROOT / "cases" / case_name
    meta = json.loads((case / "case_metadata.json").read_text())
    csv_path = case / "postProcessing" / "drying.csv"
    if not csv_path.exists():
        raise RuntimeError(f"missing result CSV: {csv_path}")
    rows: list[dict[str, float]] = []
    with csv_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            numeric = {key: float(value) for key, value in row.items()}
            if not all(math.isfinite(value) for value in numeric.values()):
                raise RuntimeError(f"non-finite result in {case_name}: {numeric}")
            rows.append(numeric)
    if len(rows) < 2:
        raise RuntimeError(f"too few result rows for {case_name}: {len(rows)}")
    return meta, rows


def validate_case(case_name: str) -> dict:
    meta, rows = load_rows(case_name)
    x_values = [row["Xmean"] for row in rows]
    source_rates = [row["sourceRate"] for row in rows]
    balances = [abs(row["openBalanceRel"]) for row in rows]
    cancellations = [abs(row["sourceCancellation"]) for row in rows]

    monotonic_violation = max(
        (x_values[i + 1] - x_values[i] for i in range(len(x_values) - 1)),
        default=0.0,
    )
    if monotonic_violation > 2.0e-7:
        raise RuntimeError(
            f"{case_name}: Xmean increased by {monotonic_violation:.6g}"
        )
    if min(source_rates) < -1.0e-12:
        raise RuntimeError(f"{case_name}: negative evaporation source")
    if max(cancellations) > 1.0e-12:
        raise RuntimeError(
            f"{case_name}: source pair does not cancel: {max(cancellations):.6g}"
        )
    if max(balances) > 5.0e-3:
        raise RuntimeError(
            f"{case_name}: open moisture balance exceeds 0.5%: {max(balances):.6g}"
        )

    xcr = float(meta["xcr"])
    critical_time = None
    for row in rows:
        if row["Xmean"] <= xcr:
            critical_time = row["time"]
            break

    final = rows[-1]
    water_removed = rows[0]["solidWater"] - final["solidWater"]
    elapsed = max(final["time"] - rows[0]["time"], 1.0e-30)
    average_solid_water_rate = water_removed / elapsed

    result = {
        "case": case_name,
        "rows": len(rows),
        "end_time_s": final["time"],
        "initial_Xmean": x_values[0],
        "final_Xmean": final["Xmean"],
        "critical_X": xcr,
        "critical_time_s": critical_time,
        "initial_Tsolid_K": rows[0]["TsolidMean"],
        "final_Tsolid_K": final["TsolidMean"],
        "peak_source_rate_kg_s_m": max(source_rates),
        "final_source_rate_kg_s_m": final["sourceRate"],
        "average_solid_water_removal_kg_s_m": average_solid_water_rate,
        "water_removed_kg_m": water_removed,
        "max_open_balance_relative": max(balances),
        "max_source_cancellation_kg_s_m": max(cancellations),
        "max_monotonic_X_violation": max(monotonic_violation, 0.0),
        "final_film_limited_fraction": final["filmLimitedFraction"],
        "final_energy_limited_fraction": final["energyLimitedFraction"],
        "dry_solid_inventory_kg_m": meta["dry_solid_inventory_kg_per_m"],
        "water_to_Xcr_kg_m": meta["water_to_xcr_kg_per_m"],
        "deff_ref_m2_s": meta["deff_ref"],
    }

    if case_name == "semolina":
        result["published_5min_X_band"] = [0.20, 0.21]
        result["published_5min_T_band_K"] = [328.0, 331.0]
        result["X_error_to_band"] = (
            max(0.20 - final["Xmean"], 0.0)
            + max(final["Xmean"] - 0.21, 0.0)
        )
        result["T_error_to_band_K"] = (
            max(328.0 - final["TsolidMean"], 0.0)
            + max(final["TsolidMean"] - 331.0, 0.0)
        )

    if case_name.startswith("industrial"):
        m_air = meta["inlet_dry_air_mass_flow_kg_per_s_per_m"]
        cp_air = meta["cp_gas"]
        tin = meta["t_inlet"]
        # A generous first-law ceiling: cool all inlet air to 373.15 K and use
        # every joule for evaporation.  Real operation will be below this.
        sensible_power = max(m_air * cp_air * (tin - 373.15), 0.0)
        energy_limited_rate = sensible_power / meta["latent_heat"]
        lower_bound_time = (
            meta["water_to_xcr_kg_per_m"] / energy_limited_rate
            if energy_limited_rate > 0
            else None
        )
        width_for_one_kg_s = (
            meta["latent_heat"]
            / (
                meta["rho_gas"]
                * meta["u_inlet"]
                * cp_air
                * max(tin - 373.15, 1.0e-30)
            )
        )
        result.update(
            {
                "inlet_dry_air_mass_flow_kg_s_m": m_air,
                "generous_sensible_power_W_m": sensible_power,
                "generous_energy_limited_evaporation_kg_s_m": energy_limited_rate,
                "first_law_minimum_time_to_Xcr_s": lower_bound_time,
                "thermal_power_required_for_1kg_s_W": meta["latent_heat"],
                "minimum_width_for_1kg_s_at_same_conditions_m": width_for_one_kg_s,
                "required_rate_for_600s_kg_s_m": meta[
                    "required_average_rate_to_xcr_600s"
                ],
                "required_rate_for_1800s_kg_s_m": meta[
                    "required_average_rate_to_xcr_1800s"
                ],
            }
        )

    return result


def markdown(summary: dict[str, dict]) -> str:
    lines = [
        "# OpenFOAM drying validation result",
        "",
        "All reported cases passed the fail-closed numerical checks: finite fields,",
        "non-negative evaporation, monotonic bulk solid moisture, exact internal",
        "gas/solid source cancellation, and open moisture balance within 0.5%.",
        "",
        "| Case | End (s) | X start | X end | t(Xcr) (s) | Peak kg/s/m | Avg kg/s/m | Max balance |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, item in summary.items():
        tc = "not reached" if item["critical_time_s"] is None else f"{item['critical_time_s']:.6g}"
        lines.append(
            f"| {name} | {item['end_time_s']:.6g} | {item['initial_Xmean']:.6g} | "
            f"{item['final_Xmean']:.6g} | {tc} | "
            f"{item['peak_source_rate_kg_s_m']:.6g} | "
            f"{item['average_solid_water_removal_kg_s_m']:.6g} | "
            f"{item['max_open_balance_relative']:.3e} |"
        )

    industrial = [v for k, v in summary.items() if k.startswith("industrial")]
    if industrial:
        central = summary.get("industrial_central", industrial[0])
        lines.extend(
            [
                "",
                "## Independent first-law screen",
                "",
                f"The reconstructed industrial section contains {central['dry_solid_inventory_kg_m']:.3f} kg/m dry solid and must remove {central['water_to_Xcr_kg_m']:.3f} kg/m to move from X=0.16 to Xcr=0.05.",
                f"At the documented inlet condition, the generous air-sensible-heat ceiling is {central['generous_energy_limited_evaporation_kg_s_m']:.4f} kg/s/m, implying an absolute lower bound of {central['first_law_minimum_time_to_Xcr_s']:.1f} s before losses and outlet-temperature constraints.",
                f"A true 1 kg/s water rate requires at least {central['thermal_power_required_for_1kg_s_W']/1e6:.3f} MW of latent duty and, at the same inlet velocity and temperature, an effective 2D width/depth scale of at least {central['minimum_width_for_1kg_s_at_same_conditions_m']:.3f} m even under the same idealized heat-use assumption.",
                "",
                "The 1 kg/s figure must therefore be compared on the same out-of-plane-depth basis as the 2D CFD result; it cannot be imposed as a per-metre target without checking the represented dryer depth and heat supply.",
            ]
        )

    if "semolina" in summary:
        s = summary["semolina"]
        lines.extend(
            [
                "",
                "## Semolina 5-minute comparison",
                "",
                f"At {s['end_time_s']:.1f} s the reduced model gives X={s['final_Xmean']:.5f} and particle-average T={s['final_Tsolid_K']:.2f} K. The client-thread benchmark band is approximately X=0.20-0.21 and T=328-331 K.",
                "This comparison is a methodology check, not material-specific validation of the industrial synthetic-rutile kinetics.",
            ]
        )

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cases", nargs="+", help="case directory names")
    parser.add_argument("--output", default=str(ROOT / "results"))
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    summary = {case: validate_case(case) for case in args.cases}
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output / "report.md").write_text(markdown(summary), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
