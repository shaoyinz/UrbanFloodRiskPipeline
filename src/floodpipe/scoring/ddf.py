"""Depth-damage function lookup and replacement value calculation.

Both operate on numpy arrays so they vectorize cleanly inside a pandas
UDF on Spark. The DDF lookup is piecewise-linear with endpoint clamping;
that matches HAZUS convention and matches notebook 05.
"""

from __future__ import annotations

import numpy as np

from floodpipe.scoring.archetypes import ArchetypeConfig

SQFT_PER_M2 = 10.7639


def damage_fraction(
    archetype: np.ndarray,
    depth_ft: np.ndarray,
    config: ArchetypeConfig,
) -> np.ndarray:
    """Per-row damage fraction in [0, 1].

    archetype: 1-D array of HAZUS codes ("RES1", "COM1", ...).
    depth_ft:  1-D array of depths above first floor, feet.
    """
    if archetype.shape != depth_ft.shape:
        raise ValueError(f"shape mismatch: {archetype.shape} vs {depth_ft.shape}")
    out = np.zeros_like(depth_ft, dtype="float64")
    for code, table in config.depth_damage_ft.items():
        mask = archetype == code
        if not mask.any():
            continue
        xs = np.array([t[0] for t in table])
        ys = np.array([t[1] for t in table])
        out[mask] = np.interp(depth_ft[mask], xs, ys, left=ys[0], right=ys[-1])
    return out


def replacement_value_usd(
    archetype: np.ndarray,
    footprint_m2: np.ndarray,
    num_floors: np.ndarray,
    config: ArchetypeConfig,
) -> np.ndarray:
    """Replacement value = area · floors · $/sqft per archetype.

    Returns dollars. Unknown archetypes (not in the config table) yield
    zero — surface those upstream via a coverage check rather than
    silently producing a number.
    """
    rate = np.zeros_like(footprint_m2, dtype="float64")
    for code, dollars in config.replacement_usd_per_sqft.items():
        rate[archetype == code] = dollars
    return footprint_m2 * SQFT_PER_M2 * num_floors * rate
