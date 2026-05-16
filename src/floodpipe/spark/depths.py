"""Pure, Spark-free flood-depth and terrain logic for the silver job.

Every function here is array-vectorised and import-light so it can run
inside a pandas UDF on the Sedona executors *and* be unit-tested on the
driver without a Spark session. This module is the single source of
truth for the depth rules first prototyped in
``notebooks/exploration/04`` — the Sedona job
(``floodpipe.spark.build_silver``) does only the spatial joins and
raster sampling, then defers every row-wise scalar decision here.

Conventions:
- depths and elevations are in **metres**;
- NFHL ``STATIC_BFE`` and ``S_BFE.ELEV`` arrive in NAVD88 **feet** and
  are converted on read;
- a missing/unresolved BFE yields a zero depth (no damage) — callers
  carry ``bfe_m`` itself to tell "above the flood" from "unscored".
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FT_TO_M = 0.3048

# NFHL flood-zone codes inside the Special Flood Hazard Area (the 1%
# annual-chance floodplain). Everything else is X / shaded-X / D / OUT.
SFHA_ZONES = frozenset({"A", "AE", "AH", "AO", "AR", "A99", "V", "VE"})

# HAZUS shadow assumption (notebook 04): with no NFHL 0.2%-event water
# surface, the 500-yr depth is the 100-yr depth + 1 ft inside the SFHA,
# and a flat 1 ft above grade in the 0.2%-shaded X band. Phase 4 swaps
# this for a real 500-yr water-surface grid.
DEPTH_500_OVER_100_M = 1.0 * FT_TO_M
DEPTH_500_X_SHADED_M = 1.0 * FT_TO_M

# Severity ranks for breaking ties when a centroid lands in overlapping
# NFHL polygons (raw NFHL zones are not mutually exclusive).
SEVERITY_NONE = 0
SEVERITY_X = 1
SEVERITY_SHADED_X = 2
SEVERITY_SFHA = 3

# Substring that marks a 0.2%-annual-chance ("shaded X") polygon in the
# NFHL ZONE_SUBTY attribute.
SHADED_SUBTYPE_TOKEN = "0.2 PCT"


def _norm(values) -> pd.Series:
    """Coerce a string-ish array to an upper-cased, stripped pandas Series."""
    return (
        pd.Series(values, dtype="object")
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )


def in_sfha(fld_zone) -> np.ndarray:
    """Boolean mask: building sits in the 1% Special Flood Hazard Area."""
    return _norm(fld_zone).isin(SFHA_ZONES).to_numpy()


def is_shaded_x(fld_zone, zone_subty) -> np.ndarray:
    """Boolean mask: zone X polygon flagged 0.2%-annual-chance ("shaded X")."""
    zone = _norm(fld_zone)
    subty = _norm(zone_subty)
    return (
        (zone == "X")
        & subty.str.contains(SHADED_SUBTYPE_TOKEN, regex=False)
    ).to_numpy()


def zone_severity(fld_zone, zone_subty) -> np.ndarray:
    """Rank overlapping NFHL polygons: SFHA > shaded-X > plain X > none.

    Used as the ``ORDER BY`` key when a centroid that falls in two
    polygons is deduplicated down to its single worst zone.
    """
    zone = _norm(fld_zone)
    subty = _norm(zone_subty)
    out = np.full(len(zone), SEVERITY_NONE, dtype="int64")
    out[(zone == "X").to_numpy()] = SEVERITY_X
    out[subty.str.contains(SHADED_SUBTYPE_TOKEN, regex=False).to_numpy()] = (
        SEVERITY_SHADED_X
    )
    out[zone.isin(SFHA_ZONES).to_numpy()] = SEVERITY_SFHA
    return out


def resolve_bfe_m(static_bfe_ft, line_elev_ft) -> np.ndarray:
    """Base flood elevation in metres.

    ``STATIC_BFE`` (the NFHL polygon attribute) is used where it is
    positive; otherwise the ``ELEV`` of the nearest ``S_BFE`` line.
    Where neither exists the result is ``NaN`` — the building is in a
    zone without a published BFE (X, D) and stays unscored for hazard.
    """
    static = pd.to_numeric(pd.Series(static_bfe_ft), errors="coerce").to_numpy(
        dtype="float64"
    )
    line = pd.to_numeric(pd.Series(line_elev_ft), errors="coerce").to_numpy(
        dtype="float64"
    )
    bfe_ft = np.where(static > 0, static, line)
    return bfe_ft * FT_TO_M


def depth_100_m(bfe_m, elev_m) -> np.ndarray:
    """100-yr inundation depth = max(0, BFE − ground elevation).

    A ``NaN`` BFE (no published value) or ``NaN`` ground elevation
    yields 0 — no depth, no damage. Callers keep ``bfe_m`` to
    distinguish that from a building genuinely above the flood.
    """
    depth = np.asarray(bfe_m, dtype="float64") - np.asarray(elev_m, dtype="float64")
    return np.clip(np.nan_to_num(depth, nan=0.0), 0.0, None)


def depth_500_m(depth_100, in_sfha_mask, shaded_x_mask) -> np.ndarray:
    """500-yr inundation depth under the HAZUS shadow assumption."""
    d100 = np.asarray(depth_100, dtype="float64")
    sfha = np.asarray(in_sfha_mask, dtype=bool)
    shaded = np.asarray(shaded_x_mask, dtype=bool)
    out = np.zeros_like(d100)
    out[sfha] = d100[sfha] + DEPTH_500_OVER_100_M
    out[shaded] = DEPTH_500_X_SHADED_M
    return out


def slope_degrees(elev_n, elev_s, elev_e, elev_w, cell_size_m) -> np.ndarray:
    """Terrain slope in degrees from a 4-neighbour finite-difference gradient.

    ``dz/dx = (E − W) / 2Δ``, ``dz/dy = (N − S) / 2Δ``, then
    ``slope = atan(|∇z|)``. ``Δ`` is the centre-to-neighbour spacing in
    metres. Coarser than Horn's 3×3 operator but adequate for a ~10 m
    DEM; Phase 4 replaces it with a proper focal-slope raster.
    """
    north = np.asarray(elev_n, dtype="float64")
    south = np.asarray(elev_s, dtype="float64")
    east = np.asarray(elev_e, dtype="float64")
    west = np.asarray(elev_w, dtype="float64")
    dz_dx = (east - west) / (2.0 * cell_size_m)
    dz_dy = (north - south) / (2.0 * cell_size_m)
    return np.degrees(np.arctan(np.hypot(dz_dx, dz_dy)))


def topographic_position_index(center, elev_n, elev_s, elev_e, elev_w) -> np.ndarray:
    """TPI = focal elevation − mean of the 4 cardinal neighbours.

    Positive = local high (levee, ridge — drains); negative = local sink
    (ponds water). NaN neighbours are ignored in the mean.
    """
    neighbours = np.vstack(
        [np.asarray(v, dtype="float64") for v in (elev_n, elev_s, elev_e, elev_w)]
    )
    with np.errstate(invalid="ignore"):
        neighbour_mean = np.nanmean(neighbours, axis=0)
    return np.asarray(center, dtype="float64") - neighbour_mean
