"""Sedona silver-zone job: raw GeoParquet + COGs → enriched building GeoParquet.

The Phase-2 step that lifts notebooks 03–04 onto Dataproc Serverless for
the whole of Florida. For every Overture building footprint it derives:

  - centroid (lon/lat) and footprint area (m², CONUS Albers EPSG:5070)
  - H3 cell IDs at resolutions 6–9 — multi-scale aggregation keys
  - FEMA NFHL flood zone + subtype, SFHA flag, hazard source
  - base flood elevation (m): STATIC_BFE, else nearest S_BFE line
  - ground elevation (m), slope (deg) and topographic position index,
    sampled from the 3DEP 1/3 arc-second DEM
  - 100-yr and 500-yr inundation depth (m)

Scoring — archetype assignment, depth-damage, EAD, equity — is
deliberately NOT here. That is the dbt gold step, which reads this job's
output as a BigQuery external table over the silver zone.

Run on Dataproc Serverless (the Sedona + GDAL jars and a packaged
``floodpipe`` zip are wired by the submit command in README Phase 2):

    python -m floodpipe.spark.build_silver --project-id <gcp-project>

``--dry-run`` prints the resolved input/output URIs and exits without a
Spark session; ``--limit N`` caps the building count for a smoke test.
"""

from __future__ import annotations

import argparse

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BooleanType, DoubleType, IntegerType

from floodpipe.ingest._common import (
    ReleaseConfig,
    load_release_config,
    raw_bucket,
    silver_bucket,
)
from floodpipe.spark import depths, paths
from floodpipe.spark.session import sedona_session

# 3DEP 1/3 arc-second resolution: one cell is 1/10800° on a side, which
# is ~10.3 m of ground at Florida latitudes (111 320 m per degree / 10800).
DEM_CELL_DEG = 1.0 / 10800.0
DEM_CELL_M = 111_320.0 * DEM_CELL_DEG

# H3 resolutions emitted per building (CLAUDE.md §Component 7).
H3_RESOLUTIONS = (6, 7, 8, 9)

# Default shuffle width for the H3-res-7 repartition that defeats
# spatial-join skew. Override with --partitions on a larger cluster.
DEFAULT_PARTITIONS = 256

# Bound for the nearest-S_BFE-line lookup, in degrees (~2.4 km at FL
# latitudes). A centroid further than this from any BFE line keeps a
# null BFE rather than snapping to an implausibly distant one.
BFE_SEARCH_RADIUS_DEG = 0.022


# --------------------------------------------------------------------------
# pandas UDFs — thin wrappers over floodpipe.spark.depths so the row-wise
# scalar logic stays pure-Python, single-sourced, and unit-testable.
# --------------------------------------------------------------------------
@pandas_udf(IntegerType())
def _zone_severity_udf(fld_zone: pd.Series, zone_subty: pd.Series) -> pd.Series:
    return pd.Series(depths.zone_severity(fld_zone, zone_subty))


@pandas_udf(BooleanType())
def _in_sfha_udf(fld_zone: pd.Series) -> pd.Series:
    return pd.Series(depths.in_sfha(fld_zone))


@pandas_udf(BooleanType())
def _is_shaded_x_udf(fld_zone: pd.Series, zone_subty: pd.Series) -> pd.Series:
    return pd.Series(depths.is_shaded_x(fld_zone, zone_subty))


@pandas_udf(DoubleType())
def _bfe_m_udf(static_bfe_ft: pd.Series, line_elev_ft: pd.Series) -> pd.Series:
    return pd.Series(depths.resolve_bfe_m(static_bfe_ft, line_elev_ft))


@pandas_udf(DoubleType())
def _depth_100_udf(bfe_m: pd.Series, elev_m: pd.Series) -> pd.Series:
    return pd.Series(depths.depth_100_m(bfe_m, elev_m))


@pandas_udf(DoubleType())
def _depth_500_udf(
    depth_100: pd.Series, fld_zone: pd.Series, zone_subty: pd.Series
) -> pd.Series:
    return pd.Series(
        depths.depth_500_m(
            depth_100,
            depths.in_sfha(fld_zone),
            depths.is_shaded_x(fld_zone, zone_subty),
        )
    )


@pandas_udf(DoubleType())
def _slope_udf(
    elev_n: pd.Series, elev_s: pd.Series, elev_e: pd.Series, elev_w: pd.Series
) -> pd.Series:
    return pd.Series(
        depths.slope_degrees(elev_n, elev_s, elev_e, elev_w, DEM_CELL_M)
    )


@pandas_udf(DoubleType())
def _tpi_udf(
    center: pd.Series,
    elev_n: pd.Series,
    elev_s: pd.Series,
    elev_e: pd.Series,
    elev_w: pd.Series,
) -> pd.Series:
    return pd.Series(
        depths.topographic_position_index(center, elev_n, elev_s, elev_e, elev_w)
    )


# --------------------------------------------------------------------------
# Pipeline stages
# --------------------------------------------------------------------------
def load_buildings(sedona, uri: str, limit: int | None) -> DataFrame:
    """Read Overture buildings and add centroid, area and H3 keys.

    Footprint area is measured in EPSG:5070 (CONUS Albers Equal Area),
    not a Web-Mercator projection, so areas are not latitude-distorted.
    The H3 cell for each resolution is taken at the centroid; ``true``
    asks Sedona for full-cover cells, of which a point yields exactly one.
    """
    df = sedona.read.format("geoparquet").load(uri)
    df = df.selectExpr(
        "id",
        "class AS building_class",
        "subtype AS building_subtype",
        "CAST(num_floors AS INT) AS num_floors",
        "CAST(height AS DOUBLE) AS height_m",
        "geometry",
        "ST_Centroid(geometry) AS centroid",
        "ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:5070')) "
        "AS footprint_area_m2",
    )
    if limit is not None:
        df = df.limit(limit)
    df = df.withColumn("centroid_lon", F.expr("ST_X(centroid)"))
    df = df.withColumn("centroid_lat", F.expr("ST_Y(centroid)"))
    for res in H3_RESOLUTIONS:
        df = df.withColumn(
            f"h3_res{res}",
            F.expr(f"element_at(ST_H3CellIDs(centroid, {res}, true), 1)"),
        )
    return df


def join_flood_zones(sedona, buildings: DataFrame, zones_uri: str) -> DataFrame:
    """Left-join building centroids to NFHL zone polygons, keeping the worst.

    A centroid that lands in overlapping NFHL polygons (AE under a
    shaded-X, say) produces one row per polygon; the row_number window
    keeps only the most severe. Joining on the centroid rather than the
    footprint avoids double-counting buildings that straddle a boundary,
    matching the HAZUS parcel convention used in notebook 04.
    """
    zones = sedona.read.format("geoparquet").load(zones_uri).selectExpr(
        "FLD_ZONE AS fld_zone",
        "ZONE_SUBTY AS zone_subty",
        "CAST(STATIC_BFE AS DOUBLE) AS static_bfe_ft",
        "geometry AS zone_geom",
    )
    joined = buildings.join(
        zones, F.expr("ST_Within(centroid, zone_geom)"), "left"
    )
    joined = joined.withColumn(
        "zone_severity", _zone_severity_udf("fld_zone", "zone_subty")
    )
    worst = Window.partitionBy("id").orderBy(F.col("zone_severity").desc())
    joined = (
        joined.withColumn("_rn", F.row_number().over(worst))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "zone_geom", "zone_severity")
    )
    # Centroids outside every NFHL polygon: label explicitly so the
    # hazard_source flag and dbt staging can tell "dry land" from null.
    joined = joined.withColumn("fld_zone", F.coalesce("fld_zone", F.lit("OUT")))
    joined = joined.withColumn("in_sfha", _in_sfha_udf("fld_zone"))
    joined = joined.withColumn(
        "is_shaded_x", _is_shaded_x_udf("fld_zone", "zone_subty")
    )
    return joined


def resolve_bfe_lines(sedona, buildings: DataFrame, bfe_uri: str) -> DataFrame:
    """Attach the nearest S_BFE line elevation to SFHA buildings lacking STATIC_BFE.

    Only SFHA buildings without a positive ``STATIC_BFE`` need a line
    lookup. ``ST_DWithin`` bounds the join to a search radius so it does
    not degrade to a full cross product; the closest surviving line per
    building wins. Buildings that need no lookup carry a null
    ``line_elev_ft`` and fall back to ``STATIC_BFE`` downstream.
    """
    needs_lookup = buildings.filter(
        F.col("in_sfha")
        & (F.col("static_bfe_ft").isNull() | (F.col("static_bfe_ft") <= 0))
    ).select("id", "centroid")

    lines = sedona.read.format("geoparquet").load(bfe_uri).selectExpr(
        "CAST(ELEV AS DOUBLE) AS line_elev_ft",
        "geometry AS bfe_geom",
    )
    candidates = needs_lookup.join(
        lines,
        F.expr(
            f"ST_DWithin(centroid, bfe_geom, {BFE_SEARCH_RADIUS_DEG})"
        ),
        "inner",
    ).withColumn("_dist", F.expr("ST_Distance(centroid, bfe_geom)"))

    nearest = Window.partitionBy("id").orderBy(F.col("_dist").asc())
    closest = (
        candidates.withColumn("_rn", F.row_number().over(nearest))
        .filter(F.col("_rn") == 1)
        .select("id", "line_elev_ft")
    )
    return buildings.join(closest, "id", "left")


def sample_dem(sedona, buildings: DataFrame, dem_glob: str) -> DataFrame:
    """Sample ground elevation, slope and TPI from the 3DEP DEM tiles.

    Rasters are loaded out-of-DB via ``RS_FromPath`` (Sedona ≥ 1.5 with
    GDAL on the image): each row carries a lightweight path-backed handle,
    not pixel bytes, so the broadcast tile table stays tiny and ``RS_Value``
    reads only the pixels it needs.

    Ground elevation is the minimum over the centroid and every footprint
    vertex — the most conservative grade under the building, which is
    what the HAZUS depth curves assume (notebook 03). Slope and TPI come
    from a 4-neighbour stencil one DEM cell off the centroid; the
    longitude offset is widened by ``1/cos(lat)`` so the stencil stays
    square on the ground.
    """
    rasters = (
        sedona.read.format("binaryFile")
        .load(dem_glob)
        .selectExpr("RS_FromPath(path) AS raster")
        .selectExpr("raster", "RS_Envelope(raster) AS tile_env")
    )
    # 27 FL tiles — broadcast the (out-of-DB) handles to every partition.
    df = buildings.join(
        F.broadcast(rasters),
        F.expr("ST_Intersects(centroid, tile_env)"),
        "left",
    )

    lon_step = "(" + str(DEM_CELL_DEG) + " / cos(radians(centroid_lat)))"
    df = df.selectExpr(
        "*",
        # conservative grade: min over centroid + footprint vertices
        "array_min(RS_Values(raster, "
        "array_union(array(centroid), ST_DumpPoints(geometry)), 1)) "
        "AS elevation_m",
        f"RS_Value(raster, ST_Point(centroid_lon, centroid_lat + {DEM_CELL_DEG}), 1) "
        "AS _elev_n",
        f"RS_Value(raster, ST_Point(centroid_lon, centroid_lat - {DEM_CELL_DEG}), 1) "
        "AS _elev_s",
        f"RS_Value(raster, ST_Point(centroid_lon + {lon_step}, centroid_lat), 1) "
        "AS _elev_e",
        f"RS_Value(raster, ST_Point(centroid_lon - {lon_step}, centroid_lat), 1) "
        "AS _elev_w",
    ).drop("raster", "tile_env")

    df = df.withColumn(
        "slope_deg", _slope_udf("_elev_n", "_elev_s", "_elev_e", "_elev_w")
    )
    df = df.withColumn(
        "tpi", _tpi_udf("elevation_m", "_elev_n", "_elev_s", "_elev_e", "_elev_w")
    )
    return df.drop("_elev_n", "_elev_s", "_elev_e", "_elev_w")


def compute_depths(buildings: DataFrame) -> DataFrame:
    """Resolve BFE and derive 100-yr / 500-yr depths and the hazard source."""
    df = buildings.withColumn(
        "bfe_m", _bfe_m_udf("static_bfe_ft", "line_elev_ft")
    )
    df = df.withColumn("depth_100_m", _depth_100_udf("bfe_m", "elevation_m"))
    df = df.withColumn(
        "depth_500_m", _depth_500_udf("depth_100_m", "fld_zone", "zone_subty")
    )
    # NFHL-derived where the centroid fell inside a mapped zone; otherwise
    # the building is outside effective NFHL coverage and must lean on the
    # ML susceptibility model (CLAUDE.md §Pitfalls — NFHL coverage gaps).
    df = df.withColumn(
        "hazard_source",
        F.when(F.col("fld_zone") == "OUT", F.lit("ml_only")).otherwise(
            F.lit("nfhl")
        ),
    )
    return df


SILVER_COLUMNS = [
    "id",
    "geometry",
    "centroid_lon",
    "centroid_lat",
    "building_class",
    "building_subtype",
    "num_floors",
    "height_m",
    "footprint_area_m2",
    *(f"h3_res{res}" for res in H3_RESOLUTIONS),
    "fld_zone",
    "zone_subty",
    "in_sfha",
    "is_shaded_x",
    "hazard_source",
    "bfe_m",
    "elevation_m",
    "slope_deg",
    "tpi",
    "depth_100_m",
    "depth_500_m",
]


def build(
    sedona,
    raw_uri: str,
    silver_uri: str,
    cfg: ReleaseConfig,
    *,
    partitions: int,
    limit: int | None,
) -> str:
    """Run the full silver pipeline and write GeoParquet. Returns the output URI."""
    overture_uri = paths.overture_input(
        raw_uri, cfg.overture_release, cfg.target_state_abbr
    )
    zones_uri = paths.nfhl_input(
        raw_uri, cfg.nfhl_snapshot, cfg.target_state_abbr, "zones"
    )
    bfe_uri = paths.nfhl_input(
        raw_uri, cfg.nfhl_snapshot, cfg.target_state_abbr, "bfe"
    )
    dem_glob = paths.dem_input_glob(
        raw_uri, cfg.dep_product, cfg.target_state_abbr
    )
    out_uri = paths.silver_buildings_output(
        silver_uri, cfg.overture_release, cfg.target_state_abbr
    )

    buildings = load_buildings(sedona, overture_uri, limit)
    # Repartition by H3 res-7 before any spatial join — a handful of dense
    # urban tiles otherwise stall a single executor (CLAUDE.md §Pitfalls).
    buildings = buildings.repartition(partitions, "h3_res7")

    buildings = join_flood_zones(sedona, buildings, zones_uri)
    buildings = resolve_bfe_lines(sedona, buildings, bfe_uri)
    buildings = sample_dem(sedona, buildings, dem_glob)
    buildings = compute_depths(buildings)

    out = buildings.select(*SILVER_COLUMNS)
    out.write.format("geoparquet").mode("overwrite").save(out_uri)
    return out_uri


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bucket-prefix", default=None)
    parser.add_argument(
        "--partitions",
        type=int,
        default=DEFAULT_PARTITIONS,
        help="shuffle width for the H3 res-7 repartition",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="cap building count for a smoke test",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    cfg = load_release_config()
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    silver_uri = silver_bucket(args.project_id, args.bucket_prefix)

    print(f"  release:  {cfg.overture_release} (NFHL {cfg.nfhl_snapshot})")
    print(f"  state:    {cfg.target_state_abbr}")
    print(f"  raw in:   {raw_uri}")
    print(
        "  out:      "
        + paths.silver_buildings_output(
            silver_uri, cfg.overture_release, cfg.target_state_abbr
        )
    )
    if args.dry_run:
        print("  dry run — no Spark session started")
        return 0

    sedona = sedona_session("floodpipe-silver-buildings")
    try:
        out_uri = build(
            sedona,
            raw_uri,
            silver_uri,
            cfg,
            partitions=args.partitions,
            limit=args.limit,
        )
        print(f"  done -> {out_uri}")
    finally:
        sedona.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
