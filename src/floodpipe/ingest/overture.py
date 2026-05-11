"""Ingest Florida Overture buildings into the raw zone.

Mirrors the pattern from notebooks/exploration/01 (Harris County, TX):
DuckDB + spatial + httpfs reads the pinned Overture release directly
from public S3, with a bbox filter pushed down via the ``bbox`` struct
that every Overture row carries. The result is staged as a single local
parquet, then uploaded to ``gs://<raw>/overture/release=.../state=FL/``.

Why a single file and not a hive-partitioned directory: Florida is
~8 M buildings ≈ 1.5 GB compressed — small enough to keep one file
hot for Spark to repartition on read. The release+state pair is the
idempotency key; bumping either forces a fresh upload.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

from floodpipe.ingest._common import (
    FLORIDA_BBOX,
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    load_release_config,
    raw_bucket,
)


def overture_s3_glob(release: str) -> str:
    return (
        f"s3://overturemaps-us-west-2/release/{release}"
        "/theme=buildings/type=building/*"
    )


def gcs_target_uri(raw_uri: str, release: str, state_abbr: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/overture/release={release}"
        f"/state={state_abbr}/buildings.parquet"
    )


def _run_query(
    con: duckdb.DuckDBPyConnection,
    s3_glob: str,
    bbox: tuple[float, float, float, float],
    out_path: Path,
) -> None:
    min_lon, min_lat, max_lon, max_lat = bbox
    con.execute(f"""
        COPY (
            SELECT
                id,
                names.primary AS name,
                class,
                subtype,
                num_floors,
                height,
                bbox,
                geometry
            FROM read_parquet('{s3_glob}', filename=false, hive_partitioning=1)
            WHERE bbox.xmin <= {max_lon}
              AND bbox.xmax >= {min_lon}
              AND bbox.ymin <= {max_lat}
              AND bbox.ymax >= {min_lat}
        )
        TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True, help="GCP project ID owning the raw bucket")
    parser.add_argument("--bucket-prefix", default=None, help="Override bucket name prefix")
    parser.add_argument("--stage-dir", default="data/raw", help="Local staging dir (gitignored)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if target exists")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do nothing")
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    target_uri = gcs_target_uri(raw_uri, cfg.overture_release, cfg.target_state_abbr)
    s3_glob = overture_s3_glob(cfg.overture_release)

    print(f"  release: {cfg.overture_release}")
    print(f"  source:  {s3_glob}")
    print(f"  bbox:    {FLORIDA_BBOX}")
    print(f"  target:  {target_uri}")

    if args.dry_run:
        return 0

    if not args.force and gcs_object_exists(target_uri):
        print("  exists — skipping (re-run with --force to overwrite)")
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    stage_path = stage_dir / f"overture_{cfg.target_state_abbr}_{cfg.overture_release}.parquet"

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("PRAGMA threads=8;")

    print(f"  querying -> {stage_path}")
    _run_query(con, s3_glob, FLORIDA_BBOX, stage_path)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{stage_path}')").fetchone()[0]
    print(f"  staged:  {n:,} buildings, {stage_path.stat().st_size / 1e9:.2f} GB")

    print(f"  uploading -> {target_uri}")
    gcs_upload(stage_path, target_uri)
    print("  done")

    if not args.force:
        # Keep the stage file around when forcing — operator may be debugging.
        shutil.move(str(stage_path), str(stage_path.with_suffix(".parquet.uploaded")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
