"""Ingest USGS 3DEP 1/3" DEM tiles covering the Florida bbox into raw.

Phase 1 (notebook 03) downloaded 4 tiles for Harris County, sampled them
in-memory, and discarded the rasters. Phase 2 keeps the COGs in GCS so
the Sedona job can sample them in parallel via ``RS_Value`` against a
tiled mosaic (CLAUDE.md §Pitfalls — DEM sampling cost).

Tile convention (matches notebook 03): each 1°×1° tile is named by its
NW corner. ``n28w083`` covers lat ∈ [27, 28], lon ∈ [-83, -82].

For Florida we enumerate ~7 × 9 = 63 candidate tiles. Many are
ocean-only and not produced by USGS; a HEAD request filters those
before we pay for the GET.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import requests

from floodpipe.ingest._common import (
    FLORIDA_BBOX,
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    http_user_agent,
    load_release_config,
    raw_bucket,
)

DEP_HTTPS_BASE = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/current"


def tiles_for_bbox(bbox: tuple[float, float, float, float]) -> list[tuple[int, int]]:
    """Return (nw_lat, nw_lon) integer corners covering the bbox.

    bbox is (min_lon, min_lat, max_lon, max_lat). NW corner convention:
    a tile named (nw_lat, nw_lon) covers lat ∈ [nw_lat-1, nw_lat] and
    lon ∈ [nw_lon, nw_lon+1].
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    nw_lats = range(int(math.floor(min_lat)) + 1, int(math.ceil(max_lat)) + 1)
    nw_lons = range(int(math.floor(min_lon)), int(math.ceil(max_lon)))
    return [(lat, lon) for lat in nw_lats for lon in nw_lons]


def tile_name(nw_lat: int, nw_lon: int) -> str:
    return f"n{nw_lat:02d}w{abs(nw_lon):03d}"


def tile_https_url(nw_lat: int, nw_lon: int) -> str:
    name = tile_name(nw_lat, nw_lon)
    return f"{DEP_HTTPS_BASE}/{name}/USGS_13_{name}.tif"


def gcs_tile_uri(raw_uri: str, product: str, state_abbr: str, name: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/usgs/3dep/product={product}"
        f"/state={state_abbr}/USGS_13_{name}.tif"
    )


def _head_exists(url: str, session: requests.Session) -> bool:
    """HEAD-check a 3DEP URL. USGS S3 returns 200 if the object exists, 404 otherwise."""
    resp = session.head(url, timeout=30, allow_redirects=True)
    return resp.status_code == 200


def _stream_download(url: str, dest: Path, session: requests.Session) -> None:
    with session.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        seen = 0
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                f.write(chunk)
                seen += len(chunk)
                if total:
                    pct = 100 * seen / total
                    print(f"\r      {seen / 1e6:.0f} / {total / 1e6:.0f} MB ({pct:.0f}%)",
                          end="")
        print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bucket-prefix", default=None)
    parser.add_argument("--stage-dir", default="data/raw/dem_tiles")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--max-tiles", type=int, default=None,
        help="Limit number of tiles processed (for smoke tests)",
    )
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    candidates = tiles_for_bbox(FLORIDA_BBOX)
    if args.max_tiles is not None:
        candidates = candidates[: args.max_tiles]

    print(f"  product:    1/3 arc-second ({cfg.dep_product})")
    print(f"  candidates: {len(candidates)} tiles for FL bbox")
    print(f"  target:     {raw_uri}/usgs/3dep/product={cfg.dep_product}/state={cfg.target_state_abbr}/")

    session = requests.Session()
    session.headers.update({"User-Agent": http_user_agent()})

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    n_skipped_present = 0
    n_skipped_missing = 0
    n_uploaded = 0

    for nw_lat, nw_lon in candidates:
        name = tile_name(nw_lat, nw_lon)
        url = tile_https_url(nw_lat, nw_lon)
        gcs_uri = gcs_tile_uri(raw_uri, cfg.dep_product, cfg.target_state_abbr, name)

        if not args.force and gcs_object_exists(gcs_uri):
            n_skipped_present += 1
            continue

        if args.dry_run:
            print(f"  would check {name}")
            continue

        if not _head_exists(url, session):
            n_skipped_missing += 1
            continue

        local = stage_dir / f"USGS_13_{name}.tif"
        if not local.exists() or local.stat().st_size == 0:
            print(f"  downloading {name}")
            _stream_download(url, local, session)
        else:
            print(f"  using cached {local.name}")

        print(f"  uploading {name} -> {gcs_uri}")
        gcs_upload(local, gcs_uri)
        n_uploaded += 1

    print(
        f"  done — uploaded {n_uploaded}, already-present {n_skipped_present}, "
        f"upstream-missing {n_skipped_missing}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
