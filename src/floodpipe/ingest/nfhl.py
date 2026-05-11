"""Ingest FEMA NFHL state geodatabase (Florida) into the raw zone.

Phase 1 (notebook 02) used the FEMA ArcGIS REST endpoint with OBJECTID
batching — fine for one county, painful at state scale. Phase 2 switches
to the state-level NFHL_State_GDB download per CLAUDE.md §Data sources:
one ~150 MB ZIP, two layers we care about.

Layers extracted:
- ``S_FLD_HAZ_AR``  flood hazard zones (polygons)
- ``S_BFE``         base flood elevations (lines)

On-ingest transform: reproject to EPSG:4326. NFHL ships in state-plane
CRS that varies by county; normalizing here saves every downstream
consumer the chore. No geometry simplification — that belongs in the
Spark silver step where the BigQuery size-limit fix is applied.
"""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import requests

from floodpipe.ingest._common import (
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    http_user_agent,
    load_release_config,
    raw_bucket,
)

NFHL_LAYERS: dict[str, str] = {
    "zones": "S_FLD_HAZ_AR",
    "bfe": "S_BFE",
}


def nfhl_zip_url(msc_base: str, state_abbr: str) -> str:
    return f"{msc_base.rstrip('/')}/{state_abbr}_NFHL.zip"


def gcs_target_uri(raw_uri: str, snapshot: str, state_abbr: str, kind: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/fema/nfhl/snapshot={snapshot}"
        f"/state={state_abbr}/{kind}.parquet"
    )


def _stream_download(url: str, dest: Path, *, chunk: int = 1 << 20) -> None:
    headers = {"User-Agent": http_user_agent()}
    with requests.get(url, stream=True, headers=headers, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        seen = 0
        with dest.open("wb") as f:
            for buf in r.iter_content(chunk_size=chunk):
                if not buf:
                    continue
                f.write(buf)
                seen += len(buf)
                if total:
                    pct = 100 * seen / total
                    print(f"\r    {seen / 1e6:.0f} / {total / 1e6:.0f} MB ({pct:.0f}%)", end="")
        print()


def _find_gdb(extracted_root: Path) -> Path:
    candidates = list(extracted_root.rglob("*.gdb"))
    if not candidates:
        raise FileNotFoundError(f"no .gdb directory under {extracted_root}")
    if len(candidates) > 1:
        raise RuntimeError(f"multiple .gdb dirs found: {candidates}")
    return candidates[0]


def _extract_layer(gdb: Path, layer_name: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(gdb, layer=layer_name, engine="pyogrio")
    if gdf.crs is None:
        raise RuntimeError(f"layer {layer_name} has no CRS")
    if str(gdf.crs).upper() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _process_layers(
    gdb: Path, layers: Iterable[tuple[str, str]], stage_dir: Path, state_abbr: str
) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for kind, layer_name in layers:
        print(f"  reading layer {layer_name} ...")
        gdf = _extract_layer(gdb, layer_name)
        path = stage_dir / f"nfhl_{state_abbr}_{kind}.parquet"
        gdf.to_parquet(path, compression="zstd")
        print(f"    {len(gdf):,} features -> {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
        out[kind] = path
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bucket-prefix", default=None)
    parser.add_argument("--stage-dir", default="data/raw")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    url = nfhl_zip_url(cfg.nfhl_msc_base, cfg.target_state_abbr)

    targets = {
        kind: gcs_target_uri(raw_uri, cfg.nfhl_snapshot, cfg.target_state_abbr, kind)
        for kind in NFHL_LAYERS
    }

    print(f"  snapshot: {cfg.nfhl_snapshot}")
    print(f"  source:   {url}")
    for kind, uri in targets.items():
        print(f"  target {kind}: {uri}")

    if args.dry_run:
        return 0

    if not args.force and all(gcs_object_exists(uri) for uri in targets.values()):
        print("  all targets exist — skipping (re-run with --force)")
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    zip_path = stage_dir / f"nfhl_{cfg.target_state_abbr}_{cfg.nfhl_snapshot}.zip"

    if not zip_path.exists():
        print(f"  downloading -> {zip_path}")
        _stream_download(url, zip_path)
    else:
        print(f"  using cached download {zip_path}")

    extract_dir = stage_dir / f"nfhl_{cfg.target_state_abbr}_{cfg.nfhl_snapshot}_extracted"
    extract_dir.mkdir(exist_ok=True)
    print(f"  unzipping -> {extract_dir}")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    gdb = _find_gdb(extract_dir)
    print(f"  geodatabase: {gdb.name}")
    staged = _process_layers(
        gdb, [(kind, layer) for kind, layer in NFHL_LAYERS.items()],
        stage_dir, cfg.target_state_abbr,
    )

    for kind, local_path in staged.items():
        uri = targets[kind]
        print(f"  uploading {kind} -> {uri}")
        gcs_upload(local_path, uri)
    print("  done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
