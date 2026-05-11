"""Ingest FEMA NFHL flood zones + BFEs (Florida) via FGDL into the raw zone.

FEMA retired the bulk MSC state-GDB downloads, and their public NFHL
ArcGIS REST endpoint is too slow + flaky for state-scale bulk export
(multi-hour halving cascades on dense coastal tiles, repeated 5xx). The
University of Florida GeoPlan Center mirrors the same upstream FEMA NFHL
data as a quarterly Florida-wide FGDB ZIP at https://fgdl.org/zips/ —
fresher snapshot than the REST endpoint, one ~1.7 GB download, parses
and uploads in minutes.

CONUS scaling (Phase 4): no single FGDL-equivalent exists nationally;
each state has its own mirror situation (TX → TNRIS, NC → NCOneMap, …).
That's out of Phase 2 scope per CLAUDE.md.

Layers extracted (FGDL keeps the FEMA NFHL schema verbatim):
- ``dfirm_fldhaz``  flood hazard zones (polygons)  → maps to S_FLD_HAZ_AR
- ``dfirm_bfe``     base flood elevations (lines)  → maps to S_BFE

On-ingest transform: reproject to EPSG:4326. FGDL ships in
EPSG:3086 (Florida Albers). No geometry simplification here — that
belongs in the silver Spark step where the BigQuery 10 MB GEOGRAPHY
limit fix is applied.
"""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

import geopandas as gpd
import pyogrio
import requests

from floodpipe.ingest._common import (
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    http_user_agent,
    load_release_config,
    raw_bucket,
)

# Logical kind → FEMA layer name (the schema the dbt silver staging
# expects). Kept as documentation of which fields are available; the
# FGDL FGDB exposes these as feature classes named ``dfirm_fldhaz`` /
# ``dfirm_bfe`` but the columns inside match the FEMA dictionary.
NFHL_LAYERS: dict[str, str] = {
    "zones": "S_FLD_HAZ_AR",
    "bfe": "S_BFE",
}

# Candidate feature-class names per kind, tried in order. FGDL has used
# both prefixed and non-prefixed forms across releases — list more than
# we strictly need so a future filename tweak doesn't break ingest.
FGDL_LAYER_CANDIDATES: dict[str, tuple[str, ...]] = {
    "zones": ("dfirm_fldhaz", "FLDHAZ", "S_FLD_HAZ_AR"),
    "bfe": ("dfirm_bfe", "BFE", "S_BFE"),
}

FGDL_BASE = "https://fgdl.org/zips/geospatial_data/current"


def fgdl_zip_url(snapshot: str, kind: str) -> str:
    """e.g. ('apr26', 'zones') → '.../current/dfirm_fldhaz_apr26.zip'."""
    fgdl_kind = {"zones": "fldhaz", "bfe": "bfe"}[kind]
    return f"{FGDL_BASE}/dfirm_{fgdl_kind}_{snapshot}.zip"


def gcs_target_uri(raw_uri: str, snapshot: str, state_abbr: str, kind: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/fema/nfhl/snapshot={snapshot}"
        f"/state={state_abbr}/{kind}.parquet"
    )


def _stream_download(url: str, dest: Path, *, chunk: int = 1 << 20) -> None:
    headers = {"User-Agent": http_user_agent()}
    with requests.get(url, stream=True, headers=headers, timeout=600) as r:
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
                    print(
                        f"\r    {seen / 1e6:.0f} / {total / 1e6:.0f} MB ({pct:.0f}%)",
                        end="",
                        flush=True,
                    )
        print()


def _find_gdb(extracted_root: Path) -> Path:
    candidates = list(extracted_root.rglob("*.gdb"))
    if not candidates:
        raise FileNotFoundError(f"no .gdb directory under {extracted_root}")
    if len(candidates) > 1:
        # Multiple .gdb dirs typically means the zip bundled additional
        # reference layers; FGDL's flood zips contain just one. Surface
        # the conflict rather than guess.
        raise RuntimeError(f"multiple .gdb dirs found: {candidates}")
    return candidates[0]


def _pick_layer(gdb: Path, candidates: tuple[str, ...]) -> str:
    """Return the first candidate present in the GDB, else raise.

    Falls back to a case-insensitive prefix match so an FGDL renaming
    like ``dfirm_fldhaz_apr26`` (snapshot baked into the layer name)
    still resolves.
    """
    available = [name for name, _ in pyogrio.list_layers(gdb)]
    available_lower = {n.lower(): n for n in available}
    for cand in candidates:
        if cand in available:
            return cand
        if cand.lower() in available_lower:
            return available_lower[cand.lower()]
    # Prefix match
    for cand in candidates:
        for name in available:
            if name.lower().startswith(cand.lower()):
                return name
    raise RuntimeError(
        f"none of {candidates} found in {gdb.name}; available layers: {available}"
    )


def _extract_layer(gdb: Path, layer_name: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(gdb, layer=layer_name, engine="pyogrio")
    if gdf.crs is None:
        raise RuntimeError(f"layer {layer_name} has no CRS")
    if str(gdf.crs).upper() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


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

    urls = {kind: fgdl_zip_url(cfg.nfhl_snapshot, kind) for kind in NFHL_LAYERS}
    targets = {
        kind: gcs_target_uri(raw_uri, cfg.nfhl_snapshot, cfg.target_state_abbr, kind)
        for kind in NFHL_LAYERS
    }

    print(f"  snapshot: {cfg.nfhl_snapshot}")
    for kind in NFHL_LAYERS:
        print(f"  source {kind}: {urls[kind]}")
        print(f"  target {kind}: {targets[kind]}")

    if args.dry_run:
        return 0

    if not args.force and all(gcs_object_exists(uri) for uri in targets.values()):
        print("  all targets exist — skipping (re-run with --force)")
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    for kind, layer_logical in NFHL_LAYERS.items():
        uri = targets[kind]
        if not args.force and gcs_object_exists(uri):
            print(f"  {kind}: target exists, skipping")
            continue

        url = urls[kind]
        zip_path = stage_dir / f"nfhl_{cfg.target_state_abbr}_{kind}_{cfg.nfhl_snapshot}.zip"
        if not zip_path.exists():
            print(f"  downloading {kind} -> {zip_path}")
            _stream_download(url, zip_path)
        else:
            print(f"  using cached download {zip_path}")

        extract_dir = zip_path.with_suffix("")
        extract_dir.mkdir(exist_ok=True)
        print(f"  unzipping -> {extract_dir}")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

        gdb = _find_gdb(extract_dir)
        layer_name = _pick_layer(gdb, FGDL_LAYER_CANDIDATES[kind])
        print(f"  geodatabase: {gdb.name}, layer: {layer_name} (logical={layer_logical})")
        gdf = _extract_layer(gdb, layer_name)

        local = stage_dir / f"nfhl_{cfg.target_state_abbr}_{kind}.parquet"
        gdf.to_parquet(local, compression="zstd")
        print(
            f"    {len(gdf):,} features -> {local.name} "
            f"({local.stat().st_size / 1e6:.1f} MB)"
        )

        print(f"  uploading {kind} -> {uri}")
        gcs_upload(local, uri)

    print("  done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
