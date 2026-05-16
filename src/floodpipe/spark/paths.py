"""Raw-zone input and silver-zone output URIs for the Spark silver job.

The raw paths re-use the ingest modules' own builders so the raw layout
has a single source of truth — change a path in ``floodpipe.ingest`` and
this job follows. Only the silver-zone layout is defined here, since the
silver job is the thing that creates it.
"""

from __future__ import annotations

from floodpipe.ingest import dem, nfhl, overture


def overture_input(raw_uri: str, release: str, state_abbr: str) -> str:
    """Overture buildings GeoParquet written by ``floodpipe.ingest.overture``."""
    return overture.gcs_target_uri(raw_uri, release, state_abbr)


def nfhl_input(raw_uri: str, snapshot: str, state_abbr: str, kind: str) -> str:
    """NFHL ``zones`` or ``bfe`` GeoParquet written by ``floodpipe.ingest.nfhl``."""
    return nfhl.gcs_target_uri(raw_uri, snapshot, state_abbr, kind)


def dem_input_glob(raw_uri: str, product: str, state_abbr: str) -> str:
    """Glob over every 3DEP COG tile written by ``floodpipe.ingest.dem``.

    ``dem.gcs_tile_uri`` builds one URI per named tile; the silver job
    reads them all at once, so we re-derive the shared directory and
    append ``*.tif`` rather than enumerating tile names again here.
    """
    sample = dem.gcs_tile_uri(raw_uri, product, state_abbr, "PLACEHOLDER")
    tile_dir = sample.rsplit("/", 1)[0]
    return f"{tile_dir}/*.tif"


def silver_buildings_output(silver_uri: str, release: str, state_abbr: str) -> str:
    """Release-tagged silver-zone directory for the enriched building GeoParquet.

    Keyed on the Overture release (not the NFHL snapshot or DEM product):
    the building set is what a re-ingest changes, and downstream BigQuery
    external tables point at one release at a time.
    """
    return (
        f"{silver_uri.rstrip('/')}/buildings"
        f"/release={release}/state={state_abbr}"
    )
