"""Unit tests for the pure-Python parts of the ingest modules.

We deliberately do NOT exercise network or gcloud here. Each ingest
module isolates its testable logic — URL builders, tile enumeration,
GCS path construction — from the I/O glue in ``main()``.
"""

from __future__ import annotations

from floodpipe.ingest._common import FLORIDA_BBOX, ReleaseConfig, raw_bucket
from floodpipe.ingest import dem, nfhl, overture


class TestCommon:
    def test_raw_bucket_default_prefix(self):
        assert raw_bucket("acme-prj") == "gs://acme-prj-floodpipe-raw"

    def test_raw_bucket_override(self):
        assert raw_bucket("acme-prj", "custom-prefix") == "gs://custom-prefix-raw"

    def test_florida_bbox_sanity(self):
        w, s, e, n = FLORIDA_BBOX
        assert w < e and s < n
        # Florida lies in the (-90..-79, 24..32) box.
        assert -90 < w < -85 and 24 < s < 26
        assert -81 < e < -78 and 30 < n < 32


class TestOverture:
    def test_s3_glob_includes_release(self):
        glob = overture.overture_s3_glob("2026-04-15.0")
        assert "release/2026-04-15.0" in glob
        assert glob.startswith("s3://overturemaps-us-west-2/")
        assert glob.endswith("/*")

    def test_target_uri_hive_partitioned(self):
        uri = overture.gcs_target_uri("gs://b-raw", "2026-04-15.0", "FL")
        assert uri == "gs://b-raw/overture/release=2026-04-15.0/state=FL/buildings.parquet"

    def test_target_uri_strips_trailing_slash(self):
        uri = overture.gcs_target_uri("gs://b-raw/", "2026-04-15.0", "FL")
        assert "//overture" not in uri


class TestNFHL:
    def test_zip_url_state_filename(self):
        url = nfhl.nfhl_zip_url("https://hazards.fema.gov/nfhlv2/output/State/", "FL")
        assert url == "https://hazards.fema.gov/nfhlv2/output/State/FL_NFHL.zip"

    def test_zip_url_trailing_slash_tolerant(self):
        # _common normalizes the base; the function itself shouldn't blow up
        # when given a trailing slash.
        url = nfhl.nfhl_zip_url("https://hazards.fema.gov/nfhlv2/output/State", "FL")
        assert url.endswith("/FL_NFHL.zip")
        assert "State//" not in url

    def test_target_uri_per_kind(self):
        zones = nfhl.gcs_target_uri("gs://b-raw", "2026-04-15", "FL", "zones")
        bfe = nfhl.gcs_target_uri("gs://b-raw", "2026-04-15", "FL", "bfe")
        assert zones.endswith("/snapshot=2026-04-15/state=FL/zones.parquet")
        assert bfe.endswith("/snapshot=2026-04-15/state=FL/bfe.parquet")

    def test_layer_mapping(self):
        # Phase 1 notebook uses S_FLD_HAZ_AR (zones) and S_BFE (lines);
        # changing this without bumping the silver schema would silently
        # break downstream — pin it.
        assert nfhl.NFHL_LAYERS == {"zones": "S_FLD_HAZ_AR", "bfe": "S_BFE"}


class TestDEM:
    def test_tile_name_zero_padded(self):
        assert dem.tile_name(28, -83) == "n28w083"
        assert dem.tile_name(7, -9) == "n07w009"

    def test_https_url_matches_usgs_layout(self):
        url = dem.tile_https_url(28, -83)
        assert url.endswith("/n28w083/USGS_13_n28w083.tif")
        assert url.startswith("https://prd-tnm.s3.amazonaws.com/")

    def test_tile_enumeration_florida_dims(self):
        tiles = dem.tiles_for_bbox(FLORIDA_BBOX)
        lats = sorted({t[0] for t in tiles})
        lons = sorted({t[1] for t in tiles})
        # FL bbox spans lat 24.4..31.0 -> NW corners 25..31 (7 rows);
        # lon -87.65..-79.96 -> NW corners -88..-80 (9 cols).
        assert lats == [25, 26, 27, 28, 29, 30, 31]
        assert lons == [-88, -87, -86, -85, -84, -83, -82, -81, -80]
        assert len(tiles) == 7 * 9

    def test_tile_enumeration_matches_phase1_notebook_harris(self):
        # Notebook 03 picks 4 tiles for Harris County bbox (29.49..30.18,
        # -95.91..-94.90). Phase 2 should reproduce the same set bit-for-bit.
        harris_bbox = (-95.91, 29.49, -94.90, 30.18)
        names = sorted(dem.tile_name(*c) for c in dem.tiles_for_bbox(harris_bbox))
        assert names == ["n30w095", "n30w096", "n31w095", "n31w096"]

    def test_gcs_tile_uri_layout(self):
        uri = dem.gcs_tile_uri("gs://b-raw", "13", "FL", "n28w083")
        assert uri == "gs://b-raw/usgs/3dep/product=13/state=FL/USGS_13_n28w083.tif"
