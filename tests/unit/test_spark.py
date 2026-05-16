"""Unit tests for the Spark silver job's pure, Spark-free helpers.

The Sedona orchestration in ``build_silver`` needs a Spark session and is
exercised by the Dataproc-emulator integration suite; here we test only
``floodpipe.spark.paths`` (URI builders) and ``floodpipe.spark.depths``
(the row-wise scalar logic the pandas UDFs wrap).
"""

from __future__ import annotations

import math

import numpy as np

from floodpipe.ingest._common import raw_bucket, silver_bucket
from floodpipe.spark import depths, paths


class TestSilverBucket:
    def test_default_prefix(self):
        assert silver_bucket("acme-prj") == "gs://acme-prj-floodpipe-silver"

    def test_override_prefix(self):
        assert silver_bucket("acme-prj", "custom") == "gs://custom-silver"


class TestPaths:
    RAW = "gs://acme-floodpipe-raw"
    SILVER = "gs://acme-floodpipe-silver"

    def test_overture_input(self):
        uri = paths.overture_input(self.RAW, "2026-04-15.0", "FL")
        assert uri == (
            f"{self.RAW}/overture/release=2026-04-15.0/state=FL/buildings.parquet"
        )

    def test_nfhl_input_zones_and_bfe(self):
        zones = paths.nfhl_input(self.RAW, "apr26", "FL", "zones")
        bfe = paths.nfhl_input(self.RAW, "apr26", "FL", "bfe")
        assert zones.endswith("/fema/nfhl/snapshot=apr26/state=FL/zones.parquet")
        assert bfe.endswith("/fema/nfhl/snapshot=apr26/state=FL/bfe.parquet")

    def test_dem_input_glob(self):
        glob = paths.dem_input_glob(self.RAW, "13", "FL")
        assert glob == f"{self.RAW}/usgs/3dep/product=13/state=FL/*.tif"
        # the placeholder tile name must not leak into the glob
        assert "PLACEHOLDER" not in glob

    def test_silver_output_release_tagged(self):
        uri = paths.silver_buildings_output(self.SILVER, "2026-04-15.0", "FL")
        assert uri == (
            f"{self.SILVER}/buildings/release=2026-04-15.0/state=FL"
        )

    def test_silver_output_strips_trailing_slash(self):
        uri = paths.silver_buildings_output(self.SILVER + "/", "r", "FL")
        assert "//buildings" not in uri.removeprefix("gs://")

    def test_raw_and_silver_buckets_are_siblings(self):
        assert raw_bucket("p").removesuffix("-raw") == (
            silver_bucket("p").removesuffix("-silver")
        )


class TestZoneClassification:
    def test_in_sfha(self):
        zones = ["AE", "VE", "X", "OUT", None, "ae"]
        np.testing.assert_array_equal(
            depths.in_sfha(zones), [True, True, False, False, False, True]
        )

    def test_is_shaded_x(self):
        zone = ["X", "X", "AE", "X"]
        subty = ["0.2 PCT ANNUAL CHANCE FLOOD HAZARD", None, "0.2 PCT", "AREA"]
        np.testing.assert_array_equal(
            depths.is_shaded_x(zone, subty), [True, False, False, False]
        )

    def test_zone_severity_ranks_worst_highest(self):
        zone = ["AE", "X", "X", "OUT"]
        subty = [None, "0.2 PCT ANNUAL", None, None]
        sev = depths.zone_severity(zone, subty)
        # SFHA(3) > shaded-X(2) > plain X(1) > none(0)
        np.testing.assert_array_equal(sev, [3, 2, 1, 0])

    def test_zone_severity_sfha_beats_shaded_token(self):
        # an SFHA zone that also carries the shaded token still ranks SFHA
        sev = depths.zone_severity(["AE"], ["0.2 PCT"])
        assert sev[0] == depths.SEVERITY_SFHA


class TestBfeResolution:
    def test_static_bfe_wins_when_positive(self):
        bfe = depths.resolve_bfe_m([30.0], [99.0])
        assert math.isclose(bfe[0], 30.0 * depths.FT_TO_M)

    def test_falls_back_to_line_elevation(self):
        bfe = depths.resolve_bfe_m([0.0], [20.0])
        assert math.isclose(bfe[0], 20.0 * depths.FT_TO_M)

    def test_unresolved_is_nan(self):
        bfe = depths.resolve_bfe_m([None], [None])
        assert np.isnan(bfe[0])


class TestDepths:
    def test_depth_100_clips_negative_to_zero(self):
        # building above the BFE -> no depth
        d = depths.depth_100_m([2.0, 5.0], [4.0, 1.0])
        np.testing.assert_allclose(d, [0.0, 4.0])

    def test_depth_100_nan_inputs_yield_zero(self):
        d = depths.depth_100_m([np.nan, 3.0], [1.0, np.nan])
        np.testing.assert_allclose(d, [0.0, 0.0])

    def test_depth_500_sfha_adds_one_foot(self):
        d = depths.depth_500_m([2.0], [True], [False])
        assert math.isclose(d[0], 2.0 + depths.DEPTH_500_OVER_100_M)

    def test_depth_500_shaded_x_is_flat_one_foot(self):
        d = depths.depth_500_m([0.0], [False], [True])
        assert math.isclose(d[0], depths.DEPTH_500_X_SHADED_M)

    def test_depth_500_outside_hazard_is_zero(self):
        d = depths.depth_500_m([0.0], [False], [False])
        assert d[0] == 0.0


class TestTerrain:
    def test_slope_flat_terrain_is_zero(self):
        s = depths.slope_degrees([5.0], [5.0], [5.0], [5.0], cell_size_m=10.0)
        assert math.isclose(s[0], 0.0, abs_tol=1e-9)

    def test_slope_known_gradient(self):
        # 10 m rise over 20 m run (N-S) -> atan(0.5) ~ 26.565 deg
        s = depths.slope_degrees([10.0], [0.0], [5.0], [5.0], cell_size_m=10.0)
        assert math.isclose(s[0], math.degrees(math.atan(0.5)), rel_tol=1e-6)

    def test_tpi_positive_on_local_high(self):
        tpi = depths.topographic_position_index([10.0], [5.0], [5.0], [5.0], [5.0])
        assert math.isclose(tpi[0], 5.0)

    def test_tpi_negative_in_sink(self):
        tpi = depths.topographic_position_index([0.0], [3.0], [3.0], [3.0], [3.0])
        assert math.isclose(tpi[0], -3.0)

    def test_tpi_ignores_nan_neighbour(self):
        tpi = depths.topographic_position_index(
            [10.0], [4.0], [np.nan], [4.0], [4.0]
        )
        assert math.isclose(tpi[0], 6.0)
