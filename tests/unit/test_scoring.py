"""Unit tests for floodpipe.scoring.

Anchored on hand-computed expectations rather than golden files so the
tests are debuggable from this file alone.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from floodpipe.scoring import (
    assign_archetype,
    damage_fraction,
    expected_annual_damage,
    interpolate_depth_gumbel,
    load_archetype_config,
    replacement_value_usd,
)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "archetypes.yaml"


@pytest.fixture(scope="module")
def config():
    return load_archetype_config(CONFIG_PATH)


class TestArchetypeAssignment:
    def test_residential_default_is_res1(self, config):
        assert assign_archetype("residential", None, config) == "RES1"

    def test_multifamily_residential_is_res3(self, config):
        assert assign_archetype("residential", "multifamily", config) == "RES3"

    def test_commercial_retail_is_com1(self, config):
        assert assign_archetype("commercial", "retail", config) == "COM1"

    def test_commercial_office_is_com4(self, config):
        assert assign_archetype("commercial", "office", config) == "COM4"

    def test_unknown_class_falls_back(self, config):
        assert assign_archetype("alien-spaceship", None, config) == config.fallback

    def test_none_inputs_fall_back(self, config):
        assert assign_archetype(None, None, config) == config.fallback

    def test_case_insensitive(self, config):
        assert assign_archetype("RESIDENTIAL", "MULTIFAMILY", config) == "RES3"


class TestDamageFraction:
    def test_negative_depth_is_zero(self, config):
        out = damage_fraction(np.array(["RES1"]), np.array([-5.0]), config)
        assert out[0] == 0.0

    def test_exact_table_point(self, config):
        # RES1 table has (1 ft -> 0.14)
        out = damage_fraction(np.array(["RES1"]), np.array([1.0]), config)
        assert out[0] == pytest.approx(0.14)

    def test_clamps_at_upper_endpoint(self, config):
        # Far past the 16 ft endpoint stays at the last value.
        out = damage_fraction(np.array(["RES1"]), np.array([100.0]), config)
        assert out[0] == pytest.approx(0.60)

    def test_linear_interpolation_midpoint(self, config):
        # RES1: (0,0.09) -> (1,0.14). At 0.5 ft, expect 0.115.
        out = damage_fraction(np.array(["RES1"]), np.array([0.5]), config)
        assert out[0] == pytest.approx(0.115)

    def test_per_row_archetype_dispatch(self, config):
        out = damage_fraction(
            np.array(["RES1", "COM1"]),
            np.array([1.0, 1.0]),
            config,
        )
        # RES1 @ 1 ft = 0.14; COM1 @ 1 ft = 0.09.
        assert out[0] == pytest.approx(0.14)
        assert out[1] == pytest.approx(0.09)

    def test_mismatched_shapes_raise(self, config):
        with pytest.raises(ValueError):
            damage_fraction(np.array(["RES1"]), np.array([1.0, 2.0]), config)


class TestReplacementValue:
    def test_res1_one_story(self, config):
        # 100 m2 · 10.7639 sqft/m2 · 1 floor · $110/sqft = $118,402.9
        out = replacement_value_usd(
            np.array(["RES1"]),
            np.array([100.0]),
            np.array([1.0]),
            config,
        )
        assert out[0] == pytest.approx(118402.9, rel=1e-4)

    def test_floors_scale_linearly(self, config):
        a = replacement_value_usd(
            np.array(["RES1"]), np.array([100.0]), np.array([1.0]), config,
        )[0]
        b = replacement_value_usd(
            np.array(["RES1"]), np.array([100.0]), np.array([3.0]), config,
        )[0]
        assert b == pytest.approx(3 * a)


class TestGumbelInterpolation:
    def test_t100_passthrough(self):
        d100 = np.array([1.0, 2.5, 0.0])
        out = interpolate_depth_gumbel(d100, np.array([3.0, 5.0, 1.0]), 100)
        np.testing.assert_array_equal(out, d100)

    def test_t500_passthrough(self):
        d500 = np.array([3.0, 5.0, 1.0])
        out = interpolate_depth_gumbel(np.array([1.0, 2.5, 0.0]), d500, 500)
        np.testing.assert_array_equal(out, d500)

    def test_t50_below_t100_when_depth_increases_with_rp(self):
        # If d_500 > d_100 (typical), depth at T=50 < d_100.
        d100 = np.full(5, 2.0)
        d500 = np.full(5, 4.0)
        out = interpolate_depth_gumbel(d100, d500, 50)
        assert np.all(out < d100)

    def test_t10_clamped_at_zero(self):
        # At T=10 the linear-in-y extrapolation typically goes negative
        # for buildings near the 100-yr WSE; clamp must hold.
        d100 = np.array([0.5])
        d500 = np.array([2.0])
        out = interpolate_depth_gumbel(d100, d500, 10)
        assert out[0] >= 0.0

    def test_monotone_in_return_period(self):
        d100 = np.array([1.0])
        d500 = np.array([3.0])
        d10 = interpolate_depth_gumbel(d100, d500, 10)[0]
        d50 = interpolate_depth_gumbel(d100, d500, 50)[0]
        d100_val = d100[0]
        d500_val = d500[0]
        assert d10 <= d50 <= d100_val <= d500_val


class TestEAD:
    def test_two_point_matches_notebook_formula(self):
        # Notebook 5: EAD = ½·(1/100 − 1/500)·(D_100 + D_500)
        d100 = np.array([10_000.0])
        d500 = np.array([50_000.0])
        ead = expected_annual_damage({100: d100, 500: d500})
        expected = 0.5 * (1 / 100 - 1 / 500) * (d100 + d500)
        np.testing.assert_allclose(ead, expected)

    def test_four_point_adds_lower_tail_segments(self):
        # Hand-computed: with D_10=D_50=0, D_100=D_500=20_000,
        # EAD = ½·(1/10 − 1/50)·(0+0)
        #     + ½·(1/50 − 1/100)·(0+20000)
        #     + ½·(1/100 − 1/500)·(20000+20000)
        #     = 0 + 100 + 160 = 260
        d = np.array([20_000.0])
        zero = np.array([0.0])
        ead4 = expected_annual_damage({10: zero, 50: zero, 100: d, 500: d})
        assert ead4[0] == pytest.approx(260.0)

    def test_requires_at_least_two_points(self):
        with pytest.raises(ValueError):
            expected_annual_damage({100: np.array([1.0])})

    def test_zeros_yield_zero(self):
        zero = np.zeros(3)
        ead = expected_annual_damage({100: zero, 500: zero})
        np.testing.assert_array_equal(ead, zero)
