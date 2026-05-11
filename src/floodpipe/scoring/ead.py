"""Expected Annual Damage integration over the loss-frequency curve.

CLAUDE.md §Methodology asks for a 4-point trapezoid over (10, 50, 100,
500)-year return periods. NFHL ships depths only at T=100 and T=500, so
we extrapolate the 10- and 50-year depths under a Gumbel (EV-I) tail —
a 2-parameter special case of the GEV with shape ξ=0. Two known points
exactly determine the two free parameters (location μ, scale σ), so this
is parameter-free given the NFHL inputs.

Gumbel quantile:
    depth(T) = μ + σ · y(T)        where y(T) = −ln(−ln(1 − 1/T))

Two known points (T_a=100, T_b=500) give
    σ = (d_b − d_a) / (y_b − y_a)
    μ = d_a − σ · y_a

For T < 100 the extrapolated depth can go negative when a building sits
above the 100-yr WSE; we clamp at zero (no flood, no damage).
"""

from __future__ import annotations

import numpy as np


def _gumbel_reduced_variate(return_period: float) -> float:
    return float(-np.log(-np.log(1.0 - 1.0 / return_period)))


def interpolate_depth_gumbel(
    depth_100_m: np.ndarray,
    depth_500_m: np.ndarray,
    target_return_period: int,
) -> np.ndarray:
    """Estimate flood depth at target_return_period from the (100, 500) pair.

    For T == 100 or T == 500 this returns the input exactly. For other T,
    the Gumbel extrapolation is applied row-wise and clamped at zero.
    """
    if target_return_period == 100:
        return np.asarray(depth_100_m, dtype="float64")
    if target_return_period == 500:
        return np.asarray(depth_500_m, dtype="float64")

    y_100 = _gumbel_reduced_variate(100)
    y_500 = _gumbel_reduced_variate(500)
    y_t = _gumbel_reduced_variate(target_return_period)

    d_100 = np.asarray(depth_100_m, dtype="float64")
    d_500 = np.asarray(depth_500_m, dtype="float64")

    sigma = (d_500 - d_100) / (y_500 - y_100)
    mu = d_100 - sigma * y_100
    depth = mu + sigma * y_t
    return np.clip(depth, 0.0, None)


def expected_annual_damage(
    damage_by_rp: dict[int, np.ndarray],
) -> np.ndarray:
    """Trapezoidal integration of dollar damage over probability.

    damage_by_rp: mapping {return_period_years: damage_usd_array}. Keys
    are not required to be sorted; the function sorts ascending by T,
    which is descending in exceedance probability 1/T.

    EAD = Σ_i ½ (1/T_i − 1/T_{i+1}) · (D_i + D_{i+1})

    Damage at the highest T (rarest event) contributes only via the last
    segment; the contribution of the infinite-T tail is ignored. Damage
    at the lowest T anchors the lower end of the curve at the
    user-supplied value — for T=10 that's an approximation of the
    "annual" tail.
    """
    if len(damage_by_rp) < 2:
        raise ValueError("need >=2 return periods to integrate")
    ts = sorted(damage_by_rp)
    ead = np.zeros_like(damage_by_rp[ts[0]], dtype="float64")
    for t_lo, t_hi in zip(ts, ts[1:]):
        d_lo = damage_by_rp[t_lo]
        d_hi = damage_by_rp[t_hi]
        dp = (1.0 / t_lo) - (1.0 / t_hi)
        ead += 0.5 * dp * (d_lo + d_hi)
    return ead
