"""Unit tests for the kinetic risk engine — no infra required."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "libs" / "kro_common" / "src"))

from kro_kinetic_risk import RollingSeries, lead_lag_confidence


def test_rolling_series_zscore_too_few_points():
    rs = RollingSeries(window_seconds=600, bucket_seconds=60)
    rs.add("m1", 0.5)
    assert rs.zscore("m1", 0.7) is None


def test_rolling_series_zscore_after_threshold():
    rs = RollingSeries(window_seconds=600, bucket_seconds=1)
    base = 60_000_000.0
    for i in range(20):
        rs.add("m1", 0.5 + (i % 5) * 0.001, ts=base + i * 1.5)
    z = rs.zscore("m1", 0.95)
    assert z is not None
    assert z > 100.0


def test_rolling_series_zscore_stable_signal():
    rs = RollingSeries(window_seconds=600, bucket_seconds=1)
    base = 60_000_000.0
    for i in range(20):
        rs.add("m1", 0.5, ts=base + i * 1.5)
    assert rs.zscore("m1", 0.5) is None


def test_rolling_series_zscore_finite_for_known_data():
    rs = RollingSeries(window_seconds=600, bucket_seconds=1)
    base = 60_000_000.0
    data = [
        0.50,
        0.51,
        0.49,
        0.52,
        0.48,
        0.50,
        0.51,
        0.49,
        0.50,
        0.51,
        0.49,
        0.52,
        0.48,
        0.50,
        0.51,
        0.49,
        0.50,
        0.51,
        0.49,
        0.50,
    ]
    for i, v in enumerate(data):
        rs.add("m1", v, ts=base + i * 1.5)
    z = rs.zscore("m1", 0.51)
    assert z is not None
    assert abs(z) < 5.0


def test_lead_lag_perfect_positive():
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
    b = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
    corr, lag = lead_lag_confidence(a, b, max_lag=3)
    assert abs(corr - 1.0) < 1e-6
    assert lag == 0


def test_lead_lag_perfect_negative():
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
    b = [-v for v in a]
    corr, _ = lead_lag_confidence(a, b, max_lag=3)
    assert abs(corr - (-1.0)) < 1e-6


def test_lead_lag_lagged_signal():
    a = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    b = [0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    corr, lag = lead_lag_confidence(a, b, max_lag=4)
    assert lag != 0
    assert abs(corr) > 0.5


def test_lead_lag_insufficient_data():
    a = [0.1, 0.2, 0.3]
    b = [0.1, 0.2, 0.3]
    corr, lag = lead_lag_confidence(a, b)
    assert corr == 0.0
    assert lag == 0


def test_lead_lag_mismatched_lengths():
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    b = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    corr, lag = lead_lag_confidence(a, b)
    assert corr == 0.0
    assert lag == 0


def test_rolling_series_rolling_buckets():
    rs = RollingSeries(window_seconds=600, bucket_seconds=1)
    base = 60_000_000.0
    for i, v in enumerate([0.1, 0.2, 0.3]):
        rs.add("m1", v, ts=base + i * 1.5)
    series = rs.rolling_buckets("m1", n=12)
    assert len(series) == 3
    values = [v for _, v in series]
    assert values == [0.1, 0.2, 0.3]


def test_rolling_series_window_prunes_old_data():
    rs = RollingSeries(window_seconds=10, bucket_seconds=1)
    for i in range(100):
        rs.add("m1", 0.5, ts=60_000_000.0 + i)
    series = rs.rolling_buckets("m1", n=24)
    assert len(series) <= 11


def test_rolling_series_known_key_returns_none():
    rs = RollingSeries()
    assert rs.bucket_aggregate("missing") is None
    assert rs.rolling_buckets("missing") == []
    assert rs.zscore("missing", 0.5) is None


def test_rolling_series_multiple_keys_independent():
    rs = RollingSeries(window_seconds=600, bucket_seconds=1)
    base = 60_000_000.0
    for i in range(20):
        rs.add("a", 0.5, ts=base + i * 1.5)
    rs.add("b", 0.9, ts=base + 1500)
    assert rs.zscore("a", 0.5) is None
    assert rs.zscore("b", 0.9) is None
