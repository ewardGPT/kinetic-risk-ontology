"""Unit tests for the kinetic risk engine — no infra required."""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "libs" / "kro_common" / "src"))

from kro_kinetic_risk import RollingSeries, lead_lag_confidence, jaccard  # noqa: E402


def test_rolling_series_zscore_too_few_points():
    rs = RollingSeries(window_seconds=600, bucket_seconds=60)
    rs.add("m1", 0.5)
    assert rs.zscore("m1", 0.7) is None


def test_rolling_series_zscore_after_threshold():
    rs = RollingSeries(window_seconds=600, bucket_seconds=60)
    for i in range(20):
        rs.add("m1", 0.5 + (i % 5) * 0.001)
    z = rs.zscore("m1", 0.95)
    assert z is not None
    assert z > 5.0


def test_lead_lag_perfect_positive():
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
    b = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
    corr, lag = lead_lag_confidence(a, b, max_lag=3)
    assert abs(corr - 1.0) < 1e-6
    assert lag == 0


def test_lead_lag_lagged_signal():
    a = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    b = [0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    corr, lag = lead_lag_confidence(a, b, max_lag=4)
    assert lag != 0
    assert abs(corr) > 0.3


def test_lead_lag_insufficient_data():
    a = [0.1, 0.2, 0.3]
    b = [0.1, 0.2, 0.3]
    corr, lag = lead_lag_confidence(a, b)
    assert corr == 0.0
    assert lag == 0


def test_jaccard():
    assert jaccard(set(), set()) == 0.0
    assert jaccard({1, 2, 3}, {3, 4, 5}) == pytest.approx(1 / 5)
    assert jaccard({1, 2, 3}, {1, 2, 3}) == 1.0
