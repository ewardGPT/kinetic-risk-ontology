"""Kinetic Risk correlation engine.

Consumes market.ticks, market.fills, chain.transfers from Redpanda.
Maintains in-memory rolling windows for per-market probability and per-cluster
stablecoin flow. When both fire a z-score breach within a short window, runs
a lead-lag cross-correlation and emits a KineticRiskAlert.
"""
from __future__ import annotations

import asyncio
import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer
from kro_common import (
    BusConsumer,
    PgPool,
    TOPICS,
    get_settings,
    insert_alert,
    setup_logging,
)

log = structlog.get_logger("kinetic-risk")


WINDOW_SECONDS = 1800
BUCKET_SECONDS = 60
MIN_BUCKETS = 12
Z_THRESHOLD = 2.0
FLOW_Z_THRESHOLD = 2.0
ALERT_COOLDOWN_SECONDS = 600
LEAD_LAG_BUCKETS = 6
MAX_CLUSTERS_IN_WINDOW = 5000


class RollingSeries:
    def __init__(self, window_seconds: int = WINDOW_SECONDS, bucket_seconds: int = BUCKET_SECONDS) -> None:
        self.window = window_seconds
        self.bucket = bucket_seconds
        self._points: dict[str, deque[tuple[float, float]]] = defaultdict(deque)

    def _prune(self, key: str, now: float) -> None:
        cutoff = now - self.window
        dq = self._points[key]
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def add(self, key: str, value: float, ts: float | None = None) -> None:
        now = ts or time.time()
        self._prune(key, now)
        self._points[key].append((now, value))

    def bucket_aggregate(self, key: str, agg: str = "last") -> tuple[float, float, float] | None:
        """Return (bucket_ts, agg_value, weight) for the current bucket, or None."""
        dq = self._points.get(key)
        if not dq:
            return None
        now = time.time()
        bucket_ts = (now // self.bucket) * self.bucket
        values = [v for ts, v in dq if ts >= bucket_ts]
        if not values:
            latest_ts, latest_v = dq[-1]
            return float(latest_ts), latest_v, 1.0
        if agg == "last":
            return float(bucket_ts), values[-1], float(len(values))
        if agg == "sum":
            return float(bucket_ts), sum(values), float(len(values))
        return float(bucket_ts), sum(values) / len(values), float(len(values))

    def rolling_buckets(self, key: str, n: int = 12) -> list[tuple[float, float]]:
        dq = self._points.get(key)
        if not dq:
            return []
        by_bucket: dict[float, list[float]] = defaultdict(list)
        for ts, v in dq:
            bts = (ts // self.bucket) * self.bucket
            by_bucket[bts].append(v)
        series = sorted(by_bucket.items())
        return [(bts, sum(vs) / len(vs)) for bts, vs in series[-n:]]

    def zscore(self, key: str, current: float) -> float | None:
        series = self.rolling_buckets(key, n=MIN_BUCKETS * 2)
        if len(series) < MIN_BUCKETS:
            return None
        values = [v for _, v in series]
        n = len(values)
        mean = sum(values) / n
        var = sum((v - mean) ** 2 for v in values) / max(n - 1, 1)
        std = var ** 0.5
        if std < 1e-9:
            return None
        return (current - mean) / std


def lead_lag_confidence(a: list[float], b: list[float], max_lag: int = LEAD_LAG_BUCKETS) -> tuple[float, int]:
    """Windowed cross-correlation. Returns (best_score, lag_buckets) where lag>0 means b leads a."""
    if len(a) < 6 or len(b) < 6 or len(a) != len(b):
        return 0.0, 0
    n = len(a)
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    var_a = sum((x - mean_a) ** 2 for x in a) or 1e-9
    var_b = sum((x - mean_b) ** 2 for x in b) or 1e-9
    best = 0.0
    best_lag = 0
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            x = a[lag:]
            y = b[: n - lag]
        else:
            x = a[:lag]
            y = b[-lag:]
        if len(x) < 6:
            continue
        m = len(x)
        cov = sum((x[i] - mean_a) * (y[i] - mean_b) for i in range(m)) / m
        corr = cov / (var_a * var_b) ** 0.5
        if abs(corr) > abs(best):
            best = corr
            best_lag = lag
    return float(best), int(best_lag)


class KineticEngine:
    def __init__(self, pool: PgPool) -> None:
        self.pool = pool
        self.market_prob = RollingSeries()
        self.market_volume = RollingSeries()
        self.cluster_flow = RollingSeries()
        self.cluster_market_traders: dict[str, set[str]] = defaultdict(set)
        self.market_traders: dict[str, set[str]] = defaultdict(set)
        self._addr_to_cluster: dict[str, str] = {}
        self.last_alert_at: dict[tuple[str, str], float] = {}
        self.curve_cache: dict[str, list[tuple[float, float]]] = {}

    async def refresh_market_traders(self) -> None:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT market_id, taker_address
                FROM market_fills
                WHERE time > NOW() - INTERVAL '24 hours'
                  AND taker_address IS NOT NULL
                """
            )
        self.market_traders.clear()
        for r in rows:
            if r["market_id"] and r["taker_address"]:
                self.market_traders[r["market_id"]].add(r["taker_address"].lower())
        async with self.pool.acquire() as conn:
            cluster_rows = await conn.fetch(
                "SELECT address, cluster_id FROM wallets WHERE cluster_id IS NOT NULL"
            )
        self._addr_to_cluster = {r["address"].lower(): r["cluster_id"] for r in cluster_rows}
        self.cluster_market_traders.clear()
        for market, wallets in self.market_traders.items():
            for w in wallets:
                c = self._addr_to_cluster.get(w)
                if c:
                    self.cluster_market_traders[market].add(c)
        log.info(
            "market_traders_refreshed",
            markets=len(self.market_traders),
            clusters=len(self._addr_to_cluster),
        )

    def handle_market_tick(self, msg: dict) -> None:
        market_id = msg.get("market_id")
        prob = msg.get("probability")
        if market_id is None or prob is None:
            return
        try:
            p = float(prob)
        except (TypeError, ValueError):
            return
        ts = msg.get("time")
        if isinstance(ts, str):
            try:
                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts_f = ts_dt.timestamp()
            except Exception:
                ts_f = time.time()
        else:
            ts_f = time.time()
        self.market_prob.add(market_id, p, ts=ts_f)
        size = msg.get("size")
        if size is not None:
            try:
                self.market_volume.add(market_id, float(size), ts=ts_f)
            except (TypeError, ValueError):
                pass

    def handle_chain_transfer(self, msg: dict) -> None:
        ts_str = msg.get("time")
        if isinstance(ts_str, str):
            try:
                ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_f = ts_dt.timestamp()
            except Exception:
                ts_f = time.time()
        else:
            ts_f = time.time()
        amount = msg.get("amount_human")
        if amount is None:
            return
        try:
            amt = float(amount)
        except (TypeError, ValueError):
            return
        from_addr = (msg.get("from_address") or "").lower()
        to_addr = (msg.get("to_address") or "").lower()
        skip = {
            "0x0000000000000000000000000000000000000000",
            "0x000000000000000000000000000000000000dead",
        }
        for addr in (from_addr, to_addr):
            if not addr or addr in skip:
                continue
            self.cluster_flow.add(addr, amt, ts=ts_f)
            cluster = self._addr_to_cluster.get(addr)
            if cluster:
                self.cluster_flow.add(f"cluster:{cluster}", amt, ts=ts_f)

    async def evaluate(self) -> list[dict]:
        alerts: list[dict] = []
        now = time.time()
        for market, traders in self.market_traders.items():
            if not traders:
                continue
            prob_now = self.market_prob.bucket_aggregate(market, "last")
            vol_now = self.market_volume.bucket_aggregate(market, "sum")
            if not prob_now or not vol_now:
                continue
            prob_z = self.market_prob.zscore(market, prob_now[1])
            if prob_z is None or abs(prob_z) < Z_THRESHOLD:
                continue
            market_series = [v for _, v in self.market_prob.rolling_buckets(market, n=24)]
            if len(market_series) < 12:
                continue
            for cluster in self.cluster_market_traders.get(market, set()):
                flow_series = [v for _, v in self.cluster_flow.rolling_buckets(cluster, n=24)]
                if len(flow_series) < 12:
                    continue
                flow_now = self.cluster_flow.bucket_aggregate(cluster, "sum")
                if not flow_now:
                    continue
                flow_z = self.cluster_flow.zscore(cluster, flow_now[1])
                if flow_z is None or abs(flow_z) < FLOW_Z_THRESHOLD:
                    continue
                if len(market_series) != len(flow_series):
                    n = min(len(market_series), len(flow_series))
                    market_series = market_series[-n:]
                    flow_series = flow_series[-n:]
                corr, lag = lead_lag_confidence(market_series, flow_series)
                if abs(corr) < 0.3:
                    continue
                cooldown_key = (market, cluster)
                last = self.last_alert_at.get(cooldown_key, 0)
                if now - last < ALERT_COOLDOWN_SECONDS:
                    continue
                self.last_alert_at[cooldown_key] = now
                market_signal = {
                    "zscore": prob_z,
                    "velocity_buckets": market_series[-8:],
                    "volume_now": vol_now[1],
                    "cluster_trader_count": len(traders),
                }
                flow_signal = {
                    "zscore": flow_z,
                    "flow_now": flow_now[1],
                    "rolling_buckets": flow_series[-8:],
                }
                entity_risk = await self.entity_risk_for_cluster(cluster)
                lead_lag = {
                    "cross_corr": corr,
                    "lag_buckets": lag,
                    "interpretation": (
                        "on-chain flow leads market" if lag > 0 else
                        "market leads on-chain flow" if lag < 0 else
                        "coincident"
                    ),
                }
                entity_w = entity_risk.get("weight", 1.0)
                composite = min(
                    1.0,
                    abs(prob_z) / 5.0 * abs(flow_z) / 5.0 * entity_w * (0.5 + abs(corr) / 2.0),
                )
                if composite < 0.05:
                    continue
                alert = {
                    "time": datetime.now(tz=timezone.utc),
                    "market_id": market,
                    "cluster_id": cluster,
                    "entity_id": entity_risk.get("entity_id"),
                    "market_signal": market_signal,
                    "flow_signal": flow_signal,
                    "entity_risk": entity_risk,
                    "lead_lag": lead_lag,
                    "composite_score": composite,
                    "state": "open",
                }
                alerts.append(alert)
        return alerts

    async def entity_risk_for_cluster(self, cluster_id: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT w.entity_id, e.risk_level, e.type, w.risk_flags
                FROM wallets w
                LEFT JOIN entities e ON w.entity_id = e.entity_id
                WHERE w.cluster_id = $1
                LIMIT 1
                """,
                cluster_id,
            )
        if not row:
            return {"weight": 1.0, "risk_level": "unknown"}
        weight = 1.0
        if row.get("risk_level") == "critical":
            weight = 3.0
        elif row.get("risk_level") == "high":
            weight = 2.0
        elif row.get("risk_level") == "medium":
            weight = 1.5
        flags = row.get("risk_flags") or []
        if "sanctioned" in flags:
            weight = max(weight, 3.0)
        if "mixer-adjacent" in flags:
            weight = max(weight, 2.0)
        return {
            "weight": weight,
            "risk_level": row.get("risk_level") or "unknown",
            "entity_id": row.get("entity_id"),
            "type": row.get("type"),
            "risk_flags": list(flags),
        }


async def run_engine() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    log.info("starting_kinetic_risk_engine", brokers=settings.redpanda_brokers)

    pool = PgPool()
    await pool.connect()

    engine = KineticEngine(pool)
    await engine.refresh_market_traders()

    consumer = BusConsumer(
        brokers=settings.redpanda_brokers,
        group_id="kinetic-risk",
        topics=[TOPICS["market_ticks"], TOPICS["chain_transfers"]],
        client_id="kinetic-risk",
    )
    await consumer.start()

    stop = asyncio.Event()
    def _sig(*_): stop.set()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    last_traders_refresh = 0.0
    last_evaluate = 0.0
    n_alerts_emitted = 0

    async def eval_loop() -> None:
        nonlocal last_evaluate, n_alerts_emitted
        while not stop.is_set():
            try:
                alerts = await engine.evaluate()
                for a in alerts:
                    async with pool.acquire() as conn:
                        alert_id = await insert_alert(conn, a)
                    a["id"] = alert_id
                    log.info(
                        "kinetic_alert_emitted",
                        alert_id=alert_id,
                        market=a["market_id"],
                        cluster=a["cluster_id"],
                        score=a["composite_score"],
                    )
                    n_alerts_emitted += 1
            except Exception as e:
                log.error("evaluate_error", err=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            last_evaluate = time.time()

    async def refresh_loop() -> None:
        nonlocal last_traders_refresh
        while not stop.is_set():
            if time.time() - last_traders_refresh > 300:
                try:
                    await engine.refresh_market_traders()
                except Exception as e:
                    log.error("refresh_traders_error", err=str(e))
                last_traders_refresh = time.time()
            try:
                await asyncio.wait_for(stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    eval_task = asyncio.create_task(eval_loop())
    refresh_task = asyncio.create_task(refresh_loop())
    try:
        async for topic, msg in consumer.consume():
            if topic == TOPICS["market_ticks"]:
                engine.handle_market_tick(msg)
            elif topic == TOPICS["chain_transfers"]:
                engine.handle_chain_transfer(msg)
    finally:
        eval_task.cancel()
        refresh_task.cancel()
        await consumer.stop()
        await pool.close()
        log.info("kinetic_risk_engine_stopped", alerts_emitted=n_alerts_emitted)


if __name__ == "__main__":
    asyncio.run(run_engine())
