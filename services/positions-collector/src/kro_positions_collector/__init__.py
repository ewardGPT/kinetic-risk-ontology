"""Positions collector — Polymarket Data API public trades feed + positions/value per wallet."""
from __future__ import annotations

import asyncio
import json
import signal
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
import structlog
from kro_common import (
    BusProducer,
    PgPool,
    TOPICS,
    get_settings,
    insert_market_fills,
    setup_logging,
    upsert_wallets,
)

log = structlog.get_logger("positions-collector")


TRADES_URL = "https://data-api.polymarket.com/trades"
POSITIONS_URL = "https://data-api.polymarket.com/positions"
VALUE_URL = "https://data-api.polymarket.com/value"
DEFAULT_USER_AGENT = "kro-positions-collector/0.1"
REQUEST_TIMEOUT = 25.0
TRADE_FEED_INTERVAL = 10.0
POSITION_REFRESH_INTERVAL = 1800.0
SMART_WALLET_LIMIT = 50


async def fetch_json(
    client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None
) -> Any:
    for attempt in range(4):
        try:
            r = await client.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 3:
                log.warning("fetch_failed", url=url, err=str(e))
                return None
            await asyncio.sleep(0.5 * (attempt + 1))


def _to_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _ts_to_dt(v: Any) -> datetime:
    if isinstance(v, (int, float)):
        if v > 1e12:
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        try:
            if v.isdigit():
                i = int(v)
                if i > 1e12:
                    return datetime.fromtimestamp(i / 1000, tz=timezone.utc)
                return datetime.fromtimestamp(i, tz=timezone.utc)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(tz=timezone.utc)


def trade_to_fill(trade: dict) -> dict | None:
    try:
        ts = _ts_to_dt(trade.get("timestamp"))
        condition_id = trade.get("conditionId")
        asset = trade.get("asset")
        if not condition_id or not asset:
            return None
        side = (trade.get("side") or "BUY").upper()
        price = _to_decimal(trade.get("price")) or Decimal(0)
        size = _to_decimal(trade.get("size")) or Decimal(0)
        outcome_raw = trade.get("outcome")
        outcome = "YES"
        if outcome_raw:
            ol = str(outcome_raw).lower()
            if ol in ("down", "no", "under", "false", "lose"):
                outcome = "NO"
            elif ol in ("up", "yes", "over", "true", "win"):
                outcome = "YES"
            else:
                outcome = outcome_raw.upper() if ol not in ("yes", "no") else outcome_raw
        wallet = (trade.get("proxyWallet") or "").lower() or None
        tx_hash = (trade.get("transactionHash") or "").lower() or None
        trade_id = tx_hash or f"{int(ts.timestamp())}:{condition_id}:{asset}"
        return {
            "time": ts,
            "trade_id": str(trade_id),
            "market_id": str(condition_id),
            "condition_id": str(condition_id),
            "asset_id": str(asset),
            "outcome": str(outcome),
            "side": side,
            "price": price,
            "size": size,
            "notional": price * size,
            "fee": None,
            "maker_address": None,
            "taker_address": wallet,
        }
    except Exception as e:
        log.warning("trade_parse_failed", err=str(e))
        return None


def value_to_features(value_data: Any, wallet: str) -> dict:
    if not isinstance(value_data, list) or not value_data:
        return {}
    entry = value_data[0] if isinstance(value_data[0], dict) else {}
    return {
        "address": wallet.lower(),
        "polymarket_pnl": _to_decimal(entry.get("value")),
        "polymarket_volume": None,
        "smart_money_score": None,
        "last_seen": datetime.now(tz=timezone.utc),
    }


async def ingest_trade_feed(pool: PgPool, bus: BusProducer, seen_hashes: set[str]) -> int:
    """Poll the public trades feed, write new fills, return count ingested."""
    async with httpx.AsyncClient(headers={"User-Agent": DEFAULT_USER_AGENT}) as client:
        data = await fetch_json(client, TRADES_URL, {"limit": 500})
        if not isinstance(data, list):
            return 0
        fills: list[dict] = []
        wallets_seen: dict[str, datetime] = {}
        now = datetime.now(tz=timezone.utc)
        for t in data:
            if not isinstance(t, dict):
                continue
            tx_hash = (t.get("transactionHash") or "").lower()
            if tx_hash and tx_hash in seen_hashes:
                continue
            f = trade_to_fill(t)
            if not f:
                continue
            if tx_hash:
                seen_hashes.add(tx_hash)
            fills.append(f)
            if f.get("taker_address"):
                wallets_seen[f["taker_address"]] = now
        if not fills:
            return 0
        async with pool.acquire() as conn:
            await insert_market_fills(conn, fills)
        for f in fills[:1500]:
            await bus.send(TOPICS["market_fills"], f, key=f["market_id"])
        if wallets_seen:
            async with pool.acquire() as conn:
                await upsert_wallets(
                    conn,
                    [
                        {"address": a, "last_seen": now, "chain": "polygon"}
                        for a in wallets_seen
                    ],
                )
        return len(fills)


async def refresh_smart_wallets(
    pool: PgPool, bus: BusProducer, top_wallets: list[str]
) -> int:
    """For each top wallet, pull value (PnL) and positions."""
    if not top_wallets:
        return 0
    n_done = 0
    async with httpx.AsyncClient(headers={"User-Agent": DEFAULT_USER_AGENT}) as client:
        for wallet in top_wallets[:SMART_WALLET_LIMIT]:
            try:
                value_data = await fetch_json(client, VALUE_URL, {"user": wallet})
                features = value_to_features(value_data, wallet)
                if features:
                    async with pool.acquire() as conn:
                        await upsert_wallets(conn, [features])
                    n_done += 1
            except Exception:
                pass
            try:
                positions = await fetch_json(client, POSITIONS_URL, {"user": wallet})
                if isinstance(positions, list) and positions:
                    sizes = [
                        _to_decimal(p.get("size")) or Decimal(0) for p in positions
                    ]
                    total_size = sum(sizes, Decimal(0))
                    async with pool.acquire() as conn:
                        await upsert_wallets(
                            conn,
                            [
                                {
                                    "address": wallet.lower(),
                                    "polymarket_volume": (total_size or None),
                                    "smart_money_score": min(1.0, float(total_size) / 1000.0)
                                    if total_size
                                    else None,
                                    "last_seen": datetime.now(tz=timezone.utc),
                                }
                            ],
                        )
            except Exception:
                pass
            await asyncio.sleep(0.05)
    return n_done


async def select_top_wallets(pool: PgPool, n: int) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT taker_address, COUNT(*) AS n_trades, SUM(notional) AS vol
            FROM market_fills
            WHERE taker_address IS NOT NULL AND time > NOW() - INTERVAL '7 days'
            GROUP BY taker_address
            ORDER BY n_trades DESC
            LIMIT $1
            """,
            n,
        )
    return [r["taker_address"] for r in rows]


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    pool = PgPool()
    await pool.connect()
    bus = BusProducer(settings.redpanda_brokers, client_id="positions-collector")
    await bus.start()
    log.info("starting_positions_collector", data=settings.polymarket_data_url)

    stop = asyncio.Event()
    def _sig(*_): stop.set()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    seen_hashes: set[str] = set()
    last_positions_refresh = 0.0
    try:
        while not stop.is_set():
            try:
                n = await ingest_trade_feed(pool, bus, seen_hashes)
                if n:
                    log.info("trade_feed_ingested", n=n, seen_total=len(seen_hashes))
            except Exception as e:
                log.error("trade_feed_error", err=str(e))
            now = asyncio.get_event_loop().time()
            if now - last_positions_refresh > POSITION_REFRESH_INTERVAL:
                last_positions_refresh = now
                try:
                    top = await select_top_wallets(pool, SMART_WALLET_LIMIT)
                    n_done = await refresh_smart_wallets(pool, bus, top)
                    log.info("smart_wallets_refreshed", wallets=len(top), n_done=n_done)
                except Exception as e:
                    log.error("smart_wallets_error", err=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=TRADE_FEED_INTERVAL)
            except asyncio.TimeoutError:
                pass
    finally:
        await bus.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
