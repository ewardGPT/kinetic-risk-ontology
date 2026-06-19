"""Market collector — Gamma metadata + CLOB WS ticks for curated geopol markets."""
from __future__ import annotations

import asyncio
import json
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import structlog
import websockets
from kro_common import (
    BusProducer,
    PgPool,
    TOPICS,
    get_settings,
    setup_logging,
    upsert_markets,
    insert_market_ticks,
)

from .curation import select_curated_markets, build_basket

log = structlog.get_logger("market-collector")


GAMMA_PAGE_SIZE = 500
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_PING_INTERVAL = 25
WS_PING_TIMEOUT = 20
RECONNECT_DELAY = 5


async def fetch_gamma_markets(
    client: httpx.AsyncClient, base_url: str
) -> list[dict]:
    """Pull all active, non-closed markets from Gamma API with pagination."""
    markets: list[dict] = []
    offset = 0
    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": GAMMA_PAGE_SIZE,
            "offset": offset,
        }
        try:
            r = await client.get(f"{base_url}/markets", params=params, timeout=30.0)
            r.raise_for_status()
        except Exception as e:
            log.warning("gamma_fetch_error", offset=offset, err=str(e))
            break
        batch = r.json()
        if not batch:
            break
        markets.extend(batch)
        if len(batch) < GAMMA_PAGE_SIZE:
            break
        offset += GAMMA_PAGE_SIZE
    return markets


def normalize_gamma_market(m: dict, basket: str) -> dict | None:
    """Convert a Gamma API market payload into our DB shape."""
    try:
        cond = m.get("conditionId") or m.get("condition_id")
        if not cond:
            return None
        tokens_raw = m.get("tokens") or m.get("clobTokenIds") or []
        tokens_norm: list[dict] = []
        if isinstance(tokens_raw, str):
            try:
                tokens_raw = json.loads(tokens_raw)
            except Exception:
                tokens_raw = []
        if isinstance(tokens_raw, list):
            for t in tokens_raw:
                if isinstance(t, str):
                    tokens_norm.append({"asset_id": t, "outcome": None})
                elif isinstance(t, dict):
                    tokens_norm.append(
                        {
                            "asset_id": t.get("token_id") or t.get("asset_id"),
                            "outcome": t.get("outcome"),
                        }
                    )
        outcomes_raw = m.get("outcomes")
        outcomes: list[str] = []
        if isinstance(outcomes_raw, str):
            try:
                outcomes = json.loads(outcomes_raw)
            except Exception:
                outcomes = []
        elif isinstance(outcomes_raw, list):
            outcomes = [str(o) for o in outcomes_raw]

        res_date = m.get("endDate") or m.get("end_date_iso") or m.get("resolutionDate")
        if isinstance(res_date, str):
            try:
                res_date = datetime.fromisoformat(res_date.replace("Z", "+00:00"))
            except Exception:
                res_date = None
        elif isinstance(res_date, (int, float)):
            res_date = datetime.fromtimestamp(res_date, tz=timezone.utc)

        def _dec(v) -> Decimal | None:
            if v is None or v == "":
                return None
            try:
                return Decimal(str(v))
            except Exception:
                return None

        tags = m.get("tags") or []
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                tags = [tags]

        return {
            "condition_id": cond,
            "market_id": m.get("id") or cond,
            "question": m.get("question") or m.get("title") or "(no question)",
            "slug": m.get("slug"),
            "category": m.get("category"),
            "tags": tags,
            "resolution_date": res_date,
            "closed": bool(m.get("closed", False)),
            "active": bool(m.get("active", True)),
            "liquidity": _dec(m.get("liquidity") or m.get("liquidityNum")),
            "open_interest": _dec(m.get("openInterest")),
            "volume_24h": _dec(m.get("volume24hr") or m.get("volumeNum")),
            "volume_total": _dec(m.get("volume")),
            "tokens": tokens_norm,
            "outcomes": outcomes,
            "metadata": {
                "description": m.get("description"),
                "image": m.get("image"),
                "endDate": m.get("endDate"),
            },
            "curated": False,
            "basket": basket,
        }
    except Exception as e:
        log.warning("normalize_failed", err=str(e), market_id=m.get("id"))
        return None


async def sync_gamma_once(
    client: httpx.AsyncClient, base_url: str, basket: str
) -> list[dict]:
    """Fetch all Gamma markets, normalize, upsert. Returns the normalized list."""
    raw = await fetch_gamma_markets(client, base_url)
    normalized = []
    for m in raw:
        n = normalize_gamma_market(m, basket)
        if n:
            normalized.append(n)
    log.info("gamma_fetched", count=len(normalized))
    return normalized


def _ts_to_dt(ts_raw) -> datetime:
    if isinstance(ts_raw, (int, float)):
        if ts_raw > 1e12:
            return datetime.fromtimestamp(ts_raw / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(ts_raw, tz=timezone.utc)
    if isinstance(ts_raw, str):
        try:
            if ts_raw.isdigit():
                v = int(ts_raw)
                if v > 1e12:
                    return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
                return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(tz=timezone.utc)


def _parse_price_change_entry(
    p: dict, condition_id_to_market: dict[str, dict]
) -> dict | None:
    try:
        asset_id = p.get("asset_id")
        if not asset_id:
            return None
        condition_id = p.get("market") or p.get("condition_id")
        if not condition_id:
            return None
        market_info = condition_id_to_market.get(condition_id, {})
        market_id = market_info.get("market_id") or condition_id
        price = Decimal(str(p.get("price", 0)))
        size_raw = p.get("size")
        size = Decimal(str(size_raw)) if size_raw is not None and size_raw != "" else None
        ts = _ts_to_dt(p.get("timestamp") or p.get("time"))
        outcomes = market_info.get("tokens") or []
        outcome = "YES"
        for t in outcomes:
            if isinstance(t, dict) and str(t.get("asset_id")) == str(asset_id):
                oc = t.get("outcome")
                if oc:
                    outcome = "YES" if str(oc).lower().startswith("y") else "NO"
                    break
        return {
            "time": ts,
            "market_id": str(market_id),
            "condition_id": str(condition_id),
            "asset_id": str(asset_id),
            "outcome": outcome,
            "price": price,
            "probability": price,
            "size": size,
            "hash": p.get("hash"),
            "source": "clob-ws",
        }
    except Exception as e:
        log.warning("ws_parse_entry_failed", err=str(e), entry=str(p)[:200])
        return None


def parse_clob_ws_message(
    payload, condition_id_to_market: dict[str, dict]
) -> list[dict]:
    """Parse a Polymarket CLOB WS payload (dict, list, or other) into MarketTick dicts."""
    out: list[dict] = []
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            cond = item.get("market") or item.get("condition_id")
            if not cond:
                continue
            for book_side in ("bids", "asks"):
                levels = item.get(book_side) or []
                if levels and isinstance(levels, list):
                    sample = levels[0]
                    if isinstance(sample, dict) and sample.get("price") is not None:
                        t = _parse_price_change_entry(
                            {
                                "asset_id": item.get("asset_id"),
                                "market": cond,
                                "price": sample.get("price"),
                                "size": sample.get("size"),
                                "timestamp": item.get("timestamp"),
                                "hash": item.get("hash"),
                            },
                            condition_id_to_market,
                        )
                        if t:
                            out.append(t)
                        break
        return out
    if not isinstance(payload, dict):
        return out
    cond = payload.get("market") or payload.get("condition_id")
    if cond and isinstance(payload.get("price_changes"), list):
        for p in payload["price_changes"]:
            if not isinstance(p, dict):
                continue
            p_with_market = dict(p)
            p_with_market.setdefault("market", cond)
            t = _parse_price_change_entry(p_with_market, condition_id_to_market)
            if t:
                out.append(t)
        return out
    if cond and "asset_id" in payload and "price" in payload:
        t = _parse_price_change_entry(payload, condition_id_to_market)
        if t:
            out.append(t)
    return out


def parse_ws_price_change(msg: dict, condition_id_to_market: dict[str, dict]) -> list[dict]:
    """Backward-compat shim — delegate to parse_clob_ws_message."""
    return parse_clob_ws_message(msg, condition_id_to_market)


def parse_ws_trade(msg: dict, condition_id_to_market: dict[str, dict]) -> dict | None:
    """Convert a Polymarket CLOB WS trade event into a MarketFill dict."""
    try:
        asset_id = msg.get("asset_id") or msg.get("asset")
        condition_id = msg.get("condition_id") or msg.get("market")
        if not asset_id or not condition_id:
            return None
        market_id = condition_id_to_market.get(condition_id, {}).get(
            "market_id", condition_id
        )
        ts_raw = msg.get("timestamp") or msg.get("time")
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        elif isinstance(ts_raw, str):
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        else:
            ts = datetime.now(tz=timezone.utc)
        price = Decimal(str(msg.get("price", 0)))
        size = Decimal(str(msg.get("size", 0)))
        return {
            "time": ts,
            "trade_id": msg.get("id") or msg.get("trade_id") or f"{msg.get('tx_hash','?')}:{asset_id}",
            "market_id": str(market_id),
            "condition_id": str(condition_id),
            "asset_id": str(asset_id),
            "outcome": "YES" if str(msg.get("outcome", "Yes")).lower().startswith("y") else "NO",
            "side": str(msg.get("side", "BUY")).upper(),
            "price": price,
            "size": size,
            "notional": price * size,
            "fee": None,
            "maker_address": (msg.get("maker_address") or msg.get("maker") or "").lower() or None,
            "taker_address": (msg.get("taker_address") or msg.get("taker") or "").lower() or None,
        }
    except Exception:
        return None


async def run_ws_loop(
    pool: PgPool,
    bus: BusProducer,
    get_state_callable,
) -> None:
    """Run the CLOB WS subscription loop. Reconnects when the asset set changes."""
    last_signature: tuple = ()
    while True:
        try:
            asset_ids, cond_map = await get_state_callable()
            signature = tuple(sorted(asset_ids))
            if not asset_ids:
                await asyncio.sleep(5)
                continue
            if signature == last_signature:
                await run_ws_once(asset_ids, cond_map, pool, bus, refresh_budget=120)
            else:
                last_signature = signature
                log.info("ws_subscribing", n_assets=len(asset_ids), n_markets=len(cond_map))
                await run_ws_once(asset_ids, cond_map, pool, bus, refresh_budget=600)
        except Exception as e:
            log.error("ws_loop_error", err=str(e))
            await asyncio.sleep(RECONNECT_DELAY)


async def run_ws_once(
    asset_ids: list[str],
    condition_id_to_market: dict[str, dict],
    pool: PgPool,
    bus: BusProducer,
    refresh_budget: int = 600,
) -> None:
    sub_payload = {
        "type": "subscribe",
        "channel": "market",
        "assets_ids": asset_ids,
    }
    log.info("ws_connecting", url=WS_URL, n_assets=len(asset_ids))
    received = 0
    async with websockets.connect(
        WS_URL,
        ping_interval=WS_PING_INTERVAL,
        ping_timeout=WS_PING_TIMEOUT,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    ) as ws:
        await ws.send(json.dumps(sub_payload))
        log.info("ws_subscribed", n_assets=len(asset_ids))
        deadline = asyncio.get_event_loop().time() + refresh_budget
        n_msgs = 0
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.info("ws_refresh_budget_exhausted", received=received, msgs=n_msgs)
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 30))
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed as e:
                log.warning("ws_closed", code=e.code, received=received, msgs=n_msgs)
                return
            n_msgs += 1
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            ticks = parse_clob_ws_message(payload, condition_id_to_market)
            if ticks:
                async with pool.acquire() as conn:
                    await insert_market_ticks(conn, ticks)
                for t in ticks:
                    await bus.send(TOPICS["market_ticks"], t, key=t["market_id"])
                received += len(ticks)
                if received < 5 or received % 100 < len(ticks):
                    log.info("ws_progress", received=received, n_msgs=n_msgs, latest=ticks[-1].get("price"))
            elif n_msgs <= 3 or n_msgs % 100 == 0:
                log.info("ws_no_ticks", n_msgs=n_msgs, payload_type=type(payload).__name__)


async def curation_loop(
    pool: PgPool, basket_name: str, refresh_seconds: int = 600
) -> None:
    """Periodically rebuild the curated basket from the live markets table."""
    while True:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM markets WHERE active = TRUE AND closed = FALSE"
                )
            markets = [dict(r) for r in rows]
            curated = select_curated_markets(markets, basket_name=basket_name, limit=25)
            build_basket(curated)
            async with pool.acquire() as conn:
                await conn.execute("UPDATE markets SET curated = FALSE, basket = NULL")
                for m in curated:
                    await conn.execute(
                        "UPDATE markets SET curated = TRUE, basket = $1 WHERE condition_id = $2",
                        basket_name,
                        m["condition_id"],
                    )
            log.info("curation_done", n_curated=len(curated))
        except Exception as e:
            log.error("curation_error", err=str(e))
        await asyncio.sleep(refresh_seconds)


async def gamma_sync_loop(
    pool: PgPool, gamma_url: str, basket: str, refresh_seconds: int = 1800
) -> None:
    """Periodically pull Gamma metadata to keep markets table fresh."""
    while True:
        try:
            async with httpx.AsyncClient() as client:
                markets = await sync_gamma_once(client, gamma_url, basket)
            if markets:
                async with pool.acquire() as conn:
                    await upsert_markets(conn, markets)
                log.info("gamma_synced", n=len(markets))
        except Exception as e:
            log.error("gamma_sync_error", err=str(e))
        await asyncio.sleep(refresh_seconds)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    pool = PgPool()
    await pool.connect()

    bus = BusProducer(settings.redpanda_brokers, client_id="market-collector")
    await bus.start()

    log.info("starting_market_collector", gamma=settings.polymarket_gamma_url)

    async with httpx.AsyncClient() as client:
        initial = await sync_gamma_once(client, settings.polymarket_gamma_url, settings.market_basket)
        if initial:
            async with pool.acquire() as conn:
                await upsert_markets(conn, initial)

    stop = asyncio.Event()
    def _sig(*_): stop.set()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    state_lock = asyncio.Lock()
    state: dict[str, Any] = {"asset_ids": [], "cond_map": {}}

    async def get_state() -> tuple[list[str], dict[str, dict]]:
        async with state_lock:
            return list(state["asset_ids"]), dict(state["cond_map"])

    async def refresh_curated_for_ws() -> None:
        while not stop.is_set():
            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT condition_id, market_id, tokens FROM markets WHERE curated = TRUE AND active = TRUE"
                    )
                new_cond_map: dict[str, dict] = {}
                ids: list[str] = []
                for r in rows:
                    new_cond_map[r["condition_id"]] = {
                        "market_id": r["market_id"],
                        "tokens": r["tokens"],
                    }
                    tokens = r["tokens"]
                    if isinstance(tokens, str):
                        try:
                            tokens = json.loads(tokens)
                        except Exception:
                            tokens = []
                    for t in (tokens or []):
                        if isinstance(t, dict) and t.get("asset_id"):
                            ids.append(t["asset_id"])
                new_asset_ids = list(dict.fromkeys(ids))
                async with state_lock:
                    state["asset_ids"] = new_asset_ids
                    state["cond_map"] = new_cond_map
                log.info("ws_target_refreshed", n_markets=len(new_cond_map), n_assets=len(new_asset_ids))
            except Exception as e:
                log.error("ws_target_refresh_error", err=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=120)
            except asyncio.TimeoutError:
                pass

    tasks = [
        asyncio.create_task(gamma_sync_loop(pool, settings.polymarket_gamma_url, settings.market_basket)),
        asyncio.create_task(curation_loop(pool, settings.market_basket)),
        asyncio.create_task(refresh_curated_for_ws()),
        asyncio.create_task(run_ws_loop(pool, bus, get_state)),
    ]

    try:
        await stop.wait()
    finally:
        log.info("shutting_down")
        for t in tasks:
            t.cancel()
        await bus.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
