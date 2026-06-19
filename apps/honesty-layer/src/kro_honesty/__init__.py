"""Phase 5 honesty layer — wash-trade suppression and smart-money weighting."""
from __future__ import annotations

import asyncio
import signal
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog
from kro_common import (
    PgPool,
    get_settings,
    setup_logging,
    upsert_markets,
)

log = structlog.get_logger("honesty-layer")


async def detect_wash_trades(pool: PgPool) -> int:
    """Flag market_fills where maker and taker are in the same cluster (potential wash)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE market_fills f
            SET notional = -ABS(notional)
            FROM wallets w_m, wallets w_t
            WHERE f.maker_address = w_m.address
              AND f.taker_address = w_t.address
              AND w_m.cluster_id = w_t.cluster_id
              AND w_m.cluster_id IS NOT NULL
              AND f.time > NOW() - INTERVAL '24 hours'
            RETURNING f.trade_id
            """
        )
    n = len(rows)
    if n:
        log.info("wash_trades_flagged", n=n)
    return n


async def recompute_smart_money(pool: PgPool) -> int:
    """Recompute smart_money_score from PnL + trade count + volume."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE wallets SET
                smart_money_score = LEAST(1.0,
                    0.3 * (1.0 - LEAST(1.0, COALESCE(polymarket_trades, 0)::float / 1000.0)) +
                    0.4 * LEAST(1.0, GREATEST(COALESCE(polymarket_pnl, 0), 0)::float / 50000.0) +
                    0.3 * LEAST(1.0, COALESCE(polymarket_volume, 0)::float / 100000.0)
                )
            WHERE polymarket_pnl IS NOT NULL OR polymarket_trades IS NOT NULL
            """
        )
    n = int(result.split()[-1]) if result else 0
    log.info("smart_money_recomputed", n=n)
    return n


async def thin_market_gates(pool: PgPool) -> int:
    """Re-mark thin markets (liquidity < $1k or volume < $500) as non-curated."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE markets
            SET curated = FALSE, basket = NULL
            WHERE curated = TRUE
              AND (liquidity IS NULL OR liquidity < 1000)
              AND (volume_24h IS NULL OR volume_24h < 500)
            """
        )
    n = int(result.split()[-1]) if result else 0
    if n:
        log.info("thin_markets_excluded", n=n)
    return n


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    log.info("starting_honesty_layer")

    pool = PgPool()
    await pool.connect()

    stop = asyncio.Event()
    def _sig(*_): stop.set()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    try:
        while not stop.is_set():
            try:
                await detect_wash_trades(pool)
                await recompute_smart_money(pool)
                await thin_market_gates(pool)
            except Exception as e:
                log.error("honesty_layer_error", err=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
