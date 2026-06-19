"""KRO Postgres connection pool + writers."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import asyncpg
import orjson

from .config import get_settings


class PgPool:
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or get_settings().pg_dsn_async
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
            init=_init_codecs,
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("PgPool not connected — call connect() first")
        return self._pool

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as conn:
            yield conn


async def _init_codecs(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )


async def upsert_markets(conn: asyncpg.Connection, markets: list[dict[str, Any]]) -> int:
    if not markets:
        return 0
    sql = """
    INSERT INTO markets (
      condition_id, market_id, question, slug, category, tags,
      resolution_date, closed, active, liquidity, open_interest,
      volume_24h, volume_total, tokens, outcomes, metadata, curated, basket, updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW())
    ON CONFLICT (condition_id) DO UPDATE SET
      market_id      = EXCLUDED.market_id,
      question       = EXCLUDED.question,
      slug           = EXCLUDED.slug,
      category       = EXCLUDED.category,
      tags           = EXCLUDED.tags,
      resolution_date= EXCLUDED.resolution_date,
      closed         = EXCLUDED.closed,
      active         = EXCLUDED.active,
      liquidity      = EXCLUDED.liquidity,
      open_interest  = EXCLUDED.open_interest,
      volume_24h     = EXCLUDED.volume_24h,
      volume_total   = EXCLUDED.volume_total,
      tokens         = EXCLUDED.tokens,
      outcomes       = EXCLUDED.outcomes,
      metadata       = EXCLUDED.metadata,
      curated        = markets.curated,
      basket         = markets.basket,
      updated_at     = NOW()
    """
    count = 0
    async with conn.transaction():
        for m in markets:
            await conn.execute(
                sql,
                m["condition_id"],
                m.get("market_id"),
                m["question"],
                m.get("slug"),
                m.get("category"),
                m.get("tags", []),
                m.get("resolution_date"),
                m.get("closed", False),
                m.get("active", True),
                m.get("liquidity"),
                m.get("open_interest"),
                m.get("volume_24h"),
                m.get("volume_total"),
                m.get("tokens", []),
                m.get("outcomes", []),
                m.get("metadata", {}),
                m.get("curated", False),
                m.get("basket"),
            )
            count += 1
    return count


async def insert_market_ticks(conn: asyncpg.Connection, ticks: list[dict[str, Any]]) -> int:
    if not ticks:
        return 0
    sql = """
    INSERT INTO market_ticks (time, market_id, condition_id, asset_id, outcome, price, probability, size, hash, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT DO NOTHING
    """
    count = 0
    async with conn.transaction():
        for t in ticks:
            await conn.execute(
                sql,
                t["time"],
                t["market_id"],
                t["condition_id"],
                t["asset_id"],
                t["outcome"],
                t["price"],
                t["probability"],
                t.get("size"),
                t.get("hash"),
                t.get("source", "clob-ws"),
            )
            count += 1
    return count


async def insert_market_fills(conn: asyncpg.Connection, fills: list[dict[str, Any]]) -> int:
    if not fills:
        return 0
    sql = """
    INSERT INTO market_fills (time, trade_id, market_id, condition_id, asset_id, outcome, side, price, size, notional, fee, maker_address, taker_address)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT DO NOTHING
    """
    count = 0
    async with conn.transaction():
        for f in fills:
            await conn.execute(
                sql,
                f["time"],
                f["trade_id"],
                f["market_id"],
                f.get("condition_id"),
                f.get("asset_id"),
                f["outcome"],
                f["side"],
                f["price"],
                f["size"],
                f.get("notional"),
                f.get("fee"),
                f.get("maker_address"),
                f.get("taker_address"),
            )
            count += 1
    return count


async def insert_chain_transfers(
    conn: asyncpg.Connection, transfers: list[dict[str, Any]]
) -> int:
    if not transfers:
        return 0
    sql = """
    INSERT INTO chain_transfers (time, chain, block_number, tx_hash, log_index, token_address, token_symbol, from_address, to_address, amount_raw, amount_human, decimals)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT DO NOTHING
    """
    count = 0
    async with conn.transaction():
        for t in transfers:
            await conn.execute(
                sql,
                t["time"],
                t.get("chain", "polygon"),
                t["block_number"],
                t["tx_hash"],
                t["log_index"],
                t["token_address"],
                t.get("token_symbol"),
                t["from_address"],
                t["to_address"],
                t["amount_raw"],
                t["amount_human"],
                t.get("decimals"),
            )
            count += 1
    return count


async def upsert_wallets(conn: asyncpg.Connection, wallets: list[dict[str, Any]]) -> int:
    if not wallets:
        return 0
    sql = """
    INSERT INTO wallets (address, chain, first_seen, last_seen, polymarket_pnl, polymarket_volume, polymarket_trades, smart_money_score, cluster_id, entity_id, risk_flags, features, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
    ON CONFLICT (address) DO UPDATE SET
      last_seen         = COALESCE(EXCLUDED.last_seen, wallets.last_seen),
      first_seen        = COALESCE(wallets.first_seen, EXCLUDED.first_seen),
      polymarket_pnl    = COALESCE(EXCLUDED.polymarket_pnl, wallets.polymarket_pnl),
      polymarket_volume = COALESCE(EXCLUDED.polymarket_volume, wallets.polymarket_volume),
      polymarket_trades = COALESCE(EXCLUDED.polymarket_trades, wallets.polymarket_trades),
      smart_money_score = COALESCE(EXCLUDED.smart_money_score, wallets.smart_money_score),
      cluster_id        = COALESCE(wallets.cluster_id, EXCLUDED.cluster_id),
      entity_id         = COALESCE(wallets.entity_id, EXCLUDED.entity_id),
      risk_flags        = COALESCE(wallets.risk_flags, EXCLUDED.risk_flags),
      features          = COALESCE(wallets.features, EXCLUDED.features),
      updated_at        = NOW()
    """
    count = 0
    async with conn.transaction():
        for w in wallets:
            await conn.execute(
                sql,
                w["address"],
                w.get("chain", "polygon"),
                w.get("first_seen"),
                w.get("last_seen"),
                w.get("polymarket_pnl"),
                w.get("polymarket_volume"),
                w.get("polymarket_trades"),
                w.get("smart_money_score"),
                w.get("cluster_id"),
                w.get("entity_id"),
                w.get("risk_flags", []),
                w.get("features", {}),
            )
            count += 1
    return count


async def insert_alert(conn: asyncpg.Connection, alert: dict[str, Any]) -> str:
    sql = """
    INSERT INTO kinetic_alerts (id, time, market_id, cluster_id, entity_id, market_signal, flow_signal, entity_risk, lead_lag, composite_score, state)
    VALUES (COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    RETURNING id
    """
    row = await conn.fetchrow(
        sql,
        alert.get("id"),
        alert["time"],
        alert["market_id"],
        alert.get("cluster_id"),
        alert.get("entity_id"),
        alert["market_signal"],
        alert.get("flow_signal"),
        alert.get("entity_risk"),
        alert.get("lead_lag"),
        alert["composite_score"],
        alert.get("state", "open"),
    )
    return str(row["id"])
