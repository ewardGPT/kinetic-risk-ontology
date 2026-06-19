"""Ontology loader — pushes PG state into Neo4j as a typed graph."""

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
from datetime import datetime, timezone
from typing import Any

import structlog
from kro_common import PgPool, get_settings, setup_logging
from neo4j import AsyncGraphDatabase, AsyncSession

log = structlog.get_logger("ontology-loader")


SCHEMA_CYPHER = [
    "CREATE CONSTRAINT market_condition_id IF NOT EXISTS FOR (m:Market) REQUIRE m.condition_id IS UNIQUE",
    "CREATE CONSTRAINT wallet_address IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE",
    "CREATE CONSTRAINT cluster_id IF NOT EXISTS FOR (c:Cluster) REQUIRE c.cluster_id IS UNIQUE",
    "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE",
    "CREATE CONSTRAINT flow_agg_id IF NOT EXISTS FOR (f:FlowAggregate) REQUIRE f.flow_id IS UNIQUE",
    "CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (a:KineticRiskAlert) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT transaction_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE",
    "CREATE INDEX wallet_cluster_idx IF NOT EXISTS FOR (w:Wallet) ON (w.cluster_id)",
    "CREATE INDEX wallet_entity_idx IF NOT EXISTS FOR (w:Wallet) ON (w.entity_id)",
    "CREATE INDEX market_active_idx IF NOT EXISTS FOR (m:Market) ON (m.active)",
    "CREATE INDEX market_curated_idx IF NOT EXISTS FOR (m:Market) ON (m.curated)",
    "CREATE INDEX alert_score_idx IF NOT EXISTS FOR (a:KineticRiskAlert) ON (a.composite_score)",
    "CREATE INDEX alert_time_idx IF NOT EXISTS FOR (a:KineticRiskAlert) ON (a.time)",
    "CREATE INDEX alert_state_idx IF NOT EXISTS FOR (a:KineticRiskAlert) ON (a.state)",
    "CREATE INDEX tx_from_time_idx IF NOT EXISTS FOR (t:Transaction) ON (t.from_address, t.time)",
    "CREATE INDEX tx_to_time_idx IF NOT EXISTS FOR (t:Transaction) ON (t.to_address, t.time)",
]


def _row_to_market(r) -> dict:
    return {
        "condition_id": r["condition_id"],
        "market_id": r["market_id"] or r["condition_id"],
        "question": r["question"],
        "slug": r["slug"],
        "category": r["category"],
        "active": r["active"],
        "curated": r["curated"],
        "liquidity": float(r["liquidity"]) if r["liquidity"] is not None else None,
        "open_interest": float(r["open_interest"]) if r["open_interest"] is not None else None,
        "volume_24h": float(r["volume_24h"]) if r["volume_24h"] is not None else None,
        "volume_total": float(r["volume_total"]) if r["volume_total"] is not None else None,
        "resolution_date": r["resolution_date"].isoformat() if r["resolution_date"] else None,
        "basket": r["basket"],
    }


def _row_to_wallet(r) -> dict:
    return {
        "address": r["address"].lower(),
        "chain": r["chain"] or "polygon",
        "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
        "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
        "polymarket_pnl": float(r["polymarket_pnl"]) if r["polymarket_pnl"] is not None else None,
        "polymarket_volume": float(r["polymarket_volume"])
        if r["polymarket_volume"] is not None
        else None,
        "smart_money_score": float(r["smart_money_score"])
        if r["smart_money_score"] is not None
        else None,
        "cluster_id": r["cluster_id"],
        "entity_id": r["entity_id"],
        "risk_flags": r["risk_flags"] or [],
    }


def _row_to_fill(r) -> dict:
    return {
        "time": r["time"].isoformat() if r["time"] else None,
        "trade_id": r["trade_id"],
        "market_id": r["market_id"],
        "condition_id": r["condition_id"] or r["market_id"],
        "asset_id": r["asset_id"],
        "outcome": r["outcome"],
        "side": r["side"],
        "price": float(r["price"]) if r["price"] is not None else None,
        "size": float(r["size"]) if r["size"] is not None else None,
        "notional": float(r["notional"]) if r["notional"] is not None else None,
        "maker": (r["maker_address"] or "").lower() or None,
        "taker": (r["taker_address"] or "").lower() or None,
    }


def _row_to_tx(r) -> dict:
    return {
        "tx_id": f"{r['chain'] or 'polygon'}:{r['tx_hash']}:{r['log_index']}",
        "chain": r["chain"] or "polygon",
        "tx_hash": r["tx_hash"],
        "log_index": r["log_index"],
        "block_number": r["block_number"],
        "time": r["time"].isoformat() if r["time"] else None,
        "token_address": r["token_address"],
        "token_symbol": r["token_symbol"],
        "from_address": (r["from_address"] or "").lower(),
        "to_address": (r["to_address"] or "").lower(),
        "amount_human": float(r["amount_human"]) if r["amount_human"] is not None else None,
        "amount_raw": str(r["amount_raw"]) if r["amount_raw"] is not None else None,
    }


def _row_to_entity(r) -> dict:
    return {
        "entity_id": r["entity_id"],
        "name": r["name"],
        "type": r["type"],
        "labels": r["labels"] or [],
        "risk_level": r["risk_level"],
        "source": r["source"],
    }


def _row_to_cluster(r) -> dict:
    return {
        "cluster_id": r["cluster_id"],
        "size": r["size"],
        "method": r["method"],
        "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
        "canonical_address": r["canonical_address"],
    }


def _row_to_alert(r) -> dict:
    return {
        "id": str(r["id"]),
        "time": r["time"].isoformat() if r["time"] else None,
        "market_id": r["market_id"],
        "cluster_id": r["cluster_id"],
        "entity_id": r["entity_id"],
        "composite_score": float(r["composite_score"]),
        "state": r["state"],
        "market_signal": r["market_signal"],
        "flow_signal": r["flow_signal"],
        "entity_risk": r["entity_risk"],
        "lead_lag": r["lead_lag"],
    }


async def apply_schema(session: AsyncSession) -> None:
    for stmt in SCHEMA_CYPHER:
        try:
            await session.run(stmt)
        except Exception as e:
            log.warning("schema_stmt_warn", stmt=stmt[:60], err=str(e))


async def upsert_markets(session: AsyncSession, markets: list[dict]) -> int:
    if not markets:
        return 0
    cypher = """
    UNWIND $markets AS m
    MERGE (market:Market {condition_id: m.condition_id})
    SET market.market_id = m.market_id,
        market.question = m.question,
        market.slug = m.slug,
        market.category = m.category,
        market.active = m.active,
        market.curated = m.curated,
        market.liquidity = m.liquidity,
        market.open_interest = m.open_interest,
        market.volume_24h = m.volume_24h,
        market.volume_total = m.volume_total,
        market.resolution_date = m.resolution_date,
        market.basket = m.basket,
        market.updated_at = datetime()
    RETURN count(market) AS n
    """
    result = await session.run(cypher, markets=markets)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def upsert_wallets(session: AsyncSession, wallets: list[dict]) -> int:
    if not wallets:
        return 0
    has_cluster = any(w.get("cluster_id") for w in wallets)
    has_entity = any(w.get("entity_id") for w in wallets)
    cypher = """
    UNWIND $wallets AS w
    MERGE (wallet:Wallet {address: w.address})
    SET wallet.chain = w.chain,
        wallet.first_seen = CASE WHEN w.first_seen IS NOT NULL THEN datetime(w.first_seen) ELSE wallet.first_seen END,
        wallet.last_seen = CASE WHEN w.last_seen IS NOT NULL THEN datetime(w.last_seen) ELSE wallet.last_seen END,
        wallet.polymarket_pnl = w.polymarket_pnl,
        wallet.polymarket_volume = w.polymarket_volume,
        wallet.smart_money_score = w.smart_money_score,
        wallet.updated_at = datetime()
    """
    if has_cluster:
        cypher += """
        WITH wallet, w
        WHERE w.cluster_id IS NOT NULL
        MERGE (cluster:Cluster {cluster_id: w.cluster_id})
        MERGE (wallet)-[r:MEMBER_OF]->(cluster)
        SET r.confidence = coalesce(wallet.smart_money_score, 0.5),
            r.updated_at = datetime()
        """
    if has_entity:
        cypher += """
        WITH wallet, w
        WHERE w.entity_id IS NOT NULL
        MERGE (entity:Entity {entity_id: w.entity_id})
        MERGE (wallet)-[r2:RESOLVES_TO]->(entity)
        SET r2.updated_at = datetime()
        """
    cypher += "RETURN count(wallet) AS n"
    result = await session.run(cypher, wallets=wallets)
    rec = await result.single()
    return int(rec["n"]) if rec else len(wallets)


async def upsert_fills(session: AsyncSession, fills: list[dict]) -> int:
    if not fills:
        return 0
    cypher = """
    UNWIND $fills AS f
    MERGE (fill:Trade {trade_id: f.trade_id})
    SET fill.time = datetime(f.time),
        fill.side = f.side,
        fill.price = f.price,
        fill.size = f.size,
        fill.notional = f.notional,
        fill.outcome = f.outcome
    WITH fill, f
    MERGE (market:Market {condition_id: f.condition_id})
    ON CREATE SET market.discovered_via_trade = true,
                  market.question = coalesce(f.question, 'unknown'),
                  market.market_id = f.condition_id,
                  market.active = true
    MERGE (fill)-[:ON_MARKET]->(market)
    WITH fill, f
    WHERE f.taker IS NOT NULL
    MERGE (taker:Wallet {address: f.taker})
    MERGE (taker)-[:PLACED]->(fill)
    WITH fill, f
    WHERE f.maker IS NOT NULL
    MERGE (maker:Wallet {address: f.maker})
    MERGE (maker)-[:PLACED]->(fill)
    RETURN count(fill) AS n
    """
    result = await session.run(cypher, fills=fills)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def upsert_transactions(session: AsyncSession, txs: list[dict]) -> int:
    if not txs:
        return 0
    cypher = """
    UNWIND $txs AS t
    MERGE (tx:Transaction {tx_id: t.tx_id})
    SET tx.chain = t.chain,
        tx.tx_hash = t.tx_hash,
        tx.log_index = t.log_index,
        tx.block_number = t.block_number,
        tx.time = datetime(t.time),
        tx.token_address = t.token_address,
        tx.token_symbol = t.token_symbol,
        tx.amount_human = t.amount_human,
        tx.amount_raw = t.amount_raw,
        tx.from_address = t.from_address,
        tx.to_address = t.to_address
    WITH tx, t
    MERGE (from:Wallet {address: t.from_address})
    MERGE (to:Wallet {address: t.to_address})
    MERGE (from)-[:SENT {time: datetime(t.time), amount: t.amount_human, token: t.token_symbol}]->(tx)
    MERGE (tx)-[:RECEIVED_BY {time: datetime(t.time), amount: t.amount_human, token: t.token_symbol}]->(to)
    RETURN count(tx) AS n
    """
    result = await session.run(cypher, txs=txs)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def upsert_entities(session: AsyncSession, entities: list[dict]) -> int:
    if not entities:
        return 0
    cypher = """
    UNWIND $entities AS e
    MERGE (entity:Entity {entity_id: e.entity_id})
    SET entity.name = e.name,
        entity.type = e.type,
        entity.labels = e.labels,
        entity.risk_level = e.risk_level,
        entity.source = e.source,
        entity.updated_at = datetime()
    RETURN count(entity) AS n
    """
    result = await session.run(cypher, entities=entities)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def upsert_clusters(session: AsyncSession, clusters: list[dict]) -> int:
    if not clusters:
        return 0
    cypher = """
    UNWIND $clusters AS c
    MERGE (cluster:Cluster {cluster_id: c.cluster_id})
    SET cluster.size = c.size,
        cluster.method = c.method,
        cluster.confidence = c.confidence,
        cluster.canonical_address = c.canonical_address,
        cluster.updated_at = datetime()
    RETURN count(cluster) AS n
    """
    result = await session.run(cypher, clusters=clusters)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def upsert_alerts(session: AsyncSession, alerts: list[dict]) -> int:
    if not alerts:
        return 0
    serialized = []
    for a in alerts:
        serialized.append(
            {
                **a,
                "market_signal": json.dumps(a.get("market_signal") or {}),
                "flow_signal": json.dumps(a.get("flow_signal") or {}),
                "entity_risk": json.dumps(a.get("entity_risk") or {}),
                "lead_lag": json.dumps(a.get("lead_lag") or {}),
            }
        )
    cypher = """
    UNWIND $alerts AS a
    MERGE (alert:KineticRiskAlert {id: a.id})
    SET alert.time = datetime(a.time),
        alert.composite_score = a.composite_score,
        alert.state = a.state,
        alert.market_signal = a.market_signal,
        alert.flow_signal = a.flow_signal,
        alert.entity_risk = a.entity_risk,
        alert.lead_lag = a.lead_lag,
        alert.updated_at = datetime()
    WITH alert, a
    WHERE a.market_id IS NOT NULL
    MATCH (market:Market {condition_id: a.market_id})
    MERGE (alert)-[:FIRES_ON]->(market)
    WITH alert, a
    WHERE a.cluster_id IS NOT NULL
    MATCH (cluster:Cluster {cluster_id: a.cluster_id})
    MERGE (alert)-[:IMPLICATES]->(cluster)
    WITH alert, a
    WHERE a.entity_id IS NOT NULL
    MATCH (entity:Entity {entity_id: a.entity_id})
    MERGE (alert)-[:EVIDENCED_BY]->(entity)
    RETURN count(alert) AS n
    """
    result = await session.run(cypher, alerts=serialized)
    rec = await result.single()
    return int(rec["n"]) if rec else 0


async def full_load(
    pg: PgPool,
    session: AsyncSession,
    lookback_hours: int = 24,
    tx_lookback_hours: int = 1,
    tx_batch: int = 200,
    tx_max: int = 10000,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    async with pg.acquire() as conn:
        markets = [dict(r) for r in await conn.fetch("SELECT * FROM markets WHERE active = TRUE")]
        counts["markets"] = await upsert_markets(session, [_row_to_market(r) for r in markets])
        log.info("ontology_chunk", markets=counts["markets"])

        wallets = [dict(r) for r in await conn.fetch("SELECT * FROM wallets")]
        counts["wallets"] = await upsert_wallets(session, [_row_to_wallet(r) for r in wallets])
        log.info("ontology_chunk", wallets=counts["wallets"])

        entities = [dict(r) for r in await conn.fetch("SELECT * FROM entities")]
        counts["entities"] = await upsert_entities(session, [_row_to_entity(r) for r in entities])

        clusters = [dict(r) for r in await conn.fetch("SELECT * FROM clusters")]
        counts["clusters"] = await upsert_clusters(session, [_row_to_cluster(r) for r in clusters])

        fills = [
            dict(r)
            for r in await conn.fetch(
                f"SELECT * FROM market_fills WHERE time > NOW() - INTERVAL '{lookback_hours} hours'"
            )
        ]
        counts["fills"] = await upsert_fills(session, [_row_to_fill(r) for r in fills])
        log.info("ontology_chunk", fills=counts["fills"])

        tx_total = 0
        tx_offset = 0
        while tx_offset < tx_max:
            batch = [
                dict(r)
                for r in await conn.fetch(
                    f"SELECT * FROM chain_transfers WHERE time > NOW() - INTERVAL '{tx_lookback_hours} hours' ORDER BY time DESC OFFSET {tx_offset} LIMIT {tx_batch}"
                )
            ]
            if not batch:
                break
            n = await upsert_transactions(session, [_row_to_tx(r) for r in batch])
            tx_total += n
            tx_offset += tx_batch
            if n == 0:
                break
        counts["txs"] = tx_total
        log.info("ontology_chunk", txs=tx_total)

        alerts = [dict(r) for r in await conn.fetch("SELECT * FROM kinetic_alerts")]
        counts["alerts"] = await upsert_alerts(session, [_row_to_alert(r) for r in alerts])
        log.info("ontology_chunk", alerts=counts["alerts"])
    return counts


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    log.info("starting_ontology_loader", neo4j=settings.neo4j_uri)

    pg = PgPool()
    await pg.connect()

    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    async with driver.session() as session:
        await apply_schema(session)
    log.info("schema_applied")

    stop = asyncio.Event()

    def _sig(*_):
        stop.set()

    for s in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            asyncio.get_running_loop().add_signal_handler(s, _sig)

    refresh_seconds = int(getattr(settings, "ontology_refresh_seconds", 300))
    first_run = True
    while not stop.is_set():
        try:
            async with driver.session() as session:
                counts = await full_load(
                    pg,
                    session,
                    tx_lookback_hours=int(getattr(settings, "ontology_tx_lookback_hours", 1)),
                    tx_max=int(getattr(settings, "ontology_tx_max", 5000)),
                )
            log.info("ontology_loaded", **counts, initial=first_run)
            first_run = False
        except Exception as e:
            log.error("ontology_load_error", err=str(e))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=refresh_seconds)
    await driver.close()
    await pg.close()


if __name__ == "__main__":
    asyncio.run(main())
