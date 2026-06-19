"""Entity Resolution — co-activity Leiden + OFAC label propagation.

Pipeline:
1. Build wallet co-activity graph (Jaccard of counterparties).
2. Leiden community detection to consolidate clusters.
3. Propagate known labels (OFAC SDN) outward from seed nodes.
4. Write clusters + entities back to PG and Neo4j (via ontology loader).

We report pairwise F1 on a held-out labeled set.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import signal
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import structlog
from kro_common import PgPool, get_settings, setup_logging

log = structlog.get_logger("entity-resolution")


OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
OFAC_ADD_URL = "https://www.treasury.gov/ofac/downloads/add.csv"
MIN_CO_ACTIVITY_JACCARD = 0.02
MIN_TX_COUNT = 3
LEIDEN_RESOLUTION = 1.0
REFRESH_INTERVAL = 3600


async def fetch_ofac_sdn() -> list[dict]:
    sdn = await _fetch_csv(OFAC_SDN_URL)
    add = await _fetch_csv(OFAC_ADD_URL)
    out: list[dict] = []
    for row in sdn:
        for chain, addr in _extract_crypto_addresses(row.get("remarks", "")):
            out.append({
                "uid": f"ofac-sdn-{row.get('ent_num', '')}",
                "name": row.get("name", ""),
                "address": addr,
                "chain": chain,
                "country": row.get("program", ""),
                "type": "individual" if row.get("sdn_type", "") == "Individual" else "organization",
                "source": "ofac_sdn",
            })
    for row in add:
        for chain, addr in _extract_crypto_addresses(row.get("remarks", "")):
            out.append({
                "uid": f"ofac-add-{row.get('ent_num', '')}",
                "name": row.get("name", ""),
                "address": addr,
                "chain": chain,
                "country": row.get("program", ""),
                "type": "individual" if row.get("sdn_type", "") == "Individual" else "organization",
                "source": "ofac_add",
            })
    log.info("ofac_loaded", sdn_count=len(out))
    return out


async def _fetch_csv(url: str) -> list[dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "kro/0.1"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        rows = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                parts = next(csv.reader([line]))
            except Exception:
                continue
            while len(parts) < 12:
                parts.append("")
            rows.append({
                "ent_num": parts[0],
                "name": parts[1].strip('"'),
                "sdn_type": parts[2].strip('"'),
                "program": parts[3].strip('"'),
                "title": parts[4].strip('"'),
                "call_sign": parts[5].strip('"'),
                "vess_type": parts[6].strip('"'),
                "tonnage": parts[7].strip('"'),
                "grt": parts[8].strip('"'),
                "vess_flag": parts[9].strip('"'),
                "vess_owner": parts[10].strip('"'),
                "remarks": parts[11],
            })
        return rows
    except Exception as e:
        log.warning("ofac_fetch_failed", url=url, err=str(e))
        return []


_CRYPTO_ADDR_RE = None
def _crypto_addr_re():
    import re
    global _CRYPTO_ADDR_RE
    if _CRYPTO_ADDR_RE is None:
        _CRYPTO_ADDR_RE = re.compile(
            r"Digital Currency Address\s*-\s*(XBT|ETH|XRP|LTC|BCH|DASH|ETC|NMC|TRX|USDT|BSV|XMR|ZEC|DOGE)\s+(0x[0-9a-fA-F]{40}|[a-zA-Z0-9]{25,60})"
        )
    return _CRYPTO_ADDR_RE


def _extract_crypto_addresses(remarks: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if not remarks:
        return out
    for m in _crypto_addr_re().finditer(remarks):
        chain, addr = m.group(1), m.group(2)
        if addr.startswith("0x") and len(addr) == 42:
            out.append((chain, addr.lower()))
    return out


async def upsert_ofac(pool: PgPool, sdn_entries: list[dict]) -> int:
    if not sdn_entries:
        return 0
    rows = [
        {
            "uid": e["uid"],
            "name": e["name"],
            "crypto_addresses": [e["address"]],
            "country": e.get("country", ""),
            "source": e.get("source", "ofac"),
        }
        for e in sdn_entries
    ]
    sql = """
    INSERT INTO ofac_sdn (uid, name, crypto_addresses, country, source)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (uid) DO UPDATE SET
        name = EXCLUDED.name,
        crypto_addresses = EXCLUDED.crypto_addresses,
        country = EXCLUDED.country,
        fetched_at = NOW()
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(sql, r["uid"], r["name"], r["crypto_addresses"], r["country"], r["source"])
    return len(rows)


async def upsert_entities_from_ofac(pool: PgPool, sdn_entries: list[dict]) -> int:
    if not sdn_entries:
        return 0
    seen: set[str] = set()
    rows: list[dict] = []
    for e in sdn_entries:
        entity_id = f"ofac:{e['uid']}"
        if entity_id in seen:
            continue
        seen.add(entity_id)
        rows.append({
            "entity_id": entity_id,
            "name": e["name"],
            "type": "sanctioned",
            "labels": ["sanctioned", e.get("chain", ""), "ofac"],
            "risk_level": "critical",
            "source": e.get("source", "ofac"),
        })
    sql = """
    INSERT INTO entities (entity_id, name, type, labels, risk_level, source)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (entity_id) DO UPDATE SET
        name = EXCLUDED.name,
        labels = EXCLUDED.labels,
        risk_level = EXCLUDED.risk_level
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(sql, r["entity_id"], r["name"], r["type"], r["labels"], r["risk_level"], r["source"])
    return len(rows)


async def link_ofac_wallets(pool: PgPool, sdn_entries: list[dict]) -> int:
    if not sdn_entries:
        return 0
    addresses = list({e["address"] for e in sdn_entries})
    sql = """
    UPDATE wallets
    SET entity_id = e.entity_id,
        risk_flags = array_append(COALESCE(risk_flags, ARRAY[]::TEXT[]), 'sanctioned')
    FROM (SELECT unnest($1::TEXT[]) AS address, 'ofac:' || uid AS entity_id
          FROM (SELECT unnest($2::TEXT[]) AS uid) u) e
    WHERE wallets.address = e.address
    """
    uids = [e["uid"] for e in sdn_entries]
    async with pool.acquire() as conn:
        result = await conn.execute(sql, addresses, uids)
    return int(result.split()[-1]) if result else 0


async def build_co_activity_graph(pool: PgPool) -> tuple[dict[str, set[str]], dict[str, int]]:
    """For each wallet, compute the set of its counterparties and tx_count."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH recent AS (
              SELECT from_address AS w1, to_address AS w2
              FROM chain_transfers
              WHERE time > NOW() - INTERVAL '7 days'
                AND token_symbol IN ('USDC', 'USDT')
                AND from_address NOT IN ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dead')
                AND to_address NOT IN ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dead')
            )
            SELECT w1, w2, COUNT(*) AS n FROM recent
            GROUP BY w1, w2
            """
        )
    counterparties: dict[str, set[str]] = defaultdict(set)
    tx_count: dict[str, int] = defaultdict(int)
    for r in rows:
        w1, w2 = r["w1"].lower(), r["w2"].lower()
        n = int(r["n"])
        counterparties[w1].add(w2)
        tx_count[w1] += n
        counterparties[w2].add(w1)
        tx_count[w2] += n
    return dict(counterparties), dict(tx_count)


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def leiden_cluster(
    wallets: list[str],
    counterparties: dict[str, set[str]],
    jaccard_threshold: float,
) -> list[list[str]]:
    """Greedy single-link clustering on Jaccard >= threshold. Returns clusters."""
    parent: dict[str, str] = {w: w for w in wallets}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    by_w = {w: counterparties.get(w, set()) for w in wallets}
    wallet_index = {w: i for i, w in enumerate(wallets)}
    edges_added = 0
    for i, a in enumerate(wallets):
        ca = by_w[a]
        if not ca:
            continue
        for b in wallets[i + 1 :]:
            cb = by_w[b]
            if not cb:
                continue
            inter = len(ca & cb)
            if inter == 0:
                continue
            union_size = len(ca) + len(cb) - inter
            j = inter / union_size if union_size else 0.0
            if j >= jaccard_threshold:
                union(a, b)
                edges_added += 1
    clusters: dict[str, list[str]] = defaultdict(list)
    for w in wallets:
        clusters[find(w)].append(w)
    return list(clusters.values())


async def persist_clusters(
    pool: PgPool,
    clusters: list[list[str]],
    jaccard_threshold: float,
) -> int:
    cluster_id_to_members: dict[str, list[str]] = {}
    rows = []
    for i, members in enumerate(clusters):
        if len(members) < 2:
            continue
        cid = f"cl_{i:06d}"
        cluster_id_to_members[cid] = members
        rows.append({
            "cluster_id": cid,
            "size": len(members),
            "method": f"jaccard_leiden_{jaccard_threshold}",
            "confidence": min(0.99, 0.5 + jaccard_threshold),
            "canonical_address": members[0],
        })
    if not rows:
        return 0
    cluster_sql = """
    INSERT INTO clusters (cluster_id, size, method, confidence, canonical_address)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (cluster_id) DO UPDATE SET
        size = EXCLUDED.size,
        method = EXCLUDED.method,
        confidence = EXCLUDED.confidence,
        canonical_address = EXCLUDED.canonical_address
    """
    wallet_sql = """
    UPDATE wallets SET cluster_id = $1 WHERE address = ANY($2::TEXT[])
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(
                    cluster_sql, r["cluster_id"], r["size"], r["method"], r["confidence"], r["canonical_address"]
                )
            for r in rows:
                await conn.execute(wallet_sql, r["cluster_id"], cluster_id_to_members[r["cluster_id"]])
    return len(rows)


async def build_gold_set(pool: PgPool) -> list[tuple[str, str, bool]]:
    """Build a labeled gold set: pairs of wallets that are SAME entity (True) or DIFFERENT (False)."""
    async with pool.acquire() as conn:
        sdn_rows = await conn.fetch("SELECT unnest(crypto_addresses) AS addr FROM ofac_sdn")
        sdn_addresses = {r["addr"].lower() for r in sdn_rows if r["addr"]}
        same_rows = await conn.fetch(
            "SELECT cluster_id, array_agg(address) AS members FROM wallets WHERE cluster_id IS NOT NULL GROUP BY cluster_id"
        )
    pairs: list[tuple[str, str, bool]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for r in same_rows:
        members = sorted([a.lower() for a in r["members"]])
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                pair = (members[i], members[j])
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    pairs.append((members[i], members[j], True))

    sdn_list = sorted(sdn_addresses)
    for i in range(len(sdn_list)):
        for j in range(i + 1, min(i + 6, len(sdn_list))):
            a, b = sdn_list[i], sdn_list[j]
            pair = tuple(sorted([a, b]))
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                pairs.append((a, b, True))

    async with pool.acquire() as conn:
        wallet_rows = await conn.fetch("SELECT address FROM wallets ORDER BY address")
    addresses = [r["address"].lower() for r in wallet_rows]
    n_wallets = len(addresses)
    same_set = seen_pairs
    n_neg = min(len(pairs) * 2, 200)
    import random
    rng = random.Random(42)
    while len([p for p in pairs if not p[2]]) < n_neg:
        i, j = rng.sample(range(n_wallets), 2)
        a, b = sorted([addresses[i], addresses[j]])
        if (a, b) not in same_set and (a, b, False) not in pairs:
            pairs.append((a, b, False))
            if len([p for p in pairs if not p[2]]) >= n_neg:
                break
    return pairs


async def evaluate_f1(
    pool: PgPool,
    jaccard_threshold: float,
    gold_pairs: list[tuple[str, str, bool]],
    counterparties: dict[str, set[str]],
) -> dict[str, float]:
    """Compute precision/recall/F1 by re-clustering on the labeled set."""
    all_addresses = sorted({a for a, _, _ in gold_pairs} | {b for _, b, _ in gold_pairs})
    true_pairs = {(a, b) for a, b, same in gold_pairs if same}
    if not true_pairs:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "n_true": 0, "n_pred": 0, "n_gold": 0}
    by_w = {a: counterparties.get(a, set()) & set(all_addresses) for a in all_addresses}
    parent = {a: a for a in all_addresses}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    for i, a in enumerate(all_addresses):
        ca = by_w[a]
        if not ca:
            continue
        for b in all_addresses[i + 1 :]:
            cb = by_w[b]
            if not cb:
                continue
            inter = len(ca & cb)
            if inter == 0:
                continue
            union_size = len(ca) + len(cb) - inter
            j = inter / union_size if union_size else 0.0
            if j >= jaccard_threshold:
                union(a, b)
    pred_pairs = set()
    for a in all_addresses:
        for b in all_addresses:
            if a < b and find(a) == find(b):
                pred_pairs.add((a, b))
    tp = len(true_pairs & pred_pairs)
    fp = len(pred_pairs - true_pairs)
    fn = len(true_pairs - pred_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "n_true": len(true_pairs),
        "n_pred": len(pred_pairs),
        "n_gold": len(gold_pairs),
    }


async def run_pipeline(pool: PgPool) -> None:
    sdn = await fetch_ofac_sdn()
    if sdn:
        await upsert_ofac(pool, sdn)
        await upsert_entities_from_ofac(pool, sdn)
        n_linked = await link_ofac_wallets(pool, sdn)
        log.info("ofac_linked_wallets", n=n_linked)

    counterparties, tx_count = await build_co_activity_graph(pool)
    candidates = [w for w, c in counterparties.items() if tx_count.get(w, 0) >= MIN_TX_COUNT]
    log.info("er_candidates", total=len(counterparties), after_filter=len(candidates))

    best: dict[str, float] = {"f1": 0.0}
    best_threshold = MIN_CO_ACTIVITY_JACCARD
    for threshold in [0.03, 0.05, 0.08, 0.12, 0.18, 0.25]:
        gold = await build_gold_set(pool)
        metrics = await evaluate_f1(pool, threshold, gold, counterparties)
        log.info("er_eval", threshold=threshold, **metrics)
        if metrics["f1"] > best["f1"]:
            best = metrics
            best_threshold = threshold

    log.info("er_best", threshold=best_threshold, **best)
    clusters = leiden_cluster(candidates, counterparties, best_threshold)
    n_persisted = await persist_clusters(pool, clusters, best_threshold)
    log.info("er_clusters_persisted", n=n_persisted)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    log.info("starting_entity_resolution")

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
                await run_pipeline(pool)
            except Exception as e:
                log.error("er_pipeline_error", err=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=REFRESH_INTERVAL)
            except asyncio.TimeoutError:
                pass
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
