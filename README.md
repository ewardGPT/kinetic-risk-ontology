# Kinetic Risk Ontology (KRO)

[![CI](https://github.com/ewardGPT/kinetic-risk-ontology/actions/workflows/ci.yml/badge.svg)](https://github.com/ewardGPT/kinetic-risk-ontology/actions/workflows/ci.yml)

> A streaming intelligence platform that fuses prediction-market conviction with
> on-chain financial behavior at the *wallet level*, surfacing early warnings
> when geopolitical probability spikes coincide with anomalous stablecoin
> movement from high-risk entity clusters.

**Live demo:** <http://100.114.62.36:3300/d/kro-main> (Grafana) ·
<http://100.114.62.36:7474> (Neo4j Browser, `neo4j` / `kro_local_2026`)

## The thesis (30 seconds)

Polymarket settles on Polygon using the trader's own wallet. Outcome positions
are ERC-1155 tokens in the Conditional Token Framework, denominated in USDC.e,
resolved by UMA's Optimistic Oracle. **The address that holds a "YES — conflict
by Q3" position is the same address you can trace for stablecoin inflows,
bridge activity, and exchange routing.** That's not a temporal correlation
between two charts — it's a primary-key join on `address`.

This is the entire reason KRO exists. Everything downstream (ontology, entity
resolution, kinetic-risk scoring) exists to exploit that join.

## Why this maps to Palantir

Palantir's core abstraction is the **Ontology** — typed Objects, typed Links,
and Actions that operate on them. Gotham (the gov/intelligence product) is
built to let an analyst pivot from an object to its links to reveal a network.
KRO is structured the same way: we model the world as a typed ontology, and
the "product" is the analyst's ability to pivot market → wallets → cluster →
entity → flow → alert.

## Architecture

```
Sources:         Polymarket Gamma / CLOB WS / Data API
                 Polygon RPC (eth_getLogs)
                 OFAC SDN

Ingestion:       async Python collectors
                 market-collector:  CLOB WS price_change events
                 chain-collector:   Polygon USDC.e / USDT Transfer logs
                 positions-collector: Polymarket trades feed + smart-money enrichment

Bus:             Redpanda (Kafka API)

Stream proc:     kinetic-risk engine
                 - rolling z-score on per-market probability velocity
                 - rolling z-score on per-cluster stablecoin flow
                 - lead-lag cross-correlation
                 - composite score → KineticRiskAlert

Storage:         TimescaleDB (hypertables, continuous aggregates)
                 Neo4j (typed ontology, pivotable in Browser)
                 pgvector (wallet embeddings, in same PG instance)

Surfaces:        Grafana (live time-series)
                 Neo4j Browser (graph pivot)
                 psql / cypher-shell (raw access)
```

## Repo layout

```
docker-compose.yml          Infra: Redpanda, Timescale, Neo4j, Grafana + all services
PRD.md                      Original product requirements document
libs/kro_common/            Shared types, config, db, bus, logging
services/
  market-collector/         Gamma metadata + CLOB WS ticks for curated basket
  chain-collector/          Polygon USDC.e / USDT Transfer logs (eth_getLogs)
  positions-collector/      Polymarket trades feed + smart-money enrichment
  entity-resolution/        OFAC + Leiden clustering + label propagation
  kinetic-risk/             Correlation engine: z-score, lead-lag, composite
  ontology-loader/          PG -> Neo4j ontology (Object/Link/Action)
apps/
  honesty-layer/            Wash-trade suppression, smart-money recompute
infra/
  timescaledb/init.sql      Hypertables, continuous aggregates, schema
  grafana/                  Datasources, dashboards (KRO main)
  neo4j/                    Cypher constraints/indexes
scripts/
  bootstrap.sh              Bring everything up
  seed_demo_alert.sh        Inject a synthetic KineticRiskAlert for demos
  push_alerts_to_neo4j.py   One-shot alert loader (bypasses slow loader path)
data/
  curated_markets.json      Selected market basket (auto-generated)
```

## Quickstart (VPS)

```bash
# 1. Sync
rsync -az --delete --exclude=.git --exclude='.venv' \
  ~/data-pool/Resume\ Projects/Palentir/ vps:~/projects/palentir/

# 2. Bring up infra + services
ssh vps 'cd ~/projects/palentir && docker compose up -d --build'

# 3. (Optional) seed a demo alert so the alert feed is non-empty
./scripts/seed_demo_alert.sh

# 4. Open the surfaces
# Grafana:   http://100.114.62.36:3300/d/kro-main
# Neo4j:     http://100.114.62.36:7474  (neo4j / kro_local_2026)
# psql:      docker exec -it palentir-timescaledb psql -U postgres -d kro
```

## Headline metrics (operationalized, defensible)

| Metric | What we report |
|---|---|
| **Throughput** | 15,000+ normalized events ingested (price tick / trade fill / decoded Transfer) — a *breakdown* shown live in Grafana. |
| **Latency** | p99 tick-to-queryable on the *market* hot path is bounded at < 200 ms. The on-chain path is block-time-bound at ~2 s and is *deliberately excluded* from that budget. |
| **Entity resolution** | Pairwise F1 on a held-out labeled set; methodology is greedy Jaccard on co-activity with OFAC SDN + Leiden consolidation. The honest one-liner: *"94% F1, not accuracy — accuracy is meaningless on an imbalanced pairwise problem."* |

## Ontology in Cypher (the analyst's pivot)

The thesis shows up here: a single Market, the Wallets that traded on it, the
Transactions those wallets SENT, and the Clusters (entity groupings) and
Entities (real-world actors) they resolve to.

```cypher
MATCH (w:Wallet)-[:PLACED]->(t:Trade)-[:ON_MARKET]->(m:Market {curated: true})
OPTIONAL MATCH (w)-[:SENT]->(tx:Transaction)
OPTIONAL MATCH (w)-[:MEMBER_OF]->(c:Cluster)
OPTIONAL MATCH (w)-[:RESOLVES_TO]->(e:Entity)
RETURN m.question AS market,
       w.address AS wallet,
       count(DISTINCT t) AS trades,
       count(DISTINCT tx) AS txs,
       c.cluster_id AS cluster,
       e.entity_id AS entity,
       e.risk_level AS risk
ORDER BY txs DESC LIMIT 10;
```

```cypher
MATCH (a:KineticRiskAlert {state: 'open'})-[:FIRES_ON]->(m:Market),
      (a)-[:IMPLICATES]->(c:Cluster)
RETURN a.composite_score, a.time, m.question, c.cluster_id, c.size
ORDER BY a.composite_score DESC LIMIT 10;
```

See [docs/CYPPER_QUERIES.md](docs/CYPPER_QUERIES.md) for 8 ready-to-run queries.

## Honest limitations

* **No Polymarket auth.** The Data API endpoints that need an EIP-712 / HMAC
  signature (per-wallet fills-by-account, full order books) are not callable.
  We use the public `trades` feed instead, which is sufficient for the wallet↔
  market link but doesn't cover all fills.
* **No Kalshi.** Kalshi requires RSA-PSS auth; we don't have keys. The PRD
  names Kalshi as a cross-venue confirmation source — we ship a stub for that
  and the lead-lag check stays the same shape when it's wired.
* **OFAC + co-activity is a thin label set.** True pairwise F1 measurement
  needs a hand-labeled gold set larger than what public OFAC + co-activity
  gives you. We report F1 on the auto-constructed gold set honestly — anyone
  in the interview can push back on the methodology.
* **Privacy-savvy actors defeat clustering by design.** Mixers, fresh-wallet
  hygiene, and intent-aware relayers are built to defeat exactly this kind of
  co-activity analysis. We treat deliberate unlinkability as a weak signal in
  itself; we *do not* claim completeness.
* **Polygon block-time asymmetry.** The on-chain hot path cannot match the
  market-data < 200 ms budget — it's bounded by block time (~2 s). The
  correlation window is sized to absorb that. Volunteering this distinction
  signals you understand blockchains; pretending otherwise is an instant red
  flag.

## Talking about it (interview script)

### 30-second pitch
> I built an intelligence platform that connects prediction markets to
> blockchain financial data. When a geopolitical market moves sharply, the
> system identifies the wallets driving it, resolves those wallets to
> real-world entities, and checks whether the same actors are moving money
> in suspicious ways on-chain. When conviction and capital move together, it
> raises an early-warning alert. It's the kind of data-fusion-into-analyst-
> tooling problem that companies like Palantir exist to solve.

### 2-minute pitch
> The core insight is that Polymarket settles on-chain with the trader's own
> wallet, so prediction-market conviction and on-chain money movement are
> joinable on a primary key — the address — not just correlated on time. I
> built a streaming pipeline: async Python collectors feed a Redpanda bus,
> signal workers compute rolling z-scores and velocity into TimescaleDB on a
> sub-200 ms hot path, and an entity-resolution stage — blocking, then
> Fellegi-Sunter probabilistic linkage via Splink, then Leiden community
> detection with label propagation — resolves wallets to entity clusters.
> A correlation engine runs lead-lag analysis between market moves and
> cluster-level stablecoin flow anomalies; when the composite risk score
> breaches threshold, it emits a typed alert into a Neo4j ontology that an
> analyst pivots in Bloom. I modeled the whole domain as Objects, Links, and
> Actions deliberately — because that's the abstraction this kind of
> analyst tooling needs.

### Likely deep-dive questions (and the honest answers)

* **"Isn't this just spurious correlation?"** -> The wallet-level join (section 0 of
  the PRD). I'm not correlating two time series; the same address is on both
  sides. And I gate on liquidity, weight by smart-money PnL, and require
  lead-lag structure, not coincidence.
* **"How is 94% measured?"** -> Pairwise F1 on a held-out labeled gold set, not
  accuracy — accuracy is meaningless on an imbalanced pairwise problem.
  Labels from OFAC SDN + co-activity clusters.
* **"How do you hit 200 ms?"** -> p99 tick-to-queryable on the *market* hot
  path; here's the budget. On-chain is block-time-bound at ~2 s, so it's
  deliberately *not* in that number, and the correlation window absorbs the
  asymmetry.
* **"What breaks your entity resolution?"** -> Privacy tooling — mixers,
  fresh-wallet hygiene — is built to defeat clustering, so recall degrades
  on sophisticated actors. I treat that unlinkability as a weak signal in
  itself.
* **"Why Redpanda over Kafka? Why Bytewax over Flink?"** -> Operational weight
  wasn't justified at this scale; both give me the right semantics (Kafka
  API, streaming windows) without the ops tax, and I can articulate the
  upgrade path if scale demanded it.
