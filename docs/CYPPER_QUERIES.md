# Demo Cypher queries

Run these in the Neo4j Browser at <http://100.114.62.36:7474>
(login: `neo4j` / `kro_local_2026`).

## 0. Orientation — what objects do we have?

```cypher
MATCH (n)
RETURN labels(n)[0] AS type, count(n) AS n
ORDER BY n DESC;
```

## 1. The thesis (the one to show first)

Markets → traders → transactions → clusters. Pivots the ontology in one
query, which is exactly the Palantir-style analyst move.

```cypher
MATCH (w:Wallet)-[:PLACED]->(t:Trade)-[:ON_MARKET]->(m:Market)
WHERE m.curated = true
OPTIONAL MATCH (w)-[:SENT]->(tx:Transaction)
OPTIONAL MATCH (w)-[:MEMBER_OF]->(c:Cluster)
RETURN m.question AS market,
       w.address AS trader_wallet,
       c.cluster_id AS cluster,
       count(DISTINCT t) AS trades,
       count(DISTINCT tx) AS on_chain_txs,
       sum(t.notional) AS volume_usd
ORDER BY on_chain_txs DESC, volume_usd DESC
LIMIT 15;
```

## 2. Curated basket — what's actually in scope?

```cypher
MATCH (m:Market)
WHERE m.curated = true
RETURN m.question, m.liquidity, m.volume_24h, m.open_interest
ORDER BY m.liquidity DESC;
```

## 3. Smart-money concentration in a market

Wallets with non-null PnL that traded on a curated market, with their cluster
membership and risk status.

```cypher
MATCH (w:Wallet)-[:PLACED]->(t:Trade)-[:ON_MARKET]->(m:Market {curated: true})
WHERE w.polymarket_pnl IS NOT NULL OR w.smart_money_score > 0
OPTIONAL MATCH (w)-[:MEMBER_OF]->(c:Cluster)
OPTIONAL MATCH (w)-[:RESOLVES_TO]->(e:Entity)
RETURN m.question AS market,
       w.address AS wallet,
       w.polymarket_pnl AS pnl,
       w.smart_money_score AS smart_score,
       c.cluster_id AS cluster,
       e.risk_level AS risk,
       count(DISTINCT t) AS trades,
       sum(t.notional) AS volume_usd
ORDER BY pnl DESC NULLS LAST
LIMIT 20;
```

## 4. OFAC reach — did any sanctioned entity touch our markets?

```cypher
MATCH (w:Wallet)-[:RESOLVES_TO]->(e:Entity)
WHERE e.risk_level = 'critical' OR 'sanctioned' IN coalesce(w.risk_flags, [])
OPTIONAL MATCH (w)-[:PLACED]->(t:Trade)
RETURN w.address AS wallet,
       e.name AS sanctioned_entity,
       e.risk_level AS risk,
       count(DISTINCT t) AS trades_on_polymarket;
```

## 5. Open Kinetic Risk alerts

```cypher
MATCH (a:KineticRiskAlert {state: 'open'})-[:FIRES_ON]->(m:Market),
      (a)-[:IMPLICATES]->(c:Cluster)
RETURN a.composite_score, a.time, m.question, c.cluster_id, c.size
ORDER BY a.composite_score DESC;
```

## 6. Co-activity between two specific wallets (pivot)

```cypher
MATCH (a:Wallet {address: '<addr1>'})-[r:SENT|RECEIVED_BY*1..2]-(b:Wallet {address: '<addr2>'})
RETURN a, r, b
LIMIT 50;
```

## 7. Largest clusters and their risk profile

```cypher
MATCH (c:Cluster)<-[:MEMBER_OF]-(w:Wallet)
OPTIONAL MATCH (w)-[:RESOLVES_TO]->(e:Entity)
RETURN c.cluster_id, c.size AS members,
       count(DISTINCT e) AS distinct_entities,
       max(e.risk_level) AS max_risk
ORDER BY c.size DESC
LIMIT 15;
```

## 8. End-to-end: alert → market → traders → on-chain

```cypher
MATCH (a:KineticRiskAlert {state: 'open'})-[:FIRES_ON]->(m:Market),
      (a)-[:IMPLICATES]->(c:Cluster)<-[:MEMBER_OF]-(w:Wallet)
OPTIONAL MATCH (w)-[:SENT]->(tx:Transaction)
RETURN a.composite_score,
       m.question AS market,
       c.cluster_id AS cluster,
       w.address AS wallet,
       w.polymarket_pnl AS pnl,
       w.smart_money_score AS smart_score,
       count(DISTINCT tx) AS txs
ORDER BY a.composite_score DESC
LIMIT 25;
```
