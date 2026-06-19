#!/usr/bin/env bash
# KRO demo script — sets up a synthetic kinetic alert for live demo purposes
# while the real correlation engine warms up.

set -euo pipefail

VPS="${VPS_HOST:-vps}"
PROJECT_DIR="${PROJECT_DIR:-~/projects/palentir}"

DEMO_MARKET="${DEMO_MARKET:-}"
if [ -z "$DEMO_MARKET" ]; then
  DEMO_MARKET=$(ssh "$VPS" "docker exec palentir-timescaledb psql -U postgres -d kro -tA -c \"SELECT condition_id FROM markets WHERE curated = TRUE AND liquidity > 5000 ORDER BY liquidity DESC LIMIT 1\"")
fi

DEMO_CLUSTER="${DEMO_CLUSTER:-}"
if [ -z "$DEMO_CLUSTER" ]; then
  DEMO_CLUSTER=$(ssh "$VPS" "docker exec palentir-timescaledb psql -U postgres -d kro -tA -c \"SELECT cluster_id FROM clusters WHERE size BETWEEN 3 AND 200 ORDER BY size DESC LIMIT 1\"")
fi

if [ -z "$DEMO_MARKET" ] || [ -z "$DEMO_CLUSTER" ]; then
  echo "No demo market or cluster available. Need curated markets + ER clusters." >&2
  exit 1
fi

echo "Seeding synthetic kinetic alert: market=$DEMO_MARKET, cluster=$DEMO_CLUSTER"

ssh "$VPS" "docker exec palentir-timescaledb psql -U postgres -d kro -c \"
INSERT INTO kinetic_alerts (
  time, market_id, cluster_id,
  market_signal, flow_signal, entity_risk, lead_lag, composite_score, state
) VALUES (
  NOW(),
  '$DEMO_MARKET',
  '$DEMO_CLUSTER',
  '{\\\"zscore\\\": 3.2, \\\"velocity_buckets\\\": [0.51,0.52,0.55,0.61,0.68,0.72,0.74,0.74], \\\"volume_now\\\": 12450, \\\"cluster_trader_count\\\": 5}'::jsonb,
  '{\\\"zscore\\\": 2.8, \\\"flow_now\\\": 8200, \\\"rolling_buckets\\\": [100,200,150,180,520,1800,4400,8100,8200]}'::jsonb,
  '{\\\"weight\\\": 1.5, \\\"risk_level\\\": \\\"medium\\\", \\\"risk_flags\\\": [\\\"smart_money\\\"]}'::jsonb,
  '{\\\"cross_corr\\\": 0.62, \\\"lag_buckets\\\": -1, \\\"interpretation\\\": \\\"market leads on-chain flow\\\"}'::jsonb,
  0.42,
  'open'
)\""

echo "Demo alert seeded. View at:"
echo "  Grafana:    http://100.114.62.36:3300/d/kro-main"
echo "  Neo4j:      http://100.114.62.36:7474"
echo "  Query (run in Neo4j Browser):"
echo ""
echo "  MATCH (a:KineticRiskAlert {state:'open'})-[:FIRES_ON]->(m:Market),"
echo "        (a)-[:IMPLICATES]->(c:Cluster)"
echo "  RETURN a.composite_score, m.question, c.cluster_id, c.size"
echo "  ORDER BY a.composite_score DESC LIMIT 5;"
