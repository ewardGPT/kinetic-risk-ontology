#!/usr/bin/env bash
# KRO bootstrap — bring up the full stack from scratch on a clean VPS.
# Usage: VPS_HOST=vps ./scripts/bootstrap.sh
set -euo pipefail

VPS="${VPS_HOST:-vps}"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Syncing project to $VPS"
rsync -az --delete --exclude='.git' --exclude='.venv' --exclude='__pycache__' \
  --exclude='*.pyc' --exclude='data/*.csv' --exclude='data/*.parquet' \
  "$LOCAL_DIR/" "$VPS:~/projects/palentir/"

echo "==> Building and starting services"
ssh "$VPS" 'cd ~/projects/palentir && docker compose up -d --build'

echo "==> Waiting for services to become healthy..."
for i in {1..30}; do
  n_healthy=$(ssh "$VPS" 'docker ps --format "{{.Status}}" 2>/dev/null | grep -c "healthy\|Up"' || echo 0)
  n_total=$(ssh "$VPS" 'docker ps --format "{{.Names}}" 2>/dev/null | wc -l' || echo 0)
  if [ "$n_healthy" -ge 6 ]; then
    echo "==> $n_healthy/$n_total services healthy"
    break
  fi
  echo "  ...waiting ($n_healthy/$n_total healthy)"
  sleep 5
done

echo "==> Seeding demo alert"
"$LOCAL_DIR/scripts/seed_demo_alert.sh" || true

echo
echo "==> Stack is up. Surfaces:"
echo "  Grafana:  http://100.114.62.36:3300/d/kro-main"
echo "  Neo4j:    http://100.114.62.36:7474  (neo4j / kro_local_2026)"
echo "  psql:     docker exec -it palentir-timescaledb psql -U postgres -d kro"
echo
echo "==> Useful Cypher queries are in the README under 'Ontology in Cypher'."
