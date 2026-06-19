-- KRO TimescaleDB init
-- Hypertables, continuous aggregates, and core tables for the market + on-chain hot path.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS markets (
  condition_id      TEXT PRIMARY KEY,
  market_id         TEXT,
  question          TEXT NOT NULL,
  slug              TEXT,
  category          TEXT,
  tags              TEXT[],
  resolution_date   TIMESTAMPTZ,
  closed            BOOLEAN DEFAULT FALSE,
  active            BOOLEAN DEFAULT TRUE,
  liquidity         NUMERIC(28, 6),
  open_interest     NUMERIC(28, 6),
  volume_24h        NUMERIC(28, 6),
  volume_total      NUMERIC(28, 6),
  tokens            JSONB,
  outcomes          JSONB,
  metadata          JSONB,
  curated           BOOLEAN DEFAULT FALSE,
  basket            TEXT,
  fetched_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS markets_active_idx     ON markets (active) WHERE active;
CREATE INDEX IF NOT EXISTS markets_basket_idx     ON markets (basket) WHERE basket IS NOT NULL;
CREATE INDEX IF NOT EXISTS markets_question_trgm  ON markets USING GIN (question gin_trgm_ops);

CREATE TABLE IF NOT EXISTS wallets (
  address             TEXT PRIMARY KEY,
  chain               TEXT NOT NULL DEFAULT 'polygon',
  first_seen          TIMESTAMPTZ,
  last_seen           TIMESTAMPTZ,
  polymarket_pnl      NUMERIC(28, 6),
  polymarket_volume   NUMERIC(28, 6),
  polymarket_trades   INT,
  smart_money_score   NUMERIC(6, 5),
  cluster_id          TEXT,
  entity_id           TEXT,
  risk_flags          TEXT[],
  features            JSONB,
  behavior_embedding  VECTOR(64),
  created_at          TIMESTAMPTZ DEFAULT NOW(),
  updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS wallets_cluster_idx  ON wallets (cluster_id) WHERE cluster_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS wallets_entity_idx   ON wallets (entity_id)  WHERE entity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS wallets_smart_idx    ON wallets (smart_money_score DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS entities (
  entity_id    TEXT PRIMARY KEY,
  name         TEXT,
  type         TEXT,           -- exchange, fund, individual, sanctioned, mixer, bridge
  labels       TEXT[],
  risk_level   TEXT,           -- low, medium, high, critical
  source       TEXT,           -- ofac, etherscan, manual, inferred
  metadata     JSONB,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS entities_type_idx ON entities (type);

CREATE TABLE IF NOT EXISTS clusters (
  cluster_id          TEXT PRIMARY KEY,
  size                INT NOT NULL DEFAULT 1,
  method              TEXT,                  -- splink, leiden, manual
  confidence          NUMERIC(6, 5),
  risk_flags          TEXT[],
  canonical_address   TEXT,
  created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- EVENT TABLES (hypertables)
-- ============================================================

-- Market price ticks (probability over time)
CREATE TABLE IF NOT EXISTS market_ticks (
  time          TIMESTAMPTZ NOT NULL,
  market_id     TEXT NOT NULL,
  condition_id  TEXT NOT NULL,
  asset_id      TEXT NOT NULL,
  outcome       TEXT NOT NULL,    -- 'YES' / 'NO'
  price         NUMERIC(10, 6) NOT NULL,
  probability   NUMERIC(6, 5) NOT NULL,
  size          NUMERIC(28, 6),
  hash          TEXT,
  source        TEXT DEFAULT 'clob-ws'
);
SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS market_ticks_market_time_idx ON market_ticks (market_id, time DESC);
CREATE INDEX IF NOT EXISTS market_ticks_cond_time_idx   ON market_ticks (condition_id, time DESC);

-- Trade fills
CREATE TABLE IF NOT EXISTS market_fills (
  time             TIMESTAMPTZ NOT NULL,
  trade_id         TEXT NOT NULL,
  market_id        TEXT NOT NULL,
  condition_id     TEXT,
  asset_id         TEXT,
  outcome          TEXT NOT NULL,
  side             TEXT NOT NULL,    -- 'BUY' / 'SELL'
  price            NUMERIC(10, 6) NOT NULL,
  size             NUMERIC(28, 6) NOT NULL,
  notional         NUMERIC(28, 6),
  fee              NUMERIC(28, 6),
  maker_address    TEXT,
  taker_address    TEXT,
  PRIMARY KEY (time, trade_id)
);
SELECT create_hypertable('market_fills', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS market_fills_taker_time_idx ON market_fills (taker_address, time DESC);
CREATE INDEX IF NOT EXISTS market_fills_maker_time_idx ON market_fills (maker_address, time DESC);
CREATE INDEX IF NOT EXISTS market_fills_market_time_idx ON market_fills (market_id, time DESC);

-- On-chain ERC-20 transfers
CREATE TABLE IF NOT EXISTS chain_transfers (
  time            TIMESTAMPTZ NOT NULL,
  chain           TEXT NOT NULL DEFAULT 'polygon',
  block_number    BIGINT NOT NULL,
  tx_hash         TEXT NOT NULL,
  log_index       INT NOT NULL,
  token_address   TEXT NOT NULL,
  token_symbol    TEXT,
  from_address    TEXT NOT NULL,
  to_address      TEXT NOT NULL,
  amount_raw      NUMERIC(78, 0) NOT NULL,
  amount_human    NUMERIC(28, 6),
  decimals        INT,
  PRIMARY KEY (time, chain, tx_hash, log_index)
);
SELECT create_hypertable('chain_transfers', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS chain_transfers_from_time_idx ON chain_transfers (from_address, time DESC);
CREATE INDEX IF NOT EXISTS chain_transfers_to_time_idx   ON chain_transfers (to_address, time DESC);
CREATE INDEX IF NOT EXISTS chain_transfers_token_idx     ON chain_transfers (token_address, time DESC);

-- Kinetic Risk alerts
CREATE TABLE IF NOT EXISTS kinetic_alerts (
  id               UUID NOT NULL DEFAULT gen_random_uuid(),
  time             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  market_id        TEXT NOT NULL,
  cluster_id       TEXT,
  entity_id        TEXT,
  market_signal    JSONB NOT NULL,
  flow_signal      JSONB,
  entity_risk      JSONB,
  lead_lag         JSONB,
  composite_score  NUMERIC(6, 5) NOT NULL,
  state            TEXT NOT NULL DEFAULT 'open',  -- 'open', 'escalated', 'false_positive', 'resolved'
  analyst          TEXT,
  notes            TEXT,
  PRIMARY KEY (time, id)
);
SELECT create_hypertable('kinetic_alerts', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS kinetic_alerts_market_time_idx ON kinetic_alerts (market_id, time DESC);
CREATE INDEX IF NOT EXISTS kinetic_alerts_score_idx       ON kinetic_alerts (composite_score DESC, time DESC);
CREATE INDEX IF NOT EXISTS kinetic_alerts_state_idx       ON kinetic_alerts (state) WHERE state = 'open';

-- ============================================================
-- DERIVED SIGNAL TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS wallet_features (
  address            TEXT PRIMARY KEY REFERENCES wallets(address) ON DELETE CASCADE,
  tx_count_total     INT,
  tx_count_30d       INT,
  unique_counterparties_30d INT,
  stablecoin_volume_30d  NUMERIC(28, 6),
  bridge_txs_30d     INT,
  exchange_txs_30d   INT,
  mean_gas_price     NUMERIC(20, 6),
  active_hours_entropy NUMERIC(6, 5),
  first_tx           TIMESTAMPTZ,
  last_tx            TIMESTAMPTZ,
  computed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS candidate_pairs (
  address_a        TEXT NOT NULL,
  address_b        TEXT NOT NULL,
  match_probability NUMERIC(6, 5),
  features         JSONB,
  method           TEXT,        -- 'splink', 'heuristic'
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (address_a, address_b)
);
CREATE INDEX IF NOT EXISTS candidate_pairs_prob_idx ON candidate_pairs (match_probability DESC);

-- ============================================================
-- CONTINUOUS AGGREGATES
-- ============================================================

-- 1-minute price summary per market+outcome
CREATE MATERIALIZED VIEW IF NOT EXISTS market_ticks_1min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time) AS bucket,
  market_id,
  condition_id,
  outcome,
  AVG(probability)              AS prob_mean,
  STDDEV_SAMP(probability)      AS prob_std,
  MIN(probability)              AS prob_low,
  MAX(probability)              AS prob_high,
  LAST(probability, time)       AS prob_last,
  SUM(COALESCE(size, 0))        AS volume
FROM market_ticks
GROUP BY bucket, market_id, condition_id, outcome
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_ticks_1min',
  start_offset => INTERVAL '2 hours',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute',
  if_not_exists => TRUE);

-- 5-minute on-chain flow summary per wallet
CREATE MATERIALIZED VIEW IF NOT EXISTS chain_outflow_5min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', time) AS bucket,
  from_address                   AS wallet,
  SUM(amount_human)              AS out_flow,
  COUNT(*)                       AS out_count
FROM chain_transfers
WHERE token_symbol IN ('USDC', 'USDT')
GROUP BY bucket, from_address
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS chain_inflow_5min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', time) AS bucket,
  to_address                     AS wallet,
  SUM(amount_human)              AS in_flow,
  COUNT(*)                       AS in_count
FROM chain_transfers
WHERE token_symbol IN ('USDC', 'USDT')
GROUP BY bucket, to_address
WITH NO DATA;

SELECT add_continuous_aggregate_policy('chain_outflow_5min',
  start_offset => INTERVAL '4 hours',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes',
  if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('chain_inflow_5min',
  start_offset => INTERVAL '4 hours',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes',
  if_not_exists => TRUE);

-- ============================================================
-- ROLLOUT METRICS
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_metrics (
  time         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  service      TEXT NOT NULL,
  metric       TEXT NOT NULL,
  value        NUMERIC(20, 6),
  labels       JSONB
);
SELECT create_hypertable('pipeline_metrics', 'time', if_not_exists => TRUE);

-- ============================================================
-- SANCTIONS / LABEL TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS ofac_sdn (
  uid           TEXT PRIMARY KEY,
  name          TEXT,
  address_line  TEXT,
  city          TEXT,
  country       TEXT,
  crypto_addresses TEXT[],
  source        TEXT DEFAULT 'ofac',
  fetched_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ofac_sdn_addr_idx ON ofac_sdn USING GIN (crypto_addresses);

CREATE TABLE IF NOT EXISTS public_labels (
  address     TEXT NOT NULL,
  label       TEXT NOT NULL,
  category    TEXT,
  source      TEXT,
  confidence  NUMERIC(6, 5),
  PRIMARY KEY (address, label, source)
);
CREATE INDEX IF NOT EXISTS public_labels_addr_idx ON public_labels (address);

-- ============================================================
-- RETENTION
-- ============================================================

SELECT add_retention_policy('market_ticks',     INTERVAL '30 days',  if_not_exists => TRUE);
SELECT add_retention_policy('market_fills',     INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('chain_transfers',  INTERVAL '90 days',  if_not_exists => TRUE);
