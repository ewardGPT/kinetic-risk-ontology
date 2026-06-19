// KRO ontology constraints (Neo4j)
CREATE CONSTRAINT market_condition_id IF NOT EXISTS FOR (m:Market) REQUIRE m.condition_id IS UNIQUE;
CREATE CONSTRAINT wallet_address IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE;
CREATE CONSTRAINT cluster_id IF NOT EXISTS FOR (c:Cluster) REQUIRE c.cluster_id IS UNIQUE;
CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;
CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (a:KineticRiskAlert) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT flow_agg_id IF NOT EXISTS FOR (f:FlowAggregate) REQUIRE f.flow_id IS UNIQUE;

CREATE INDEX wallet_cluster_idx IF NOT EXISTS FOR (w:Wallet) ON (w.cluster_id);
CREATE INDEX wallet_entity_idx IF NOT EXISTS FOR (w:Wallet) ON (w.entity_id);
CREATE INDEX market_active_idx IF NOT EXISTS FOR (m:Market) ON (m.active);
CREATE INDEX alert_score_idx IF NOT EXISTS FOR (a:KineticRiskAlert) ON (a.composite_score);
CREATE INDEX alert_time_idx IF NOT EXISTS FOR (a:KineticRiskAlert) ON (a.time);
