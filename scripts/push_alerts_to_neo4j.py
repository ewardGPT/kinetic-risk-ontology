"""One-shot: push kinetic_alerts to Neo4j (bypasses the slow loader)."""
import asyncio
import json
import os
import sys

from kro_common import PgPool, get_settings
from neo4j import AsyncGraphDatabase


async def main():
    settings = get_settings()
    pg = PgPool()
    await pg.connect()
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
    )
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM kinetic_alerts")
    async with driver.session() as session:
        for r in rows:
            d = dict(r)
            d["market_signal"] = json.dumps(d.get("market_signal") or {})
            d["flow_signal"] = json.dumps(d.get("flow_signal") or {})
            d["entity_risk"] = json.dumps(d.get("entity_risk") or {})
            d["lead_lag"] = json.dumps(d.get("lead_lag") or {})
            await session.run(
                """
                MERGE (a:KineticRiskAlert {id: $id})
                SET a.time = datetime($time),
                    a.composite_score = $score,
                    a.state = $state,
                    a.market_signal = $market_signal,
                    a.flow_signal = $flow_signal,
                    a.entity_risk = $entity_risk,
                    a.lead_lag = $lead_lag
                WITH a
                WHERE $market_id IS NOT NULL
                MATCH (m:Market {condition_id: $market_id})
                MERGE (a)-[:FIRES_ON]->(m)
                WITH a
                WHERE $cluster_id IS NOT NULL
                MATCH (c:Cluster {cluster_id: $cluster_id})
                MERGE (a)-[:IMPLICATES]->(c)
                WITH a
                WHERE $entity_id IS NOT NULL
                MATCH (e:Entity {entity_id: $entity_id})
                MERGE (a)-[:EVIDENCED_BY]->(e)
                """,
                id=str(d["id"]),
                time=d["time"].isoformat() if d["time"] else None,
                score=float(d["composite_score"]),
                state=d["state"],
                market_signal=d["market_signal"],
                flow_signal=d["flow_signal"],
                entity_risk=d["entity_risk"],
                lead_lag=d["lead_lag"],
                market_id=d.get("market_id"),
                cluster_id=d.get("cluster_id"),
                entity_id=d.get("entity_id"),
            )
    await driver.close()
    await pg.close()
    print(f"Pushed {len(rows)} alerts to Neo4j.")


if __name__ == "__main__":
    asyncio.run(main())
