"""KRO Redpanda/Kafka bus producer + consumer."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


def _ser(v: Any) -> bytes:
    return orjson.dumps(v, default=str)


def _deser(v: bytes) -> Any:
    return orjson.loads(v)


class BusProducer:
    def __init__(self, brokers: str, client_id: str = "kro-producer") -> None:
        self.brokers = brokers
        self._producer: AIOKafkaProducer | None = None
        self._client_id = client_id

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            client_id=self._client_id,
            value_serializer=_ser,
            acks=1,
            enable_idempotence=False,
            compression_type=None,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        assert self._producer is not None, "BusProducer not started"
        await self._producer.send_and_wait(topic, value=value, key=key.encode() if key else None)

    async def send_batch(self, topic: str, values: list[dict[str, Any]], key_fn=None) -> None:
        assert self._producer is not None, "BusProducer not started"
        for v in values:
            key = key_fn(v) if key_fn else None
            await self._producer.send_and_wait(topic, value=v, key=key.encode() if key else None)


class BusConsumer:
    def __init__(
        self,
        brokers: str,
        group_id: str,
        topics: list[str],
        client_id: str = "kro-consumer",
    ) -> None:
        self.brokers = brokers
        self.group_id = group_id
        self.topics = topics
        self._client_id = client_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            client_id=self._client_id,
            value_deserializer=_deser,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def consume(self) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        assert self._consumer is not None, "BusConsumer not started"
        async for msg in self._consumer:
            yield (msg.topic, msg.value)
            await self._consumer.commit()


TOPICS = {
    "market_ticks": "market.ticks",
    "market_fills": "market.fills",
    "chain_transfers": "chain.transfers",
    "alerts": "kro.alerts",
}
