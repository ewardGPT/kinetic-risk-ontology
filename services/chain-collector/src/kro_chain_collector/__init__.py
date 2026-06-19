"""On-chain chain collector — decodes USDC.e/USDT Transfer events from Polygon."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
from datetime import UTC, datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
import structlog
from kro_common import (
    TOPICS,
    BusProducer,
    PgPool,
    get_settings,
    insert_chain_transfers,
    setup_logging,
    upsert_wallets,
)

log = structlog.get_logger("chain-collector")


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
DEFAULT_TOKENS = [
    {"address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", "symbol": "USDC", "decimals": 6},
    {"address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359", "symbol": "USDC", "decimals": 6},
    {"address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", "symbol": "USDT", "decimals": 6},
]
MAX_BLOCK_RANGE = 2000
POLL_INTERVAL = 3.0
SAFE_REORG_DEPTH = 32


def _hex_to_int(h: str) -> int:
    if not h:
        return 0
    return int(h, 16) if isinstance(h, str) and h.startswith("0x") else int(h)


def _hex_topic_to_addr(topic: str) -> str:
    if not topic or topic == "0x":
        return "0x0000000000000000000000000000000000000000"
    return "0x" + topic[-40:].lower()


def _parse_log(
    log_entry: dict, token_meta: dict, block_ts_lookup: dict[int, datetime]
) -> dict | None:
    try:
        address = (log_entry.get("address") or "").lower()
        topics = log_entry.get("topics") or []
        data = log_entry.get("data") or "0x"
        if len(topics) < 3:
            return None
        from_addr = _hex_topic_to_addr(topics[1]).lower()
        to_addr = _hex_topic_to_addr(topics[2]).lower()
        amount_raw = (
            int(data, 16) if isinstance(data, str) and data.startswith("0x") else int(data or 0)
        )
        decimals = token_meta["decimals"]
        amount_human = Decimal(amount_raw) / (Decimal(10) ** decimals)
        block_number = _hex_to_int(log_entry.get("blockNumber", "0x0"))
        tx_hash = (log_entry.get("transactionHash") or "").lower()
        log_index = (
            int(log_entry.get("logIndex", "0x0"), 16)
            if isinstance(log_entry.get("logIndex"), str)
            else int(log_entry.get("logIndex") or 0)
        )
        ts = block_ts_lookup.get(block_number) or datetime.now(tz=UTC)
        return {
            "time": ts,
            "chain": "polygon",
            "block_number": block_number,
            "tx_hash": tx_hash,
            "log_index": log_index,
            "token_address": address,
            "token_symbol": token_meta["symbol"],
            "from_address": from_addr,
            "to_address": to_addr,
            "amount_raw": amount_raw,
            "amount_human": amount_human,
            "decimals": decimals,
        }
    except Exception as e:
        log.warning("log_parse_failed", err=str(e), entry=str(log_entry)[:200])
        return None


async def rpc_call(client: httpx.AsyncClient, rpc_url: str, method: str, params: list) -> Any:
    for attempt in range(3):
        try:
            r = await client.post(
                rpc_url,
                json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                timeout=20.0,
            )
            r.raise_for_status()
            payload = r.json()
            if "error" in payload:
                raise RuntimeError(f"rpc error: {payload['error']}")
            return payload.get("result")
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(0.5 * (attempt + 1))


async def fetch_logs_in_range(
    client: httpx.AsyncClient, rpc_url: str, from_block: int, to_block: int, tokens: list[dict]
) -> list[dict]:
    addr_filter = [t["address"].lower() for t in tokens]
    return await rpc_call(
        client,
        rpc_url,
        "eth_getLogs",
        [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": addr_filter,
                "topics": [TRANSFER_TOPIC],
            }
        ],
    )


async def fetch_block_timestamp(
    client: httpx.AsyncClient, rpc_url: str, block_number: int
) -> datetime:
    block = await rpc_call(client, rpc_url, "eth_getBlockByNumber", [hex(block_number), False])
    ts_hex = (block or {}).get("timestamp", "0x0")
    return datetime.fromtimestamp(_hex_to_int(ts_hex), tz=UTC)


async def backfill_wallet(conn, address: str, first_block: int | None = None) -> None:
    await upsert_wallets(
        conn,
        [
            {
                "address": address.lower(),
                "chain": "polygon",
                "first_seen": datetime.now(tz=UTC) if first_block is None else None,
                "last_seen": datetime.now(tz=UTC),
            }
        ],
    )


async def chain_loop(pool: PgPool, bus: BusProducer, rpc_url: str, tokens: list[dict]) -> None:
    state_file = "/var/tmp/kro_chain_state.json"
    last_block = None
    if os.path.exists(state_file):
        try:
            last_block = json.loads(open(state_file).read()).get("last_block")
        except Exception:
            last_block = None
    async with httpx.AsyncClient() as client:
        while True:
            try:
                head_hex = await rpc_call(client, rpc_url, "eth_blockNumber", [])
                head = _hex_to_int(head_hex)
                if last_block is None:
                    last_block = max(head - SAFE_REORG_DEPTH, 1)
                    log.info("chain_starting", head=head, last_block=last_block)
                cursor = last_block + 1
                while cursor <= head - SAFE_REORG_DEPTH:
                    chunk_end = min(cursor + MAX_BLOCK_RANGE - 1, head - SAFE_REORG_DEPTH)
                    raw_logs = await fetch_logs_in_range(client, rpc_url, cursor, chunk_end, tokens)
                    if raw_logs:
                        blocks = sorted(
                            {_hex_to_int(e.get("blockNumber", "0x0")) for e in raw_logs}
                        )
                        ts_lookup: dict[int, datetime] = {}
                        for b in blocks:
                            try:
                                ts_lookup[b] = await fetch_block_timestamp(client, rpc_url, b)
                            except Exception as e:
                                log.warning("block_ts_failed", block=b, err=str(e))
                                ts_lookup[b] = datetime.now(tz=UTC)
                        transfers: list[dict] = []
                        for entry in raw_logs:
                            token_addr = (entry.get("address") or "").lower()
                            token_meta = next(
                                (t for t in tokens if t["address"].lower() == token_addr), None
                            )
                            if not token_meta:
                                continue
                            t = _parse_log(entry, token_meta, ts_lookup)
                            if t:
                                transfers.append(t)
                        if transfers:
                            async with pool.acquire() as conn:
                                await insert_chain_transfers(conn, transfers)
                            for t in transfers:
                                await bus.send(TOPICS["chain_transfers"], t, key=t["to_address"])
                            seen = {t["from_address"] for t in transfers} | {
                                t["to_address"] for t in transfers
                            }
                            seen.discard("0x0000000000000000000000000000000000000000")
                            seen.discard("0x000000000000000000000000000000000000dead")
                            if seen:
                                now = datetime.now(tz=UTC)
                                async with pool.acquire() as conn:
                                    await upsert_wallets(
                                        conn,
                                        [
                                            {"address": a, "last_seen": now, "chain": "polygon"}
                                            for a in seen
                                        ],
                                    )
                            log.info(
                                "chain_chunk_ingested",
                                from_block=cursor,
                                to_block=chunk_end,
                                transfers=len(transfers),
                            )
                    cursor = chunk_end + 1
                    last_block = chunk_end
                    with contextlib.suppress(Exception):
                        open(state_file, "w").write(json.dumps({"last_block": last_block}))
            except Exception as e:
                log.error("chain_loop_error", err=str(e))
            await asyncio.sleep(POLL_INTERVAL)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    tokens = []
    for t in DEFAULT_TOKENS:
        tokens.append(t)
    extra = os.environ.get("CHAIN_COLLECTOR_TOKENS")
    if extra:
        try:
            for t in json.loads(extra):
                tokens.append(
                    {
                        "address": t["address"],
                        "symbol": t.get("symbol", "?"),
                        "decimals": int(t.get("decimals", 6)),
                    }
                )
        except Exception as e:
            log.warning("extra_tokens_parse_failed", err=str(e))

    pool = PgPool()
    await pool.connect()
    bus = BusProducer(settings.redpanda_brokers, client_id="chain-collector")
    await bus.start()
    log.info("starting_chain_collector", tokens=len(t), rpc=settings.polygon_rpc_https)

    stop = asyncio.Event()

    def _sig(*_):
        stop.set()

    for s in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            asyncio.get_running_loop().add_signal_handler(s, _sig)

    task = asyncio.create_task(chain_loop(pool, bus, settings.polygon_rpc_https, tokens))
    try:
        await stop.wait()
    finally:
        task.cancel()
        await bus.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
