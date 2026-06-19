"""KRO common types — the ontology as Pydantic models."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


class Market(BaseModel):
    condition_id: str
    market_id: str | None = None
    question: str
    slug: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    resolution_date: datetime | None = None
    closed: bool = False
    active: bool = True
    liquidity: Decimal | None = None
    open_interest: Decimal | None = None
    volume_24h: Decimal | None = None
    volume_total: Decimal | None = None
    tokens: list[dict[str, Any]] = Field(default_factory=list)
    outcomes: list[str] = Field(default_factory=list)
    curated: bool = False
    basket: str | None = None


class MarketTick(BaseModel):
    time: datetime
    market_id: str
    condition_id: str
    asset_id: str
    outcome: str
    price: Decimal
    probability: Decimal
    size: Decimal | None = None
    hash: str | None = None
    source: str = "clob-ws"


class MarketFill(BaseModel):
    time: datetime
    trade_id: str
    market_id: str
    condition_id: str | None = None
    asset_id: str | None = None
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    notional: Decimal | None = None
    fee: Decimal | None = None
    maker_address: str | None = None
    taker_address: str | None = None


class Wallet(BaseModel):
    address: str
    chain: str = "polygon"
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    polymarket_pnl: Decimal | None = None
    polymarket_volume: Decimal | None = None
    polymarket_trades: int | None = None
    smart_money_score: float | None = None
    cluster_id: str | None = None
    entity_id: str | None = None
    risk_flags: list[str] = Field(default_factory=list)


class ChainTransfer(BaseModel):
    time: datetime
    chain: str = "polygon"
    block_number: int
    tx_hash: str
    log_index: int
    token_address: str
    token_symbol: str | None = None
    from_address: str
    to_address: str
    amount_raw: int
    amount_human: Decimal
    decimals: int


class Entity(BaseModel):
    entity_id: str
    name: str
    type: str
    labels: list[str] = Field(default_factory=list)
    risk_level: str = "unknown"
    source: str = "manual"
    metadata: dict[str, Any] = Field(default_factory=dict)


class Cluster(BaseModel):
    cluster_id: str
    size: int
    method: str
    confidence: float | None = None
    risk_flags: list[str] = Field(default_factory=list)
    canonical_address: str | None = None


class KineticRiskAlert(BaseModel):
    id: str | None = None
    time: datetime
    market_id: str
    cluster_id: str | None = None
    entity_id: str | None = None
    composite_score: float
    state: str = "open"
    market_signal: dict[str, Any] = Field(default_factory=dict)
    flow_signal: dict[str, Any] | None = None
    entity_risk: dict[str, Any] | None = None
    lead_lag: dict[str, Any] | None = None
