"""Curated market selection — keep the signal basket small, liquid, and on-topic."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import structlog

log = structlog.get_logger("curation")

GEO_KEYWORDS = (
    "russia", "ukraine", "china", "taiwan", "iran", "israel", "gaza", "hamas",
    "hezbollah", "putin", "zelensky", "xi jinping", "netanyahu", "biden", "trump",
    "election", "president", "congress", "senate", "nato", "war", "invasion", "ceasefire",
    "sanction", "tariff", "fed", "powell", "interest rate", "recession", "oil",
    "opec", "nuclear", "missile", "korea", "kim", "venezuela", "maduro", "zelenskyy",
)

CATEGORY_HINTS = (
    "geopolitics", "politics", "world", "elections", "conflicts", "policy", "economy",
)

DEFAULT_LIQ_MIN = Decimal("1000")
DEFAULT_VOL_MIN = Decimal("500")
BASKET_FILE = Path(__file__).resolve().parents[3] / "data" / "curated_markets.json"


def is_geopolitical(m: dict) -> bool:
    cat = (m.get("category") or "").lower()
    if any(c in cat for c in CATEGORY_HINTS):
        return True
    q = (m.get("question") or "").lower()
    return any(kw in q for kw in GEO_KEYWORDS)


def passes_liquidity_gate(
    m: dict, min_liq: Decimal = DEFAULT_LIQ_MIN, min_vol: Decimal = DEFAULT_VOL_MIN
) -> bool:
    liq = m.get("liquidity")
    vol = m.get("volume_24h") or m.get("volume_total")
    if liq is None and vol is None:
        return False
    try:
        if liq is not None and Decimal(str(liq)) < min_liq and (
            vol is None or Decimal(str(vol)) < min_vol * 4
        ):
            return False
    except Exception:
        return False
    return True


def select_curated_markets(
    markets: list[dict], basket_name: str, limit: int = 25
) -> list[dict]:
    candidates = [m for m in markets if is_geopolitical(m) and passes_liquidity_gate(m)]
    candidates.sort(
        key=lambda m: (
            -(m.get("liquidity") or Decimal(0)),
            -(m.get("volume_24h") or Decimal(0)),
        )
    )
    chosen = []
    for m in candidates:
        if len(chosen) >= limit:
            break
        m2 = dict(m)
        m2["curated"] = True
        m2["basket"] = basket_name
        chosen.append(m2)
    log.info("curated_selected", n=len(chosen), candidates=len(candidates), total=len(markets))
    return chosen


def build_basket(curated: list[dict]) -> None:
    """Persist the basket selection to data/curated_markets.json for visibility."""
    BASKET_FILE.parent.mkdir(parents=True, exist_ok=True)
    import json
    payload = {
        "basket_name": "geopolitical",
        "n_markets": len(curated),
        "condition_ids": [m["condition_id"] for m in curated],
        "questions": [
            {"condition_id": m["condition_id"], "question": m["question"]} for m in curated
        ],
    }
    BASKET_FILE.write_text(json.dumps(payload, indent=2, default=str))
