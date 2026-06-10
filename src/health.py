"""服务健康与数据流可观测性。"""
from __future__ import annotations

import time
from typing import Any

from .storage import MarketStore
from .tick import STALE_MS_DEFAULT, _MAX_VALID_MS

_STREAMS = ("mark", "book", "depth", "trade")


def _age(now_ms: int, last_ms: int | None) -> int | None:
    if last_ms is None:
        return None
    return now_ms - int(last_ms)


def stream_status(store: MarketStore, symbols: list[str]) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    per_symbol: dict[str, dict[str, Any]] = {}

    for sym in symbols:
        s = sym.upper()
        mark_row = store.query(
            "SELECT MAX(event_time) AS t FROM mark_prices WHERE symbol=?", [s],
        )
        book_row = store.query(
            "SELECT MAX(event_time) AS t FROM book_tickers WHERE symbol=?", [s],
        )
        depth_row = store.query(
            "SELECT MAX(CASE WHEN snapshot_time > ? THEN last_update_id ELSE snapshot_time END) AS t "
            "FROM depth_snapshots WHERE symbol=?",
            [_MAX_VALID_MS, s],
        )
        trade_row = store.query(
            "SELECT MAX(trade_time) AS t FROM agg_trades WHERE symbol=?", [s],
        )

        last = {
            "mark": mark_row[0]["t"] if mark_row else None,
            "book": book_row[0]["t"] if book_row else None,
            "depth": depth_row[0]["t"] if depth_row else None,
            "trade": trade_row[0]["t"] if trade_row else None,
        }
        ages = {k: _age(now_ms, last[k]) for k in _STREAMS}
        per_symbol[s] = {
            "last_event_ms": last,
            "age_ms": ages,
            "is_mark_stale": ages["mark"] is not None and ages["mark"] > STALE_MS_DEFAULT,
            "is_book_stale": ages["book"] is not None and ages["book"] > STALE_MS_DEFAULT,
            "is_depth_stale": ages["depth"] is not None and ages["depth"] > 60_000,
        }

    return {
        "server_time": now_ms,
        "stale_threshold_ms": STALE_MS_DEFAULT,
        "symbols": per_symbol,
    }
