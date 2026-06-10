"""聚合 tick 查询层 — 供 paper wallet / 回测引擎对接。"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from .storage import MarketStore

STALE_MS_DEFAULT = 30_000
# 毫秒时间戳合理上界（约 2100 年），用于识别历史错位列
_MAX_VALID_MS = 4_000_000_000_000


def _depth_time(row: dict[str, Any]) -> int:
    """兼容旧数据：snapshot_time / last_update_id 曾写入错位。"""
    st = int(row["snapshot_time"])
    lid = row.get("last_update_id")
    if st > _MAX_VALID_MS and lid and int(lid) <= _MAX_VALID_MS:
        return int(lid)
    return st


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + (
        f"{ms % 1000:03d}Z"
    )


def _parse_depth(raw: str | None, levels: int) -> list[dict[str, float]]:
    if not raw:
        return []
    data = json.loads(raw)
    out: list[dict[str, float]] = []
    for item in data[:levels]:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            out.append({"price": float(item[0]), "qty": float(item[1])})
        elif isinstance(item, dict):
            out.append({"price": float(item["price"]), "qty": float(item["qty"])})
    return out


def _nearest(
    store: MarketStore,
    table: str,
    sym_col: str,
    symbol: str,
    time_col: str,
    ts: int,
) -> dict[str, Any] | None:
    rows = store.query(
        f"SELECT * FROM {table} WHERE {sym_col}=? AND {time_col}<=? "
        f"ORDER BY {time_col} DESC LIMIT 1",
        [symbol.upper(), ts],
    )
    return rows[0] if rows else None


def _latest(store: MarketStore, table: str, sym_col: str, symbol: str, time_col: str) -> dict[str, Any] | None:
    if table == "depth_snapshots":
        rows = store.query(
            "SELECT * FROM depth_snapshots WHERE symbol=? "
            "ORDER BY CASE WHEN snapshot_time > ? THEN last_update_id ELSE snapshot_time END DESC "
            "LIMIT 1",
            [symbol.upper(), _MAX_VALID_MS],
        )
        return rows[0] if rows else None
    rows = store.query(
        f"SELECT * FROM {table} WHERE {sym_col}=? ORDER BY {time_col} DESC LIMIT 1",
        [symbol.upper()],
    )
    return rows[0] if rows else None


def _nearest_depth(store: MarketStore, symbol: str, ts: int) -> dict[str, Any] | None:
    rows = store.query(
        "SELECT * FROM depth_snapshots WHERE symbol=? AND "
        "(CASE WHEN snapshot_time > ? THEN last_update_id ELSE snapshot_time END) <= ? "
        "ORDER BY CASE WHEN snapshot_time > ? THEN last_update_id ELSE snapshot_time END DESC "
        "LIMIT 1",
        [symbol.upper(), _MAX_VALID_MS, ts, _MAX_VALID_MS],
    )
    return rows[0] if rows else None


def _funding_at(store: MarketStore, symbol: str, ts: int) -> tuple[float | None, int | None, str]:
    """历史时刻取已结算费率；实时回退 mark_prices 预测费率。"""
    row = _nearest(store, "funding_rates", "symbol", symbol, "funding_time", ts)
    if row:
        return row["funding_rate"], row["funding_time"], "funding_rates"
    return None, None, "funding_rates"


def _volume_1m_at(store: MarketStore, symbol: str, ts: int) -> float | None:
    rows = store.query(
        "SELECT volume FROM klines WHERE symbol=? AND interval='1m' AND open_time<=? "
        "ORDER BY open_time DESC LIMIT 1",
        [symbol.upper(), ts],
    )
    return rows[0]["volume"] if rows else None


def _last_price_at(store: MarketStore, symbol: str, ts: int) -> tuple[float | None, int | None]:
    tp = _nearest(store, "ticker_price", "symbol", symbol, "event_time", ts)
    if tp:
        return tp["price"], tp["event_time"]
    t24 = _nearest(store, "ticker_24h", "symbol", symbol, "event_time", ts)
    if t24 and t24.get("last_price"):
        return t24["last_price"], t24["event_time"]
    return None, None


def _assemble(
    symbol: str,
    mark: dict[str, Any] | None,
    book: dict[str, Any] | None,
    *,
    funding_rate: float | None = None,
    funding_time: int | None = None,
    funding_source: str | None = None,
    depth: dict[str, Any] | None = None,
    depth_levels: int = 20,
    last_price: float | None = None,
    last_price_time: int | None = None,
    volume_1m: float | None = None,
    now_ms: int | None = None,
    stale_ms: int = STALE_MS_DEFAULT,
    include_meta: bool = True,
    matched_time: int | None = None,
) -> dict[str, Any] | None:
    if mark is None and book is None:
        return None

    sym = symbol.upper()
    core_times: list[int] = []
    if mark:
        core_times.append(int(mark["event_time"]))
    if book:
        core_times.append(int(book["event_time"]))

    if not core_times:
        return None

    primary_time = max(core_times) if matched_time is None else matched_time
    now = now_ms if now_ms is not None else int(time.time() * 1000)

    mark_age = (now - int(mark["event_time"])) if mark else None
    book_age = (now - int(book["event_time"])) if book else None
    depth_age = (now - _depth_time(depth)) if depth else None

    is_mark_stale = mark_age is not None and mark_age > stale_ms
    is_book_stale = book_age is not None and book_age > stale_ms
    is_stale = is_mark_stale or is_book_stale

    if funding_rate is None and mark:
        funding_rate = mark.get("funding_rate")
        funding_source = funding_source or "mark_prices"

    tick: dict[str, Any] = {
        "symbol": sym,
        "mark_price": float(mark["mark_price"]) if mark else None,
        "index_price": float(mark["index_price"]) if mark and mark.get("index_price") is not None else None,
        "last_price": last_price,
        "best_bid": float(book["bid_price"]) if book else None,
        "best_ask": float(book["ask_price"]) if book else None,
        "bid_qty": float(book["bid_qty"]) if book else None,
        "ask_qty": float(book["ask_qty"]) if book else None,
        "funding_rate": float(funding_rate) if funding_rate is not None else 0.0,
        "next_funding_time": int(mark["next_funding_time"]) if mark and mark.get("next_funding_time") else None,
        "bid_depth": _parse_depth(depth["bids"], depth_levels) if depth else [],
        "ask_depth": _parse_depth(depth["asks"], depth_levels) if depth else [],
        "volume_1m": volume_1m,
        "event_time": primary_time,
        "timestamp": _ms_to_iso(primary_time),
    }

    if include_meta:
        tick.update({
            "age_ms": now - primary_time,
            "mark_age_ms": mark_age,
            "book_age_ms": book_age,
            "depth_age_ms": depth_age,
            "is_mark_stale": is_mark_stale,
            "is_book_stale": is_book_stale,
            "is_stale": is_stale,
        })
        if funding_source:
            tick["funding_source"] = funding_source
        if funding_time is not None:
            tick["funding_time"] = funding_time

    return tick


def tick_latest(
    store: MarketStore,
    symbol: str,
    *,
    include_depth: bool = False,
    depth_levels: int = 20,
    stale_ms: int = STALE_MS_DEFAULT,
) -> dict[str, Any] | None:
    sym = symbol.upper()
    mark = _latest(store, "mark_prices", "symbol", sym, "event_time")
    book = _latest(store, "book_tickers", "symbol", sym, "event_time")
    depth = _latest(store, "depth_snapshots", "symbol", sym, "snapshot_time") if include_depth else None
    last_price, lp_time = _last_price_at(store, sym, int(time.time() * 1000))
    vol = _volume_1m_at(store, sym, int(time.time() * 1000))
    return _assemble(
        sym, mark, book,
        depth=depth, depth_levels=depth_levels,
        last_price=last_price, last_price_time=lp_time,
        volume_1m=vol, stale_ms=stale_ms,
    )


def tick_at(
    store: MarketStore,
    symbol: str,
    timestamp: int,
    *,
    include_depth: bool = False,
    depth_levels: int = 20,
    tolerance_ms: int = 60_000,
) -> dict[str, Any] | None:
    sym = symbol.upper()
    mark = _nearest(store, "mark_prices", "symbol", sym, "event_time", timestamp)
    book = _nearest(store, "book_tickers", "symbol", sym, "event_time", timestamp)
    depth = _nearest_depth(store, sym, timestamp) if include_depth else None

    if mark is None and book is None:
        return None

    times: list[tuple[str, int]] = []
    if mark:
        times.append(("mark_prices", int(mark["event_time"])))
    if book:
        times.append(("book_tickers", int(book["event_time"])))
    if depth:
        times.append(("depth_snapshots", _depth_time(depth)))

    anchor_name, anchor_time = max(times, key=lambda x: x[1])
    if timestamp - anchor_time > tolerance_ms:
        return None

    fr, ft, fs = _funding_at(store, sym, timestamp)
    if fr is None and mark:
        fr = mark.get("funding_rate")
        fs = "mark_prices"

    last_price, lp_time = _last_price_at(store, sym, timestamp)
    vol = _volume_1m_at(store, sym, timestamp)

    tick = _assemble(
        sym, mark, book,
        funding_rate=fr, funding_time=ft, funding_source=fs,
        depth=depth, depth_levels=depth_levels,
        last_price=last_price, last_price_time=lp_time,
        volume_1m=vol,
        now_ms=timestamp,
        include_meta=True,
        matched_time=anchor_time,
    )
    if tick:
        tick["matched_time"] = anchor_time
        tick["time_delta_ms"] = timestamp - anchor_time
        tick["sources"] = {name: t for name, t in times}
        tick["primary_source"] = anchor_name
        tick["is_stale"] = tick["time_delta_ms"] > tolerance_ms
    return tick


def ticks_latest(
    store: MarketStore,
    symbols: list[str],
    *,
    include_depth: bool = False,
    depth_levels: int = 20,
    stale_ms: int = STALE_MS_DEFAULT,
) -> dict[str, Any]:
    ticks: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for sym in symbols:
        t = tick_latest(store, sym, include_depth=include_depth, depth_levels=depth_levels, stale_ms=stale_ms)
        if t:
            ticks[sym.upper()] = t
        else:
            missing.append(sym.upper())
    return {
        "ticks": ticks,
        "missing": missing,
        "server_time": int(time.time() * 1000),
    }


def ticks_range(
    store: MarketStore,
    symbol: str,
    start_time: int,
    end_time: int,
    interval: str = "1h",
    *,
    include_depth: bool = False,
    depth_levels: int = 20,
    limit: int = 5000,
    offset: int = 0,
) -> dict[str, Any]:
    sym = symbol.upper()
    rows = store.query(
        "SELECT open_time, open, high, low, close, volume, close_time FROM klines "
        "WHERE symbol=? AND interval=? AND open_time>=? AND open_time<=? "
        "ORDER BY open_time ASC LIMIT ? OFFSET ?",
        [sym, interval, start_time, end_time, limit, offset],
    )
    total_row = store.query(
        "SELECT COUNT(*) AS total FROM klines "
        "WHERE symbol=? AND interval=? AND open_time>=? AND open_time<=?",
        [sym, interval, start_time, end_time],
    )
    total = total_row[0]["total"] if total_row else 0

    data: list[dict[str, Any]] = []
    for bar in rows:
        ts = int(bar["close_time"])
        ot = int(bar["open_time"])
        mark = _nearest(store, "mark_prices", "symbol", sym, "event_time", ts)
        book = _nearest(store, "book_tickers", "symbol", sym, "event_time", ts)

        # 历史回填阶段：tick 流可能尚未覆盖，回退 K 线 mark/index，最后用 close 代理
        if mark is None:
            mk = store.query(
                "SELECT close FROM mark_price_klines "
                "WHERE symbol=? AND interval=? AND open_time=?",
                [sym, interval, ot],
            )
            ix = store.query(
                "SELECT close FROM index_price_klines "
                "WHERE pair=? AND interval=? AND open_time=?",
                [sym, interval, ot],
            )
            mark = {
                "mark_price": mk[0]["close"] if mk else float(bar["close"]),
                "index_price": ix[0]["close"] if ix else None,
                "funding_rate": None,
                "next_funding_time": None,
                "event_time": ts,
            }

        depth = _nearest_depth(store, sym, ts) if include_depth else None
        fr, ft, fs = _funding_at(store, sym, ot)
        if fr is None and mark:
            fr = mark.get("funding_rate")
            fs = "mark_prices"
        last_price, lp_time = _last_price_at(store, sym, ts)
        if last_price is None:
            last_price = float(bar["close"])
            lp_time = ts

        tick = _assemble(
            sym, mark, book,
            funding_rate=fr, funding_time=ft, funding_source=fs,
            depth=depth, depth_levels=depth_levels,
            last_price=last_price, last_price_time=lp_time,
            volume_1m=bar.get("volume"),
            now_ms=ts,
            include_meta=True,
            matched_time=ts,
        )
        if tick:
            tick["open_time"] = ot
            tick["close_time"] = ts
            tick["open"] = bar["open"]
            tick["high"] = bar["high"]
            tick["low"] = bar["low"]
            tick["close"] = bar["close"]
            tick["volume"] = bar["volume"]
            data.append(tick)

    return {
        "symbol": sym,
        "interval": interval,
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data,
    }


def backtest_bars(
    store: MarketStore,
    symbol: str,
    interval: str,
    start_time: int,
    end_time: int,
    *,
    limit: int = 5000,
    offset: int = 0,
) -> dict[str, Any]:
    sym = symbol.upper()
    rows = store.query(
        "SELECT k.open_time, k.open, k.high, k.low, k.close, k.volume, k.close_time, "
        "       m.close AS mark_close, i.close AS index_close "
        "FROM klines k "
        "LEFT JOIN mark_price_klines m "
        "  ON k.symbol=m.symbol AND k.interval=m.interval AND k.open_time=m.open_time "
        "LEFT JOIN index_price_klines i "
        "  ON k.symbol=i.pair AND k.interval=i.interval AND k.open_time=i.open_time "
        "WHERE k.symbol=? AND k.interval=? AND k.open_time>=? AND k.open_time<=? "
        "ORDER BY k.open_time ASC LIMIT ? OFFSET ?",
        [sym, interval, start_time, end_time, limit, offset],
    )
    total_row = store.query(
        "SELECT COUNT(*) AS total FROM klines "
        "WHERE symbol=? AND interval=? AND open_time>=? AND open_time<=?",
        [sym, interval, start_time, end_time],
    )
    total = total_row[0]["total"] if total_row else 0

    data: list[dict[str, Any]] = []
    for bar in rows:
        ts = int(bar["close_time"])
        book = _nearest(store, "book_tickers", "symbol", sym, "event_time", ts)
        fr, ft, _ = _funding_at(store, sym, int(bar["open_time"]))
        mark_px = bar.get("mark_close")
        if mark_px is None:
            mark_px = float(bar["close"])
        item: dict[str, Any] = {
            "open_time": int(bar["open_time"]),
            "close_time": ts,
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
            "mark_price": mark_px,
            "index_price": bar.get("index_close"),
            "mark_source": "mark_price_klines" if bar.get("mark_close") else "klines_close",
            "funding_rate": fr,
            "funding_time": ft,
            "best_bid": float(book["bid_price"]) if book else None,
            "best_ask": float(book["ask_price"]) if book else None,
        }
        data.append(item)

    return {
        "symbol": sym,
        "interval": interval,
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data,
    }
