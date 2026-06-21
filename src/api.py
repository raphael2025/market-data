from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .config import Config
from .rate_limit import get_futures_data_limiter, get_limiter
from .storage import ALL_TABLES, MarketStore
from . import tick as tick_svc
from .health import stream_status

# 每张表用于分页排序的时间戳列。
# 必须存在且为 INTEGER — 由 _resolve_order_col 校验，避免 SQL 注入。
TABLE_TIME_COL: dict[str, str] = {
    "klines": "open_time",
    "mark_price_klines": "open_time",
    "index_price_klines": "open_time",
    "continuous_klines": "open_time",
    "kline_updates": "event_time",
    "agg_trades": "trade_time",
    "trades": "trade_time",
    "mark_prices": "event_time",
    "book_tickers": "event_time",
    "ticker_price": "event_time",
    "ticker_24h": "event_time",
    "ticker_snapshots": "event_time",
    "depth_snapshots": "snapshot_time",
    "depth_updates": "event_time",
    "depth_gaps": "gap_start_ms",
    "open_interest": "event_time",
    "open_interest_hist": "event_time",
    "funding_rates": "funding_time",
    "funding_info": "snapshot_time",
    "long_short_ratio": "event_time",
    "basis": "event_time",
    "liquidations": "event_time",
    "insurance_balance": "snapshot_time",
    "delivery_prices": "delivery_time",
    "exchange_info": "snapshot_time",
}

# 整数类型列名集合（PRAGMA table_info 返回的 type 字段）
_INT_TYPES = {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"}


def _resolve_order_col(store: MarketStore, table: str, time_col: str | None) -> str:
    """解析 ORDER BY 列名。

    优先使用显式传入的 time_col；若未传入则从 PRAGMA table_info 自动探测
    （跳过 'symbol' / 'pair'，挑首个 INTEGER 列）。

    列名必须在表 schema 里存在 — 否则抛 400，杜绝任何注入路径。
    """
    if table not in ALL_TABLES:
        raise HTTPException(400, f"未知表: {table}")
    cols = store.query(f"PRAGMA table_info({table})")
    col_map = {c["name"]: (c["type"] or "").upper() for c in cols}
    if not col_map:
        raise HTTPException(500, f"无法读取表 {table} 的 schema")

    if time_col:
        if time_col not in col_map:
            raise HTTPException(
                400, f"表 {table} 不存在列 {time_col}，可选: {list(col_map)}"
            )
        return time_col

    # 自动探测：排除 'symbol' / 'pair'，取首个 INTEGER
    for name, ctype in col_map.items():
        if name in ("symbol", "pair"):
            continue
        if any(t in ctype for t in _INT_TYPES):
            return name
    # 兜底：取第一个列名（极端情况，比如全 TEXT 表）
    return next(iter(col_map))


def _paginate(
    store: MarketStore,
    table: str,
    where: str = "",
    params: list[Any] | None = None,
    order: str = "DESC",
    limit: int = 1000,
    offset: int = 0,
    time_col: str | None = None,
) -> dict:
    params = list(params or [])
    count_sql = f"SELECT COUNT(*) AS total FROM {table} {where}"
    total = store.query(count_sql, params)[0]["total"]
    order_col = _resolve_order_col(store, table, time_col)
    data_sql = (
        f"SELECT * FROM {table} {where} "
        f"ORDER BY {order_col} {order} LIMIT ? OFFSET ?"
    )
    rows = store.query(data_sql, params + [limit, offset])
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


def create_app(config: Config, store: MarketStore) -> FastAPI:
    app = FastAPI(
        title="Market Data API",
        description="BTC/ETH/SOL 币安合约本地全量行情 — 无请求限制，仅供本地研究",
        version="2.2.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get("/health")
    def health():
        streams = stream_status(store, config.symbols)
        l2_ok = streams.get("l2_open_gaps_total", 0) == 0
        trade_ok = all(
            not s.get("is_trade_stale", False)
            for s in streams.get("symbols", {}).values()
        )
        status = "ok" if l2_ok and trade_ok else "degraded"
        return {
            "status": status,
            "symbols": config.symbols,
            "rate_limit": {
                **get_limiter().snapshot(),
                "futures_data": get_futures_data_limiter().snapshot(),
            },
            "l2": {
                "snapshot_interval_s": config.l2_snapshot_interval,
                "snapshot_limit": config.l2_snapshot_limit,
                "open_gaps": streams.get("l2_open_gaps_total", 0),
            },
            "streams": streams,
        }

    @app.get("/tables")
    def tables():
        return store.table_counts()

    @app.get("/tables/{table}/schema")
    def table_schema(table: str):
        if table not in ALL_TABLES:
            raise HTTPException(400, f"未知表，可选: {ALL_TABLES}")
        rows = store.query(f"PRAGMA table_info({table})")
        return {"table": table, "columns": rows}

    # ── K 线 ──

    @app.get("/v1/klines")
    def klines(
        symbol: str,
        interval: str = "1h",
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = Query(1000, description="无上限，本地研究用途"),
        offset: int = 0,
    ):
        where, params = "WHERE symbol=? AND interval=?", [symbol.upper(), interval]
        if start_time:
            where += " AND open_time>=?"; params.append(start_time)
        if end_time:
            where += " AND open_time<=?"; params.append(end_time)
        return _paginate(
            store, "klines", where, params, limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["klines"],
        )

    @app.get("/v1/mark-price-klines")
    def mark_price_klines(symbol: str, interval: str = "1h", limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "mark_price_klines",
            "WHERE symbol=? AND interval=?", [symbol.upper(), interval],
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["mark_price_klines"],
        )

    @app.get("/v1/index-price-klines")
    def index_price_klines(pair: str, interval: str = "1h", limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "index_price_klines",
            "WHERE pair=? AND interval=?", [pair.upper(), interval],
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["index_price_klines"],
        )

    @app.get("/v1/continuous-klines")
    def continuous_klines(
        pair: str, contract_type: str = "PERPETUAL", interval: str = "1h",
        limit: int = 1000, offset: int = 0,
    ):
        return _paginate(
            store, "continuous_klines",
            "WHERE pair=? AND contract_type=? AND interval=?",
            [pair.upper(), contract_type, interval],
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["continuous_klines"],
        )

    @app.get("/v1/kline-updates")
    def kline_updates(
        symbol: str, interval: str = "1m",
        start_time: int | None = None, limit: int = 5000, offset: int = 0,
    ):
        where, params = "WHERE symbol=? AND interval=?", [symbol.upper(), interval]
        if start_time:
            where += " AND event_time>=?"; params.append(start_time)
        return _paginate(
            store, "kline_updates", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["kline_updates"],
        )

    # ── 成交 ──

    @app.get("/v1/agg-trades")
    def agg_trades(symbol: str, start_time: int | None = None, limit: int = 5000, offset: int = 0):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if start_time:
            where += " AND trade_time>=?"; params.append(start_time)
        return _paginate(
            store, "agg_trades", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["agg_trades"],
        )

    @app.get("/v1/trades")
    def trades(symbol: str, start_time: int | None = None, limit: int = 5000, offset: int = 0):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if start_time:
            where += " AND trade_time>=?"; params.append(start_time)
        return _paginate(
            store, "trades", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["trades"],
        )

    # ── 价格 / 行情 ──

    @app.get("/v1/mark-price")
    def mark_price(symbol: str, limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "mark_prices", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["mark_prices"],
        )

    @app.get("/v1/mark-price/latest")
    def mark_price_latest(symbol: str):
        rows = store.query(
            "SELECT * FROM mark_prices WHERE symbol=? ORDER BY event_time DESC LIMIT 1",
            [symbol.upper()],
        )
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    @app.get("/v1/book-ticker")
    def book_ticker(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "book_tickers", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["book_tickers"],
        )

    @app.get("/v1/book-ticker/latest")
    def book_ticker_latest(symbol: str):
        rows = store.query(
            "SELECT * FROM book_tickers WHERE symbol=? ORDER BY event_time DESC LIMIT 1",
            [symbol.upper()],
        )
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    @app.get("/v1/ticker-price")
    def ticker_price(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "ticker_price", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["ticker_price"],
        )

    @app.get("/v1/ticker/24h")
    def ticker_24h(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "ticker_24h", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["ticker_24h"],
        )

    @app.get("/v1/ticker/24h/latest")
    def ticker_24h_latest(symbol: str):
        rows = store.query(
            "SELECT * FROM ticker_24h WHERE symbol=? ORDER BY event_time DESC LIMIT 1",
            [symbol.upper()],
        )
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    @app.get("/v1/ticker/snapshots")
    def ticker_snapshots(
        symbol: str, event_type: str | None = None, limit: int = 1000, offset: int = 0,
    ):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if event_type:
            where += " AND event_type=?"; params.append(event_type)
        return _paginate(
            store, "ticker_snapshots", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["ticker_snapshots"],
        )

    # ── 深度 ──

    @app.get("/v1/depth/snapshots")
    def depth_snapshots(symbol: str, limit: int = 500, offset: int = 0):
        return _paginate(
            store, "depth_snapshots", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["depth_snapshots"],
        )

    @app.get("/v1/depth/snapshots/latest")
    def depth_latest(symbol: str):
        rows = store.query(
            "SELECT * FROM depth_snapshots WHERE symbol=? "
            "ORDER BY CASE WHEN snapshot_time > 4000000000000 THEN last_update_id "
            "ELSE snapshot_time END DESC LIMIT 1",
            [symbol.upper()],
        )
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    @app.get("/v1/depth/updates")
    def depth_updates(
        symbol: str, start_time: int | None = None, limit: int = 10000, offset: int = 0,
    ):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if start_time:
            where += " AND event_time>=?"; params.append(start_time)
        return _paginate(
            store, "depth_updates", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["depth_updates"],
        )

    # ── 持仓 / 资金费率 ──

    @app.get("/v1/open-interest")
    def open_interest(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "open_interest", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["open_interest"],
        )

    @app.get("/v1/open-interest/latest")
    def open_interest_latest(symbol: str):
        rows = store.query(
            "SELECT * FROM open_interest WHERE symbol=? ORDER BY event_time DESC LIMIT 1",
            [symbol.upper()],
        )
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    @app.get("/v1/open-interest/history")
    def open_interest_hist(symbol: str, period: str = "1h", limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "open_interest_hist",
            "WHERE symbol=? AND period=?", [symbol.upper(), period],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["open_interest_hist"],
        )

    @app.get("/v1/funding-rates")
    def funding_rates(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "funding_rates", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["funding_rates"],
        )

    @app.get("/v1/funding-info")
    def funding_info(symbol: str | None = None, limit: int = 1000, offset: int = 0):
        where, params = "", []
        if symbol:
            where, params = "WHERE symbol=?", [symbol.upper()]
        return _paginate(
            store, "funding_info", where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["funding_info"],
        )

    # ── 多空比 / 基差 / 爆仓 ──

    @app.get("/v1/long-short-ratio")
    def long_short_ratio(
        symbol: str, data_type: str = "global_account", period: str = "1h",
        limit: int = 5000, offset: int = 0,
    ):
        return _paginate(
            store, "long_short_ratio",
            "WHERE symbol=? AND data_type=? AND period=?",
            [symbol.upper(), data_type, period],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["long_short_ratio"],
        )

    @app.get("/v1/basis")
    def basis(
        pair: str = "BTCUSDT", contract_type: str = "PERPETUAL", period: str = "1h",
        limit: int = 5000, offset: int = 0,
    ):
        return _paginate(
            store, "basis",
            "WHERE pair=? AND contract_type=? AND period=?",
            [pair.upper(), contract_type, period],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["basis"],
        )

    @app.get("/v1/liquidations")
    def liquidations(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "liquidations", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["liquidations"],
        )

    # ── 系统 / 元数据 ──

    @app.get("/v1/insurance-balance")
    def insurance_balance(limit: int = 500, offset: int = 0):
        return _paginate(
            store, "insurance_balance", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["insurance_balance"],
        )

    @app.get("/v1/delivery-prices")
    def delivery_prices(pair: str, limit: int = 500, offset: int = 0):
        return _paginate(
            store, "delivery_prices", "WHERE pair=?", [pair.upper()],
            order="DESC", limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["delivery_prices"],
        )

    @app.get("/v1/exchange-info")
    def exchange_info(limit: int = 100, offset: int = 0):
        return _paginate(
            store, "exchange_info", order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL["exchange_info"],
        )

    @app.get("/v1/exchange-info/latest")
    def exchange_info_latest():
        rows = store.query("SELECT * FROM exchange_info ORDER BY snapshot_time DESC LIMIT 1")
        if not rows:
            raise HTTPException(404, "无数据")
        return rows[0]

    # ── 聚合 tick（paper wallet 对接）──

    @app.get("/v1/tick/latest")
    def tick_latest(
        symbol: str,
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
    ):
        result = tick_svc.tick_latest(
            store, symbol, include_depth=include_depth, depth_levels=depth_levels,
        )
        if not result:
            raise HTTPException(404, "无数据")
        return result

    @app.get("/v1/ticks/latest")
    def ticks_latest(
        symbols: str = Query(..., description="逗号分隔，如 BTCUSDT,ETHUSDT"),
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
    ):
        sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
        if not sym_list:
            raise HTTPException(400, "symbols 不能为空")
        result = tick_svc.ticks_latest(
            store, sym_list, include_depth=include_depth, depth_levels=depth_levels,
        )
        if not result["ticks"]:
            raise HTTPException(404, "无数据")
        return result

    @app.get("/v1/tick/at")
    def tick_at(
        symbol: str,
        timestamp: int = Query(..., description="Unix 毫秒"),
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
        tolerance_ms: int = Query(60_000, ge=0),
    ):
        result = tick_svc.tick_at(
            store, symbol, timestamp,
            include_depth=include_depth, depth_levels=depth_levels,
            tolerance_ms=tolerance_ms,
        )
        if not result:
            raise HTTPException(404, "无数据或超出 tolerance_ms")
        return result

    @app.get("/v1/ticks/range")
    def ticks_range(
        symbol: str,
        start_time: int = Query(..., description="起始 open_time（ms）"),
        end_time: int = Query(..., description="结束 open_time（ms）"),
        interval: str = "1h",
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
        limit: int = Query(5000, ge=1),
        offset: int = 0,
    ):
        return tick_svc.ticks_range(
            store, symbol, start_time, end_time, interval,
            include_depth=include_depth, depth_levels=depth_levels,
            limit=limit, offset=offset,
        )

    @app.get("/v1/backtest/bars")
    def backtest_bars(
        symbol: str,
        interval: str = "1h",
        start_time: int = Query(..., description="起始 open_time（ms）"),
        end_time: int = Query(..., description="结束 open_time（ms）"),
        limit: int = Query(5000, ge=1),
        offset: int = 0,
    ):
        return tick_svc.backtest_bars(
            store, symbol, interval, start_time, end_time,
            limit=limit, offset=offset,
        )

    # paper wallet 兼容路径
    @app.get("/v1/market/tick/{symbol}")
    def market_tick_compat(
        symbol: str,
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
    ):
        return tick_latest(symbol, include_depth=include_depth, depth_levels=depth_levels)

    @app.get("/v1/market/ticks")
    def market_ticks_compat(
        symbols: str = Query(..., description="逗号分隔"),
        include_depth: bool = False,
        depth_levels: int = Query(20, ge=1, le=1000),
    ):
        return ticks_latest(symbols=symbols, include_depth=include_depth, depth_levels=depth_levels)

    # ── 通用查询 ──

    @app.get("/v1/query/{table}")
    def raw_query(
        table: str,
        symbol: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ):
        if table not in ALL_TABLES:
            raise HTTPException(400, f"无效表名: {ALL_TABLES}")
        where, params = "", []
        sym_col = "pair" if table in ("basis", "index_price_klines", "continuous_klines", "delivery_prices") else "symbol"
        if symbol and sym_col in ("symbol", "pair"):
            where = f"WHERE {sym_col}=?"
            params = [symbol.upper()]
        return _paginate(
            store, table, where, params, order="DESC",
            limit=limit, offset=offset,
            time_col=TABLE_TIME_COL.get(table),
        )

    # 兼容旧版路径
    @app.get("/klines")
    def legacy_klines(symbol: str, interval: str = "1h", limit: int = 1000):
        return klines(symbol, interval, limit=limit)["data"]

    @app.get("/trades")
    def legacy_trades(symbol: str, limit: int = 1000):
        return agg_trades(symbol, limit=limit)["data"]

    return app
