from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .config import Config
from .storage import ALL_TABLES, MarketStore
from . import tick as tick_svc


def _paginate(
    store: MarketStore,
    table: str,
    where: str = "",
    params: list[Any] | None = None,
    order: str = "DESC",
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    params = list(params or [])
    count_sql = f"SELECT COUNT(*) AS total FROM {table} {where}"
    total = store.query(count_sql, params)[0]["total"]
    data_sql = f"SELECT * FROM {table} {where} ORDER BY 1 {order} LIMIT ? OFFSET ?"
    rows = store.query(data_sql, params + [limit, offset])
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


def create_app(config: Config, store: MarketStore) -> FastAPI:
    app = FastAPI(
        title="Market Data API",
        description="BTC/ETH/SOL 币安合约本地全量行情 — 无请求限制，仅供本地研究",
        version="2.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get("/health")
    def health():
        return {"status": "ok", "symbols": config.symbols, "rate_limit": None}

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
        return _paginate(store, "klines", where, params, limit=limit, offset=offset)

    @app.get("/v1/mark-price-klines")
    def mark_price_klines(symbol: str, interval: str = "1h", limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "mark_price_klines",
            "WHERE symbol=? AND interval=?", [symbol.upper(), interval],
            limit=limit, offset=offset,
        )

    @app.get("/v1/index-price-klines")
    def index_price_klines(pair: str, interval: str = "1h", limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "index_price_klines",
            "WHERE pair=? AND interval=?", [pair.upper(), interval],
            limit=limit, offset=offset,
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
        )

    @app.get("/v1/kline-updates")
    def kline_updates(
        symbol: str, interval: str = "1m",
        start_time: int | None = None, limit: int = 5000, offset: int = 0,
    ):
        where, params = "WHERE symbol=? AND interval=?", [symbol.upper(), interval]
        if start_time:
            where += " AND event_time>=?"; params.append(start_time)
        return _paginate(store, "kline_updates", where, params, order="DESC", limit=limit, offset=offset)

    # ── 成交 ──

    @app.get("/v1/agg-trades")
    def agg_trades(symbol: str, start_time: int | None = None, limit: int = 5000, offset: int = 0):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if start_time:
            where += " AND trade_time>=?"; params.append(start_time)
        return _paginate(store, "agg_trades", where, params, order="DESC", limit=limit, offset=offset)

    @app.get("/v1/trades")
    def trades(symbol: str, start_time: int | None = None, limit: int = 5000, offset: int = 0):
        where, params = "WHERE symbol=?", [symbol.upper()]
        if start_time:
            where += " AND trade_time>=?"; params.append(start_time)
        return _paginate(store, "trades", where, params, order="DESC", limit=limit, offset=offset)

    # ── 价格 / 行情 ──

    @app.get("/v1/mark-price")
    def mark_price(symbol: str, limit: int = 1000, offset: int = 0):
        return _paginate(
            store, "mark_prices", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
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
        )

    @app.get("/v1/ticker/24h")
    def ticker_24h(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "ticker_24h", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
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
        return _paginate(store, "ticker_snapshots", where, params, order="DESC", limit=limit, offset=offset)

    # ── 深度 ──

    @app.get("/v1/depth/snapshots")
    def depth_snapshots(symbol: str, limit: int = 500, offset: int = 0):
        return _paginate(
            store, "depth_snapshots", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
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
        return _paginate(store, "depth_updates", where, params, order="DESC", limit=limit, offset=offset)

    # ── 持仓 / 资金费率 ──

    @app.get("/v1/open-interest")
    def open_interest(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "open_interest", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
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
        )

    @app.get("/v1/funding-rates")
    def funding_rates(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "funding_rates", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
        )

    @app.get("/v1/funding-info")
    def funding_info(symbol: str | None = None, limit: int = 1000, offset: int = 0):
        where, params = "", []
        if symbol:
            where, params = "WHERE symbol=?", [symbol.upper()]
        return _paginate(store, "funding_info", where, params, order="DESC", limit=limit, offset=offset)

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
        )

    @app.get("/v1/liquidations")
    def liquidations(symbol: str, limit: int = 5000, offset: int = 0):
        return _paginate(
            store, "liquidations", "WHERE symbol=?", [symbol.upper()],
            order="DESC", limit=limit, offset=offset,
        )

    # ── 系统 / 元数据 ──

    @app.get("/v1/insurance-balance")
    def insurance_balance(limit: int = 500, offset: int = 0):
        return _paginate(store, "insurance_balance", limit=limit, offset=offset)

    @app.get("/v1/delivery-prices")
    def delivery_prices(pair: str, limit: int = 500, offset: int = 0):
        return _paginate(
            store, "delivery_prices", "WHERE pair=?", [pair.upper()],
            order="DESC", limit=limit, offset=offset,
        )

    @app.get("/v1/exchange-info")
    def exchange_info(limit: int = 100, offset: int = 0):
        return _paginate(store, "exchange_info", order="DESC", limit=limit, offset=offset)

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
        return _paginate(store, table, where, params, order="DESC", limit=limit, offset=offset)

    # 兼容旧版路径
    @app.get("/klines")
    def legacy_klines(symbol: str, interval: str = "1h", limit: int = 1000):
        return klines(symbol, interval, limit=limit)["data"]

    @app.get("/trades")
    def legacy_trades(symbol: str, limit: int = 1000):
        return agg_trades(symbol, limit=limit)["data"]

    return app
