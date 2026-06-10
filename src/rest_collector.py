from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

from .config import Config
from .rate_limit import (
    PRIORITY_BACKFILL,
    PRIORITY_REALTIME,
    get_futures_data_limiter,
    get_limiter,
    is_futures_data_path,
    weight_for,
)
from .storage import MarketStore

log = logging.getLogger(__name__)

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000,
}


class RestCollector:
    def __init__(self, config: Config, store: MarketStore):
        self.config = config
        self.store = store
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "market-data-collector/2.0"})
        self._onboard_dates: dict[str, int] = {}
        self._limiter = get_limiter(config.rate_limit_max_weight, config.backfill_max_weight)
        self._futures_data = get_futures_data_limiter(config.futures_data_max_per_5min)
        self._priority_local = threading.local()

    def _get_priority(self) -> str:
        return getattr(self._priority_local, "value", PRIORITY_REALTIME)

    def _set_priority(self, value: str) -> None:
        self._priority_local.value = value

    class _BackfillScope:
        def __init__(self, collector: RestCollector):
            self._collector = collector
            self._prev = PRIORITY_REALTIME

        def __enter__(self) -> None:
            self._prev = self._collector._get_priority()
            self._collector._set_priority(PRIORITY_BACKFILL)

        def __exit__(self, *_) -> None:
            self._collector._set_priority(self._prev)

    def backfill_scope(self) -> _BackfillScope:
        """回填任务内使用，限制权重走低速通道。"""
        return RestCollector._BackfillScope(self)

    def _get(self, path: str, params: dict | None = None, *, priority: str | None = None) -> object:
        url = f"{self.config.rest_base}{path}"
        w = weight_for(path, params)
        prio = priority if priority is not None else self._get_priority()
        last_exc: Exception | None = None
        for attempt in range(5):
            if is_futures_data_path(path):
                self._futures_data.acquire()
            self._limiter.acquire(w, prio)
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                retry = int(resp.headers.get("Retry-After", 60))
                log.warning("429 %s (attempt %d), 冷却 %ds", path, attempt + 1, retry)
                if is_futures_data_path(path):
                    self._futures_data.penalize(retry)
                self._limiter.penalize(retry)
                last_exc = requests.HTTPError(f"429 for {path}", response=resp)
                continue
            try:
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                last_exc = exc
                raise
        if last_exc:
            raise last_exc
        resp.raise_for_status()
        return resp.json()

    def _pair(self, symbol: str) -> str:
        return symbol

    def _backfill_start_ms(self, symbol: str) -> int:
        if self.config.backfill_days > 0:
            return int(
                (datetime.now(timezone.utc) - timedelta(days=self.config.backfill_days)).timestamp()
                * 1000
            )
        if not self._onboard_dates:
            info = self._get("/fapi/v1/exchangeInfo")
            for s in info.get("symbols", []):
                self._onboard_dates[s["symbol"]] = int(s.get("onboardDate", 0))
        return self._onboard_dates.get(symbol, 0) or int(
            (datetime.now(timezone.utc) - timedelta(days=3650)).timestamp() * 1000
        )

    def _parser_for_table(self, table: str):
        return {
            "klines": self._parse_trade_kline,
            "mark_price_klines": self._parse_price_kline,
            "index_price_klines": self._parse_price_kline_pair,
            "continuous_klines": self._parse_continuous_kline,
        }[table]

    def _upsert_for_table(self, table: str):
        return {
            "klines": self.store.upsert_klines,
            "mark_price_klines": self.store.upsert_mark_price_klines,
            "index_price_klines": self.store.upsert_index_price_klines,
            "continuous_klines": self.store.upsert_continuous_klines,
        }[table]

    def _symbol_for_key(self, key_col: str, key_val: str) -> str:
        if key_col == "symbol":
            return key_val
        for sym in self.config.symbols:
            if self._pair(sym) == key_val:
                return sym
        return self.config.symbols[0]

    def _kline_cursor(self, table: str, key_col: str, key_val: str, interval: str) -> int:
        start_ms = self._backfill_start_ms(self._symbol_for_key(key_col, key_val))
        existing = self.store.count_klines(table, key_col, key_val, interval)
        latest = self.store.get_latest_kline_time(table, key_col, key_val, interval)
        now = int(time.time() * 1000)
        expected = max(1, (now - start_ms) // INTERVAL_MS.get(interval, 60_000))
        if existing >= expected * 0.95 and latest:
            return latest + INTERVAL_MS[interval]
        if existing >= expected * 0.8 and latest:
            return latest + INTERVAL_MS[interval]
        return start_ms

    def is_kline_complete(self, table: str, key_col: str, key_val: str, interval: str) -> bool:
        start_ms = self._kline_cursor(table, key_col, key_val, interval)
        now = int(time.time() * 1000)
        return start_ms >= now

    def backfill_kline_batch(self, task) -> tuple[bool, int, bool]:
        """执行单个 K 线任务的一批（最多 1500 条）。

        返回 (是否完成, 写入条数, 是否历史批次即 cursor 在 24h 以前)。
        """
        t = task
        cursor = self._kline_cursor(t.table, t.key_col, t.key_val, t.interval)
        now = int(time.time() * 1000)
        if cursor >= now:
            return True, 0, False

        is_historical = cursor < now - 86_400_000
        params: dict = {t.key_col: t.key_val, "interval": t.interval, "startTime": cursor, "limit": 1500}
        if t.extra:
            params.update(t.extra)
        try:
            data = self._get(t.path, params, priority=PRIORITY_BACKFILL)
        except requests.HTTPError:
            return False, 0, is_historical
        if not data:
            return True, 0, is_historical

        parser = self._parser_for_table(t.table)
        rows = parser(t.key_val, t.interval, data, t.extra or None)
        self._upsert_for_table(t.table)(rows)
        next_cursor = int(data[-1][0]) + INTERVAL_MS[t.interval]
        done = next_cursor >= now
        if is_historical:
            time.sleep(self.config.backfill_historical_sleep)
        else:
            time.sleep(self.config.backfill_recent_sleep)
        return done, len(rows), is_historical

    def backfill_agg_trades_24h(self) -> None:
        """回填最近 24h 聚合成交（币安 REST 窗口限制）。"""
        now = int(time.time() * 1000)
        start = now - 86_400_000
        total = 0
        for symbol in self.config.symbols:
            cursor = start
            while cursor < now:
                try:
                    data = self._get("/fapi/v1/aggTrades", {
                        "symbol": symbol, "startTime": cursor, "limit": 1000,
                    }, priority=PRIORITY_BACKFILL)
                except requests.HTTPError:
                    break
                if not data:
                    break
                rows = [
                    (symbol, int(t["a"]), float(t["p"]), float(t["q"]),
                     int(t["T"]), int(t["m"]))
                    for t in data
                ]
                self.store.insert_agg_trades(rows)
                total += len(rows)
                last_t = int(data[-1]["T"])
                if last_t <= cursor:
                    break
                cursor = last_t + 1
                time.sleep(self.config.backfill_agg_trades_sleep)
        log.info("回填 24h 聚合成交: %d 条", total)

    def incremental_backfill_round(self) -> None:
        """增量维护：每个 symbol 各 K 线周期补一批。"""
        from .backfill_worker import KlineTask, KLINE_INTERVAL_ORDER

        for symbol in self.config.symbols:
            for interval in KLINE_INTERVAL_ORDER:
                if interval not in self.config.kline_intervals:
                    continue
                task = KlineTask("klines", "/fapi/v1/klines", "symbol", symbol, interval)
                if not self.is_kline_complete("klines", "symbol", symbol, interval):
                    self.backfill_kline_batch(task)

    def incremental_backfill(self) -> None:
        self.incremental_backfill_round()

    def backfill_all(self) -> None:
        """阻塞式全量回填（仅 --backfill-only 使用）。"""
        from .backfill_worker import BackfillWorker
        worker = BackfillWorker(self.config, self.store)
        worker.rest = self
        worker._run()

    def _parse_trade_kline(self, symbol, interval, data, _extra=None):
        return [
            (symbol, interval, int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]),
             float(k[5]), int(k[6]), float(k[7]), int(k[8]), float(k[9]), float(k[10]))
            for k in data
        ]

    def _parse_price_kline(self, symbol, interval, data, _extra=None):
        return [
            (symbol, interval, int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), int(k[6]))
            for k in data
        ]

    def _parse_price_kline_pair(self, pair, interval, data, _extra=None):
        return [
            (pair, interval, int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), int(k[6]))
            for k in data
        ]

    def _parse_continuous_kline(self, pair, interval, data, extra=None):
        ct = (extra or {}).get("contractType", "PERPETUAL")
        return [
            (pair, ct, interval, int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]),
             float(k[5]), int(k[6]), float(k[7]), int(k[8]), float(k[9]), float(k[10]))
            for k in data
        ]

    def fetch_trades(self) -> None:
        rows = []
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/trades", {"symbol": symbol, "limit": 1000})
            for t in data:
                rows.append((
                    symbol, int(t["id"]), float(t["price"]), float(t["qty"]),
                    float(t.get("quoteQty", 0)), int(t["time"]), int(t["isBuyerMaker"]),
                ))
        self.store.insert_trades(rows)
        log.info("采集逐笔成交: %d 条", len(rows))

    def fetch_agg_trades_poll(self) -> None:
        rows = []
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/aggTrades", {"symbol": symbol, "limit": 1000})
            for t in data:
                rows.append((
                    symbol, int(t["a"]), float(t["p"]), float(t["q"]),
                    int(t["T"]), int(t["m"]),
                ))
        self.store.insert_agg_trades(rows)
        log.info("轮询聚合成交: %d 条", len(rows))

    def fill_agg_trades_gaps(self) -> None:
        """P1：自库内最新 trade_time 起 REST 补缺（24h 内，≤1h 窗口）。"""
        if not self.config.agg_gap_fill_enabled:
            return
        now = int(time.time() * 1000)
        window_start = now - 86_400_000
        filled = 0
        for symbol in self.config.symbols:
            row = self.store.query(
                "SELECT MAX(trade_time) AS t FROM agg_trades WHERE symbol=?", [symbol],
            )
            last_t = int(row[0]["t"]) if row and row[0]["t"] else window_start
            if now - last_t < 15_000:
                continue
            cursor = max(last_t + 1, window_start)
            while cursor < now:
                end = min(cursor + 3_599_000, now)
                try:
                    data = self._get("/fapi/v1/aggTrades", {
                        "symbol": symbol,
                        "startTime": cursor,
                        "endTime": end,
                        "limit": 1000,
                    }, priority=PRIORITY_REALTIME)
                except requests.HTTPError as e:
                    log.warning("agg 缺口回补 %s 失败: %s", symbol, e)
                    break
                if not data:
                    cursor = end + 1
                    continue
                rows = [
                    (symbol, int(t["a"]), float(t["p"]), float(t["q"]),
                     int(t["T"]), int(t["m"]))
                    for t in data
                ]
                self.store.insert_agg_trades(rows)
                filled += len(rows)
                last_trade = int(data[-1]["T"])
                if last_trade <= cursor:
                    break
                cursor = last_trade + 1
                if len(data) < 1000:
                    cursor = max(cursor, end + 1)
        if filled:
            log.info("agg REST 缺口回补: %d 条", filled)

    def fetch_open_interest(self) -> None:
        rows = []
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/openInterest", {"symbol": symbol})
            rows.append((symbol, float(data["openInterest"]), int(data["time"])))
        self.store.insert_open_interest(rows)

    def fetch_open_interest_hist(self, full: bool = False) -> None:
        rows = []
        limit = 500 if full else 30
        for symbol in self.config.symbols:
            for period in self.config.data_periods:
                data = self._get(
                    "/futures/data/openInterestHist",
                    {"symbol": symbol, "period": period, "limit": limit},
                )
                for item in data:
                    rows.append((
                        symbol, period, float(item["sumOpenInterest"]),
                        float(item.get("sumOpenInterestValue", 0)), int(item["timestamp"]),
                    ))
        self.store.insert_open_interest_hist(rows)
        log.info("采集持仓量历史: %d 条", len(rows))

    def fetch_funding_rates(self, full: bool = False) -> None:
        rows = []
        limit = 1000 if full else 200
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})
            for item in data:
                rows.append((
                    symbol, float(item["fundingRate"]), int(item["fundingTime"]),
                    float(item.get("markPrice", 0)),
                ))
        self.store.upsert_funding_rates(rows)
        log.info("采集资金费率: %d 条", len(rows))

    def fetch_funding_info(self) -> None:
        now = int(time.time() * 1000)
        data = self._get("/fapi/v1/fundingInfo")
        rows = [
            (item["symbol"], float(item.get("adjustedFundingRateCap", 0)),
             float(item.get("adjustedFundingRateFloor", 0)),
             int(item.get("fundingIntervalHours", 8)), now)
            for item in data
            if item["symbol"] in self.config.symbols
        ]
        self.store.insert_funding_info(rows)

    def fetch_long_short_ratios(self, full: bool = False) -> None:
        endpoints = {
            "global_account": "/futures/data/globalLongShortAccountRatio",
            "top_account": "/futures/data/topLongShortAccountRatio",
            "top_position": "/futures/data/topLongShortPositionRatio",
            "taker": "/futures/data/takerlongshortRatio",
        }
        limit = 500 if full else 10
        rows = []
        for symbol in self.config.symbols:
            for period in self.config.data_periods:
                for data_type, path in endpoints.items():
                    try:
                        data = self._get(path, {"symbol": symbol, "period": period, "limit": limit})
                    except requests.HTTPError:
                        continue
                    for item in data:
                        if data_type == "taker":
                            rows.append((
                                symbol, data_type, period, float(item["buySellRatio"]),
                                None, None, float(item["buyVol"]), float(item["sellVol"]),
                                int(item["timestamp"]),
                            ))
                        else:
                            rows.append((
                                symbol, data_type, period, float(item["longShortRatio"]),
                                float(item["longAccount"]), float(item["shortAccount"]),
                                None, None, int(item["timestamp"]),
                            ))
        self.store.insert_long_short_ratio(rows)
        log.info("采集多空比: %d 条", len(rows))

    def fetch_ticker_24h(self) -> None:
        now = int(time.time() * 1000)
        rows = []
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/ticker/24hr", {"symbol": symbol})
            rows.append((
                symbol, float(data["priceChange"]), float(data["priceChangePercent"]),
                float(data["lastPrice"]), float(data["volume"]), float(data["quoteVolume"]),
                float(data["highPrice"]), float(data["lowPrice"]), now,
            ))
        self.store.insert_ticker_24h(rows)

    def fetch_ticker_price(self) -> None:
        now = int(time.time() * 1000)
        rows = []
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/ticker/price", {"symbol": symbol})
            rows.append((symbol, float(data["price"]), now))
        self.store.insert_ticker_price(rows)

    def fetch_basis(self, full: bool = False) -> None:
        limit = 500 if full else 10
        rows = []
        for symbol in self.config.symbols:
            pair = self._pair(symbol)
            for contract_type in ("PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"):
                for period in self.config.data_periods:
                    try:
                        data = self._get("/futures/data/basis", {
                            "pair": pair, "contractType": contract_type,
                            "period": period, "limit": limit,
                        })
                    except requests.HTTPError:
                        continue
                    for item in data:
                        rows.append((
                            item["pair"], item["contractType"], period,
                            float(item.get("indexPrice", 0)), float(item.get("futuresPrice", 0)),
                            float(item.get("basis", 0)), float(item.get("basisRate", 0) or 0),
                            int(item["timestamp"]),
                        ))
        self.store.insert_basis(rows)
        log.info("采集基差: %d 条", len(rows))

    def fetch_book_tickers(self) -> None:
        """REST 兜底：WS bookTicker 停更时保持 bid/ask 新鲜。"""
        now = int(time.time() * 1000)
        sym_set = set(self.config.symbols)
        rows: list[tuple] = []
        try:
            data = self._get("/fapi/v1/ticker/bookTicker")
        except requests.HTTPError as exc:
            log.warning("REST bookTicker 批量请求失败: %s", exc)
            return
        if isinstance(data, dict):
            data = [data]
        for item in data:
            sym = item.get("symbol")
            if sym not in sym_set:
                continue
            event_time = int(item.get("time") or now)
            rows.append((
                sym,
                float(item["bidPrice"]), float(item["bidQty"]),
                float(item["askPrice"]), float(item["askQty"]),
                event_time,
            ))
        if rows:
            self.store.insert_book_tickers(rows)
            log.debug("REST book_ticker: %d 条", len(rows))

    def fetch_depth_snapshot(self, symbol: str, *, reason: str = "") -> int:
        """单币种 L2 REST 快照（实时最高优先级）。"""
        limit = self.config.l2_snapshot_limit
        data = self._get(
            "/fapi/v1/depth",
            {"symbol": symbol, "limit": limit},
            priority=PRIORITY_REALTIME,
        )
        last_id = int(data["lastUpdateId"])
        ts = int(time.time() * 1000)
        self.store.insert_depth_snapshot(
            symbol, json.dumps(data["bids"]), json.dumps(data["asks"]),
            last_id, ts,
        )
        if reason:
            log.debug("depth snapshot %s id=%d (%s)", symbol, last_id, reason)
        return last_id

    def fetch_depth_snapshots(self) -> None:
        for symbol in self.config.symbols:
            self.fetch_depth_snapshot(symbol, reason="scheduled")

    def fetch_mark_prices_rest(self) -> None:
        """REST 兜底：WS markPrice 停更时保持标记价新鲜（用本地时间戳写入）。"""
        rows = []
        now = int(time.time() * 1000)
        for symbol in self.config.symbols:
            data = self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
            rows.append((
                symbol, float(data["markPrice"]), float(data["indexPrice"]),
                float(data["lastFundingRate"]), int(data["nextFundingTime"]), now,
            ))
        self.store.insert_mark_prices(rows)

    def fetch_insurance_balance(self) -> None:
        data = self._get("/fapi/v1/insuranceBalance")
        self.store.insert_insurance_balance(json.dumps(data), int(time.time() * 1000))

    def fetch_delivery_prices(self) -> None:
        rows = []
        for symbol in self.config.symbols:
            pair = self._pair(symbol)
            try:
                data = self._get("/futures/data/delivery-price", {"pair": pair})
            except requests.HTTPError:
                continue
            for item in data:
                rows.append((pair, int(item["deliveryTime"]), float(item["deliveryPrice"])))
        self.store.upsert_delivery_prices(rows)
        log.info("采集交割价: %d 条", len(rows))

    def fetch_exchange_info(self) -> None:
        data = self._get("/fapi/v1/exchangeInfo")
        self.store.insert_exchange_info(json.dumps(data), int(time.time() * 1000))
        for s in data.get("symbols", []):
            if s["symbol"] in self.config.symbols:
                self._onboard_dates[s["symbol"]] = int(s.get("onboardDate", 0))
