from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

# 所有数据表（供 API 文档与查询白名单使用）
ALL_TABLES = [
    "klines",
    "mark_price_klines",
    "index_price_klines",
    "continuous_klines",
    "kline_updates",
    "agg_trades",
    "trades",
    "mark_prices",
    "book_tickers",
    "ticker_price",
    "depth_snapshots",
    "depth_updates",
    "open_interest",
    "open_interest_hist",
    "funding_rates",
    "funding_info",
    "long_short_ratio",
    "ticker_24h",
    "ticker_snapshots",
    "basis",
    "liquidations",
    "insurance_balance",
    "delivery_prices",
    "exchange_info",
]


class MarketStore:
    """SQLite WAL 本地存储，永久保留，支持并发读取。"""

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT NOT NULL, interval TEXT NOT NULL, open_time INTEGER NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume REAL NOT NULL, close_time INTEGER NOT NULL, quote_volume REAL,
            trades INTEGER, taker_buy_volume REAL, taker_buy_quote_volume REAL,
            PRIMARY KEY (symbol, interval, open_time)
        );
        CREATE TABLE IF NOT EXISTS mark_price_klines (
            symbol TEXT NOT NULL, interval TEXT NOT NULL, open_time INTEGER NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            close_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, interval, open_time)
        );
        CREATE TABLE IF NOT EXISTS index_price_klines (
            pair TEXT NOT NULL, interval TEXT NOT NULL, open_time INTEGER NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            close_time INTEGER NOT NULL,
            PRIMARY KEY (pair, interval, open_time)
        );
        CREATE TABLE IF NOT EXISTS continuous_klines (
            pair TEXT NOT NULL, contract_type TEXT NOT NULL, interval TEXT NOT NULL,
            open_time INTEGER NOT NULL, open REAL NOT NULL, high REAL NOT NULL,
            low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL,
            close_time INTEGER NOT NULL, quote_volume REAL, trades INTEGER,
            taker_buy_volume REAL, taker_buy_quote_volume REAL,
            PRIMARY KEY (pair, contract_type, interval, open_time)
        );
        CREATE TABLE IF NOT EXISTS kline_updates (
            symbol TEXT NOT NULL, interval TEXT NOT NULL, open_time INTEGER NOT NULL,
            event_time INTEGER NOT NULL, open REAL, high REAL, low REAL, close REAL,
            volume REAL, is_closed INTEGER NOT NULL,
            PRIMARY KEY (symbol, interval, open_time, event_time)
        );
        CREATE TABLE IF NOT EXISTS agg_trades (
            symbol TEXT NOT NULL, agg_id INTEGER NOT NULL, price REAL NOT NULL,
            qty REAL NOT NULL, trade_time INTEGER NOT NULL, is_buyer_maker INTEGER NOT NULL,
            PRIMARY KEY (symbol, agg_id)
        );
        CREATE TABLE IF NOT EXISTS trades (
            symbol TEXT NOT NULL, trade_id INTEGER NOT NULL, price REAL NOT NULL,
            qty REAL NOT NULL, quote_qty REAL, trade_time INTEGER NOT NULL,
            is_buyer_maker INTEGER NOT NULL,
            PRIMARY KEY (symbol, trade_id)
        );
        CREATE TABLE IF NOT EXISTS mark_prices (
            symbol TEXT NOT NULL, mark_price REAL NOT NULL, index_price REAL NOT NULL,
            funding_rate REAL, next_funding_time INTEGER, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time)
        );
        CREATE TABLE IF NOT EXISTS book_tickers (
            symbol TEXT NOT NULL, bid_price REAL NOT NULL, bid_qty REAL NOT NULL,
            ask_price REAL NOT NULL, ask_qty REAL NOT NULL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time)
        );
        CREATE TABLE IF NOT EXISTS ticker_price (
            symbol TEXT NOT NULL, price REAL NOT NULL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time)
        );
        CREATE TABLE IF NOT EXISTS depth_snapshots (
            symbol TEXT NOT NULL, bids TEXT NOT NULL, asks TEXT NOT NULL,
            snapshot_time INTEGER NOT NULL, last_update_id INTEGER,
            PRIMARY KEY (symbol, snapshot_time)
        );
        CREATE TABLE IF NOT EXISTS depth_updates (
            symbol TEXT NOT NULL, event_time INTEGER NOT NULL, first_update_id INTEGER,
            final_update_id INTEGER, prev_update_id INTEGER, bids TEXT NOT NULL,
            asks TEXT NOT NULL,
            PRIMARY KEY (symbol, final_update_id)
        );
        CREATE TABLE IF NOT EXISTS open_interest (
            symbol TEXT NOT NULL, open_interest REAL NOT NULL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time)
        );
        CREATE TABLE IF NOT EXISTS open_interest_hist (
            symbol TEXT NOT NULL, period TEXT NOT NULL, sum_open_interest REAL NOT NULL,
            sum_open_interest_value REAL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, period, event_time)
        );
        CREATE TABLE IF NOT EXISTS funding_rates (
            symbol TEXT NOT NULL, funding_rate REAL NOT NULL, funding_time INTEGER NOT NULL,
            mark_price REAL, PRIMARY KEY (symbol, funding_time)
        );
        CREATE TABLE IF NOT EXISTS funding_info (
            symbol TEXT NOT NULL, adjusted_cap REAL, adjusted_floor REAL,
            funding_interval_hours INTEGER, snapshot_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, snapshot_time)
        );
        CREATE TABLE IF NOT EXISTS long_short_ratio (
            symbol TEXT NOT NULL, data_type TEXT NOT NULL, period TEXT NOT NULL,
            long_short_ratio REAL NOT NULL, long_account REAL, short_account REAL,
            buy_vol REAL, sell_vol REAL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, data_type, period, event_time)
        );
        CREATE TABLE IF NOT EXISTS ticker_24h (
            symbol TEXT NOT NULL, price_change REAL, price_change_percent REAL,
            last_price REAL, volume REAL, quote_volume REAL, high_price REAL,
            low_price REAL, event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time)
        );
        CREATE TABLE IF NOT EXISTS ticker_snapshots (
            symbol TEXT NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL,
            event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_type, event_time)
        );
        CREATE TABLE IF NOT EXISTS basis (
            pair TEXT NOT NULL, contract_type TEXT NOT NULL, period TEXT NOT NULL,
            index_price REAL, futures_price REAL, basis REAL, basis_rate REAL,
            event_time INTEGER NOT NULL,
            PRIMARY KEY (pair, contract_type, period, event_time)
        );
        CREATE TABLE IF NOT EXISTS liquidations (
            symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
            time_in_force TEXT, price REAL NOT NULL, avg_price REAL,
            orig_qty REAL NOT NULL, executed_qty REAL, order_status TEXT,
            event_time INTEGER NOT NULL,
            PRIMARY KEY (symbol, event_time, side, price)
        );
        CREATE TABLE IF NOT EXISTS insurance_balance (
            snapshot_time INTEGER NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY (snapshot_time)
        );
        CREATE TABLE IF NOT EXISTS delivery_prices (
            pair TEXT NOT NULL, delivery_time INTEGER NOT NULL,
            delivery_price REAL NOT NULL,
            PRIMARY KEY (pair, delivery_time)
        );
        CREATE TABLE IF NOT EXISTS exchange_info (
            snapshot_time INTEGER NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY (snapshot_time)
        );

        CREATE INDEX IF NOT EXISTS idx_klines ON klines(symbol, interval, open_time);
        CREATE INDEX IF NOT EXISTS idx_agg_trades ON agg_trades(symbol, trade_time);
        CREATE INDEX IF NOT EXISTS idx_trades ON trades(symbol, trade_time);
        CREATE INDEX IF NOT EXISTS idx_mark ON mark_prices(symbol, event_time);
        CREATE INDEX IF NOT EXISTS idx_depth_upd ON depth_updates(symbol, event_time);
        CREATE INDEX IF NOT EXISTS idx_kline_upd ON kline_updates(symbol, interval, open_time);
    """

    def __init__(self, db_path: Path, read_only: bool = False):
        self.db_path = db_path
        self.read_only = read_only
        if not read_only:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False)
        else:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-128000")
            conn.execute("PRAGMA temp_store=MEMORY")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        if self.read_only:
            return
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(self.SCHEMA)
                self._migrate(conn)
                conn.commit()
            finally:
                conn.close()

    def _migrate(self, conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(depth_snapshots)").fetchall()}
        if cols and "last_update_id" not in cols:
            conn.execute("ALTER TABLE depth_snapshots ADD COLUMN last_update_id INTEGER")

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        if self.read_only:
            raise RuntimeError("只读模式不可写入")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(sql, params or [])
                conn.commit()
            finally:
                conn.close()

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        if self.read_only:
            raise RuntimeError("只读模式不可写入")
        with self._lock:
            conn = self._connect()
            try:
                conn.executemany(sql, rows)
                conn.commit()
            finally:
                conn.close()

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict]:
        for attempt in range(8):
            try:
                conn = self._connect()
                try:
                    return [dict(r) for r in conn.execute(sql, params or []).fetchall()]
                finally:
                    conn.close()
            except sqlite3.OperationalError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
        return []

    def table_counts(self) -> dict[str, int]:
        return {t: self.query(f"SELECT COUNT(*) AS c FROM {t}")[0]["c"] for t in ALL_TABLES}

    def upsert_klines(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO klines VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )

    def upsert_mark_price_klines(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO mark_price_klines VALUES (?,?,?,?,?,?,?,?)", rows
        )

    def upsert_index_price_klines(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO index_price_klines VALUES (?,?,?,?,?,?,?,?)", rows
        )

    def upsert_continuous_klines(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO continuous_klines VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    def insert_kline_updates(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR IGNORE INTO kline_updates VALUES (?,?,?,?,?,?,?,?,?,?)", rows
        )

    def insert_agg_trades(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO agg_trades VALUES (?,?,?,?,?,?)", rows)

    def insert_trades(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?)", rows
        )

    def insert_mark_prices(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO mark_prices VALUES (?,?,?,?,?,?)", rows)

    def insert_book_tickers(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO book_tickers VALUES (?,?,?,?,?,?)", rows)

    def insert_ticker_price(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO ticker_price VALUES (?,?,?)", rows)

    def insert_depth_snapshot(
        self, symbol: str, bids: str, asks: str, last_id: int, ts: int
    ) -> None:
        self.execute(
            "INSERT OR IGNORE INTO depth_snapshots "
            "(symbol, bids, asks, snapshot_time, last_update_id) VALUES (?,?,?,?,?)",
            [symbol, bids, asks, ts, last_id],
        )

    def insert_depth_updates(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR IGNORE INTO depth_updates VALUES (?,?,?,?,?,?,?)", rows
        )

    def insert_open_interest(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO open_interest VALUES (?,?,?)", rows)

    def insert_open_interest_hist(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO open_interest_hist VALUES (?,?,?,?,?)", rows
        )

    def upsert_funding_rates(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR REPLACE INTO funding_rates VALUES (?,?,?,?)", rows)

    def insert_funding_info(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO funding_info VALUES (?,?,?,?,?)", rows)

    def insert_long_short_ratio(self, rows: list[tuple]) -> None:
        self.executemany(
            "INSERT OR REPLACE INTO long_short_ratio VALUES (?,?,?,?,?,?,?,?,?)", rows
        )

    def insert_ticker_24h(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO ticker_24h VALUES (?,?,?,?,?,?,?,?,?)", rows)

    def insert_ticker_snapshot(self, symbol: str, event_type: str, payload: str, ts: int) -> None:
        self.execute(
            "INSERT OR IGNORE INTO ticker_snapshots VALUES (?,?,?,?)",
            [symbol, event_type, payload, ts],
        )

    def insert_basis(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR REPLACE INTO basis VALUES (?,?,?,?,?,?,?,?)", rows)

    def insert_liquidations(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR IGNORE INTO liquidations VALUES (?,?,?,?,?,?,?,?,?,?)", rows)

    def insert_insurance_balance(self, payload: str, ts: int) -> None:
        self.execute(
            "INSERT OR IGNORE INTO insurance_balance VALUES (?,?)", [ts, payload]
        )

    def upsert_delivery_prices(self, rows: list[tuple]) -> None:
        self.executemany("INSERT OR REPLACE INTO delivery_prices VALUES (?,?,?)", rows)

    def insert_exchange_info(self, payload: str, ts: int) -> None:
        self.execute("INSERT OR IGNORE INTO exchange_info VALUES (?,?)", [ts, payload])

    def get_latest_kline_time(self, table: str, key_col: str, key_val: str, interval: str) -> int | None:
        rows = self.query(
            f"SELECT MAX(open_time) AS t FROM {table} WHERE {key_col}=? AND interval=?",
            [key_val, interval],
        )
        return rows[0]["t"] if rows and rows[0]["t"] is not None else None

    def count_klines(self, table: str, key_col: str, key_val: str, interval: str) -> int:
        rows = self.query(
            f"SELECT COUNT(*) AS c FROM {table} WHERE {key_col}=? AND interval=?",
            [key_val, interval],
        )
        return int(rows[0]["c"]) if rows else 0
