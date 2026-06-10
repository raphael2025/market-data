from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Config:
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    kline_intervals: list[str] = field(default_factory=list)
    data_periods: list[str] = field(default_factory=list)
    rest_base: str = "https://fapi.binance.com"
    ws_base: str = "wss://fstream.binance.com"
    db_path: Path = field(default_factory=lambda: ROOT / "data" / "market.db")
    api_host: str = "0.0.0.0"
    api_port: int = 8765
    schedules: dict[str, int] = field(default_factory=dict)
    backfill_days: int = 0
    rate_limit_max_weight: int = 1800
    backfill_max_weight: int = 350
    backfill_agg_trades_sleep: float = 1.0
    backfill_historical_sleep: float = 8.0
    backfill_recent_sleep: float = 2.0
    backfill_batches_per_round: int = 3
    l2_snapshot_interval: int = 10
    l2_snapshot_limit: int = 1000
    ws_stale_agg_s: float = 45
    ws_stale_book_s: float = 45
    ws_stale_mark_s: float = 30
    ws_stale_kline_s: float = 120
    ws_watchdog_interval_s: float = 15
    agg_gap_fill_enabled: bool = True
    backfill_startup_delay_s: int = 600
    backfill_defer_30d_full: bool = True
    futures_data_max_per_5min: int = 900

    @classmethod
    def load(cls, path: Path | None = None) -> Config:
        path = path or ROOT / "config.yaml"
        with open(path) as f:
            raw = yaml.safe_load(f)

        storage = raw.get("storage", {})
        api = raw.get("api", {})
        binance = raw.get("binance", {})

        return cls(
            symbols=raw.get("symbols", cls().symbols),
            kline_intervals=raw.get("kline_intervals", cls().kline_intervals),
            data_periods=raw.get("data_periods", cls().data_periods),
            rest_base=binance.get("rest_base", cls().rest_base),
            ws_base=binance.get("ws_base", cls().ws_base),
            db_path=ROOT / storage.get("db_path", "data/market.db"),
            api_host=api.get("host", cls().api_host),
            api_port=api.get("port", cls().api_port),
            schedules=raw.get("schedules", {}),
            backfill_days=raw.get("backfill_days", 0),
            rate_limit_max_weight=raw.get("rate_limit", {}).get("max_weight_per_minute", 1800),
            backfill_max_weight=raw.get("rate_limit", {}).get("backfill_max_weight_per_minute", 350),
            backfill_agg_trades_sleep=raw.get("rate_limit", {}).get("backfill_agg_trades_sleep", 1.0),
            backfill_historical_sleep=raw.get("rate_limit", {}).get("backfill_historical_sleep", 8.0),
            backfill_recent_sleep=raw.get("rate_limit", {}).get("backfill_recent_sleep", 2.0),
            backfill_batches_per_round=raw.get("rate_limit", {}).get("batches_per_round", 3),
            l2_snapshot_interval=raw.get("l2", {}).get("snapshot_interval", 10),
            l2_snapshot_limit=raw.get("l2", {}).get("snapshot_limit", 1000),
            ws_stale_agg_s=raw.get("realtime", {}).get("ws_stale_agg_s", 45),
            ws_stale_book_s=raw.get("realtime", {}).get("ws_stale_book_s", 45),
            ws_stale_mark_s=raw.get("realtime", {}).get("ws_stale_mark_s", 30),
            ws_stale_kline_s=raw.get("realtime", {}).get("ws_stale_kline_s", 120),
            ws_watchdog_interval_s=raw.get("realtime", {}).get("watchdog_interval_s", 15),
            agg_gap_fill_enabled=raw.get("realtime", {}).get("agg_gap_fill", True),
            backfill_startup_delay_s=raw.get("backfill", {}).get("startup_delay_s", 600),
            backfill_defer_30d_full=raw.get("backfill", {}).get("defer_30d_full", True),
            futures_data_max_per_5min=raw.get("rate_limit", {}).get(
                "futures_data_max_per_5min", 900
            ),
        )
