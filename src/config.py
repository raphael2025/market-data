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
        )
