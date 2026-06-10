from __future__ import annotations

import asyncio
import json
import logging
import time
from urllib.parse import urlencode

import websockets

from .config import Config
from .storage import MarketStore

log = logging.getLogger(__name__)


class WsCollector:
    """币安合约 WebSocket 全量采集：public / market 双连接。"""

    def __init__(self, config: Config, store: MarketStore):
        self.config = config
        self.store = store
        self._running = False

    def _book_streams(self) -> list[str]:
        return [f"{s.lower()}@bookTicker" for s in self.config.symbols]

    def _public_streams(self) -> list[str]:
        streams = []
        for s in self.config.symbols:
            sym = s.lower()
            streams.append(f"{sym}@depth@100ms")
            streams.append(f"{sym}@depth20@100ms")
        return streams

    def _market_streams(self) -> list[str]:
        streams = []
        for symbol in self.config.symbols:
            s = symbol.lower()
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@markPrice@1s")
            streams.append(f"{s}@ticker")
            streams.append(f"{s}@miniTicker")
            streams.append(f"{s}@forceOrder")
            for interval in self.config.kline_intervals:
                streams.append(f"{s}@kline_{interval}")
        return streams

    def _stream_url(self, route: str, streams: list[str]) -> str:
        base = self.config.ws_base.rstrip("/")
        query = urlencode({"streams": "/".join(streams)})
        return f"{base}/{route}/stream?{query}"

    async def run(self) -> None:
        self._running = True
        book = self._book_streams()
        pub = self._public_streams()
        mkt = self._market_streams()
        log.info("WebSocket book=%d public=%d market=%d 流", len(book), len(pub), len(mkt))
        await asyncio.gather(
            self._run_connection("book", self._stream_url("public", book)),
            self._run_connection("public", self._stream_url("public", pub)),
            self._run_connection("market", self._stream_url("market", mkt)),
        )

    async def _run_connection(self, name: str, url: str) -> None:
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=180, ping_timeout=60) as ws:
                    log.info("WebSocket [%s] 已连接", name)
                    async for raw in ws:
                        msg = json.loads(raw)
                        self._dispatch(msg.get("data", msg))
            except Exception as e:
                log.warning("WebSocket [%s] 断开: %s, 5秒后重连", name, e)
                await asyncio.sleep(5)

    def stop(self) -> None:
        self._running = False

    def _dispatch(self, data: dict) -> None:
        event = data.get("e")
        if event == "aggTrade":
            self._on_agg_trade(data)
        elif event == "markPriceUpdate":
            self._on_mark_price(data)
        elif event == "kline":
            self._on_kline(data)
        elif event == "depthUpdate":
            self._on_depth_update(data)
        elif event == "24hrTicker":
            self._on_ticker(data, "24hrTicker")
        elif event == "24hrMiniTicker":
            self._on_ticker(data, "24hrMiniTicker")
        elif event == "forceOrder":
            self._on_liquidation(data)
        elif event == "bookTicker" or (
            "b" in data and "a" in data and "s" in data and "e" not in data
        ):
            self._on_book_ticker(data)

    def _on_agg_trade(self, data: dict) -> None:
        self.store.insert_agg_trades([(
            data["s"], int(data["a"]), float(data["p"]), float(data["q"]),
            int(data["T"]), int(data["m"]),
        )])

    def _on_mark_price(self, data: dict) -> None:
        self.store.insert_mark_prices([(
            data["s"], float(data["p"]), float(data["i"]), float(data["r"]),
            int(data["T"]), int(data["E"]),
        )])

    def _on_book_ticker(self, data: dict) -> None:
        event_time = int(data.get("E") or data.get("T") or time.time() * 1000)
        self.store.insert_book_tickers([(
            data["s"], float(data["b"]), float(data["B"]),
            float(data["a"]), float(data["A"]),
            event_time,
        )])

    def _on_kline(self, data: dict) -> None:
        k = data["k"]
        self.store.insert_kline_updates([(
            k["s"], k["i"], int(k["t"]), int(data["E"]),
            float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
            float(k["v"]), int(k["x"]),
        )])
        if k["x"]:
            self.store.upsert_klines([(
                k["s"], k["i"], int(k["t"]), float(k["o"]), float(k["h"]),
                float(k["l"]), float(k["c"]), float(k["v"]), int(k["T"]),
                float(k["q"]), int(k["n"]), float(k["V"]), float(k["Q"]),
            )])

    def _on_depth_update(self, data: dict) -> None:
        self.store.insert_depth_updates([(
            data["s"], int(data["E"]), int(data["U"]), int(data["u"]),
            int(data.get("pu", 0)), json.dumps(data["b"]), json.dumps(data["a"]),
        )])

    def _on_ticker(self, data: dict, event_type: str) -> None:
        self.store.insert_ticker_snapshot(
            data["s"], event_type, json.dumps(data), int(data["E"]),
        )

    def _on_liquidation(self, data: dict) -> None:
        o = data["o"]
        self.store.insert_liquidations([(
            o["s"], o["S"], o["o"], o.get("f", ""),
            float(o["p"]), float(o.get("ap", 0)), float(o["q"]),
            float(o.get("z", 0)), o.get("X", ""), int(o["T"]),
        )])
