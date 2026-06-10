from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import websockets

from .config import Config
from .storage import MarketStore

if TYPE_CHECKING:
    from .depth_guardian import DepthGuardian
    from .stream_guardian import StreamGuardian

log = logging.getLogger(__name__)

_STALE_TIMEOUT = {
    "book": "ws_stale_book_s",
    "trade": "ws_stale_agg_s",
    "quotes": "ws_stale_mark_s",
    "klines": "ws_stale_kline_s",
}

_STALE_KIND = {
    "book": "book",
    "trade": "agg",
    "quotes": "mark",
    "klines": "kline",
}


class WsCollector:
    """币安合约 WebSocket：book / depth(L2) / trade / quotes / klines 五路独立连接。"""

    def __init__(
        self,
        config: Config,
        store: MarketStore,
        guardian: DepthGuardian | None = None,
        stream_guardian: StreamGuardian | None = None,
    ):
        self.config = config
        self.store = store
        self._guardian = guardian
        self._stream = stream_guardian
        self._running = False
        self._conn_name = ""

    def _book_streams(self) -> list[str]:
        return [f"{s.lower()}@bookTicker" for s in self.config.symbols]

    def _depth_streams(self) -> list[str]:
        streams = []
        for s in self.config.symbols:
            sym = s.lower()
            streams.append(f"{sym}@depth@100ms")
            streams.append(f"{sym}@depth20@100ms")
        return streams

    def _trade_streams(self) -> list[str]:
        """P0：aggTrade + 强平 — 与 K 线隔离，避免 market 僵死拖垮成交。"""
        streams = []
        for s in self.config.symbols:
            sym = s.lower()
            streams.append(f"{sym}@aggTrade")
            streams.append(f"{sym}@forceOrder")
        return streams

    def _quote_streams(self) -> list[str]:
        streams = []
        for s in self.config.symbols:
            sym = s.lower()
            streams.append(f"{sym}@markPrice@1s")
            streams.append(f"{sym}@ticker")
            streams.append(f"{sym}@miniTicker")
        return streams

    def _kline_streams(self) -> list[str]:
        streams = []
        for symbol in self.config.symbols:
            s = symbol.lower()
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
        depth = self._depth_streams()
        trade = self._trade_streams()
        quotes = self._quote_streams()
        klines = self._kline_streams()
        log.info(
            "WebSocket book=%d depth=%d trade=%d quotes=%d klines=%d 流",
            len(book), len(depth), len(trade), len(quotes), len(klines),
        )
        async def _delayed(delay: float, name: str, url: str, *, l2: bool = False) -> None:
            await asyncio.sleep(delay)
            await self._run_connection(name, url, l2=l2)

        await asyncio.gather(
            _delayed(0, "book", self._stream_url("public", book)),
            _delayed(1, "depth", self._stream_url("public", depth), l2=True),
            _delayed(2, "trade", self._stream_url("market", trade)),
            _delayed(3, "quotes", self._stream_url("market", quotes)),
            _delayed(4, "klines", self._stream_url("market", klines)),
        )

    def _stale_timeout(self, name: str) -> float:
        attr = _STALE_TIMEOUT.get(name)
        return float(getattr(self.config, attr, 60)) if attr else 60.0

    async def _watchdog(self, name: str, ws_holder: dict) -> None:
        if not self._stream:
            return
        kind = _STALE_KIND.get(name)
        if not kind:
            return
        interval = self.config.ws_watchdog_interval_s
        timeout = self._stale_timeout(name)
        while self._running:
            await asyncio.sleep(interval)
            ws = ws_holder.get("ws")
            if ws is None:
                continue
            if self._stream.all_symbols_stale(kind, self.config.symbols, timeout):
                log.warning("WebSocket [%s] 活性超时 (>%.0fs)，强制重连", name, timeout)
                self._stream.record_reconnect(name)
                await ws.close()
                return

    async def _run_connection(
        self, name: str, url: str, *, l2: bool = False
    ) -> None:
        backoff = 5.0
        max_backoff = 60.0
        while self._running:
            ws_holder: dict = {"ws": None}
            watchdog = asyncio.create_task(self._watchdog(name, ws_holder))
            try:
                async with websockets.connect(
                    url,
                    ping_interval=180,
                    ping_timeout=60,
                    open_timeout=45,
                    close_timeout=10,
                ) as ws:
                    ws_holder["ws"] = ws
                    log.info("WebSocket [%s] 已连接", name)
                    backoff = 5.0
                    if l2 and self._guardian:
                        await asyncio.to_thread(self._guardian.on_connect, name)
                    async for raw in ws:
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        self._conn_name = name
                        if self._stream:
                            self._stream.touch_conn(name)
                        self._dispatch(msg.get("data", msg), stream=stream)
            except Exception as e:
                if l2 and self._guardian:
                    await asyncio.to_thread(self._guardian.on_disconnect, name)
                log.warning(
                    "WebSocket [%s] 断开: %s, %.0f秒后重连", name, e, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            finally:
                watchdog.cancel()
                try:
                    await watchdog
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._running = False

    def _touch(self, kind: str, symbol: str | None) -> None:
        if self._stream:
            self._stream.touch(kind, symbol)

    def _dispatch(self, data: dict, *, stream: str = "") -> None:
        event = data.get("e")
        if event == "aggTrade":
            self._on_agg_trade(data)
        elif event == "markPriceUpdate":
            self._on_mark_price(data)
        elif event == "kline":
            self._on_kline(data)
        elif event == "depthUpdate":
            self._on_depth_update(data, stream=stream)
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
        sym = data["s"]
        self._touch("agg", sym)
        self.store.insert_agg_trades([(
            sym, int(data["a"]), float(data["p"]), float(data["q"]),
            int(data["T"]), int(data["m"]),
        )])

    def _on_mark_price(self, data: dict) -> None:
        sym = data["s"]
        self._touch("mark", sym)
        self.store.insert_mark_prices([(
            sym, float(data["p"]), float(data["i"]), float(data["r"]),
            int(data["T"]), int(data["E"]),
        )])

    def _on_book_ticker(self, data: dict) -> None:
        sym = data["s"]
        self._touch("book", sym)
        event_time = int(data.get("E") or data.get("T") or time.time() * 1000)
        self.store.insert_book_tickers([(
            sym, float(data["b"]), float(data["B"]),
            float(data["a"]), float(data["A"]),
            event_time,
        )])

    def _on_kline(self, data: dict) -> None:
        k = data["k"]
        self._touch("kline", k["s"])
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

    def _on_depth_update(self, data: dict, *, stream: str = "") -> None:
        if self._guardian:
            self._guardian.handle_depth_update(data, stream=stream)
            return
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
        sym = o["s"]
        self._touch("liq", sym)
        self.store.insert_liquidations([(
            sym, o["S"], o["o"], o.get("f", ""),
            float(o["p"]), float(o.get("ap", 0)), float(o["q"]),
            float(o.get("z", 0)), o.get("X", ""), int(o["T"]),
        )])
