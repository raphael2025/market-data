"""L2 连续性守护：序号校验、断连/跳号时 REST 补快照、记录缺口。"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config
    from .rest_collector import RestCollector
    from .storage import MarketStore

log = logging.getLogger(__name__)


class DepthGuardian:
    """保证 L2 可重建：增量连续写入，异常时用 REST 快照桥接。"""

    def __init__(self, config: Config, store: MarketStore, rest: RestCollector):
        self.config = config
        self.store = store
        self.rest = rest
        self._lock = threading.Lock()
        self._last_u: dict[str, int] = {}
        self._snapshot_id: dict[str, int] = {}
        self._awaiting_sync: dict[str, bool] = {s: True for s in config.symbols}

    def bootstrap(self) -> None:
        """启动时先拉 REST 快照，再允许 WS 增量。"""
        self.snapshot_all("startup")
        with self._lock:
            for sym in self.config.symbols:
                self._awaiting_sync[sym] = True

    def on_connect(self, connection: str) -> None:
        if connection != "depth":
            return
        symbols = list(self.config.symbols)
        with self._lock:
            for sym in symbols:
                self._awaiting_sync[sym] = True
                self._last_u.pop(sym, None)
        self.snapshot_all("ws_reconnect")
        now = int(time.time() * 1000)
        for sym in symbols:
            snap_id = self._snapshot_id.get(sym)
            if snap_id is not None:
                n = self.store.close_all_depth_gaps(sym, now, snap_id)
                if n:
                    log.info("L2 重连闭合 %s: %d 个缺口", sym, n)

    def on_disconnect(self, connection: str) -> None:
        if connection != "depth":
            return
        now = int(time.time() * 1000)
        symbols = list(self.config.symbols)
        gap_meta: list[tuple[str, int | None]] = []
        with self._lock:
            for sym in symbols:
                self._awaiting_sync[sym] = True
                gap_meta.append((sym, self._last_u.get(sym)))
                self._last_u.pop(sym, None)
        for sym, before_u in gap_meta:
            self.store.open_depth_gap(sym, now, "ws_disconnect", before_u)
        self.snapshot_all("ws_disconnect")

    def snapshot_all(self, reason: str) -> None:
        for symbol in self.config.symbols:
            try:
                self.snapshot_symbol(symbol, reason)
            except Exception as exc:
                log.warning("L2 快照失败 %s (%s): %s", symbol, reason, exc)

    def snapshot_symbol(self, symbol: str, reason: str) -> int:
        last_id = self.rest.fetch_depth_snapshot(symbol, reason=reason)
        with self._lock:
            self._snapshot_id[symbol] = last_id
        log.info("L2 快照 %s lastUpdateId=%d (%s)", symbol, last_id, reason)
        return last_id

    def handle_depth_update(self, data: dict, *, stream: str) -> bool:
        """校验序号并写入。返回 False 表示丢弃（过期或等待同步）。"""
        symbol = data["s"]
        U, u, pu = int(data["U"]), int(data["u"]), int(data.get("pu", 0))
        is_full = "@depth20" not in stream

        gap_reason: str | None = None
        gap_before: int | None = None

        with self._lock:
            if self._awaiting_sync.get(symbol):
                snap = self._snapshot_id.get(symbol, 0)
                if snap and u < snap:
                    return False
                if snap and not (U <= snap + 1 <= u):
                    if u < snap + 1:
                        return False
                self._awaiting_sync[symbol] = False

            if is_full:
                last = self._last_u.get(symbol)
                if last is not None and pu != 0 and pu != last:
                    gap_reason = "sequence_gap"
                    gap_before = last
                    self._awaiting_sync[symbol] = True
                    self._last_u.pop(symbol, None)

        if gap_reason:
            now = int(time.time() * 1000)
            log.warning(
                "L2 跳号 %s: pu=%d != last_u=%d，补快照桥接",
                symbol, pu, gap_before,
            )
            self.store.open_depth_gap(symbol, now, gap_reason, gap_before)
            snap_id = self.snapshot_symbol(symbol, gap_reason)
            self.store.close_depth_gap(symbol, now, snap_id, gap_reason)
            with self._lock:
                if u < self._snapshot_id.get(symbol, 0) + 1:
                    return False
                self._awaiting_sync[symbol] = False
                self._last_u[symbol] = u
        elif is_full:
            with self._lock:
                self._last_u[symbol] = u

        self.store.insert_depth_updates([(
            symbol, int(data["E"]), U, u, pu,
            json.dumps(data["b"]), json.dumps(data["a"]),
        )])
        return True

    def status(self) -> dict:
        with self._lock:
            open_gaps = self.store.count_open_depth_gaps()
            return {
                "last_update_id": dict(self._last_u),
                "snapshot_id": dict(self._snapshot_id),
                "awaiting_sync": dict(self._awaiting_sync),
                "open_gaps": open_gaps,
            }
