"""WS 流活性追踪：无历史 / 极短窗口数据优先保障。"""
from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config

_guardian: StreamGuardian | None = None


def get_stream_guardian() -> StreamGuardian | None:
    return _guardian


def set_stream_guardian(guardian: StreamGuardian) -> None:
    global _guardian
    _guardian = guardian


class StreamGuardian:
    """记录各流最后事件时间，供看门狗判定僵死连接。"""

    def __init__(self, config: Config):
        self._config = config
        self._lock = threading.Lock()
        self._last: dict[str, float] = {}
        self._forced_reconnects: dict[str, int] = {}

    def touch(self, kind: str, symbol: str | None = None) -> None:
        key = f"{kind}:{symbol.upper()}" if symbol else kind
        with self._lock:
            self._last[key] = time.monotonic()

    def touch_conn(self, conn: str) -> None:
        self.touch(f"conn:{conn}")

    def record_reconnect(self, conn: str) -> None:
        with self._lock:
            self._forced_reconnects[conn] = self._forced_reconnects.get(conn, 0) + 1

    def all_symbols_stale(self, kind: str, symbols: list[str], timeout_s: float) -> bool:
        """全部 symbol 超过 timeout 视为该连接僵死。"""
        if not symbols or timeout_s <= 0:
            return False
        with self._lock:
            now = time.monotonic()
            for sym in symbols:
                key = f"{kind}:{sym.upper()}"
                if now - self._last.get(key, 0.0) < timeout_s:
                    return False
            return True

    def snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            ages = {k: round(now - t, 1) for k, t in self._last.items()}
            return {
                "last_age_s": ages,
                "forced_reconnects": dict(self._forced_reconnects),
            }
