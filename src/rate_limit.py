"""币安 REST 权重限流：实时优先，历史回填走低速通道。"""
from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger(__name__)

# 常用端点权重（USDⓈ-M Futures）
ENDPOINT_WEIGHT: dict[str, int] = {
    "/fapi/v1/aggTrades": 20,
    "/fapi/v1/klines": 10,
    "/fapi/v1/markPriceKlines": 10,
    "/fapi/v1/indexPriceKlines": 10,
    "/fapi/v1/continuousKlines": 10,
    "/fapi/v1/depth": 10,
    "/fapi/v1/trades": 5,
    "/fapi/v1/ticker/bookTicker": 2,
    "/fapi/v1/premiumIndex": 1,
    "/fapi/v1/openInterest": 1,
    "/fapi/v1/ticker/24hr": 1,
    "/fapi/v1/ticker/price": 1,
    "/fapi/v1/fundingRate": 1,
    "/fapi/v1/fundingInfo": 1,
    "/fapi/v1/exchangeInfo": 1,
    "/fapi/v1/insuranceBalance": 1,
    "/futures/data/openInterestHist": 1,
    "/futures/data/globalLongShortAccountRatio": 1,
    "/futures/data/topLongShortAccountRatio": 1,
    "/futures/data/topLongShortPositionRatio": 1,
    "/futures/data/takerlongshortRatio": 1,
    "/futures/data/basis": 1,
    "/futures/data/delivery-price": 1,
}

PRIORITY_REALTIME = "realtime"
PRIORITY_BACKFILL = "backfill"

FUTURES_DATA_PREFIX = "/futures/data/"

_global: PriorityRateLimit | None = None
_futures_data: FuturesDataLimit | None = None
_lock = threading.Lock()


class PriorityRateLimit:
    """总权重上限 + 回填独立上限，保证实时 REST/WS 兜底不被历史回填挤占。"""

    def __init__(self, max_total: int = 1800, backfill_cap: int = 350):
        self.max_total = max_total
        self.backfill_cap = backfill_cap
        self._used_total = 0
        self._used_backfill = 0
        self._window_start = time.monotonic()
        self._mutex = threading.Lock()
        self._cond = threading.Condition(self._mutex)

    def _maybe_reset(self, now: float) -> bool:
        if now - self._window_start >= 60.0:
            self._window_start = now
            self._used_total = 0
            self._used_backfill = 0
            self._cond.notify_all()
            return True
        return False

    def acquire(self, weight: int, priority: str = PRIORITY_REALTIME) -> None:
        weight = max(1, weight)
        while True:
            with self._cond:
                now = time.monotonic()
                reset = self._maybe_reset(now)
                if priority == PRIORITY_REALTIME:
                    if self._used_total + weight <= self.max_total:
                        self._used_total += weight
                        return
                elif (
                    self._used_backfill + weight <= self.backfill_cap
                    and self._used_total + weight <= self.max_total
                ):
                    self._used_backfill += weight
                    self._used_total += weight
                    return
                elapsed = now - self._window_start
                wait = max(0.5, 60.0 - elapsed + 0.1)
                if reset:
                    continue
            log.debug(
                "限流等待 priority=%s weight=%d total=%d/%d backfill=%d/%d wait=%.1fs",
                priority, weight, self._used_total, self.max_total,
                self._used_backfill, self.backfill_cap, wait,
            )
            with self._cond:
                self._cond.wait(timeout=min(wait, 5.0))

    def penalize(self, seconds: float) -> None:
        with self._cond:
            self._used_total = self.max_total
            self._used_backfill = self.backfill_cap
            self._cond.notify_all()
        time.sleep(max(1.0, seconds))
        with self._cond:
            self._cond.notify_all()

    def snapshot(self) -> dict:
        with self._cond:
            now = time.monotonic()
            elapsed = now - self._window_start
            return {
                "used_total": self._used_total,
                "used_backfill": self._used_backfill,
                "max_total": self.max_total,
                "backfill_cap": self.backfill_cap,
                "window_elapsed_s": round(elapsed, 1),
                "window_remaining_s": round(max(0.0, 60.0 - elapsed), 1),
            }


class FuturesDataLimit:
    """币安 /futures/data/* 独立配额：1000 次 / 5 分钟（与 REQUEST_WEIGHT 无关）。"""

    def __init__(self, max_requests: int = 900, window_s: float = 300.0):
        self.max_requests = max_requests
        self.window_s = window_s
        self._used = 0
        self._window_start = time.monotonic()
        self._cond = threading.Condition()

    def _maybe_reset(self, now: float) -> bool:
        if now - self._window_start >= self.window_s:
            self._window_start = now
            self._used = 0
            self._cond.notify_all()
            return True
        return False

    def acquire(self) -> None:
        while True:
            with self._cond:
                now = time.monotonic()
                reset = self._maybe_reset(now)
                if self._used + 1 <= self.max_requests:
                    self._used += 1
                    return
                elapsed = now - self._window_start
                wait = max(0.5, self.window_s - elapsed + 0.1)
                if reset:
                    continue
            log.debug(
                "futures/data 限流等待 used=%d/%d wait=%.1fs",
                self._used, self.max_requests, wait,
            )
            with self._cond:
                self._cond.wait(timeout=min(wait, 5.0))

    def penalize(self, seconds: float) -> None:
        with self._cond:
            self._used = self.max_requests
            self._cond.notify_all()
        time.sleep(max(1.0, seconds))
        with self._cond:
            self._cond.notify_all()

    def snapshot(self) -> dict:
        with self._cond:
            now = time.monotonic()
            elapsed = now - self._window_start
            return {
                "used": self._used,
                "max": self.max_requests,
                "window_elapsed_s": round(elapsed, 1),
                "window_remaining_s": round(max(0.0, self.window_s - elapsed), 1),
            }


def get_limiter(max_total: int = 1800, backfill_cap: int = 350) -> PriorityRateLimit:
    global _global
    with _lock:
        if _global is None:
            _global = PriorityRateLimit(max_total, backfill_cap)
        return _global


def get_futures_data_limiter(max_requests: int = 900) -> FuturesDataLimit:
    global _futures_data
    with _lock:
        if _futures_data is None:
            _futures_data = FuturesDataLimit(max_requests)
        return _futures_data


def is_futures_data_path(path: str) -> bool:
    return path.startswith(FUTURES_DATA_PREFIX)


def weight_for(path: str, params: dict | None = None) -> int:
    base = ENDPOINT_WEIGHT.get(path, 5)
    if path == "/fapi/v1/depth" and params:
        limit = int(params.get("limit", 1000))
        if limit <= 100:
            return 5
        if limit <= 500:
            return 10
        return 20
    return base
