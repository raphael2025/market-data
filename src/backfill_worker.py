from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from .config import Config
from .rest_collector import RestCollector
from .storage import MarketStore

log = logging.getLogger(__name__)

# 长历史 K 线：大周期优先，1m 最后
KLINE_INTERVAL_ORDER = [
    "1M", "1w", "3d", "1d", "12h", "8h", "6h", "4h", "2h", "1h",
    "30m", "15m", "5m", "3m", "1m",
]

KLINE_TABLES = [
    ("klines", "/fapi/v1/klines", "symbol"),
    ("mark_price_klines", "/fapi/v1/markPriceKlines", "symbol"),
    ("index_price_klines", "/fapi/v1/indexPriceKlines", "pair"),
    ("continuous_klines", "/fapi/v1/continuousKlines", "pair"),
]


@dataclass
class KlineTask:
    table: str
    path: str
    key_col: str
    key_val: str
    interval: str
    extra: dict = field(default_factory=dict)
    done: bool = False


class BackfillWorker:
    """后台历史回填：短周期数据优先，长历史 K 线分批让路实时采集。"""

    def __init__(self, config: Config, store: MarketStore):
        self.config = config
        self.store = store
        self.rest = RestCollector(config, store)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._kline_tasks: list[KlineTask] = []

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="backfill-worker", daemon=True)
        self._thread.start()
        log.info("后台回填线程已启动（实时采集优先）")

    def stop(self) -> None:
        self._stop.set()

    def _pause(self, seconds: float = 1.5) -> None:
        """每步回填后暂停，把 DB/API 让给实时任务。"""
        if self._stop.wait(seconds):
            return

    def _run_tier(self, name: str, tasks: list[tuple[str, Callable[[], None]]]) -> None:
        log.info("回填阶段 [%s] 开始 (%d 项)", name, len(tasks))
        for label, fn in tasks:
            if self._stop.is_set():
                return
            try:
                log.info("回填任务: %s", label)
                fn()
            except Exception as e:
                log.warning("回填任务 %s 失败: %s", label, e)
            self._pause(1.0)
        log.info("回填阶段 [%s] 完成", name)

    def _build_kline_tasks(self) -> list[KlineTask]:
        tasks: list[KlineTask] = []
        ordered = [iv for iv in KLINE_INTERVAL_ORDER if iv in self.config.kline_intervals]
        for symbol in self.config.symbols:
            pair = self.rest._pair(symbol)
            for table, path, key_col in KLINE_TABLES:
                key_val = symbol if key_col == "symbol" else pair
                extra = {"contractType": "PERPETUAL"} if table == "continuous_klines" else {}
                for interval in ordered:
                    if self.rest.is_kline_complete(table, key_col, key_val, interval):
                        continue
                    tasks.append(KlineTask(table, path, key_col, key_val, interval, extra))
        return tasks

    def _run_kline_round(self) -> bool:
        """轮询执行每个 K 线任务的一批，返回是否还有未完成项。"""
        any_pending = False
        for task in self._kline_tasks:
            if self._stop.is_set():
                return False
            if task.done:
                continue
            any_pending = True
            done, n = self.rest.backfill_kline_batch(task)
            if n:
                log.info(
                    "回填批次 %s %s %s: +%d%s",
                    task.table, task.key_val, task.interval, n,
                    " (完成)" if done else "",
                )
            task.done = done
            self._pause(1.5)
        return any_pending

    def _run(self) -> None:
        try:
            # 阶段1：短窗口（易过期）
            self._run_tier("短周期", [
                ("exchange_info", self.rest.fetch_exchange_info),
                ("agg_trades_24h", self.rest.backfill_agg_trades_24h),
                ("delivery_prices", self.rest.fetch_delivery_prices),
            ])

            # 阶段2：平台保留约30天的统计数据
            self._run_tier("30天统计", [
                ("open_interest_hist", lambda: self.rest.fetch_open_interest_hist(full=True)),
                ("long_short_ratio", lambda: self.rest.fetch_long_short_ratios(full=True)),
                ("basis", lambda: self.rest.fetch_basis(full=True)),
            ])

            # 阶段3：中等历史
            self._run_tier("中等历史", [
                ("funding_rates", lambda: self.rest.fetch_funding_rates(full=True)),
            ])

            # 阶段4：长历史 K 线（分批，大周期优先）
            self._kline_tasks = self._build_kline_tasks()
            log.info("长历史 K 线: %d 个任务待回填", len(self._kline_tasks))

            idle_rounds = 0
            while not self._stop.is_set():
                if self._run_kline_round():
                    idle_rounds = 0
                else:
                    idle_rounds += 1
                    if idle_rounds >= 3:
                        log.info("历史回填全部完成，进入每小时增量维护")
                        break
                    self._pause(5)

            # 完成后定期增量补漏
            while not self._stop.is_set():
                self._pause(3600)
                if self._stop.is_set():
                    break
                log.info("增量维护: 检查 K 线缺口")
                self._kline_tasks = self._build_kline_tasks()
                if self._kline_tasks:
                    self._run_kline_round()
                self.rest.incremental_backfill_round()

            log.info("后台回填线程退出")
        except Exception as e:
            log.exception("后台回填异常: %s", e)
