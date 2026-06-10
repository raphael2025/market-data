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
    """后台历史回填：24h 以外低速回填，实时数据（WS/REST 兜底）优先。"""

    def __init__(self, config: Config, store: MarketStore, rest: RestCollector | None = None):
        self.config = config
        self.store = store
        self.rest = rest if rest is not None else RestCollector(config, store)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._kline_tasks: list[KlineTask] = []

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="backfill-worker", daemon=True)
        self._thread.start()
        log.info(
            "后台回填线程已启动（历史低速: sleep=%.0fs, 每轮最多 %d 批）",
            self.config.backfill_historical_sleep,
            self.config.backfill_batches_per_round,
        )

    def stop(self) -> None:
        self._stop.set()

    def _pause(self, seconds: float) -> None:
        if self._stop.wait(seconds):
            return

    def _run_tier(self, name: str, tasks: list[tuple[str, Callable[[], None]]]) -> None:
        log.info("回填阶段 [%s] 开始 (%d 项)", name, len(tasks))
        with self.rest.backfill_scope():
            for label, fn in tasks:
                if self._stop.is_set():
                    return
                try:
                    log.info("回填任务: %s", label)
                    fn()
                except Exception as e:
                    log.warning("回填任务 %s 失败: %s", label, e)
                self._pause(self.config.backfill_recent_sleep)
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
        """每轮最多执行 batches_per_round 批，避免挤占实时 REST。"""
        any_pending = False
        batches = 0
        max_batches = self.config.backfill_batches_per_round

        with self.rest.backfill_scope():
            for task in self._kline_tasks:
                if self._stop.is_set():
                    return False
                if task.done:
                    continue
                any_pending = True
                if batches >= max_batches:
                    log.debug("本轮回填已达上限 %d 批，让路实时采集", max_batches)
                    return True

                done, n, is_historical = self.rest.backfill_kline_batch(task)
                if n:
                    tag = "历史" if is_historical else "近期"
                    log.info(
                        "回填批次 [%s] %s %s %s: +%d%s",
                        tag, task.table, task.key_val, task.interval, n,
                        " (完成)" if done else "",
                    )
                if done:
                    task.done = True
                batches += 1
                # backfill_kline_batch 内部已 sleep；历史批次额外让路
                if is_historical:
                    self._pause(2.0)

        return any_pending

    def _run(self) -> None:
        try:
            # P1：24h 极短 REST 窗口（启动时一次性拉满）
            self._run_tier("短窗口", [
                ("exchange_info", self.rest.fetch_exchange_info),
                ("agg_trades_24h", self.rest.backfill_agg_trades_24h),
            ])

            # 让路 P0/P1 实时采集
            delay = self.config.backfill_startup_delay_s
            if delay > 0:
                log.info("长历史回填延迟 %ds，优先保障 WS/极短 REST", delay)
                self._pause(delay)

            # P3：长历史 K 线 — 最低优先级、每轮限量
            self._kline_tasks = self._build_kline_tasks()
            log.info("长历史 K 线: %d 个任务待低速回填", len(self._kline_tasks))

            idle_rounds = 0
            while not self._stop.is_set():
                if self._run_kline_round():
                    idle_rounds = 0
                    self._pause(self.config.backfill_historical_sleep)
                else:
                    idle_rounds += 1
                    if idle_rounds >= 3:
                        log.info("K 线历史回填完成")
                        break
                    self._pause(5)

            # P2/P3：K 线完成后才做 30 天统计与资金费率全量（定时任务负责增量）
            if not self.config.backfill_defer_30d_full:
                self._run_tier("30天统计", [
                    ("open_interest_hist", lambda: self.rest.fetch_open_interest_hist(full=True)),
                    ("long_short_ratio", lambda: self.rest.fetch_long_short_ratios(full=True)),
                    ("basis", lambda: self.rest.fetch_basis(full=True)),
                ])
                self._run_tier("中等历史", [
                    ("funding_rates", lambda: self.rest.fetch_funding_rates(full=True)),
                    ("delivery_prices", self.rest.fetch_delivery_prices),
                ])
            else:
                log.info("30 天统计全量回填已推迟，由定时 REST 增量维护")

            while not self._stop.is_set():
                self._pause(3600)
                if self._stop.is_set():
                    break
                log.info("增量维护: 检查 K 线缺口")
                self._kline_tasks = self._build_kline_tasks()
                if self._kline_tasks:
                    self._run_kline_round()
                with self.rest.backfill_scope():
                    self.rest.incremental_backfill_round()

            log.info("后台回填线程退出")
        except Exception as e:
            log.exception("后台回填异常: %s", e)
