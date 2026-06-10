"""将 REST 采集移出 APScheduler 线程，避免限流等待阻塞调度器。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Callable

log = logging.getLogger(__name__)


class RestJobRunner:
    """调度器只负责触发；实际 HTTP 在独立线程池执行，同 job 不重叠。"""

    def __init__(self, max_workers: int = 8) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="rest-job"
        )
        self._running: dict[str, bool] = {}
        self._lock = threading.Lock()

    def wrap(self, job_id: str, func: Callable[[], None]) -> Callable[[], None]:
        @wraps(func)
        def trigger() -> None:
            with self._lock:
                if self._running.get(job_id):
                    log.debug("任务 %s 仍在执行，跳过本轮", job_id)
                    return
                self._running[job_id] = True

            def run() -> None:
                try:
                    func()
                except Exception as exc:
                    log.warning("任务 %s 失败: %s", job_id, exc)
                finally:
                    with self._lock:
                        self._running[job_id] = False

            self._executor.submit(run)

        trigger.__name__ = f"rest_job_{job_id}"
        trigger.__qualname__ = f"RestJobRunner.{job_id}"
        return trigger

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)
