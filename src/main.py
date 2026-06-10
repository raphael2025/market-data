from __future__ import annotations

import argparse
import asyncio
import logging
import threading

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler

from .api import create_app
from .backfill_worker import BackfillWorker
from .config import Config
from .rest_collector import RestCollector
from .storage import MarketStore
from .ws_collector import WsCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("market-data")


def run_scheduler(rest: RestCollector, schedules: dict[str, int]) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    jobs = {
        "open_interest": (rest.fetch_open_interest, schedules.get("open_interest", 30)),
        "open_interest_hist": (rest.fetch_open_interest_hist, schedules.get("open_interest_hist", 300)),
        "funding_rate": (rest.fetch_funding_rates, schedules.get("funding_rate", 1800)),
        "funding_info": (rest.fetch_funding_info, schedules.get("funding_info", 3600)),
        "long_short_ratio": (rest.fetch_long_short_ratios, schedules.get("long_short_ratio", 300)),
        "ticker_24h": (rest.fetch_ticker_24h, schedules.get("ticker_24h", 30)),
        "ticker_price": (rest.fetch_ticker_price, schedules.get("ticker_price", 30)),
        "basis": (rest.fetch_basis, schedules.get("basis", 300)),
        "depth_snapshot": (rest.fetch_depth_snapshots, schedules.get("depth_snapshot", 15)),
        "mark_prices_rest": (rest.fetch_mark_prices_rest, schedules.get("mark_prices_rest", 30)),
        "insurance_balance": (rest.fetch_insurance_balance, schedules.get("insurance_balance", 3600)),
        "delivery_price": (rest.fetch_delivery_prices, schedules.get("delivery_price", 3600)),
        "exchange_info": (rest.fetch_exchange_info, schedules.get("exchange_info", 86400)),
        "trades_poll": (rest.fetch_trades, schedules.get("trades_poll", 10)),
        "agg_trades_poll": (rest.fetch_agg_trades_poll, schedules.get("agg_trades_poll", 60)),
    }
    for name, (func, interval) in jobs.items():
        scheduler.add_job(func, "interval", seconds=interval, id=name, max_instances=1)
        log.info("定时任务 %s: 每 %ds", name, interval)
    scheduler.start()
    return scheduler


def start_realtime(config: Config, store: MarketStore) -> tuple[RestCollector, WsCollector, BackgroundScheduler]:
    """立即启动实时采集（优先于历史回填）。"""
    rest = RestCollector(config, store)
    ws = WsCollector(config, store)

    for fn in (
        rest.fetch_open_interest, rest.fetch_ticker_24h, rest.fetch_ticker_price,
        rest.fetch_depth_snapshots, rest.fetch_mark_prices_rest, rest.fetch_trades,
        rest.fetch_funding_info,
    ):
        try:
            fn()
        except Exception as e:
            log.warning("初始采集 %s 失败: %s", fn.__name__, e)

    scheduler = run_scheduler(rest, config.schedules)

    def ws_thread():
        asyncio.run(ws.run())

    threading.Thread(target=ws_thread, name="ws-collector", daemon=True).start()
    log.info("实时采集已启动 (WebSocket + REST 定时任务)")
    return rest, ws, scheduler


def run_collector(config: Config, background_backfill: bool = True) -> None:
    store = MarketStore(config.db_path)
    rest, ws, scheduler = start_realtime(config, store)

    backfill_worker: BackfillWorker | None = None
    if background_backfill:
        backfill_worker = BackfillWorker(config, store)
        backfill_worker.start()

    read_store = MarketStore(config.db_path, read_only=True)
    app = create_app(config, read_store)
    log.info("API: http://%s:%d  数据库: %s", config.api_host, config.api_port, config.db_path)
    try:
        uvicorn.run(app, host=config.api_host, port=config.api_port, log_level="info")
    finally:
        ws.stop()
        if backfill_worker:
            backfill_worker.stop()
        scheduler.shutdown(wait=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="币安合约全量行情采集服务")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--backfill-only", action="store_true", help="阻塞式全量回填")
    parser.add_argument("--no-backfill", action="store_true", help="不启动后台回填")
    parser.add_argument("--api-only", action="store_true", help="仅 API")
    args = parser.parse_args()

    config = Config.load() if not args.config else Config.load(
        __import__("pathlib").Path(args.config)
    )

    if args.api_only:
        app = create_app(config, MarketStore(config.db_path, read_only=True))
        uvicorn.run(app, host=config.api_host, port=config.api_port)
        return

    if args.backfill_only:
        store = MarketStore(config.db_path)
        BackfillWorker(config, store)._run()
        return

    run_collector(config, background_backfill=not args.no_backfill)


if __name__ == "__main__":
    main()
