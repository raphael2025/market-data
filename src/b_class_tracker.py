"""
B 类数据 24小时窗口追踪器

B 类数据特点：
- 成交数据（trades/agg_trades）仅 REST 可获 24h 窗口
- 币安 24h 后自动删除数据，无法恢复
- 如中断 > 2h，数据永久丢失

策略：
1. 启动时一次性回填 24h 所有成交
2. 持续每 10s 轮询最近数据（增量追踪）
3. 追踪游标（ID），支持断点续传
4. 中断 > 2h 时触发告警
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .multi_ip_fetcher import MultiIPFetcher, RequestPriority
from .storage import MarketStore

log = logging.getLogger(__name__)


@dataclass
class BClassCursor:
    """B 类数据游标追踪"""
    symbol: str
    data_type: str  # "trades" or "agg_trades"
    
    # 游标信息
    last_id: Optional[int] = None
    last_timestamp: int = 0  # ms
    last_update_time: float = field(default_factory=time.time)  # 最后一次成功获取的系统时间
    
    # 统计
    total_rows: int = 0
    batch_count: int = 0
    
    def is_stale(self, threshold_seconds: int = 7200) -> bool:
        """检查是否超期（默认 2 小时）"""
        now = time.time()
        return (now - self.last_update_time) > threshold_seconds
    
    def seconds_since_update(self) -> float:
        """距离最后更新的秒数"""
        return time.time() - self.last_update_time


class BClassTracker:
    """B 类数据 24h 窗口追踪和保护"""
    
    def __init__(
        self,
        store: MarketStore,
        multi_ip: MultiIPFetcher,
        rest_base: str = "https://fapi.binance.com",
        state_dir: Path = Path("data/b_class_state"),
    ):
        self.store = store
        self.multi_ip = multi_ip
        self.rest_base = rest_base
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # 游标追踪
        self.cursors: dict[tuple[str, str], BClassCursor] = {}
        self._load_state()
    
    def _state_file(self, symbol: str, data_type: str) -> Path:
        """获取游标状态文件"""
        return self.state_dir / f"{symbol}_{data_type}_cursor.json"
    
    def _load_state(self) -> None:
        """从磁盘加载游标状态"""
        for state_file in self.state_dir.glob("*_cursor.json"):
            try:
                data = json.loads(state_file.read_text())
                cursor = BClassCursor(**data)
                self.cursors[(cursor.symbol, cursor.data_type)] = cursor
                log.info(
                    f"加载游标: {cursor.symbol}/{cursor.data_type} "
                    f"(last_id={cursor.last_id}, rows={cursor.total_rows})"
                )
            except Exception as e:
                log.warning(f"加载游标失败 {state_file}: {e}")
    
    def _save_state(self, cursor: BClassCursor) -> None:
        """保存游标状态到磁盘"""
        state_file = self._state_file(cursor.symbol, cursor.data_type)
        data = {
            "symbol": cursor.symbol,
            "data_type": cursor.data_type,
            "last_id": cursor.last_id,
            "last_timestamp": cursor.last_timestamp,
            "last_update_time": cursor.last_update_time,
            "total_rows": cursor.total_rows,
            "batch_count": cursor.batch_count,
        }
        state_file.write_text(json.dumps(data, indent=2))
    
    def _parse_trades(self, symbol: str, data: list[dict]) -> list[tuple]:
        """解析逐笔成交"""
        return [
            (
                symbol,
                int(t["id"]),
                float(t["price"]),
                float(t["qty"]),
                float(t.get("quoteQty", 0)),
                int(t["time"]),
                int(t["isBuyerMaker"]),
            )
            for t in data
        ]
    
    def _parse_agg_trades(self, symbol: str, data: list[dict]) -> list[tuple]:
        """解析聚合成交"""
        return [
            (
                symbol,
                int(t["a"]),  # agg_id
                float(t["p"]),
                float(t["q"]),
                int(t["T"]),
                int(t["m"]),
            )
            for t in data
        ]
    
    async def backfill_24h_trades(
        self,
        symbol: str,
        force: bool = False,
    ) -> int:
        """
        启动时回填最近 24h 所有成交
        
        Args:
            symbol: 交易对
            force: 强制重新回填
        
        Returns:
            回填的总行数
        """
        key = (symbol, "trades")
        cursor = self.cursors.get(key) or BClassCursor(symbol, "trades")
        
        # 检查是否需要回填
        if not force and cursor.last_id is not None:
            log.info(f"{symbol}/trades 已回填, 跳过 (last_id={cursor.last_id})")
            return 0
        
        log.info(f"开始回填 24h trades: {symbol}")
        
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 86_400_000  # 24h ago
        
        current_cursor = start_ms
        total_rows = 0
        batch_count = 0
        
        try:
            while current_cursor < now_ms:
                # 拉取最多 1000 条
                url = f"{self.rest_base}/fapi/v1/trades"
                params = {
                    "symbol": symbol,
                    "startTime": current_cursor,
                    "limit": 1000,
                }
                
                data = await self.multi_ip.fetch_async(
                    url,
                    params=params,
                    weight=1,
                    priority=RequestPriority.PRIORITY_B,
                )
                
                if not data:
                    log.info(f"[{symbol}/trades] 回填完成（无更多数据）")
                    break
                
                # 解析并保存
                rows = self._parse_trades(symbol, data)
                self.store.insert_trades(rows)
                
                total_rows += len(rows)
                batch_count += 1
                
                # 更新游标
                last_trade = data[-1]
                current_cursor = int(last_trade["time"]) + 1
                
                # 每 10 批保存一次检查点
                if batch_count % 10 == 0:
                    cursor.last_id = int(last_trade["id"])
                    cursor.last_timestamp = int(last_trade["time"])
                    cursor.total_rows = total_rows
                    cursor.batch_count = batch_count
                    cursor.last_update_time = time.time()
                    self._save_state(cursor)
                    
                    log.info(
                        f"[{symbol}/trades] 回填进度: {total_rows} 行, "
                        f"cursor={current_cursor}, batch={batch_count}"
                    )
            
            # 最后保存状态
            cursor.last_id = int(data[-1]["id"]) if data else cursor.last_id
            cursor.last_timestamp = int(data[-1]["time"]) if data else cursor.last_timestamp
            cursor.total_rows = total_rows
            cursor.batch_count = batch_count
            cursor.last_update_time = time.time()
            self.cursors[key] = cursor
            self._save_state(cursor)
            
            log.info(f"✓ {symbol}/trades 24h 回填完成: {total_rows} 行")
        
        except Exception as e:
            log.error(f"✗ {symbol}/trades 回填失败: {e}")
            raise
        
        return total_rows
    
    async def poll_trades_incremental(
        self,
        symbol: str,
        interval: int = 10,
    ) -> None:
        """
        持续增量采集成交，保证无遗漏
        
        Args:
            symbol: 交易对
            interval: 轮询间隔（秒）
        """
        key = (symbol, "trades")
        cursor = self.cursors.get(key) or BClassCursor(symbol, "trades")
        self.cursors[key] = cursor
        
        log.info(f"启动 {symbol}/trades 增量追踪 (interval={interval}s)")
        
        while True:
            try:
                # 获取最近 1000 条
                url = f"{self.rest_base}/fapi/v1/trades"
                params = {"symbol": symbol, "limit": 1000}
                
                data = await self.multi_ip.fetch_async(
                    url,
                    params=params,
                    weight=1,
                    priority=RequestPriority.PRIORITY_B,
                )
                
                if data:
                    # 按 ID 过滤已有数据
                    new_rows = [
                        t for t in data
                        if cursor.last_id is None or int(t["id"]) > cursor.last_id
                    ]
                    
                    if new_rows:
                        rows = self._parse_trades(symbol, new_rows)
                        self.store.insert_trades(rows)
                        
                        # 更新游标
                        cursor.last_id = int(new_rows[-1]["id"])
                        cursor.last_timestamp = int(new_rows[-1]["time"])
                        cursor.total_rows += len(new_rows)
                        cursor.batch_count += 1
                        cursor.last_update_time = time.time()
                        
                        self._save_state(cursor)
                        
                        log.debug(
                            f"[{symbol}/trades] 增量: +{len(new_rows)} 行, "
                            f"last_id={cursor.last_id}"
                        )
                    else:
                        log.debug(f"[{symbol}/trades] 无新数据")
                
                # 检查超期告警
                if cursor.is_stale(threshold_seconds=7200):
                    log.warning(
                        f"⚠️ [{symbol}/trades] 已 {cursor.seconds_since_update():.0f}s "
                        f"未更新，即将丢失历史数据!"
                    )
                
                await asyncio.sleep(interval)
            
            except Exception as e:
                log.error(f"[{symbol}/trades] 增量采集失败: {e}")
                # 继续重试
                await asyncio.sleep(interval * 2)
    
    async def backfill_24h_agg_trades(
        self,
        symbol: str,
        force: bool = False,
    ) -> int:
        """回填 24h 聚合成交（同 trades，但有 WebSocket 支持）"""
        key = (symbol, "agg_trades")
        cursor = self.cursors.get(key) or BClassCursor(symbol, "agg_trades")
        
        if not force and cursor.last_id is not None:
            log.info(f"{symbol}/agg_trades 已回填, 跳过")
            return 0
        
        log.info(f"开始回填 24h agg_trades: {symbol}")
        
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 86_400_000
        
        current_cursor = start_ms
        total_rows = 0
        batch_count = 0
        
        try:
            while current_cursor < now_ms:
                url = f"{self.rest_base}/fapi/v1/aggTrades"
                params = {
                    "symbol": symbol,
                    "startTime": current_cursor,
                    "limit": 1000,
                }
                
                data = await self.multi_ip.fetch_async(
                    url,
                    params=params,
                    weight=1,
                    priority=RequestPriority.PRIORITY_B,
                )
                
                if not data:
                    break
                
                rows = self._parse_agg_trades(symbol, data)
                self.store.insert_agg_trades(rows)
                
                total_rows += len(rows)
                batch_count += 1
                
                last_trade = data[-1]
                current_cursor = int(last_trade["T"]) + 1
                
                if batch_count % 10 == 0:
                    cursor.last_id = int(last_trade["a"])
                    cursor.last_timestamp = int(last_trade["T"])
                    cursor.total_rows = total_rows
                    cursor.batch_count = batch_count
                    cursor.last_update_time = time.time()
                    self._save_state(cursor)
            
            cursor.last_id = int(data[-1]["a"]) if data else cursor.last_id
            cursor.last_timestamp = int(data[-1]["T"]) if data else cursor.last_timestamp
            cursor.total_rows = total_rows
            cursor.batch_count = batch_count
            cursor.last_update_time = time.time()
            self.cursors[key] = cursor
            self._save_state(cursor)
            
            log.info(f"✓ {symbol}/agg_trades 24h 回填完成: {total_rows} 行")
        
        except Exception as e:
            log.error(f"✗ {symbol}/agg_trades 回填失败: {e}")
            raise
        
        return total_rows
    
    async def start_b_class_collection(
        self,
        symbols: list[str],
        backfill: bool = True,
    ) -> None:
        """
        启动 B 类数据采集
        
        包括：
        1. 24h 回填 (一次性)
        2. 增量追踪 (持续)
        """
        tasks = []
        
        for symbol in symbols:
            # 回填 24h
            if backfill:
                try:
                    await self.backfill_24h_trades(symbol)
                    await self.backfill_24h_agg_trades(symbol)
                except Exception as e:
                    log.error(f"回填 {symbol} 失败: {e}")
            
            # 启动增量追踪任务
            tasks.append(self.poll_trades_incremental(symbol, interval=10))
        
        # 并行运行所有增量追踪
        await asyncio.gather(*tasks)
    
    def get_cursor_status(self) -> dict:
        """获取所有游标的状态"""
        status = {}
        for (symbol, data_type), cursor in self.cursors.items():
            key = f"{symbol}/{data_type}"
            status[key] = {
                "last_id": cursor.last_id,
                "last_timestamp": cursor.last_timestamp,
                "total_rows": cursor.total_rows,
                "batch_count": cursor.batch_count,
                "seconds_since_update": cursor.seconds_since_update(),
                "is_stale": cursor.is_stale(),
            }
        return status
    
    def log_cursor_status(self) -> None:
        """打印游标状态"""
        status = self.get_cursor_status()
        log.info("=== B 类数据游标状态 ===")
        for key, info in status.items():
            stale = "⚠️ STALE" if info["is_stale"] else "✓"
            log.info(
                f"{key:20} {stale} last_id={info['last_id']:>10} "
                f"rows={info['total_rows']:>8} age={info['seconds_since_update']:.0f}s"
            )
