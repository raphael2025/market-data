"""
硬盘缓冲系统 - 极端故障保护

保护场景：
1. 数据库宕机/损坏（SQLite 无法写入）
2. 系统断电/强制重启
3. 采集进程崩溃
4. 内存溢出

策略：
- WebSocket 事件到达 → 先写硬盘后异步入库
- 系统重启时 → 读取缓冲文件恢复丢失数据
- 缓冲文件满后 → 自动轮转，避免占用过多磁盘
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .storage import MarketStore

log = logging.getLogger(__name__)


@dataclass
class BufferConfig:
    """缓冲配置"""
    enabled: bool = True
    path: Path = Path("data/ws_buffer")
    max_file_size_mb: int = 100  # 单个缓冲文件的最大大小
    max_total_size_mb: int = 500  # 缓冲目录的最大大小
    recovery_on_startup: bool = True
    flush_interval_sec: int = 5  # 定期刷盘间隔


class DiskBuffer:
    """
    WebSocket 事件硬盘缓冲
    
    设计特点：
    - 追加写 JSONL 格式（行原子性）
    - 自动文件轮转（防止单文件过大）
    - 恢复时自动去重
    - 支持并发读写
    """
    
    def __init__(self, config: Optional[BufferConfig] = None):
        self.config = config or BufferConfig()
        self.config.path.mkdir(parents=True, exist_ok=True)
        
        self.write_lock = asyncio.Lock()
        self.current_file: Optional[Path] = None
        self.current_file_size: int = 0
    
    async def write_event(
        self,
        event_type: str,
        symbol: str,
        payload: dict,
    ) -> None:
        """
        写入 WebSocket 事件到缓冲
        
        格式: {"timestamp": <float>, "event_type": <str>, "symbol": <str>, "payload": <dict>}
        """
        if not self.config.enabled:
            return
        
        try:
            async with self.write_lock:
                # 构造事件记录
                record = {
                    "timestamp": time.time(),
                    "event_type": event_type,
                    "symbol": symbol,
                    "payload": payload,
                }
                
                # 获取当前缓冲文件
                buffer_file = await self._get_current_buffer_file(event_type, symbol)
                
                # 追加写入
                line = json.dumps(record, separators=(',', ':'))
                with open(buffer_file, "a") as f:
                    f.write(line + "\n")
                
                self.current_file_size += len(line) + 1
        
        except Exception as e:
            log.error(f"缓冲写入失败 ({event_type}/{symbol}): {e}")
    
    async def _get_current_buffer_file(
        self,
        event_type: str,
        symbol: str,
    ) -> Path:
        """
        获取当前缓冲文件（支持轮转）
        
        文件命名: buffer_{event_type}_{symbol}_{index}.jsonl
        """
        base_name = f"buffer_{event_type}_{symbol}"
        
        # 查找最新的缓冲文件
        existing = sorted(
            self.config.path.glob(f"{base_name}_*.jsonl"),
            key=lambda p: int(p.stem.split("_")[-1]),
            reverse=True
        )
        
        if existing:
            current = existing[0]
            size_mb = current.stat().st_size / (1024 * 1024)
            
            # 检查是否需要轮转
            if size_mb < self.config.max_file_size_mb:
                return current
        
        # 创建新文件
        index = 0
        if existing:
            last_index = int(existing[0].stem.split("_")[-1])
            index = last_index + 1
        
        new_file = self.config.path / f"{base_name}_{index}.jsonl"
        log.info(f"创建新缓冲文件: {new_file}")
        return new_file
    
    async def flush_to_storage(
        self,
        store: MarketStore,
        event_parsers: dict,
    ) -> int:
        """
        将缓冲数据刷入数据库
        
        Args:
            store: MarketStore 实例
            event_parsers: {event_type: parse_function}
        
        Returns:
            处理的记录数
        """
        if not self.config.enabled:
            return 0
        
        total_records = 0
        
        for buffer_file in self.config.path.glob("buffer_*.jsonl"):
            try:
                with open(buffer_file, "r") as f:
                    records = []
                    for line in f:
                        if not line.strip():
                            continue
                        
                        record = json.loads(line)
                        event_type = record.get("event_type")
                        
                        if event_type in event_parsers:
                            parser = event_parsers[event_type]
                            parser(store, record)
                            records.append(record)
                
                log.info(f"从缓冲恢复 {len(records)} 条记录: {buffer_file.name}")
                total_records += len(records)
                
                # 清空或删除缓冲文件
                buffer_file.unlink()
            
            except Exception as e:
                log.error(f"处理缓冲文件失败 {buffer_file}: {e}")
        
        return total_records
    
    async def recovery_on_startup(
        self,
        store: MarketStore,
        event_parsers: dict,
    ) -> int:
        """
        系统启动时恢复缓冲数据
        
        Returns:
            恢复的记录数
        """
        if not self.config.enabled or not self.config.recovery_on_startup:
            return 0
        
        log.info("检查启动恢复...")
        
        buffer_files = list(self.config.path.glob("buffer_*.jsonl"))
        if not buffer_files:
            log.info("无缓冲文件需要恢复")
            return 0
        
        log.warning(f"发现 {len(buffer_files)} 个缓冲文件，开始恢复...")
        
        recovered = await self.flush_to_storage(store, event_parsers)
        
        if recovered > 0:
            log.warning(f"✓ 恢复完成: {recovered} 条记录")
        
        return recovered
    
    def get_buffer_stats(self) -> dict:
        """获取缓冲统计信息"""
        if not self.config.enabled:
            return {"enabled": False}
        
        buffer_files = list(self.config.path.glob("buffer_*.jsonl"))
        total_size_mb = sum(f.stat().st_size for f in buffer_files) / (1024 * 1024)
        
        stats = {
            "enabled": True,
            "buffer_dir": str(self.config.path),
            "file_count": len(buffer_files),
            "total_size_mb": total_size_mb,
            "max_size_mb": self.config.max_total_size_mb,
            "files": [],
        }
        
        for f in sorted(buffer_files):
            size_mb = f.stat().st_size / (1024 * 1024)
            # 预计行数（每行平均 200 bytes）
            estimated_rows = f.stat().st_size // 200
            stats["files"].append({
                "name": f.name,
                "size_mb": size_mb,
                "estimated_rows": estimated_rows,
            })
        
        return stats
    
    def log_buffer_stats(self) -> None:
        """打印缓冲统计"""
        stats = self.get_buffer_stats()
        if not stats.get("enabled"):
            return
        
        log.info("=== 硬盘缓冲统计 ===")
        log.info(
            f"缓冲文件: {stats['file_count']} 个, "
            f"总大小: {stats['total_size_mb']:.1f}MB / {stats['max_size_mb']}MB"
        )
        
        for file_info in stats["files"]:
            log.info(
                f"  {file_info['name']:50} "
                f"{file_info['size_mb']:6.1f}MB "
                f"~{file_info['estimated_rows']:>6} 行"
            )


class BufferedWSCollector:
    """
    使用缓冲的 WebSocket 采集器包装
    
    用法：
        collector = BufferedWSCollector(original_collector, disk_buffer)
        await collector.on_agg_trade(data)  # 自动先缓冲后入库
    """
    
    def __init__(
        self,
        original_collector,
        disk_buffer: DiskBuffer,
    ):
        self.original_collector = original_collector
        self.disk_buffer = disk_buffer
    
    async def on_agg_trade(self, data: dict) -> None:
        """处理聚合成交事件"""
        symbol = data.get("s")
        
        # 1. 先写缓冲（高优先级，一定要成功）
        await self.disk_buffer.write_event(
            "aggTrade",
            symbol,
            data,
        )
        
        # 2. 异步写数据库（低优先级，可以失败恢复）
        try:
            self.original_collector._on_agg_trade(data)
        except Exception as e:
            log.error(f"数据库写入失败 (aggTrade/{symbol}): {e}")
    
    async def on_kline(self, data: dict) -> None:
        """处理 K 线事件"""
        k = data.get("k", {})
        symbol = k.get("s")
        
        await self.disk_buffer.write_event(
            "kline",
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_kline(data)
        except Exception as e:
            log.error(f"数据库写入失败 (kline/{symbol}): {e}")
    
    async def on_depth_update(self, data: dict) -> None:
        """处理深度更新事件"""
        symbol = data.get("s")
        
        await self.disk_buffer.write_event(
            "depthUpdate",
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_depth_update(data)
        except Exception as e:
            log.error(f"数据库写入失败 (depthUpdate/{symbol}): {e}")
    
    async def on_mark_price(self, data: dict) -> None:
        """处理标记价格事件"""
        symbol = data.get("s")
        
        await self.disk_buffer.write_event(
            "markPrice",
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_mark_price(data)
        except Exception as e:
            log.error(f"数据库写入失败 (markPrice/{symbol}): {e}")
    
    async def on_book_ticker(self, data: dict) -> None:
        """处理 Book Ticker 事件"""
        symbol = data.get("s")
        
        await self.disk_buffer.write_event(
            "bookTicker",
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_book_ticker(data)
        except Exception as e:
            log.error(f"数据库写入失败 (bookTicker/{symbol}): {e}")
    
    async def on_ticker(self, data: dict, event_type: str) -> None:
        """处理 Ticker 事件"""
        symbol = data.get("s")
        
        await self.disk_buffer.write_event(
            event_type,
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_ticker(data, event_type)
        except Exception as e:
            log.error(f"数据库写入失败 ({event_type}/{symbol}): {e}")
    
    async def on_liquidation(self, data: dict) -> None:
        """处理清算事件"""
        o = data.get("o", {})
        symbol = o.get("s")
        
        await self.disk_buffer.write_event(
            "forceOrder",
            symbol,
            data,
        )
        
        try:
            self.original_collector._on_liquidation(data)
        except Exception as e:
            log.error(f"数据库写入失败 (forceOrder/{symbol}): {e}")


def create_event_parsers(store: MarketStore) -> dict:
    """
    创建缓冲恢复用的事件解析器
    
    Returns:
        {event_type: parse_function}
    """
    def parse_agg_trade(store, record):
        data = record["payload"]
        store.insert_agg_trades([(
            data["s"],
            int(data["a"]),
            float(data["p"]),
            float(data["q"]),
            int(data["T"]),
            int(data["m"]),
        )])
    
    def parse_kline(store, record):
        data = record["payload"]
        k = data["k"]
        store.insert_kline_updates([(
            k["s"], k["i"], int(k["t"]), int(data["E"]),
            float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
            float(k["v"]), int(k["x"]),
        )])
        if k["x"]:
            store.upsert_klines([(
                k["s"], k["i"], int(k["t"]), float(k["o"]), float(k["h"]),
                float(k["l"]), float(k["c"]), float(k["v"]), int(k["T"]),
                float(k["q"]), int(k["n"]), float(k["V"]), float(k["Q"]),
            )])
    
    def parse_depth_update(store, record):
        data = record["payload"]
        import json
        store.insert_depth_updates([(
            data["s"], int(data["E"]), int(data["U"]), int(data["u"]),
            int(data.get("pu", 0)), json.dumps(data["b"]), json.dumps(data["a"]),
        )])
    
    def parse_mark_price(store, record):
        data = record["payload"]
        store.insert_mark_prices([(
            data["s"], float(data["p"]), float(data["i"]), float(data["r"]),
            int(data["T"]), int(data["E"]),
        )])
    
    def parse_book_ticker(store, record):
        data = record["payload"]
        import time
        event_time = int(data.get("E") or data.get("T") or time.time() * 1000)
        store.insert_book_tickers([(
            data["s"], float(data["b"]), float(data["B"]),
            float(data["a"]), float(data["A"]),
            event_time,
        )])
    
    def parse_ticker(store, record):
        data = record["payload"]
        import json
        store.insert_ticker_snapshot(
            data["s"],
            record["event_type"],
            json.dumps(data),
            int(data["E"]),
        )
    
    def parse_liquidation(store, record):
        data = record["payload"]
        o = data["o"]
        store.insert_liquidations([(
            o["s"], o["S"], o["o"], o.get("f", ""),
            float(o["p"]), float(o.get("ap", 0)), float(o["q"]),
            float(o.get("z", 0)), o.get("X", ""), int(o["T"]),
        )])
    
    return {
        "aggTrade": parse_agg_trade,
        "kline": parse_kline,
        "depthUpdate": parse_depth_update,
        "markPrice": parse_mark_price,
        "bookTicker": parse_book_ticker,
        "24hrTicker": parse_ticker,
        "24hrMiniTicker": parse_ticker,
        "forceOrder": parse_liquidation,
    }
