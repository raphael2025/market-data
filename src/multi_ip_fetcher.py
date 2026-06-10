"""
Multi-IP 代理轮转器 - 分散币安 API 限速

策略：
1. 维护多个独立 IP 的配速器
2. 每次请求选择最优 IP（余量最多或恢复最快）
3. 支持优先级分离（实时 vs 回填）
4. 自动故障转移
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiohttp
import requests

log = logging.getLogger(__name__)


class RequestPriority(str, Enum):
    """请求优先级"""
    REALTIME = "realtime"  # A 类实时采集
    PRIORITY_B = "priority_b"  # B 类 24h 窗口
    NORMAL = "normal"  # C 类 30day
    BACKFILL = "backfill"  # D 类长期回填


@dataclass
class RateLimitBucket:
    """单个 IP 的配速桶"""
    ip: str
    max_weight_per_min: int = 2400
    max_requests_per_sec: int = 10
    
    weight_consumed: int = 0
    request_count: int = 0
    last_reset_time: float = field(default_factory=time.time)
    last_request_time: float = field(default_factory=time.time)
    
    penalty_until: float = 0  # 429 冷却时间
    unhealthy_until: float = 0  # 标记不健康的时间
    
    def reset_if_needed(self) -> None:
        """按分钟重置配额"""
        now = time.time()
        if now - self.last_reset_time > 60:
            self.weight_consumed = 0
            self.request_count = 0
            self.last_reset_time = now
    
    def available_weight(self) -> int:
        """可用权重"""
        self.reset_if_needed()
        return max(0, self.max_weight_per_min - self.weight_consumed)
    
    def can_acquire(self, weight: int) -> bool:
        """是否可以获取指定权重"""
        self.reset_if_needed()
        now = time.time()
        
        # 检查 429 冷却
        if now < self.penalty_until:
            return False
        
        # 检查不健康状态
        if now < self.unhealthy_until:
            return False
        
        # 检查权重
        if self.weight_consumed + weight > self.max_weight_per_min:
            return False
        
        # 检查请求频率
        if self.request_count >= self.max_requests_per_sec:
            time_since_last = now - self.last_request_time
            if time_since_last < 1.0:
                return False
        
        return True
    
    def acquire(self, weight: int) -> bool:
        """尝试获取权重"""
        if not self.can_acquire(weight):
            return False
        
        self.weight_consumed += weight
        self.request_count += 1
        self.last_request_time = time.time()
        return True
    
    def penalize(self, seconds: int) -> None:
        """429 冷却"""
        self.penalty_until = time.time() + seconds
        log.warning(f"IP {self.ip} 被限速 {seconds}s")
    
    def mark_unhealthy(self, duration: int = 30) -> None:
        """标记 IP 不健康"""
        self.unhealthy_until = time.time() + duration
        log.warning(f"IP {self.ip} 标记为不健康 {duration}s")
    
    def recovery_time(self) -> float:
        """距离恢复的时间（秒）"""
        now = time.time()
        return max(
            0,
            max(self.penalty_until, self.unhealthy_until) - now
        )
    
    def health_score(self) -> float:
        """0-100 的健康评分"""
        now = time.time()
        score = 100.0
        
        # 冷却中
        if now < self.penalty_until:
            score -= 50
        
        # 不健康中
        if now < self.unhealthy_until:
            score -= 30
        
        # 权重消耗
        self.reset_if_needed()
        score -= (self.weight_consumed / self.max_weight_per_min) * 20
        
        return max(0, score)


class MultiIPFetcher:
    """多 IP 轮转获取器"""
    
    def __init__(
        self,
        ip_list: list[str],
        config: Optional[dict] = None,
    ):
        """
        初始化多 IP 获取器
        
        Args:
            ip_list: IP 地址列表（可以是代理地址或本地 IP）
            config: 配置字典
                {
                    "max_weight_per_min": 2400,
                    "max_requests_per_sec": 10,
                    "priority_weights": {  # 不同优先级的权重分配
                        "realtime": 1200,
                        "priority_b": 600,
                        "normal": 400,
                        "backfill": 200,
                    }
                }
        """
        self.ip_list = ip_list
        self.config = config or {}
        
        # 为每个 IP 创建配速器
        self.buckets = {
            ip: RateLimitBucket(
                ip=ip,
                max_weight_per_min=self.config.get("max_weight_per_min", 2400),
            )
            for ip in ip_list
        }
        
        # 优先级权重分配
        self.priority_weights = self.config.get("priority_weights", {
            RequestPriority.REALTIME: 1200,
            RequestPriority.PRIORITY_B: 600,
            RequestPriority.NORMAL: 400,
            RequestPriority.BACKFILL: 200,
        })
        
        self.session: Optional[requests.Session] = None
        self.aio_session: Optional[aiohttp.ClientSession] = None
    
    def select_best_ip(
        self,
        weight_required: int,
        priority: RequestPriority = RequestPriority.NORMAL,
    ) -> Optional[str]:
        """
        选择最优 IP
        
        策略：
        1. 首先查找有充足权重的 IP
        2. 若无，则选择恢复最快的 IP
        3. 考虑优先级预留
        """
        now = time.time()
        
        # 优先找有余量的 IP
        candidates = []
        for ip, bucket in self.buckets.items():
            if bucket.can_acquire(weight_required):
                score = bucket.health_score()
                candidates.append((score, ip))
        
        if candidates:
            candidates.sort(reverse=True)
            log.debug(f"选择 IP: {candidates[0][1]} (score={candidates[0][0]:.1f})")
            return candidates[0][1]
        
        # 没有充足余量的 IP，选择恢复最快的
        best_ip = min(
            self.buckets.items(),
            key=lambda x: x[1].recovery_time()
        )[0]
        
        recovery_time = self.buckets[best_ip].recovery_time()
        log.warning(
            f"无可用 IP，等待 {best_ip} 恢复 ({recovery_time:.1f}s)"
        )
        
        return best_ip
    
    async def wait_for_ip_available(
        self,
        weight_required: int,
        priority: RequestPriority = RequestPriority.NORMAL,
        timeout: float = 300,
    ) -> str:
        """等待有可用 IP，支持超时"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            ip = self.select_best_ip(weight_required, priority)
            if ip and self.buckets[ip].can_acquire(weight_required):
                return ip
            
            await asyncio.sleep(0.5)
        
        raise TimeoutError(f"等待可用 IP 超时 ({timeout}s)")
    
    def _get_session(self) -> requests.Session:
        """获取 requests session"""
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "market-data-collector/3.0"
            })
        return self.session
    
    async def _get_aio_session(self) -> aiohttp.ClientSession:
        """获取异步 session"""
        if self.aio_session is None:
            self.aio_session = aiohttp.ClientSession()
        return self.aio_session
    
    def fetch_sync(
        self,
        url: str,
        params: dict | None = None,
        weight: int = 1,
        priority: RequestPriority = RequestPriority.NORMAL,
        max_retries: int = 3,
    ) -> dict:
        """
        同步获取请求（带故障转移）
        
        Args:
            url: 完整 URL
            params: 查询参数
            weight: API 权重
            priority: 请求优先级
            max_retries: 最大重试次数
        
        Returns:
            JSON 响应
        
        Raises:
            requests.HTTPError: 所有 IP 都失败
        """
        session = self._get_session()
        
        for attempt in range(max_retries):
            # 选择最优 IP
            ip = self.select_best_ip(weight, priority)
            if ip is None:
                raise RuntimeError("没有可用的 IP")
            
            bucket = self.buckets[ip]
            
            try:
                # 等待权重可用
                while not bucket.can_acquire(weight):
                    time.sleep(0.1)
                
                # 执行请求
                resp = session.get(
                    url,
                    params=params,
                    timeout=30,
                    proxies={"https": f"http://{ip}", "http": f"http://{ip}"}
                    if ip != "direct" else {}
                )
                
                # 检查 429
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    bucket.penalize(retry_after)
                    log.warning(
                        f"429 from {ip}, 冷却 {retry_after}s, 重试 {attempt+1}/{max_retries}"
                    )
                    continue
                
                resp.raise_for_status()
                return resp.json()
            
            except requests.ConnectionError as e:
                bucket.mark_unhealthy(30)
                log.warning(f"IP {ip} 连接失败: {e}")
                continue
            
            except requests.HTTPError as e:
                if resp.status_code >= 500:
                    bucket.mark_unhealthy(10)
                log.warning(f"IP {ip} HTTP 错误: {resp.status_code}")
                continue
            
            except Exception as e:
                bucket.mark_unhealthy(5)
                log.warning(f"IP {ip} 未知错误: {e}")
                continue
        
        raise requests.HTTPError(
            f"所有 {len(self.ip_list)} 个 IP 都失败，无法完成请求"
        )
    
    async def fetch_async(
        self,
        url: str,
        params: dict | None = None,
        weight: int = 1,
        priority: RequestPriority = RequestPriority.NORMAL,
        max_retries: int = 3,
    ) -> dict:
        """异步获取请求"""
        session = await self._get_aio_session()
        
        for attempt in range(max_retries):
            ip = self.select_best_ip(weight, priority)
            if ip is None:
                raise RuntimeError("没有可用的 IP")
            
            bucket = self.buckets[ip]
            
            try:
                # 等待权重
                while not bucket.can_acquire(weight):
                    await asyncio.sleep(0.1)
                
                # 执行异步请求
                proxy = None
                if ip != "direct":
                    proxy = f"http://{ip}"
                
                async with session.get(
                    url, params=params, timeout=30, proxy=proxy
                ) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        bucket.penalize(retry_after)
                        await asyncio.sleep(0.5)
                        continue
                    
                    resp.raise_for_status()
                    return await resp.json()
            
            except asyncio.TimeoutError:
                bucket.mark_unhealthy(10)
                continue
            
            except aiohttp.ClientError as e:
                bucket.mark_unhealthy(30)
                continue
            
            except Exception as e:
                bucket.mark_unhealthy(5)
                continue
        
        raise RuntimeError(
            f"所有 {len(self.ip_list)} 个 IP 都失败"
        )
    
    def get_stats(self) -> dict:
        """获取所有 IP 的统计信息"""
        stats = {}
        for ip, bucket in self.buckets.items():
            bucket.reset_if_needed()
            stats[ip] = {
                "health_score": bucket.health_score(),
                "available_weight": bucket.available_weight(),
                "weight_consumed": bucket.weight_consumed,
                "recovery_time": bucket.recovery_time(),
                "penalty_until": bucket.penalty_until,
                "unhealthy_until": bucket.unhealthy_until,
            }
        return stats
    
    def log_stats(self) -> None:
        """打印统计信息"""
        stats = self.get_stats()
        log.info("=== Multi-IP 统计 ===")
        for ip, info in stats.items():
            log.info(
                f"{ip}: health={info['health_score']:.0f} "
                f"weight={info['available_weight']}/{self.buckets[ip].max_weight_per_min} "
                f"recovery={info['recovery_time']:.1f}s"
            )
    
    async def close(self) -> None:
        """关闭所有连接"""
        if self.session:
            self.session.close()
        if self.aio_session:
            await self.aio_session.close()
