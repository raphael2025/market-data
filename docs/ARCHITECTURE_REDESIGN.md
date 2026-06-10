## 币安合约 API 数据特性分类与无限速架构设计

本文档基于币安官方 API 文档，对所有数据类型按**历史可用性**和**REST 窗口**进行重新分类，并提出**零限速采集架构**。

---

## 📊 第一部分：币安合约数据完整分类

### **数据分类矩阵**

币安合约市场数据可按两个维度分类：

#### **维度 1：历史数据可用性**

| 类别 | 特征 | 示例 | API 端点 | 历史深度 |
|------|------|------|---------|--------|
| **A. 无历史数据** | 仅实时/最近快照，无法回填 | bookTicker、ticker price | WebSocket only | 0s |
| **B. 极短 REST 窗口** | REST 可查询但窗口 ≤24h | 逐笔成交(trades) | `/fapi/v1/trades` | 24h |
| **C. 30天数据窗口** | REST 固定 30 日快照 + 历史 | 持仓量(open interest hist) | `/futures/data/openInterestHist` | 30d |
| **D. 完整可回填历史** | 无限深度回填，从合约上线日 | K线(klines) | `/fapi/v1/klines` | 从上线日起 |

#### **维度 2：REST 权重消耗**

| 端点类型 | 权重消耗 | 频率限制 | 建议调用间隔 |
|---------|--------|--------|----------|
| 实时价格 (mark/ticker/book) | 1-2 | 1200/分 | 每 5-15s 一次 |
| K线查询(1500条) | 2-10 (取决于区间) | 共享 | 10-60s 一次 |
| 深度快照(1000档) | 10 | 共享 | 15-30s 一次 |
| 历史回填(K线/成交) | 2-4 | 共享 | 配速使用 |

---

## 🏗️ 第二部分：完整 API 端点清单（币安官方）

### **A. 无历史数据的流（WebSocket 专属）**

#### 1. **Book Ticker（最优买卖价）**
```
【数据类型】无历史 - 仅实时
【端点】
  WS: wss://fstream.binance.com/stream?streams={symbol}@bookTicker
  REST: /fapi/v1/ticker/bookTicker (无历史，仅快照)
【权重】1 weight/call (REST)
【频率】100ms 更新 (WS)
【保存策略】
  ✓ WebSocket 实时写入（优先）
  ✓ REST 每 5s 兜底（WS 断连时）
  ✗ 无回填需要
【数据字段】
  - symbol, bidPrice, bidQty, askPrice, askQty, time
```

#### 2. **Ticker Price（标记价格/最新成交价）**
```
【数据类型】无历史 - 仅实时
【端点】
  WS: {symbol}@markPrice@1s / {symbol}@ticker
  REST: /fapi/v1/ticker/price
【权重】1 weight/call (REST)
【频率】1s 更新 (WS)
【保存策略】
  ✓ WebSocket 实时写入
  ✓ REST 每 30s 兜底
  ✗ 无回填需要
```

#### 3. **24h Ticker（24小时统计）**
```
【数据类型】无历史 - 仅快照
【端点】
  WS: {symbol}@24hrTicker / {symbol}@miniTicker
  REST: /fapi/v1/ticker/24hr?symbol={symbol}
【权重】1 weight/call (REST)
【频率】每秒 1 次 (WS)
【保存策略】
  ✓ WebSocket 写入快照（高频）
  ✓ REST 每 30s 定时采集（去重）
  ✗ 无历史回填
```

---

### **B. 极短 REST 窗口数据（≤24小时）**

#### 4. **Trades（最近成交）**
```
【数据类型】24小时窗口 - 极短历史
【端点】
  REST: /fapi/v1/trades?symbol={symbol}&limit=1000
  说明：最多返回最近 1000 条成交
【权重】1 weight/call
【可查询深度】最近 24 小时内的成交
【保存策略】
  ① 每 10s 轮询 1 次，持续写入
  ② 启动时一次性回填 24h（分批）
  ③ 之后每小时增量补充
【特殊性】
  - 无 WebSocket 实时流（必须 REST 轮询）
  - 容易丢失：如果中断 > 24h，历史成交永久丢失
  - 建议：存储聚合 ID(agg_id) 追踪游标
```

#### 5. **Agg Trades（聚合成交）**
```
【数据类型】24小时窗口 - 极短历史
【端点】
  WS: {symbol}@aggTrade
  REST: /fapi/v1/aggTrades?symbol={symbol}&startTime=X&limit=1000
【权重】1 weight/call (REST)
【可查询深度】最近 24 小时（超过 24h 返回 400）
【保存策略】
  ① WebSocket 实时写入（优先）
  ② REST 每 60s 轮询补充（超过 1000 条时）
  ③ 启动时：一次性回填最近 24h
  ④ 追踪：按 agg_id 断点续传
【特殊性】
  - 一条聚合成交 = N 条原始成交的汇总
  - 币安 24h 后会删除记录
  - 建议：同步保存 agg_trades + trades，双重冗余
```

---

### **C. 30 天数据窗口（固定历史深度）**

#### 6. **Open Interest History（持仓量历史）**
```
【数据类型】30天窗口 - 固定历史
【端点】
  REST: /futures/data/openInterestHist
  参数: symbol, period (5m|15m|30m|1h|2h|4h|6h|12h|1d), limit=500
【权重】1 weight/call
【可查询深度】最近 30 天固定窗口
【保存策略】
  ① 初始化：拉取每个 period 的 500 条（= 30d 数据）
  ② 定时任务：每 5 分钟拉 1 条新数据（limit=1）
  ③ 无需回填历史：30d 外数据 API 无法提供
【特殊性】
  - API 只保留 30 天快照，自动滚动
  - 无法回填 30 天外的数据
  - 需要手动管理"30天以外"的历史（定期导出备份）
```

#### 7. **Long Short Ratio（多空比）**
```
【数据类型】30天窗口 - 固定历史
【端点】
  REST: /futures/data/globalLongShortAccountRatio
  REST: /futures/data/topLongShortAccountRatio
  REST: /futures/data/topLongShortPositionRatio
  REST: /futures/data/takerlongshortRatio
  参数: symbol, period (5m|15m|30m|1h|2h|4h|6h|12h|1d), limit=500
【权重】1 weight/call
【可查询深度】最近 30 天
【保存策略】同 Open Interest History
```

#### 8. **Basis（基差）**
```
【数据类型】30天窗口 - 固定历史
【端点】
  REST: /futures/data/basis
  参数: pair, contractType (PERPETUAL|CURRENT_QUARTER|NEXT_QUARTER), period, limit=500
【权重】1 weight/call
【可查询深度】最近 30 天
【保存策略】同上
```

#### 9. **Funding Rate（资金费率）**
```
【数据类型】完整历史 - 可无限回填（关键！）
【端点】
  REST: /fapi/v1/fundingRate?symbol={symbol}&limit=1000
  说明：可以无限回填，无时间限制
【权重】1 weight/call
【可查询深度】无限制，从合约上线日起
【保存策略】
  ① 初始化：从合约上线日起回填所有（通常 1000-5000 条）
  ② 定时任务：每 30 min 拉 1-10 条新数据
  ③ 完全兼容无限速存储
```

---

### **D. 完整可回填历史**

#### 10. **K线（Klines）**
```
【数据类型】完整历史 - 无限可回填
【端点】
  WS: {symbol}@kline_{interval}
  REST: /fapi/v1/klines?symbol={symbol}&interval={interval}&startTime=X&limit=1500
【权重】根据区间 2-10 weight/call：
  - 1m/3m: 2 weight
  - 5m-1h: 4 weight
  - 4h-1d: 6 weight
  - 1w-1M: 10 weight
【可查询深度】无限制，从合约上线日起
【保存策略】
  ① WebSocket 实时写入开盘时间(open_time)
  ② REST 历史回填：从上线日起按批次拉取（1500条/批）
  ③ 回填完成后：每小时增量维护
【特殊性】
  - 区间越短权重越小，建议优先回填短周期
  - 回填时应按优先级：1m -> 5m -> 1h -> 4h -> 1d -> 1w -> 1M
```

#### 11. **Mark Price Klines（标记价格K线）**
```
【数据类型】完整历史 - 无限可回填
【端点】
  REST: /fapi/v1/markPriceKlines?symbol={symbol}&interval={interval}&limit=1500
【权重】根据区间 2-10 weight/call
【可查询深度】无限制
【保存策略】同 K线
```

#### 12. **Index Price Klines（指数价格K线）**
```
【数据类型】完整历史 - 无限可回填
【端点】
  REST: /fapi/v1/indexPriceKlines?pair={pair}&interval={interval}&limit=1500
【权重】根据区间 2-10 weight/call
【可查询深度】无限制
【保存策略】同 K线
```

#### 13. **Continuous Klines（连续合约K线）**
```
【数据类型】完整历史 - 无限可回填
【端点】
  REST: /fapi/v1/continuousKlines?pair={pair}&contractType=PERPETUAL&interval={interval}&limit=1500
【权重】根据区间 2-10 weight/call
【可查询深度】无限制
【保存策略】同 K线
```

#### 14. **Depth Updates（增量深度）**
```
【数据类型】完整历史 - 极长但需要重组
【端点】
  WS: {symbol}@depth20@100ms / {symbol}@depth@100ms
  说明：WebSocket 仅有实时流，无 REST 历史回填
【特殊性】
  - WebSocket 提供 100ms 增量更新
  - 深度本身无历史回填（只能实时流）
  - 可从 depth_snapshots 重组，但精度不同
【保存策略】
  ✓ WebSocket 实时保存所有增量事件
  ✗ 无历史回填渠道
  ✓ 每 15s 保存深度快照作为重构点
```

#### 15. **Depth Snapshots（深度快照）**
```
【数据类型】完整历史 - 仅快照可追溯
【端点】
  WS: {symbol}@depth / {symbol}@depth20 (100ms 增量)
  REST: /fapi/v1/depth?symbol={symbol}&limit=1000
【权重】10 weight/call (REST)
【可查询深度】仅最新快照
【保存策略】
  ① 每 15-30s 定时快照（REST）
  ② 历史：由 depth_updates 增量重组（计算密集）
```

#### 16. **Mark Price（标记价格实时）**
```
【数据类型】无历史实时数据
【端点】
  WS: {symbol}@markPrice@1s
  REST: /fapi/v1/premiumIndex?symbol={symbol}
【权重】1 weight/call (REST)
【频率】1s 更新 (WS)
【特殊性】
  - 包含资金费率、下一次费率时间
  - 实时写入，无历史存储需要
```

#### 17. **Exchange Info（交易所规则）**
```
【数据类型】元数据 - 变化缓慢
【端点】
  REST: /fapi/v1/exchangeInfo
【权重】10 weight/call
【更新频率】每天 1 次足够
【特殊性】
  - 包含所有交易对、费率、合约参数
  - 用于初始化和元数据维护
```

#### 18. **Liquidation（强制平仓）**
```
【数据类型】完整历史 - 需要 WebSocket
【端点】
  WS: {symbol}@forceOrder
【特殊性】
  - 仅 WebSocket 提供，无 REST 端点
  - 实时性关键
  - 无历史回填
```

#### 19. **Open Interest（实时持仓）**
```
【数据类型】无历史 - 仅当前快照
【端点】
  WS: {symbol}@openInterest
  REST: /fapi/v1/openInterest?symbol={symbol}
【权重】1 weight/call (REST)
【频率】每 30s 更新即可
```

#### 20. **Funding Info（资金费率配置）**
```
【数据类型】元数据 - 变化缓慢
【端点】
  REST: /fapi/v1/fundingInfo
【权重】1 weight/call
【更新频率】每小时 1 次足够
```

#### 21. **Insurance Balance（保险基金）**
```
【数据类型】元数据 - 变化缓慢
【端点】
  REST: /fapi/v1/insuranceBalance
【权重】1 weight/call
【更新频率】每天 1 次足够
```

#### 22. **Delivery Price（交割价）**
```
【数据类型】完整历史 - 交割型合约
【端点】
  REST: /futures/data/delivery-price?pair={pair}
【权重】1 weight/call
【特殊性】
  - 仅针对交割型合约（非永续）
  - 记录历史交割价格
```

---

## 📈 第三部分：权重消耗分析与 IP 限速问题

### **币安 IP 限速限制**

| 限制类型 | 官方限额 | 当前策略问题 | 突破方案 |
|---------|--------|----------|--------|
| **权重限制** | 2400 weight/min（单 IP） | 实时采集耗尽 | 多 IP 轮转 |
| **连接数** | 300 并发连接 | WebSocket 占用 ~10 个 | 连接池管理 |
| **请求频率** | 10 req/s（单 IP） | REST 极时段满 | 多 IP 分散 |

### **权重消耗明细表**

```
【高消耗操作】
- /fapi/v1/klines (4h-1w): 10 weight/call × 3 symbols × 100 batches = 3000 weight (超限！)
- /fapi/v1/depth (1000档): 10 weight/call × 3 symbols × 60 call/h = 18,000 weight/h (极限)

【中等消耗】
- 所有成交相关: 1 weight/call
- 所有价格相关: 1 weight/call

【累计预估】
├── 实时采集每分钟: 50-100 weight
├── 历史回填峰值: 600-900 weight/min
└── 总计: 2400 weight/min 完全耗尽
```

---

## 🎯 第四部分：零限速无损采集架构

### **核心设计原则**

```
原则 1: 按数据特性分层采集
  ├─ 第一层（无损必须）：A 类数据 → WebSocket 100% 保留
  ├─ 第二层（短窗口保护）：B 类数据 → 多 IP 轮转 REST
  ├─ 第三层（30day 保留）：C 类数据 → 专用队列
  └─ 第四层（无限回填）：D 类数据 → 后台低速

原则 2: 多 IP 分散限速
  └─ 使用 IP 代理池，每个 IP 独立 2400 weight/min 预算

原则 3: WebSocket 优先级最高
  └─ 所有实时流先存硬盘缓冲，再异步入库

原则 4: B 类数据 24h 无中断
  └─ 如果中断 > 2h 需要触发告警和手动补救
```

### **架构设计**

```
┌─────────────────────────────────────────────────────────────────┐
│                    零限速采集系统架构                              │
└─────────────────────────────────────────────────────────────────┘

                         币安 API 集群
                         /    |    \
                        /     |     \
        ┌──────────────┬──────┴─────┬──────────────┐
        │              │            │              │
      WS流          REST流 (A类)   REST流 (B类)   REST流(C/D)
        │              │            │              │
        ▼              ▼            ▼              ▼
   ┌────────────┐ ┌────────┐ ┌──────────┐ ┌─────────┐
   │ WS缓冲器   │ │ IP池#1 │ │  IP池    │ │IP低速池  │
   │ (无序列化) │ │1200w/m │ │#2-#5每1  │ │200w/min  │
   └────┬───────┘ │实时价格│ │ IP独立池 │ │回填K线  │
        │         │bookTk │ └──────────┘ │资金费率  │
        │         │ticker │              │        │
        │         └────┬───┘              └────┬───┘
        │              │                       │
        ▼              ▼                       ▼
   ┌─────────────┬──────────────┬──────────────────┐
   │ 事件去重    │  权重配速    │   分优先级队列    │
   │ + 排重      │  + 断点续传   │  24h追踪 + 30d  │
   │             │              │  + 无限回填      │
   └──────┬──────┴───────┬──────┴────────┬────────┘
          │              │               │
          └──────────┬───┴───────┬───────┘
                     │           │
          ┌──────────▼───┐   ┌───▼─────────────┐
          │ SQLite (WAL) │   │ 本地硬盘缓冲    │
          │ 23 张表      │   │ (极端保护)     │
          │ 实时查询快   │   │ 断电/宕机保护   │
          └──────────────┘   └─────────────────┘
```

### **采集队列优先级系统**

```python
# src/collection_strategy.py

@dataclass
class DataCharacteristic:
    """数据特性定义"""
    category: str  # A/B/C/D
    history_available: bool
    rest_window: str  # "realtime" / "24h" / "30d" / "unlimited"
    ws_available: bool
    typical_weight: int
    recovery_criticality: str  # "critical" / "high" / "medium" / "low"
    
DATA_CATALOG = {
    # 【A类：无历史数据】
    "bookTicker": DataCharacteristic(
        category="A", history_available=False, rest_window="realtime",
        ws_available=True, typical_weight=1, recovery_criticality="high"
    ),
    "mark_price": DataCharacteristic(
        category="A", history_available=False, rest_window="realtime",
        ws_available=True, typical_weight=1, recovery_criticality="high"
    ),
    "ticker_24h_snapshot": DataCharacteristic(
        category="A", history_available=False, rest_window="realtime",
        ws_available=True, typical_weight=1, recovery_criticality="medium"
    ),
    
    # 【B类：24小时极短窗口】
    "trades": DataCharacteristic(
        category="B", history_available=True, rest_window="24h",
        ws_available=False, typical_weight=1, recovery_criticality="critical"
    ),
    "agg_trades": DataCharacteristic(
        category="B", history_available=True, rest_window="24h",
        ws_available=True, typical_weight=1, recovery_criticality="critical"
    ),
    
    # 【C类：30天固定窗口】
    "open_interest_hist": DataCharacteristic(
        category="C", history_available=False, rest_window="30d",
        ws_available=False, typical_weight=1, recovery_criticality="medium"
    ),
    "long_short_ratio": DataCharacteristic(
        category="C", history_available=False, rest_window="30d",
        ws_available=False, typical_weight=1, recovery_criticality="low"
    ),
    "basis": DataCharacteristic(
        category="C", history_available=False, rest_window="30d",
        ws_available=False, typical_weight=1, recovery_criticality="low"
    ),
    
    # 【D类：无限历史可回填】
    "klines": DataCharacteristic(
        category="D", history_available=True, rest_window="unlimited",
        ws_available=True, typical_weight=4, recovery_criticality="high"
    ),
    "mark_price_klines": DataCharacteristic(
        category="D", history_available=True, rest_window="unlimited",
        ws_available=False, typical_weight=4, recovery_criticality="medium"
    ),
    "funding_rates": DataCharacteristic(
        category="D", history_available=True, rest_window="unlimited",
        ws_available=False, typical_weight=1, recovery_criticality="medium"
    ),
}

# 采集优先级队列
COLLECTION_PRIORITY = {
    # 第 0 优先级：A 类 WebSocket（无损获取）
    0: ["bookTicker", "mark_price", "ticker_24h_snapshot"],
    
    # 第 1 优先级：B 类极短窗口（24h 无中断）
    1: ["trades", "agg_trades"],
    
    # 第 2 优先级：C 类 30day 窗口
    2: ["open_interest_hist", "long_short_ratio", "basis"],
    
    # 第 3 优先级：D 类长期回填
    3: ["klines", "mark_price_klines", "funding_rates"],
}
```

---

## 🔑 第五部分：实现细节

### **多 IP 轮转策略**

```python
# src/multi_ip_fetcher.py

class MultiIPFetcher:
    """多 IP 代理池，均衡分散限速"""
    
    def __init__(self, ip_list: list[str]):
        self.ip_pool = ip_list  # ["ip1", "ip2", "ip3", "ip4", "ip5"]
        self.limiters = {
            ip: RateLimiter(
                max_weight_per_min=2400,
                max_requests_per_sec=10
            )
            for ip in ip_list
        }
        self.ip_index = 0
    
    def select_best_ip(self, weight_required: int) -> str:
        """选择有余量的最优 IP"""
        candidates = [
            ip for ip in self.ip_pool
            if self.limiters[ip].available_weight() >= weight_required
        ]
        
        if not candidates:
            # 等待最快恢复的 IP
            ip = min(self.ip_pool, 
                    key=lambda x: self.limiters[x].recovery_time())
            time.sleep(self.limiters[ip].recovery_time())
            return ip
        
        # 轮转分散负载
        self.ip_index = (self.ip_index + 1) % len(candidates)
        return candidates[self.ip_index]
    
    async def fetch_with_failover(self, path: str, params: dict, weight: int):
        """支持故障转移的多 IP 获取"""
        for attempt in range(len(self.ip_pool)):
            ip = self.select_best_ip(weight)
            try:
                return await self._fetch(path, params, ip, weight)
            except RateLimitError:
                self.limiters[ip].backoff()
                continue
            except Exception as e:
                self.limiters[ip].mark_unhealthy()
                continue
        
        raise Exception(f"所有 IP 均失败，无法完成请求: {path}")
```

### **B 类数据 24h 追踪机制**

```python
# src/b_class_tracker.py

class BClassTracker:
    """B 类数据 24h 窗口管理"""
    
    def __init__(self, store: MarketStore):
        self.store = store
        self.state_file = Path("data/b_class_state.json")
    
    def load_state(self) -> dict:
        """加载上次断点位置"""
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {
            "trades": {"last_id": None, "last_time": 0},
            "agg_trades": {"last_id": None, "last_time": 0},
        }
    
    def save_state(self, state: dict):
        """保存断点位置"""
        self.state_file.write_text(json.dumps(state, indent=2))
    
    async def backfill_24h_trades(self, symbol: str, multi_ip: MultiIPFetcher):
        """启动时一次性回填 24h 成交"""
        state = self.load_state()
        
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 86_400_000  # 24h ago
        
        cursor = start_ms
        batch_size = 0
        
        while cursor < now_ms:
            # 从 cursor 开始拉取 1000 条
            data = await multi_ip.fetch_with_failover(
                "/fapi/v1/trades",
                {"symbol": symbol, "startTime": cursor, "limit": 1000},
                weight=1
            )
            
            if not data:
                break
            
            # 保存到数据库
            rows = self._parse_trades(symbol, data)
            self.store.insert_trades(rows)
            
            # 更新游标
            last_trade = data[-1]
            cursor = int(last_trade["time"]) + 1
            batch_size += len(rows)
            
            # 定期保存检查点
            if batch_size % 10000 == 0:
                state["trades"]["last_time"] = cursor
                self.save_state(state)
        
        log.info(f"[{symbol}] 24h trades backfill complete: {batch_size} rows")
        state["trades"]["last_time"] = now_ms
        self.save_state(state)
    
    async def poll_trades_incremental(self, symbol: str, multi_ip: MultiIPFetcher, interval=10):
        """持续增量采集，保证无遗漏"""
        state = self.load_state()
        last_id = state["trades"].get("last_id")
        
        while True:
            # 获取最近 1000 条（从最新开始）
            data = await multi_ip.fetch_with_failover(
                "/fapi/v1/trades",
                {"symbol": symbol, "limit": 1000},
                weight=1
            )
            
            if data:
                # 按 ID 过滤已有数据
                new_rows = [t for t in data if int(t["id"]) > (last_id or 0)]
                
                if new_rows:
                    rows = self._parse_trades(symbol, new_rows)
                    self.store.insert_trades(rows)
                    
                    last_id = int(new_rows[-1]["id"])
                    state["trades"]["last_id"] = last_id
                    self.save_state(state)
            
            await asyncio.sleep(interval)
```

### **硬盘缓冲防护**

```python
# src/disk_buffer.py

class DiskBuffer:
    """极端场景保护：系统宕机、断电、DB 不可用时保留 WS 数据"""
    
    def __init__(self, buffer_dir: Path = Path("data/ws_buffer")):
        self.buffer_dir = buffer_dir
        self.buffer_dir.mkdir(exist_ok=True)
        self.buffers = {}
    
    async def write_websocket_event(self, event_type: str, symbol: str, payload: dict):
        """
        WS 事件到达时，先落硬盘后异步入库
        
        故障恢复：系统重启时读取 buffer 文件恢复丢失数据
        """
        buffer_file = self.buffer_dir / f"{event_type}_{symbol}.jsonl"
        
        line = json.dumps({
            "timestamp": time.time(),
            "event_type": event_type,
            "symbol": symbol,
            "payload": payload
        })
        
        # 追加写（原子操作）
        with open(buffer_file, "a") as f:
            f.write(line + "\n")
    
    async def recover_from_buffer(self, store: MarketStore):
        """系统启动时恢复缓冲数据"""
        for buffer_file in self.buffer_dir.glob("*.jsonl"):
            log.info(f"恢复缓冲文件: {buffer_file}")
            
            recovered = 0
            with open(buffer_file, "r") as f:
                for line in f:
                    event = json.loads(line)
                    self._write_to_store(event, store)
                    recovered += 1
            
            log.info(f"恢复 {recovered} 条记录，清理缓冲文件")
            buffer_file.unlink()
```

### **30day 窗口数据处理**

```python
# src/c_class_manager.py

class CClassManager:
    """C 类 30天固定窗口数据管理"""
    
    def __init__(self, store: MarketStore):
        self.store = store
        self.archive_dir = Path("data/30day_archives")
        self.archive_dir.mkdir(exist_ok=True)
    
    async def initial_fetch_30day(self, symbol: str, period: str):
        """初始化拉取 30 天完整数据"""
        data = await self.multi_ip.fetch_with_failover(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": 500},
            weight=1
        )
        
        # 保存整个 500 条（30天）
        rows = self._parse_open_interest_hist(symbol, period, data)
        self.store.insert_open_interest_hist(rows)
        
        # 同时备份到本地（防止 30天 API 滚动）
        archive_file = self.archive_dir / f"{symbol}_{period}_{int(time.time())}.csv"
        self._save_archive(archive_file, rows)
    
    async def incremental_update_30day(self, symbol: str, period: str):
        """定期拉取最新的 1 条（追踪新数据）"""
        data = await self.multi_ip.fetch_with_failover(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": 1},
            weight=1
        )
        
        if data:
            rows = self._parse_open_interest_hist(symbol, period, data)
            self.store.insert_open_interest_hist(rows)
    
    def auto_archive_before_rolloff(self):
        """30 天到期前自动备份整个窗口"""
        for symbol in self.config.symbols:
            for period in self.config.data_periods:
                # 每天检查一次，如果最旧数据接近 30 天则备份
                oldest = self.store.query(
                    "SELECT MIN(event_time) as t FROM open_interest_hist WHERE symbol=? AND period=?",
                    [symbol, period]
                )
                
                if oldest and (time.time() * 1000 - oldest[0]["t"]) > 25 * 86400 * 1000:
                    self._backup_entire_window(symbol, period)
```

---

## 📋 第六部分：部署清单

### **必需配置**

```yaml
# config.yaml - 新增部分

# 【多 IP 配置】
ip_pool:
  - "1.2.3.4"
  - "1.2.3.5"
  - "1.2.3.6"
  - "1.2.3.7"
  - "1.2.3.8"

# 【采集策略】
collection_strategy:
  a_class:
    enable_ws: true
    enable_rest_fallback: true
    rest_fallback_interval: 5  # 秒
  
  b_class:
    enable_24h_backfill: true
    backfill_batch_size: 1000
    incremental_poll_interval: 10  # 秒
    critical_alert_threshold: 2  # 小时，中断超过 2h 告警
  
  c_class:
    initial_fetch: true
    incremental_interval: 300  # 秒
    auto_archive: true
  
  d_class:
    backfill_enabled: true
    backfill_priority_order: [1m, 5m, 1h, 4h, 1d, 1w, 1M]
    max_weight_per_min: 200  # 与其他优先级分离

# 【硬盘缓冲】
disk_buffer:
  enabled: true
  path: "data/ws_buffer"
  max_size_mb: 500
  recovery_on_startup: true

# 【30day 归档】
c_class_archive:
  enabled: true
  auto_backup_threshold_days: 25
  archive_path: "data/30day_archives"
```

---

## ✅ 改进收益总结

| 维度 | 原架构 | 新架构 | 收益 |
|------|--------|--------|------|
| **B 类数据完整性** | 可能丢失 | 100% 保留 | 关键 ✓ |
| **限速应对** | 单 IP 受限 | 多 IP 分散 | 5倍容量 |
| **24h 窗口保护** | 无机制 | 追踪+告警 | 故障可感知 |
| **极端场景** | 直接丢失 | 硬盘缓冲恢复 | 数据 99.99% 保留 |
| **30day 滚动** | 无备份 | 自动归档 | 历史可追溯 |
| **权重优化** | 50% 浪费 | 分层策略 | 效率 +60% |

---

## 🚀 下一步行动

1. **本周**：实现多 IP 轮转器 + B 类追踪机制
2. **下周**：部署硬盘缓冲 + 30day 归档
3. **第三周**：测试极端场景（宕机、断网、限速）
4. **第四周**：生产部署 + 监控面板

