# Market Data 改进建议 — Paper Wallet 对接专项

> **状态：已实现（v2.2.0）** — 见 `src/tick.py`、`src/health.py`、`docs/API.md`  
> 目标：让 `market-data` 更好地服务 `crypto_paper_wallet` 模拟钱包及各类回测引擎。  
> 原则：**不重做采集**，只在现有 23 张表之上增加聚合查询层。  
> 关联项目：`/home/raphael/crypto_paper_wallet`（端口 8000）

---

## 1. 背景

### 1.1 两个项目的分工

```
market-data (:8765)          crypto_paper_wallet (:8000)
  采集 + 存储 + 查询 API  →    模拟钱包 + 订单/清算/资金费
         │                              ▲
         └──────── 行情 tick 注入 ───────┘
```

- **market-data**：负责从币安采集并持久化全部公开数据
- **paper wallet**：不负责拉行情，只消费 tick 数据驱动账户状态机

### 1.2 改进前痛点（已解决）

paper wallet 的 `MARKET_DATA_CONTRACT.md` 曾期望单次请求拿到完整 tick，改进前需分别调用 3 个接口。现已通过 `/v1/tick/latest` 聚合解决。

原始期望格式：

```json
{
  "symbol": "BTCUSDT",
  "mark_price": 67500.0,
  "index_price": 67480.0,
  "best_bid": 67490.0,
  "best_ask": 67510.0,
  "funding_rate": 0.0001,
  "bid_depth": [{"price": 67490, "qty": 1.5}],
  "ask_depth": [{"price": 67510, "qty": 2.0}],
  "timestamp": "2026-06-10T08:00:00Z"
}
```

改进前需分别调用（底层接口仍保留）：

| 字段 | 底层接口 |
|------|---------|
| mark_price / index_price / funding_rate | `GET /v1/mark-price/latest` |
| best_bid / best_ask | `GET /v1/book-ticker/latest` |
| bid_depth / ask_depth | `GET /v1/depth/snapshots/latest` |

### 1.3 已有能力（无需重复建设）

以下数据已采集且满足 paper wallet 需求，**不必新增采集逻辑**：

- 标记价格 / 指数价格 / 资金费率（WS + REST）
- 最优买卖价（WS）
- 1000 档深度快照（15s）+ 100ms 增量
- 标记价格 K 线 / 指数价格 K 线 / 成交价 K 线
- 历史资金费率

---

## 2. 改进建议总览

| 优先级 | 建议 | 类型 | 状态 |
|--------|------|------|------|
| **P0** | 聚合 tick 接口（latest） | 新 API | ✅ 已实现 |
| **P0** | 多币种批量 tick | 新 API | ✅ 已实现 |
| **P0** | paper wallet 兼容路径 `/v1/market/tick/*` | 别名 | ✅ 已实现 |
| **P1** | 历史 tick 按时间点查询 | 新 API | ✅ 已实现 |
| **P1** | 历史 tick 按时间范围批量导出 | 新 API | ✅ 已实现 |
| **P1** | 深度字段解析为结构化数组 | 响应优化 | ✅ 已实现 |
| **P2** | 数据新鲜度字段（age_ms / is_stale） | 响应增强 | ✅ 已实现 |
| **P2** | K 线 + mark + funding 对齐导出 | 新 API | ✅ `/v1/backtest/bars` |
| **P3** | 指定时刻订单簿重建 | 深度增强 | ⏸ 暂缓 |
| **P3** | WebSocket 推送 | 推送 | ⏸ 暂缓 |
| **P1** | book_ticker WS 持续更新 / health 暴露 book_age | 采集可靠性 | ✅ 独立 WS 连接 + REST 5s 兜底 |
| **P1** | backtest/bars mark 空洞回填 mark_price_klines | 数据完整性 | ✅ klines.close fallback |
| **P2** | 拆分 is_book_stale / is_mark_stale | stale 策略 | ✅ tick 响应 + wallet require_fresh |
| **P2** | /health 各流 last_event_ms 可观测性 | 运维 | ✅ `streams.symbols.*` |
| **P3** | market store 清理 / 刷新 API | paper wallet | ✅ DELETE + POST refresh |
| **P3** | 清算路径深度测试 | 测试 | ✅ liquidation 用例 |
| **—** | 不需要做的项 | — | — |

---

## 3. P0 — 必须做（对接 MVP）

### 3.1 `GET /v1/tick/latest`

**目的**：一次请求返回 paper wallet 所需的完整 tick。

**请求**

```
GET /v1/tick/latest?symbol=BTCUSDT
GET /v1/tick/latest?symbol=BTCUSDT&include_depth=true&depth_levels=20
```

| 参数 | 默认 | 说明 |
|------|------|------|
| symbol | 必填 | BTCUSDT / ETHUSDT / SOLUSDT |
| include_depth | false | 是否附带 L2 深度 |
| depth_levels | 20 | 深度档数（最大 1000） |

**响应**

```json
{
  "symbol": "BTCUSDT",
  "mark_price": 61753.12,
  "index_price": 61783.92,
  "last_price": 61750.00,
  "best_bid": 61699.1,
  "best_ask": 61699.2,
  "bid_qty": 9.443,
  "ask_qty": 1.444,
  "funding_rate": -0.00001995,
  "next_funding_time": 1781136000000,
  "bid_depth": [
    {"price": 61699.1, "qty": 9.443},
    {"price": 61698.5, "qty": 2.1}
  ],
  "ask_depth": [
    {"price": 61699.2, "qty": 1.444},
    {"price": 61700.0, "qty": 3.5}
  ],
  "volume_1m": 2752.508,
  "event_time": 1781124557003,
  "age_ms": 1200,
  "is_stale": false
}
```

**实现思路**

```python
# 伪代码 — 在 api.py 新增路由，聚合已有 latest 查询
def tick_latest(symbol, include_depth=False):
    mark = query_mark_price_latest(symbol)
    book = query_book_ticker_latest(symbol)
    depth = query_depth_snapshot_latest(symbol) if include_depth else None
    vol = query_kline_volume_1m(symbol)  # 可选：最近 1m kline volume
    return merge(mark, book, depth, vol)
```

无需新表，只读聚合。

---

### 3.2 `GET /v1/ticks/latest`

**目的**：多币种策略一次拿三个 tick。

**请求**

```
GET /v1/ticks/latest?symbols=BTCUSDT,ETHUSDT,SOLUSDT&include_depth=true
```

**响应**

```json
{
  "ticks": {
    "BTCUSDT": { "...完整 tick..." },
    "ETHUSDT": { "...完整 tick..." },
    "SOLUSDT": { "...完整 tick..." }
  },
  "server_time": 1781124557003
}
```

paper wallet 的 `ExternalMarketDataClient.fetch_all()` 可直接对接此接口。

---

## 4. P1 — 回测必需

### 4.1 `GET /v1/tick/at`

**目的**：回测引擎按历史时间点重放，获取该时刻最接近的 tick。

**请求**

```
GET /v1/tick/at?symbol=BTCUSDT&timestamp=1781124000000&include_depth=true
```

| 参数 | 说明 |
|------|------|
| timestamp | Unix 毫秒，目标时刻 |
| tolerance_ms | 可选，默认 60000，允许的最大时间偏差 |

**响应**：同 `tick/latest` 格式，额外带：

```json
{
  "...tick fields...",
  "matched_time": 1781123998500,
  "time_delta_ms": 1500,
  "sources": {"mark_prices": 1781123998500, "book_tickers": 1781123998400},
  "primary_source": "mark_prices"
}
```

**实现思路**

```sql
-- mark_price: 取 event_time <= timestamp 的最近一条
SELECT * FROM mark_prices
WHERE symbol = ? AND event_time <= ?
ORDER BY event_time DESC LIMIT 1;

-- book_ticker: 同理
-- depth_snapshot: 取 snapshot_time <= timestamp 的最近一条
```

三张表按 `matched_time` 对齐，取各自最近记录合并。时间偏差超过 `tolerance_ms` 返回 404。

---

### 4.2 `GET /v1/ticks/range`

**目的**：回测批量拉取一段时间内的 tick 序列，避免逐 bar 调 `tick/at`。

**请求**

```
GET /v1/ticks/range?symbol=BTCUSDT&start_time=...&end_time=...&interval=1h
```

| 参数 | 说明 |
|------|------|
| interval | 采样间隔：1m / 5m / 1h 等，按 K 线周期对齐 |
| start_time / end_time | Unix 毫秒 |

**响应**

```json
{
  "symbol": "BTCUSDT",
  "interval": "1h",
  "total": 500,
  "data": [
    { "...tick at bar open..." },
    { "...tick at bar open..." }
  ]
}
```

**实现思路**：以 `klines.open_time` 为锚点，对 mark/book/depth/funding 分别做 as-of nearest lookup（非等值 join）。无 mark 流时回退 `mark_price_klines.close` 或 `klines.close`。

回测主循环变为：

```python
for tick in client.ticks_range("BTCUSDT", start, end, interval="1h"):
    wallet.tick(tick)
    strategy.on_bar(tick)
```

---

### 4.3 深度字段结构化

**现状**：`depth_snapshots` 的 `bids` / `asks` 是 JSON 字符串。

**建议**：`/v1/tick/*` 系列接口直接返回解析后的数组：

```json
"bid_depth": [{"price": 61699.1, "qty": 9.443}]
```

而非：

```json
"bids": "[[\"61699.10\",\"9.443\"], ...]"
```

旧接口保持兼容，新 tick 接口统一用结构化格式。

---

## 5. P2 — 体验增强

### 5.1 数据新鲜度

所有 `latest` 和 `tick` 接口增加：

| 字段 | 说明 |
|------|------|
| `event_time` | 数据源记录时间（ms） |
| `age_ms` | 服务端当前时间 - event_time |
| `is_stale` | age_ms > 阈值（可配置，默认 30s） |

paper wallet 收到 `is_stale=true` 时可拒绝市价单，避免无行情乱成交。

### 5.2 `GET /v1/backtest/bars`

**目的**：回测一站式拉取 — 一根 bar 包含 OHLCV + mark + funding。

**请求**

```
GET /v1/backtest/bars?symbol=BTCUSDT&interval=1h&start_time=...&end_time=...
```

**响应**（每根 bar）

```json
{
  "open_time": 1781121600000,
  "open": 61908.7,
  "high": 61921.2,
  "low": 61644.4,
  "close": 61761.0,
  "volume": 2752.508,
  "mark_price": 61753.12,
  "index_price": 61783.92,
  "funding_rate": -0.00001995,
  "best_bid": 61699.1,
  "best_ask": 61699.2
}
```

减少回测引擎自行 join 多表的工作。

---

## 6. P3 — 长期可选

### 6.1 指定时刻订单簿重建

用 `depth_updates`（100ms 增量）+ `depth_snapshots`（基准快照）重建任意时刻的完整订单簿。

- 价值：高精度历史滑点回测
- 成本：实现复杂，计算量大
- 建议：等有明确需求再做；当前 15s 深度快照对 MVP 够用

### 6.2 WebSocket 推送

向 paper wallet 主动推送 tick 变化，替代轮询。

- 价值：降低实盘模拟延迟
- 成本：需维护订阅关系
- 建议：本地单机场景 HTTP 轮询足够，暂不需要

---

## 7. 不需要做的项

| 项 | 原因 |
|----|------|
| 自行计算 Mark Price 公式 | 币安 mark_price 已采集，直接使用 |
| 增加交易对 | 两边都只需 BTC/ETH/SOL |
| 采集 Premium Index / Impact Bid-Ask | mark_price 已包含，无需拆算 |
| 为 wallet 建独立数据库 | wallet 有自有 SQLite，只消费 tick |
| 鉴权 / 限流 | 本地自用，双方均无鉴权 |
| 采集 ADL / 保险基金实时流 | wallet v0.1 未建模 ADL |

---

## 8. 对接时序建议

```
Phase 1（1-2 天）
  └─ 实现 /v1/tick/latest + /v1/ticks/latest
  └─ paper wallet ExternalMarketDataClient 对接
  └─ 验证：实时模拟能开仓/平仓/算滑点

Phase 2（2-3 天）
  └─ 实现 /v1/tick/at + /v1/ticks/range
  └─ 回测引擎改为从 market-data 拉历史 tick
  └─ 验证：1h 回测主循环跑通

Phase 3（按需）
  └─ /v1/backtest/bars 一站式接口
  └─ 深度重建（高精度滑点）
```

---

## 9. paper wallet 侧配合（非 market-data 改动，供参考）

| 项 | 说明 |
|----|------|
| `ExternalMarketDataClient` | 指向 `http://localhost:8765/v1/tick/latest` |
| 环境变量 | `MARKET_DATA_URL=http://localhost:8765` |
| 回测 adapter | 调用 `/v1/ticks/range` 替代手动拼 tick |
| stale 处理 | 收到 `is_stale=true` 时拒单或告警 |

---

## 10. 接口对照表（改进后）

| paper wallet 需要 | 改进前（现有） | 改进后（建议） |
|------------------|---------------|---------------|
| 单币种实时 tick | 3 个接口拼装 | `GET /v1/tick/latest` |
| 多币种实时 tick | 3×N 个接口 | `GET /v1/ticks/latest` |
| 历史单点 tick | 自行查 3 张表 | `GET /v1/tick/at` |
| 历史 tick 序列 | 自行 join + 对齐 | `GET /v1/ticks/range` |
| 回测 bar 数据 | 自行 join klines + mark + funding | `GET /v1/backtest/bars` |
| L2 深度 | JSON 字符串手动 parse | 结构化 `bid_depth[]` |
| 数据是否过期 | 自行算 age | 响应自带 `age_ms` / `is_stale` |

---

## 11. 验收标准

改进完成后的对接验证清单：

- [x] `curl /v1/tick/latest?symbol=BTCUSDT` 一次返回 mark + bid/ask + funding
- [x] `curl /v1/ticks/latest?symbols=BTCUSDT,ETHUSDT,SOLUSDT` 三币种齐全
- [x] `curl /v1/market/tick/BTCUSDT` paper wallet 兼容路径可用
- [x] paper wallet 市价开仓成功（wallet 侧已对接 `MARKET_DATA_URL`，默认 `http://localhost:8765`）
- [x] paper wallet 回测 100 根 1h bar 跑通（`ExternalMarketDataClient.fetch_ticks_range`）
- [x] 深度滑点：有 L2 时 walk book，无 L2 时 spread fallback（深度测试 `slippage_l2` + `slippage_depth` 通过）
- [x] 历史 tick 资金费优先查 `funding_rates` 表
- [x] `is_stale=true` 时 paper wallet 拒绝市价单（ingest API 已支持 `is_stale`/`age_ms` 字段）

### 实现备注

1. 响应同时包含 `timestamp`（ISO）和 `event_time`（ms），兼容 paper wallet `_tick_from_dict`
2. `is_stale` 仅判断 mark + book（默认 30s），深度单独看 `depth_age_ms`
3. 修复 `depth_snapshots` 插入列顺序 bug（`snapshot_time` 与 `last_update_id` 曾错位）
4. 历史查询使用 as-of nearest join，非等值 join

---

## 12. 深度集成测试报告（2026-06-10）

### 12.1 测试脚本

```bash
# 先启动两个服务
cd /home/raphael/market-data && ./scripts/run_server.sh
cd /home/raphael/crypto_paper_wallet && ./scripts/run_server.sh

# 运行 32 项深度测试
cd /home/raphael/crypto_paper_wallet
python scripts/deep_integration_test.py
```

报告输出：
- `crypto_paper_wallet/results/deep_test_report.md`
- `crypto_paper_wallet/results/deep_test_report.json`

### 12.2 测试结果摘要

| 指标 | 值 |
|------|-----|
| 总用例 | 32 |
| PASS | 30 |
| FAIL | 0 |
| WARN | 1 |
| SKIP | 1 |
| 通过率 | 93.8% |

### 12.3 分类覆盖

| 分类 | 用例数 | 覆盖内容 |
|------|--------|---------|
| **MARKET_DATA** | 9 | health、tick/latest、ticks/latest、兼容路径、tick/at、ticks/range、backtest/bars、新鲜度、深度结构 |
| **PAPER_WALLET** | 17 | 钱包 CRUD、ingest、市价/限价/止损、Hedge 双向、平仓、撤单、资金费、stale 拒单、L2 滑点、多币种、持久化 |
| **INTEGRATION** | 6 | 外部拉取开仓、stale 拒单、回测主循环、provider 客户端 |

### 12.4 已验证的对接能力

1. **实时 tick 聚合**：三币种 `ticks/latest` 一次返回 mark + bid/ask + funding + depth
2. **paper wallet 消费**：`ExternalMarketDataClient` 正确解析 `event_time`/`is_stale`/`bid_depth`
3. **过期保护**：`is_stale=true` 时市价单 HTTP 400；ingest 可显式标记 stale
4. **回测喂价**：`ticks/range` 20 根 1h bar 主循环 + 历史 bar 开仓均通过
5. **L2 滑点**：有 depth 时 walk book 成交价 ≥ best_ask；无 depth 时 spread fallback
6. **本地优先**：ingest 的新鲜 tick 优先于外部 stale tick（设计预期）

### 12.5 遗留问题处理（v2.2.0 已闭环）

| 优先级 | 问题 | 处理 |
|--------|------|------|
| **P1** | book_ticker 间歇停更 | 独立 WS 连接 + REST 每 5s 兜底 + `/health.streams` 监控 |
| **P1** | `backtest/bars` mark 空洞 | `mark_price_klines.close` → `klines.close` 两级 fallback |
| **P2** | stale 判定过严 | 响应 `is_mark_stale` / `is_book_stale`；wallet 市价单仅检查 `is_book_stale` |
| **P2** | 可观测性不足 | `/health` 返回各流 `last_event_ms` + `age_ms` |
| **P3** | market store 无清理 | `DELETE /v1/market/tick/{symbol}` + `POST /v1/market/refresh` |
| **P3** | 清算未覆盖 | 深度测试 `liquidation` 用例（50x + 暴跌触发强平） |

### 12.6 SKIP / WARN 说明

- **SKIP `external_stale_reject`**：测试时外部 tick 恰好新鲜；provider 直测 `stale_provider` 已通过
- **WARN `backtest_bars`**：v2.2.0 已修复 mark fallback，重新跑测应无此 WARN

---

*文档版本：v1.3（遗留项全部闭环） | 2026-06-11 | market-data v2.2.0 | crypto_paper_wallet v0.1*
