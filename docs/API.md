# Market Data API — 外部项目接入文档

本文档供**其他本地项目**调用行情数据库。服务持续采集币安 U 本位合约 **BTCUSDT / ETHUSDT / SOLUSDT** 全部公开市场数据，永久保存。

---

## 连接信息

| 项 | 值 |
|---|---|
| 基址 | `http://localhost:8765` |
| 交互式文档 | http://localhost:8765/docs |
| 数据库文件 | `/home/raphael/market-data/data/market.db` |
| 认证 | 无 |
| 请求限制 | **无** |
| 数据更新 | 7×24 持续写入 |

### 健康检查

```bash
curl http://localhost:8765/health
```

```json
{
  "status": "ok",
  "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
  "rate_limit": null
}
```

服务未启动时：

```bash
sudo systemctl status market-data
sudo systemctl start market-data
```

---

## 快速接入

### Python

```python
import requests

BASE = "http://localhost:8765"

# 最新标记价格
r = requests.get(f"{BASE}/v1/mark-price/latest", params={"symbol": "BTCUSDT"})
mark = r.json()
print(mark["mark_price"], mark["funding_rate"])

# 分页拉取 K 线
def fetch_all_klines(symbol: str, interval: str = "1h") -> list:
    offset, rows = 0, []
    while True:
        resp = requests.get(f"{BASE}/v1/klines", params={
            "symbol": symbol, "interval": interval,
            "limit": 50000, "offset": offset,
        }).json()
        rows.extend(resp["data"])
        if offset + resp["limit"] >= resp["total"]:
            break
        offset += resp["limit"]
    return rows
```

### JavaScript / TypeScript

```typescript
const BASE = "http://localhost:8765";

const res = await fetch(`${BASE}/v1/book-ticker/latest?symbol=ETHUSDT`);
const book = await res.json();
console.log(book.bid_price, book.ask_price);
```

### 直接读 SQLite（高性能场景）

采集服务使用 WAL 模式，支持**并发读取**：

```python
import sqlite3

DB = "/home/raphael/market-data/data/market.db"
con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
con.row_factory = sqlite3.Row

rows = con.execute("""
    SELECT * FROM klines
    WHERE symbol = 'BTCUSDT' AND interval = '1h'
    ORDER BY open_time DESC LIMIT 500
""").fetchall()
```

推荐：一般查询走 HTTP API；大批量分析、回测可直接读 SQLite。

---

## 通用约定

### 分页响应格式

绝大多数列表接口返回：

```json
{
  "total": 1512760,
  "limit": 1000,
  "offset": 0,
  "data": [ { ... }, { ... } ]
}
```

- `total`：符合条件的总条数
- `data`：当前页数据，**默认按时间倒序**（最新在前）
- `limit` / `offset`：分页参数，**无上限**

### 时间戳

所有时间字段均为 **Unix 毫秒**（ms）。

### 交易对

| symbol | 说明 |
|--------|------|
| BTCUSDT | 比特币永续 |
| ETHUSDT | 以太坊永续 |
| SOLUSDT | Solana 永续 |

`pair` 参数与 symbol 相同（如 `BTCUSDT`）。

### K 线周期 `interval`

```
1m  3m  5m  15m  30m  1h  2h  4h  6h  8h  12h  1d  3d  1w  1M
```

### 统计周期 `period`（多空比、持仓量历史、基差）

```
5m  15m  30m  1h  2h  4h  6h  12h  1d
```

### 错误码

| HTTP | 含义 |
|------|------|
| 200 | 成功 |
| 404 | 无数据（latest 类接口） |
| 400 | 参数错误（如无效表名） |

---

## 接口索引

### 系统

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 服务状态 |
| GET | `/tables` | 各表数据量 |
| GET | `/tables/{table}/schema` | 表结构 |

### K 线

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/klines` | 成交价 K 线 OHLCV |
| GET | `/v1/mark-price-klines` | 标记价格 K 线 |
| GET | `/v1/index-price-klines` | 指数价格 K 线 |
| GET | `/v1/continuous-klines` | 连续合约 K 线 |
| GET | `/v1/kline-updates` | K 线盘中每次推送（含未收盘） |

### 成交

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/agg-trades` | 聚合成交 |
| GET | `/v1/trades` | 逐笔成交 |

### 价格 / 行情

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/mark-price` | 标记价格序列 |
| GET | `/v1/mark-price/latest` | 最新标记价格 |
| GET | `/v1/book-ticker` | 最优买卖价序列 |
| GET | `/v1/book-ticker/latest` | 最新买卖价 |
| GET | `/v1/ticker-price` | 最新成交价快照 |
| GET | `/v1/ticker/24h` | 24h Ticker 序列 |
| GET | `/v1/ticker/24h/latest` | 最新 24h Ticker |
| GET | `/v1/ticker/snapshots` | WS Ticker 原始 JSON |

### 深度

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/depth/snapshots` | 深度全量快照（1000档） |
| GET | `/v1/depth/snapshots/latest` | 最新深度快照 |
| GET | `/v1/depth/updates` | 深度增量更新（100ms） |

### 持仓 / 资金费率

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/open-interest` | 实时持仓量 |
| GET | `/v1/open-interest/latest` | 最新持仓量 |
| GET | `/v1/open-interest/history` | 持仓量历史 |
| GET | `/v1/funding-rates` | 资金费率历史 |
| GET | `/v1/funding-info` | 资金费率配置 |

### 市场情绪

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/long-short-ratio` | 多空比 |
| GET | `/v1/basis` | 基差 |
| GET | `/v1/liquidations` | 爆仓/强平 |

### 元数据

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/insurance-balance` | 保险基金 |
| GET | `/v1/delivery-prices` | 历史交割价 |
| GET | `/v1/exchange-info` | 交易所规则快照 |
| GET | `/v1/exchange-info/latest` | 最新交易所规则 |

### 通用

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/query/{table}` | 按表名直接查询 |

---

## 接口详情

### `GET /v1/klines`

成交价 K 线，最常用的接口。

**请求参数**

| 参数 | 必填 | 默认 | 说明 |
|------|------|------|------|
| symbol | 是 | — | BTCUSDT / ETHUSDT / SOLUSDT |
| interval | 否 | 1h | K 线周期 |
| start_time | 否 | — | 起始 open_time（ms） |
| end_time | 否 | — | 结束 open_time（ms） |
| limit | 否 | 1000 | 每页条数 |
| offset | 否 | 0 | 偏移 |

**请求示例**

```bash
curl "http://localhost:8765/v1/klines?symbol=BTCUSDT&interval=1h&limit=10"
```

**响应示例**

```json
{
  "total": 5000,
  "limit": 10,
  "offset": 0,
  "data": [
    {
      "symbol": "BTCUSDT",
      "interval": "1h",
      "open_time": 1781121600000,
      "open": 61908.7,
      "high": 61921.2,
      "low": 61644.4,
      "close": 61761.0,
      "volume": 2752.508,
      "close_time": 1781125199999,
      "quote_volume": 170104613.75,
      "trades": 119532,
      "taker_buy_volume": 1319.101,
      "taker_buy_quote_volume": 81524511.78
    }
  ]
}
```

**字段说明**

| 字段 | 类型 | 说明 |
|------|------|------|
| open_time | int | K 线开盘时间（ms） |
| open / high / low / close | float | 开高低收 |
| volume | float | 成交量（BTC 个数） |
| quote_volume | float | 成交额（USDT） |
| trades | int | 成交笔数 |
| taker_buy_volume | float | 主动买入量 |
| taker_buy_quote_volume | float | 主动买入额 |

---

### `GET /v1/mark-price/latest`

获取最新标记价格、指数价格、资金费率。适合策略实时定价。

**请求**

```bash
curl "http://localhost:8765/v1/mark-price/latest?symbol=BTCUSDT"
```

**响应**

```json
{
  "symbol": "BTCUSDT",
  "mark_price": 61753.12,
  "index_price": 61783.92,
  "funding_rate": -0.00001995,
  "next_funding_time": 1781136000000,
  "event_time": 1781124557003
}
```

---

### `GET /v1/book-ticker/latest`

最新买一卖一，适合价差监控。

```json
{
  "symbol": "BTCUSDT",
  "bid_price": 61699.1,
  "bid_qty": 9.443,
  "ask_price": 61699.2,
  "ask_qty": 1.444,
  "event_time": 1781124856146
}
```

---

### `GET /v1/agg-trades`

聚合成交，实时性高（WebSocket 写入）。

| 参数 | 必填 | 说明 |
|------|------|------|
| symbol | 是 | 交易对 |
| start_time | 否 | 起始 trade_time（ms） |
| limit / offset | 否 | 分页 |

```json
{
  "symbol": "BTCUSDT",
  "agg_id": 2845729183,
  "price": 61700.5,
  "qty": 0.012,
  "trade_time": 1781124856146,
  "is_buyer_maker": 1
}
```

`is_buyer_maker=1` 表示买方是挂单方（被动成交）。

---

### `GET /v1/long-short-ratio`

市场情绪数据。

| 参数 | 必填 | 默认 | 说明 |
|------|------|------|------|
| symbol | 是 | — | 交易对 |
| data_type | 否 | global_account | 见下表 |
| period | 否 | 1h | 统计周期 |
| limit / offset | 否 | — | 分页 |

**data_type 取值**

| 值 | 含义 |
|----|------|
| global_account | 全市场账户多空比 |
| top_account | 大户（前20%）账户多空比 |
| top_position | 大户持仓多空比 |
| taker | 主动买卖量比 |

```json
{
  "symbol": "BTCUSDT",
  "data_type": "global_account",
  "period": "1h",
  "long_short_ratio": 1.25,
  "long_account": 0.55,
  "short_account": 0.45,
  "buy_vol": null,
  "sell_vol": null,
  "event_time": 1781124000000
}
```

`taker` 类型时 `buy_vol` / `sell_vol` 有值，`long_account` 为 null。

---

### `GET /v1/funding-rates`

```json
{
  "symbol": "BTCUSDT",
  "funding_rate": -0.00001716,
  "funding_time": 1781136000000,
  "mark_price": 61859.98
}
```

---

### `GET /v1/depth/snapshots/latest`

1000 档深度快照。`bids` / `asks` 为 JSON 字符串，需二次解析。

```json
{
  "symbol": "BTCUSDT",
  "bids": "[[\"61699.10\",\"9.443\"], ...]",
  "asks": "[[\"61699.20\",\"1.444\"], ...]",
  "last_update_id": 8234567890,
  "snapshot_time": 1781124856146
}
```

```python
import json
depth = requests.get(f"{BASE}/v1/depth/snapshots/latest", params={"symbol": "BTCUSDT"}).json()
bids = json.loads(depth["bids"])  # [[price, qty], ...]
```

---

### `GET /v1/query/{table}`

万能查询，按表名读取任意数据。

```bash
# 查爆仓记录
curl "http://localhost:8765/v1/query/liquidations?symbol=BTCUSDT&limit=50"

# 查资金费率
curl "http://localhost:8765/v1/query/funding_rates?symbol=ETHUSDT&limit=100"
```

**可用表名**

```
klines                  mark_price_klines       index_price_klines
continuous_klines       kline_updates           agg_trades
trades                  mark_prices             book_tickers
ticker_price            depth_snapshots         depth_updates
open_interest           open_interest_hist      funding_rates
funding_info            long_short_ratio        ticker_24h
ticker_snapshots        basis                   liquidations
insurance_balance       delivery_prices         exchange_info
```

---

## 数据表与字段速查

### klines — 成交价 K 线

| 列 | 类型 | 说明 |
|----|------|------|
| symbol | TEXT | 交易对 |
| interval | TEXT | 周期 |
| open_time | INTEGER | 开盘时间 PK |
| open/high/low/close | REAL | OHLC |
| volume | REAL | 成交量 |
| close_time | INTEGER | 收盘时间 |
| quote_volume | REAL | 成交额 |
| trades | INTEGER | 笔数 |
| taker_buy_volume | REAL | 主动买入量 |
| taker_buy_quote_volume | REAL | 主动买入额 |

### agg_trades — 聚合成交

| 列 | 说明 |
|----|------|
| agg_id | 聚合 ID |
| price / qty | 价格 / 数量 |
| trade_time | 成交时间 |
| is_buyer_maker | 0/1 |

### mark_prices — 标记价格

| 列 | 说明 |
|----|------|
| mark_price | 标记价 |
| index_price | 指数价 |
| funding_rate | 资金费率 |
| next_funding_time | 下次结算 |
| event_time | 记录时间 |

### book_tickers — 最优买卖

| 列 | 说明 |
|----|------|
| bid_price / bid_qty | 买一 |
| ask_price / ask_qty | 卖一 |
| event_time | 时间 |

### depth_snapshots — 深度快照

| 列 | 说明 |
|----|------|
| bids / asks | JSON 字符串 `[[price,qty],...]` |
| last_update_id | 更新序号 |
| snapshot_time | 快照时间 |

### long_short_ratio — 多空比

| 列 | 说明 |
|----|------|
| data_type | global_account / top_account / top_position / taker |
| period | 统计周期 |
| long_short_ratio | 比值 |
| long_account / short_account | 多空账户占比 |
| buy_vol / sell_vol | 主动买卖量（taker 类型） |
| event_time | 时间 |

---

## 典型使用场景

### 策略获取最新价 + 资金费率

```python
mark = requests.get(f"{BASE}/v1/mark-price/latest", params={"symbol": "BTCUSDT"}).json()
book = requests.get(f"{BASE}/v1/book-ticker/latest", params={"symbol": "BTCUSDT"}).json()
spread = book["ask_price"] - book["bid_price"]
```

### 回测拉取历史 K 线

```python
klines = fetch_all_klines("BTCUSDT", "1h")
# 按 open_time 升序排列用于回测
klines.sort(key=lambda k: k["open_time"])
```

### 监控持仓量 + 多空比

```python
oi = requests.get(f"{BASE}/v1/open-interest/latest", params={"symbol": "ETHUSDT"}).json()
ratio = requests.get(f"{BASE}/v1/long-short-ratio", params={
    "symbol": "ETHUSDT", "data_type": "global_account", "period": "1h", "limit": 1,
}).json()["data"][0]
```

### 按时间范围查询

```python
import time
day_ago = int((time.time() - 86400) * 1000)
trades = requests.get(f"{BASE}/v1/agg-trades", params={
    "symbol": "SOLUSDT", "start_time": day_ago, "limit": 100000,
}).json()
```

---

## 注意事项

1. **数据持续写入**：历史回填在后台进行，早期数据会随时间逐步补全
2. **排序**：列表接口默认**最新在前**（DESC），回测请自行按 `open_time` 升序
3. **深度 JSON**：`depth_snapshots` 的 bids/asks 是字符串，需 `json.loads()`
4. **ticker_snapshots**：`payload` 字段为原始 WS JSON 字符串
5. **并发安全**：HTTP API 只读，可与采集服务并行；SQLite 直连请用 `mode=ro`
6. **服务依赖**：调用前确认 `curl http://localhost:8765/health` 返回 ok

---

## 兼容旧路径

| 旧路径 | 新路径 | 说明 |
|--------|--------|------|
| `GET /klines` | `GET /v1/klines` | 旧路径直接返回 `data` 数组 |
| `GET /trades` | `GET /v1/agg-trades` | 旧路径直接返回 `data` 数组 |

新项目请使用 `/v1/` 前缀接口。
