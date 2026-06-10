# Market Data

币安 U 本位永续合约本地行情采集服务。持续采集 **BTCUSDT / ETHUSDT / SOLUSDT** 的全部公开市场数据，永久保存至本地 SQLite，并通过 HTTP API 供其他项目（策略、回测、研究）调用。

> 仓库地址：https://github.com/raphael2025/market-data（私有）

---

## 项目简介

本项目解决一个问题：**把币安合约行情搬到本地，变成随时可查的数据库**。

与直接调币安 API 相比的优势：

| 对比项 | 直接调币安 API | 本项目 |
|--------|---------------|--------|
| 请求限制 | 有权重/频率限制 | 本地查询**无限制** |
| 历史数据 | 需反复拉取、拼接 | 已持久化，按需查询 |
| 实时数据 | 需自己维护 WebSocket | 7×24 自动采集 |
| 多项目共用 | 每个项目各自连币安 | 统一数据源，API 共享 |
| 数据保留 | 无 | **永久保存**，只增不删 |

典型使用场景：

- 量化策略实时读取标记价格、买卖价、资金费率
- 回测系统批量拉取历史 K 线
- 研究项目分析多空比、持仓量、爆仓、基差
- 多个本地项目共用同一份行情数据

---

## 功能概览

### 采集范围

覆盖币安合约市场数据的 **REST + WebSocket 全部公开接口**：

| 类别 | 数据 | 采集方式 |
|------|------|----------|
| K 线 | 成交价 / 标记价 / 指数价 / 连续合约 K 线（15 种周期） | REST 回填 + WS 实时 |
| 成交 | 聚合成交、逐笔成交 | WS 实时 + REST 轮询 |
| 价格 | 标记价格、最优买卖价、24h Ticker | WS + REST |
| 深度 | 1000 档快照、100ms 增量更新 | REST + WS |
| 持仓 | 实时持仓量、历史持仓量统计 | REST 定时 |
| 资金费率 | 历史费率、费率配置 | REST 定时 |
| 市场情绪 | 多空比（4 种）、基差、爆仓 | REST + WS |
| 系统 | 保险基金、交割价、交易所规则 | REST 定时 |

共 **23 张数据表**，详见 [docs/API.md](docs/API.md)。

### 采集策略

```
启动
  │
  ├─ ① 立即启动实时采集（WebSocket + 高频 REST）  ← 优先
  ├─ ② 启动 HTTP API（立即可查询）
  └─ ③ 后台线程按优先级回填历史数据            ← 不阻塞实时
        ├─ 短周期（24h 成交、交割价）
        ├─ 30 天统计（多空比、基差、持仓量历史）
        ├─ 中等历史（资金费率）
        └─ 长历史 K 线（大周期优先，1m 最后，分批执行）
```

- 实时数据启动后**秒级可用**
- 历史回填在后台慢慢补，不影响 API 和实时写入
- 回填完成后自动进入**每小时增量维护**

---

## 系统架构

```
                    币安合约 API
                   (REST + WebSocket)
                          │
                          ▼
              ┌───────────────────────┐
              │   market-data 服务     │
              │                       │
              │  ws_collector.py      │ ← WebSocket 实时流
              │  rest_collector.py    │ ← REST 定时采集
              │  backfill_worker.py   │ ← 后台历史回填
              │                       │
              │  SQLite (WAL 模式)     │
              │  data/market.db       │
              │                       │
              │  api.py (:8765)       │ ← HTTP 查询接口
              └───────────┬───────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
        策略项目       回测项目       研究项目
     (HTTP API)     (HTTP API)    (SQLite 直连)
```

---

## 环境要求

- **操作系统**：Linux（已配置 systemd）
- **Python**：3.10+
- **网络**：能访问 `fapi.binance.com` 和 `fstream.binance.com`
- **磁盘**：数据持续增长，建议预留 50GB+（深度增量数据量较大）

---

## 安装与启动

### 方式一：systemd 系统服务（推荐）

一次安装，开机自启，崩溃自动重启。**日常使用只需这一步。**

```bash
cd /home/raphael/market-data

# 安装依赖 + 注册系统服务
./install-service.sh
```

脚本会自动完成：

1. 创建 Python 虚拟环境（`.venv`）
2. 安装 `requirements.txt` 依赖
3. 创建 `data/` 和 `logs/` 目录
4. 注册 systemd 服务 `market-data`
5. 设置开机自启并立即启动

验证是否正常运行：

```bash
# 服务状态应为 active (running)
sudo systemctl status market-data

# API 健康检查
curl http://localhost:8765/health
# 期望返回 status=ok，且 streams 中 is_mark_stale / is_book_stale 均为 false

# 查看数据量
curl http://localhost:8765/tables
```

### 方式二：手动启动（开发调试）

```bash
cd /home/raphael/market-data

# 首次需要安装依赖
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 启动（实时采集 + 后台回填 + API）
./run.sh
```

`Ctrl+C` 可停止。手动启动不会开机自启。

### 启动模式说明

| 命令 | 说明 | 使用场景 |
|------|------|----------|
| `./run.sh` | 实时采集 + 后台回填 + API | 等同 systemd 服务 |
| `./run.sh --no-backfill` | 仅实时采集 + API | 不需要历史回填时 |
| `./run.sh --backfill-only` | 阻塞式全量回填 | 一次性补历史，完成后退出 |
| `./run.sh --api-only` | 仅 API 查询 | 数据库已有数据，只读查询 |

> 生产环境用 `./install-service.sh` 即可，不需要手动选模式。

---

## 服务信息

| 项 | 值 |
|---|---|
| API 地址 | http://localhost:8765 |
| 交互式 API 文档 | http://localhost:8765/docs |
| 外部项目接入文档 | [docs/API.md](docs/API.md) |
| 数据库文件 | `data/market.db` |
| 运行日志 | `logs/collector.log` |
| systemd 服务名 | `market-data` |

---

## 日常运维

```bash
# 查看服务状态
sudo systemctl status market-data

# 启动 / 停止 / 重启
sudo systemctl start market-data
sudo systemctl stop market-data
sudo systemctl restart market-data

# 实时查看日志
tail -f logs/collector.log

# 查看数据库大小
ls -lh data/market.db

# 重新安装服务（更新代码后）
./install-service.sh
```

日志中正常运行时应看到：

```
实时采集已启动 (WebSocket + REST 定时任务)
WebSocket [public] 已连接
WebSocket [market] 已连接
后台回填线程已启动（实时采集优先）
```

---

## 配置

编辑 `config.yaml` 可自定义：

```yaml
symbols:              # 采集的交易对
  - BTCUSDT
  - ETHUSDT
  - SOLUSDT

kline_intervals:      # K 线周期（15 种）
  - 1m ... 1M

api:
  host: 0.0.0.0
  port: 8765          # API 端口

schedules:            # REST 采集频率（秒）
  depth_snapshot: 15  # 深度快照每 15 秒
  trades_poll: 10     # 逐笔成交每 10 秒
  ...

backfill_days: 0      # 0 = 从合约上线日起尽可能回填
```

修改配置后重启服务生效：

```bash
sudo systemctl restart market-data
```

### 回填 vs 实时优先级

| 通道 | 数据 | 策略 |
|------|------|------|
| **WebSocket**（最高） | mark/book/depth/trade/kline 实时 | 不占 REST 权重 |
| **REST 定时任务** | book/mark/depth 兜底、持仓量等 | `max_weight_per_minute` 主通道 |
| **REST 历史回填** | 24h 以外 K 线、资金费率历史等 | 独立 `backfill_max_weight_per_minute`，每批后 sleep 8s |

深度增量 `depth_updates` 等**官方拉不到历史**的数据，只靠 WS 实时写入，回填不会挤占其 REST 配额。

---

## 其他项目接入

其他本地项目通过 HTTP API 读取数据，无需直连币安：

```python
import requests

BASE = "http://localhost:8765"

# 确认服务在线
assert requests.get(f"{BASE}/health").json()["status"] == "ok"

# 获取 BTC 最新标记价格和资金费率
mark = requests.get(f"{BASE}/v1/mark-price/latest", params={"symbol": "BTCUSDT"}).json()

# 获取 ETH 1 小时 K 线（最近 500 根）
klines = requests.get(f"{BASE}/v1/klines", params={
    "symbol": "ETHUSDT", "interval": "1h", "limit": 500,
}).json()["data"]

# 获取 SOL 多空比
ratio = requests.get(f"{BASE}/v1/long-short-ratio", params={
    "symbol": "SOLUSDT", "data_type": "global_account", "period": "1h", "limit": 10,
}).json()["data"]
```

完整接口列表、请求参数、响应字段、分页方式、SQLite 直连方法，见 **[docs/API.md](docs/API.md)**。

对接 `crypto_paper_wallet` 时，优先使用聚合 tick 接口（一次请求拿齐 mark/bid/ask/funding）：

```python
tick = requests.get(f"{BASE}/v1/tick/latest", params={"symbol": "BTCUSDT"}).json()
# 或兼容路径：GET /v1/market/tick/BTCUSDT
```

详见 [docs/IMPROVEMENTS_FOR_PAPER_WALLET.md](docs/IMPROVEMENTS_FOR_PAPER_WALLET.md)。

---

## 项目结构

```
market-data/
├── README.md                 # 项目说明（本文件）
├── config.yaml               # 配置文件
├── requirements.txt          # Python 依赖
├── run.sh                    # 手动启动脚本
├── install-service.sh        # 安装 systemd 服务
│
├── docs/
│   └── API.md                # 外部项目接入文档
│
├── deploy/
│   └── market-data.service   # systemd 服务单元
│
├── src/
│   ├── main.py               # 入口：启动实时采集 + API + 后台回填
│   ├── api.py                # FastAPI HTTP 查询接口
│   ├── ws_collector.py       # WebSocket 实时采集
│   ├── rest_collector.py     # REST 定时采集
│   ├── backfill_worker.py    # 后台历史回填（按优先级排队）
│   ├── storage.py            # SQLite 存储层（23 张表）
│   └── config.py             # 配置加载
│
├── data/
│   └── market.db             # SQLite 数据库（不纳入 git）
│
└── logs/
    └── collector.log         # 运行日志（不纳入 git）
```

---

## 常见问题

**Q: API 返回 404 "无数据"？**
历史回填尚在后台进行中，该数据类型的历史尚未补全。实时数据（标记价、买卖价、成交）通常立即可用。

**Q: 如何确认服务在运行？**
```bash
curl http://localhost:8765/health
sudo systemctl status market-data
```

**Q: `/health` 显示 `is_book_stale` 或 `is_mark_stale` 为 true？**

检查 `logs/collector.log` 是否有 `bookTicker` / `markPrice` 相关错误。常见原因：

- REST 兜底路径错误（应为 `/fapi/v1/ticker/bookTicker`，不是 `/ticker/book`）
- WebSocket 握手超时（网络慢时 REST 每 5s/15s 兜底应仍保持新鲜）

修复后重启：`sudo systemctl restart market-data`

**Q: 数据库越来越大怎么办？**
设计为永久保留。如需清理，停止服务后删除 `data/market.db`，重启会自动重建并重新采集。

**Q: 如何添加更多交易对？**
编辑 `config.yaml` 的 `symbols` 列表，重启服务。后台会自动回填新交易对的历史数据。

**Q: 克隆到新机器怎么部署？**
```bash
git clone https://github.com/raphael2025/market-data.git
cd market-data
./install-service.sh
```

---

## 技术栈

- **Python 3.12** + asyncio
- **SQLite WAL** — 支持并发读写
- **FastAPI** — HTTP API
- **APScheduler** — REST 定时任务
- **websockets** — 币安 WebSocket 实时流
- **systemd** — 系统级进程管理
