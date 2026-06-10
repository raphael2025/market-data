# Market Data

币安 U 本位合约（BTC / ETH / SOL）本地全量行情采集服务。持续写入 SQLite，通过 HTTP API 供其他项目调用。

## 服务信息

| 项 | 值 |
|---|---|
| API 地址 | http://localhost:8765 |
| API 文档 | [docs/API.md](docs/API.md) ← **其他项目接入看这里** |
| 交互式文档 | http://localhost:8765/docs |
| 数据库 | `/home/raphael/market-data/data/market.db` |
| 日志 | `logs/collector.log` |

## 运维命令

```bash
# 查看状态
sudo systemctl status market-data

# 启停
sudo systemctl start market-data
sudo systemctl stop market-data

# 查看日志
tail -f logs/collector.log

# 重新安装服务
./install-service.sh
```

## 采集策略

- **实时优先**：启动即采集 WebSocket + REST，API 立即可用
- **后台回填**：历史数据在独立线程按优先级排队，不阻塞实时
- **永久保留**：数据只增不删

## 开发命令

```bash
./run.sh                  # 实时 + 后台回填（等同 systemd 服务）
./run.sh --no-backfill    # 仅实时
./run.sh --backfill-only  # 阻塞式全量回填
./run.sh --api-only       # 仅 API
```

## 其他项目接入

```python
import requests
BASE = "http://localhost:8765"

# 健康检查
requests.get(f"{BASE}/health").json()

# 最新标记价格
requests.get(f"{BASE}/v1/mark-price/latest", params={"symbol": "BTCUSDT"}).json()

# K 线
requests.get(f"{BASE}/v1/klines", params={"symbol": "ETHUSDT", "interval": "1h", "limit": 500}).json()
```

完整接口说明、字段定义、分页方式、SQLite 直连方式见 **[docs/API.md](docs/API.md)**。

## 项目结构

```
market-data/
├── config.yaml          # 交易对、采集频率
├── data/market.db       # SQLite 数据库
├── docs/API.md          # 外部项目接入文档
├── deploy/              # systemd 服务文件
├── src/
│   ├── api.py           # HTTP API
│   ├── ws_collector.py  # WebSocket 实时采集
│   ├── rest_collector.py# REST 定时采集
│   └── backfill_worker.py # 后台历史回填
├── install-service.sh
└── run.sh
```
