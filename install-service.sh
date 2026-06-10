#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install -r requirements.txt -q
fi

mkdir -p logs data

echo "安装 systemd 服务..."
sudo cp deploy/market-data.service /etc/systemd/system/market-data.service
sudo systemctl daemon-reload
sudo systemctl enable market-data
sudo systemctl restart market-data

echo ""
echo "服务状态:"
sudo systemctl status market-data --no-pager
echo ""
echo "API: http://localhost:8765"
echo "文档: http://localhost:8765/docs"
echo "API 说明: docs/API.md"
