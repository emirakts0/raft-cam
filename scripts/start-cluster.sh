#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

echo "=================================================="
echo " Starting Raft Cluster Binary Build..."
echo "=================================================="
mkdir -p bin data logs
go build -o bin/raft-node .

echo "Stopping any previously running instances..."
pkill -f "bin/raft-node" 2>/dev/null || true
sleep 1

# Clean old data directories for clean cluster start
rm -rf data/* logs/*

echo "=================================================="
echo " Starting 3-Node Raft Cluster"
echo "=================================================="

# Node 1: Bootstrap Node
echo "-> Starting Node 1 (Bootstrap) on Raft :7001 | HTTP :8001..."
setsid ./bin/raft-node \
  --node-id=node-1 \
  --raft-addr=127.0.0.1:7001 \
  --http-addr=127.0.0.1:8001 \
  --data-dir=data/node-1 \
  --bootstrap < /dev/null > logs/node-1.log 2>&1 &
PID1=$!
echo "   Node 1 PID: $PID1"

# Give Node 1 a moment to bootstrap and elect itself as leader
sleep 2

# Node 2: Joins via Node 1
echo "-> Starting Node 2 on Raft :7002 | HTTP :8002..."
setsid ./bin/raft-node \
  --node-id=node-2 \
  --raft-addr=127.0.0.1:7002 \
  --http-addr=127.0.0.1:8002 \
  --data-dir=data/node-2 \
  --join-addr=http://127.0.0.1:8001 < /dev/null > logs/node-2.log 2>&1 &
PID2=$!
echo "   Node 2 PID: $PID2"

sleep 1

# Node 3: Joins via Node 1
echo "-> Starting Node 3 on Raft :7003 | HTTP :8003..."
setsid ./bin/raft-node \
  --node-id=node-3 \
  --raft-addr=127.0.0.1:7003 \
  --http-addr=127.0.0.1:8003 \
  --data-dir=data/node-3 \
  --join-addr=http://127.0.0.1:8001 < /dev/null > logs/node-3.log 2>&1 &
PID3=$!
echo "   Node 3 PID: $PID3"

sleep 1

echo "=================================================="
echo " Cluster successfully initialized!"
echo " Web UI & SSE Endpoints:"
echo "   - Node 1: http://localhost:8001"
echo "   - Node 2: http://localhost:8002"
echo "   - Node 3: http://localhost:8003"
echo "=================================================="
echo " Logs are streamed in logs/ directory."
echo " To stop the cluster, run: ./scripts/stop-cluster.sh"
