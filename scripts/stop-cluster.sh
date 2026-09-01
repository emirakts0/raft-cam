#!/usr/bin/env bash
echo "[INFO] Stopping all Raft cluster nodes..."
pkill -f "bin/raft-node" 2>/dev/null || true
sleep 1
echo "[INFO] All nodes stopped."
