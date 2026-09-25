#!/bin/bash
echo "Starting AegisStore Storage Node Daemons..."
python3 backend/app/node_server.py --port 8001 --node-id node-1 --storage-dir storage/node1 &
python3 backend/app/node_server.py --port 8002 --node-id node-2 --storage-dir storage/node2 &
python3 backend/app/node_server.py --port 8003 --node-id node-3 --storage-dir storage/node3 &
python3 backend/app/node_server.py --port 8004 --node-id node-4 --storage-dir storage/node4 &
echo "All 4 storage nodes started in background on ports 8001-8004."
