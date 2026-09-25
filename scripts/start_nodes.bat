@echo off
echo Starting AegisStore Storage Node Daemons...
start "Node 1" python backend\app\node_server.py --port 8001 --node-id node-1 --storage-dir storage\node1
start "Node 2" python backend\app\node_server.py --port 8002 --node-id node-2 --storage-dir storage\node2
start "Node 3" python backend\app\node_server.py --port 8003 --node-id node-3 --storage-dir storage\node3
start "Node 4" python backend\app\node_server.py --port 8004 --node-id node-4 --storage-dir storage\node4
echo All 4 storage nodes started on ports 8001-8004.
