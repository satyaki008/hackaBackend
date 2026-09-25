#!/bin/bash
echo "Stopping AegisStore node processes..."
pkill -f "backend/app/node_server.py"
echo "Done."
