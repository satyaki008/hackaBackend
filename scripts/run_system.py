"""Unified System Launcher for AegisStore Distributed Object Storage.
Starts the Controller Gateway (port 8000) and the 4 Storage Node Daemons (ports 8001-8004).
Includes automatic port cleanup to prevent [Errno 10048] address in use errors.
"""
import os
import sys
import time
from pathlib import Path
import psutil
import uvicorn

# Add backend directory and its parent to sys.path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))
if str(backend_dir.parent) not in sys.path:
    sys.path.insert(0, str(backend_dir.parent))

import types
if "backend" not in sys.modules:
    backend_pkg = types.ModuleType("backend")
    backend_pkg.__path__ = [str(backend_dir)]
    sys.modules["backend"] = backend_pkg

def cleanup_stale_ports(ports=[8000, 8001, 8002, 8003, 8004]):
    """Terminate any orphaned processes holding AegisStore cluster ports."""
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.pid == current_pid:
                continue
            for conn in proc.net_connections(kind='inet'):
                if conn.laddr and conn.laddr.port in ports:
                    print(f"  [!] Freeing port {conn.laddr.port} from stale process {proc.pid} ({proc.name()})...")
                    proc.kill()
                    time.sleep(0.2)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

def main():
    print("=" * 70)
    print("  RESISTORE: ENTERPRISE SELF-HEALING DISTRIBUTED OBJECT STORAGE")
    print("=" * 70)
    print("  Checking for and cleaning any stale port bindings...")
    cleanup_stale_ports()

    print("  Initializing storage directories & cluster gateway...")
    print("  [+] Controller Gateway ONLINE -> http://127.0.0.1:8000")
    print("  [+] Node 1 [ACTIVE CLOUD]     -> MongoDB Atlas Cloud (cluster0.i7avjwe.mongodb.net)")
    print("  [+] Node 2 [STANDBY LOCAL]    -> http://127.0.0.1:8002 (Attach Cloud MongoDB anytime)")
    print("  [+] Node 3 [STANDBY LOCAL]    -> http://127.0.0.1:8003 (Attach Cloud MongoDB anytime)")
    print("  [+] Node 4 [STANDBY LOCAL]    -> http://127.0.0.1:8004 (Attach Cloud MongoDB anytime)")
    print("  [+] Interactive Swagger Docs  -> http://127.0.0.1:8000/docs")
    print("  [+] Built Web Dashboard       -> http://127.0.0.1:8000/dashboard/")
    print("  [+] Frontend Dev Server       -> http://localhost:5173 (run 'npm run dev' in /frontend)")
    print("=" * 70)
    print("  Press Ctrl+C to shut down.")
    print("=" * 70)

    try:
        uvicorn.run(
            "backend.app.main:app",
            host="0.0.0.0",
            port=8000,
            log_level="info",
            access_log=False
        )
    except KeyboardInterrupt:
        print("\n  AegisStore cluster shut down cleanly.")

if __name__ == "__main__":
    main()
