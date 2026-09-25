"""ResiStore Backend Entrypoint for Render / Cloud Hosting.
Provides standalone execution and uvicorn binding with dynamic $PORT support.
"""
import os
import sys
from pathlib import Path

# Add backend directory to sys.path so 'app' is immediately importable
backend_dir = Path(__file__).resolve().parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# Import the FastAPI application
from app.main import app

# Backwards-compatibility alias for legacy imports
import app as app_module
sys.modules["backend.app"] = app_module
sys.modules["backend"] = app_module

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
