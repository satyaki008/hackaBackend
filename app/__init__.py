"""ResiStore Distributed Object Storage System backend package."""
import sys
from pathlib import Path

# Ensure backend root is in sys.path
_backend_dir = Path(__file__).resolve().parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

__version__ = "2.0.0"
