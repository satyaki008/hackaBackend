"""Test configuration, fixtures, and unified ASGI test transport."""
import shutil
import sys
import types
from pathlib import Path
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import httpx

# Ensure 'backend' is resolvable when pytest is run directly from backend folder
_backend_dir = Path(__file__).resolve().parent.parent
if str(_backend_dir.parent) not in sys.path:
    sys.path.insert(0, str(_backend_dir.parent))
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))
if "backend" not in sys.modules:
    _pkg = types.ModuleType("backend")
    _pkg.__path__ = [str(_backend_dir)]
    sys.modules["backend"] = _pkg

from backend.app.database import Base, get_db
from backend.app.models.models import NodeModel
from backend.app.node_server import create_node_app
from backend.app.main import app

TEST_STORAGE_DIR = Path(__file__).resolve().parent / "test_storage"
TEST_DB_URL = "sqlite:///./test_metadata.db"

test_engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test storage directories and database tables."""
    if TEST_STORAGE_DIR.exists():
        try:
            shutil.rmtree(TEST_STORAGE_DIR)
        except Exception:
            pass
    TEST_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    
    for i in range(1, 5):
        (TEST_STORAGE_DIR / f"node{i}").mkdir(parents=True, exist_ok=True)

    yield
    
    test_engine.dispose()
    try:
        if TEST_STORAGE_DIR.exists():
            shutil.rmtree(TEST_STORAGE_DIR, ignore_errors=True)
        test_db_file = Path("./test_metadata.db")
        if test_db_file.exists():
            test_db_file.unlink(missing_ok=True)
    except Exception:
        pass

@pytest.fixture(autouse=True)
def reset_database():
    """Reset all tables and re-seed the 4 nodes cleanly before each test."""
    Base.metadata.drop_all(bind=test_engine)
    Base.metadata.create_all(bind=test_engine)
    
    db = TestingSessionLocal()
    for i in range(1, 5):
        node = NodeModel(
            id=f"node-{i}",
            name=f"Test Node {i}",
            url=f"http://127.0.0.1:800{i}",
            storage_path=str(TEST_STORAGE_DIR / f"node{i}"),
            status="ONLINE",
            total_capacity_bytes=524288000,
            used_capacity_bytes=0,
            free_capacity_bytes=524288000,
            object_count=0,
            is_partitioned=False,
            simulated_latency_ms=0
        )
        db.add(node)
    db.commit()
    db.close()
    yield

@pytest.fixture
def db_session():
    """Provides a fresh transactional database session for tests."""
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

@pytest.fixture(autouse=True)
def route_storage_node_http(monkeypatch):
    """Intercept HTTP requests and route to node apps or main app."""
    node_apps = {
        "8001": create_node_app("node-1", str(TEST_STORAGE_DIR / "node1")),
        "8002": create_node_app("node-2", str(TEST_STORAGE_DIR / "node2")),
        "8003": create_node_app("node-3", str(TEST_STORAGE_DIR / "node3")),
        "8004": create_node_app("node-4", str(TEST_STORAGE_DIR / "node4")),
    }

    node_transports = {
        f"127.0.0.1:{p}": httpx.ASGITransport(app=node_apps[p])
        for p in node_apps
    }
    app_transport = httpx.ASGITransport(app=app)

    original_async_client_init = httpx.AsyncClient.__init__

    def mock_async_client_init(self, *args, **kwargs):
        passed_transport = kwargs.get("transport")

        class SmartRouterTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                host_port = f"{request.url.host}:{request.url.port}"
                if host_port in node_transports:
                    return await node_transports[host_port].handle_async_request(request)
                if request.url.host in ("test", "localhost", "127.0.0.1"):
                    if passed_transport:
                        return await passed_transport.handle_async_request(request)
                    return await app_transport.handle_async_request(request)
                if passed_transport:
                    return await passed_transport.handle_async_request(request)
                raise httpx.ConnectError(f"Connection refused to {request.url}")

        kwargs["transport"] = SmartRouterTransport()
        original_async_client_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", mock_async_client_init)

    # Override get_db dependency in main FastAPI app
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
