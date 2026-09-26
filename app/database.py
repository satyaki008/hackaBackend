import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.engine import Engine
from app.config import settings

from sqlalchemy.pool import NullPool

# Connection args and pool configuration
connect_args = {}
if "sqlite" in settings.DATABASE_URL:
    connect_args["check_same_thread"] = False
    connect_args["timeout"] = 30  # SQLite busy timeout at DB-API driver level
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args=connect_args,
        poolclass=NullPool,  # Completely eliminates QueuePool limit exhaustion for SQLite
    )
else:
    engine = create_engine(
        settings.DATABASE_URL,
        pool_size=25,
        max_overflow=50,
        pool_timeout=60,
        pool_pre_ping=True
    )

# Enable WAL mode, foreign keys, and busy timeout for high-performance concurrent SQLite
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if "sqlite" in settings.DATABASE_URL:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def apply_migrations(target_engine):
    """Automatically check and apply schema migrations for SQLite."""
    try:
        from sqlalchemy import inspect, text
        inspector = inspect(target_engine)
        if "objects" in inspector.get_table_names():
            columns = [c["name"] for c in inspector.get_columns("objects")]
            with target_engine.begin() as conn:
                if "storage_policy" not in columns:
                    conn.execute(text("ALTER TABLE objects ADD COLUMN storage_policy VARCHAR(30) NOT NULL DEFAULT 'REPLICA_3X'"))
                if "bucket_name" not in columns:
                    conn.execute(text("ALTER TABLE objects ADD COLUMN bucket_name VARCHAR(100) NOT NULL DEFAULT 'default-bucket'"))
                if "merkle_root" not in columns:
                    conn.execute(text("ALTER TABLE objects ADD COLUMN merkle_root VARCHAR(64)"))
    except Exception as e:
        import logging
        logging.getLogger("database").warning(f"Schema migration warning: {e}")

def get_db():
    """FastAPI Dependency for database session."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
