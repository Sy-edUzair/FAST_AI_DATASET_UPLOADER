import os
import logging
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy.engine import make_url
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    Text,
    Table,
    create_engine,
    func,
    text,
)

load_dotenv()
logger = logging.getLogger(__name__)


metadata = MetaData()

_mongo_client = None
engine = None


def _get_postgres_url():
    return os.environ["POSTGRES_URL"]


def _get_database_name():
    return make_url(_get_postgres_url()).database


def _build_admin_url():
    url = make_url(_get_postgres_url())
    return url.set(database="postgres")


def _ensure_database_exists():
    database_name = _get_database_name()
    if not database_name:
        raise RuntimeError("POSTGRES_URL must include a database name")

    admin_engine = create_engine(_build_admin_url(), isolation_level="AUTOCOMMIT")
    quoted_name = database_name.replace('"', '""')
    with admin_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": database_name},
        ).scalar()
        if not exists:
            conn.exec_driver_sql(f'CREATE DATABASE "{quoted_name}"')
            logger.info("Created missing PostgreSQL database %s", database_name)
    admin_engine.dispose()


def _get_engine():
    global engine
    if engine is None:
        engine = create_engine(_get_postgres_url(), pool_pre_ping=True)
    return engine


datasets = Table(
    "datasets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("file_type", Text, nullable=False),
    Column("size_mb", Float, nullable=False),
    Column("cloud_storage_url", Text),
    Column("status", Text, nullable=False, server_default=text("'submitted'")),
    Column("priority_score", Integer, nullable=False, server_default=text("1")),
    Column("priority_level", Text, nullable=False, server_default=text("'Low'")),
    Column("is_active", Boolean, nullable=False, server_default=text("TRUE")),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column(
        "updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
)


def setup_postgres():
    _ensure_database_exists()
    current_engine = _get_engine()
    metadata.create_all(current_engine)
    logger.info("PostgreSQL table ready")


def get_pg_engine():
    return _get_engine()


def get_mongo_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(os.environ["MONGODB_URL"])
    return _mongo_client["ai_datasets"]
