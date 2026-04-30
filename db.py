import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from typing import AsyncGenerator

logger = logging.getLogger("lumen.db")

# Read MySQL URL from env (Supabase provides a MySQL connection string)
DATABASE_URL = os.environ.get("MYSQL_URL") or os.environ.get("SUPABASE_MYSQL_URL") or ""
USE_SQL = bool(DATABASE_URL)

if USE_SQL:
    # expect asyncmy driver: mysql+asyncmy://user:pass@host:3306/dbname
    engine = create_async_engine(DATABASE_URL, future=True, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
else:
    engine = None
    async_session = None


async def get_session() -> AsyncGenerator:
    if not USE_SQL:
        raise RuntimeError("No DATABASE_URL set; SQL disabled")
    async with async_session() as session:
        yield session


async def init_db():
    """Create tables using models.Base metadata. Requires models to be importable."""
    if not USE_SQL:
        raise RuntimeError("No DATABASE_URL set; skipping init")
    # import models here to ensure Base is available
    from .models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("SQL schema created/verified")
