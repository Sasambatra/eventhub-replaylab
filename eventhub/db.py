import uuid
from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


def now():
    return datetime.now(UTC)


def uid():
    return uuid.uuid4()


class Base(DeclarativeBase):
    pass


class Database:
    def __init__(self, config):
        self.config = config
        self.engine = create_async_engine(
            config.database_url(), pool_pre_ping=True, pool_size=12, max_overflow=4, hide_parameters=True
        )
        self.session = async_sessionmaker(self.engine, expire_on_commit=False)

    async def close(self):
        await self.engine.dispose()


def migration_engine(config, database=None):
    return create_engine(config.database_url(migration=True, database=database), pool_pre_ping=True)
