from __future__ import annotations

import logging
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self
from urllib.parse import parse_qs, urlsplit

import asyncpg

from .config import PROJECT_ROOT, Settings

logger = logging.getLogger(__name__)

DEFAULT_MIGRATIONS_PATH = PROJECT_ROOT / "migrations"

CREATE_MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@dataclass(frozen=True)
class DatabaseCheckResult:
    connected: bool
    postgres_version: str
    database_utc_time: datetime
    applied_migrations: int


class DatabaseConnectionError(RuntimeError):
    pass


class PostgresDatabase:
    def __init__(
        self,
        *,
        database_url: str,
        min_size: int = 1,
        max_size: int = 10,
        command_timeout_seconds: float = 30,
        migrations_path: Path = DEFAULT_MIGRATIONS_PATH,
    ) -> None:
        self.database_url = database_url
        self.min_size = min_size
        self.max_size = max_size
        self.command_timeout_seconds = command_timeout_seconds
        self.migrations_path = migrations_path
        self._pool: Any | None = None

    @classmethod
    def from_settings(cls, settings: Settings) -> PostgresDatabase:
        return cls(
            database_url=settings.database_url,
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
            command_timeout_seconds=settings.database_command_timeout_seconds,
        )

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def open(self) -> None:
        if self._pool is not None:
            return
        try:
            self._pool = await asyncpg.create_pool(
                dsn=self.database_url,
                min_size=self.min_size,
                max_size=self.max_size,
                command_timeout=self.command_timeout_seconds,
                ssl=_ssl_from_database_url(self.database_url),
            )
        except (OSError, asyncpg.PostgresError) as exc:
            raise DatabaseConnectionError(
                "Could not connect to PostgreSQL. Verify DATABASE_URL, SSL "
                "settings, network access, and Supabase pooler availability."
            ) from exc

    async def close(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        if self._pool is None:
            raise RuntimeError("PostgreSQL pool is not open")
        async with self._pool.acquire() as connection:
            yield connection

    async def check_connection(self) -> DatabaseCheckResult:
        async with self.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT version() AS version, NOW() AT TIME ZONE 'UTC' AS utc_now"
            )
            applied_migrations = await applied_migrations_count(connection)
        utc_time = _attach_utc(row["utc_now"])
        return DatabaseCheckResult(
            connected=True,
            postgres_version=row["version"],
            database_utc_time=utc_time,
            applied_migrations=applied_migrations,
        )

    async def migrate(self) -> list[str]:
        async with self.acquire() as connection:
            return await run_migrations(connection, self.migrations_path)


async def run_migrations(connection: Any, migrations_path: Path) -> list[str]:
    await connection.execute(CREATE_MIGRATIONS_TABLE_SQL)
    applied = {
        row["version"]
        for row in await connection.fetch(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
    }
    applied_now: list[str] = []

    for migration_file in sorted(migrations_path.glob("*.sql")):
        version = migration_file.name
        if version in applied:
            continue
        sql = migration_file.read_text(encoding="utf-8")
        async with connection.transaction():
            await connection.execute(sql)
            await connection.execute(
                "INSERT INTO schema_migrations(version) VALUES ($1)",
                version,
            )
        applied_now.append(version)
        logger.info("applied migration %s", version)

    return applied_now


async def applied_migrations_count(connection: Any) -> int:
    try:
        count = await connection.fetchval("SELECT COUNT(*) FROM schema_migrations")
    except asyncpg.UndefinedTableError:
        return 0
    return int(count or 0)


def _ssl_from_database_url(database_url: str) -> Any | None:
    query = parse_qs(urlsplit(database_url).query)
    sslmode = query.get("sslmode", ["require"])[0].lower()
    if sslmode == "disable":
        return None
    if sslmode in {"verify-ca", "verify-full"}:
        return ssl.create_default_context()
    return "require"


def _attach_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
