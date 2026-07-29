import asyncio
from pathlib import Path

import pytest

import funding_monitor.database as database_module
from funding_monitor.database import PostgresDatabase, run_migrations


def test_pool_is_created_once_and_closed(monkeypatch) -> None:
    calls = []
    pool = FakePool(FakeConnection())

    async def fake_create_pool(**kwargs):
        calls.append(kwargs)
        return pool

    monkeypatch.setattr(database_module.asyncpg, "create_pool", fake_create_pool)

    async def scenario() -> None:
        database = PostgresDatabase(
            database_url="postgresql://example.invalid/postgres?sslmode=disable"
        )
        await database.open()
        await database.open()
        await database.close()

    asyncio.run(scenario())

    assert len(calls) == 1
    assert pool.closed


def test_migrations_are_sorted_by_filename(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("SELECT 'second';", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("SELECT 'first';", encoding="utf-8")
    connection = FakeConnection()

    applied = asyncio.run(run_migrations(connection, tmp_path))

    assert applied == ["001_first.sql", "002_second.sql"]
    assert connection.migration_sql == ["SELECT 'first';", "SELECT 'second';"]


def test_applied_migration_is_not_run_again(tmp_path: Path) -> None:
    (tmp_path / "001_first.sql").write_text("SELECT 'first';", encoding="utf-8")
    (tmp_path / "002_second.sql").write_text("SELECT 'second';", encoding="utf-8")
    connection = FakeConnection(applied={"001_first.sql"})

    applied = asyncio.run(run_migrations(connection, tmp_path))

    assert applied == ["002_second.sql"]
    assert connection.migration_sql == ["SELECT 'second';"]


def test_failed_migration_does_not_record_version(tmp_path: Path) -> None:
    (tmp_path / "001_bad.sql").write_text("SELECT 'bad';", encoding="utf-8")
    connection = FakeConnection(fail_on="bad")

    with pytest.raises(RuntimeError):
        asyncio.run(run_migrations(connection, tmp_path))

    assert connection.inserted_versions == []


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, applied=None, fail_on: str | None = None) -> None:
        self.applied = set(applied or set())
        self.fail_on = fail_on
        self.migration_sql: list[str] = []
        self.inserted_versions: list[str] = []

    async def execute(self, sql, *args):
        if sql.lstrip().startswith("CREATE TABLE IF NOT EXISTS schema_migrations"):
            return "CREATE TABLE"
        if sql.startswith("INSERT INTO schema_migrations"):
            self.inserted_versions.append(args[0])
            self.applied.add(args[0])
            return "INSERT 0 1"
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("migration failed")
        self.migration_sql.append(sql.strip())
        return "OK"

    async def fetch(self, _sql, *args):
        return [{"version": version} for version in sorted(self.applied)]

    async def fetchval(self, _sql, *args):
        return len(self.applied)

    async def fetchrow(self, _sql, *args):
        return None

    def transaction(self):
        return FakeTransaction()


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        return False


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.closed = False

    def acquire(self):
        return FakeAcquire(self.connection)

    async def close(self):
        self.closed = True
