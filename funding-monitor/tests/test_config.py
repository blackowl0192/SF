import pytest
from pydantic import ValidationError

from funding_monitor.config import Settings


def test_database_url_is_required_for_runtime_config(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
