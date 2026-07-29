from decimal import Decimal

import pytest
from pydantic import ValidationError

from funding_monitor.config import Settings


def test_database_url_is_required_for_runtime_config(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_analytics_defaults_use_decimal_and_observation_mode(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://example.invalid/postgres")

    settings = Settings(_env_file=None)

    assert settings.abs_min_funding_rate == Decimal("0.0003")
    assert settings.default_funding_interval_hours == 8
    assert settings.analytics_observation_only is True
    assert settings.window_cache_minutes == 120
    assert settings.default_metrics_window == 60
    assert settings.binance_spot_base_url == "https://api.binance.com"
    assert settings.supported_spot_quote_asset == "USDT"
    assert settings.instrument_mapping_sync_on_startup is False
