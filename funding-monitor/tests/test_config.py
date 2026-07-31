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
    assert settings.candidate_engine_enabled is True
    assert settings.candidate_min_funding_rate == Decimal("0.0003")
    assert settings.candidate_min_history_minutes == 15
    assert settings.candidate_primary_window_minutes == 30
    assert settings.candidate_short_window_minutes == 5
    assert settings.candidate_long_window_minutes == 60
    assert settings.candidate_min_snapshot_count == 10
    assert settings.candidate_max_snapshot_age_seconds == 120
    assert settings.candidate_min_persistence_ratio == Decimal("0.70")
    assert settings.candidate_max_std_dev == Decimal("0.0002")
    assert settings.candidate_max_threshold_crossings == 4
    assert settings.candidate_max_direction_changes == 8
    assert settings.candidate_late_spike_lookback_minutes == 5
    assert settings.candidate_late_spike_min_jump_ratio == Decimal("1.50")
    assert settings.candidate_deterioration_lookback_minutes == 5
    assert settings.candidate_max_negative_velocity == Decimal("-0.00002")
    assert settings.candidate_min_minutes_to_funding == 5
    assert settings.candidate_max_minutes_to_funding == 480
    assert settings.candidate_strong_score == Decimal(80)
    assert settings.candidate_min_score == Decimal(60)
    assert settings.candidate_persist_interval_seconds == 60
    assert settings.candidate_max_results == 50
    assert settings.funding_interval_point_tolerance_seconds == 90
    assert settings.funding_interval_summary_batch_size == 500
