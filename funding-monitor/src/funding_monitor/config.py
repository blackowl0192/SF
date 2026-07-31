from decimal import Decimal
from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    database_url: str = Field(min_length=1)
    database_pool_min_size: int = 1
    database_pool_max_size: int = 10
    database_command_timeout_seconds: float = 30
    abs_min_funding_rate: Decimal = Decimal("0.0003")
    default_funding_interval_hours: int = 8
    analytics_observation_only: bool = True
    window_cache_minutes: int = 120
    default_metrics_window: int = 60
    binance_spot_base_url: str = "https://api.binance.com"
    supported_spot_quote_asset: str = "USDT"
    instrument_mapping_sync_on_startup: bool = False
    candidate_engine_enabled: bool = True
    candidate_min_funding_rate: Decimal = Decimal("0.0003")
    candidate_min_history_minutes: int = 15
    candidate_primary_window_minutes: int = 30
    candidate_short_window_minutes: int = 5
    candidate_long_window_minutes: int = 60
    candidate_min_snapshot_count: int = 10
    candidate_max_snapshot_age_seconds: int = 120
    candidate_min_persistence_ratio: Decimal = Decimal("0.70")
    candidate_max_std_dev: Decimal = Decimal("0.0002")
    candidate_max_threshold_crossings: int = 4
    candidate_max_direction_changes: int = 8
    candidate_late_spike_lookback_minutes: int = 5
    candidate_late_spike_min_jump_ratio: Decimal = Decimal("1.50")
    candidate_deterioration_lookback_minutes: int = 5
    candidate_max_negative_velocity: Decimal = Decimal("-0.00002")
    candidate_min_minutes_to_funding: int = 5
    candidate_max_minutes_to_funding: int = 480
    candidate_strong_score: Decimal = Decimal(80)
    candidate_min_score: Decimal = Decimal(60)
    candidate_persist_interval_seconds: int = 60
    candidate_max_results: int = 50
    funding_interval_point_tolerance_seconds: int = 90
    funding_interval_summary_batch_size: int = 500
    snapshot_persist_interval_seconds: int = Field(default=60, gt=0)
    snapshot_batch_size: int = Field(default=500, gt=0)
    snapshot_flush_interval_seconds: int = Field(default=5, gt=0)
    collector_health_window_minutes: int = Field(default=5, gt=0)
    collector_health_max_snapshot_age_seconds: int = Field(default=180, gt=0)
    collector_health_min_coverage_ratio: Decimal = Field(
        default=Decimal("0.80"),
        ge=Decimal(0),
        le=Decimal(1),
    )
    candidate_evaluation_interval_seconds: int = Field(default=60, gt=0)
    interval_summary_build_interval_seconds: int = Field(default=300, gt=0)
    confirmation_backfill_interval_seconds: int = Field(default=300, gt=0)
    confirmation_backfill_batch_size: int = Field(default=100, gt=0)
    confirmation_overdue_grace_minutes: int = Field(default=30, ge=0)
    log_level: str = "INFO"
    normal_snapshot_interval_seconds: int = 60
    funding_window_before_seconds: int = 600
    funding_window_after_seconds: int = 300
    detailed_snapshot_interval_seconds: int = 1
    confirmation_initial_delay_seconds: int = 10
    confirmation_retry_seconds: int = 10
    confirmation_max_attempts: int = 6
    rest_timeout_seconds: float = 10
    ws_max_reconnect_delay_seconds: int = 60

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def load_settings() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        raise RuntimeError(
            "DATABASE_URL is required. Create .env from .env.example and set a "
            "Supabase PostgreSQL connection string."
        ) from exc
