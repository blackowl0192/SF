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
