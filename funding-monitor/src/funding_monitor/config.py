from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_path: Path = Path("data/funding_monitor.db")
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
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
