from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.config import Settings
from funding_monitor.diagnostics import (
    CoverageMetrics,
    PipelineDiagnostics,
    WindowSnapshotMetrics,
    _diagnostic_warnings,
    diagnostics_to_dict,
)

NOW = datetime(2024, 1, 1, 12, tzinfo=UTC)


def test_diagnostic_warnings_are_empty_for_healthy_pipeline(monkeypatch) -> None:
    settings = settings_from_env(monkeypatch)
    coverage = CoverageMetrics(
        expected_symbols=100,
        observed_symbols=95,
        missing_symbols=5,
        coverage_ratio=Decimal("0.95"),
        snapshot_gap_seconds=20,
        maximum_gap_seconds=60,
        median_gap_seconds=Decimal(60),
    )

    warnings = _diagnostic_warnings(
        latest_snapshot_age_seconds=20,
        coverage=coverage,
        latest_candidate_evaluation=NOW - timedelta(seconds=30),
        overdue_confirmations=0,
        interval_summary_backlog=0,
        settings=settings,
        now=NOW,
    )

    assert warnings == ()


def test_diagnostic_warnings_detect_stale_low_coverage_and_backlogs(
    monkeypatch,
) -> None:
    settings = settings_from_env(monkeypatch)
    coverage = CoverageMetrics(
        expected_symbols=100,
        observed_symbols=10,
        missing_symbols=90,
        coverage_ratio=Decimal("0.10"),
        snapshot_gap_seconds=600,
        maximum_gap_seconds=None,
        median_gap_seconds=None,
    )

    warnings = _diagnostic_warnings(
        latest_snapshot_age_seconds=600,
        coverage=coverage,
        latest_candidate_evaluation=None,
        overdue_confirmations=3,
        interval_summary_backlog=2,
        settings=settings,
        now=NOW,
    )

    assert "SNAPSHOT_COLLECTION_STALE" in warnings
    assert "LOW_SYMBOL_COVERAGE" in warnings
    assert "CONFIRMATION_BACKLOG" in warnings
    assert "CANDIDATE_PIPELINE_NOT_RUNNING" in warnings
    assert "INTERVAL_SUMMARY_BACKLOG" in warnings


def test_diagnostics_json_serialization_uses_safe_strings() -> None:
    diagnostics = PipelineDiagnostics(
        snapshots_total=10,
        latest_snapshot_at=NOW,
        snapshot_age_seconds=1,
        expected_active_symbols=2,
        windows=(
            WindowSnapshotMetrics(
                minutes=1,
                snapshots=2,
                unique_symbols=2,
                coverage_ratio=Decimal("1.000000"),
            ),
        ),
        coverage=CoverageMetrics(
            expected_symbols=2,
            observed_symbols=2,
            missing_symbols=0,
            coverage_ratio=Decimal("1.000000"),
            snapshot_gap_seconds=1,
            maximum_gap_seconds=60,
            median_gap_seconds=Decimal(60),
        ),
        events_by_status={"confirmed": 1},
        future_confirmations=0,
        pending_confirmations=0,
        failed_confirmations=0,
        overdue_confirmations=0,
        invalid_events=0,
        latest_confirmed_funding_event=NOW,
        latest_candidate_evaluation=NOW,
        latest_funding_interval_summary=NOW,
        candidate_evaluations_last_hour=2,
        interval_summaries_last_24h=1,
        interval_summary_backlog=0,
        warnings=(),
        critical=False,
    )

    payload = diagnostics_to_dict(diagnostics)

    assert payload["latest_snapshot_at"] == "2024-01-01T12:00:00+00:00"
    assert payload["coverage"]["coverage_ratio"] == "1.000000"


def settings_from_env(monkeypatch) -> Settings:
    monkeypatch.setenv("DATABASE_URL", "postgresql://example.invalid/postgres")
    return Settings(_env_file=None)
