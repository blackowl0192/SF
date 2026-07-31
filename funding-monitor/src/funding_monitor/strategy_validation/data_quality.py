from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from itertools import pairwise

from funding_monitor.instrument_mapping import InstrumentMapping, SpotMappingStatus
from funding_monitor.models import FundingSnapshot, ensure_utc

from .models import DataQualityResult, DataQualityStatus, StrategyValidationConfig


class DataQualityEvaluator:
    def __init__(self, config: StrategyValidationConfig) -> None:
        self.config = config

    def evaluate(
        self,
        snapshots: Sequence[FundingSnapshot],
        mapping: InstrumentMapping | None,
    ) -> DataQualityResult:
        reasons: list[str] = []
        duplicate_count = _duplicate_event_time_count(snapshots)
        maximum_gap_seconds = _maximum_gap_seconds(snapshots)

        if not snapshots:
            return DataQualityResult(
                status=DataQualityStatus.INVALID,
                reasons=("no_snapshots",),
                maximum_gap_seconds=None,
                duplicate_count=0,
            )

        ordered = sorted(snapshots, key=lambda item: ensure_utc(item.event_time))
        if list(snapshots) != ordered:
            reasons.append("snapshots_not_sorted")
        if duplicate_count:
            reasons.append("duplicate_snapshot_timestamps")
        if any(not _rate_is_valid(snapshot.predicted_funding_rate) for snapshot in ordered):
            reasons.append("invalid_predicted_funding_rate")
        if any(snapshot.mark_price <= 0 for snapshot in ordered):
            reasons.append("invalid_mark_price")
        if any(
            snapshot.index_price is not None and snapshot.index_price <= 0
            for snapshot in ordered
        ):
            reasons.append("invalid_index_price")

        funding_time = ensure_utc(ordered[0].next_funding_time)
        if any(ensure_utc(snapshot.next_funding_time) != funding_time for snapshot in ordered):
            reasons.append("inconsistent_next_funding_time")
        if any(ensure_utc(snapshot.event_time) > funding_time for snapshot in ordered):
            reasons.append("snapshots_after_funding_time")

        if mapping is None:
            reasons.append("missing_instrument_mapping")
        elif mapping.spot_mapping_status != SpotMappingStatus.MATCHED:
            reasons.append(f"spot_mapping_{mapping.spot_mapping_status.value}")
        elif not mapping.spot_trading_allowed:
            reasons.append("spot_trading_disabled")
        if (
            maximum_gap_seconds is not None
            and maximum_gap_seconds > self.config.maximum_snapshot_age_seconds
        ):
            reasons.append("large_snapshot_gap")

        if _has_invalid_reason(reasons):
            status = DataQualityStatus.INVALID
        elif (
            maximum_gap_seconds is not None
            and maximum_gap_seconds > self.config.maximum_snapshot_age_seconds * 3
        ):
            status = DataQualityStatus.POOR
        elif reasons or (
            maximum_gap_seconds is not None
            and maximum_gap_seconds > self.config.maximum_snapshot_age_seconds
        ):
            status = DataQualityStatus.PARTIAL
        else:
            status = DataQualityStatus.GOOD

        return DataQualityResult(
            status=status,
            reasons=tuple(dict.fromkeys(reasons)),
            maximum_gap_seconds=maximum_gap_seconds,
            duplicate_count=duplicate_count,
        )


def _duplicate_event_time_count(snapshots: Sequence[FundingSnapshot]) -> int:
    seen: set[object] = set()
    duplicates = 0
    for snapshot in snapshots:
        key = ensure_utc(snapshot.event_time)
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def _maximum_gap_seconds(snapshots: Sequence[FundingSnapshot]) -> int | None:
    ordered = sorted(snapshots, key=lambda item: ensure_utc(item.event_time))
    if len(ordered) < 2:
        return None
    return max(
        max(
            0,
            int(
                (
                    ensure_utc(current.event_time) - ensure_utc(previous.event_time)
                ).total_seconds()
            ),
        )
        for previous, current in pairwise(ordered)
    )


def _rate_is_valid(value: Decimal) -> bool:
    return value.is_finite()


def _has_invalid_reason(reasons: Sequence[str]) -> bool:
    invalid_prefixes = (
        "invalid_",
        "snapshots_after_funding_time",
        "inconsistent_next_funding_time",
    )
    return any(reason.startswith(invalid_prefixes) for reason in reasons)
