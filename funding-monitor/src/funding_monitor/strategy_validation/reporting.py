from __future__ import annotations

import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from funding_monitor.models import datetime_to_text

from .models import StrategyValidationAggregate, StrategyValidationResult

ZERO = Decimal(0)


def aggregate_results(
    results: list[StrategyValidationResult],
    *,
    run_id: int,
) -> tuple[StrategyValidationAggregate, ...]:
    groups: dict[tuple[str, str], list[StrategyValidationResult]] = defaultdict(list)
    for result in results:
        groups[("overall", "all")].append(result)
        groups[("symbol", result.symbol)].append(result)
        groups[
            (
                "funding_threshold",
                str(result.metadata.get("funding_threshold", "")),
            )
        ].append(result)
        groups[("entry_mode", str(result.metadata.get("entry_mode", "")))].append(
            result
        )
        groups[
            (
                "entry_timing",
                str(result.metadata.get("entry_minutes_before_funding_config", "")),
            )
        ].append(result)
        groups[
            (
                "funding_interval",
                str(result.metadata.get("funding_interval_hours", "")),
            )
        ].append(result)
        groups[
            (
                "candidate_status",
                str(result.metadata.get("candidate_status") or "unknown"),
            )
        ].append(result)
        groups[("validation_mode", result.validation_mode.value)].append(result)
        groups[
            (
                "calendar_month",
                result.funding_time.strftime("%Y-%m"),
            )
        ].append(result)

    return tuple(
        StrategyValidationAggregate(
            run_id=run_id,
            grouping_type=grouping_type,
            grouping_key=grouping_key,
            metrics=_metrics(rows),
        )
        for (grouping_type, grouping_key), rows in sorted(groups.items())
    )


def format_validation_summary(
    *,
    run_id: int | None,
    total_events: int,
    processed_events: int,
    successful_events: int,
    failed_events: int,
    aggregates: tuple[StrategyValidationAggregate, ...],
) -> str:
    overall = next(
        (
            aggregate
            for aggregate in aggregates
            if aggregate.grouping_type == "overall" and aggregate.grouping_key == "all"
        ),
        None,
    )
    lines = [
        f"run_id: {run_id or ''}",
        f"total_events: {total_events}",
        f"processed_events: {processed_events}",
        f"successful_events: {successful_events}",
        f"failed_events: {failed_events}",
    ]
    if overall is not None:
        for key in (
            "signal_detected_count",
            "eligible_count",
            "funding_only_count",
            "insufficient_market_data_count",
            "gross_funding_pnl",
            "gross_return_rate_mean",
            "success_rate",
        ):
            lines.append(f"{key}: {overall.metrics.get(key, '')}")
    return "\n".join(lines)


def format_validation_report(
    results: list[StrategyValidationResult],
    aggregates: tuple[StrategyValidationAggregate, ...],
) -> str:
    lines = ["grouping_type grouping_key events signals success_rate gross_return_rate_mean"]
    for aggregate in aggregates:
        metrics = aggregate.metrics
        lines.append(
            " ".join(
                [
                    aggregate.grouping_type,
                    aggregate.grouping_key or "-",
                    str(metrics.get("event_count", 0)),
                    str(metrics.get("signal_detected_count", 0)),
                    str(metrics.get("success_rate", "")),
                    str(metrics.get("gross_return_rate_mean", "")),
                ]
            )
        )
    if results:
        lines.extend(["", "latest_results:"])
        lines.append(
            "symbol funding_time outcome signal realized_funding gross_funding_pnl rejection"
        )
        for result in sorted(results, key=lambda item: item.funding_time, reverse=True)[
            :10
        ]:
            lines.append(
                " ".join(
                    [
                        result.symbol,
                        datetime_to_text(result.funding_time),
                        result.outcome_status.value,
                        str(result.signal_detected).lower(),
                        _decimal_text(result.realized_funding_rate),
                        _decimal_text(result.gross_funding_pnl),
                        result.rejection_reason.value
                        if result.rejection_reason is not None
                        else "-",
                    ]
                )
            )
    return "\n".join(lines)


def export_results_csv(
    results: list[StrategyValidationResult],
    output_path: Path,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "exchange",
        "symbol",
        "spot_symbol",
        "funding_time",
        "outcome_status",
        "signal_detected",
        "entry_time",
        "predicted_funding_at_entry",
        "realized_funding_rate",
        "gross_funding_pnl",
        "gross_return_rate",
        "net_pnl",
        "net_return_rate",
        "rejection_reason",
        "data_quality_status",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "run_id": result.run_id,
                    "exchange": result.exchange,
                    "symbol": result.symbol,
                    "spot_symbol": result.spot_symbol,
                    "funding_time": datetime_to_text(result.funding_time),
                    "outcome_status": result.outcome_status.value,
                    "signal_detected": result.signal_detected,
                    "entry_time": datetime_to_text(result.entry_time)
                    if result.entry_time is not None
                    else "",
                    "predicted_funding_at_entry": _decimal_text(
                        result.predicted_funding_at_entry
                    ),
                    "realized_funding_rate": _decimal_text(result.realized_funding_rate),
                    "gross_funding_pnl": _decimal_text(result.gross_funding_pnl),
                    "gross_return_rate": _decimal_text(result.gross_return_rate),
                    "net_pnl": _decimal_text(result.net_pnl),
                    "net_return_rate": _decimal_text(result.net_return_rate),
                    "rejection_reason": result.rejection_reason.value
                    if result.rejection_reason is not None
                    else "",
                    "data_quality_status": result.data_quality_status.value,
                }
            )
    return len(results)


def _metrics(results: list[StrategyValidationResult]) -> dict[str, object]:
    total = len(results)
    signal_count = sum(1 for result in results if result.signal_detected)
    eligible_count = sum(1 for result in results if result.eligible)
    success_count = sum(1 for result in results if result.success)
    gross_rates = [
        result.gross_return_rate
        for result in results
        if result.gross_return_rate is not None and result.eligible
    ]
    net_rates = [
        result.net_return_rate
        for result in results
        if result.net_return_rate is not None and result.eligible
    ]
    gross_pnl = sum(
        ((result.gross_funding_pnl or ZERO) for result in results if result.eligible),
        ZERO,
    )
    metrics: dict[str, object] = {
        "event_count": total,
        "signal_detected_count": signal_count,
        "eligible_count": eligible_count,
        "rejected_count": total - eligible_count,
        "success_count": success_count,
        "success_rate": _ratio(success_count, eligible_count),
        "signal_rate": _ratio(signal_count, total),
        "funding_only_count": sum(
            1 for result in results if result.outcome_status.value == "funding_only"
        ),
        "full_economic_count": sum(
            1 for result in results if result.outcome_status.value == "full_economic"
        ),
        "insufficient_market_data_count": sum(
            1
            for result in results
            if result.outcome_status.value == "insufficient_market_data"
        ),
        "gross_funding_pnl": _decimal_text(gross_pnl),
        "gross_return_rate_mean": _decimal_text(_mean(gross_rates)),
        "gross_return_rate_median": _decimal_text(_median(gross_rates)),
        "net_return_rate_mean": _decimal_text(_mean(net_rates)),
        "net_return_rate_median": _decimal_text(_median(net_rates)),
    }
    rejection_counts: dict[str, int] = defaultdict(int)
    for result in results:
        if result.rejection_reason is not None:
            rejection_counts[result.rejection_reason.value] += 1
    metrics["rejection_counts"] = dict(sorted(rejection_counts.items()))
    return metrics


def _ratio(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return ""
    return format(Decimal(numerator) / Decimal(denominator), "f")


def _mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def _median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _decimal_text(value: Decimal | None) -> str:
    return format(value, "f") if value is not None else ""
