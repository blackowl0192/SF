from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from itertools import product

from .models import EntryMode, StrategyValidationConfig


class ParameterGridTooLargeError(ValueError):
    pass


class StrategyParameterGrid:
    def __init__(
        self,
        *,
        funding_thresholds: tuple[Decimal, ...] = (Decimal("0.0003"),),
        entry_modes: tuple[EntryMode, ...] = (EntryMode.FIXED_TIME,),
        entry_minutes_before_funding: tuple[int, ...] = (60,),
        signal_confirmation_minutes: tuple[int, ...] = (5,),
        minimum_persistence_ratios: tuple[Decimal, ...] = (Decimal("0.70"),),
        maximum_funding_stds: tuple[Decimal, ...] = (Decimal("0.0002"),),
        exit_minutes_after_funding: tuple[int, ...] = (0,),
        max_combinations: int = 100,
    ) -> None:
        self.funding_thresholds = funding_thresholds
        self.entry_modes = entry_modes
        self.entry_minutes_before_funding = entry_minutes_before_funding
        self.signal_confirmation_minutes = signal_confirmation_minutes
        self.minimum_persistence_ratios = minimum_persistence_ratios
        self.maximum_funding_stds = maximum_funding_stds
        self.exit_minutes_after_funding = exit_minutes_after_funding
        self.max_combinations = max_combinations

    def count(self) -> int:
        total = 1
        for values in (
            self.funding_thresholds,
            self.entry_modes,
            self.entry_minutes_before_funding,
            self.signal_confirmation_minutes,
            self.minimum_persistence_ratios,
            self.maximum_funding_stds,
            self.exit_minutes_after_funding,
        ):
            total *= len(values)
        return total

    def iter_configs(
        self,
        base_config: StrategyValidationConfig,
    ) -> tuple[StrategyValidationConfig, ...]:
        count = self.count()
        if count > self.max_combinations:
            raise ParameterGridTooLargeError(
                f"parameter grid has {count} combinations; max is {self.max_combinations}"
            )

        configs: list[StrategyValidationConfig] = []
        for (
            threshold,
            entry_mode,
            entry_minutes,
            confirmation_minutes,
            persistence_ratio,
            funding_std,
            exit_minutes,
        ) in product(
            self.funding_thresholds,
            self.entry_modes,
            self.entry_minutes_before_funding,
            self.signal_confirmation_minutes,
            self.minimum_persistence_ratios,
            self.maximum_funding_stds,
            self.exit_minutes_after_funding,
        ):
            configs.append(
                replace(
                    base_config,
                    funding_threshold=threshold,
                    entry_mode=entry_mode,
                    entry_minutes_before_funding=entry_minutes,
                    signal_confirmation_minutes=confirmation_minutes,
                    minimum_persistence_ratio=persistence_ratio,
                    maximum_funding_std=funding_std,
                    exit_minutes_after_funding=exit_minutes,
                )
            )
        return tuple(configs)
