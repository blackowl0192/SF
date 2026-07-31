from __future__ import annotations

from .market_data import HistoricalMarketDataProvider
from .models import (
    StrategyValidationConfig,
    StrategyValidationDataset,
    StrategyValidationSummary,
)
from .parameter_grid import StrategyParameterGrid
from .replay_engine import StrategyReplayEngine
from .reporting import aggregate_results
from .repository import StrategyValidationRepository


class StrategyValidationService:
    def __init__(
        self,
        repository: StrategyValidationRepository,
        *,
        market_data_provider: HistoricalMarketDataProvider | None = None,
    ) -> None:
        self.repository = repository
        self.market_data_provider = market_data_provider

    async def run_validation(
        self,
        *,
        config: StrategyValidationConfig,
        dataset: StrategyValidationDataset,
    ) -> StrategyValidationSummary:
        run_id = await self.repository.create_run(config, dataset)
        try:
            events = await self.repository.fetch_events(
                exchange=config.exchange,
                period_start=dataset.period_start,
                period_end=dataset.period_end,
                symbols=dataset.requested_symbols,
                limit=dataset.limit,
            )
            engine = StrategyReplayEngine(
                config,
                market_data_provider=self.market_data_provider,
            )
            results = [
                await engine.replay_event(event, run_id=run_id) for event in events
            ]
            aggregates = aggregate_results(results, run_id=run_id)
            await self.repository.save_results(results)
            await self.repository.save_aggregates(aggregates)
            successful_events = sum(1 for result in results if result.success)
            failed_events = sum(1 for result in results if not result.eligible)
            await self.repository.complete_run(
                run_id=run_id,
                total_events=len(events),
                processed_events=len(results),
                successful_events=successful_events,
                failed_events=failed_events,
            )
            return StrategyValidationSummary(
                run_id=run_id,
                total_events=len(events),
                processed_events=len(results),
                successful_events=successful_events,
                failed_events=failed_events,
                aggregates=aggregates,
            )
        except Exception as exc:
            await self.repository.fail_run(run_id=run_id, error_message=str(exc))
            raise

    async def run_grid(
        self,
        *,
        base_config: StrategyValidationConfig,
        grid: StrategyParameterGrid,
        dataset: StrategyValidationDataset,
    ) -> tuple[StrategyValidationSummary, ...]:
        summaries: list[StrategyValidationSummary] = []
        for config in grid.iter_configs(base_config):
            summaries.append(
                await self.run_validation(config=config, dataset=dataset)
            )
        return tuple(summaries)
