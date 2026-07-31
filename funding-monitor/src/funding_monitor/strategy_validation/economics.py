from __future__ import annotations

from decimal import Decimal

from .models import (
    EconomicResult,
    HedgeSizingResult,
    MarketPrice,
    MarketPriceSet,
    SizingMode,
    StrategyValidationConfig,
    ValidationMode,
)

ZERO = Decimal(0)


class HedgeSizingCalculator:
    def calculate(
        self,
        *,
        notional: Decimal,
        entry_price: MarketPrice | None,
    ) -> HedgeSizingResult:
        if (
            entry_price is None
            or entry_price.spot_price is None
            or entry_price.futures_price is None
        ):
            return HedgeSizingResult(
                sizing_mode=SizingMode.IDEALIZED_SIZING,
                requested_notional=notional,
                spot_quantity=None,
                futures_quantity=None,
                spot_entry_notional=None,
                futures_entry_notional=None,
                initial_hedge_mismatch_rate=None,
            )

        spot_quantity = notional / entry_price.spot_price
        futures_quantity = notional / entry_price.futures_price
        spot_entry_notional = spot_quantity * entry_price.spot_price
        futures_entry_notional = futures_quantity * entry_price.futures_price
        return HedgeSizingResult(
            sizing_mode=SizingMode.IDEALIZED_SIZING,
            requested_notional=notional,
            spot_quantity=spot_quantity,
            futures_quantity=futures_quantity,
            spot_entry_notional=spot_entry_notional,
            futures_entry_notional=futures_entry_notional,
            initial_hedge_mismatch_rate=(spot_entry_notional - futures_entry_notional)
            / notional,
        )


class EconomicCalculator:
    def __init__(
        self,
        sizing_calculator: HedgeSizingCalculator | None = None,
    ) -> None:
        self.sizing_calculator = sizing_calculator or HedgeSizingCalculator()

    def evaluate(
        self,
        *,
        config: StrategyValidationConfig,
        realized_funding_rate: Decimal | None,
        market_prices: MarketPriceSet,
    ) -> EconomicResult:
        gross_funding_pnl = (
            config.position_notional * realized_funding_rate
            if realized_funding_rate is not None
            else None
        )
        gross_return_rate = realized_funding_rate
        hedge_sizing = self.sizing_calculator.calculate(
            notional=config.position_notional,
            entry_price=market_prices.entry,
        )

        if config.validation_mode == ValidationMode.FUNDING_ONLY or not market_prices.complete:
            return EconomicResult(
                validation_mode=config.validation_mode,
                market_data_complete=market_prices.complete,
                missing_data_reasons=market_prices.missing_reasons(),
                position_notional=config.position_notional,
                gross_funding_pnl=gross_funding_pnl,
                spot_price_pnl=None,
                futures_price_pnl=None,
                basis_pnl=None,
                spot_fees=None,
                futures_fees=None,
                slippage_cost=None,
                additional_cost=None,
                net_pnl=None,
                gross_return_rate=gross_return_rate,
                net_return_rate=None,
                hedge_sizing=hedge_sizing,
            )

        entry = market_prices.entry
        exit_ = market_prices.exit
        if entry is None or exit_ is None:
            raise RuntimeError("complete market price set is inconsistent")
        if (
            entry.spot_price is None
            or entry.futures_price is None
            or exit_.spot_price is None
            or exit_.futures_price is None
        ):
            raise RuntimeError("complete market price set has missing prices")

        spot_quantity = hedge_sizing.spot_quantity
        futures_quantity = hedge_sizing.futures_quantity
        if spot_quantity is None or futures_quantity is None:
            raise RuntimeError("complete market price set has no hedge sizing")

        spot_price_pnl = (exit_.spot_price - entry.spot_price) * spot_quantity
        futures_price_pnl = (entry.futures_price - exit_.futures_price) * futures_quantity
        basis_pnl = spot_price_pnl + futures_price_pnl
        spot_fees = (
            config.position_notional * config.spot_entry_fee_rate
            + config.position_notional * config.spot_exit_fee_rate
        )
        futures_fees = (
            config.position_notional * config.futures_entry_fee_rate
            + config.position_notional * config.futures_exit_fee_rate
        )
        slippage_cost = config.position_notional * (
            config.spot_slippage_rate + config.futures_slippage_rate
        )
        additional_cost = config.position_notional * config.additional_cost_rate
        net_pnl = (
            (gross_funding_pnl or ZERO)
            + basis_pnl
            - spot_fees
            - futures_fees
            - slippage_cost
            - additional_cost
        )
        return EconomicResult(
            validation_mode=config.validation_mode,
            market_data_complete=True,
            missing_data_reasons=(),
            position_notional=config.position_notional,
            gross_funding_pnl=gross_funding_pnl,
            spot_price_pnl=spot_price_pnl,
            futures_price_pnl=futures_price_pnl,
            basis_pnl=basis_pnl,
            spot_fees=spot_fees,
            futures_fees=futures_fees,
            slippage_cost=slippage_cost,
            additional_cost=additional_cost,
            net_pnl=net_pnl,
            gross_return_rate=gross_return_rate,
            net_return_rate=net_pnl / config.position_notional,
            hedge_sizing=hedge_sizing,
        )
