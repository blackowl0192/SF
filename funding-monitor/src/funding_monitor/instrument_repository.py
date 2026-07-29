from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import asyncpg

from .database import PostgresDatabase
from .instrument_mapping import (
    InstrumentMapping,
    InstrumentMappingSummary,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from .models import ensure_utc

UPSERT_INSTRUMENT_MAPPING_SQL = """
INSERT INTO instrument_mappings (
    futures_symbol,
    futures_pair,
    futures_base_asset,
    futures_quote_asset,
    futures_margin_asset,
    futures_contract_type,
    futures_status,
    spot_symbol,
    spot_base_asset,
    spot_quote_asset,
    spot_status,
    spot_trading_allowed,
    spot_pair_exists,
    spot_mapping_status,
    mapping_reason,
    positive_strategy_available,
    negative_strategy_available,
    negative_strategy_status,
    mapping_source,
    mapping_updated_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
)
ON CONFLICT(futures_symbol) DO UPDATE SET
    futures_pair = excluded.futures_pair,
    futures_base_asset = excluded.futures_base_asset,
    futures_quote_asset = excluded.futures_quote_asset,
    futures_margin_asset = excluded.futures_margin_asset,
    futures_contract_type = excluded.futures_contract_type,
    futures_status = excluded.futures_status,
    spot_symbol = excluded.spot_symbol,
    spot_base_asset = excluded.spot_base_asset,
    spot_quote_asset = excluded.spot_quote_asset,
    spot_status = excluded.spot_status,
    spot_trading_allowed = excluded.spot_trading_allowed,
    spot_pair_exists = excluded.spot_pair_exists,
    spot_mapping_status = excluded.spot_mapping_status,
    mapping_reason = excluded.mapping_reason,
    positive_strategy_available = excluded.positive_strategy_available,
    negative_strategy_available = excluded.negative_strategy_available,
    negative_strategy_status = excluded.negative_strategy_status,
    mapping_source = excluded.mapping_source,
    mapping_updated_at = excluded.mapping_updated_at,
    created_at = instrument_mappings.created_at,
    updated_at = excluded.updated_at
"""

SELECT_INSTRUMENT_MAPPING_COLUMNS = """
SELECT
    futures_symbol,
    futures_pair,
    futures_base_asset,
    futures_quote_asset,
    futures_margin_asset,
    futures_contract_type,
    futures_status,
    spot_symbol,
    spot_base_asset,
    spot_quote_asset,
    spot_status,
    spot_trading_allowed,
    spot_pair_exists,
    spot_mapping_status,
    mapping_reason,
    positive_strategy_available,
    negative_strategy_available,
    negative_strategy_status,
    mapping_source,
    mapping_updated_at,
    created_at,
    updated_at
FROM instrument_mappings
"""


class InstrumentMappingRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def upsert_mappings(self, mappings: Iterable[InstrumentMapping]) -> int:
        rows = list(mappings)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                UPSERT_INSTRUMENT_MAPPING_SQL,
                [
                    (
                        mapping.futures_symbol,
                        mapping.futures_pair,
                        mapping.futures_base_asset,
                        mapping.futures_quote_asset,
                        mapping.futures_margin_asset,
                        mapping.futures_contract_type,
                        mapping.futures_status,
                        mapping.spot_symbol,
                        mapping.spot_base_asset,
                        mapping.spot_quote_asset,
                        mapping.spot_status,
                        mapping.spot_trading_allowed,
                        mapping.spot_pair_exists,
                        mapping.spot_mapping_status.value,
                        mapping.mapping_reason.value
                        if mapping.mapping_reason is not None
                        else None,
                        mapping.positive_strategy_available,
                        mapping.negative_strategy_available,
                        mapping.negative_strategy_status.value,
                        mapping.mapping_source,
                        ensure_utc(mapping.mapping_updated_at),
                        ensure_utc(mapping.created_at),
                        ensure_utc(mapping.updated_at),
                    )
                    for mapping in rows
                ],
            )
        return len(rows)

    async def summary(self) -> InstrumentMappingSummary:
        try:
            async with self.database.acquire() as connection:
                row = await connection.fetchrow(
                    """
                    SELECT
                        COUNT(*) AS futures_symbols_processed,
                        COUNT(*) FILTER (WHERE spot_pair_exists)
                            AS futures_with_spot,
                        COUNT(*) FILTER (WHERE NOT spot_pair_exists)
                            AS futures_without_spot,
                        COUNT(*) FILTER (WHERE spot_mapping_status = 'matched')
                            AS matched,
                        COUNT(*) FILTER (WHERE spot_mapping_status = 'missing')
                            AS missing,
                        COUNT(*) FILTER (WHERE spot_mapping_status = 'ambiguous')
                            AS ambiguous,
                        COUNT(*) FILTER (WHERE spot_mapping_status = 'unsupported')
                            AS unsupported,
                        COUNT(*) FILTER (
                            WHERE spot_mapping_status = 'spot_trading_disabled'
                        ) AS spot_trading_disabled,
                        COUNT(*) FILTER (WHERE positive_strategy_available)
                            AS positive_strategy_available,
                        COUNT(*) FILTER (WHERE negative_strategy_available)
                            AS negative_strategy_available,
                        COUNT(*) FILTER (
                            WHERE negative_strategy_status =
                                'borrow_check_not_implemented'
                        ) AS negative_strategy_pending_borrow_implementation,
                        MAX(mapping_updated_at) AS mappings_last_updated_at
                    FROM instrument_mappings
                    """
                )
        except asyncpg.UndefinedTableError:
            return _empty_summary(table_available=False)

        return InstrumentMappingSummary(
            table_available=True,
            futures_symbols_processed=int(row["futures_symbols_processed"] or 0),
            futures_with_spot=int(row["futures_with_spot"] or 0),
            futures_without_spot=int(row["futures_without_spot"] or 0),
            matched=int(row["matched"] or 0),
            missing=int(row["missing"] or 0),
            ambiguous=int(row["ambiguous"] or 0),
            unsupported=int(row["unsupported"] or 0),
            spot_trading_disabled=int(row["spot_trading_disabled"] or 0),
            positive_strategy_available=int(
                row["positive_strategy_available"] or 0
            ),
            negative_strategy_available=int(
                row["negative_strategy_available"] or 0
            ),
            negative_strategy_pending_borrow_implementation=int(
                row["negative_strategy_pending_borrow_implementation"] or 0
            ),
            mappings_last_updated_at=ensure_utc(row["mappings_last_updated_at"])
            if row["mappings_last_updated_at"] is not None
            else None,
        )

    async def list_mappings(
        self, *, status: SpotMappingStatus | None = None
    ) -> list[InstrumentMapping]:
        query = SELECT_INSTRUMENT_MAPPING_COLUMNS
        args: tuple[Any, ...] = ()
        if status is not None:
            query += "\nWHERE spot_mapping_status = $1"
            args = (status.value,)
        query += "\nORDER BY futures_symbol"

        try:
            async with self.database.acquire() as connection:
                rows = await connection.fetch(query, *args)
        except asyncpg.UndefinedTableError:
            return []
        return [self._row_to_mapping(row) for row in rows]

    async def get_mapping(self, futures_symbol: str) -> InstrumentMapping | None:
        try:
            async with self.database.acquire() as connection:
                row = await connection.fetchrow(
                    f"{SELECT_INSTRUMENT_MAPPING_COLUMNS}\nWHERE futures_symbol = $1",
                    futures_symbol,
                )
        except asyncpg.UndefinedTableError:
            return None
        return self._row_to_mapping(row) if row is not None else None

    def _row_to_mapping(self, row: Any) -> InstrumentMapping:
        return InstrumentMapping(
            futures_symbol=row["futures_symbol"],
            futures_pair=row["futures_pair"],
            futures_base_asset=row["futures_base_asset"],
            futures_quote_asset=row["futures_quote_asset"],
            futures_margin_asset=row["futures_margin_asset"],
            futures_contract_type=row["futures_contract_type"],
            futures_status=row["futures_status"],
            spot_symbol=row["spot_symbol"],
            spot_base_asset=row["spot_base_asset"],
            spot_quote_asset=row["spot_quote_asset"],
            spot_status=row["spot_status"],
            spot_trading_allowed=bool(row["spot_trading_allowed"]),
            spot_pair_exists=bool(row["spot_pair_exists"]),
            spot_mapping_status=SpotMappingStatus(row["spot_mapping_status"]),
            mapping_reason=MappingReason(row["mapping_reason"])
            if row["mapping_reason"] is not None
            else None,
            positive_strategy_available=bool(row["positive_strategy_available"]),
            negative_strategy_available=bool(row["negative_strategy_available"]),
            negative_strategy_status=NegativeStrategyStatus(
                row["negative_strategy_status"]
            ),
            mapping_source=row["mapping_source"],
            mapping_updated_at=ensure_utc(row["mapping_updated_at"]),
            created_at=ensure_utc(row["created_at"]),
            updated_at=ensure_utc(row["updated_at"]),
        )


def _empty_summary(*, table_available: bool) -> InstrumentMappingSummary:
    return InstrumentMappingSummary(
        table_available=table_available,
        futures_symbols_processed=0,
        futures_with_spot=0,
        futures_without_spot=0,
        matched=0,
        missing=0,
        ambiguous=0,
        unsupported=0,
        spot_trading_disabled=0,
        positive_strategy_available=0,
        negative_strategy_available=0,
        negative_strategy_pending_borrow_implementation=0,
        mappings_last_updated_at=None,
    )
