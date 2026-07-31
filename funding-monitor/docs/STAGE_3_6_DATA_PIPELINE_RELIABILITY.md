# Stage 3.6 Data Pipeline Reliability

## Purpose

Stage 3.6 makes the observation pipeline continuous, measurable, and safe to
backfill. It does not start Stage 4 and does not add spot, order book, execution,
portfolio, or dashboard components.

The goal is to accumulate a uniform dataset for Strategy Validation:

```text
Binance mark price WebSocket
-> FundingSnapshot
-> FundingEvent observation
-> FundingEvent confirmation
-> CandidateEvaluation
-> FundingIntervalSummary
-> StrategyValidation dataset
```

## Root Cause From The Audit

The collector used a valid WebSocket endpoint and a 60-second per-symbol
throttle, but it wrote snapshots one by one and had no lifecycle diagnostics.
The observed `1115` snapshots represent roughly two full 529-symbol collection
rounds, not a continuous 45-minute run.

At 529 active symbols and one snapshot per symbol per minute, a continuous
45-minute run should produce about:

```text
529 symbols * 45 minutes = 23,805 snapshots
```

The gap was operational: the collector had not been running continuously, and
there was no health command or lifecycle log trail that made the stop obvious.

`candidate_evaluations` was `0` because Candidate Engine was only invoked by
manual CLI commands. It was not part of collector runtime orchestration.

`funding_interval_summaries` was `0` because the interval builder was only
invoked by a manual CLI command. Confirming a funding event did not trigger
summary creation or retry/backfill.

## Snapshot Persistence Policy

The policy is explicit and configurable:

- normal mode writes one heartbeat snapshot per symbol every
  `SNAPSHOT_PERSIST_INTERVAL_SECONDS`;
- pre/post funding windows use `DETAILED_SNAPSHOT_INTERVAL_SECONDS`;
- capture windows are still controlled by `FUNDING_WINDOW_BEFORE_SECONDS` and
  `FUNDING_WINDOW_AFTER_SECONDS`;
- snapshots are flushed in batches using `SNAPSHOT_BATCH_SIZE` and
  `SNAPSHOT_FLUSH_INTERVAL_SECONDS`;
- all active symbols and funding directions remain observable.

Defaults:

```text
SNAPSHOT_PERSIST_INTERVAL_SECONDS=60
SNAPSHOT_BATCH_SIZE=500
SNAPSHOT_FLUSH_INTERVAL_SECONDS=5
FUNDING_WINDOW_BEFORE_SECONDS=600
FUNDING_WINDOW_AFTER_SECONDS=300
DETAILED_SNAPSHOT_INTERVAL_SECONDS=1
```

The detailed interval can produce high write volume near funding time. Do not
reduce `SNAPSHOT_PERSIST_INTERVAL_SECONDS` or keep a very low detailed interval
without checking PostgreSQL write capacity.

## Expected Volume

For 529 symbols and a 60-second normal interval:

```text
per day:   529 * 1,440 = 761,760 snapshots
per month: 761,760 * 30 = 22,852,800 snapshots
```

With three 15-minute detailed windows per day at a 1-second interval:

```text
detailed per day: 529 * 45 * 60 = 1,428,300 snapshots
normal remainder: 529 * 1,395 = 737,955 snapshots
total per day:    about 2,166,255 snapshots
```

Retention and downsampling should be designed later. Stage 3.6 does not delete
historical snapshots.

## Collector Lifecycle

The collector now logs structured lifecycle events:

- `collector_started`
- `websocket_connected`
- `websocket_disconnected`
- `reconnect_attempt`
- `reconnect_success`
- `batch_persisted`
- `collector_stopped`
- `collector_error`

Batch logs include attempted/inserted counts, active symbol count, latest
message time, and latest persist time. The collector does not log every snapshot
row.

Graceful shutdown handles:

- SIGINT;
- SIGTERM;
- WebSocket loop stop;
- final snapshot batch flush;
- background pipeline task cancellation;
- confirmation task cancellation;
- PostgreSQL pool close.

## Health Checks

Use:

```powershell
python -m funding_monitor collector-health
python -m funding_monitor collector-health --json
```

The command reports:

- latest snapshot timestamp;
- snapshot age;
- snapshots over 1, 5, 15, and 60 minutes;
- unique symbols over 1, 5, 15, and 60 minutes;
- expected active symbols;
- coverage ratio;
- latest confirmed funding event;
- pending and failed confirmations;
- latest candidate evaluation;
- latest funding interval summary;
- pipeline warnings.

`collector-health --json` exits non-zero for critical states:

- snapshots are stale;
- symbol coverage is too low;
- confirmation backlog is overdue;
- candidate pipeline has not run.

Use worst-symbol coverage:

```powershell
python -m funding_monitor coverage-report --minutes 60 --limit 20
```

Coverage metrics include expected symbols, observed symbols, missing symbols,
coverage ratio, latest snapshot gap, maximum gap, and median gap.

## Pipeline Status

Use:

```powershell
python -m funding_monitor pipeline-status
```

It reports:

- total snapshots;
- snapshots in the last hour;
- symbols covered in the last hour;
- funding events by status;
- future, pending, failed, overdue, and invalid confirmations;
- candidate evaluations in the last hour;
- interval summaries in the last 24 hours;
- latest timestamp for each pipeline stage;
- warnings.

Warnings:

- `SNAPSHOT_COLLECTION_STALE`
- `LOW_SYMBOL_COVERAGE`
- `CONFIRMATION_BACKLOG`
- `CANDIDATE_PIPELINE_NOT_RUNNING`
- `INTERVAL_SUMMARY_BACKLOG`

## Candidate Evaluation Pipeline

The collector runtime starts periodic candidate evaluation using existing
Candidate Engine rules. It does not change scoring, weights, thresholds, or
statuses.

Default schedule:

```text
CANDIDATE_EVALUATION_INTERVAL_SECONDS=60
```

Manual command:

```powershell
python -m funding_monitor evaluate-candidates
python -m funding_monitor evaluate-candidates --symbols BTCUSDT,ETHUSDT --dry-run
```

Options:

- `--symbols`
- `--limit`
- `--at`
- `--dry-run`
- `--json`

Persistence is idempotent through the existing
`(exchange, futures_symbol, evaluated_at_bucket, engine_version)` key.

## Confirmation Backfill

The live scheduler still confirms events observed by the running collector.
Stage 3.6 adds manual overdue backfill:

```powershell
python -m funding_monitor backfill-confirmations
python -m funding_monitor backfill-confirmations --limit 100 --retry-failed
```

The command only reads public Binance funding history and updates matching due
events when Binance returns an exact funding timestamp. Future events are not
treated as failures.

Diagnostic confirmation buckets:

- future;
- waiting;
- confirmed;
- failed;
- overdue;
- invalid.

## Funding Interval Summary Backfill

The interval summary pipeline is:

```text
confirmed FundingEvent
-> FundingIntervalBuilder
-> FundingIntervalSummary
-> PostgreSQL upsert
```

Manual command:

```powershell
python -m funding_monitor backfill-funding-intervals
python -m funding_monitor backfill-funding-intervals --from 2024-01-01T00:00:00+00:00 --to 2024-01-02T00:00:00+00:00 --symbols BTCUSDT
```

Options:

- `--from`
- `--to`
- `--symbols`
- `--limit`
- `--dry-run`
- `--retry-failed`
- `--json`

The command is idempotent through the existing
`(exchange, futures_symbol, funding_time)` key. If snapshots are insufficient,
the persisted summary status remains `insufficient_history` or
`partial_history`; the builder does not create false complete summaries.

## Runtime Command

Run the complete operational collector:

```powershell
python -m funding_monitor collect
```

This runtime performs:

1. sync symbols;
2. collect snapshots;
3. detect/create funding events;
4. confirm due funding events;
5. evaluate candidates;
6. build interval summaries for confirmed events;
7. repeat.

## Troubleshooting

If snapshots stop:

```powershell
python -m funding_monitor collector-health
python -m funding_monitor coverage-report --minutes 60 --limit 20
```

If confirmations lag:

```powershell
python -m funding_monitor pipeline-status
python -m funding_monitor backfill-confirmations --limit 100
```

If candidate rows are missing:

```powershell
python -m funding_monitor evaluate-candidates --dry-run
python -m funding_monitor evaluate-candidates
```

If interval summaries are missing:

```powershell
python -m funding_monitor backfill-funding-intervals --dry-run
python -m funding_monitor backfill-funding-intervals
```

If Strategy Validation rejects old events, first check whether each event has
enough pre-funding snapshots. Do not fill gaps with synthetic data.
