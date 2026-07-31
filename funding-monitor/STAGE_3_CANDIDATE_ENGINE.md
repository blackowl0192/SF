# Stage 3 Candidate Engine

## Purpose

Stage 3 answers one operational question:

Which positive funding opportunities look best right now?

The engine evaluates only the positive funding strategy:

- long Spot
- short USD-M perpetual futures

Positive funding means long perpetual holders pay short perpetual holders. The
engine does not evaluate negative funding strategies.

## Not Implemented

Stage 3 does not implement short Spot, Margin Borrow, Binance private APIs,
borrow or repay operations, automatic trading, orders, position management,
portfolio allocation, Telegram, UI, FastAPI, Docker, SQLAlchemy, machine
learning, final net edge, order book depth, spread, slippage, or fee modeling.

## Predicted vs Realized Funding

Predicted funding is the changing value from Binance mark price stream before a
funding settlement. It is stored in `funding_snapshots` as
`predicted_funding_rate` and `funding_rate`.

Realized funding is the final value returned by Binance funding history after
settlement. It is stored in confirmed `funding_events.actual_funding_rate`.

Candidate evaluation uses current predicted funding. Funding interval summaries
use realized funding from confirmed events and never replace it with predicted
values.

## Hard Filters

The engine evaluates machine-readable hard filters before final score
classification:

- funding must be positive
- funding must be at least `CANDIDATE_MIN_FUNDING_RATE`
- futures instrument must be active
- Spot mapping must exist
- `spot_mapping_status` must be `matched`
- `positive_strategy_available` must be true
- Spot trading must be allowed
- next funding time must exist and be in the future
- latest snapshot must not be stale
- minimum history duration and snapshot count must be available
- values must be valid `Decimal` values

Filter failures return a typed `CandidateEvaluation`; they do not raise for the
whole batch.

## Statuses

Candidate statuses are:

- `strong_candidate`
- `candidate`
- `weak_candidate`
- `observing`
- `deteriorating`
- `funding_falling`
- `unstable`
- `late_spike`
- `too_early`
- `too_late`
- `stale`
- `insufficient_history`
- `rejected`
- `expired`

Rule-based statuses have priority over score. A late spike with a high numeric
score remains `late_spike`, not `strong_candidate`.

## Rejection Reasons

Rejection and warning codes are stable machine-readable values:

- `funding_not_positive`
- `funding_below_threshold`
- `futures_inactive`
- `spot_mapping_missing`
- `spot_mapping_ambiguous`
- `spot_trading_disabled`
- `positive_strategy_unavailable`
- `next_funding_time_missing`
- `funding_time_expired`
- `stale_snapshot`
- `insufficient_history`
- `insufficient_snapshot_count`
- `persistence_too_low`
- `volatility_too_high`
- `too_many_threshold_crossings`
- `too_many_direction_changes`
- `negative_velocity`
- `negative_acceleration`
- `late_spike_detected`
- `funding_deteriorating`
- `too_early`
- `too_late`
- `invalid_data`
- `calculation_error`

## Score

Score is deterministic and bounded to `0..100`. Financial values use `Decimal`.

Components:

- funding magnitude: `0..30`
- persistence: `0..25`
- stability: `0..15`
- trend: `0..15`
- signal lifetime: `0..10`
- time to funding: `0..5`

Penalties:

- `late_spike_penalty`
- `deterioration_penalty`
- `stale_data_penalty`
- `insufficient_history_penalty`
- `instability_penalty`
- `threshold_crossing_penalty`
- `too_close_to_funding_penalty`

Final score is `max(0, base_score - penalties)`.

The implementation is split into calculators:

- `FundingScoreCalculator`
- `PersistenceScoreCalculator`
- `StabilityScoreCalculator`
- `TrendScoreCalculator`
- `LifetimeScoreCalculator`
- `TimingScoreCalculator`
- `PenaltyCalculator`

`CandidateScoringService` only calls those calculators and assembles
`ScoreComponents`. The current funding formula remains linear, but
`FundingScoreCalculator` is isolated so a sigmoid, logarithmic, or saturating
formula can be introduced later without rewriting `CandidateEngine`.

## Late Spike

Late spike detection combines multiple signals:

- current funding is much higher than the previous mean
- signal age is short
- persistence is below the configured minimum
- funding settlement is close
- short-window velocity is positive
- short-window acceleration is not negative

This prevents a single late predicted funding jump from becoming a
`strong_candidate`.

## Deterioration

Deterioration detection combines:

- negative velocity
- negative acceleration
- current funding below the short-window mean
- short-window mean below the primary-window mean
- funding close to the minimum threshold
- consecutive declines

The result can be `deteriorating` or `funding_falling` while current funding is
still above the minimum threshold.

## Tables

`candidate_evaluations` stores one controlled evaluation snapshot per symbol and
time bucket. The uniqueness key is:

```text
(exchange, futures_symbol, evaluated_at_bucket, engine_version)
```

Important fields include predicted funding, next funding time, status, score
components, penalties, persistence, volatility, velocity, acceleration, signal
age, rejection reasons, warning flags, metrics details, and engine version.

`funding_interval_summaries` links one confirmed funding event to its predicted
snapshots inside the same funding interval. It stores realized funding, first
and last predicted rates, min/max/mean/median predicted rates, pre-funding point
rates at 120/60/30/15/5 minutes, peak predicted timestamp, signal start,
positive streaks, threshold durations, prediction error, history coverage, and
summary status.

The uniqueness key is:

```text
(exchange, futures_symbol, funding_time)
```

`symbol_funding_profiles` is a prepared table for the future Funding
Intelligence Engine. Stage 3 does not write to it.

## Funding Interval Summary

Point snapshots are selected by nearest timestamp within
`FUNDING_INTERVAL_POINT_TOLERANCE_SECONDS`. If no snapshot is inside tolerance,
the point value is `NULL`.

Snapshots after `funding_time` are not used for pre-funding metrics.

`prediction_error` is:

```text
last_predicted_rate - realized_funding_rate
```

`FundingIntervalBuilder` is independent from PostgreSQL. It receives a confirmed
`FundingEvent` and its snapshots, then returns a `FundingIntervalSummary`. This
keeps replay, simulation, backtesting, and offline analytics independent from
repository code.

## CLI

```powershell
python -m funding_monitor migrate
python -m funding_monitor candidates
python -m funding_monitor candidates --top 20
python -m funding_monitor candidates --min-score 60
python -m funding_monitor candidates --status candidate
python -m funding_monitor candidates --status strong_candidate
python -m funding_monitor candidates --symbol BTCUSDT
python -m funding_monitor candidates --include-rejected
python -m funding_monitor candidates --no-persist
python -m funding_monitor candidates --json
python -m funding_monitor candidate-rejections
python -m funding_monitor build-funding-interval-summaries
```

The default `candidates` command hides rejected, stale, expired, and
insufficient-history rows. Use `--include-rejected` or `--status rejected` for
diagnostics.

## Configuration

Main settings:

```text
CANDIDATE_ENGINE_ENABLED=true
CANDIDATE_MIN_FUNDING_RATE=0.0003
CANDIDATE_MIN_HISTORY_MINUTES=15
CANDIDATE_PRIMARY_WINDOW_MINUTES=30
CANDIDATE_SHORT_WINDOW_MINUTES=5
CANDIDATE_LONG_WINDOW_MINUTES=60
CANDIDATE_MIN_SNAPSHOT_COUNT=10
CANDIDATE_MAX_SNAPSHOT_AGE_SECONDS=120
CANDIDATE_MIN_PERSISTENCE_RATIO=0.70
CANDIDATE_MAX_STD_DEV=0.0002
CANDIDATE_MAX_THRESHOLD_CROSSINGS=4
CANDIDATE_MAX_DIRECTION_CHANGES=8
CANDIDATE_LATE_SPIKE_LOOKBACK_MINUTES=5
CANDIDATE_LATE_SPIKE_MIN_JUMP_RATIO=1.50
CANDIDATE_DETERIORATION_LOOKBACK_MINUTES=5
CANDIDATE_MAX_NEGATIVE_VELOCITY=-0.00002
CANDIDATE_MIN_MINUTES_TO_FUNDING=5
CANDIDATE_MAX_MINUTES_TO_FUNDING=480
CANDIDATE_STRONG_SCORE=80
CANDIDATE_MIN_SCORE=60
CANDIDATE_PERSIST_INTERVAL_SECONDS=60
CANDIDATE_MAX_RESULTS=50
FUNDING_INTERVAL_POINT_TOLERANCE_SECONDS=90
FUNDING_INTERVAL_SUMMARY_BATCH_SIZE=500
```

Velocity and acceleration use the existing `FundingHistoryService` units:
funding-rate change per second.

## Ranking

Candidates are sorted by:

1. status priority
2. total score descending
3. predicted funding descending
4. persistence descending
5. minutes to funding ascending
6. futures symbol ascending

## Engine Version

The current algorithm version is:

```text
CANDIDATE_ENGINE_VERSION = "1.0"
```

The version is persisted in `candidate_evaluations` so future scoring changes
can be compared safely.

## Future Architecture

Stage 3 remains the current-signal layer:

```text
FundingHistoryService
-> CandidateRuleEvaluator
-> Score Calculators
-> CandidateScoringService
-> CandidateEngine
-> CandidateRepository
```

Funding interval analytics is separate:

```text
FundingSnapshots + confirmed FundingEvents
-> FundingIntervalBuilder
-> FundingIntervalSummary
-> Repository
```

The future long-term layer is separate again:

```text
Funding Intelligence Engine
-> Symbol Funding Profiles
-> Trading Decision Engine
```

`src/funding_monitor/funding_intelligence.py` is intentionally a placeholder.
It documents that long-term analytics must be built from
`funding_interval_summaries`, not from current WebSocket snapshots.

Predicted Funding is the current changing estimate before settlement. Realized
Funding is the confirmed payout after settlement.

Signal Frequency and Realized Reliability must remain separate:

- Signal Frequency: how often predicted funding became high.
- Realized Reliability: how often high predicted funding finished as high
  confirmed realized funding.

This separation prevents the future intelligence layer from treating temporary
predicted spikes as realized yield.

## Later Stages

Recommended next work:

- add spread and fee inputs
- add depth and slippage
- add realized candidate outcome tracking
- add symbol reliability profiles
- build net edge after reliable market-cost inputs exist
