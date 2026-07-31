"""
Foundation for the future Funding Intelligence Engine.

This module intentionally contains no runtime service yet. The next analytical
layer should work from finalized funding interval summaries, not from current
WebSocket snapshots. That keeps current candidate detection focused on one
question: whether a symbol is worth considering right now.

Future responsibilities:

- Symbol Funding Profiles
- long-term realized funding reliability
- predicted signal frequency
- separation of Signal Frequency from Realized Reliability
- ranking based on interval summaries and realized outcomes
- historical funding intelligence for later trading-decision components

Signal Frequency means how often predicted funding became high.
Realized Reliability means how often a high predicted signal ended as a high
confirmed funding payout. These metrics must remain separate.

TODO: implement profile builders after enough complete funding interval
summaries exist and after market-cost inputs are available.
"""
