# MinuteLedger

A local stream processor for measurements that can be corrected after a window has already been published. It keeps a durable changelog of window sums and counts. Consumers checkpoint a sequence number and apply newer revisions by window start.

## Reproduce a correction

Requires Python 3.11+ with SQLite. In a fresh checkout:

```sh
python3 ledger.py ingest sensors examples/corrections.jsonl
python3 ledger.py changes
python3 ledger.py ingest sensors examples/corrections.jsonl
python3 ledger.py changes --after 4
python3 -m unittest discover -v
```

Two readings first produce a sum of 50 in the first minute. A timestamp correction moves one reading into the next minute, reducing the first sum to 30. A deletion reduces it to zero. At watermark 180 the first window closes permanently. The last record tries to restore a deleted reading there and receives a too_late disposition. Replaying the same input file resumes from its committed cursor. [Input](examples/corrections.jsonl) · [Expected change feed](examples/changes.jsonl)

## Processing semantics

- Windows are half-open intervals aligned to integer event time. At the end watermark, an existing window becomes published and receives a snapshot. Late changes emit a new full snapshot with a higher revision. At end plus grace the window becomes final. A watermark jump can publish a final snapshot directly.
- Each logical reading has an increasing version. Null values are tombstones. A newer reading removes the old contribution and adds the new contribution in one transaction. If either affected window is final, the correction is rejected as a whole. A repeated identical version is accepted as a duplicate. Conflicting content at the current version stops ingestion.
- A source name identifies an immutable, append-only JSONL stream. Its zero-based line number is the offset. The processor commits the cursor, reading, window updates and output revisions in one SQLite write transaction. A crash before commit leaves the offset available to replay. Gaps and malformed records stop progress. A too-late disposition advances the cursor.
- The watermark is global and supplied by the caller. With multiple producers, callers must coordinate a safe global watermark upstream. Use a new source name for an unrelated file. Rewriting earlier records under an existing source name violates the input contract.
- The database retains version and output history without a compaction policy. SQLite serializes writers on one machine. Inputs are bounded integers and aggregate sums must fit a signed 64-bit integer. Downstream consumers are responsible for atomically checkpointing their own effects.

The differential test applies 600 seeded updates with timestamp changes, deletions, zeroes and negative values, then compares final aggregates against an independent offline grouping. Other tests cover rollback on conflicting versions, restart replay, downstream cursors and atomic rejection of a move out of a finalized window.
