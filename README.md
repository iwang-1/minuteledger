# MinuteLedger

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/) [![dependencies: standard library only](https://img.shields.io/badge/dependencies-stdlib_only-brightgreen)](#)

A local stream processor for measurements that can be corrected after a window has already been published. It keeps a durable changelog of window sums and counts. Consumers checkpoint a sequence number and apply newer revisions by window start.

## At a glance

- **Late corrections done right.** Timestamp moves, deletions and late data emit revised window snapshots; once a window is final, a correction is rejected atomically.
- **Differential-tested:** **600** seeded updates match an independent offline computation exactly.
- **Crash-tested:** **40** trials, each crashing twice before chosen database writes, all recover to output identical to an uninterrupted run.
- Per-source watermarks that survive restarts, and a replayable changelog for downstream consumers.

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
- By default the caller supplies a coordinated global watermark. For independent producers, initialize a fresh database with repeated global options such as `--source-member east --source-member west`. Each member then advances its own watermark. The minimum persisted member watermark determines window finality, including after restart. An idle member holds back progress until it advances. Membership is fixed for the database lifetime. Use a new source name for an unrelated file. Rewriting earlier records under an existing source name violates the input contract.
- The database retains version and output history without a compaction policy. SQLite serializes writers on one machine. Inputs are bounded integers and aggregate sums must fit a signed 64-bit integer. Downstream consumers are responsible for atomically checkpointing their own effects.

The differential test applies 600 seeded updates with timestamp changes, deletions, zeroes and negative values, then compares final aggregates against an independent offline grouping. Other tests cover rollback on conflicting versions, restart replay, downstream cursors and atomic rejection of a move out of a finalized window.

The crash test runs the real CLI under a harness that ends the process with `os._exit` immediately before a chosen SQL statement, the equivalent of a power cut between two writes. Each of 40 trials crashes once at a random statement of a 150-record run, crashes again during recovery, then reruns to completion. After every crash the change feed must be a prefix of an uninterrupted run's feed, and at the end every table must match it exactly. Moving a snapshot's revision bump into its own transaction makes this test fail, which shows it can detect a split commit. ([test](tests/test_crash.py) · [harness](tests/crash_harness.py))
