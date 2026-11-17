"""Crash the real CLI at many points and check that rerunning it converges to the uninterrupted result.

The README promises that the cursor, reading, window updates and output revisions commit in one SQLite
transaction, so a crash before commit only leaves an offset to replay. The harness ends the process with
os._exit immediately before a chosen SQL statement, so crash points can be swept across the whole run,
including the gaps between statements of one transaction that a wall-clock SIGKILL almost never hits.
After every crash the change feed must be a prefix of the clean run's feed, and after the final rerun every
table must match: no revision lost, invented, reordered, or emitted twice.
"""
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HARNESS = Path(__file__).resolve().parent / "crash_harness.py"


def write_stream(path, seed, n):
    rng = random.Random(seed)
    versions, watermark = {}, -1
    with path.open("w") as out:
        for _ in range(n):
            if rng.random() < 0.08:
                watermark += rng.randrange(1, 40)
                out.write(json.dumps({"watermark": watermark}) + "\n")
                continue
            key = f"r{rng.randrange(60)}"
            versions[key] = versions.get(key, 0) + 1
            value = rng.choice([None, 0, rng.randrange(-100, 100), rng.randrange(-100, 100)])
            ts = max(0, watermark + rng.randrange(-200, 120))
            out.write(json.dumps(dict(id=key, version=versions[key], ts=ts, value=value)) + "\n")


def ingest(db, stream, crash_at=0):
    """Run the CLI, crashing before statement `crash_at` (0 = never). Returns (finished, statements run)."""
    env = dict(os.environ, MINUTELEDGER_CRASH_AT=str(crash_at))
    proc = subprocess.run([sys.executable, str(HARNESS), "--db", str(db), "ingest", "s", str(stream)],
                          capture_output=True, text=True, env=env)
    if proc.returncode == 137:
        return False, crash_at
    if proc.returncode != 0:
        raise AssertionError(f"ingest failed ({proc.returncode}): {proc.stderr}")
    return True, int(re.search(r"statements=(\d+)", proc.stderr).group(1))


def dump(db):
    with sqlite3.connect(db) as conn:
        return {table: conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
                for table in ("changes", "windows", "readings", "cursors", "config")}


class CrashTests(unittest.TestCase):
    def test_crashes_at_swept_statements_converge_to_the_uninterrupted_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            stream = tmp / "stream.jsonl"
            write_stream(stream, seed=7, n=150)
            _, total = ingest(tmp / "clean.sqlite", stream)
            expected = dump(tmp / "clean.sqlite")
            self.assertGreater(len(expected["changes"]), 10, "stream should publish many revisions")

            rng = random.Random(11)
            for trial in range(40):
                # A first crash anywhere in the run, then a second crash somewhere in the recovery run.
                db = tmp / f"crashed-{trial}.sqlite"
                first = rng.randrange(1, total + 1)
                for crash_at in (first, rng.randrange(1, total + 1)):
                    finished, _ = ingest(db, stream, crash_at=crash_at)
                    if finished:
                        break
                    partial = dump(db)["changes"]
                    self.assertEqual(partial, expected["changes"][:len(partial)],
                                     f"trial {trial}: change feed diverged after a crash at statement {crash_at}")
                ingest(db, stream)
                self.assertEqual(dump(db), expected, f"trial {trial}: first crash at statement {first}")


if __name__ == "__main__":
    unittest.main()
