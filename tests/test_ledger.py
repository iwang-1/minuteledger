import json
import random
import tempfile
import unittest
from pathlib import Path

from ledger import Ledger


class WindowTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "ledger"
        self.ledger = Ledger(self.path)
        self.offset = 0

    def send(self, item):
        result = self.ledger.ingest("fixture", self.offset, item)
        self.offset += 1
        return result

    def test_replay_corrections_and_finality(self):
        rows = [json.loads(line) for line in Path("examples/corrections.jsonl").read_text().splitlines()]
        for row in rows:
            status = self.send(row)
        self.assertEqual(status, "too_late")
        changes = self.ledger.changes()
        self.assertEqual([(r["total"], r["status"]) for r in changes if r["start"] == 0],
                         [(50, "published"), (30, "published"), (0, "published"), (0, "final")])
        self.ledger = Ledger(self.path)
        for i, row in enumerate(rows):
            self.assertEqual(self.ledger.ingest("fixture", i, row), "replayed")
        self.assertEqual(self.ledger.changes(), changes)
        self.assertEqual(self.ledger.changes(changes[-2]["seq"]), changes[-1:])

    def test_conflict_rolls_back_offset(self):
        row = dict(id="a", version=2, ts=3, value=4)
        self.send(row)
        with self.assertRaises(ValueError):
            self.send(dict(row, value=5))
        self.assertEqual(self.send(row), "duplicate")
        self.assertEqual(self.send(dict(row, version=1)), "superseded")
        with self.assertRaises(ValueError):
            self.ledger.ingest("fixture", 9, row)

    def test_atomic_rejection_across_window_boundary(self):
        self.send(dict(id="x", ts=1, version=1, value=10))
        self.send({"watermark":180})
        self.assertEqual(self.send(dict(id="x", ts=181, version=2, value=99)), "too_late")
        self.send({"watermark":500})
        self.assertEqual([(r["start"], r["total"]) for r in self.ledger.changes()], [(0,10)])

    def test_differential_against_offline_grouping(self):
        rng = random.Random(394)
        latest = {}
        for _ in range(600):
            key = str(rng.randrange(45))
            version = latest.get(key, (0,0,None))[0] + 1
            ts = rng.randrange(240)
            value = rng.choice([None, 0, rng.randrange(-50, 50)])
            self.send(dict(id=key, version=version, ts=ts, value=value))
            latest[key] = (version, ts, value)
        self.send({"watermark":1000})
        expected = {}
        for _, ts, value in latest.values():
            if value is not None:
                bucket = ts // 60 * 60
                total, count = expected.get(bucket, (0,0))
                expected[bucket] = (total+value, count+1)
        self.assertEqual({r["start"]:(r["total"],r["count"]) for r in self.ledger.changes() if r["count"]}, expected)

    def test_slow_registered_source_holds_back_finality_after_restart(self):
        path = self.path.with_name("multi")
        ledger = Ledger(path, sources=["fast", "slow"])
        ledger.ingest("fast", 0, {"watermark": 300})
        ledger = Ledger(path)
        ledger.ingest("slow", 0, dict(id="a", version=1, ts=8, value=9))
        self.assertEqual(ledger.changes(), [])
        ledger.ingest("slow", 1, {"watermark": 60})
        self.assertEqual(ledger.changes()[0]["status"], "published")
        with self.assertRaises(ValueError):
            ledger.ingest("slow", 2, {"watermark": 50})
        ledger.ingest("slow", 2, {"watermark": 300})
        self.assertEqual(ledger.changes()[-1]["status"], "final")
        with self.assertRaises(ValueError):
            ledger.ingest("unknown", 0, {"watermark": 400})
        with self.assertRaises(ValueError):
            Ledger(path, sources=["fast"])
