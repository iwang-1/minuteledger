"""Transactional event-time windows over versioned integer measurements."""
import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path


class Ledger:
    def __init__(self, path, width=60, grace=120):
        if type(width) != int or type(grace) != int or width < 1 or grace < 0:
            raise ValueError("invalid window or grace")
        self.path, self.width, self.grace = Path(path), width, grace
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS config(id INTEGER PRIMARY KEY, width INTEGER, grace INTEGER, watermark INTEGER);
                CREATE TABLE IF NOT EXISTS readings(id TEXT PRIMARY KEY, version INTEGER, ts INTEGER, value INTEGER);
                CREATE TABLE IF NOT EXISTS windows(start INTEGER PRIMARY KEY, total INTEGER, count INTEGER, revision INTEGER);
                CREATE TABLE IF NOT EXISTS changes(seq INTEGER PRIMARY KEY, start INTEGER, total INTEGER, count INTEGER,
                    revision INTEGER, status TEXT);
                CREATE TABLE IF NOT EXISTS cursors(source TEXT PRIMARY KEY, offset INTEGER);
                """)
            db.execute("INSERT OR IGNORE INTO config VALUES(1,?,?,-1)", (width, grace))
            if db.execute("SELECT width,grace FROM config").fetchone() != (width, grace):
                raise ValueError("window configuration differs from stored settings")

    def status(self, start, watermark):
        end = start + self.width
        if watermark < end:
            return "open"
        return "final" if watermark >= end + self.grace else "published"

    def snapshot(self, db, start, watermark):
        db.execute("UPDATE windows SET revision=revision+1 WHERE start=?", (start,))
        db.execute("""INSERT INTO changes(start,total,count,revision,status)
            SELECT start,total,count,revision,? FROM windows WHERE start=?""",
                   (self.status(start, watermark), start))

    def advance(self, db, previous, current):
        if type(current) != int or not previous <= current <= 2**53:
            raise ValueError("watermarks must advance monotonically")
        db.execute("UPDATE config SET watermark=?", (current,))
        # Only open or revisable windows can cross a lifecycle boundary.
        for (start,) in db.execute("SELECT start FROM windows WHERE start>? AND start<=? ORDER BY start",
                                   (previous-self.width-self.grace, current-self.width)).fetchall():
            if self.status(start, previous) != self.status(start, current):
                self.snapshot(db, start, current)
        return "advanced"

    def reading(self, db, item, watermark):
        key, version, ts, value = [item[k] for k in ("id", "version", "ts", "value")]
        if not isinstance(key, str) or not key or any(type(x) != int for x in (version, ts)):
            raise ValueError("invalid reading identity")
        if not 1 <= version <= 2**53 or not 0 <= ts <= 2**53 or (value is not None and
                (type(value) != int or abs(value) > 10**9)):
            raise ValueError("reading is outside supported bounds")
        old = db.execute("SELECT version,ts,value FROM readings WHERE id=?", (key,)).fetchone()
        if old and version <= old[0]:
            if version == old[0] and old != (version, ts, value):
                raise ValueError("conflicting reading version")
            return "duplicate" if version == old[0] else "superseded"
        start = ts // self.width * self.width
        deltas = {}
        if old and old[2] is not None:
            deltas[old[1] // self.width * self.width] = (-old[2], -1)
        if value is not None:
            total, count = deltas.get(start, (0, 0))
            deltas[start] = (total + value, count + 1)
        if any(self.status(s, watermark) == "final" for s in set(deltas) | {start}):
            return "too_late"
        db.execute("INSERT OR REPLACE INTO readings VALUES(?,?,?,?)", (key, version, ts, value))
        for s, (total, count) in sorted(deltas.items()):
            if total or count:
                prior = db.execute("SELECT total FROM windows WHERE start=?", (s,)).fetchone()
                updated = (prior[0] if prior else 0) + total
                if not -(2**63) <= updated < 2**63:
                    raise ValueError("window sum exceeds signed 64-bit range")
                db.execute("""INSERT INTO windows VALUES(?,?,?,0) ON CONFLICT(start) DO UPDATE SET
                    total=windows.total+excluded.total,count=windows.count+excluded.count""", (s, total, count))
                if self.status(s, watermark) == "published":
                    self.snapshot(db, s, watermark)
        return "applied"

    def ingest(self, source, offset, item):
        if not isinstance(source, str) or not source or type(offset) != int or offset < 0:
            raise ValueError("invalid source position")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT offset FROM cursors WHERE source=?", (source,)).fetchone()
            previous = row[0] if row else -1
            if offset <= previous:
                return "replayed"
            if offset != previous + 1:
                raise ValueError("source offset gap")
            watermark, = db.execute("SELECT watermark FROM config").fetchone()
            if set(item) == {"watermark"}:
                result = self.advance(db, watermark, item["watermark"])
            elif set(item) == {"id", "version", "ts", "value"}:
                result = self.reading(db, item, watermark)
            else:
                raise ValueError("unknown record shape")
            db.execute("INSERT OR REPLACE INTO cursors VALUES(?,?)", (source, offset))
            return result

    def changes(self, after=0):
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM changes WHERE seq>? ORDER BY seq", (after,))]


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--db", default="ledger.sqlite")
    cli.add_argument("--width", type=int, default=60)
    cli.add_argument("--grace", type=int, default=120)
    sub = cli.add_subparsers(dest="cmd", required=True)
    read = sub.add_parser("ingest")
    read.add_argument("source")
    read.add_argument("file", type=Path)
    changes = sub.add_parser("changes")
    changes.add_argument("--after", type=int, default=0)
    args = cli.parse_args()
    try:
        ledger = Ledger(args.db, args.width, args.grace)
        if args.cmd == "ingest":
            with args.file.open() as stream:
                for offset, line in enumerate(stream):
                    item = json.loads(line)
                    print(json.dumps({"offset":offset, "status":ledger.ingest(args.source, offset, item)}))
        else:
            for record in ledger.changes(args.after):
                print(json.dumps(record))
    except (ValueError, TypeError, OSError, sqlite3.Error) as exc:
        cli.exit(2, f"minuteledger: {exc}\n")


if __name__ == "__main__":
    main()
