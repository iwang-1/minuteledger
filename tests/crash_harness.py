"""Run ledger.py's CLI, but terminate the process (os._exit, no cleanup, no commit) immediately before the
Nth SQL statement it would execute, where N comes from MINUTELEDGER_CRASH_AT. With no N it runs normally and
prints the statement count to stderr, so a test can sweep crash points across a whole run."""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

crash_at = int(os.environ.get("MINUTELEDGER_CRASH_AT", "0"))
executed = 0
_connect = sqlite3.connect


def connect(*args, **kwargs):
    conn = _connect(*args, **kwargs)

    def trace(_sql):
        global executed
        executed += 1
        if executed == crash_at:
            os._exit(137)

    conn.set_trace_callback(trace)
    return conn


sqlite3.connect = connect
import ledger  # noqa: E402

sys.argv = ["ledger.py"] + sys.argv[1:]
try:
    ledger.main()
finally:
    print(f"statements={executed}", file=sys.stderr)
