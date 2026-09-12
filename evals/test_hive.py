"""Golden evals: deterministic units + live checks on real session JSONs.

Live checks assert shapes/counts only — never file contents, never secrets.
Run: python3 -m pytest evals/ -q   (add --live to include real-data checks)
"""
import json
import os
import sqlite3
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hive import extract, store, sync as syncmod
from hive import tools as T

ORG = {"engineers": {"arin": {"manager": None},
                     "teammate": {"manager": "lead"},
                     "lead": {"manager": None}},
       "callers": {"local": "arin", "boss": "lead"}}


def _memdb():
    con = sqlite3.connect(":memory:")
    con.executescript(store.SCHEMA)
    return con


# ---------- deterministic units ----------

def test_miners_find_ip_arn_path():
    hits = extract._mine_text("use 10.0.0.11 and arn:aws:iam::123456789012:role/X "
                              "at /Users/arin/x and i-0abcdef1234567890")
    assert hits["ip"] == ["10.0.0.11"]
    assert hits["ec2_id"] == ["i-0abcdef1234567890"]
    assert any("123456789012" in a for a in hits["arn"])
    assert hits["path"] == ["/Users/arin/x"]


def test_miners_reject_bad_ipv4():
    assert "ip" not in extract._mine_text("999.999.1.1")
    assert "ip" not in extract._mine_text("version 1.2.3 of the tool")


def test_secret_keys_dropped():
    obj = {"api_key": "sk-123", "nested": {"token": "abc"}, "tool": "Read"}
    out = extract._scrub(obj)
    assert "api_key" not in out and "token" not in out.get("nested", {})
    assert out["tool"] == "Read"


def test_range_too_large_and_too_many():
    con = _memdb()
    con.close()
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = f.name
    store.connect(db).close()
    now = int(time.time())
    r = T.activity(db, ORG, "local", ["arin"], now - 30 * 86400, now)
    assert r["status"] == "range_too_large"
    r = T.activity(db, ORG, "local", ["a", "b", "c", "d", "e", "f"])
    assert r["status"] == "too_many_engineers"
    os.unlink(db)


def test_forbidden_and_no_coverage():
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = f.name
    store.connect(db).close()
    now = int(time.time())
    r = T.lookup(db, ORG, "local", "teammate", now - 100, now)
    assert r["status"] == "forbidden"
    r = T.lookup(db, ORG, "boss", "teammate", now - 100, now)
    assert r["status"] == "no_coverage"
    os.unlink(db)


def test_kiro_tool_shape():
    obj = {"id": "1", "timestamp": 1725000000000,
           "payload": {"toolCallId": "t", "title": "Read",
                       "filePath": "/Users/arin/personal/hive/x.py",
                       "kind": "tool", "status": "ok"}}
    docs = extract.extract_kiro_line(obj, "arin", "s", 1725000000, "f:0")
    kinds = [d["kind"] for d in docs]
    assert "tool_use" in kinds
    tu = next(d for d in docs if d["kind"] == "tool_use")
    assert tu["payload"]["tool"] == "Read"
    assert tu["payload"]["filePath"].endswith("x.py")


LIVE = os.environ.get("HIVE_LIVE") == "1" or "--live" in sys.argv


@pytest.mark.skipif(not LIVE, reason="needs HIVE_LIVE=1 or --live")
def test_live_sync_extracts_real_sessions(tmp_path):
    import glob
    home = os.path.expanduser("~")
    rich = sorted(
        (p for p in glob.glob(home + "/.kiro/sessions/*/sess_*/messages.jsonl")
         if os.path.getsize(p) > 50000),
        key=os.path.getsize, reverse=True)[:8]
    kiro_dirs = sorted({os.path.dirname(os.path.dirname(p)) for p in rich})
    sources = {"arin": [
        ("codex", home + "/.codex/sessions/2026/09/03"),
        ("claude", home + "/.claude/projects"),
    ] + [("kiro", d) for d in kiro_dirs]}
    assert kiro_dirs, "no rich Kiro sessions found"
    db = str(tmp_path / "live.db")
    stats = syncmod.sync(db, sources=sources, max_files=120)
    assert stats["inserted"] > 500, stats
    con = sqlite3.connect(db)
    kinds = dict(con.execute("SELECT kind, COUNT(*) FROM extract_events "
                             "GROUP BY kind").fetchall())
    assert kinds.get("tool_use", 0) > 0, kinds
    assert kinds.get("residual", 0) > 0, kinds
    # Tool names are real and non-empty.
    names = [r[0] for r in con.execute(
        "SELECT DISTINCT json_extract(payload_json,'$.tool')"
        " FROM extract_events WHERE kind='tool_use' LIMIT 50").fetchall()]
    assert names and all(n for n in names)
    # No secret keys or values leaked into payloads (real patterns only).
    import re
    secret_rx = re.compile(
        r"sk-[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}|xox[bap]-|ghp_[A-Za-z0-9]+|"
        r"-----BEGIN [A-Z ]*PRIVATE KEY|\"api_key\"\s*:")
    bad = 0
    for (pj,) in con.execute(
            "SELECT payload_json FROM extract_events").fetchall():
        if secret_rx.search(pj):
            bad += 1
    assert bad == 0
    # Lookup + activity + report serve from the same DB.
    now = int(time.time())
    r = T.lookup(db, ORG, "local", "arin", 0, now, kind="tool_use")
    assert r["status"] == "ok" and r["data"]["count"] > 0
    r = T.activity(db, ORG, "local", ["arin"], now - 7 * 86400, now)
    assert r["status"] == "ok"
    assert r["data"]["total_events"] > 0
    ym = time.strftime("%Y-%m", time.gmtime(now))
    r = T.report(db, ORG, "local", ["arin"], ym)
    assert r["status"] == "ok"
    con.close()
