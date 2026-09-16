"""Sync: scan local session dirs (stand-in for IT connector), incremental,
write order facts->grains->cursor, tombstones for vanished files."""
from __future__ import annotations

import json
import os
import time

from . import store
from .extract import extract_file

# Local stand-ins for enrolled device stores. engineer_id -> [(adapter, dir)].
DEFAULT_SOURCES = {
    "arin": [
        ("kiro", os.path.expanduser("~/.kiro/sessions")),
        ("codex", os.path.expanduser("~/.codex/sessions")),
        ("claude", os.path.expanduser("~/.claude/projects")),
    ],
}


def _session_id(adapter, path):
    parts = path.replace(os.path.expanduser("~"), "~").split(os.sep)
    if adapter == "kiro":
        for i, p in enumerate(parts):
            if p.startswith("sess_"):
                return p
        return parts[-2] if len(parts) > 1 else parts[-1]
    return os.path.splitext(os.path.basename(path))[0][:80]


def _iter_jsonl(adapter, root):
    if not os.path.isdir(root):
        return
    for dirpath, _, files in os.walk(root):
        for fn in sorted(files):
            if fn.endswith((".jsonl", ".json")):
                yield os.path.join(dirpath, fn)


def sync(db_path, sources=None, max_files=0, now=None):
    """Run one sync cycle. Returns stats dict. Incremental via file_cursor."""
    sources = sources or DEFAULT_SOURCES
    now = int(now if now is not None else time.time())
    con = store.connect(db_path)
    stats = {"files": 0, "inserted": 0, "skipped_dup": 0, "touched": set(),
             "adapters": {}}
    seen_paths = set()
    scanned_roots = set()
    truncated = False  # max_files budget hit: scan is partial, so no tombstones
    for engineer_id, srcs in sources.items():
        if truncated:
            break
        for adapter, root in srcs:
            if truncated:
                break
            if not os.path.isdir(root) or not os.access(root, os.R_OK | os.X_OK):
                continue  # nothing observable; exempt from tombstones below
            root_complete = True
            n = 0
            for path in _iter_jsonl(adapter, root):
                if max_files and stats["files"] >= max_files:
                    truncated = True
                    root_complete = False
                    break
                seen_paths.add(path)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if st.st_size == 0:
                    continue  # empty stub; costs no budget
                row = con.execute("SELECT mtime, size FROM file_cursor"
                                  " WHERE path=?", (path,)).fetchone()
                if row and row[0] == st.st_mtime and row[1] == st.st_size:
                    continue  # unchanged
                docs, status = extract_file(
                    path, adapter, engineer_id,
                    _session_id(adapter, path), int(st.st_mtime))
                stats["adapters"].setdefault(adapter, {"ok": 0, "skip": 0})
                if status != "ok":
                    stats["adapters"][adapter]["skip"] += 1
                    continue
                stats["adapters"][adapter]["ok"] += 1
                ins, skp = store.upsert_docs(con, docs)
                stats["inserted"] += ins
                stats["skipped_dup"] += skp
                stats["files"] += 1
                n += 1
                for d in docs:
                    if d["kind"] != "residual":
                        stats["touched"].add(
                            (engineer_id, store.day_of(d["event_at"]),
                             store.period_of(d["event_at"])))
                con.execute("INSERT OR REPLACE INTO file_cursor VALUES(?,?,?)",
                            (path, st.st_mtime, st.st_size))
                con.commit()
            if root_complete:
                scanned_roots.add(root)
    # Tombstones for vanished files. Only under roots fully observed this
    # run: a missing/unreadable root or a budget-truncated scan is absence
    # of evidence, not evidence of deletion.
    if not truncated:
        for (path,) in con.execute("SELECT path FROM file_cursor").fetchall():
            if path in seen_paths:
                continue
            if not any(path == r or path.startswith(r + os.sep)
                       for r in scanned_roots):
                continue
            if not os.path.exists(path):
                # Grains covering these events must be recomputed below;
                # collect before the tombstone hides them (deleted=1).
                for eng, day, period in con.execute(
                        "SELECT DISTINCT engineer_id,"
                        " date(event_at,'unixepoch'),"
                        " substr(date(event_at,'unixepoch'),1,7)"
                        " FROM extract_events WHERE source_ref LIKE ?"
                        " AND deleted=0", (path + "%",)).fetchall():
                    stats["touched"].add((eng, day, period))
                store.mark_file_deleted(con, path)
                con.execute("DELETE FROM file_cursor WHERE path=?", (path,))
                con.commit()
    stats["truncated"] = truncated
    # Recompute touched grains, then publish cursors (write order).
    for engineer_id, day, period in sorted(stats["touched"]):
        store.recompute_daily(con, engineer_id, day)
        store.recompute_monthly(con, engineer_id, period)
    for engineer_id in sources:
        last_ev = con.execute(
            "SELECT max(event_at) FROM extract_events WHERE engineer_id=?"
            " AND deleted=0", (engineer_id,)).fetchone()[0]
        prev = con.execute("SELECT consecutive_failures FROM engineer_sync"
                           " WHERE engineer_id=?", (engineer_id,)).fetchone()
        fails = 0 if stats["inserted"] or stats["files"] else (prev[0] + 1 if prev else 1)
        con.execute("INSERT OR REPLACE INTO engineer_sync VALUES(?,?,?,?,?)",
                    (engineer_id, now, last_ev, now, fails))
        con.commit()
    stats["touched"] = sorted(stats["touched"])
    con.close()
    return stats


def load_org(org_path):
    if org_path and os.path.exists(org_path):
        with open(org_path) as fh:
            return json.load(fh)
    return {"engineers": {"arin": {"manager": None}}, "callers": {"local": "arin"}}
