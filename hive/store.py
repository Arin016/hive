"""SQLite stores: extract_events (+FTS), residual, daily, monthly, cursors."""
from __future__ import annotations

import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS extract_events(
  id INTEGER PRIMARY KEY,
  engineer_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_at INTEGER NOT NULL,
  project_id TEXT,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  content_hash TEXT UNIQUE NOT NULL,
  extractor_version TEXT NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ev_eng_time ON extract_events(engineer_id, event_at);
CREATE INDEX IF NOT EXISTS idx_ev_kind ON extract_events(kind);
CREATE TABLE IF NOT EXISTS extract_fts(
  rowid INTEGER PRIMARY KEY, engineer_id TEXT, kind TEXT, text TEXT
);
CREATE TABLE IF NOT EXISTS daily_activity(
  engineer_id TEXT NOT NULL, day TEXT NOT NULL,
  session_count INTEGER, projects_json TEXT, tool_counts_json TEXT,
  event_count INTEGER, last_event_at INTEGER,
  PRIMARY KEY(engineer_id, day)
);
CREATE TABLE IF NOT EXISTS monthly_rollup(
  engineer_id TEXT NOT NULL, period TEXT NOT NULL,
  event_count INTEGER, session_count INTEGER, projects_json TEXT,
  tool_counts_json TEXT, covered_days INTEGER, as_of INTEGER,
  PRIMARY KEY(engineer_id, period)
);
CREATE TABLE IF NOT EXISTS engineer_sync(
  engineer_id TEXT PRIMARY KEY, last_sync_at INTEGER,
  last_event_at INTEGER, last_attempt_at INTEGER, consecutive_failures INTEGER
);
CREATE TABLE IF NOT EXISTS file_cursor(
  path TEXT PRIMARY KEY, mtime REAL, size INTEGER
);
"""


def connect(db_path):
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def upsert_docs(con, docs):
    """Insert docs; idempotent on content_hash. Returns (inserted, skipped)."""
    ins = skp = 0
    for d in docs:
        try:
            cur = con.execute(
                "INSERT INTO extract_events(engineer_id,session_id,event_at,"
                "project_id,kind,payload_json,source_ref,content_hash,"
                "extractor_version,deleted) VALUES(?,?,?,?,?,?,?,?,?,0)",
                (d["engineer_id"], d["session_id"], d["event_at"],
                 d["project_id"], d["kind"], json.dumps(d["payload"]),
                 d["source_ref"], d["content_hash"], d["extractor_version"]))
            con.execute(
                "INSERT INTO extract_fts(rowid,engineer_id,kind,text)"
                " VALUES(?,?,?,?)",
                (cur.lastrowid, d["engineer_id"], d["kind"],
                 json.dumps(d["payload"])[:4000]))
            ins += 1
        except sqlite3.IntegrityError:
            skp += 1
    con.commit()
    return ins, skp


def mark_file_deleted(con, path_prefix):
    con.execute("UPDATE extract_events SET deleted=1 WHERE source_ref LIKE ?",
                (path_prefix + "%",))
    con.execute("DELETE FROM extract_fts WHERE rowid IN "
                "(SELECT id FROM extract_events WHERE source_ref LIKE ?)",
                (path_prefix + "%",))
    con.commit()


def day_of(ts):
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def period_of(ts):
    return time.strftime("%Y-%m", time.gmtime(ts))


def recompute_daily(con, engineer_id, day):
    rows = con.execute(
        "SELECT session_id, project_id, kind, payload_json, event_at"
        " FROM extract_events WHERE engineer_id=? AND date(event_at,'unixepoch')=?"
        " AND deleted=0", (engineer_id, day)).fetchall()
    sessions, projects, tools = set(), set(), {}
    last = 0
    for sid, proj, kind, pj, eat in rows:
        sessions.add(sid)
        if proj:
            projects.add(proj)
        last = max(last, eat)
        if kind == "tool_use":
            try:
                t = json.loads(pj).get("tool", "?")
            except (ValueError, TypeError):
                t = "?"
            tools[t] = tools.get(t, 0) + 1
    con.execute("INSERT OR REPLACE INTO daily_activity VALUES(?,?,?,?,?,?,?)",
                (engineer_id, day, len(sessions), json.dumps(sorted(projects)),
                 json.dumps(tools), len(rows), last))
    con.commit()
    return len(rows)


def recompute_monthly(con, engineer_id, period):
    rows = con.execute(
        "SELECT day, session_count, projects_json, tool_counts_json,"
        " event_count FROM daily_activity WHERE engineer_id=?"
        " AND substr(day,1,7)=?", (engineer_id, period)).fetchall()
    sessions = ev = 0
    projects, tools = set(), {}
    for _, sc, pj, tj, ec in rows:
        sessions += sc or 0
        ev += ec or 0
        try:
            projects.update(json.loads(pj or "[]"))
            for k, v in json.loads(tj or "{}").items():
                tools[k] = tools.get(k, 0) + v
        except (ValueError, TypeError):
            pass
    con.execute("INSERT OR REPLACE INTO monthly_rollup VALUES(?,?,?,?,?,?,?,?)",
                (engineer_id, period, ev, sessions, json.dumps(sorted(projects)),
                 json.dumps(tools), len(rows), int(time.time())))
    con.commit()
    return len(rows)
