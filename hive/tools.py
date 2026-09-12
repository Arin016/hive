"""Three deterministic tool handlers. Envelope statuses mirror the spec."""
from __future__ import annotations

import json
import time

from . import store

MAX_ACTIVITY_DAYS = 14
MAX_ACTIVITY_ENGINEERS = 5
DEFAULT_ACTIVITY_DAYS = 7


def _descendants(org, engineer_id):
    kids = {e for e, v in org.get("engineers", {}).items()
            if v.get("manager") == engineer_id}
    out = set(kids)
    for k in list(kids):
        out |= _descendants(org, k)
    return out


def authorize(org, caller, engineer_ids):
    """Return (allowed, forbidden). Unknown caller -> all forbidden."""
    me = org.get("callers", {}).get(caller)
    if me is None:
        return [], list(engineer_ids)
    allowed_set = {me} | _descendants(org, me)
    return ([e for e in engineer_ids if e in allowed_set],
            [e for e in engineer_ids if e not in allowed_set])


def _freshness(con, engineer_ids):
    out = {}
    for e in engineer_ids:
        row = con.execute("SELECT last_sync_at, last_event_at FROM engineer_sync"
                          " WHERE engineer_id=?", (e,)).fetchone()
        out[e] = {"last_sync_at": row[0] if row else None,
                  "last_event_at": row[1] if row else None}
    return out


def _envelope(status, engineers, asked_window, freshness, data=None,
              warnings=None):
    return {"status": status, "engineers": engineers,
            "asked_window": asked_window, "freshness": freshness,
            "warnings": warnings or [], "data": data or {}}


def _covered(con, engineer_id, frm, to):
    """Return (covered_to | None). None = nothing covered (no sync ever)."""
    row = con.execute("SELECT last_sync_at FROM engineer_sync WHERE engineer_id=?",
                      (engineer_id,)).fetchone()
    if not row or not row[0]:
        return None
    return row[0]


def lookup(db_path, org, caller, engineer_id, frm, to, kind=None, filters=None):
    filters = filters or {}
    allowed, forbidden = authorize(org, caller, [engineer_id])
    con = store.connect(db_path)
    fresh = _freshness(con, [engineer_id])
    window = {"from": frm, "to": to}
    if forbidden:
        con.close()
        return _envelope("forbidden", [engineer_id], window, fresh,
                         warnings=["caller cannot see this engineer"])
    covered_to = _covered(con, engineer_id, frm, to)
    if covered_to is None or frm > covered_to:
        con.close()
        return _envelope("no_coverage", [engineer_id], window, fresh)
    warnings = []
    if to > covered_to:
        warnings.append(f"covered through {covered_to}; newer events unsynced")
        to = covered_to
        window = {"from": frm, "to": to}
    q = ("SELECT event_at, kind, payload_json, source_ref FROM extract_events"
         " WHERE engineer_id=? AND event_at>=? AND event_at<=? AND deleted=0")
    args = [engineer_id, frm, to]
    if kind:
        q += " AND kind=?"
        args.append(kind)
    for col in ("project_id",):
        if filters.get(col):
            q += f" AND {col}=?"
            args.append(filters[col])
    rows = con.execute(q + " ORDER BY event_at", args).fetchall()
    matches = []
    for eat, k, pj, src in rows:
        try:
            payload = json.loads(pj)
        except (ValueError, TypeError):
            continue
        ok = True
        for fk in ("ip", "path", "tool", "arn", "ec2_id"):
            want = filters.get(fk)
            if want is None:
                continue
            val = payload.get(fk)
            vals = val if isinstance(val, list) else [val]
            if not any(want in str(v) for v in vals if v is not None):
                ok = False
                break
        if ok:
            matches.append({"event_at": eat, "kind": k, "payload": payload,
                            "source_ref": src})
    con.close()
    if not matches:
        return _envelope("not_found", [engineer_id], window, fresh)
    return _envelope("ok", [engineer_id], window, fresh,
                     {"matches": matches, "count": len(matches)})


def activity(db_path, org, caller, engineer_ids, frm=None, to=None):
    now = int(time.time())
    to = int(to) if to is not None else now
    frm = int(frm) if frm is not None else to - DEFAULT_ACTIVITY_DAYS * 86400
    allowed, forbidden = authorize(org, caller, engineer_ids)
    con = store.connect(db_path)
    fresh = _freshness(con, engineer_ids)
    window = {"from": frm, "to": to}
    warnings = ([f"forbidden: {', '.join(forbidden)}"] if forbidden else [])
    if len(engineer_ids) > MAX_ACTIVITY_ENGINEERS:
        con.close()
        return _envelope("too_many_engineers", engineer_ids, window, fresh,
                         warnings=warnings)
    if to - frm > MAX_ACTIVITY_DAYS * 86400:
        con.close()
        return _envelope("range_too_large", engineer_ids, window, fresh,
                         warnings=warnings + ["use hive_report for >14 days"])
    days, projects, tools, total = [], set(), {}, 0
    for e in allowed:
        if not _covered(con, e, frm, to):
            warnings.append(f"no_coverage: {e}")
            continue
        for day, sc, pj, tj, ec in con.execute(
                "SELECT day, session_count, projects_json, tool_counts_json,"
                " event_count FROM daily_activity WHERE engineer_id=?"
                " AND day>=date(?,'unixepoch') AND day<=date(?,'unixepoch')"
                " ORDER BY day", (e, frm, to)).fetchall():
            days.append({"engineer": e, "day": day, "sessions": sc,
                         "events": ec})
            total += ec or 0
            try:
                projects.update(json.loads(pj or "[]"))
                for k, v in json.loads(tj or "{}").items():
                    tools[k] = tools.get(k, 0) + v
            except (ValueError, TypeError):
                pass
    con.close()
    return _envelope("ok", allowed, window, fresh,
                     {"per_day_highlights": days,
                      "projects_in_range": sorted(projects),
                      "notable_resources_tools": tools,
                      "covered_through": to, "total_events": total},
                     warnings)


def report(db_path, org, caller, engineer_ids, period):
    allowed, forbidden = authorize(org, caller, engineer_ids)
    con = store.connect(db_path)
    fresh = _freshness(con, engineer_ids)
    window = {"period": period}
    warnings = ([f"forbidden: {', '.join(forbidden)}"] if forbidden else [])
    per, total_ev, total_sess, projects = [], 0, 0, set()
    for e in allowed:
        row = con.execute("SELECT event_count, session_count, projects_json,"
                          " tool_counts_json, covered_days, as_of"
                          " FROM monthly_rollup WHERE engineer_id=? AND period=?",
                          (e, period)).fetchone()
        if not row:
            warnings.append(f"no row: {e} {period}")
            continue
        ev, ss, pj, tj, cov, asof = row
        total_ev += ev or 0
        total_sess += ss or 0
        try:
            projects.update(json.loads(pj or "[]"))
            tools = json.loads(tj or "{}")
        except (ValueError, TypeError):
            tools = {}
        per.append({"engineer": e, "events": ev, "sessions": ss,
                    "tools": tools, "covered_days": cov, "as_of": asof})
    con.close()
    return _envelope("ok", allowed, window, fresh,
                     {"engineers": per, "total_events": total_ev,
                      "total_sessions": total_sess,
                      "projects": sorted(projects)}, warnings)
