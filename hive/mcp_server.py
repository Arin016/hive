"""Minimal MCP stdio server (stdlib only): initialize, tools/list, tools/call."""
from __future__ import annotations

import json
import sys

TOOL_DEFS = [
    {"name": "hive_lookup",
     "description": ("Exact fact in a tight window. Use for one concrete fact "
                     "(IP, path, project on a day). Do NOT use for weekly "
                     "narratives or monthly summaries."),
     "inputSchema": {"type": "object",
                     "properties": {
                         "engineer_id": {"type": "string"},
                         "from": {"type": "integer"},
                         "to": {"type": "integer"},
                         "kind": {"type": "string"},
                         "filters": {"type": "object"}},
                     "required": ["engineer_id", "from", "to"]}},
    {"name": "hive_activity",
     "description": ("Recent what-have-they-been-up-to (days to ~week, max 14 "
                     "days, max 5 engineers). Do NOT use for exact facts or "
                     "monthly summaries."),
     "inputSchema": {"type": "object",
                     "properties": {
                         "engineer_ids": {"type": "array", "items": {"type": "string"}},
                         "from": {"type": "integer"},
                         "to": {"type": "integer"}},
                     "required": ["engineer_ids"]}},
    {"name": "hive_report",
     "description": ("Monthly / large-span summary for one or many engineers. "
                     "Do NOT use for single facts or last-few-days questions."),
     "inputSchema": {"type": "object",
                     "properties": {
                         "engineer_ids": {"type": "array", "items": {"type": "string"}},
                         "period": {"type": "string"}},
                     "required": ["engineer_ids", "period"]}},
]


def _read_msg():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline().decode().strip()
        if not line:
            break
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    n = int(headers.get("content-length", "0"))
    if not n:
        return None
    return json.loads(sys.stdin.buffer.read(n).decode())


def _send(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(
        f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    sys.stdout.buffer.flush()


def serve(db_path, org, caller):
    from . import tools as T
    _id = 0
    while True:
        msg = _read_msg()
        if msg is None:
            break
        _id = msg.get("id", _id)
        method = msg.get("method", "")
        try:
            if method == "initialize":
                res = {"protocolVersion": "2024-11-05",
                       "capabilities": {"tools": {}},
                       "serverInfo": {"name": "hive", "version": "0.1.0"}}
            elif method in ("notifications/initialized", "notifications/cancelled"):
                continue
            elif method == "ping":
                res = {}
            elif method == "tools/list":
                res = {"tools": TOOL_DEFS}
            elif method == "tools/call":
                name = msg["params"]["name"]
                a = msg["params"].get("arguments", {})
                if name == "hive_lookup":
                    out = T.lookup(db_path, org, caller, a["engineer_id"],
                                   a["from"], a["to"], a.get("kind"),
                                   a.get("filters"))
                elif name == "hive_activity":
                    out = T.activity(db_path, org, caller, a["engineer_ids"],
                                     a.get("from"), a.get("to"))
                elif name == "hive_report":
                    out = T.report(db_path, org, caller, a["engineer_ids"],
                                   a["period"])
                else:
                    raise ValueError(f"unknown tool {name}")
                res = {"content": [{"type": "text",
                                    "text": json.dumps(out, indent=1)}]}
            else:
                raise ValueError(f"unknown method {method}")
            _send({"jsonrpc": "2.0", "id": _id, "result": res})
        except (ValueError, TypeError, KeyError) as e:
            _send({"jsonrpc": "2.0", "id": _id,
                   "error": {"code": -32602, "message": str(e)}})
