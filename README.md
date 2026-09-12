# Hive — deterministic activity mirror + 3 MCP tools (v0.1.0 demo slice)

Built from `razorpay_prep_old/{LLM.txt,hive.html,hive-spec.md,hive-risks.md}`.
Backend makes **zero LLM calls**: sync extracts typed facts from on-device
session JSONs into SQLite; three tools serve them; the assistant only routes
and narrates. Single-stack demo substitutions are marked `[demo]`.

| Spec | Demo |
|---|---|
| Postgres `engineer_sync` | SQLite table, same columns |
| Elasticsearch indexes | SQLite tables + payload scan (16k docs; ES at fleet scale) |
| SQS/Kafka + workers | In-process sequential sync |
| IT connector | Local scan of `~/.kiro/sessions`, `~/.codex/sessions`, `~/.claude/projects` |
| SSO/HRIS org + ACL | `org.json` file (`{"engineers":…, "callers":…}`); default self-only |
| MCP SDK | stdlib-only stdio JSON-RPC server |

## What's implemented (spec phases 0–4 core)

- **Extractor** (`hive/extract.py`): Kiro / Codex / Claude adapters → typed
  `tool_use` (real names: Run Command, Read File, web_search…), `resource`
  (mined IP/ARN/EC2/path/SHA), `session` markers, TTL-style `residual`
  chunks. Secret-looking keys dropped **before** indexing; values truncated.
- **Sync** (`hive/sync.py`): incremental via file cursor, tombstones for
  vanished files, write order facts → daily/monthly → cursor (cursor never
  advances on failure), offline streak counter.
- **Tools** (`hive/tools.py`): `lookup` (exact, all-matches, past-sync clamp
  with warning), `activity` (daily grains, 14d / 5-engineer gates), `report`
  (monthly rollups). Envelope statuses: `ok not_found no_coverage forbidden
  range_too_large too_many_engineers`.
- **MCP server** (`hive/mcp_server.py`): stdio, `initialize / tools-list /
  tools-call / ping`, purpose + anti-purpose tool descriptions.
- **Evals** (`evals/test_hive.py`): 6 deterministic units + 1 live suite on
  real session JSONs (shape/count only — never contents, plus a real
  secret-pattern scan asserting zero leaks).

## Measured on Arin's own machine (2026-09-09)

```
$ python3 -m hive sync
  files: 6626  inserted: 16592   (~7s)

$ python3 -m hive lookup --engineer arin --frm <week-ago> --to <now> --kind tool_use
  ok  139 matches — Run Command 34, Read 16, Replace in File 9, Bash 7 …

$ python3 -m hive activity --engineers arin
  ok  7080 events over 8 day-rows

$ python3 -m hive report --engineers arin --period 2026-09
  ok  8563 events, 62 sessions
```

Tests: `HIVE_LIVE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest evals/ -q`
→ **7 passed** (note: env pytest 6.2.4 needs plugin autoload off on py3.13;
unrelated to Hive).

## Wire into Kiro / Claude Code

```json
{ "mcpServers": { "hive": {
    "command": "python3", "args": ["-m", "hive", "serve"],
    "cwd": "/Users/arin.mallanna/personal/hive",
    "env": { "HIVE_DB": "/Users/arin.mallanna/.hive/hive.db" }
} } }
```

(`--db` flag also works: `python3 -m hive --db /path.db serve`.)
Copy `LLM.txt` guidance into the agent system prompt (or rely on the tool
descriptions, which carry the same routing contract).

## Honest boundaries (matches hive-risks.md)

- Kiro tool-name precision is heuristic (`NON_TOOL_TITLES` denylist) — expect
  stragglers on new message shapes; extractor version stamps every doc so a
  v2 pass can backfill.
- Deduplication is content-based: repeated identical records collapse by
  design (normalize + dedupe), so counts are *unique facts*, not raw lines.
- Single-user demo ACL (`callers: {local: arin}`); production needs SSO/HRIS.
- No redaction-beyond-denylist, no DLQ/alerting, no sharding — all spec'd,
  all deferred, all visible in code comments where they'd attach.
