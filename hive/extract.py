"""Deterministic extractor: adapters per session format + miners + residual.

Emits dicts: {engineer_id, session_id, event_at, project_id, kind,
payload, source_ref, content_hash, extractor_version}. Never emits secrets.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone

from . import EXTRACTOR_VERSION

# Secret-looking keys are dropped before indexing (payload + residual).
SECRET_KEY_RE = re.compile(
    r"(api[_-]?key|apikey|secret|passwd|password|token|auth|cookie|"
    r"session[_-]?token|private[_-]?key|aws_secret|access[_-]?key|bearer)",
    re.IGNORECASE,
)

IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}"
                     r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b")
ARN_RE = re.compile(r"\barn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:\d{0,12}:[^\s\"']+")
EC2_RE = re.compile(r"\bi-[0-9a-f]{8,17}\b")
S3_RE = re.compile(r"\bs3://[^\s\"']+")
PATH_RE = re.compile(r"(?:/Users/|/home/|/tmp/|/var/|/etc/|/opt/)[^\s\"']{2,180}")
SHA_RE = re.compile(r"\b[0-9a-f]{40}\b|\b[0-9a-f]{64}\b")

MAX_WALK_DEPTH = 12
MAX_STR_SCAN = 20000
MAX_RESIDUAL_CHARS = 600


def _scrub(obj, depth=0):
    """Deep-copy dropping secret-looking keys; truncate long strings."""
    if depth > MAX_WALK_DEPTH:
        return None
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(k, str) and SECRET_KEY_RE.search(k):
                continue
            out[k] = _scrub(v, depth + 1)
        return out
    if isinstance(obj, list):
        return [_scrub(v, depth + 1) for v in obj[:200]]
    if isinstance(obj, str) and len(obj) > 2000:
        return obj[:2000] + "…[truncated]"
    return obj


def _mine_text(text):
    """Return {field: [values]} mined from a string. Deterministic order."""
    found = {}
    for name, rx in (("ip", IPV4_RE), ("arn", ARN_RE), ("ec2_id", EC2_RE),
                     ("s3uri", S3_RE), ("path", PATH_RE), ("sha", SHA_RE)):
        vals = sorted(set(rx.findall(text)))
        if vals:
            found[name] = vals[:50]
    return found


def _walk_strings(obj, depth=0):
    """Yield (field_path, string) pairs for mining + residual."""
    if depth > MAX_WALK_DEPTH:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and SECRET_KEY_RE.search(k):
                continue
            if isinstance(v, str):
                yield (str(k), v[:MAX_STR_SCAN])
            else:
                yield from _walk_strings(v, depth + 1)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:200]):
            if isinstance(v, str):
                yield (f"[{i}]", v[:MAX_STR_SCAN])
            else:
                yield from _walk_strings(v, depth + 1)


def content_hash(engineer_id, session_id, kind, payload):
    canon = json.dumps([engineer_id, session_id, kind, payload],
                       sort_keys=True, default=str)
    return hashlib.sha256(canon.encode()).hexdigest()


def parse_event_at(value, fallback):
    """Flexible timestamp parse -> epoch seconds int. Fallback on failure."""
    try:
        if value is None:
            return fallback
        if isinstance(value, (int, float)):
            v = float(value)
            if v > 1e14:      # microseconds
                v /= 1e6
            elif v > 1e11:    # milliseconds
                v /= 1e3
            return int(v)
        if isinstance(value, str):
            s = value.strip().rstrip("Z")
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                return fallback
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
    except (ValueError, OverflowError, OSError):
        pass
    return fallback


def _mk(engineer_id, session_id, event_at, project_id, kind, payload,
        source_ref):
    payload = _scrub(payload) or {}
    return {
        "engineer_id": engineer_id,
        "session_id": session_id,
        "event_at": int(event_at),
        "project_id": project_id,
        "kind": kind,
        "payload": payload,
        "source_ref": source_ref,
        "content_hash": content_hash(engineer_id, session_id, kind, payload),
        "extractor_version": EXTRACTOR_VERSION,
    }


def _residual_doc(engineer_id, session_id, event_at, project_id, texts,
                  source_ref):
    body = " | ".join(t for _, t in texts if t)[:MAX_RESIDUAL_CHARS]
    if not body.strip():
        return None
    return _mk(engineer_id, session_id, event_at, project_id, "residual",
               {"text": body}, source_ref + "#residual")


def extract_kiro_line(obj, engineer_id, session_id, fallback_ts, source_ref):
    """One messages.jsonl line -> list of docs."""
    docs = []
    p = obj.get("payload", obj) if isinstance(obj, dict) else {}
    if not isinstance(p, dict):
        return docs
    ts = parse_event_at(obj.get("timestamp"), fallback_ts)
    ptype = str(p.get("type", ""))
    # Tool-call shapes carry toolCallId + args/title/kind/filePath.
    if "toolCallId" in p or "filePath" in p or p.get("kind") == "tool":
        name = (p.get("title") or p.get("actionType") or p.get("kind")
                or ptype or "tool")
        if str(name) in NON_TOOL_TITLES or ptype in NON_TOOL_TITLES:
            pass  # not an invocation; falls through to residual below
        else:
            payload = {"tool": str(name)[:160]}
            for fk in ("filePath", "kind", "actionType", "status"):
                if p.get(fk) is not None:
                    payload[fk] = str(p[fk])[:300]
            args = p.get("args")
            if isinstance(args, dict):
                for ak, av in list(args.items())[:10]:
                    if isinstance(av, str) and len(av) < 300:
                        payload["arg_" + str(ak)[:40]] = av
            for _, s in _walk_strings(p):
                for fk, fv in _mine_text(s).items():
                    payload.setdefault(fk, []).extend(
                        v for v in fv if v not in payload.get(fk, []))
            docs.append(_mk(engineer_id, session_id, ts, None, "tool_use",
                            payload, source_ref))
    # Text-bearing shapes -> residual fuel.
    texts = []
    for fk, s in _walk_strings(p):
        if fk in ("content", "value", "text", "question", "context") and s.strip():
            texts.append((fk, s[:400]))
    if texts:
        r = _residual_doc(engineer_id, session_id, ts, None, texts, source_ref)
        if r:
            docs.append(r)
    # Project grain: explicit session markers only (not every executionId line).
    if ptype in ("session", "execution", "session_start", "session_metadata",
                 "session_event"):
        docs.append(_mk(engineer_id, session_id, ts, None, "session",
                        {"marker": ptype or "execution"}, source_ref))
    return docs


def extract_codex_line(obj, engineer_id, session_id, fallback_ts, source_ref):
    """One rollout jsonl record -> list of docs."""
    docs = []
    if not isinstance(obj, dict):
        return docs
    ts = parse_event_at(obj.get("timestamp"), fallback_ts)
    rtype = str(obj.get("type", ""))
    p = obj.get("payload", {})
    if rtype == "session_meta" and isinstance(p, dict):
        payload = {}
        for fk in ("cwd", "model", "id", "workdir", "git"):
            if p.get(fk) is not None:
                payload[fk] = str(p[fk])[:300]
        project = payload.get("cwd")
        docs.append(_mk(engineer_id, session_id, ts, project, "session",
                        payload or {"marker": "session_meta"}, source_ref))
        return docs
    if isinstance(p, dict):
        # Function/tool call shapes: {name, arguments} anywhere one level down.
        for key in ("function_call", "tool_call", "call", "action"):
            c = p.get(key)
            if isinstance(c, dict) and c.get("name"):
                docs.append(_mk(engineer_id, session_id, ts, None, "tool_use",
                                {"tool": str(c["name"])[:160]}, source_ref))
        texts = [s[:400] for _, s in _walk_strings(p)
                 if s.strip()][:4]
        if texts:
            r = _residual_doc(engineer_id, session_id, ts, None,
                              [("text", t) for t in texts], source_ref)
            if r:
                docs.append(r)
        mined = {}
        for _, s in _walk_strings(p):
            for fk, fv in _mine_text(s).items():
                mined.setdefault(fk, []).extend(
                    v for v in fv if v not in mined.get(fk, []))
        if mined and not any(d["kind"] == "tool_use" for d in docs):
            docs.append(_mk(engineer_id, session_id, ts, None, "resource",
                            mined, source_ref))
    return docs


def extract_claude_line(obj, engineer_id, session_id, fallback_ts,
                        source_ref):
    """One Claude session jsonl record -> list of docs."""
    docs = []
    if not isinstance(obj, dict):
        return docs
    ts = parse_event_at(obj.get("timestamp"), fallback_ts)
    rtype = str(obj.get("type", ""))
    if rtype in ("permission-mode", "session-meta"):
        return docs
    msg = obj.get("message", {})
    blocks = msg.get("content") if isinstance(msg, dict) else None
    if isinstance(blocks, str):
        blocks = [{"type": "text", "text": blocks}]
    if not isinstance(blocks, list):
        return docs
    for b in blocks:
        if not isinstance(b, dict):
            continue
        if b.get("type") == "tool_use" and b.get("name"):
            payload = {"tool": str(b["name"])[:160]}
            inp = b.get("input")
            if isinstance(inp, dict):
                for ak in ("file_path", "path", "command", "pattern",
                           "url", "query"):
                    if isinstance(inp.get(ak), str):
                        payload[ak] = inp[ak][:300]
            docs.append(_mk(engineer_id, session_id, ts, None, "tool_use",
                            payload, source_ref))
        elif b.get("type") == "text" and str(b.get("text", "")).strip():
            r = _residual_doc(engineer_id, session_id, ts, None,
                              [("text", str(b["text"])[:400])], source_ref)
            if r:
                docs.append(r)
    return docs


ADAPTERS = {"kiro": extract_kiro_line, "codex": extract_codex_line,
            "claude": extract_claude_line}

# Kiro message titles that carry a toolCallId but are not tool invocations.
NON_TOOL_TITLES = {"tool_result", "pending_interaction", "usage_summary",
                   "session_event", "turn_end", "turn_start",
                   "steering_inclusion", "session_metadata"}


def extract_file(path, adapter, engineer_id, session_id, fallback_ts,
                 max_bytes=25_000_000):
    """Extract one JSONL file -> docs list. Skips oversize/non-JSONL safely."""
    docs = []
    try:
        if os.path.getsize(path) > max_bytes:
            return docs, "skipped_oversize"
        fn = ADAPTERS.get(adapter)
        if fn is None:
            return docs, "unknown_adapter"
        with open(path, encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                try:
                    docs.extend(fn(obj, engineer_id, session_id, fallback_ts,
                                   f"{path}:{i}"))
                except (ValueError, TypeError, AttributeError):
                    continue
    except OSError:
        return docs, "unreadable"
    return docs, "ok"
