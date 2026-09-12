"""Hive CLI: sync | lookup | activity | report | serve (MCP stdio)."""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import store, sync as syncmod
from . import tools as T

DEFAULT_DB = os.environ.get("HIVE_DB") or os.path.expanduser("~/.hive/hive.db")


def _org(args):
    return syncmod.load_org(args.org)


def cmd_sync(args):
    stats = syncmod.sync(args.db, max_files=args.max_files)
    stats["touched"] = [list(t) for t in stats["touched"]]
    print(json.dumps(stats, indent=1))


def cmd_lookup(args):
    print(json.dumps(T.lookup(args.db, _org(args), args.caller, args.engineer,
                              args.frm, args.to, args.kind,
                              json.loads(args.filters or "{}")), indent=1))


def cmd_activity(args):
    print(json.dumps(T.activity(args.db, _org(args), args.caller,
                                args.engineers, args.frm, args.to), indent=1))


def cmd_report(args):
    print(json.dumps(T.report(args.db, _org(args), args.caller,
                              args.engineers, args.period), indent=1))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="hive")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--org", default=None)
    ap.add_argument("--caller", default="local")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sync")
    s.add_argument("--max-files", type=int, default=0)
    s.set_defaults(fn=cmd_sync)
    s = sub.add_parser("lookup")
    s.add_argument("--engineer", required=True)
    s.add_argument("--frm", type=int, required=True)
    s.add_argument("--to", type=int, required=True)
    s.add_argument("--kind", default=None)
    s.add_argument("--filters", default="{}")
    s.set_defaults(fn=cmd_lookup)
    s = sub.add_parser("activity")
    s.add_argument("--engineers", nargs="+", required=True)
    s.add_argument("--frm", type=int, default=None)
    s.add_argument("--to", type=int, default=None)
    s.set_defaults(fn=cmd_activity)
    s = sub.add_parser("report")
    s.add_argument("--engineers", nargs="+", required=True)
    s.add_argument("--period", required=True)
    s.set_defaults(fn=cmd_report)
    s = sub.add_parser("serve")
    from .mcp_server import serve as _serve
    s.set_defaults(fn=lambda a: _serve(a.db, _org(a), a.caller))
    args = ap.parse_args(argv)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
