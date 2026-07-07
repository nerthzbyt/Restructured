"""Command-line interface for nertz_engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _ensure_paths() -> None:
    root = Path(__file__).resolve().parent.parent
    src = root / "src"
    for path in (root, src):
        entry = str(path)
        if entry not in sys.path:
            sys.path.insert(0, entry)


def cmd_serve(args: argparse.Namespace) -> int:
    _ensure_paths()
    import uvicorn

    uvicorn.run(
        "src.Nertzh:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    _ensure_paths()
    from src.settings import ConfigSettings

    config = ConfigSettings()
    for key, value in sorted(vars(config).items()):
        if key.startswith("_"):
            continue
        print(f"{key}={value}")
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    _ensure_paths()
    from nertz_engine.storage.migrator import migrate_metrics_jsonl, migrate_sqlite_trades

    root = Path(__file__).resolve().parent.parent
    duckdb_path = str(args.duckdb or (root / "data" / "nertz.duckdb"))
    results = {}
    if not args.skip_jsonl:
        results["jsonl"] = migrate_metrics_jsonl(
            str(root / "data" / "metrics_snapshots.jsonl"),
            duckdb_path,
            limit=int(args.limit),
        )
    if not args.skip_sqlite:
        results["sqlite_trades"] = migrate_sqlite_trades(
            str(root / "data" / "trading.db"),
            duckdb_path,
            limit=int(args.limit),
        )
    print(json.dumps(results, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nertz", description="Nertz engine CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the API server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8081)
    serve.add_argument("--reload", action="store_true")
    serve.set_defaults(func=cmd_serve)

    status = sub.add_parser("status", help="Print current configuration")
    status.set_defaults(func=cmd_status)

    migrate = sub.add_parser("migrate", help="Migrate legacy data into DuckDB")
    migrate.add_argument("--duckdb", default=None, help="Target DuckDB path")
    migrate.add_argument("--limit", type=int, default=500000)
    migrate.add_argument("--skip-jsonl", action="store_true")
    migrate.add_argument("--skip-sqlite", action="store_true")
    migrate.set_defaults(func=cmd_migrate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())