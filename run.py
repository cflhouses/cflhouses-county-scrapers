"""
CLI entry point for local and manual runs.

Examples:
    # Run a specific date range against Lake code enforcement
    python run.py lake-code-enforcement --from 2026-03-15 --to 2026-04-15

    # Run last 14 days (default for weekly scheduled run)
    python run.py lake-code-enforcement --last-days 14
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def cmd_lake_code_enforcement(args: argparse.Namespace) -> int:
    # Imported here so that `python run.py --help` works without installing deps
    from scrapers.lake_fl.code_enforcement import LakeCodeEnforcementPipeline

    if args.last_days:
        date_to = date.today()
        date_from = date_to - timedelta(days=args.last_days)
    elif args.from_date and args.to_date:
        date_from = _parse_date(args.from_date)
        date_to = _parse_date(args.to_date)
    else:
        print("Specify either --last-days N or both --from YYYY-MM-DD --to YYYY-MM-DD")
        return 2

    pipeline = LakeCodeEnforcementPipeline()
    summary = pipeline.run(date_from, date_to)
    print("---- Run summary ----")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CFL Houses county scrapers")
    sub = parser.add_subparsers(dest="command", required=True)

    lake_ce = sub.add_parser("lake-code-enforcement", help="Lake County code enforcement pipeline")
    lake_ce.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD (inclusive)")
    lake_ce.add_argument("--to", dest="to_date", help="End date YYYY-MM-DD (inclusive)")
    lake_ce.add_argument("--last-days", type=int, help="Shortcut: run for the last N days")

    args = parser.parse_args()
    if args.command == "lake-code-enforcement":
        return cmd_lake_code_enforcement(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
