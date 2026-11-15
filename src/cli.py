"""Command line entry point that runs inside the project image.

    python -m marketplace.cli <command>

`scripts/dev.py` wraps these commands for use from the host.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date

from marketplace.batch import Batch, new_run_id
from marketplace.bi.metabase import provision
from marketplace.config import Settings
from marketplace.dq.loader import LAYERS
from marketplace.dq.model import PASSED, CheckResult, DataQualityError
from marketplace.dq.reconciliation import (
    load_detectors,
    render_markdown,
    score_detection,
    write_report,
)
from marketplace.generator.ledger import Ledger
from marketplace.generator.model import PROFILES, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator
from marketplace.generator.stores import PostgresMongoStore
from marketplace.logging_setup import configure_logging
from marketplace.ops.status import PipelineStatus, pipeline_status
from marketplace.pipeline import PipelineContext, check_layer, maintain, open_context, run_batch


def _status(ctx: PipelineContext) -> PipelineStatus:
    with Ledger(ctx.settings.ledger_path) as ledger:
        current_day = ledger.get_state("current_day")
    return pipeline_status(
        ctx.spark, ctx.settings.sim_start_date, None if current_day is None else int(current_day)
    )


def cmd_seed(args: argparse.Namespace, settings: Settings) -> None:
    config = GeneratorConfig.from_settings(settings, PROFILES[args.profile])
    with PostgresMongoStore(settings) as store, Ledger(settings.ledger_path) as ledger:
        summary = MarketplaceSimulator(store, ledger, config).seed()
    print(f"Seeded day 0 ({config.start_date}): {dict(summary.inserted)}")
    print(f"Injected defects: {dict(summary.defects)}")


def cmd_advance(args: argparse.Namespace, settings: Settings) -> None:
    with PostgresMongoStore(settings) as store, Ledger(settings.ledger_path) as ledger:
        profile = PROFILES[ledger.get_state("profile", "default")]
        config = GeneratorConfig.from_settings(settings, profile)
        for summary in MarketplaceSimulator(store, ledger, config).advance(args.days):
            print(
                f"Day {summary.day}: inserted {dict(summary.inserted)}, "
                f"updated {dict(summary.updated)}, defects {dict(summary.defects)}"
            )


def cmd_run(args: argparse.Namespace, settings: Settings) -> None:
    """Run the pipeline for one date, or for every simulated date not completed yet."""
    ctx = open_context(settings)
    dates = [date.fromisoformat(args.date)] if args.date else list(_status(ctx).pending_dates)
    if not dates:
        print("Nothing to run: the pipeline is up to date with the simulated sources.")
        return
    for batch_date in dates:
        batch = Batch(batch_date, new_run_id())
        started = time.monotonic()
        try:
            run_batch(ctx, batch)
        except DataQualityError as error:
            sys.exit(f"Batch {batch.batch_id} stopped by a quality gate: {error}")
        print(f"Batch {batch.batch_id} completed in {time.monotonic() - started:.1f}s")


def _print_check_results(layer: str, results: list[CheckResult]) -> None:
    not_passed = [result for result in results if result.status != PASSED]
    print(f"{layer}: {len(results)} checks, {len(results) - len(not_passed)} passed")
    for result in not_passed:
        print(
            f"  [{result.status}] {result.spec.check_id}: {result.details} "
            f"(observed {result.observed_value}, threshold {result.spec.threshold})"
        )


def cmd_dq(args: argparse.Namespace, settings: Settings) -> None:
    """Re-run all checks for the latest completed batch and score defect detection."""
    ctx = open_context(settings)
    status = _status(ctx)
    if status.latest_completed_batch is None:
        sys.exit("No completed batch found. Run the pipeline first.")
    batch = Batch(status.latest_completed_batch, new_run_id("dq"))

    print(f"Quality checks for batch {batch.batch_id}")
    for layer in LAYERS:
        _print_check_results(layer, check_layer(ctx, batch, layer))

    processed_days = (status.latest_completed_batch - settings.sim_start_date).days
    with Ledger(settings.ledger_path) as ledger:
        injected = ledger.keys_by_type(through_day=processed_days)
    scores = score_detection(ctx.spark, injected, load_detectors(settings.config_dir))
    write_report(scores, settings.project_root / "reports")
    print("\nInjected defects versus detection (generator ledger is the answer key)\n")
    print(render_markdown(scores))


def cmd_status(args: argparse.Namespace, settings: Settings) -> None:
    status = _status(open_context(settings))
    summary = {
        "latest_simulated_date": _iso(status.latest_simulated_date),
        "latest_completed_batch": _iso(status.latest_completed_batch),
        "pending_dates": [day.isoformat() for day in status.pending_dates],
    }
    if args.json:
        print(json.dumps(summary))
        return
    for name, value in summary.items():
        print(f"{name}: {value}")


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value else None


def cmd_maintain(args: argparse.Namespace, settings: Settings) -> None:
    ctx = open_context(settings)
    batch = Batch(date.today(), new_run_id("maintenance"))
    for result in maintain(ctx, batch):
        print(
            f"{result.table}: rewrote {result.data_files_rewritten} data files into "
            f"{result.data_files_added}, deleted {result.snapshot_files_deleted} expired and "
            f"{result.orphan_files_deleted} orphan files"
        )


def cmd_provision_bi(args: argparse.Namespace, settings: Settings) -> None:
    for name, dashboard_id in provision(settings).items():
        print(f"Dashboard ready: {name} (/dashboard/{dashboard_id})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketplace", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    seed = commands.add_parser("seed", help="recreate the source data as of day 0")
    seed.add_argument("--profile", choices=sorted(PROFILES), default="default")
    seed.set_defaults(handler=cmd_seed)

    advance = commands.add_parser("advance", help="simulate more days in the sources")
    advance.add_argument("--days", type=int, required=True)
    advance.set_defaults(handler=cmd_advance)

    run = commands.add_parser("run", help="run the pipeline for pending dates or one date")
    run.add_argument("--date", help="logical date, YYYY-MM-DD (default: all pending dates)")
    run.set_defaults(handler=cmd_run)

    commands.add_parser(
        "dq", help="re-run quality checks and report defect detection"
    ).set_defaults(handler=cmd_dq)

    status = commands.add_parser("status", help="show simulated dates and pipeline progress")
    status.add_argument("--json", action="store_true", help="print one JSON object")
    status.set_defaults(handler=cmd_status)

    commands.add_parser(
        "maintain", help="compact tables, expire snapshots, remove orphan files"
    ).set_defaults(handler=cmd_maintain)

    commands.add_parser(
        "provision-bi", help="create or update the Metabase connection and dashboards"
    ).set_defaults(handler=cmd_provision_bi)
    return parser


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()
    args.handler(args, Settings.from_env())


if __name__ == "__main__":
    main()
