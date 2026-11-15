"""Developer command line for the marketplace lakehouse.

    python scripts/dev.py <command> [options]

This script only needs Python and Docker on the host. Every command that runs
project code does so inside the `tools` container, so the host never needs a
JDK, Spark or the Python dependencies.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
DAG_ID = "daily_marketplace_pipeline"
AIRFLOW_RUN_TIMEOUT_SECONDS = 3600
LOCAL_STATE_DIRS = ("data", "reports")


def run(command: list[str], capture: bool = False) -> str:
    """Run a command from the repository root, failing loudly if it fails."""
    print(f"$ {' '.join(command)}", flush=True)
    completed = subprocess.run(
        command, cwd=ROOT, check=False, text=True, stdout=subprocess.PIPE if capture else None
    )
    if completed.returncode != 0:
        if capture and completed.stdout:
            print(completed.stdout)
        sys.exit(completed.returncode)
    return completed.stdout or ""


def compose(*args: str, capture: bool = False) -> str:
    return run(["docker", "compose", *args], capture=capture)


def tools(*args: str, capture: bool = False) -> str:
    """Run a command in a one-off container built from the project image."""
    return compose("run", "--rm", "tools", *args, capture=capture)


def pipeline_cli(*args: str, capture: bool = False) -> str:
    return tools("python", "-m", "marketplace.cli", *args, capture=capture)


def scheduler(*args: str, capture: bool = False) -> str:
    return compose("exec", "-T", "airflow-scheduler", *args, capture=capture)


def ensure_env_file() -> None:
    """Create .env from the template on first use; never overwrite an existing one."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        shutil.copyfile(ROOT / ".env.example", env_file)
        print("Created .env from .env.example")


def cmd_up(_: argparse.Namespace) -> None:
    ensure_env_file()
    compose("up", "-d", "--build", "--wait")


def cmd_down(_: argparse.Namespace) -> None:
    compose("down")


def cmd_reset(_: argparse.Namespace) -> None:
    """Stop the stack and delete every volume and all local state."""
    ensure_env_file()
    compose("--profile", "tools", "down", "--volumes", "--remove-orphans")
    for name in LOCAL_STATE_DIRS:
        shutil.rmtree(ROOT / name, ignore_errors=True)


def cmd_seed(args: argparse.Namespace) -> None:
    pipeline_cli("seed", "--profile", "small" if args.small else "default")


def cmd_advance(args: argparse.Namespace) -> None:
    pipeline_cli("advance", "--days", str(args.days))


def cmd_run(args: argparse.Namespace) -> None:
    if args.airflow:
        if not args.date:
            sys.exit("--airflow needs --date YYYY-MM-DD")
        trigger_airflow_run(date.fromisoformat(args.date))
        return
    pipeline_cli("run", *(["--date", args.date] if args.date else []))


def cmd_dq(_: argparse.Namespace) -> None:
    pipeline_cli("dq")


def cmd_test(args: argparse.Namespace) -> None:
    pytest_args = args.pytest_args or ["--cov", "--cov-report=term-missing:skip-covered"]
    tools("python", "-m", "pytest", *pytest_args)


def cmd_lint(args: argparse.Namespace) -> None:
    if args.fix:
        tools("ruff", "check", "--fix", ".")
        tools("ruff", "format", ".")
    else:
        tools("ruff", "check", ".")
        tools("ruff", "format", "--check", ".")


def trigger_airflow_run(batch_date: date) -> None:
    """Trigger the daily DAG for one logical date and wait for it to finish."""
    run_id = f"manual__{batch_date.isoformat()}__{datetime.now(UTC):%Y%m%dT%H%M%S}"
    scheduler("airflow", "dags", "unpause", DAG_ID)
    scheduler(
        "airflow",
        "dags",
        "trigger",
        DAG_ID,
        "--logical-date",
        f"{batch_date.isoformat()}T00:00:00+00:00",
        "--run-id",
        run_id,
    )
    deadline = time.monotonic() + AIRFLOW_RUN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        state = scheduler("airflow", "dags", "state", DAG_ID, run_id, capture=True).split()[-1]
        print(f"Airflow run {run_id}: {state}", flush=True)
        if state == "success":
            return
        if state == "failed":
            sys.exit(f"Airflow run {run_id} failed. See http://localhost:8080 for task logs.")
        time.sleep(20)
    sys.exit(f"Airflow run {run_id} did not finish within {AIRFLOW_RUN_TIMEOUT_SECONDS} seconds.")


def cmd_demo(args: argparse.Namespace) -> None:
    """Run the whole story: sources, pipeline, quality report, orchestration, BI."""
    timings: dict[str, float] = {}

    def step(name: str, action, *action_args) -> None:
        print(f"\n=== {name} ===", flush=True)
        started = time.monotonic()
        action(*action_args)
        timings[name] = round(time.monotonic() - started, 1)

    profile = "small" if args.small else "default"
    step("start services", cmd_up, args)
    step("seed sources (day 0)", pipeline_cli, "seed", "--profile", profile)
    step("pipeline: initial full load", pipeline_cli, "run")
    step("simulate 3 more days", pipeline_cli, "advance", "--days", "3")
    step("pipeline: 3 incremental days", pipeline_cli, "run")
    step("data quality and defect report", pipeline_cli, "dq")
    step("simulate 1 more day", pipeline_cli, "advance", "--days", "1")

    status = json.loads(pipeline_cli("status", "--json", capture=True).strip().splitlines()[-1])
    airflow_date = date.fromisoformat(status["latest_simulated_date"])
    step("pipeline: 1 day through Airflow", trigger_airflow_run, airflow_date)
    step("iceberg maintenance", pipeline_cli, "maintain")
    step("provision Metabase", pipeline_cli, "provision-bi")

    REPORTS_DIR.mkdir(exist_ok=True)
    report = {
        "profile": profile,
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "step_seconds": timings,
        "total_seconds": round(sum(timings.values()), 1),
    }
    (REPORTS_DIR / "demo_timings.json").write_text(json.dumps(report, indent=2) + "\n")

    print("\n=== demo finished ===")
    for name, seconds in timings.items():
        print(f"{name:<36} {timedelta(seconds=round(seconds))}")
    print(
        "\nAirflow   http://localhost:8080"
        "\nMetabase  http://localhost:3000  (login is in .env: METABASE_ADMIN_*)"
        "\nMinIO     http://localhost:9001  (login is in .env: S3_ACCESS_KEY / S3_SECRET_KEY)"
        "\nReports   reports/defect_reconciliation.md, reports/demo_timings.json"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("up", help="build the image and start all services").set_defaults(
        handler=cmd_up
    )
    commands.add_parser("down", help="stop all services, keeping their data").set_defaults(
        handler=cmd_down
    )
    commands.add_parser("reset", help="stop services and delete all data").set_defaults(
        handler=cmd_reset
    )

    seed = commands.add_parser("seed", help="recreate the source data as of day 0")
    seed.add_argument("--small", action="store_true", help="use the small profile")
    seed.set_defaults(handler=cmd_seed)

    advance = commands.add_parser("advance", help="simulate more days in the sources")
    advance.add_argument("--days", type=int, required=True)
    advance.set_defaults(handler=cmd_advance)

    run_parser = commands.add_parser("run", help="run the pipeline for pending or given dates")
    run_parser.add_argument("--date", help="logical date, YYYY-MM-DD (default: all pending)")
    run_parser.add_argument("--airflow", action="store_true", help="run through the Airflow DAG")
    run_parser.set_defaults(handler=cmd_run)

    commands.add_parser(
        "dq", help="re-run quality checks and print the defect detection report"
    ).set_defaults(handler=cmd_dq)

    test = commands.add_parser("test", help="run the test suite (extra args go to pytest)")
    test.add_argument("pytest_args", nargs=argparse.REMAINDER)
    test.set_defaults(handler=cmd_test)

    lint = commands.add_parser("lint", help="run ruff lint and format checks")
    lint.add_argument("--fix", action="store_true", help="apply fixes and reformat")
    lint.set_defaults(handler=cmd_lint)

    demo = commands.add_parser("demo", help="run the end-to-end demonstration")
    demo.add_argument("--small", action="store_true", help="use the small profile")
    demo.set_defaults(handler=cmd_demo)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
