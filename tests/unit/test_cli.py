"""The in-container command line: run, status and the quality report."""

from __future__ import annotations

import dataclasses
import json
from argparse import Namespace

import pytest

from marketplace import cli
from marketplace.generator.ledger import Ledger
from marketplace.generator.model import PROFILES, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator
from marketplace.generator.stores import MemoryStore
from tests.conftest import use_fresh_catalog
from tests.helpers import memory_context

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def simulated(spark, settings, tmp_path_factory):
    """A seeded in-memory marketplace wired into the CLI in place of the real sources."""
    base = tmp_path_factory.mktemp("cli")
    cli_settings = dataclasses.replace(
        settings, ledger_path=base / "ledger.sqlite", reports_dir=base / "reports"
    )
    use_fresh_catalog(spark, cli_settings, base / "warehouse")
    store = MemoryStore()
    with Ledger(cli_settings.ledger_path) as ledger:
        ledger.set_state("profile", "small")
        config = GeneratorConfig.from_settings(cli_settings, PROFILES["small"])
        MarketplaceSimulator(store, ledger, config).seed()

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(cli, "open_context", lambda _: memory_context(spark, cli_settings, store))
        yield cli_settings


def test_parser_exposes_every_command():
    parser = cli.build_parser()

    assert parser.parse_args(["seed", "--profile", "small"]).profile == "small"
    assert parser.parse_args(["advance", "--days", "3"]).days == 3
    assert parser.parse_args(["run", "--date", "2026-07-01"]).date == "2026-07-01"
    assert parser.parse_args(["status", "--json"]).json is True
    for command in ("dq", "maintain", "provision-bi"):
        assert parser.parse_args([command]).handler is not None
    with pytest.raises(SystemExit):
        parser.parse_args(["seed", "--profile", "enormous"])


def test_status_before_any_run_lists_day_zero_as_pending(simulated, capsys):
    cli.cmd_status(Namespace(json=True), simulated)

    assert json.loads(capsys.readouterr().out) == {
        "latest_simulated_date": "2026-06-30",
        "latest_completed_batch": None,
        "pending_dates": ["2026-06-30"],
    }


def test_dq_refuses_to_report_before_a_batch_has_completed(simulated):
    with pytest.raises(SystemExit, match="No completed batch"):
        cli.cmd_dq(Namespace(), simulated)


def test_run_processes_pending_dates_then_reports_nothing_left(simulated, capsys):
    cli.cmd_run(Namespace(date=None), simulated)
    assert "Batch 2026-06-30 completed" in capsys.readouterr().out

    cli.cmd_run(Namespace(date=None), simulated)
    assert "Nothing to run" in capsys.readouterr().out

    cli.cmd_status(Namespace(json=False), simulated)
    assert "latest_completed_batch: 2026-06-30" in capsys.readouterr().out


def test_dq_prints_check_summary_and_writes_the_defect_report(simulated, capsys):
    cli.cmd_dq(Namespace(), simulated)

    output = capsys.readouterr().out
    assert "Quality checks for batch 2026-06-30" in output
    assert "[warn] brz_orders_customer_id_not_null" in output
    assert "silver: 47 checks, 47 passed" in output
    assert "| duplicate_order |" in output
    report = json.loads((simulated.reports_dir / "defect_reconciliation.json").read_text())
    scored = {entry["defect_type"]: entry for entry in report}
    # Late updates and schema drift only start on day 1, so day 0 has none to find.
    for defect_type in ("duplicate_order", "future_order_date", "orphan_order_item"):
        assert scored[defect_type]["injected"] > 0
        assert scored[defect_type]["recall"] == 1.0
        assert scored[defect_type]["precision"] == 1.0
    assert scored["late_arriving_update"]["injected"] == 0
    assert (simulated.reports_dir / "defect_reconciliation.md").exists()
