"""Loading check declarations and scoring detection against the defect ledger."""

from __future__ import annotations

from pathlib import Path

import pytest

from marketplace.config import Settings
from marketplace.dq.loader import CheckConfigError, load_layer_checks
from marketplace.dq.reconciliation import DefectScore, render_markdown, score_detection


def write_config(tmp_path: Path, layer: str, body: str) -> Path:
    (tmp_path / "dq").mkdir(exist_ok=True)
    (tmp_path / "dq" / f"{layer}.yml").write_text(body, encoding="utf-8")
    return tmp_path


def test_common_checks_are_expanded_for_each_table_with_placeholders(tmp_path):
    config_dir = write_config(
        tmp_path,
        "silver",
        """
layer: silver
common_checks:
  - id: slv_{table}_pk_unique
    type: unique
    columns: ["{key}"]
    severity: error
    threshold: 0
    why: one row per key
  - id: slv_{table}_reconciliation
    type: row_count_reconciliation
    severity: error
    threshold: 0.01
    except_tables: [reviews]
    why: no lost rows
    left_sql: SELECT count(DISTINCT {source_key}) FROM bronze.{table} WHERE b = '{batch_id}'
    right_sql: SELECT count(*) FROM silver.{table}
tables:
  orders:
    key: order_id
    filter: "_batch_id = '{batch_id}'"
    checks:
      - id: slv_orders_total
        type: range
        column: total_amount
        min: 0
        severity: warn
        threshold: 0.05
        why: no negative totals
  reviews:
    key: review_id
    source_key: doc_id
""",
    )

    checks = {check.check_id: check for check in load_layer_checks(config_dir, "silver")}

    assert list(checks) == [
        "slv_orders_pk_unique",
        "slv_orders_reconciliation",
        "slv_orders_total",
        "slv_reviews_pk_unique",
    ]
    assert checks["slv_orders_pk_unique"].params == {"columns": ["order_id"]}
    assert checks["slv_reviews_pk_unique"].params == {"columns": ["review_id"]}
    # Batch placeholders are left for run time; table placeholders are filled now.
    assert checks["slv_orders_reconciliation"].params["left_sql"] == (
        "SELECT count(DISTINCT order_id) FROM bronze.orders WHERE b = '{batch_id}'"
    )
    total = checks["slv_orders_total"]
    assert (total.severity, total.threshold, total.relation) == ("warn", 0.05, "silver.orders")
    assert total.row_filter == "_batch_id = '{batch_id}'"
    assert total.params == {"column": "total_amount", "min": 0}


@pytest.mark.parametrize(
    ("check_body", "message"),
    [
        ("{id: a, type: sorcery, severity: warn, threshold: 0, why: x}", "unknown type"),
        ("{id: a, type: not_null, severity: fatal, threshold: 0, why: x}", "unknown severity"),
        ("{id: a, type: not_null, severity: warn, threshold: 0}", "missing"),
    ],
)
def test_invalid_declarations_are_rejected(tmp_path, check_body, message):
    config_dir = write_config(
        tmp_path,
        "gold",
        f"layer: gold\ntables:\n  t:\n    key: id\n    checks:\n      - {check_body}\n",
    )

    with pytest.raises(CheckConfigError, match=message):
        load_layer_checks(config_dir, "gold")


def test_duplicate_check_ids_are_rejected(tmp_path):
    check = "{id: same, type: not_null, column: id, severity: warn, threshold: 0, why: x}"
    config_dir = write_config(
        tmp_path,
        "gold",
        f"layer: gold\ntables:\n  t:\n    key: id\n    checks:\n      - {check}\n      - {check}\n",
    )

    with pytest.raises(CheckConfigError, match="Duplicate check ids"):
        load_layer_checks(config_dir, "gold")


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_shipped_configuration_is_valid(layer):
    checks = load_layer_checks(Settings.from_env().config_dir, layer)

    assert checks
    assert all(check.why.strip() for check in checks)


@pytest.mark.spark
def test_detection_is_scored_per_defect_type(lake):
    lake.sql(
        "CREATE TABLE silver_quarantine.orders (record_key STRING, rule_id STRING) USING iceberg"
    )
    lake.sql(
        """
        INSERT INTO silver_quarantine.orders VALUES
            ('1', 'ORD_DUPLICATE_NUMBER'), ('2', 'ORD_DUPLICATE_NUMBER'),
            ('7', 'ORD_DUPLICATE_NUMBER'), ('2', 'ORD_FUTURE_DATE')
        """
    )
    lake.sql("CREATE TABLE dq.failed_rows (check_id STRING, record_key STRING) USING iceberg")
    lake.sql("INSERT INTO dq.failed_rows VALUES ('null_check', '5'), ('null_check', '5')")
    detectors = {
        "duplicate_order": {
            "kind": "quarantine",
            "table": "orders",
            "rule_id": "ORD_DUPLICATE_NUMBER",
        },
        "null_customer_id": {"kind": "check", "check_id": "null_check"},
        "schema_drift": {"kind": "schema_change", "change_type": "column_added"},
    }
    injected = {
        "duplicate_order": {"1", "2", "3", "4"},
        "null_customer_id": {"5"},
        "schema_drift": {"order_items.gift_wrap"},
    }

    scores = {s.defect_type: s for s in score_detection(lake, injected, detectors)}

    # Three orders flagged, two of them truly injected, out of four injected.
    duplicate = scores["duplicate_order"]
    assert (duplicate.injected, duplicate.detected, duplicate.true_positives) == (4, 3, 2)
    assert duplicate.precision == pytest.approx(2 / 3)
    assert duplicate.recall == pytest.approx(0.5)
    assert (scores["null_customer_id"].precision, scores["null_customer_id"].recall) == (1.0, 1.0)
    # Nothing logged yet: no detections, so precision is undefined and recall is zero.
    assert (scores["schema_drift"].precision, scores["schema_drift"].recall) == (None, 0.0)


def test_report_table_includes_a_totals_row():
    report = render_markdown([DefectScore("a", 4, 3, 2), DefectScore("b", 1, 0, 0)])

    assert "| a | 4 | 3 | 2 | 66.7% | 50.0% |" in report
    assert "| b | 1 | 0 | 0 | n/a | 0.0% |" in report
    assert "| all types | 5 | 3 | 2 | 66.7% | 40.0% |" in report
