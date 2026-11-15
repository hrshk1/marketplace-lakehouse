"""Every data quality check type, thresholds, persistence and pipeline gating."""

from __future__ import annotations

import pytest

from marketplace.dq.checks import evaluate
from marketplace.dq.model import ERROR, FAILED, PASSED, WARN, WARNED, CheckSpec, DataQualityError
from marketplace.dq.runner import enforce, run_checks
from tests.helpers import batch, rows

pytestmark = pytest.mark.spark

BATCH = batch("2026-06-30")


@pytest.fixture
def shop(lake):
    """A tiny orders table with one of each kind of problem, plus its parent table."""
    lake.sql(
        """
        CREATE TABLE silver.orders (
            order_id BIGINT, customer_id BIGINT, order_status STRING, total_amount DECIMAL(12,2),
            email STRING, updated_at TIMESTAMP, _batch_id STRING
        ) USING iceberg
        """
    )
    lake.sql(
        """
        INSERT INTO silver.orders VALUES
            (1, 10,   'placed',    100.00, 'a@mail.in',  TIMESTAMP '2026-06-30 20:00:00', '2026-06-30'),
            (2, 11,   'shipped',   250.00, 'b@mail.in',  TIMESTAMP '2026-06-30 21:00:00', '2026-06-30'),
            (3, NULL, 'placed',    -5.00,  'not-an-email', TIMESTAMP '2026-06-30 22:00:00', '2026-06-30'),
            (3, 99,   'teleported', 80.00, 'c@mail.in',  TIMESTAMP '2026-06-29 09:00:00', '2026-06-29')
        """
    )
    lake.sql("CREATE TABLE silver.customers (customer_id BIGINT) USING iceberg")
    lake.sql("INSERT INTO silver.customers VALUES (10), (11)")
    return lake


def spec(check_type: str, threshold: float = 0.0, severity: str = ERROR, **params) -> CheckSpec:
    return CheckSpec(
        check_id=f"test_{check_type}",
        layer="silver",
        table="orders",
        check_type=check_type,
        severity=severity,
        threshold=threshold,
        why="test",
        key=("order_id",),
        row_filter=params.pop("row_filter", None),
        params=params,
    )


def failing_keys(shop, check: CheckSpec) -> list[str]:
    evaluation = evaluate(shop, check, BATCH)
    return sorted(row.record_key for row in evaluation.failing.collect())


def test_not_null_reports_rows_with_a_missing_value(shop):
    evaluation = evaluate(shop, spec("not_null", column="customer_id"), BATCH)

    assert (evaluation.failed_rows, evaluation.rows_checked) == (1, 4)
    assert evaluation.observed_value == pytest.approx(0.25)
    assert failing_keys(shop, spec("not_null", column="customer_id")) == ["3"]


def test_unique_reports_every_row_of_a_duplicated_key(shop):
    assert failing_keys(shop, spec("unique", columns=["order_id"])) == ["3", "3"]


def test_accepted_values_ignores_nulls_and_flags_unknown_values(shop):
    check = spec("accepted_values", column="order_status", values=["placed", "shipped"])

    assert failing_keys(shop, check) == ["3"]


def test_relationships_flags_non_null_keys_missing_from_the_parent(shop):
    check = spec(
        "relationships", column="customer_id", to_table="silver.customers", to_column="customer_id"
    )

    evaluation = evaluate(shop, check, BATCH)

    # customer 99 is missing; the NULL customer is not a broken reference.
    assert (evaluation.failed_rows, evaluation.rows_checked) == (1, 4)
    assert failing_keys(shop, check) == ["3"]


def test_range_supports_lower_and_upper_bounds(shop):
    assert failing_keys(shop, spec("range", column="total_amount", min=0)) == ["3"]
    assert failing_keys(shop, spec("range", column="total_amount", min=0, max=200)) == ["2", "3"]


def test_regex_flags_values_that_do_not_match(shop):
    check = spec("regex", column="email", pattern=r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")

    assert failing_keys(shop, check) == ["3"]


def test_custom_sql_uses_the_query_result_and_batch_placeholders(shop):
    check = spec(
        "custom_sql",
        sql=(
            "SELECT CAST(order_id AS STRING) AS record_key FROM silver.orders "
            "WHERE _batch_id = '{batch_id}' AND total_amount > 200"
        ),
    )

    assert failing_keys(shop, check) == ["2"]


def test_row_filter_limits_the_rows_a_check_looks_at(shop):
    check = spec("unique", columns=["order_id"], row_filter="_batch_id = '{batch_id}'")

    evaluation = evaluate(shop, check, BATCH)

    assert (evaluation.failed_rows, evaluation.rows_checked) == (0, 3)


def test_freshness_measures_lag_against_the_end_of_the_batch(shop):
    evaluation = evaluate(shop, spec("freshness", column="updated_at"), BATCH)

    # Newest row is 22:00 and the batch ends at midnight.
    assert evaluation.observed_value == pytest.approx(2.0)


def test_freshness_on_an_empty_table_never_passes(shop):
    shop.sql("DELETE FROM silver.orders")

    result = run_checks(shop, [spec("freshness", threshold=24, column="updated_at")], BATCH)[0]

    assert result.observed_value is None
    assert result.status == FAILED


def test_reconciliation_reports_the_relative_difference(shop):
    check = spec(
        "row_count_reconciliation",
        left_sql="SELECT count(*) FROM silver.orders",
        right_sql="SELECT count(DISTINCT order_id) FROM silver.orders",
    )

    evaluation = evaluate(shop, check, BATCH)

    assert evaluation.observed_value == pytest.approx(0.25)
    assert evaluation.details == "left=4 right=3"


@pytest.mark.parametrize(
    ("severity", "threshold", "expected"),
    [(ERROR, 0.0, FAILED), (WARN, 0.0, WARNED), (ERROR, 0.25, PASSED), (WARN, 0.3, PASSED)],
)
def test_status_depends_on_threshold_and_severity(shop, severity, threshold, expected):
    check = spec("not_null", threshold=threshold, severity=severity, column="customer_id")

    assert run_checks(shop, [check], BATCH)[0].status == expected


def test_results_and_failing_keys_are_written_to_the_dq_tables(shop):
    checks = [
        spec("not_null", severity=WARN, column="customer_id"),
        spec("range", column="total_amount", min=-1000),
    ]

    run_checks(shop, checks, BATCH)

    assert rows(
        shop,
        "SELECT run_id, batch_id, layer, table_name, check_id, check_type, severity, status, "
        "observed_value, threshold, failed_rows, rows_checked FROM dq.check_results ORDER BY check_id",
    ) == [
        ("test-run", "2026-06-30", "silver", "orders", "test_not_null", "not_null", "warn",
         "warn", 0.25, 0.0, 1, 4),
        ("test-run", "2026-06-30", "silver", "orders", "test_range", "range", "error",
         "pass", 0.0, 0.0, 0, 4),
    ]  # fmt: skip
    assert rows(shop, "SELECT check_id, record_key FROM dq.failed_rows") == [("test_not_null", "3")]


def test_enforce_blocks_on_error_failures_but_not_on_warnings(shop):
    warning = run_checks(shop, [spec("not_null", severity=WARN, column="customer_id")], BATCH)
    enforce("silver", warning)

    failure = run_checks(shop, [spec("not_null", severity=ERROR, column="customer_id")], BATCH)
    with pytest.raises(DataQualityError, match="test_not_null"):
        enforce("silver", failure)
