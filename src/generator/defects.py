"""Controlled, reproducible data defects.

Each method damages freshly generated rows in one specific way and returns a
`Defect` per damaged record for the ledger. The injector draws from its own
random stream, so the random choices that shape healthy rows do not depend
on the defect rate.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal

from marketplace.generator.model import Defect
from marketplace.generator.stores import Row

# A late update lands this many minutes before the previous extraction
# watermark: inside the one-hour lookback window, so it must still be caught.
_LATE_MINUTES = (5, 50)
_LATE_SAFETY_MARGIN = timedelta(minutes=55)


class DefectInjector:
    """Injects the defects for one simulated day."""

    def __init__(self, seed: int, day: int, rate: float) -> None:
        self._rng = random.Random(f"{seed}:defects:{day}")
        self._day = max(day, 0)
        self._rate = rate

    def _count(self, population: int) -> int:
        if self._rate <= 0 or population == 0:
            return 0
        return min(population, max(1, round(self._rate * population)))

    def _defect(self, defect_type: str, table: str, key: object, detail: str) -> Defect:
        return Defect(self._day, defect_type, table, str(key), detail)

    def corrupt_orders(
        self,
        orders: list[Row],
        items_by_order: dict[int, list[Row]],
        next_order_id: Callable[[], int],
    ) -> tuple[list[Row], list[Row], list[Defect]]:
        """Damage order headers and items in place; return extra rows and the defects.

        Covers null customer ids, duplicate orders from checkout retries,
        future-dated orders and order items whose order does not exist.
        """
        per_type = self._count(len(orders))
        picks = self._rng.sample(orders, min(len(orders), per_type * 3))
        null_customer, retried, future_dated = (picks[i::3] for i in range(3))

        extra_orders: list[Row] = []
        extra_items: list[Row] = []
        defects: list[Defect] = []

        for order in null_customer:
            order["customer_id"] = None
            defects.append(
                self._defect("null_customer_id", "orders", order["order_id"], "customer_id is null")
            )

        for order in retried:
            retried_at = order["created_at"] + timedelta(seconds=self._rng.randint(1, 3))
            duplicate = {
                **order,
                "order_id": next_order_id(),
                "created_at": retried_at,
                "updated_at": retried_at,
            }
            extra_orders.append(duplicate)
            defects.append(
                self._defect(
                    "duplicate_order",
                    "orders",
                    duplicate["order_id"],
                    f"retry of order {order['order_id']} with the same order_number",
                )
            )

        for order in future_dated:
            new_id = next_order_id()
            order_ts = order["order_ts"] + timedelta(days=self._rng.randint(2, 30))
            extra_orders.append(
                {
                    **order,
                    "order_id": new_id,
                    "order_number": f"MP{order_ts:%y%m%d}-{new_id:08d}",
                    "order_ts": order_ts,
                }
            )
            first_item = items_by_order[order["order_id"]][0]
            extra_items.append({**first_item, "order_item_id": new_id * 10 + 1, "order_id": new_id})
            defects.append(
                self._defect(
                    "future_order_date", "orders", new_id, f"order_ts {order_ts.isoformat()}"
                )
            )

        all_items = [item for items in items_by_order.values() for item in items]
        for item in self._rng.sample(all_items, self._count(len(orders))):
            missing_order_id = next_order_id()
            orphan = {
                **item,
                "order_item_id": missing_order_id * 10 + 1,
                "order_id": missing_order_id,
            }
            extra_items.append(orphan)
            defects.append(
                self._defect(
                    "orphan_order_item",
                    "order_items",
                    orphan["order_item_id"],
                    f"order {missing_order_id} does not exist",
                )
            )
        return extra_orders, extra_items, defects

    def corrupt_payments(self, payments: list[Row]) -> list[Defect]:
        """Set the amount of some payments to zero or to a negative value."""
        defects = []
        for payment in self._rng.sample(payments, self._count(len(payments))):
            payment["amount"] = Decimal("0.00") if self._rng.random() < 0.3 else -payment["amount"]
            defects.append(
                self._defect(
                    "non_positive_amount",
                    "payments",
                    payment["payment_id"],
                    f"amount {payment['amount']}",
                )
            )
        return defects

    def delay_updates(
        self,
        order_updates: list[Row],
        previous_event_at: dict[int, datetime],
        previous_watermark: datetime,
    ) -> list[Defect]:
        """Stamp some status updates with an `updated_at` older than the last watermark.

        Only updates whose previous change is older than the new timestamp are
        eligible, so the late version still carries the row's latest `updated_at`.
        """
        eligible = [
            update
            for update in order_updates
            if previous_event_at[update["order_id"]] < previous_watermark - _LATE_SAFETY_MARGIN
        ]
        defects = []
        for update in self._rng.sample(
            eligible, min(len(eligible), self._count(len(order_updates)))
        ):
            late_at = previous_watermark - timedelta(minutes=self._rng.randint(*_LATE_MINUTES))
            update["updated_at"] = late_at
            if update["order_status"] == "delivered":
                update["delivered_at"] = late_at
            defects.append(
                self._defect(
                    "late_arriving_update",
                    "orders",
                    update["order_id"],
                    f"status {update['order_status']} stamped {late_at.isoformat()}",
                )
            )
        return defects
