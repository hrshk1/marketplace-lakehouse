"""Order, payment and review rows, materialised from an order's lifecycle plan."""

from __future__ import annotations

import bisect
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from marketplace.generator.entities import money
from marketplace.generator.lifecycle import OrderPlan, PaymentPlan, ReviewPlan, state_at
from marketplace.generator.reference import REVIEW_PHRASES, SELLER_REPLIES
from marketplace.generator.stores import Row

_FREE_SHIPPING_ABOVE = Decimal("499")
_SHIPPING_FEE = Decimal("49.00")
_DISCOUNT_RATES = (0, 0, 0, 5, 10, 15, 20, 30)
_GIFT_WRAP_RATE = 0.08

# Relative order volume by hour of day (UTC): quiet nights, an evening peak.
_HOUR_WEIGHTS = (1, 1, 1, 2, 4, 6, 8, 9, 9, 8, 8, 9, 10, 11, 12, 13, 12, 10, 7, 5, 3, 2, 1, 1)
_HOURS = range(24)


def order_second(rng: random.Random) -> int:
    """Pick the second of the day at which an order is placed."""
    hour = rng.choices(_HOURS, weights=_HOUR_WEIGHTS)[0]
    return hour * 3600 + rng.randint(0, 3599)


@dataclass
class Catalogue:
    """The customers and products an order can be built from on a given day."""

    customers: list[Row]
    products: list[Row]
    # customer_id -> (moved_at, address before the move), for same-day moves.
    moves: dict[int, tuple[datetime, Row]] = field(default_factory=dict)
    _customer_created: list[datetime] = field(init=False)

    def __post_init__(self) -> None:
        self.customers.sort(key=lambda row: (row["created_at"], row["customer_id"]))
        self.products.sort(key=lambda row: row["product_id"])
        self._customer_created = [row["created_at"] for row in self.customers]

    def pick_customer(self, rng: random.Random, order_ts: datetime) -> Row:
        """Pick a customer who already existed at `order_ts`; long-time customers buy more."""
        eligible = max(1, bisect.bisect_right(self._customer_created, order_ts))
        return self.customers[int(eligible * rng.random() ** 1.4)]

    def pick_products(self, rng: random.Random, count: int) -> list[Row]:
        """Pick distinct products; low product ids are the bestsellers."""
        chosen: dict[int, Row] = {}
        while len(chosen) < min(count, len(self.products)):
            product = self.products[int(len(self.products) * rng.random() ** 1.8)]
            chosen[product["product_id"]] = product
        return list(chosen.values())

    def address_at(self, customer: Row, order_ts: datetime) -> Row:
        """Shipping address of a customer at order time, honouring same-day moves."""
        move = self.moves.get(customer["customer_id"])
        return move[1] if move and order_ts < move[0] else customer


def build_order(
    order_id: int,
    order_ts: datetime,
    catalogue: Catalogue,
    rng: random.Random,
    with_gift_wrap: bool,
) -> tuple[Row, list[Row]]:
    """Create an order header (without lifecycle fields) and its line items."""
    customer = catalogue.pick_customer(rng, order_ts)
    address = catalogue.address_at(customer, order_ts)
    item_count = rng.choices((1, 2, 3, 4), weights=(55, 28, 12, 5))[0]

    items: list[Row] = []
    subtotal = Decimal("0")
    for line_no, product in enumerate(catalogue.pick_products(rng, item_count), start=1):
        quantity = rng.choices((1, 2, 3), weights=(80, 15, 5))[0]
        gross = product["price"] * quantity
        discount = money(gross * rng.choice(_DISCOUNT_RATES) / 100)
        subtotal += gross - discount
        item = {
            "order_item_id": order_id * 10 + line_no,
            "order_id": order_id,
            "product_id": product["product_id"],
            "seller_id": product["seller_id"],
            "quantity": quantity,
            "unit_price": product["price"],
            "discount_amount": discount,
            "created_at": order_ts,
            "updated_at": order_ts,
        }
        if with_gift_wrap:
            item["gift_wrap"] = rng.random() < _GIFT_WRAP_RATE
        items.append(item)

    shipping_fee = Decimal("0.00") if subtotal >= _FREE_SHIPPING_ABOVE else _SHIPPING_FEE
    order = {
        "order_id": order_id,
        "order_number": f"MP{order_ts:%y%m%d}-{order_id:08d}",
        "customer_id": customer["customer_id"],
        "order_status": "placed",
        "order_ts": order_ts,
        "total_amount": subtotal + shipping_fee,
        "shipping_fee": shipping_fee,
        "shipping_city": address["city"],
        "shipping_state": address["state"],
        "shipping_pincode": address["pincode"],
        "delivered_at": None,
        "created_at": order_ts,
        "updated_at": order_ts,
    }
    return order, items


def _first_time(events: tuple, status: str, moment: datetime) -> datetime | None:
    return next((e.at for e in events if e.status == status and e.at <= moment), None)


def order_state(plan: OrderPlan, moment: datetime) -> Row:
    """Lifecycle columns of an order as they stand at `moment`."""
    current = state_at(plan.events, moment)
    return {
        "order_status": current.status,
        "delivered_at": _first_time(plan.events, "delivered", moment),
        "updated_at": current.at,
    }


def payment_id_for(order_id: int, attempt_no: int) -> int:
    return order_id * 10 + attempt_no


def payment_state(payment: PaymentPlan, moment: datetime) -> Row:
    """Mutable columns of a payment attempt as they stand at `moment`."""
    current = state_at(payment.events, moment)
    return {
        "payment_status": current.status,
        "paid_at": _first_time(payment.events, "success", moment),
        "updated_at": current.at,
    }


def payment_row(order_id: int, amount: Decimal, payment: PaymentPlan, moment: datetime) -> Row:
    """Full payment row for an attempt that exists at `moment`."""
    payment_id = payment_id_for(order_id, payment.attempt_no)
    state = payment_state(payment, moment)
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_method": payment.method,
        "payment_status": state["payment_status"],
        "amount": amount,
        "attempt_no": payment.attempt_no,
        "gateway_txn_ref": None if payment.method == "COD" else f"TXN{payment_id:014d}",
        "paid_at": state["paid_at"],
        "created_at": payment.created_at,
        "updated_at": state["updated_at"],
    }


def review_id_for(order_id: int) -> str:
    return f"rev-{order_id}"


def seller_reply(review: ReviewPlan, order_id: int) -> Row:
    """The nested reply sub-document a seller adds to a review."""
    return {
        "text": SELLER_REPLIES[order_id % len(SELLER_REPLIES)],
        "replied_at": review.reply_at,
    }


def review_document(
    order_id: int, customer_id: int, product_id: int, review: ReviewPlan, moment: datetime
) -> Row:
    """Review document as it stands at `moment` (with the reply, if already given)."""
    title, text = REVIEW_PHRASES[review.rating][review.phrase_index]
    replied = review.reply_at is not None and review.reply_at <= moment
    return {
        "_id": review_id_for(order_id),
        "order_id": order_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "rating": review.rating,
        "title": title,
        "text": text,
        "helpful_votes": review.helpful_votes,
        "seller_reply": seller_reply(review, order_id) if replied else None,
        "created_at": review.created_at,
        "updated_at": review.reply_at if replied else review.created_at,
    }


def daily_order_count(base: int, day_start: datetime, rng: random.Random) -> int:
    """Orders to place on a day: weekends are busier, with some day-to-day noise."""
    weekend_lift = 1.25 if day_start.weekday() >= 5 else 0.9
    return max(1, round(base * weekend_lift * rng.uniform(0.92, 1.08)))


def random_moment(rng: random.Random, day_start: datetime) -> datetime:
    """A uniformly random second within the day that starts at `day_start`."""
    return day_start + timedelta(seconds=rng.randint(0, 86_399))
