"""The life of one order as a pure function of the seed and the order.

`plan_order` returns every status change an order, its payments and its review
will ever go through. The simulator never stores this plan: it recomputes it
and applies whichever events fall inside the day being simulated. That keeps
`advance --days 2` followed by `--days 3` identical to a single `--days 5`.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from marketplace.generator.reference import PAYMENT_METHODS

TERMINAL_STATUSES = frozenset({"cancelled", "returned"})

# Longest gap between an order being placed and its last possible event
# (refund after a return, or a seller reply to a late review), with margin.
MAX_LIFECYCLE_DAYS = 30

_PAYMENT_FAILURE_RATE = 0.10
_RETRY_FAILURE_RATE = 0.30
_CANCELLATION_RATE = 0.06
_RETURN_RATE = 0.05
_REVIEW_RATE = 0.30
_SELLER_REPLY_RATE = 0.25


@dataclass(frozen=True)
class StatusEvent:
    """A status that takes effect at a point in time."""

    at: datetime
    status: str


@dataclass(frozen=True)
class PaymentPlan:
    """One payment attempt; its first event is the status it is created with."""

    attempt_no: int
    method: str
    events: tuple[StatusEvent, ...]

    @property
    def created_at(self) -> datetime:
        return self.events[0].at


@dataclass(frozen=True)
class ReviewPlan:
    """A customer review and, optionally, when the seller replies to it."""

    created_at: datetime
    rating: int
    helpful_votes: int
    phrase_index: int
    reply_at: datetime | None


@dataclass(frozen=True)
class OrderPlan:
    """Everything that will happen to one order."""

    events: tuple[StatusEvent, ...]
    payments: tuple[PaymentPlan, ...]
    review: ReviewPlan | None


def state_at(events: tuple[StatusEvent, ...], moment: datetime) -> StatusEvent | None:
    """Return the latest event at or before `moment`, or None if nothing happened yet."""
    current = None
    for event in events:
        if event.at > moment:
            break
        current = event
    return current


def changed_between(events: tuple[StatusEvent, ...], after: datetime, until: datetime) -> bool:
    """True when at least one event falls in the half-open window (after, until]."""
    return any(after < event.at <= until for event in events)


def _minutes(rng: random.Random, low: int, high: int) -> timedelta:
    return timedelta(minutes=rng.randint(low, high))


def plan_order(seed: int, order_id: int, order_ts: datetime) -> OrderPlan:
    """Derive the full lifecycle of an order from the generator seed."""
    rng = random.Random(seed * 1_000_003 + order_id)
    method = rng.choices(
        [name for name, _ in PAYMENT_METHODS], weights=[weight for _, weight in PAYMENT_METHODS]
    )[0]
    is_cod = method == "COD"

    events = [StatusEvent(order_ts, "placed")]
    failed_attempts: list[PaymentPlan] = []
    clock = order_ts

    if is_cod:
        payment_events = [StatusEvent(order_ts, "pending")]
        attempt_no = 1
    else:
        clock += timedelta(seconds=rng.randint(20, 180))
        attempt_no = 1
        if rng.random() < _PAYMENT_FAILURE_RATE:
            failed_attempts.append(PaymentPlan(1, method, (StatusEvent(clock, "failed"),)))
            clock += _minutes(rng, 2, 10)
            attempt_no = 2
            if rng.random() < _RETRY_FAILURE_RATE:
                failed_attempts.append(PaymentPlan(2, method, (StatusEvent(clock, "failed"),)))
                events.append(StatusEvent(clock + timedelta(minutes=30), "cancelled"))
                return OrderPlan(tuple(events), tuple(failed_attempts), None)
        events.append(StatusEvent(clock, "paid"))
        payment_events = [StatusEvent(clock, "success")]

    def finish(review: ReviewPlan | None) -> OrderPlan:
        live_payment = PaymentPlan(attempt_no, method, tuple(payment_events))
        return OrderPlan(tuple(events), (*failed_attempts, live_payment), review)

    if rng.random() < _CANCELLATION_RATE:
        cancelled_at = clock + _minutes(rng, 60, 1200)
        events.append(StatusEvent(cancelled_at, "cancelled"))
        if is_cod:
            payment_events.append(StatusEvent(cancelled_at, "failed"))
        else:
            payment_events.append(StatusEvent(cancelled_at + _minutes(rng, 60, 1440), "refunded"))
        return finish(None)

    packed_at = clock + _minutes(rng, 240, 1800)
    shipped_at = packed_at + _minutes(rng, 360, 2160)
    delivered_at = shipped_at + _minutes(rng, 1440, 8640)
    events.extend(
        [
            StatusEvent(packed_at, "packed"),
            StatusEvent(shipped_at, "shipped"),
            StatusEvent(delivered_at, "delivered"),
        ]
    )
    if is_cod:
        payment_events.append(StatusEvent(delivered_at, "success"))

    is_returned = rng.random() < _RETURN_RATE
    if is_returned:
        returned_at = delivered_at + _minutes(rng, 1440, 10080)
        events.append(StatusEvent(returned_at, "returned"))
        payment_events.append(StatusEvent(returned_at + _minutes(rng, 60, 2880), "refunded"))

    review = None
    if rng.random() < _REVIEW_RATE:
        created_at = delivered_at + _minutes(rng, 120, 5760)
        ratings, weights = (
            ([1, 2, 3], [50, 30, 20])
            if is_returned
            else (
                [1, 2, 3, 4, 5],
                [7, 6, 12, 30, 45],
            )
        )
        reply_at = (
            created_at + _minutes(rng, 240, 4320) if rng.random() < _SELLER_REPLY_RATE else None
        )
        review = ReviewPlan(
            created_at=created_at,
            rating=rng.choices(ratings, weights=weights)[0],
            helpful_votes=int(rng.expovariate(0.25)),
            phrase_index=rng.randint(0, 1),
            reply_at=reply_at,
        )
    return finish(review)
