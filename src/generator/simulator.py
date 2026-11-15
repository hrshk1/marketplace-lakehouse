"""Day-by-day simulation of the marketplace's operational data."""

from __future__ import annotations

import random
from collections import Counter
from datetime import datetime, timedelta

from marketplace.generator import schema
from marketplace.generator.defects import DefectInjector
from marketplace.generator.entities import (
    EntityFactory,
    address_fields,
    money,
    pick_city,
    pick_tier,
)
from marketplace.generator.ledger import Ledger
from marketplace.generator.lifecycle import (
    MAX_LIFECYCLE_DAYS,
    changed_between,
    plan_order,
    state_at,
)
from marketplace.generator.model import DaySummary, Defect, GeneratorConfig
from marketplace.generator.orders import (
    Catalogue,
    build_order,
    daily_order_count,
    order_second,
    order_state,
    payment_id_for,
    payment_row,
    payment_state,
    random_moment,
    review_document,
    review_id_for,
    seller_reply,
)
from marketplace.generator.reference import CITIES
from marketplace.generator.stores import Row, SourceStore
from marketplace.logging_setup import get_logger

log = get_logger("generator")

_CUSTOMER_COLUMNS = ["customer_id", "city", "state", "pincode", "loyalty_tier", "created_at"]
_PRODUCT_COLUMNS = ["product_id", "seller_id", "price", "mrp", "is_active"]

# Daily churn in master data, as a share of the current population.
_NEW_CUSTOMER_RATE = 0.002
_CUSTOMER_MOVE_RATE = 0.004
_TIER_CHANGE_RATE = 0.003
_PRICE_CHANGE_RATE = 0.005
_PRODUCT_RETIRE_RATE = 0.001


class StateMismatchError(RuntimeError):
    """Raised when `advance` is asked to continue a simulation it did not start."""


class MarketplaceSimulator:
    """Creates the initial state (`seed`) and moves it forward in time (`advance`)."""

    def __init__(self, store: SourceStore, ledger: Ledger, config: GeneratorConfig) -> None:
        self._store = store
        self._ledger = ledger
        self._config = config
        self._next_order_id = 0
        self._next_customer_id = 0

    def seed(self) -> DaySummary:
        """Reset the sources and build the state as of the end of day 0."""
        config, profile = self._config, self._config.profile
        self._store.reset()
        self._ledger.reset()
        self._next_order_id = 1
        self._next_customer_id = 1

        rng = random.Random(f"{config.seed}:seed")
        factory = EntityFactory(config.seed, stream=0)
        epoch = config.day_start(0)
        as_of = config.day_end(0)
        summary = DaySummary(0, Counter(), Counter(), Counter())

        categories, leaves = factory.categories(epoch - timedelta(days=800))
        sellers = [
            factory.seller(seller_id, rng, epoch - timedelta(days=rng.randint(200, 800)))
            for seller_id in range(1, profile.sellers + 1)
        ]
        products, attributes = [], []
        for product_id in range(1, profile.products + 1):
            category_id, department, leaf = leaves[rng.randrange(len(leaves))]
            product, document = factory.product(
                product_id,
                rng.randint(1, profile.sellers),
                category_id,
                department,
                leaf,
                rng,
                epoch - timedelta(days=rng.randint(30, 600)),
            )
            products.append(product)
            attributes.append(document)
        customers = [
            self._new_customer(
                factory, rng, as_of - timedelta(seconds=rng.randint(1, 730 * 86_400))
            )
            for _ in range(profile.customers)
        ]
        self._insert(summary, "categories", categories)
        self._insert(summary, "sellers", sellers)
        self._insert(summary, "products", products)
        self._insert(summary, "product_attributes", attributes)
        self._insert(summary, "customers", customers)

        catalogue = Catalogue(customers, products)
        for day in range(1 - profile.history_days, 1):
            count = daily_order_count(profile.orders_per_day, config.day_start(day), rng)
            # Each history day gets its own defect stream, so the seed does not
            # damage the same relative positions on every day.
            injector = DefectInjector(config.seed, day, config.defect_rate)
            self._place_orders(
                day, count, as_of, rng, catalogue, summary, injector, with_gift_wrap=False
            )

        self._save_position(day=0)
        self._ledger.set_state("seed", str(config.seed))
        self._ledger.set_state("profile", profile.name)
        log.info("generator_seeded", extra={"inserted": dict(summary.inserted)})
        return summary

    def advance(self, days: int) -> list[DaySummary]:
        """Simulate `days` more days after the last simulated day."""
        self._load_position()
        first = int(self._ledger.get_state("current_day")) + 1
        return [self._advance_one_day(day) for day in range(first, first + days)]

    def _advance_one_day(self, day: int) -> DaySummary:
        config = self._config
        rng = random.Random(f"{config.seed}:day:{day}")
        factory = EntityFactory(config.seed, stream=day)
        day_start, day_end = config.day_start(day), config.day_end(day)
        summary = DaySummary(day, Counter(), Counter(), Counter())

        if day == config.schema_drift_day:
            self._apply_schema_drift(day, summary)
        with_gift_wrap = self._ledger.get_state("drift_applied") == "1"

        customers = self._store.fetch("customers", _CUSTOMER_COLUMNS)
        products = self._store.fetch("products", _PRODUCT_COLUMNS)
        moves = self._change_customers(customers, rng, day_start, summary)
        products = self._change_products(products, rng, day_start, summary)
        new_customers = [
            self._new_customer(factory, rng, random_moment(rng, day_start))
            for _ in range(max(1, round(len(customers) * _NEW_CUSTOMER_RATE)))
        ]
        self._insert(summary, "customers", new_customers)

        injector = DefectInjector(config.seed, day, config.defect_rate)
        self._progress_open_orders(day_start, day_end, summary, injector)

        catalogue = Catalogue(customers + new_customers, products, moves)
        count = daily_order_count(config.profile.orders_per_day, day_start, rng)
        self._place_orders(day, count, day_end, rng, catalogue, summary, injector, with_gift_wrap)

        self._save_position(day)
        log.info(
            "generator_day_simulated",
            extra={
                "day": day,
                "inserted": dict(summary.inserted),
                "updated": dict(summary.updated),
                "defects": dict(summary.defects),
            },
        )
        return summary

    def _apply_schema_drift(self, day: int, summary: DaySummary) -> None:
        self._store.add_column(schema.DRIFT_TABLE, schema.DRIFT_COLUMN, schema.DRIFT_SPARK_TYPE)
        self._ledger.set_state("drift_applied", "1")
        self._record(
            summary,
            [
                Defect(
                    day,
                    "schema_drift",
                    schema.DRIFT_TABLE,
                    f"{schema.DRIFT_TABLE}.{schema.DRIFT_COLUMN}",
                    f"ALTER TABLE ADD COLUMN {schema.DRIFT_COLUMN} {schema.DRIFT_SPARK_TYPE}",
                )
            ],
        )

    def _change_customers(
        self, customers: list[Row], rng: random.Random, day_start: datetime, summary: DaySummary
    ) -> dict[int, tuple[datetime, Row]]:
        """Move some customers to another city and change some loyalty tiers."""
        ordered = sorted(customers, key=lambda row: row["customer_id"])
        move_count = max(2, round(len(ordered) * _CUSTOMER_MOVE_RATE))
        tier_count = max(2, round(len(ordered) * _TIER_CHANGE_RATE))
        picks = rng.sample(ordered, min(len(ordered), move_count + tier_count))

        moves: dict[int, tuple[datetime, Row]] = {}
        move_updates, tier_updates = [], []
        for customer in picks[:move_count]:
            moved_at = random_moment(rng, day_start)
            previous = {key: customer[key] for key in ("city", "state", "pincode")}
            customer.update(
                address_fields(rng, pick_city(rng, exclude=_city_of(previous["pincode"])))
            )
            moves[customer["customer_id"]] = (moved_at, previous)
            move_updates.append(
                {
                    "customer_id": customer["customer_id"],
                    "city": customer["city"],
                    "state": customer["state"],
                    "pincode": customer["pincode"],
                    "updated_at": moved_at,
                }
            )
        for customer in picks[move_count:]:
            customer["loyalty_tier"] = pick_tier(rng, exclude=customer["loyalty_tier"])
            tier_updates.append(
                {
                    "customer_id": customer["customer_id"],
                    "loyalty_tier": customer["loyalty_tier"],
                    "updated_at": random_moment(rng, day_start),
                }
            )
        self._update(summary, "customers", move_updates)
        self._update(summary, "customers", tier_updates)
        return moves

    def _change_products(
        self, products: list[Row], rng: random.Random, day_start: datetime, summary: DaySummary
    ) -> list[Row]:
        """Reprice some products and soft-delete a few; return the ones still on sale."""
        active = sorted(
            (row for row in products if row["is_active"]), key=lambda row: row["product_id"]
        )
        reprice_count = max(1, round(len(active) * _PRICE_CHANGE_RATE))
        retire_count = max(1, round(len(active) * _PRODUCT_RETIRE_RATE))
        picks = rng.sample(active, min(len(active), reprice_count + retire_count))

        price_updates, retire_updates = [], []
        for product in picks[:reprice_count]:
            repriced = money(round(float(product["price"]) * rng.uniform(0.85, 1.1)))
            product["price"] = max(money(1), min(repriced, product["mrp"]))
            price_updates.append(
                {
                    "product_id": product["product_id"],
                    "price": product["price"],
                    "updated_at": random_moment(rng, day_start),
                }
            )
        retired_ids = set()
        for product in picks[reprice_count:]:
            retired_at = random_moment(rng, day_start)
            retired_ids.add(product["product_id"])
            retire_updates.append(
                {
                    "product_id": product["product_id"],
                    "is_active": False,
                    "deleted_at": retired_at,
                    "updated_at": retired_at,
                }
            )
        self._update(summary, "products", price_updates)
        self._update(summary, "products", retire_updates)
        return [product for product in active if product["product_id"] not in retired_ids]

    def _progress_open_orders(
        self,
        day_start: datetime,
        day_end: datetime,
        summary: DaySummary,
        injector: DefectInjector,
    ) -> None:
        """Apply every lifecycle event of existing orders that falls inside this day."""
        cutoff = day_start - timedelta(days=MAX_LIFECYCLE_DAYS)
        open_orders = self._store.fetch(
            "orders", ["order_id", "order_ts", "customer_id", "total_amount"], "order_ts", cutoff
        )
        first_product: dict[int, int] = {}
        for item in sorted(
            self._store.fetch(
                "order_items", ["order_item_id", "order_id", "product_id"], "created_at", cutoff
            ),
            key=lambda row: row["order_item_id"],
        ):
            first_product.setdefault(item["order_id"], item["product_id"])
        frozen = self._ledger.frozen_order_ids()

        order_updates, payment_inserts, payment_updates = [], [], []
        review_inserts, review_updates = [], []
        previous_event_at: dict[int, datetime] = {}

        for order in sorted(open_orders, key=lambda row: row["order_id"]):
            order_id = order["order_id"]
            if order_id in frozen:
                continue
            plan = plan_order(self._config.seed, order_id, order["order_ts"])

            if changed_between(plan.events, day_start, day_end):
                order_updates.append({"order_id": order_id, **order_state(plan, day_end)})
                previous_event_at[order_id] = state_at(plan.events, day_start).at

            for payment in plan.payments:
                if day_start < payment.created_at <= day_end:
                    payment_inserts.append(
                        payment_row(order_id, order["total_amount"], payment, day_end)
                    )
                elif changed_between(payment.events, day_start, day_end):
                    payment_updates.append(
                        {
                            "payment_id": payment_id_for(order_id, payment.attempt_no),
                            **payment_state(payment, day_end),
                        }
                    )

            review = plan.review
            if review is None or order["customer_id"] is None:
                continue
            if day_start < review.created_at <= day_end:
                review_inserts.append(
                    review_document(
                        order_id, order["customer_id"], first_product[order_id], review, day_end
                    )
                )
            elif review.reply_at is not None and day_start < review.reply_at <= day_end:
                review_updates.append(
                    {
                        "_id": review_id_for(order_id),
                        "seller_reply": seller_reply(review, order_id),
                        "updated_at": review.reply_at,
                    }
                )

        self._record(summary, injector.delay_updates(order_updates, previous_event_at, day_start))

        self._update(summary, "orders", order_updates)
        self._insert(summary, "payments", payment_inserts)
        self._update(summary, "payments", payment_updates)
        self._insert(summary, "reviews", review_inserts)
        self._update(summary, "reviews", review_updates)

    def _place_orders(
        self,
        day: int,
        count: int,
        as_of: datetime,
        rng: random.Random,
        catalogue: Catalogue,
        summary: DaySummary,
        injector: DefectInjector,
        with_gift_wrap: bool,
    ) -> None:
        """Create the orders placed on `day`, with their state as it stands at `as_of`."""
        day_start = self._config.day_start(day)
        orders: list[Row] = []
        items_by_order: dict[int, list[Row]] = {}
        for _ in range(count):
            order_ts = day_start + timedelta(seconds=order_second(rng))
            order, items = build_order(
                self._take_order_id(), order_ts, catalogue, rng, with_gift_wrap
            )
            orders.append(order)
            items_by_order[order["order_id"]] = items

        extra_orders, extra_items, defects = injector.corrupt_orders(
            orders, items_by_order, self._take_order_id
        )

        payments, reviews = [], []
        for order in orders:
            plan = plan_order(self._config.seed, order["order_id"], order["order_ts"])
            order.update(order_state(plan, as_of))
            payments.extend(
                payment_row(order["order_id"], order["total_amount"], payment, as_of)
                for payment in plan.payments
                if payment.created_at <= as_of
            )
            review = plan.review
            if review and review.created_at <= as_of and order["customer_id"] is not None:
                first_item = items_by_order[order["order_id"]][0]
                reviews.append(
                    review_document(
                        order["order_id"],
                        order["customer_id"],
                        first_item["product_id"],
                        review,
                        as_of,
                    )
                )
        defects.extend(injector.corrupt_payments(payments))
        self._record(summary, defects)

        all_items = [item for items in items_by_order.values() for item in items]
        self._insert(summary, "orders", orders + extra_orders)
        self._insert(summary, "order_items", all_items + extra_items)
        self._insert(summary, "payments", payments)
        self._insert(summary, "reviews", reviews)

    def _new_customer(
        self, factory: EntityFactory, rng: random.Random, created_at: datetime
    ) -> Row:
        customer_id = self._next_customer_id
        self._next_customer_id += 1
        return factory.customer(customer_id, rng, created_at)

    def _take_order_id(self) -> int:
        order_id = self._next_order_id
        self._next_order_id += 1
        return order_id

    def _insert(self, summary: DaySummary, table: str, rows: list[Row]) -> None:
        self._store.insert(table, rows)
        summary.inserted[table] += len(rows)

    def _update(self, summary: DaySummary, table: str, rows: list[Row]) -> None:
        self._store.update(table, rows)
        summary.updated[table] += len(rows)

    def _record(self, summary: DaySummary, defects: list[Defect]) -> None:
        self._ledger.record(defects)
        summary.defects.update(defect.defect_type for defect in defects)

    def _save_position(self, day: int) -> None:
        self._ledger.set_state("current_day", str(day))
        self._ledger.set_state("next_order_id", str(self._next_order_id))
        self._ledger.set_state("next_customer_id", str(self._next_customer_id))

    def _load_position(self) -> None:
        stored_seed = self._ledger.get_state("seed")
        if stored_seed is None:
            raise StateMismatchError("No seeded simulation found. Run `seed` first.")
        if int(stored_seed) != self._config.seed:
            raise StateMismatchError(
                f"The ledger was seeded with seed {stored_seed}, "
                f"but the current configuration uses seed {self._config.seed}."
            )
        self._next_order_id = int(self._ledger.get_state("next_order_id"))
        self._next_customer_id = int(self._ledger.get_state("next_customer_id"))


def _city_of(pincode: str) -> str | None:
    """Canonical city name for a pincode, so a move never lands in the same city."""
    return next((city.name for city in CITIES if pincode.startswith(city.pincode_prefix)), None)
