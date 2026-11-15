"""Builders for master data: categories, sellers, customers and products."""

from __future__ import annotations

import math
import random
import re
from datetime import datetime
from decimal import Decimal

from faker import Faker

from marketplace.generator.reference import (
    CITIES,
    DEPARTMENTS,
    LOYALTY_TIERS,
    City,
    Department,
    Leaf,
)
from marketplace.generator.stores import Row

_CITY_WEIGHTS = [city.weight for city in CITIES]
_TIER_NAMES = [name for name, _ in LOYALTY_TIERS]
_TIER_WEIGHTS = [weight for _, weight in LOYALTY_TIERS]
_PRODUCT_LINES = ("Classic", "Pro", "Lite", "Max", "Essential", "Prime")

# Share of address fields written the way real applications leave them:
# inconsistent casing, stray spaces, former city names, state codes.
_ADDRESS_NOISE_RATE = 0.10


def money(value: float | Decimal) -> Decimal:
    """Round to paise, the precision of every monetary column."""
    return Decimal(value).quantize(Decimal("0.01"))


def pick_city(rng: random.Random, exclude: str | None = None) -> City:
    """Pick a city weighted by market size, optionally avoiding one."""
    while True:
        city = rng.choices(CITIES, weights=_CITY_WEIGHTS)[0]
        if city.name != exclude:
            return city


def pick_tier(rng: random.Random, exclude: str | None = None) -> str:
    """Pick a loyalty tier, optionally different from the current one."""
    while True:
        tier = rng.choices(_TIER_NAMES, weights=_TIER_WEIGHTS)[0]
        if tier != exclude:
            return tier


def address_fields(rng: random.Random, city: City) -> Row:
    """Return city, state and pincode as an application would have stored them."""
    name, state = city.name, city.state
    if rng.random() < _ADDRESS_NOISE_RATE:
        variant = rng.randint(0, 4)
        if variant == 0:
            name = name.upper()
        elif variant == 1:
            name = f"  {name.lower()} "
        elif variant == 2 and city.former_name:
            name = city.former_name
        elif variant == 3:
            state = city.state_code
        else:
            state = state.lower()
    return {
        "city": name,
        "state": state,
        "pincode": f"{city.pincode_prefix}{rng.randint(0, 999):03d}",
    }


class EntityFactory:
    """Creates master-data rows. `stream` separates the seed from each simulated day."""

    def __init__(self, seed: int, stream: int) -> None:
        self._fake = Faker("en_IN")
        self._fake.seed_instance(seed * 10_000 + stream)

    def categories(
        self, created_at: datetime
    ) -> tuple[list[Row], list[tuple[int, Department, Leaf]]]:
        """Return category rows and, for each leaf, (category_id, department, leaf)."""
        rows: list[Row] = []
        leaves: list[tuple[int, Department, Leaf]] = []
        next_id = 1
        for department in DEPARTMENTS:
            parent_id = next_id
            rows.append(self._category(parent_id, department.name, None, created_at))
            next_id += 1
            for leaf in department.leaves:
                rows.append(self._category(next_id, leaf.name, parent_id, created_at))
                leaves.append((next_id, department, leaf))
                next_id += 1
        return rows, leaves

    @staticmethod
    def _category(category_id: int, name: str, parent_id: int | None, created_at: datetime) -> Row:
        return {
            "category_id": category_id,
            "category_name": name,
            "parent_category_id": parent_id,
            "created_at": created_at,
            "updated_at": created_at,
        }

    def seller(self, seller_id: int, rng: random.Random, created_at: datetime) -> Row:
        city = pick_city(rng)
        pan_letters = "".join(rng.choice("ABCDEFGHJKLMNPRSTUVWXYZ") for _ in range(5))
        gstin = (
            f"{rng.randint(1, 37):02d}{pan_letters}{seller_id % 10_000:04d}F1Z{rng.randint(0, 9)}"
        )
        return {
            "seller_id": seller_id,
            "seller_name": self._fake.company(),
            "gstin": gstin,
            "city": city.name,
            "state": city.state,
            "rating": Decimal(rng.randint(28, 49)) / 10,
            "is_active": True,
            "created_at": created_at,
            "updated_at": created_at,
        }

    def customer(self, customer_id: int, rng: random.Random, created_at: datetime) -> Row:
        full_name = self._fake.name()
        handle = re.sub(r"[^a-z]+", ".", full_name.lower()).strip(".")
        return {
            "customer_id": customer_id,
            "full_name": full_name,
            "email": f"{handle}.{customer_id}@{self._fake.free_email_domain()}",
            "phone": f"+91{rng.randint(6_000_000_000, 9_999_999_999)}",
            **address_fields(rng, pick_city(rng)),
            "loyalty_tier": pick_tier(rng),
            "created_at": created_at,
            "updated_at": created_at,
        }

    @staticmethod
    def product(
        product_id: int,
        seller_id: int,
        category_id: int,
        department: Department,
        leaf: Leaf,
        rng: random.Random,
        created_at: datetime,
    ) -> tuple[Row, Row]:
        """Return the relational product row and its schemaless attribute document."""
        brand = rng.choice(department.brands)
        price = money(
            round(math.exp(rng.uniform(math.log(leaf.min_price), math.log(leaf.max_price))))
        )
        row = {
            "product_id": product_id,
            "seller_id": seller_id,
            "category_id": category_id,
            "product_name": f"{brand} {leaf.name} {rng.choice(_PRODUCT_LINES)} "
            f"{rng.randint(100, 999)}",
            "mrp": money(round(float(price) * rng.uniform(1.05, 1.6))),
            "price": price,
            "is_active": True,
            "deleted_at": None,
            "created_at": created_at,
            "updated_at": created_at,
        }
        # Attributes are sparse on purpose: sellers fill in different fields.
        attributes = {"brand": brand}
        for name, values in department.attributes.items():
            if rng.random() < 0.8:
                attributes[name] = rng.choice(values)
        document = {
            "_id": f"prod-{product_id}",
            "product_id": product_id,
            "department": department.name,
            "attributes": attributes,
            "created_at": created_at,
            "updated_at": created_at,
        }
        return row, document
