"""Command line entry point that runs inside the project image.

    python -m marketplace.cli <command>

`scripts/dev.py` wraps these commands for use from the host.
"""

from __future__ import annotations

import argparse

from marketplace.config import Settings
from marketplace.generator.ledger import Ledger
from marketplace.generator.model import PROFILES, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator
from marketplace.generator.stores import PostgresMongoStore
from marketplace.logging_setup import configure_logging


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketplace", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    seed = commands.add_parser("seed", help="recreate the source data as of day 0")
    seed.add_argument("--profile", choices=sorted(PROFILES), default="default")
    seed.set_defaults(handler=cmd_seed)

    advance = commands.add_parser("advance", help="simulate more days in the sources")
    advance.add_argument("--days", type=int, required=True)
    advance.set_defaults(handler=cmd_advance)
    return parser


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()
    args.handler(args, Settings.from_env())


if __name__ == "__main__":
    main()
