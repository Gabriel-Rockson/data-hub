#!/usr/bin/env python3
"""
Data cleaning CLI for TimescaleDB market data.

Removes synthetic/placeholder bars that data providers insert for weekends
and daily server rollovers. Currently handles Dukascopy CSV export artifacts.

Safe to run repeatedly — all operations are idempotent.
"""

import argparse
import logging
import sys

from sqlalchemy import text
from sqlalchemy.orm import Session

from .config import settings
from .database.connection import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# Brokers whose CSV exports are known to include synthetic placeholder bars.
# Dukascopy pads every calendar day (including weekends) with flat zero-volume
# records and inserts additional flat bars during their daily rollover (~21:00-23:59 UTC).
SYNTHETIC_BAR_BROKERS: set[str] = {"dukascopy"}


def _build_where(broker: str, symbol: str | None, timeframe: str | None) -> tuple[str, dict]:
    clause = "broker = :broker"
    params: dict = {"broker": broker}
    if symbol:
        clause += " AND symbol = :symbol"
        params["symbol"] = symbol
    if timeframe:
        clause += " AND timeframe = :timeframe"
        params["timeframe"] = timeframe
    return clause, params


def count_dirty_bars(session: Session, broker: str, symbol: str | None, timeframe: str | None) -> tuple[int, int]:
    """Return (weekend_count, flat_weekday_count) without modifying any data."""
    where, params = _build_where(broker, symbol, timeframe)

    weekend = session.execute(
        text(f"SELECT COUNT(*) FROM market_data WHERE {where} AND EXTRACT(DOW FROM time) IN (0, 6)"),
        params,
    ).scalar()

    flat = session.execute(
        text(
            f"SELECT COUNT(*) FROM market_data WHERE {where}"
            " AND EXTRACT(DOW FROM time) BETWEEN 1 AND 5"
            " AND volume = 0 AND high = low AND open = high"
        ),
        params,
    ).scalar()

    return int(weekend), int(flat)


def clean_broker(
    session: Session,
    broker: str,
    symbol: str | None,
    timeframe: str | None,
    dry_run: bool,
) -> tuple[int, int]:
    """
    Delete synthetic bars for a broker.

    Returns (weekend_deleted, flat_weekday_deleted).
    """
    where, params = _build_where(broker, symbol, timeframe)
    label = broker + (f"/{symbol}" if symbol else "") + (f"/{timeframe}" if timeframe else "")

    weekend_count, flat_count = count_dirty_bars(session, broker, symbol, timeframe)

    if dry_run:
        logger.info(
            f"[dry-run] {label}: {weekend_count:,} weekend bars, "
            f"{flat_count:,} flat weekday bars would be removed"
        )
        return weekend_count, flat_count

    weekend_deleted = 0
    flat_deleted = 0

    if weekend_count > 0:
        r = session.execute(
            text(f"DELETE FROM market_data WHERE {where} AND EXTRACT(DOW FROM time) IN (0, 6)"),
            params,
        )
        weekend_deleted = r.rowcount
        logger.info(f"{label}: removed {weekend_deleted:,} weekend bars")
    else:
        logger.info(f"{label}: no weekend bars found")

    if flat_count > 0:
        r = session.execute(
            text(
                f"DELETE FROM market_data WHERE {where}"
                " AND EXTRACT(DOW FROM time) BETWEEN 1 AND 5"
                " AND volume = 0 AND high = low AND open = high"
            ),
            params,
        )
        flat_deleted = r.rowcount
        logger.info(f"{label}: removed {flat_deleted:,} flat weekday bars")
    else:
        logger.info(f"{label}: no flat weekday bars found")

    return weekend_deleted, flat_deleted


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Remove synthetic placeholder bars from market_data. "
            "Idempotent — safe to run repeatedly."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Cleans two categories of bad bars inserted by Dukascopy's CSV export tool:
  1. Weekend bars       — Saturday/Sunday flat bars with zero range and zero volume
  2. Flat weekday bars  — daily server-rollover placeholders (~21:00-23:59 UTC)

Examples:
  python -m data_hub.cli_clean --dry-run
  python -m data_hub.cli_clean
  python -m data_hub.cli_clean --symbol XAUUSD --dry-run
  python -m data_hub.cli_clean --symbol XAUUSD --timeframe M5
        """,
    )
    parser.add_argument(
        "--broker",
        default=None,
        help=f"Limit to this broker (default: all synthetic-bar brokers: {sorted(SYNTHETIC_BAR_BROKERS)})",
    )
    parser.add_argument("--symbol", default=None, help="Limit to this symbol (default: all)")
    parser.add_argument("--timeframe", default=None, help="Limit to this timeframe (default: all)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without modifying the database",
    )
    args = parser.parse_args()

    brokers = [args.broker] if args.broker else sorted(SYNTHETIC_BAR_BROKERS)

    db_manager = DatabaseManager(settings.database_url)
    if not db_manager.test_connection():
        logger.error("Cannot connect to database")
        return 1

    total_weekend = 0
    total_flat = 0

    with db_manager.get_session() as session:
        for broker in brokers:
            w, f = clean_broker(session, broker, args.symbol, args.timeframe, dry_run=args.dry_run)
            total_weekend += w
            total_flat += f

    action = "would remove" if args.dry_run else "removed"
    logger.info(
        f"Complete — {action} {total_weekend:,} weekend bars "
        f"and {total_flat:,} flat weekday bars"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
