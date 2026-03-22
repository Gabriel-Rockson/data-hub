#!/usr/bin/env python3
"""
Download all available XAUUSD data from Dukascopy (from ~2000 to yesterday)
and insert into TimescaleDB as M1, M5, M15, H1, and D1 bars.

Since Dukascopy only serves M1 candle files, higher timeframes are resampled
from the M1 data before insertion. Processing is done year-by-year to keep
memory usage bounded (~500K bars / year).

Usage:
    cd data-hub
    .venv/bin/python download_xauusd_dukascopy.py
    .venv/bin/python download_xauusd_dukascopy.py --start-year 2020  # resume from a year
    .venv/bin/python download_xauusd_dukascopy.py --dry-run           # fetch but skip DB writes
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from data_hub.config import settings
from data_hub.database.connection import DatabaseManager
from data_hub.ingestion.batch_processor import BatchProcessor
from data_hub.ingestion.dukascopy_fetcher import (
    POINT_MULTIPLIER,
    _fetch_day_raw,
    _parse_day,
    _resample_bars,
)
from data_hub.models.bar_data import BarData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

SYMBOL = "XAUUSD"
INSTRUMENT = "XAUUSD"
BROKER = "dukascopy"

# Only M1 files are available from Dukascopy; the rest are resampled
TIMEFRAMES: list[tuple[str, str | None]] = [
    ("M1", None),
    ("M5", "5min"),
    ("M15", "15min"),
    ("H1", "1h"),
    ("D1", "1D"),
    ("W1", "W"),
    ("MN1", "MS"),
]

# M1 data starts 2003-05-05; D1 starts 1999-06-03 but is derived from M1 here
DATA_START = date(2003, 5, 5)
WORKERS = 8  # concurrent HTTP requests — higher values trigger Dukascopy rate limiting


def _fetch_one(day: date) -> tuple[date, list[BarData]]:
    """Fetch and parse a single day. Returns (day, bars) — bars is empty on no data."""
    multiplier = POINT_MULTIPLIER[INSTRUMENT]
    raw = _fetch_day_raw(INSTRUMENT, day.year, day.month, day.day)
    bars = _parse_day(raw, day, multiplier) if raw else []
    return day, bars


def download_year(year: int, end_date: date) -> list[BarData]:
    """Download all M1 bars for a given year in parallel, up to end_date."""
    start = max(date(year, 1, 1), DATA_START)
    end = min(date(year, 12, 31), end_date)

    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)

    results: dict[date, list[BarData]] = {}
    completed = 0
    last_logged_month = None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_fetch_one, day): day for day in days}
        for future in as_completed(futures):
            day, bars = future.result()
            results[day] = bars
            completed += 1
            if day.day == 1 and day.strftime("%Y-%m") != last_logged_month:
                last_logged_month = day.strftime("%Y-%m")
                done_bars = sum(len(b) for b in results.values())
                logger.info(f"  {last_logged_month}  bars so far: {done_bars:,}  ({completed}/{len(days)} days fetched)")

    m1_bars = []
    for day in sorted(results):
        m1_bars.extend(results[day])

    days_with_data = sum(1 for b in results.values() if b)
    logger.info(f"{year}: {days_with_data}/{len(days)} days had data, {len(m1_bars):,} M1 bars")
    return m1_bars


def insert_bars(
    processor: BatchProcessor,
    bars: list[BarData],
    timeframe: str,
    dry_run: bool,
) -> int:
    """Insert bars into DB, return inserted count."""
    if not bars:
        return 0
    stats = processor.process_bars(iter(bars), SYMBOL, timeframe, BROKER, dry_run=dry_run)
    inserted = stats["total_inserted"]
    errors = stats["total_errors"]
    if errors:
        logger.warning(f"{timeframe}: {errors} insert errors")
    return inserted


def process_year(
    year: int,
    end_date: date,
    processor: BatchProcessor,
    dry_run: bool,
) -> None:
    """Download M1, resample to all target timeframes, and insert."""
    m1_bars = download_year(year, end_date)
    if not m1_bars:
        logger.info(f"{year}: no bars, skipping")
        return

    for tf, rule in TIMEFRAMES:
        if rule is None:
            bars = m1_bars
        else:
            bars = _resample_bars(m1_bars, rule)

        inserted = insert_bars(processor, bars, tf, dry_run)
        logger.info(f"{year} {tf}: {len(bars)} bars → {inserted} inserted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--start-year",
        type=int,
        default=DATA_START.year,
        help=f"First year to download. Default: {DATA_START.year}",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Last date to download (inclusive). Default: yesterday",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and parse data but skip DB inserts",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    end_date = (
        date.fromisoformat(args.end_date)
        if args.end_date
        else date.today() - timedelta(days=1)
    )

    start_year = args.start_year
    end_year = end_date.year

    logger.info(f"Downloading XAUUSD [{start_year} → {end_date}]  broker={BROKER}  dry_run={args.dry_run}")
    logger.info(f"Timeframes: {[tf for tf, _ in TIMEFRAMES]}")

    if args.dry_run:
        logger.info("DRY RUN — no DB writes")
        for year in range(start_year, end_year + 1):
            m1_bars = download_year(year, end_date)
            for tf, rule in TIMEFRAMES:
                bars = m1_bars if rule is None else _resample_bars(m1_bars, rule)
                logger.info(f"{year} {tf}: {len(bars)} bars (dry run)")
        return 0

    db = DatabaseManager(settings.database_url)
    if not db.test_connection():
        logger.error("Cannot connect to database")
        return 1

    db.create_tables()
    db.create_hypertable()

    with db.get_session() as session:
        processor = BatchProcessor(session, batch_size=10000)
        processor.optimize_for_timeseries()

        for year in range(start_year, end_year + 1):
            try:
                process_year(year, end_date, processor, dry_run=False)
            except KeyboardInterrupt:
                logger.warning("Interrupted")
                break
            except Exception as e:
                logger.error(f"Year {year} failed: {e}", exc_info=True)
                continue

        processor.reset_optimizations()

    logger.info("Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
