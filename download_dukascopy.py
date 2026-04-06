#!/usr/bin/env python3
"""
Download historical OHLC data from Dukascopy for any supported symbol and insert
into TimescaleDB. Dukascopy only serves M1 candle files; higher timeframes are
resampled from M1 before insertion. Processing is done year-by-year to keep
memory usage bounded.

Usage:
    cd data-hub
    .venv/bin/python download_dukascopy.py --symbol XAUUSD
    .venv/bin/python download_dukascopy.py --symbol NAS100 --from 2020-01-01
    .venv/bin/python download_dukascopy.py --symbol EURUSD --from 2020-01-01 --to 2024-12-31 --dry-run
"""

import argparse
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from data_hub.config import settings
from data_hub.database.connection import DatabaseManager
from data_hub.ingestion.batch_processor import BatchProcessor
from data_hub.ingestion.dukascopy_fetcher import (
    INSTRUMENT_MAP,
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

BROKER = "dukascopy"

TIMEFRAMES: list[tuple[str, str | None]] = [
    ("M1", None),
    ("M5", "5min"),
    ("M15", "15min"),
    ("M30", "30min"),
    ("H1", "1h"),
    ("H4", "4h"),
    ("D1", "1D"),
    ("W1", "W"),
    ("MN1", "MS"),
]

DEFAULT_START = date(2000, 1, 1)
WORKERS = 4


def _fetch_one(instrument: str, multiplier: int, day: date) -> tuple[date, list[BarData]]:
    """Fetch and parse a single day. Returns (day, bars) — bars is empty on no data."""
    time.sleep(random.uniform(0, 0.5))
    raw = _fetch_day_raw(instrument, day.year, day.month, day.day)
    bars = _parse_day(raw, day, multiplier) if raw else []
    return day, bars


def download_year(symbol: str, instrument: str, multiplier: int, year: int, start_date: date, end_date: date) -> list[BarData]:
    """Download all M1 bars for a given year in parallel, clipped to [start_date, end_date]."""
    start = max(date(year, 1, 1), start_date)
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
        futures = {pool.submit(_fetch_one, instrument, multiplier, day): day for day in days}
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


def insert_bars(processor: BatchProcessor, bars: list[BarData], symbol: str, timeframe: str, dry_run: bool) -> int:
    """Insert bars into DB, return inserted count."""
    if not bars:
        return 0
    stats = processor.process_bars(iter(bars), symbol, timeframe, BROKER, dry_run=dry_run)
    if stats["total_errors"]:
        logger.warning(f"{timeframe}: {stats['total_errors']} insert errors")
    return stats["total_inserted"]


def process_year(
    year: int,
    start_date: date,
    end_date: date,
    symbol: str,
    instrument: str,
    multiplier: int,
    processor: BatchProcessor,
    dry_run: bool,
) -> None:
    """Download M1, resample to all target timeframes, and insert."""
    m1_bars = download_year(symbol, instrument, multiplier, year, start_date, end_date)
    if not m1_bars:
        logger.info(f"{year}: no bars, skipping")
        return

    for tf, rule in TIMEFRAMES:
        bars = m1_bars if rule is None else _resample_bars(m1_bars, rule)
        inserted = insert_bars(processor, bars, symbol, tf, dry_run)
        logger.info(f"{year} {tf}: {len(bars)} bars → {inserted} inserted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--symbol",
        required=True,
        nargs="+",
        metavar="SYMBOL",
        help=f"One or more symbols to download. Supported: {', '.join(sorted(INSTRUMENT_MAP))}",
    )
    parser.add_argument(
        "--from",
        dest="start",
        default=None,
        metavar="YYYY-MM-DD",
        help=f"First date to download (inclusive). Default: {DEFAULT_START}",
    )
    parser.add_argument(
        "--to",
        dest="end",
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

    try:
        start_date = date.fromisoformat(args.start) if args.start else DEFAULT_START
        end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    except ValueError as e:
        logger.error(f"Invalid date: {e}")
        return 1

    if start_date > end_date:
        logger.error(f"--from {start_date} is after --to {end_date}")
        return 1

    # Validate all symbols up front before touching the DB
    symbols: list[tuple[str, str, int]] = []  # (symbol, instrument, multiplier)
    for raw in args.symbol:
        symbol = raw.upper()
        instrument = INSTRUMENT_MAP.get(symbol)
        if not instrument:
            logger.error(f"Unsupported symbol '{symbol}'. Supported: {', '.join(sorted(INSTRUMENT_MAP))}")
            return 1
        symbols.append((symbol, instrument, POINT_MULTIPLIER[instrument]))

    timeframe_names = [tf for tf, _ in TIMEFRAMES]
    symbol_names = [s for s, _, _ in symbols]
    logger.info(f"Symbols: {symbol_names}  [{start_date} → {end_date}]  broker={BROKER}  dry_run={args.dry_run}")
    logger.info(f"Timeframes: {timeframe_names}")

    start_year = start_date.year
    end_year = end_date.year

    if args.dry_run:
        for symbol, instrument, multiplier in symbols:
            for year in range(start_year, end_year + 1):
                m1_bars = download_year(symbol, instrument, multiplier, year, start_date, end_date)
                for tf, rule in TIMEFRAMES:
                    bars = m1_bars if rule is None else _resample_bars(m1_bars, rule)
                    logger.info(f"{symbol} {year} {tf}: {len(bars)} bars (dry run)")
        return 0

    db = DatabaseManager(settings.database_url)
    if not db.test_connection():
        logger.error("Cannot connect to database")
        return 1

    db.create_tables()
    db.create_hypertable()

    failed: list[str] = []
    with db.get_session() as session:
        processor = BatchProcessor(session, batch_size=10000)
        processor.optimize_for_timeseries()

        for symbol, instrument, multiplier in symbols:
            logger.info(f"--- {symbol} ---")
            for year in range(start_year, end_year + 1):
                try:
                    process_year(year, start_date, end_date, symbol, instrument, multiplier, processor, dry_run=False)
                except KeyboardInterrupt:
                    logger.warning("Interrupted")
                    processor.reset_optimizations()
                    return 1
                except Exception as e:
                    logger.error(f"{symbol} {year} failed: {e}", exc_info=True)
                    failed.append(f"{symbol}/{year}")
                    continue

        processor.reset_optimizations()

    if failed:
        logger.warning(f"Completed with failures: {failed}")
        return 1

    logger.info("Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
