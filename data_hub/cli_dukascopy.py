#!/usr/bin/env python3
"""
Dukascopy historical data ingestion CLI.

Downloads OHLC candle data from Dukascopy's public datafeed and inserts
into TimescaleDB under broker name "dukascopy".
"""

import argparse
import logging
import sys
from datetime import date

from .config import settings
from .database.connection import DatabaseManager
from .ingestion.batch_processor import BatchProcessor
from .ingestion.dukascopy_fetcher import INSTRUMENT_MAP, RESAMPLE_RULES, fetch_bars

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

BROKER = "dukascopy"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest Dukascopy OHLC data into TimescaleDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported symbols: {", ".join(sorted(INSTRUMENT_MAP))}
Supported timeframes: {", ".join(sorted(RESAMPLE_RULES))}

Examples:
  python -m data_hub.cli_dukascopy --symbol XAUUSD --from 2020-01-01 --to 2024-12-31 --timeframe M1
  python -m data_hub.cli_dukascopy --symbol NAS100 --from 2023-01-01 --to 2023-12-31 --timeframe D1
  python -m data_hub.cli_dukascopy --symbol EURUSD --from 2020-01-01 --to 2020-12-31 --timeframe M5 --dry-run
        """,
    )
    parser.add_argument(
        "--symbol",
        required=True,
        metavar="SYMBOL",
        help="Symbol to fetch (e.g. XAUUSD, NAS100, EURUSD)",
    )
    parser.add_argument(
        "--from",
        dest="start",
        required=True,
        metavar="YYYY-MM-DD",
        help="Start date (inclusive)",
    )
    parser.add_argument(
        "--to",
        dest="end",
        required=True,
        metavar="YYYY-MM-DD",
        help="End date (inclusive)",
    )
    parser.add_argument(
        "--timeframe",
        default="M1",
        metavar="TF",
        help="Target timeframe (M1, M5, M15, H1, D1, ...). Default: M1",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="DB insert batch size. Default: 10000",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        metavar="SECONDS",
        help="Seconds between HTTP requests. Default: 0.1",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and count bars without inserting into the database",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as e:
        logger.error(f"Invalid date: {e}")
        return 1

    if start > end:
        logger.error("--from must be before --to")
        return 1

    symbol = args.symbol.upper()
    timeframe = args.timeframe.upper()

    logger.info(f"Fetching {symbol} {timeframe} [{start} → {end}]  broker={BROKER}")

    bars = fetch_bars(symbol, start, end, timeframe, request_delay=args.delay)

    if args.dry_run:
        count = sum(1 for _ in bars)
        logger.info(f"Dry run complete: {count} bars found (nothing inserted)")
        return 0

    db_manager = DatabaseManager(settings.database_url)
    if not db_manager.test_connection():
        logger.error("Cannot connect to database")
        return 1

    db_manager.create_tables()
    db_manager.create_hypertable()

    with db_manager.get_session() as session:
        processor = BatchProcessor(session, args.batch_size)
        processor.optimize_for_timeseries()
        stats = processor.process_bars(bars, symbol, timeframe, BROKER)
        processor.reset_optimizations()

    skipped = stats.get("total_skipped", 0)
    skipped_msg = f", {skipped:,} synthetic bars excluded" if skipped else ""
    logger.info(
        f"Done: {stats['total_processed']:,} processed, "
        f"{stats['total_inserted']:,} inserted, "
        f"{stats['total_errors']} errors{skipped_msg}"
    )
    return 0 if stats["total_errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
