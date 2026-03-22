#!/usr/bin/env python3
"""
Exness tick data ingestion CLI.

Imports Exness tick CSV exports (zip or plain CSV) into the ticks TimescaleDB table.
"""

import argparse
import logging
import sys
from pathlib import Path

from .config import settings
from .database.connection import DatabaseManager
from .database.models import Base
from .ingestion.exness_tick_importer import stream_ticks
from .ingestion.tick_batch_processor import TickBatchProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Exness tick data into TimescaleDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m data_hub.cli_ticks data/Exness_XAUUSDm_2025.zip
  python -m data_hub.cli_ticks data/Exness_XAUUSDm_2025.csv --batch-size 200000
  python -m data_hub.cli_ticks data/Exness_XAUUSDm_2025.zip --dry-run
        """,
    )
    parser.add_argument(
        "file",
        metavar="FILE",
        help="Path to Exness tick export (.zip or .csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="DB insert batch size. Default: 100000",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and count ticks without inserting into the database",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    path = Path(args.file)
    if not path.exists():
        logger.error(f"File not found: {path}")
        return 1

    if args.dry_run:
        count = sum(1 for _ in stream_ticks(path))
        logger.info(f"Dry run: {count:,} ticks found (nothing inserted)")
        return 0

    db_manager = DatabaseManager(settings.database_url)
    if not db_manager.test_connection():
        logger.error("Cannot connect to database")
        return 1

    Base.metadata.create_all(db_manager.engine)
    db_manager.create_tick_hypertable()
    db_manager.setup_tick_compression()

    with db_manager.get_session() as session:
        processor = TickBatchProcessor(session, args.batch_size)
        processor.optimize_for_bulk()
        stats = processor.process_ticks(stream_ticks(path))
        processor.reset_optimizations()

    logger.info(
        f"Done: {stats['total_processed']:,} processed, "
        f"{stats['total_inserted']:,} inserted, "
        f"{stats.get('total_errors', 0)} errors"
    )
    return 0 if stats.get("total_errors", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
