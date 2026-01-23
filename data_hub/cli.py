#!/usr/bin/env python3
"""
Historical Data Ingestion Script

Ingest MetaTrader CSV files into TimescaleDB with validation and progress tracking.
Supports both single files and directory batch processing.
"""

import argparse
import itertools
import logging
import sys
import time
from pathlib import Path
from typing import Any

from .config import settings
from .database.connection import DatabaseManager
from .ingestion.batch_processor import BatchProcessor
from .ingestion.csv_parser import MetaTraderCSVParser
from .ingestion.validator import DataValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("ingestion.log")],
)
logger = logging.getLogger(__name__)


class IngestionProgress:
    """Track and display ingestion progress"""

    def __init__(self):
        self.start_time = time.time()
        self.total_files = 0
        self.processed_files = 0
        self.total_records = 0
        self.processed_records = 0
        self.errors = 0
        self.file_stats = []

    def start_file(self, filename: str, estimated_records: int = 0):
        """Mark the start of processing a file"""
        self.current_file = filename
        self.file_start_time = time.time()
        self.current_file_records = 0
        if estimated_records:
            self.total_records += estimated_records

        logger.info(f"Starting file {self.processed_files + 1}/{self.total_files}: {filename}")

    def update_progress(self, records_processed: int):
        """Update progress with number of records processed"""
        self.current_file_records += records_processed
        self.processed_records += records_processed

        if self.processed_records % 10000 == 0:
            elapsed = time.time() - self.start_time
            rate = self.processed_records / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {self.processed_records:,} records processed ({rate:.0f} records/sec)"
            )

    def finish_file(self, file_stats: dict[str, Any]):
        """Mark the completion of a file"""
        self.processed_files += 1
        file_duration = time.time() - self.file_start_time

        file_stats.update(
            {
                "filename": self.current_file,
                "duration_seconds": file_duration,
                "records_processed": self.current_file_records,
            }
        )
        self.file_stats.append(file_stats)

        logger.info(
            f"Completed {self.current_file}: {self.current_file_records:,} records "
            f"in {file_duration:.1f}s ({file_stats.get('total_inserted', 0)} inserted, "
            f"{file_stats.get('total_errors', 0)} errors)"
        )

    def summary(self):
        """Print final summary"""
        total_duration = time.time() - self.start_time
        avg_rate = self.processed_records / total_duration if total_duration > 0 else 0

        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Files processed: {self.processed_files}/{self.total_files}")
        logger.info(f"Total records: {self.processed_records:,}")
        logger.info(f"Total duration: {total_duration:.1f}s")
        logger.info(f"Average rate: {avg_rate:.0f} records/sec")

        total_inserted = sum(stat.get("total_inserted", 0) for stat in self.file_stats)
        total_errors = sum(stat.get("total_errors", 0) for stat in self.file_stats)

        logger.info(f"Successfully inserted: {total_inserted:,}")
        logger.info(f"Total errors: {total_errors:,}")

        if self.file_stats:
            logger.info("\nPer-file summary:")
            for stat in self.file_stats:
                logger.info(
                    f"  {stat['filename']}: {stat['records_processed']:,} processed, "
                    f"{stat.get('total_inserted', 0)} inserted, "
                    f"{stat.get('total_errors', 0)} errors"
                )


def find_csv_files(path: Path, recursive: bool = True) -> list[Path]:
    """Find all CSV files in a directory"""
    if path.is_file():
        return [path] if path.suffix.lower() == ".csv" else []

    pattern = "**/*.csv" if recursive else "*.csv"
    return list(path.glob(pattern))


def validate_files(csv_files: list[Path], validator: DataValidator) -> dict[str, Any]:
    """Validate CSV files before processing"""
    logger.info(f"Validating {len(csv_files)} CSV files...")

    validation_results = {
        "valid_files": [],
        "invalid_files": [],
        "warnings": [],
        "total_estimated_records": 0,
    }

    for csv_file in csv_files:
        try:
            result = validator.validate_csv_file(csv_file, sample_size=500)

            if result["valid"]:
                validation_results["valid_files"].append(
                    {
                        "path": csv_file,
                        "estimated_records": result["file_stats"].get("estimated_rows", 0),
                        "symbol": result["file_stats"].get("symbol", "UNKNOWN"),
                        "timeframe": result["file_stats"].get("timeframe", "UNKNOWN"),
                    }
                )
                validation_results["total_estimated_records"] += result["file_stats"].get(
                    "estimated_rows", 0
                )
            else:
                validation_results["invalid_files"].append(
                    {"path": csv_file, "error": result.get("error", "Unknown validation error")}
                )

            if result.get("warnings"):
                validation_results["warnings"].extend(
                    [{"file": csv_file, "warnings": result["warnings"]}]
                )

        except Exception as e:
            logger.error(f"Validation failed for {csv_file}: {e}")
            validation_results["invalid_files"].append({"path": csv_file, "error": str(e)})

    logger.info(
        f"Validation complete: {len(validation_results['valid_files'])} valid, "
        f"{len(validation_results['invalid_files'])} invalid"
    )

    if validation_results["invalid_files"]:
        logger.warning("Invalid files found:")
        for invalid in validation_results["invalid_files"]:
            logger.warning(f"  {invalid['path']}: {invalid['error']}")

    return validation_results


def ingest_file(
    csv_path: Path,
    parser: MetaTraderCSVParser,
    batch_processor: BatchProcessor,
    broker_override: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ingest a single CSV file"""

    logger.info(f"Ingesting {csv_path} with broker override: {broker_override}")

    try:
        bars = parser.parse_file(csv_path, broker=broker_override)

        bar_iter = iter(bars)
        try:
            first_bar = next(bar_iter)
            symbol = first_bar.symbol or "XAUUSD"
            timeframe = first_bar.timeframe or "h1"
            broker = first_bar.broker or "unknown"

            bars = itertools.chain([first_bar], bar_iter)

        except StopIteration:
            logger.warning(f"No bars found in {csv_path}")
            return {
                "total_processed": 0,
                "total_inserted": 0,
                "total_errors": 1,
                "fatal_error": "No bars found",
            }

        logger.info(
            f"Processing {csv_path} - Symbol: {symbol}, Timeframe: {timeframe}, Broker: {broker}"
        )

        return batch_processor.process_bars(bars, symbol, timeframe, broker, dry_run)

    except Exception as e:
        logger.error(f"Failed to ingest {csv_path}: {e}")
        return {"total_processed": 0, "total_inserted": 0, "total_errors": 1, "fatal_error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Ingest MetaTrader CSV files into TimescaleDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest single CSV file
  python -m data_hub.cli --csv-file data/XAUUSD_H1.csv --symbol XAUUSD --timeframe H1

  # Ingest directory of CSV files
  python -m data_hub.cli --csv-dir data/historical_csv/MetaTrader/IC_Markets/

  # Validate files without inserting (dry run)
  python -m data_hub.cli --csv-dir data/ --dry-run --validate --verbose
        """,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--csv-file", type=Path, help="Single CSV file to ingest")
    input_group.add_argument("--csv-dir", type=Path, help="Directory containing CSV files")

    parser.add_argument("--symbol", type=str, help="Override symbol (for single file)")
    parser.add_argument("--timeframe", type=str, help="Override timeframe (for single file)")
    parser.add_argument("--broker", type=str, help="Override broker name")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for processing")

    parser.add_argument("--dry-run", action="store_true", help="Validate data without inserting")
    parser.add_argument("--validate", action="store_true", help="Validate files before processing")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--recursive", action="store_true", default=True, help="Search subdirectories"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Historical Data Ingestion Starting")
    logger.info(f"Arguments: {vars(args)}")

    try:
        csv_parser = MetaTraderCSVParser()
        validator = DataValidator()

        if args.csv_file:
            csv_files = [args.csv_file]
        else:
            csv_files = find_csv_files(args.csv_dir, args.recursive)

        if not csv_files:
            logger.error("No CSV files found to process")
            return 1

        logger.info(f"Found {len(csv_files)} CSV files")

        if args.validate or args.dry_run:
            validation_results = validate_files(csv_files, validator)

            if not validation_results["valid_files"] and not args.dry_run:
                logger.error("No valid files found")
                return 1

            csv_files = [item["path"] for item in validation_results["valid_files"]]

        if args.dry_run and not args.validate:
            logger.info("Dry run completed - no database operations performed")
            return 0

        progress = IngestionProgress()
        progress.total_files = len(csv_files)
        if args.validate and "validation_results" in locals():
            progress.total_records = validation_results["total_estimated_records"]

        logger.info(f"Starting ingestion of {len(csv_files)} files")

        if not args.dry_run:
            logger.info("Connecting to database...")
            db_manager = DatabaseManager(settings.database_url)

            if not db_manager.test_connection():
                logger.error("Failed to connect to database")
                return 1

            db_manager.create_tables()
            db_manager.create_hypertable()

            with db_manager.get_session() as db_session:
                batch_processor = BatchProcessor(db_session, args.batch_size)

                batch_processor.optimize_for_timeseries()

                for csv_file in csv_files:
                    try:
                        progress.start_file(csv_file.name)

                        file_stats = ingest_file(
                            csv_file, csv_parser, batch_processor, args.broker, args.dry_run
                        )

                        progress.update_progress(file_stats.get("total_processed", 0))
                        progress.finish_file(file_stats)

                    except KeyboardInterrupt:
                        logger.warning("Ingestion interrupted by user")
                        break
                    except Exception as e:
                        logger.error(f"Error processing {csv_file}: {e}")
                        progress.errors += 1
                        continue

                batch_processor.reset_optimizations()
        else:
            batch_processor = BatchProcessor(None, args.batch_size)

            for csv_file in csv_files:
                try:
                    progress.start_file(csv_file.name)

                    file_stats = ingest_file(
                        csv_file, csv_parser, batch_processor, args.broker, args.dry_run
                    )

                    progress.update_progress(file_stats.get("total_processed", 0))
                    progress.finish_file(file_stats)

                except KeyboardInterrupt:
                    logger.warning("Ingestion interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Error processing {csv_file}: {e}")
                    progress.errors += 1
                    continue

        progress.summary()

        logger.info("Historical data ingestion completed successfully")
        return 0

    except KeyboardInterrupt:
        logger.warning("Ingestion interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
