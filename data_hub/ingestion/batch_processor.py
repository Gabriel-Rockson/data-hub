import io
import logging
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..models.bar_data import BarData

logger = logging.getLogger(__name__)


class BatchProcessor:
    """High-performance batch processor using PostgreSQL COPY commands"""

    def __init__(self, db_session: Session, batch_size: int = 10000):
        """
        Initialize batch processor

        Args:
            db_session: SQLAlchemy database session
            batch_size: Number of records to process in each batch
        """
        self.db_session = db_session
        self.batch_size = batch_size

    def process_bars(
        self,
        bars: Iterator[BarData],
        symbol: str,
        timeframe: str,
        broker: str = "unknown",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Process bars in batches using PostgreSQL COPY for optimal performance

        Args:
            bars: Iterator of BarData objects
            symbol: Symbol name
            timeframe: Timeframe
            broker: Broker name
            dry_run: If True, validate data without inserting

        Returns:
            Processing statistics
        """
        stats = {
            "total_processed": 0,
            "total_inserted": 0,
            "total_errors": 0,
            "total_duplicates": 0,
            "batches_processed": 0,
            "start_time": datetime.now(),
            "errors": [],
        }

        current_batch = []
        records_processed = 0

        stats["total_skipped"] = 0

        try:
            for bar in bars:
                if self._is_synthetic_bar(bar):
                    stats["total_skipped"] += 1
                    continue
                current_batch.append(bar)
                records_processed += 1

                if len(current_batch) >= self.batch_size:
                    batch_stats = self._process_batch(
                        current_batch, symbol, timeframe, broker, dry_run
                    )
                    self._update_stats(stats, batch_stats)
                    current_batch = []

                    if stats["batches_processed"] % 10 == 0:
                        logger.info(
                            f"Progress: {records_processed:,} records processed, "
                            f"{stats['total_inserted']:,} inserted"
                        )

                if records_processed % 50000 == 0:
                    elapsed = datetime.now() - stats["start_time"]
                    rate = (
                        records_processed / elapsed.total_seconds()
                        if elapsed.total_seconds() > 0
                        else 0
                    )
                    logger.info(
                        f"Parsing progress: {records_processed:,} records loaded "
                        f"({rate:,.0f} records/sec)"
                    )

            if current_batch:
                batch_stats = self._process_batch(current_batch, symbol, timeframe, broker, dry_run)
                self._update_stats(stats, batch_stats)

            stats["end_time"] = datetime.now()
            stats["duration"] = stats["end_time"] - stats["start_time"]
            stats["records_per_second"] = (
                stats["total_processed"] / stats["duration"].total_seconds()
                if stats["duration"].total_seconds() > 0
                else 0
            )

            skipped = stats.get("total_skipped", 0)
            skipped_msg = f", {skipped:,} synthetic bars skipped" if skipped else ""
            logger.info(
                f"Batch processing completed: {stats['total_processed']:,} processed, "
                f"{stats['total_inserted']:,} inserted, "
                f"{stats['total_errors']} errors{skipped_msg}"
            )

        except Exception as e:
            stats["fatal_error"] = str(e)
            logger.error(f"Fatal error in batch processing: {e}")

        return stats

    def _is_synthetic_bar(self, bar: BarData) -> bool:
        """Reject placeholder bars inserted by data providers during weekends or daily rollovers."""
        dow = bar.timestamp.weekday()  # 5=Saturday, 6=Sunday
        if dow >= 5:
            return True
        tick_vol = bar.tick_volume if bar.tick_volume is not None else 0
        if tick_vol == 0 and bar.high == bar.low == bar.open:
            return True
        return False

    def _process_batch(
        self, bars: list[BarData], symbol: str, timeframe: str, broker: str, dry_run: bool
    ) -> dict[str, Any]:
        """Process a single batch of bars"""
        batch_stats = {
            "processed": len(bars),
            "inserted": 0,
            "errors": 0,
            "duplicates": 0,
            "error_details": [],
        }

        if dry_run:
            for bar in bars:
                validation_errors = bar.validate_ohlcv_relationships()
                if validation_errors:
                    batch_stats["errors"] += 1
                    batch_stats["error_details"].append(
                        {"timestamp": bar.timestamp, "errors": validation_errors}
                    )
            batch_stats["inserted"] = batch_stats["processed"] - batch_stats["errors"]
            return batch_stats

        try:
            inserted_count = self._bulk_insert_copy(bars, symbol, timeframe, broker)
            batch_stats["inserted"] = inserted_count

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            batch_stats = self._fallback_individual_inserts(
                bars, symbol, timeframe, broker, batch_stats
            )

        return batch_stats

    def _bulk_insert_copy(
        self, bars: list[BarData], symbol: str, timeframe: str, broker: str
    ) -> int:
        """
        Use PostgreSQL COPY command for high-performance bulk insert
        """
        if not bars:
            return 0

        copy_data = []
        rows_prepared = 0

        for bar in bars:
            try:
                db_dict = bar.to_db_dict(symbol, timeframe, broker)

                row_data = "\t".join(
                    [
                        db_dict["time"].isoformat(),
                        db_dict["broker"],
                        db_dict["symbol"],
                        db_dict["timeframe"],
                        str(db_dict["open"]),
                        str(db_dict["high"]),
                        str(db_dict["low"]),
                        str(db_dict["close"]),
                        str(db_dict["volume"]),
                        str(db_dict["tick_volume"]),
                        str(db_dict["spread"]),
                    ]
                )
                copy_data.append(row_data)
                rows_prepared += 1

            except Exception as e:
                logger.warning(f"Error preparing row for COPY: {e}")
                continue

        if rows_prepared == 0:
            return 0

        copy_text = "\n".join(copy_data)

        try:
            temp_table_name = f"temp_market_data_{int(time.time() * 1000)}"

            create_temp_sql = text(f"""
                CREATE TEMP TABLE {temp_table_name} (
                    time TIMESTAMP,
                    broker VARCHAR(20),
                    symbol VARCHAR(20),
                    timeframe VARCHAR(10),
                    open NUMERIC(12,6),
                    high NUMERIC(12,6),
                    low NUMERIC(12,6),
                    close NUMERIC(12,6),
                    volume BIGINT,
                    tick_volume BIGINT,
                    spread INTEGER
                )
            """)
            self.db_session.execute(create_temp_sql)

            connection = self.db_session.connection()
            raw_connection = connection.connection

            with raw_connection.cursor() as cursor:
                copy_sql = (
                    f"COPY {temp_table_name} "
                    "(time, broker, symbol, timeframe, open, high, low, close, "
                    "volume, tick_volume, spread) "
                    "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')"
                )
                cursor.copy_expert(copy_sql, io.StringIO(copy_text))

            # temp_table_name is internally generated, not user input - safe from SQL injection
            upsert_sql = text(
                f"""
                INSERT INTO market_data (
                    time, broker, symbol, timeframe, open, high, low, close,
                    volume, tick_volume, spread
                )
                SELECT time, broker, symbol, timeframe, open, high, low, close,
                       volume, tick_volume, spread
                FROM {temp_table_name}
                ON CONFLICT (time, broker, symbol, timeframe) DO NOTHING
            """  # noqa: S608
            )

            result = self.db_session.execute(upsert_sql)
            inserted_count = result.rowcount

            drop_temp_sql = text(f"DROP TABLE {temp_table_name}")
            self.db_session.execute(drop_temp_sql)

            self.db_session.commit()

            skipped_count = rows_prepared - inserted_count
            logger.debug(
                f"UPSERT inserted {inserted_count} new rows (skipped {skipped_count} duplicates)"
            )
            return inserted_count

        except Exception as e:
            logger.error(f"UPSERT operation failed: {e}")
            self.db_session.rollback()
            raise

    def _fallback_individual_inserts(
        self,
        bars: list[BarData],
        symbol: str,
        timeframe: str,
        broker: str,
        batch_stats: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Fallback to individual inserts when COPY fails
        This allows us to identify and skip problematic records
        """
        logger.warning("Falling back to individual inserts")

        for bar in bars:
            try:
                validation_errors = bar.validate_ohlcv_relationships()
                if validation_errors:
                    batch_stats["errors"] += 1
                    batch_stats["error_details"].append(
                        {"timestamp": bar.timestamp, "errors": validation_errors}
                    )
                    continue

                db_dict = bar.to_db_dict(symbol, timeframe, broker)

                upsert_sql = text("""
                    INSERT INTO market_data (
                        time, broker, symbol, timeframe, open, high, low, close,
                        volume, tick_volume, spread
                    )
                    VALUES (
                        :time, :broker, :symbol, :timeframe, :open, :high, :low, :close,
                        :volume, :tick_volume, :spread
                    )
                    ON CONFLICT (time, broker, symbol, timeframe) DO NOTHING
                """)

                result = self.db_session.execute(upsert_sql, db_dict)
                if result.rowcount > 0:
                    batch_stats["inserted"] += 1
                else:
                    batch_stats["duplicates"] += 1

            except Exception as e:
                batch_stats["errors"] += 1
                batch_stats["error_details"].append({"timestamp": bar.timestamp, "error": str(e)})
                logger.warning(f"Individual upsert failed for {bar.timestamp}: {e}")

        try:
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Commit failed: {e}")
            batch_stats["errors"] += batch_stats["inserted"]
            batch_stats["inserted"] = 0

        return batch_stats

    def _update_stats(self, total_stats: dict[str, Any], batch_stats: dict[str, Any]) -> None:
        """Update total statistics with batch results"""
        total_stats["total_processed"] += batch_stats["processed"]
        total_stats["total_inserted"] += batch_stats["inserted"]
        total_stats["total_errors"] += batch_stats["errors"]
        total_stats["total_duplicates"] += batch_stats.get("duplicates", 0)
        total_stats["batches_processed"] += 1

        if batch_stats.get("error_details"):
            total_stats["errors"].extend(batch_stats["error_details"])
            if len(total_stats["errors"]) > 100:
                total_stats["errors"] = total_stats["errors"][-100:]

    def handle_conflicts(self, conflict_strategy: str = "skip") -> None:
        """
        Configure how to handle duplicate records

        Args:
            conflict_strategy: 'skip', 'update', or 'error'
        """
        self.conflict_strategy = conflict_strategy

    def optimize_for_timeseries(self) -> None:
        """Apply TimescaleDB-specific optimizations"""
        optimizations_applied = []

        try:
            self.db_session.execute(text("SET max_parallel_workers_per_gather = 0"))
            optimizations_applied.append("max_parallel_workers_per_gather")
        except Exception as e:
            logger.debug(f"Could not set max_parallel_workers_per_gather: {e}")

        try:
            self.db_session.execute(text("SET work_mem = '256MB'"))
            optimizations_applied.append("work_mem")
        except Exception as e:
            logger.debug(f"Could not set work_mem: {e}")

        try:
            self.db_session.execute(text("SET synchronous_commit = off"))
            optimizations_applied.append("synchronous_commit")
        except Exception as e:
            logger.debug(f"Could not set synchronous_commit: {e}")

        self._applied_optimizations = optimizations_applied

        if optimizations_applied:
            logger.info(f"Applied optimizations: {', '.join(optimizations_applied)}")
        else:
            logger.info("No optimizations could be applied (this is normal)")

    def reset_optimizations(self) -> None:
        """Reset database settings after bulk insert"""
        if not hasattr(self, "_applied_optimizations"):
            return

        reset_count = 0
        for optimization in self._applied_optimizations:
            try:
                self.db_session.execute(text(f"RESET {optimization}"))
                reset_count += 1
            except Exception as e:
                logger.debug(f"Could not reset {optimization}: {e}")

        if reset_count > 0:
            logger.info(f"Reset {reset_count} database settings")

        self._applied_optimizations = []
