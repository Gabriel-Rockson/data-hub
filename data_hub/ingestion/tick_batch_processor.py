"""
High-performance tick batch processor using PostgreSQL COPY.
"""

import io
import logging
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..models.tick_data import TickData

logger = logging.getLogger(__name__)


class TickBatchProcessor:
    def __init__(self, db_session: Session, batch_size: int = 100_000):
        self.db_session = db_session
        self.batch_size = batch_size

    def process_ticks(
        self,
        ticks: Iterator[TickData],
        dry_run: bool = False,
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "total_processed": 0,
            "total_inserted": 0,
            "total_errors": 0,
            "start_time": datetime.now(),
        }

        batch: list[TickData] = []

        try:
            for tick in ticks:
                batch.append(tick)
                if len(batch) >= self.batch_size:
                    self._flush(batch, dry_run, stats)
                    batch = []

            if batch:
                self._flush(batch, dry_run, stats)

        except Exception as e:
            stats["fatal_error"] = str(e)
            logger.error(f"Fatal error in tick batch processing: {e}")

        stats["end_time"] = datetime.now()
        duration = (stats["end_time"] - stats["start_time"]).total_seconds()
        stats["duration"] = duration
        stats["ticks_per_second"] = stats["total_processed"] / duration if duration > 0 else 0

        logger.info(
            f"Done: {stats['total_processed']:,} processed, "
            f"{stats['total_inserted']:,} inserted, "
            f"{stats['total_errors']} errors, "
            f"{stats['ticks_per_second']:,.0f} ticks/sec"
        )
        return stats

    def _flush(self, batch: list[TickData], dry_run: bool, stats: dict[str, Any]) -> None:
        stats["total_processed"] += len(batch)
        if dry_run:
            stats["total_inserted"] += len(batch)
            return
        try:
            inserted = self._bulk_copy(batch)
            stats["total_inserted"] += inserted
            logger.debug(f"Inserted {inserted:,} ticks ({len(batch) - inserted} duplicates)")
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            stats["total_errors"] += len(batch)
            self.db_session.rollback()

    def _bulk_copy(self, batch: list[TickData]) -> int:
        rows = "\n".join(
            f"{t.timestamp.isoformat()}\t{t.broker}\t{t.symbol}\t{t.bid}\t{t.ask}"
            for t in batch
        )

        temp = f"temp_ticks_{int(time.time() * 1000)}"

        self.db_session.execute(text(f"""
            CREATE TEMP TABLE {temp} (
                time TIMESTAMPTZ,
                broker VARCHAR(20),
                symbol VARCHAR(20),
                bid NUMERIC(12, 6),
                ask NUMERIC(12, 6)
            )
        """))

        conn = self.db_session.connection()
        raw = conn.connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {temp} (time, broker, symbol, bid, ask) "  # noqa: S608
                "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')",
                io.StringIO(rows),
            )

        result = self.db_session.execute(text(f"""
            INSERT INTO ticks (time, broker, symbol, bid, ask)
            SELECT time, broker, symbol, bid, ask FROM {temp}
            ON CONFLICT (time, broker, symbol) DO NOTHING
        """))  # noqa: S608
        inserted = result.rowcount

        self.db_session.execute(text(f"DROP TABLE {temp}"))
        self.db_session.commit()
        return inserted

    def optimize_for_bulk(self) -> None:
        for setting in [
            "SET synchronous_commit = off",
            "SET work_mem = '256MB'",
        ]:
            try:
                self.db_session.execute(text(setting))
            except Exception:
                pass

    def reset_optimizations(self) -> None:
        for setting in ["synchronous_commit", "work_mem"]:
            try:
                self.db_session.execute(text(f"RESET {setting}"))
            except Exception:
                pass
