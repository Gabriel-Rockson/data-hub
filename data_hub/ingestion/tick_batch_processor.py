"""
High-performance tick batch processor using PostgreSQL COPY.
"""

import io
import logging
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_TEMP_TABLE = "temp_ticks_bulk"


class TickBatchProcessor:
    def __init__(self, db_session: Session, batch_size: int = 100_000):
        self.db_session = db_session
        self.batch_size = batch_size

    def process_ticks(self, chunks: Iterator[pd.DataFrame]) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "total_processed": 0,
            "total_inserted": 0,
            "total_errors": 0,
            "start_time": datetime.now(),
        }

        self._create_temp_table()
        chunk_num = 0
        try:
            for chunk in chunks:
                chunk_num += 1
                logger.info(f"Chunk {chunk_num}: parsed {len(chunk):,} ticks, inserting...")
                t0 = time.perf_counter()
                try:
                    inserted = self._bulk_copy(chunk)
                    elapsed = time.perf_counter() - t0
                    stats["total_processed"] += len(chunk)
                    stats["total_inserted"] += inserted
                    total_elapsed = (datetime.now() - stats["start_time"]).total_seconds()
                    rate = stats["total_processed"] / total_elapsed if total_elapsed > 0 else 0
                    logger.info(
                        f"Chunk {chunk_num}: inserted {inserted:,} in {elapsed:.1f}s "
                        f"— total {stats['total_processed']:,} ticks "
                        f"@ {rate:,.0f} ticks/sec"
                    )
                except Exception as e:
                    logger.error(f"Chunk {chunk_num}: insert failed: {e}")
                    stats["total_errors"] += len(chunk)
                    self.db_session.rollback()
        except Exception as e:
            stats["fatal_error"] = str(e)
            logger.error(f"Fatal error in tick batch processing: {e}")
        finally:
            self._drop_temp_table()

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

    def _create_temp_table(self) -> None:
        self.db_session.execute(text(f"""
            CREATE TEMP TABLE IF NOT EXISTS {_TEMP_TABLE} (
                time TIMESTAMPTZ,
                broker VARCHAR(20),
                symbol VARCHAR(20),
                bid NUMERIC(12, 6),
                ask NUMERIC(12, 6)
            )
        """))

    def _drop_temp_table(self) -> None:
        self.db_session.execute(text(f"DROP TABLE IF EXISTS {_TEMP_TABLE}"))

    def _bulk_copy(self, chunk: pd.DataFrame) -> int:
        self.db_session.execute(text(f"TRUNCATE {_TEMP_TABLE}"))

        buf = io.StringIO()
        chunk.to_csv(buf, header=False, index=False, sep="\t")
        buf.seek(0)

        conn = self.db_session.connection()
        raw = conn.connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {_TEMP_TABLE} (time, broker, symbol, bid, ask) "  # noqa: S608
                "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')",
                buf,
            )

        result = self.db_session.execute(text(f"""
            INSERT INTO ticks (time, broker, symbol, bid, ask)
            SELECT time, broker, symbol, bid, ask FROM {_TEMP_TABLE}
            ON CONFLICT (time, broker, symbol) DO NOTHING
        """))  # noqa: S608
        self.db_session.commit()
        return result.rowcount

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
