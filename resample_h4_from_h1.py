#!/usr/bin/env python3
"""
Derive dukascopy/XAUUSD H4 bars from existing H1 data already in the database.

Aggregation uses time_bucket('4 hours') which aligns to UTC epoch multiples:
00:00, 04:00, 08:00, 12:00, 16:00, 20:00.

Uses TimescaleDB first()/last() for open/close ordering, which is more efficient
than array_agg + sort.

Usage:
    cd data-hub
    .venv/bin/python resample_h4_from_h1.py
    .venv/bin/python resample_h4_from_h1.py --dry-run
    .venv/bin/python resample_h4_from_h1.py --broker dukascopy --symbol XAUUSD
"""

import argparse
import logging
import sys

from data_hub.config import settings
from data_hub.database.connection import DatabaseManager
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

COUNT_SQL = """
SELECT COUNT(*) FROM market_data
WHERE broker = :broker AND symbol = :symbol AND timeframe = 'H1'
"""

DRY_RUN_SQL = """
SELECT COUNT(*) FROM (
    SELECT time_bucket('4 hours', time) AS bucket
    FROM market_data
    WHERE broker = :broker AND symbol = :symbol AND timeframe = 'H1'
    GROUP BY bucket
) sub
"""

INSERT_SQL = """
INSERT INTO market_data (time, broker, symbol, timeframe, open, high, low, close, volume, tick_volume, spread)
SELECT
    time_bucket('4 hours', time)      AS time,
    broker,
    symbol,
    'H4'                              AS timeframe,
    first(open,  time)                AS open,
    MAX(high)                         AS high,
    MIN(low)                          AS low,
    last(close,  time)                AS close,
    SUM(volume)                       AS volume,
    SUM(tick_volume)                  AS tick_volume,
    (AVG(spread))::int                AS spread
FROM market_data
WHERE broker = :broker AND symbol = :symbol AND timeframe = 'H1'
GROUP BY time_bucket('4 hours', time), broker, symbol
ON CONFLICT (time, broker, symbol, timeframe) DO NOTHING
"""

VERIFY_SQL = """
SELECT
    COUNT(*)                        AS bars,
    MIN(time)                       AS first_bar,
    MAX(time)                       AS last_bar,
    MIN(low)                        AS min_low,
    MAX(high)                       AS max_high,
    ROUND(AVG(high - low)::numeric, 4) AS avg_range
FROM market_data
WHERE broker = :broker AND symbol = :symbol AND timeframe = 'H4'
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--broker", default="dukascopy")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--dry-run", action="store_true", help="Count rows that would be inserted, skip writes")
    args = parser.parse_args()

    db = DatabaseManager(settings.database_url)
    if not db.test_connection():
        logger.error("Cannot connect to database")
        return 1

    params = {"broker": args.broker, "symbol": args.symbol}

    with db.get_session() as session:
        h1_count = session.execute(text(COUNT_SQL), params).scalar()
        logger.info(f"Source H1 bars for {args.broker}/{args.symbol}: {h1_count:,}")

        if h1_count == 0:
            logger.error("No H1 data found — nothing to resample")
            return 1

        if args.dry_run:
            expected = session.execute(text(DRY_RUN_SQL), params).scalar()
            logger.info(f"Dry run: would generate {expected:,} H4 bars (nothing inserted)")
            return 0

        result = session.execute(text(INSERT_SQL), params)
        inserted = result.rowcount
        logger.info(f"Inserted {inserted:,} H4 bars")

        row = session.execute(text(VERIFY_SQL), params).one()
        logger.info(
            f"Verification — bars: {row.bars:,}  range: {row.first_bar} → {row.last_bar}  "
            f"price range: {row.min_low}–{row.max_high}  avg_candle_range: {row.avg_range}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
