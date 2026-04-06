#!/usr/bin/env python3
"""
Validate bar data in TimescaleDB for gaps and coverage.

Gaps that span a Saturday (typical forex weekend close) are reported separately
and not counted as anomalous. Everything else larger than N * timeframe interval
is flagged.

Usage:
    cd data-hub
    .venv/bin/python validate_data.py --broker dukascopy --symbol XAUUSD
    .venv/bin/python validate_data.py --broker dukascopy --symbol XAUUSD --timeframe M1
    .venv/bin/python validate_data.py --broker exness --symbol XAUUSD --from 2023-01-01 --to 2023-12-31
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta

from sqlalchemy import text

from data_hub.config import settings
from data_hub.database.connection import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

TF_INTERVALS: dict[str, timedelta] = {
    "M1":  timedelta(minutes=1),
    "M2":  timedelta(minutes=2),
    "M3":  timedelta(minutes=3),
    "M4":  timedelta(minutes=4),
    "M5":  timedelta(minutes=5),
    "M15": timedelta(minutes=15),
    "M30": timedelta(minutes=30),
    "H1":  timedelta(hours=1),
    "H4":  timedelta(hours=4),
    "D1":  timedelta(days=1),
    "W1":  timedelta(weeks=1),
    "MN1": timedelta(days=30),
}

TF_ORDER = list(TF_INTERVALS)


def _sort_key(tf: str) -> int:
    try:
        return TF_ORDER.index(tf.upper())
    except ValueError:
        return 999


def _spans_weekend(start: datetime, end: datetime) -> bool:
    """True if the gap contains a Saturday — typical forex weekend close."""
    current = start.date()
    while current <= end.date():
        if current.weekday() == 5:  # Saturday
            return True
        current += timedelta(days=1)
    return False


def _validate_timeframe(
    conn,
    broker: str,
    symbol: str,
    timeframe: str,
    start: date | None,
    end: date | None,
    gap_multiplier: float,
) -> int:
    """Validate one timeframe. Prints results and returns anomalous gap count."""
    interval = TF_INTERVALS.get(timeframe.upper())
    if interval is None:
        print(f"  {timeframe}: unknown timeframe, skipping")
        return 0

    threshold_secs = interval.total_seconds() * gap_multiplier

    date_filter = ""
    params: dict = {
        "broker": broker,
        "symbol": symbol,
        "tf": timeframe.upper(),
        "threshold_secs": threshold_secs,
    }
    if start:
        date_filter += " AND time >= :start"
        params["start"] = start
    if end:
        date_filter += " AND time <= :end_date"
        params["end_date"] = end

    count_row = conn.execute(text(f"""
        SELECT COUNT(*), MIN(time), MAX(time)
        FROM market_data
        WHERE broker = :broker AND symbol = :symbol AND timeframe = :tf
        {date_filter}
    """), params).fetchone()

    total_bars, min_time, max_time = count_row
    if not total_bars:
        print(f"\n  {timeframe}: no data")
        return 0

    gaps = conn.execute(text(f"""
        SELECT prev_time, time, EXTRACT(EPOCH FROM (time - prev_time)) AS gap_secs
        FROM (
            SELECT
                time,
                LAG(time) OVER (ORDER BY time) AS prev_time
            FROM market_data
            WHERE broker = :broker AND symbol = :symbol AND timeframe = :tf
            {date_filter}
        ) sub
        WHERE prev_time IS NOT NULL
          AND EXTRACT(EPOCH FROM (time - prev_time)) > :threshold_secs
        ORDER BY time
    """), params).fetchall()

    weekend_gaps = [g for g in gaps if _spans_weekend(g.prev_time, g.time)]
    anomalous = [g for g in gaps if not _spans_weekend(g.prev_time, g.time)]
    missing_bars = sum(int(float(g.gap_secs) / interval.total_seconds()) - 1 for g in anomalous)

    print(f"\n  {timeframe}")
    print(f"    Range:          {min_time} → {max_time}")
    print(f"    Total bars:     {total_bars:,}")
    print(f"    Weekend gaps:   {len(weekend_gaps)} (skipped)")
    print(f"    Anomalous gaps: {len(anomalous)}  (~{missing_bars:,} bars missing)")

    if anomalous:
        for g in anomalous:
            gap_secs = float(g.gap_secs)
            n = int(gap_secs / interval.total_seconds()) - 1
            duration = timedelta(seconds=gap_secs)
            print(f"      {g.prev_time}  →  {g.time}  ({duration}, ~{n} bars missing)")

    return len(anomalous)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--broker", required=True, help="Broker name (e.g. dukascopy, exness)")
    parser.add_argument("--symbol", required=True, help="Symbol (e.g. XAUUSD, EURUSD)")
    parser.add_argument(
        "--timeframe",
        default=None,
        metavar="TF",
        help="Timeframe to check (e.g. M1, H1). Default: all available",
    )
    parser.add_argument("--from", dest="start", metavar="YYYY-MM-DD", default=None)
    parser.add_argument("--to", dest="end", metavar="YYYY-MM-DD", default=None)
    parser.add_argument(
        "--gap-multiplier",
        type=float,
        default=3.0,
        metavar="N",
        help="Flag gaps larger than N * timeframe interval. Default: 3.0",
    )
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start) if args.start else None
        end = date.fromisoformat(args.end) if args.end else None
    except ValueError as e:
        logger.error(f"Invalid date: {e}")
        return 1

    if start and end and start > end:
        logger.error("--from must be before --to")
        return 1

    db = DatabaseManager(settings.database_url)
    if not db.test_connection():
        logger.error("Cannot connect to database")
        return 1

    print(f"\nBroker:  {args.broker}")
    print(f"Symbol:  {args.symbol.upper()}")
    if start or end:
        print(f"Period:  {start or 'beginning'} → {end or 'now'}")
    print(f"Threshold: {args.gap_multiplier}x interval")

    with db.engine.connect() as conn:
        if args.timeframe:
            timeframes = [args.timeframe.upper()]
        else:
            rows = conn.execute(text("""
                SELECT DISTINCT timeframe FROM market_data
                WHERE broker = :broker AND symbol = :symbol
            """), {"broker": args.broker, "symbol": args.symbol.upper()}).fetchall()
            timeframes = sorted([r[0] for r in rows], key=_sort_key)
            if not timeframes:
                print("\n  No data found.")
                return 1

        total_anomalous = 0
        for tf in timeframes:
            total_anomalous += _validate_timeframe(
                conn, args.broker, args.symbol.upper(), tf, start, end, args.gap_multiplier
            )

    print(f"\nTotal anomalous gaps: {total_anomalous}")
    return 0 if total_anomalous == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
