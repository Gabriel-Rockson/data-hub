"""
Exness historical tick data importer.

Reads Exness tick CSV exports (zip or plain CSV). The format is:
    "Exness","Symbol","Timestamp","Bid","Ask"
    "exness","XAUUSDm","2025-01-01 23:05:07.737Z",2625.179,2625.339

Timestamps are UTC millisecond precision. Symbol names use an 'm' suffix
(e.g. XAUUSDm) which is stripped on import to produce canonical names.
"""

import csv
import io
import logging
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from ..models.tick_data import TickData

logger = logging.getLogger(__name__)

BROKER = "exness"

# Strip trailing 'm' from Exness mini/market symbols
def _normalize_symbol(raw: str) -> str:
    s = raw.upper()
    if s.endswith("M"):
        return s[:-1]
    return s


def _parse_timestamp(raw: str) -> datetime:
    # Format: "2025-01-01 23:05:07.737Z"
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%fZ").replace(tzinfo=UTC)


def stream_ticks(path: str | Path) -> Iterator[TickData]:
    """
    Stream TickData from an Exness tick export file (zip or CSV).

    Skips the header row. Yields one TickData per tick row.
    """
    path = Path(path)

    if path.suffix == ".zip":
        yield from _stream_from_zip(path)
    else:
        yield from _stream_from_csv(path)


def _stream_from_zip(path: Path) -> Iterator[TickData]:
    with zipfile.ZipFile(path, "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found inside {path}")
        if len(csv_names) > 1:
            logger.warning(f"Multiple CSV files in zip, using first: {csv_names[0]}")

        with zf.open(csv_names[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8")
            yield from _parse_csv(text)


def _stream_from_csv(path: Path) -> Iterator[TickData]:
    with open(path, encoding="utf-8") as f:
        yield from _parse_csv(f)


def _parse_csv(f: io.TextIOWrapper) -> Iterator[TickData]:
    reader = csv.reader(f)
    header = next(reader, None)
    if header is None:
        return

    # Locate columns by header name (case-insensitive, strip quotes)
    headers = [h.strip().strip('"').lower() for h in header]
    try:
        ts_idx = headers.index("timestamp")
        bid_idx = headers.index("bid")
        ask_idx = headers.index("ask")
        sym_idx = headers.index("symbol")
    except ValueError as e:
        raise ValueError(f"Missing expected column in Exness CSV: {e}") from e

    rows_ok = 0
    rows_err = 0

    for row in reader:
        try:
            symbol = _normalize_symbol(row[sym_idx].strip().strip('"'))
            ts = _parse_timestamp(row[ts_idx].strip().strip('"'))
            bid = float(row[bid_idx])
            ask = float(row[ask_idx])
            yield TickData(timestamp=ts, broker=BROKER, symbol=symbol, bid=bid, ask=ask)
            rows_ok += 1

            if rows_ok % 1_000_000 == 0:
                logger.info(f"Parsed {rows_ok:,} ticks...")

        except Exception as e:
            rows_err += 1
            if rows_err <= 5:
                logger.warning(f"Skipping malformed row {row}: {e}")

    logger.info(f"Finished parsing: {rows_ok:,} ticks, {rows_err} errors")
