"""
Exness historical tick data importer.

Reads Exness tick CSV exports (zip or plain CSV). The format is:
    "Exness","Symbol","Timestamp","Bid","Ask"
    "exness","XAUUSDm","2025-01-01 23:05:07.737Z",2625.179,2625.339

Timestamps are UTC millisecond precision. Symbol names use an 'm' suffix
(e.g. XAUUSDm) which is stripped on import to produce canonical names.
"""

import logging
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

BROKER = "exness"


def _normalize_symbol(raw: str) -> str:
    s = raw.upper()
    # Strip Exness suffixes: "_RAW_SPREAD", "_RAW", trailing "M"
    for suffix in ("_RAW_SPREAD", "_RAW"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    if s.endswith("M"):
        return s[:-1]
    return s


def stream_chunks(path: str | Path, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
    """
    Stream DataFrames from an Exness tick export file (zip or CSV).

    Each chunk has columns: time (UTC timestamptz), broker, symbol, bid, ask.
    """
    path = Path(path)
    reader = pd.read_csv(
        path,
        chunksize=chunksize,
        dtype={"Bid": "float64", "Ask": "float64"},
    )
    for chunk in reader:
        chunk.columns = [c.strip('"').lower() for c in chunk.columns]
        # columns: exness (broker val), symbol, timestamp, bid, ask
        chunk["time"] = pd.to_datetime(
            chunk["timestamp"].str.strip('"'),
            format="%Y-%m-%d %H:%M:%S.%fZ",
            utc=True,
        )
        chunk["symbol"] = chunk["symbol"].str.strip('"').str.upper().map(_normalize_symbol)
        chunk["broker"] = BROKER
        yield chunk[["time", "broker", "symbol", "bid", "ask"]]
