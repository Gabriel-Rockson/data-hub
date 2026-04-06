"""
Dukascopy historical OHLC data fetcher.

Downloads M1 candle data from Dukascopy's public datafeed and optionally
resamples to coarser timeframes. Dukascopy stores candles as LZMA-compressed
bi5 binary files (one per day). Each 24-byte record encodes:
  time_sec (uint32 BE) – seconds from UTC midnight
  open, high, low, close (uint32 BE each) – price in raw integer points
  volume (float32 BE) – tick volume

Prices are converted to float by dividing by the instrument's point multiplier.
"""

import logging
import lzma
import random
import struct
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from ..models.bar_data import BarData

logger = logging.getLogger(__name__)

# Maps canonical symbol names to Dukascopy instrument identifiers
INSTRUMENT_MAP: dict[str, str] = {
    # Metals
    "XAUUSD": "XAUUSD",
    "XAGUSD": "XAGUSD",
    "XAUEUR": "XAUEUR",
    "XAUGBP": "XAUGBP",
    # FX majors
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDJPY": "USDJPY",
    "USDCHF": "USDCHF",
    "USDCAD": "USDCAD",
    "AUDUSD": "AUDUSD",
    "NZDUSD": "NZDUSD",
    # FX crosses — EUR
    "EURGBP": "EURGBP",
    "EURJPY": "EURJPY",
    "EURCHF": "EURCHF",
    "EURCAD": "EURCAD",
    "EURAUD": "EURAUD",
    "EURNZD": "EURNZD",
    # FX crosses — GBP
    "GBPJPY": "GBPJPY",
    "GBPCHF": "GBPCHF",
    "GBPCAD": "GBPCAD",
    "GBPAUD": "GBPAUD",
    "GBPNZD": "GBPNZD",
    # FX crosses — AUD
    "AUDJPY": "AUDJPY",
    "AUDCHF": "AUDCHF",
    "AUDCAD": "AUDCAD",
    "AUDNZD": "AUDNZD",
    # FX crosses — NZD
    "NZDJPY": "NZDJPY",
    "NZDCHF": "NZDCHF",
    "NZDCAD": "NZDCAD",
    # FX crosses — CAD / CHF
    "CADJPY": "CADJPY",
    "CADCHF": "CADCHF",
    "CHFJPY": "CHFJPY",
    # Indices
    "NAS100": "USATECHIDXUSD",
    "US30":   "WSJUSAIDXUSD",
    "SPX500": "SPXUSD",
    "DAX40":  "DEUIDXEUR",
    "AUS200": "AUSIDXAUD",
    # Crypto
    "BTCUSD": "BTCUSD",
    "ETHUSD": "ETHUSD",
    "LTCUSD": "LTCUSD",
    "XRPUSD": "XRPUSD",
    "BCHUSD": "BCHUSD",
}

# Divide raw integer price by this to get the actual float price.
# Rule of thumb: JPY pairs and metals → 1000; standard FX → 100000;
# large crypto (BTC/ETH/LTC/BCH) → 10; small crypto (XRP) → 1000;
# equity indices → 1000.
POINT_MULTIPLIER: dict[str, int] = {
    # Metals
    "XAUUSD": 1000,
    "XAGUSD": 1000,
    "XAUEUR": 1000,
    "XAUGBP": 1000,
    # FX majors
    "EURUSD": 100000,
    "GBPUSD": 100000,
    "USDJPY": 1000,
    "USDCHF": 100000,
    "USDCAD": 100000,
    "AUDUSD": 100000,
    "NZDUSD": 100000,
    # FX crosses — non-JPY → 100000, JPY → 1000
    "EURGBP": 100000,
    "EURJPY": 1000,
    "EURCHF": 100000,
    "EURCAD": 100000,
    "EURAUD": 100000,
    "EURNZD": 100000,
    "GBPJPY": 1000,
    "GBPCHF": 100000,
    "GBPCAD": 100000,
    "GBPAUD": 100000,
    "GBPNZD": 100000,
    "AUDJPY": 1000,
    "AUDCHF": 100000,
    "AUDCAD": 100000,
    "AUDNZD": 100000,
    "NZDJPY": 1000,
    "NZDCHF": 100000,
    "NZDCAD": 100000,
    "CADJPY": 1000,
    "CADCHF": 100000,
    "CHFJPY": 1000,
    # Indices
    "USATECHIDXUSD": 1000,
    "WSJUSAIDXUSD":  1000,
    "SPXUSD":        1000,
    "DEUIDXEUR":     1000,
    "AUSIDXAUD":     1000,
    # Crypto
    "BTCUSD": 10,
    "ETHUSD": 10,
    "LTCUSD": 10,
    "XRPUSD": 1000,
    "BCHUSD": 10,
}

# pandas resample rules for each supported timeframe
RESAMPLE_RULES: dict[str, str | None] = {
    "M1": None,
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
    "W1": "W",
    "MN1": "MS",
}

_BASE_URL = "https://datafeed.dukascopy.com/datafeed"
_RECORD_SIZE = 24
_RECORD_FORMAT = ">IIIIIf"  # time_ms, open, high, low, close, volume


_RETRY_DELAYS = [3, 10, 30]  # seconds between attempts; jitter added on top


def _fetch_day_raw(instrument: str, year: int, month: int, day: int) -> bytes | None:
    """
    Fetch and LZMA-decompress one day's M1 bi5 candle file.

    Dukascopy URLs use 0-indexed months (January = 00, December = 11).
    Returns decompressed bytes, or None if unavailable (weekend/holiday/404).
    Retries up to 3 times on transient errors with increasing backoff.
    """
    url = f"{_BASE_URL}/{instrument}/{year}/{month - 1:02d}/{day:02d}/BID_candles_min_1.bi5"
    label = f"{instrument} {year}-{month:02d}-{day:02d}"
    for attempt in range(len(_RETRY_DELAYS) + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                compressed = resp.read()
            if not compressed:
                return None
            return lzma.decompress(compressed)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt < len(_RETRY_DELAYS):
                delay = _RETRY_DELAYS[attempt] + random.uniform(0, 2)
                logger.debug(f"HTTP {e.code} fetching {label}, retrying in {delay:.1f}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue
            logger.warning(f"HTTP {e.code} fetching {label}: {e}")
            return None
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < len(_RETRY_DELAYS):
                delay = _RETRY_DELAYS[attempt] + random.uniform(0, 2)
                logger.debug(f"Network error fetching {label}, retrying in {delay:.1f}s (attempt {attempt + 1}): {e}")
                time.sleep(delay)
                continue
            logger.warning(f"URL error fetching {label}: {e}")
            return None
        except lzma.LZMAError:
            return None
    return None


_MAX_PRICE = 999_999.0  # matches Numeric(12, 6) — anything above this is a multiplier bug


def _parse_day(raw: bytes, day: date, multiplier: int) -> list[BarData]:
    """Parse decompressed bi5 bytes into BarData objects for a single day."""
    bars: list[BarData] = []
    n_records = len(raw) // _RECORD_SIZE
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)

    for i in range(n_records):
        chunk = raw[i * _RECORD_SIZE : (i + 1) * _RECORD_SIZE]
        # Dukascopy field order: time_sec, open, close, low, high, volume
        time_sec, open_raw, close_raw, low_raw, high_raw, volume = struct.unpack(_RECORD_FORMAT, chunk)
        ts = day_start + timedelta(seconds=int(time_sec))
        o, h, l, c = open_raw / multiplier, high_raw / multiplier, low_raw / multiplier, close_raw / multiplier
        if h > _MAX_PRICE:
            raise ValueError(
                f"Price overflow on {day}: high={h:.2f} exceeds {_MAX_PRICE:,.0f}. "
                f"Wrong POINT_MULTIPLIER? (used {multiplier}, raw high={high_raw})"
            )
        try:
            bars.append(
                BarData(
                    timestamp=ts,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    # Dukascopy volume is in millions of base currency units; scale to integer
                    tick_volume=int(round(volume * 1_000_000)),
                )
            )
        except Exception as e:
            logger.debug(f"Skipping malformed record at {ts}: {e}")
    return bars


def _resample_bars(bars: list[BarData], rule: str) -> list[BarData]:
    """Resample a list of M1 BarData objects to a coarser timeframe."""
    if not bars:
        return []

    df = pd.DataFrame(
        {
            "time": [b.timestamp for b in bars],
            "open": [b.open for b in bars],
            "high": [b.high for b in bars],
            "low": [b.low for b in bars],
            "close": [b.close for b in bars],
            "tick_volume": [b.tick_volume for b in bars],
        }
    )
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)

    resampled = df.resample(rule, label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "tick_volume": "sum",
        }
    ).dropna(subset=["open"])

    result: list[BarData] = []
    for ts, row in resampled.iterrows():
        try:
            result.append(
                BarData(
                    timestamp=ts.to_pydatetime().replace(tzinfo=UTC),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    tick_volume=int(row["tick_volume"]),
                )
            )
        except Exception as e:
            logger.debug(f"Skipping resampled bar at {ts}: {e}")
    return result


def fetch_bars(
    symbol: str,
    start: date,
    end: date,
    timeframe: str = "M1",
    request_delay: float = 0.1,
) -> Iterator[BarData]:
    """
    Fetch OHLC bars for a symbol from Dukascopy's public datafeed.

    Args:
        symbol: Canonical symbol name (e.g., "XAUUSD", "NAS100").
        start: Start date, inclusive.
        end: End date, inclusive.
        timeframe: Target timeframe — one of M1, M5, M15, M30, H1, H4, D1.
        request_delay: Seconds to sleep between HTTP requests to avoid rate limiting.

    Yields:
        BarData objects in ascending time order.
    """
    tf_upper = timeframe.upper()
    if tf_upper not in RESAMPLE_RULES:
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {sorted(RESAMPLE_RULES)}")

    sym_upper = symbol.upper()
    instrument = INSTRUMENT_MAP.get(sym_upper)
    if not instrument:
        raise ValueError(f"Unsupported symbol: {symbol}. Supported: {sorted(INSTRUMENT_MAP)}")

    multiplier = POINT_MULTIPLIER[instrument]
    resample_rule = RESAMPLE_RULES[tf_upper]
    is_m1 = tf_upper == "M1"

    buffer: list[BarData] = []
    current = start
    total_days = (end - start).days + 1
    days_done = 0

    while current <= end:
        raw = _fetch_day_raw(instrument, current.year, current.month, current.day)
        days_done += 1

        if raw:
            day_bars = _parse_day(raw, current, multiplier)
            if is_m1:
                yield from day_bars
            else:
                buffer.extend(day_bars)

        if days_done % 30 == 0 or current == end:
            logger.info(f"{sym_upper} {tf_upper}: {days_done}/{total_days} days fetched")

        if request_delay > 0:
            time.sleep(request_delay)

        current += timedelta(days=1)

    if not is_m1 and buffer:
        yield from _resample_bars(buffer, resample_rule)
