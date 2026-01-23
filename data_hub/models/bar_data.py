import logging
from datetime import UTC, datetime
from decimal import Decimal

import pandas as pd
from pydantic import BaseModel, ConfigDict, computed_field, field_validator

from .symbol import Symbol
from .timeframe import Timeframe

logger = logging.getLogger(__name__)


class BarData(BaseModel):
    model_config = ConfigDict(validate_assignment=True, str_strip_whitespace=True)

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int = 0
    volume: int = 0
    spread: int = 0

    # Optional fields for ingestion metadata
    symbol: Symbol | None = None
    timeframe: Timeframe | None = None
    broker: str | None = None

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v):
        """Ensure timestamp is timezone-aware UTC"""
        if v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        elif v.tzinfo != UTC:
            v = v.astimezone(UTC)
        return v

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v):
        """Normalize symbol to proper Symbol enum"""
        if not v:
            return None
        if isinstance(v, Symbol):
            return v
        if isinstance(v, str):
            try:
                return Symbol(v.upper())
            except ValueError:
                logger.warning(f"Unknown symbol: {v}")
                return v.upper()
        return v

    @field_validator("timeframe", mode="before")
    @classmethod
    def normalize_timeframe(cls, v):
        """Normalize timeframe to proper Timeframe enum"""
        if not v:
            return None
        if isinstance(v, Timeframe):
            return v
        if isinstance(v, str):
            try:
                return Timeframe(v.upper())
            except ValueError:
                logger.warning(f"Unknown timeframe: {v}")
                return v.upper()
        return v

    @field_validator("open", "high", "low", "close")
    @classmethod
    def validate_prices(cls, v):
        """Ensure prices are positive"""
        if v <= 0:
            raise ValueError("Price must be positive")
        return v

    @field_validator("tick_volume", "volume")
    @classmethod
    def validate_volumes(cls, v):
        """Ensure volumes are non-negative"""
        if v < 0:
            raise ValueError("Volume must be non-negative")
        return v

    @computed_field
    @property
    def body_size(self) -> float:
        """Calculate candle body size (absolute difference between open and close)"""
        return abs(self.close - self.open)

    @computed_field
    @property
    def upper_shadow(self) -> float:
        """Calculate upper shadow length"""
        return self.high - max(self.open, self.close)

    @computed_field
    @property
    def lower_shadow(self) -> float:
        """Calculate lower shadow length"""
        return min(self.open, self.close) - self.low

    @computed_field
    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish (close > open)"""
        return self.close > self.open

    def validate_ohlcv_relationships(self) -> list[str]:
        """Validate OHLCV price relationships, returns list of errors"""
        errors = []

        if self.high < self.open:
            errors.append(f"High ({self.high}) < Open ({self.open})")
        if self.high < self.low:
            errors.append(f"High ({self.high}) < Low ({self.low})")
        if self.high < self.close:
            errors.append(f"High ({self.high}) < Close ({self.close})")

        if self.low > self.open:
            errors.append(f"Low ({self.low}) > Open ({self.open})")
        if self.low > self.close:
            errors.append(f"Low ({self.low}) > Close ({self.close})")

        if self.high == self.low and (self.open != self.close or self.open != self.high):
            errors.append("High equals Low but Open/Close differ")

        return errors

    def to_db_dict(self, symbol: str, timeframe: str, broker: str = "unknown") -> dict:
        """Convert to dictionary format for database insertion"""
        return {
            "time": self.timestamp,
            "broker": broker,
            "symbol": symbol.upper(),
            "timeframe": timeframe.upper(),
            "open": Decimal(str(self.open)),
            "high": Decimal(str(self.high)),
            "low": Decimal(str(self.low)),
            "close": Decimal(str(self.close)),
            "volume": self.volume,
            "tick_volume": self.tick_volume,
            "spread": self.spread,
        }

    @classmethod
    def from_metatrader_row(
        cls,
        row: dict,
        symbol: str | None = None,
        timeframe: str | None = None,
        broker: str | None = None,
    ):
        """Create BarData from MetaTrader CSV row"""
        date_str = row.get("<DATE>", "").strip()
        time_str = row.get("<TIME>", "").strip()

        if not date_str:
            raise ValueError(f"Missing date in row: {row}")

        if not time_str:
            time_str = "00:00:00"

        dt_str = f"{date_str} {time_str}"
        try:
            timestamp = datetime.strptime(dt_str, "%Y.%m.%d %H:%M:%S")
        except ValueError as e:
            raise ValueError(f"Invalid datetime format '{dt_str}': {e}") from e

        return cls(
            timestamp=timestamp,
            open=float(row.get("<OPEN>", 0)),
            high=float(row.get("<HIGH>", 0)),
            low=float(row.get("<LOW>", 0)),
            close=float(row.get("<CLOSE>", 0)),
            tick_volume=int(row.get("<TICKVOL>", 0)),
            volume=int(row.get("<VOL>", 0)),
            spread=int(row.get("<SPREAD>", 0)),
            symbol=symbol,
            timeframe=timeframe,
            broker=broker,
        )

    def to_dataframe(self) -> pd.DataFrame:
        """Convert single bar to single-row DataFrame with datetime index"""
        data = {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "tick_volume": self.tick_volume,
            "spread": self.spread,
        }

        if self.symbol:
            data["symbol"] = self.symbol
        if self.timeframe:
            data["timeframe"] = self.timeframe
        if self.broker:
            data["broker"] = self.broker

        data.update(
            {
                "body_size": self.body_size,
                "upper_shadow": self.upper_shadow,
                "lower_shadow": self.lower_shadow,
                "is_bullish": self.is_bullish,
            }
        )

        return pd.DataFrame([data], index=[self.timestamp])

    @staticmethod
    def to_dataframe_batch(bars: list["BarData"]) -> pd.DataFrame:
        """Convert list of BarData to DataFrame optimized for analysis"""
        if not bars:
            return pd.DataFrame()

        data = {
            "timestamp": [bar.timestamp for bar in bars],
            "open": [bar.open for bar in bars],
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
            "volume": [bar.volume for bar in bars],
            "tick_volume": [bar.tick_volume for bar in bars],
            "spread": [bar.spread for bar in bars],
            "body_size": [bar.body_size for bar in bars],
            "upper_shadow": [bar.upper_shadow for bar in bars],
            "lower_shadow": [bar.lower_shadow for bar in bars],
            "is_bullish": [bar.is_bullish for bar in bars],
        }

        if all(bar.symbol for bar in bars):
            data["symbol"] = [bar.symbol for bar in bars]
        if all(bar.timeframe for bar in bars):
            data["timeframe"] = [bar.timeframe for bar in bars]
        if all(bar.broker for bar in bars):
            data["broker"] = [bar.broker for bar in bars]

        df = pd.DataFrame(data)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        return df
