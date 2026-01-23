"""
Timeframe Normalization

Handles mapping of external timeframe formats to standardized Timeframe enum values.
Supports MetaTrader exports and other common timeframe naming conventions.
"""

import logging
import re
from typing import ClassVar

from ..models.timeframe import Timeframe

logger = logging.getLogger(__name__)


class TimeframeNormalizer:
    """Normalize timeframe strings from various external formats to internal Timeframe enum"""

    TIMEFRAME_MAPPINGS: ClassVar = {
        # === METATRADER EXPORTS ===
        "M1": Timeframe.M1,
        "M2": Timeframe.M2,
        "M3": Timeframe.M3,
        "M4": Timeframe.M4,
        "M5": Timeframe.M5,
        "M15": Timeframe.M15,
        "M30": Timeframe.M30,
        "H1": Timeframe.H1,
        "H4": Timeframe.H4,
        "D1": Timeframe.D1,
        "Daily": Timeframe.D1,
        "W1": Timeframe.W1,
        "Weekly": Timeframe.W1,
        "MN1": Timeframe.MN1,
        "Monthly": Timeframe.MN1,
        # === ALTERNATIVE FORMATS ===
        "1m": Timeframe.M1,
        "1min": Timeframe.M1,
        "1minute": Timeframe.M1,
        "1_minute": Timeframe.M1,
        "2m": Timeframe.M2,
        "2min": Timeframe.M2,
        "2minute": Timeframe.M2,
        "3m": Timeframe.M3,
        "3min": Timeframe.M3,
        "3minute": Timeframe.M3,
        "4m": Timeframe.M4,
        "4min": Timeframe.M4,
        "4minute": Timeframe.M4,
        "5m": Timeframe.M5,
        "5min": Timeframe.M5,
        "5minute": Timeframe.M5,
        "5_minute": Timeframe.M5,
        "15m": Timeframe.M15,
        "15min": Timeframe.M15,
        "15minute": Timeframe.M15,
        "15_minute": Timeframe.M15,
        "30m": Timeframe.M30,
        "30min": Timeframe.M30,
        "30minute": Timeframe.M30,
        "30_minute": Timeframe.M30,
        "1h": Timeframe.H1,
        "1hr": Timeframe.H1,
        "1hour": Timeframe.H1,
        "1_hour": Timeframe.H1,
        "60m": Timeframe.H1,
        "60min": Timeframe.H1,
        "4h": Timeframe.H4,
        "4hr": Timeframe.H4,
        "4hour": Timeframe.H4,
        "4_hour": Timeframe.H4,
        "240m": Timeframe.H4,
        "240min": Timeframe.H4,
        "1d": Timeframe.D1,
        "1day": Timeframe.D1,
        "1_day": Timeframe.D1,
        "day": Timeframe.D1,
        "daily": Timeframe.D1,
        "DAILY": Timeframe.D1,
        "24h": Timeframe.D1,
        "1440m": Timeframe.D1,
        "1w": Timeframe.W1,
        "1week": Timeframe.W1,
        "1_week": Timeframe.W1,
        "week": Timeframe.W1,
        "weekly": Timeframe.W1,
        "WEEKLY": Timeframe.W1,
        "7d": Timeframe.W1,
        "1M": Timeframe.MN1,
        "1mo": Timeframe.MN1,
        "1mon": Timeframe.MN1,
        "1month": Timeframe.MN1,
        "1_month": Timeframe.MN1,
        "month": Timeframe.MN1,
        "monthly": Timeframe.MN1,
        "MONTHLY": Timeframe.MN1,
        # === TRADING PLATFORM VARIATIONS ===
        "1": Timeframe.M1,
        "2": Timeframe.M2,
        "3": Timeframe.M3,
        "4": Timeframe.M4,
        "5": Timeframe.M5,
        "15": Timeframe.M15,
        "30": Timeframe.M30,
        "60": Timeframe.H1,
        "240": Timeframe.H4,
        "D": Timeframe.D1,
        "W": Timeframe.W1,
        "M": Timeframe.MN1,
        "m1": Timeframe.M1,
        "m2": Timeframe.M2,
        "m3": Timeframe.M3,
        "m4": Timeframe.M4,
        "m5": Timeframe.M5,
        "m15": Timeframe.M15,
        "m30": Timeframe.M30,
        "h1": Timeframe.H1,
        "h4": Timeframe.H4,
        "d1": Timeframe.D1,
        "w1": Timeframe.W1,
        "mn1": Timeframe.MN1,
    }

    def __init__(self):
        self.unmapped_timeframes = set()

    def normalize(self, timeframe: str) -> str:
        """
        Normalize a timeframe string to internal Timeframe enum value

        Args:
            timeframe: Raw timeframe from external source

        Returns:
            Normalized timeframe string (Timeframe enum value)
        """
        if not timeframe:
            logger.warning("Empty timeframe provided")
            return Timeframe.H1.value

        cleaned_timeframe = timeframe.strip()

        if cleaned_timeframe in self.TIMEFRAME_MAPPINGS:
            return self.TIMEFRAME_MAPPINGS[cleaned_timeframe].value

        for tf_key, tf_enum in self.TIMEFRAME_MAPPINGS.items():
            if cleaned_timeframe.lower() == tf_key.lower():
                return tf_enum.value

        normalized = self._pattern_match(cleaned_timeframe)
        if normalized:
            return normalized.value

        if cleaned_timeframe not in self.unmapped_timeframes:
            self.unmapped_timeframes.add(cleaned_timeframe)
            logger.warning(f"Unmapped timeframe: '{timeframe}' - defaulting to H1")

        return Timeframe.H1.value

    def _pattern_match(self, timeframe: str) -> Timeframe | None:
        """
        Use pattern matching for complex timeframe variations

        Args:
            timeframe: Cleaned timeframe string

        Returns:
            Matched Timeframe enum or None
        """
        if timeframe.isdigit():
            minutes = int(timeframe)
            if minutes == 1:
                return Timeframe.M1
            if minutes == 2:
                return Timeframe.M2
            if minutes == 3:
                return Timeframe.M3
            if minutes == 4:
                return Timeframe.M4
            if minutes == 5:
                return Timeframe.M5
            if minutes == 15:
                return Timeframe.M15
            if minutes == 30:
                return Timeframe.M30
            if minutes == 60:
                return Timeframe.H1
            if minutes == 240:
                return Timeframe.H4
            if minutes == 1440:
                return Timeframe.D1

        minute_match = re.match(r"^(\d+)m(?:in)?(?:ute)?(?:s)?$", timeframe.lower())
        if minute_match:
            minutes = int(minute_match.group(1))
            if minutes == 1:
                return Timeframe.M1
            if minutes == 2:
                return Timeframe.M2
            if minutes == 3:
                return Timeframe.M3
            if minutes == 4:
                return Timeframe.M4
            if minutes == 5:
                return Timeframe.M5
            if minutes == 15:
                return Timeframe.M15
            if minutes == 30:
                return Timeframe.M30
            if minutes == 60:
                return Timeframe.H1
            if minutes == 240:
                return Timeframe.H4

        hour_match = re.match(r"^(\d+)h(?:r)?(?:our)?(?:s)?$", timeframe.lower())
        if hour_match:
            hours = int(hour_match.group(1))
            if hours == 1:
                return Timeframe.H1
            if hours == 4:
                return Timeframe.H4
            if hours == 24:
                return Timeframe.D1

        day_match = re.match(r"^(\d+)d(?:ay)?(?:s)?$", timeframe.lower())
        if day_match:
            days = int(day_match.group(1))
            if days == 1:
                return Timeframe.D1
            if days == 7:
                return Timeframe.W1

        week_match = re.match(r"^(\d+)w(?:eek)?(?:s)?$", timeframe.lower())
        if week_match:
            weeks = int(week_match.group(1))
            if weeks == 1:
                return Timeframe.W1

        if "_" in timeframe:
            parts = timeframe.split("_")
            for part in parts:
                if part.upper() in self.TIMEFRAME_MAPPINGS:
                    return self.TIMEFRAME_MAPPINGS[part.upper()]

        return None

    def get_unmapped_timeframes(self) -> set:
        """Get set of timeframes that couldn't be mapped"""
        return self.unmapped_timeframes.copy()

    def add_timeframe_mapping(self, external_timeframe: str, internal_timeframe: Timeframe) -> None:
        """
        Add a new timeframe mapping dynamically

        Args:
            external_timeframe: External timeframe identifier
            internal_timeframe: Internal Timeframe enum value
        """
        self.TIMEFRAME_MAPPINGS[external_timeframe] = internal_timeframe
        logger.info(
            f"Added timeframe mapping: '{external_timeframe}' -> '{internal_timeframe.value}'"
        )

    def get_supported_timeframes(self) -> dict[str, Timeframe]:
        """Get all currently supported timeframe mappings"""
        return self.TIMEFRAME_MAPPINGS.copy()

    def validate_timeframe(self, timeframe: str) -> bool:
        """
        Check if a timeframe can be successfully normalized

        Args:
            timeframe: Timeframe to validate

        Returns:
            True if timeframe can be normalized
        """
        try:
            normalized = self.normalize(timeframe)
            return normalized in [tf.value for tf in Timeframe]
        except Exception:
            return False

    def get_timeframe_minutes(self, timeframe: str) -> int | None:
        """
        Get the number of minutes for a normalized timeframe

        Args:
            timeframe: Timeframe string (normalized or raw)

        Returns:
            Number of minutes or None if invalid
        """
        normalized = self.normalize(timeframe)

        minute_map = {
            Timeframe.M1.value: 1,
            Timeframe.M2.value: 2,
            Timeframe.M3.value: 3,
            Timeframe.M4.value: 4,
            Timeframe.M5.value: 5,
            Timeframe.M15.value: 15,
            Timeframe.M30.value: 30,
            Timeframe.H1.value: 60,
            Timeframe.H4.value: 240,
            Timeframe.D1.value: 1440,
            Timeframe.W1.value: 10080,
            Timeframe.MN1.value: 43200,
        }

        return minute_map.get(normalized)
