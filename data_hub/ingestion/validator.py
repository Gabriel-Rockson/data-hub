import logging
from pathlib import Path
from statistics import mean, median
from typing import Any

from ..models.bar_data import BarData
from .csv_parser import MetaTraderCSVParser

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate trading data quality and integrity"""

    def __init__(self, max_price_change_pct: float = 50.0, max_spread_pips: float = 1000.0):
        """
        Initialize validator with thresholds

        Args:
            max_price_change_pct: Maximum allowed price change percentage between bars
            max_spread_pips: Maximum allowed spread in pips (for forex, adjust for other assets)
        """
        self.max_price_change_pct = max_price_change_pct
        self.max_spread_pips = max_spread_pips

    def validate_bar_sequence(self, bars: list[BarData]) -> dict[str, Any]:
        """
        Validate a sequence of bars for quality and consistency

        Args:
            bars: List of BarData objects in chronological order

        Returns:
            Dict with validation results and statistics
        """
        if not bars:
            return {"valid": False, "error": "No bars to validate"}

        validation_result = {
            "valid": True,
            "total_bars": len(bars),
            "errors": [],
            "warnings": [],
            "statistics": {},
        }

        try:
            invalid_bars = []
            for i, bar in enumerate(bars):
                bar_errors = bar.validate_ohlcv_relationships()
                if bar_errors:
                    invalid_bars.append(
                        {"index": i, "timestamp": bar.timestamp, "errors": bar_errors}
                    )

            if invalid_bars:
                validation_result["errors"].append(
                    {
                        "type": "ohlcv_validation",
                        "count": len(invalid_bars),
                        "details": invalid_bars[:10],
                    }
                )

            time_issues = self._validate_time_sequence(bars)
            if time_issues["errors"]:
                validation_result["errors"].extend(time_issues["errors"])
            if time_issues["warnings"]:
                validation_result["warnings"].extend(time_issues["warnings"])

            price_issues = self._validate_price_movements(bars)
            if price_issues["errors"]:
                validation_result["errors"].extend(price_issues["errors"])
            if price_issues["warnings"]:
                validation_result["warnings"].extend(price_issues["warnings"])

            validation_result["statistics"] = self._calculate_statistics(bars)

            validation_result["valid"] = len(validation_result["errors"]) == 0

        except Exception as e:
            validation_result["valid"] = False
            validation_result["error"] = f"Validation failed: {e}"
            logger.error(f"Validation error: {e}")

        return validation_result

    def _validate_time_sequence(self, bars: list[BarData]) -> dict[str, list]:
        """Validate chronological order and time gaps"""
        errors = []
        warnings = []

        if len(bars) < 2:
            return {"errors": errors, "warnings": warnings}

        out_of_order = [
            {"index": i, "timestamp": bars[i].timestamp, "previous": bars[i - 1].timestamp}
            for i in range(1, len(bars))
            if bars[i].timestamp <= bars[i - 1].timestamp
        ]

        if out_of_order:
            errors.append(
                {
                    "type": "time_order",
                    "message": f"Found {len(out_of_order)} bars out of chronological order",
                    "details": out_of_order[:5],
                }
            )

        if not out_of_order:
            gaps = self._find_time_gaps(bars)
            if gaps:
                warnings.append(
                    {
                        "type": "time_gaps",
                        "message": f"Found {len(gaps)} significant time gaps",
                        "details": gaps[:5],
                    }
                )

        timestamps = [bar.timestamp for bar in bars]
        duplicates = []
        seen = set()
        for i, ts in enumerate(timestamps):
            if ts in seen:
                duplicates.append({"index": i, "timestamp": ts})
            seen.add(ts)

        if duplicates:
            errors.append(
                {
                    "type": "duplicate_timestamps",
                    "message": f"Found {len(duplicates)} duplicate timestamps",
                    "details": duplicates[:5],
                }
            )

        return {"errors": errors, "warnings": warnings}

    def _find_time_gaps(self, bars: list[BarData]) -> list[dict[str, Any]]:
        """Find significant gaps in time series"""
        gaps = []

        if len(bars) < 10:
            return gaps

        time_diffs = []
        for i in range(1, min(50, len(bars))):
            diff = (bars[i].timestamp - bars[i - 1].timestamp).total_seconds()
            time_diffs.append(diff)

        if not time_diffs:
            return gaps

        expected_interval = median(time_diffs)

        for i in range(1, len(bars)):
            actual_diff = (bars[i].timestamp - bars[i - 1].timestamp).total_seconds()
            if actual_diff > expected_interval * 3:
                gaps.append(
                    {
                        "start_index": i - 1,
                        "end_index": i,
                        "start_time": bars[i - 1].timestamp,
                        "end_time": bars[i].timestamp,
                        "gap_duration": actual_diff,
                        "expected_duration": expected_interval,
                    }
                )

        return gaps

    def _validate_price_movements(self, bars: list[BarData]) -> dict[str, list]:
        """Validate price movements for suspicious jumps"""
        errors = []
        warnings = []

        if len(bars) < 2:
            return {"errors": errors, "warnings": warnings}

        suspicious_moves = []

        for i in range(1, len(bars)):
            prev_bar = bars[i - 1]
            curr_bar = bars[i]

            close_to_close_change = abs(curr_bar.close - prev_bar.close) / prev_bar.close * 100
            gap_change = abs(curr_bar.open - prev_bar.close) / prev_bar.close * 100

            if close_to_close_change > self.max_price_change_pct:
                suspicious_moves.append(
                    {
                        "type": "large_price_move",
                        "index": i,
                        "timestamp": curr_bar.timestamp,
                        "change_pct": close_to_close_change,
                        "prev_close": prev_bar.close,
                        "curr_close": curr_bar.close,
                    }
                )

            if gap_change > self.max_price_change_pct:
                suspicious_moves.append(
                    {
                        "type": "large_gap",
                        "index": i,
                        "timestamp": curr_bar.timestamp,
                        "gap_pct": gap_change,
                        "prev_close": prev_bar.close,
                        "curr_open": curr_bar.open,
                    }
                )

        if suspicious_moves:
            warnings.append(
                {
                    "type": "price_movements",
                    "message": f"Found {len(suspicious_moves)} suspicious price movements",
                    "details": suspicious_moves[:10],
                }
            )

        return {"errors": errors, "warnings": warnings}

    def _calculate_statistics(self, bars: list[BarData]) -> dict[str, Any]:
        """Calculate descriptive statistics for the bar sequence"""
        if not bars:
            return {}

        try:
            [bar.open for bar in bars]
            highs = [bar.high for bar in bars]
            lows = [bar.low for bar in bars]
            closes = [bar.close for bar in bars]

            tick_volumes = [bar.tick_volume for bar in bars if bar.tick_volume > 0]
            spreads = [bar.spread for bar in bars if bar.spread > 0]

            start_time = bars[0].timestamp
            end_time = bars[-1].timestamp
            duration = end_time - start_time

            ranges = [bar.high - bar.low for bar in bars]
            body_sizes = [abs(bar.close - bar.open) for bar in bars]

            stats = {
                "time_range": {
                    "start": start_time,
                    "end": end_time,
                    "duration_days": duration.days,
                    "duration_hours": duration.total_seconds() / 3600,
                },
                "price_stats": {
                    "min_low": min(lows),
                    "max_high": max(highs),
                    "mean_close": mean(closes),
                    "median_close": median(closes),
                },
                "volatility": {
                    "mean_range": mean(ranges),
                    "median_range": median(ranges),
                    "mean_body_size": mean(body_sizes),
                    "max_range": max(ranges),
                },
            }

            if tick_volumes:
                stats["volume_stats"] = {
                    "mean_tick_volume": mean(tick_volumes),
                    "median_tick_volume": median(tick_volumes),
                    "max_tick_volume": max(tick_volumes),
                }

            if spreads:
                stats["spread_stats"] = {
                    "mean_spread": mean(spreads),
                    "median_spread": median(spreads),
                    "max_spread": max(spreads),
                }

            bullish_count = sum(1 for bar in bars if bar.is_bullish)
            stats["market_stats"] = {
                "bullish_bars": bullish_count,
                "bearish_bars": len(bars) - bullish_count,
                "bullish_ratio": bullish_count / len(bars),
            }

            return stats

        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
            return {"error": str(e)}

    def validate_csv_file(self, csv_path: Path, sample_size: int = 1000) -> dict[str, Any]:
        """
        Quick validation of CSV file by sampling data

        Args:
            csv_path: Path to CSV file
            sample_size: Number of rows to sample for validation

        Returns:
            Validation results
        """
        parser = MetaTraderCSVParser()

        structure_validation = parser.validate_file_structure(csv_path)
        if not structure_validation["valid"]:
            return structure_validation

        try:
            bars = []
            for _i, bar in enumerate(parser.parse_file(csv_path)):
                bars.append(bar)
                if len(bars) >= sample_size:
                    break

            if not bars:
                return {"valid": False, "error": "No valid bars found in file"}

            validation_result = self.validate_bar_sequence(bars)
            validation_result.update(
                {
                    "file_path": str(csv_path),
                    "sampled_bars": len(bars),
                    "file_stats": structure_validation,
                }
            )

            return validation_result

        except Exception as e:
            return {"valid": False, "error": f"Validation failed: {e}"}
