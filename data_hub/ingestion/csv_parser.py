import csv
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ..models.bar_data import BarData
from ..normalization import default_normalizer

logger = logging.getLogger(__name__)


class MetaTraderCSVParser:
    """Parse MetaTrader CSV files into BarData objects"""

    def __init__(self, encoding: str = "utf-8", delimiter: str = "\t"):
        self.encoding = encoding
        self.delimiter = delimiter

    def parse_file(
        self,
        csv_path: Path,
        symbol: str | None = None,
        timeframe: str | None = None,
        broker: str | None = None,
    ) -> Iterator[BarData]:
        """
        Parse a MetaTrader CSV file and yield BarData objects

        Args:
            csv_path: Path to CSV file
            symbol: Symbol name (extracted from filename if None)
            timeframe: Timeframe (extracted from filename if None)
            broker: Broker name (will be normalized)

        Yields:
            BarData objects for each valid row
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        raw_symbol, raw_timeframe = self._extract_metadata_from_filename(
            csv_path, symbol, timeframe
        )

        normalized = default_normalizer.normalize_ingestion_metadata(
            raw_symbol, raw_timeframe, csv_path, broker
        )

        symbol = normalized["symbol"]
        timeframe = normalized["timeframe"]
        broker = normalized["broker"]

        logger.info(
            f"Parsing {csv_path} - Raw: {raw_symbol}/{raw_timeframe} -> "
            f"Normalized: {symbol}/{timeframe} @ {broker}"
        )

        try:
            with open(csv_path, encoding=self.encoding) as file:
                reader = csv.DictReader(file, delimiter=self.delimiter)

                row_count = 0
                error_count = 0

                for row_num, row in enumerate(reader, start=1):
                    try:
                        if not any(row.values()):
                            continue

                        bar_data = BarData.from_metatrader_row(
                            row, symbol=symbol, timeframe=timeframe, broker=broker
                        )

                        validation_errors = bar_data.validate_ohlcv_relationships()
                        if validation_errors:
                            logger.warning(
                                f"Validation errors in {csv_path} row {row_num}: "
                                f"{', '.join(validation_errors)}"
                            )
                            error_count += 1
                            continue

                        yield bar_data
                        row_count += 1

                    except Exception as e:
                        logger.warning(f"Error parsing row {row_num} in {csv_path}: {e}")
                        error_count += 1
                        continue

                logger.info(f"Parsed {csv_path}: {row_count} valid rows, {error_count} errors")

        except Exception as e:
            logger.error(f"Failed to parse {csv_path}: {e}")
            raise

    def _extract_metadata_from_filename(
        self, csv_path: Path, symbol: str | None = None, timeframe: str | None = None
    ) -> tuple[str, str]:
        """
        Extract symbol and timeframe from filename

        Expected format: SYMBOL_TIMEFRAME_START_END.csv
        Example: XAUUSDm_H1_201401140000_202507180300.csv
        """
        filename = csv_path.stem
        parts = filename.split("_")

        if not symbol and len(parts) >= 1:
            symbol = parts[0]

        if not timeframe and len(parts) >= 2:
            timeframe = parts[1]

        symbol = symbol or "UNKNOWN"
        timeframe = timeframe or "UNKNOWN"

        return symbol, timeframe

    def validate_file_structure(self, csv_path: Path) -> dict[str, Any]:
        """
        Validate CSV file structure and return metadata

        Returns:
            Dict with file metadata and validation results
        """
        if not csv_path.exists():
            return {"valid": False, "error": f"File not found: {csv_path}"}

        try:
            with open(csv_path, encoding=self.encoding) as file:
                reader = csv.DictReader(file, delimiter=self.delimiter)
                headers = reader.fieldnames

                required_headers = {"<DATE>", "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>"}
                missing_headers = required_headers - set(headers or [])

                if missing_headers:
                    return {
                        "valid": False,
                        "error": f"Missing required headers: {missing_headers}",
                        "headers": headers,
                    }

                sample_rows = []
                row_count = 0
                for i, row in enumerate(reader):
                    if i >= 5:
                        break
                    sample_rows.append(row)
                    row_count += 1

                file.seek(0)
                total_lines = sum(1 for _ in file) - 1

                raw_symbol, raw_timeframe = self._extract_metadata_from_filename(csv_path)
                normalized = default_normalizer.normalize_ingestion_metadata(
                    raw_symbol, raw_timeframe, csv_path
                )

                return {
                    "valid": True,
                    "headers": headers,
                    "sample_rows": sample_rows,
                    "estimated_rows": total_lines,
                    "symbol": normalized["symbol"],
                    "timeframe": normalized["timeframe"],
                    "broker": normalized["broker"],
                    "raw_symbol": raw_symbol,
                    "raw_timeframe": raw_timeframe,
                    "file_size": csv_path.stat().st_size,
                }

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def get_file_stats(self, csv_path: Path) -> dict[str, Any]:
        """Get basic statistics about the CSV file"""
        validation = self.validate_file_structure(csv_path)
        if not validation["valid"]:
            return validation

        try:
            with open(csv_path, encoding=self.encoding) as file:
                reader = csv.DictReader(file, delimiter=self.delimiter)

                first_row = None
                last_row = None
                row_count = 0

                for row in reader:
                    if first_row is None:
                        first_row = row
                    last_row = row
                    row_count += 1

                start_time = None
                end_time = None

                if first_row:
                    try:
                        bar = BarData.from_metatrader_row(first_row)
                        start_time = bar.timestamp
                    except Exception:
                        pass

                if last_row:
                    try:
                        bar = BarData.from_metatrader_row(last_row)
                        end_time = bar.timestamp
                    except Exception:
                        pass

                validation.update(
                    {
                        "actual_rows": row_count,
                        "start_time": start_time,
                        "end_time": end_time,
                        "time_range": f"{start_time} to {end_time}"
                        if start_time and end_time
                        else None,
                    }
                )

                return validation

        except Exception as e:
            return {"valid": False, "error": str(e)}
