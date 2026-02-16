"""
Data Normalizer - Main Orchestrator

Coordinates all normalization activities for trading data ingestion.
Provides a unified interface for normalizing broker, symbol, and timeframe data.
"""

import logging
from pathlib import Path
from typing import Any

from ..models.symbol import Symbol
from ..models.timeframe import Timeframe
from .broker import BrokerNormalizer
from .symbol import SymbolNormalizer
from .timeframe import TimeframeNormalizer

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Main normalizer that orchestrates broker, symbol, and timeframe normalization
    """

    def __init__(self):
        self.broker_normalizer = BrokerNormalizer()
        self.symbol_normalizer = SymbolNormalizer()
        self.timeframe_normalizer = TimeframeNormalizer()

        logger.info("DataNormalizer initialized with all normalizers")

    def normalize_ingestion_metadata(
        self,
        raw_symbol: str,
        raw_timeframe: str,
        file_path: Path,
        broker_override: str | None = None,
    ) -> dict[str, str]:
        """
        Normalize all metadata for data ingestion

        Args:
            raw_symbol: Raw symbol from external source
            raw_timeframe: Raw timeframe from external source
            file_path: Path to the data file (for broker detection)
            broker_override: Optional explicit broker name

        Returns:
            Dict with normalized broker, symbol, and timeframe

        Raises:
            ValueError: If normalization fails for any field
        """
        if broker_override:
            broker = self.broker_normalizer.normalize_name(broker_override)
            logger.debug(f"Using broker override: '{broker_override}' -> '{broker}'")
        else:
            broker = self.broker_normalizer.normalize_from_path(file_path)
            logger.debug(f"Detected broker from path: '{broker}'")

        symbol = self.symbol_normalizer.normalize(raw_symbol, broker)
        logger.debug(f"Normalized symbol: '{raw_symbol}' -> '{symbol}'")

        timeframe = self.timeframe_normalizer.normalize(raw_timeframe)
        logger.debug(f"Normalized timeframe: '{raw_timeframe}' -> '{timeframe}'")

        result = {"broker": broker, "symbol": symbol, "timeframe": timeframe}

        logger.info(
            f"Normalized metadata: {raw_symbol}/{raw_timeframe} -> "
            f"{symbol}/{timeframe} @ {broker}"
        )
        return result

    def normalize_for_database(
        self,
        raw_symbol: str,
        raw_timeframe: str,
        file_path: Path,
        broker_override: str | None = None,
    ) -> tuple[str, str, str]:
        """
        Normalize data for database insertion (returns tuple for convenience)

        Args:
            raw_symbol: Raw symbol from external source
            raw_timeframe: Raw timeframe from external source
            file_path: Path to the data file
            broker_override: Optional explicit broker name

        Returns:
            Tuple of (normalized_symbol, normalized_timeframe, normalized_broker)
        """
        normalized = self.normalize_ingestion_metadata(
            raw_symbol, raw_timeframe, file_path, broker_override
        )

        return (normalized["symbol"], normalized["timeframe"], normalized["broker"])

    def validate_normalization(
        self, raw_symbol: str, raw_timeframe: str, file_path: Path
    ) -> dict[str, Any]:
        """
        Validate that normalization will work for given inputs

        Args:
            raw_symbol: Raw symbol to validate
            raw_timeframe: Raw timeframe to validate
            file_path: File path for broker detection

        Returns:
            Validation results with success flags and normalized values
        """
        results = {"valid": True, "warnings": [], "errors": [], "normalized": {}}

        try:
            normalized = self.normalize_ingestion_metadata(raw_symbol, raw_timeframe, file_path)
            results["normalized"] = normalized

            if normalized["broker"] == "unknown":
                results["warnings"].append(f"Broker could not be detected from path: {file_path}")

            if raw_symbol.upper() in self.symbol_normalizer.get_unmapped_symbols():
                results["warnings"].append(f"Symbol '{raw_symbol}' is unmapped - using fallback")

            if raw_timeframe in self.timeframe_normalizer.get_unmapped_timeframes():
                results["warnings"].append(
                    f"Timeframe '{raw_timeframe}' is unmapped - using fallback"
                )

        except Exception as e:
            results["valid"] = False
            results["errors"].append(f"Normalization failed: {e}")

        return results

    def get_normalization_stats(self) -> dict[str, Any]:
        """
        Get statistics about normalization mappings and unmapped values

        Returns:
            Statistics dictionary
        """
        return {
            "supported_brokers": len(self.broker_normalizer.get_supported_brokers()),
            "supported_symbols": len(self.symbol_normalizer.get_supported_symbols()),
            "supported_timeframes": len(self.timeframe_normalizer.get_supported_timeframes()),
            "unmapped_brokers": len(self.broker_normalizer.get_unmapped_brokers()),
            "unmapped_symbols": len(self.symbol_normalizer.get_unmapped_symbols()),
            "unmapped_timeframes": len(self.timeframe_normalizer.get_unmapped_timeframes()),
            "unmapped_broker_list": list(self.broker_normalizer.get_unmapped_brokers()),
            "unmapped_symbol_list": list(self.symbol_normalizer.get_unmapped_symbols()),
            "unmapped_timeframe_list": list(self.timeframe_normalizer.get_unmapped_timeframes()),
        }

    def add_custom_mappings(
        self,
        broker_mappings: dict[str, str] | None = None,
        symbol_mappings: dict[str, str] | None = None,
        timeframe_mappings: dict[str, str] | None = None,
    ) -> None:
        """
        Add custom mappings to the normalizers

        Args:
            broker_mappings: Dict of external_name -> internal_name for brokers
            symbol_mappings: Dict of external_symbol -> internal_symbol for symbols
            timeframe_mappings: Dict of external_timeframe -> internal_timeframe
        """
        if broker_mappings:
            for external, internal in broker_mappings.items():
                self.broker_normalizer.add_broker_mapping(external, internal)

        if symbol_mappings:
            for external, internal in symbol_mappings.items():
                symbol_enum = getattr(Symbol, internal.upper(), None)
                if symbol_enum:
                    self.symbol_normalizer.add_symbol_mapping(external, symbol_enum)
                else:
                    logger.warning(f"Invalid symbol enum: {internal}")

        if timeframe_mappings:
            for external, internal in timeframe_mappings.items():
                timeframe_enum = getattr(Timeframe, internal.upper(), None)
                if timeframe_enum:
                    self.timeframe_normalizer.add_timeframe_mapping(external, timeframe_enum)
                else:
                    logger.warning(f"Invalid timeframe enum: {internal}")

        logger.info("Custom mappings added to normalizers")

    def export_mappings(self) -> dict[str, dict]:
        """
        Export all current mappings for backup/configuration purposes

        Returns:
            Dictionary containing all normalizer mappings
        """
        return {
            "brokers": self.broker_normalizer.get_supported_brokers(),
            "symbols": {
                k: v.value for k, v in self.symbol_normalizer.get_supported_symbols().items()
            },
            "timeframes": {
                k: v.value for k, v in self.timeframe_normalizer.get_supported_timeframes().items()
            },
        }

    def reset_unmapped_tracking(self) -> None:
        """Reset the tracking of unmapped values (useful for testing)"""
        self.broker_normalizer.unmapped_brokers.clear()
        self.symbol_normalizer.unmapped_symbols.clear()
        self.timeframe_normalizer.unmapped_timeframes.clear()
        logger.info("Reset unmapped value tracking for all normalizers")
