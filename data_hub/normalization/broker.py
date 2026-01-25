"""
Broker Normalization

Extracts broker name from file path and applies basic formatting.
No hardcoded mappings - uses directory structure directly.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class BrokerNormalizer:
    """Extract and normalize broker names from file paths"""

    def __init__(self):
        self.unmapped_brokers = set()

    def normalize_from_path(self, file_path: Path) -> str:
        """
        Extract broker name from file path

        Args:
            file_path: Path to the data file

        Returns:
            Normalized broker name from path, or 'unknown'
        """
        path_parts = file_path.parts

        # Find the directory immediately after "data"
        # This is the broker name in the structure: data/BROKER/[SYMBOL/]file.csv
        try:
            data_index = None
            for i, part in enumerate(path_parts):
                if part.lower() == "data":
                    data_index = i
                    break

            if data_index is not None and data_index + 1 < len(path_parts) - 1:
                broker_part = path_parts[data_index + 1]
                return self._clean_broker_name(broker_part)
        except Exception as e:
            logger.warning(f"Error extracting broker from path {file_path}: {e}")

        logger.debug(f"No broker detected in path {file_path}, using 'unknown'")
        return "unknown"

    def normalize_name(self, broker_name: str) -> str:
        """
        Normalize a broker name string

        Args:
            broker_name: Raw broker name from external source

        Returns:
            Cleaned broker name
        """
        if not broker_name:
            return "unknown"

        return self._clean_broker_name(broker_name)

    def _clean_broker_name(self, name: str) -> str:
        """
        Clean and format broker name

        Args:
            name: Raw broker name

        Returns:
            Formatted broker name (lowercase with hyphens)
        """
        cleaned = name.strip()
        # Replace underscores and spaces with hyphens
        cleaned = cleaned.replace("_", "-").replace(" ", "-")
        # Lowercase for consistency
        cleaned = cleaned.lower()
        # Remove multiple consecutive hyphens
        while "--" in cleaned:
            cleaned = cleaned.replace("--", "-")
        # Remove leading/trailing hyphens
        cleaned = cleaned.strip("-")

        return cleaned if cleaned else "unknown"

    def get_unmapped_brokers(self) -> set:
        """Get set of broker names that couldn't be mapped (unused in new implementation)"""
        return set()

    def add_broker_mapping(self, external_name: str, internal_name: str) -> None:
        """Not used in simplified implementation but kept for API compatibility"""
        logger.warning("Broker mappings are not used in simplified implementation")

    def get_supported_brokers(self) -> dict[str, str]:
        """Returns empty dict - no predefined mappings"""
        return {}
