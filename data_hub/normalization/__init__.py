"""
Trading Data Normalization Package

This package handles normalization of external trading data to internal system standards.
Ensures consistent broker names, symbols, and timeframes across all data sources.
"""

from .broker import BrokerNormalizer
from .mapper import DataNormalizer
from .symbol import SymbolNormalizer
from .timeframe import TimeframeNormalizer

default_normalizer = DataNormalizer()

__all__ = [
    "BrokerNormalizer",
    "DataNormalizer",
    "SymbolNormalizer",
    "TimeframeNormalizer",
    "default_normalizer",
]
