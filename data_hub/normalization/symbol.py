"""
Symbol Normalization

Handles mapping of broker-specific symbol names to standardized Symbol enum values.
Supports various broker naming conventions and formats.
"""

import logging
import re
from typing import ClassVar

from ..models.symbol import Symbol

logger = logging.getLogger(__name__)


class SymbolNormalizer:
    """Normalize symbol names from various external formats to internal Symbol enum"""

    SYMBOL_MAPPINGS: ClassVar = {
        # === GOLD VARIATIONS ===
        "XAUUSD": Symbol.XAUUSD,
        "XAUUSDm": Symbol.XAUUSD,
        "XAUUSD.": Symbol.XAUUSD,
        "XAUUSD_": Symbol.XAUUSD,
        "GOLD": Symbol.XAUUSD,
        "GOLDUSD": Symbol.XAUUSD,
        "GC": Symbol.XAUUSD,
        # === SILVER VARIATIONS ===
        "XAGUSD": Symbol.XAGUSD,
        "XAGUSDm": Symbol.XAGUSD,
        "XAGUSD.": Symbol.XAGUSD,
        "SILVER": Symbol.XAGUSD,
        "SILVERUSD": Symbol.XAGUSD,
        "SI": Symbol.XAGUSD,
        # === MAJOR FOREX PAIRS ===
        "EURUSD": Symbol.EURUSD,
        "EURUSDm": Symbol.EURUSD,
        "EURUSD.": Symbol.EURUSD,
        "EURUSD_ECN": Symbol.EURUSD,
        "GBPUSD": Symbol.GBPUSD,
        "GBPUSDm": Symbol.GBPUSD,
        "GBPUSD.": Symbol.GBPUSD,
        "GBPUSD_ECN": Symbol.GBPUSD,
        "USDJPY": Symbol.USDJPY,
        "USDJPYm": Symbol.USDJPY,
        "USDJPY.": Symbol.USDJPY,
        "USDCHF": Symbol.USDCHF,
        "USDCHFm": Symbol.USDCHF,
        "USDCHF.": Symbol.USDCHF,
        "USDCAD": Symbol.USDCAD,
        "USDCADm": Symbol.USDCAD,
        "USDCAD.": Symbol.USDCAD,
        "AUDUSD": Symbol.AUDUSD,
        "AUDUSDm": Symbol.AUDUSD,
        "AUDUSD.": Symbol.AUDUSD,
        "NZDUSD": Symbol.NZDUSD,
        "NZDUSDm": Symbol.NZDUSD,
        "NZDUSD.": Symbol.NZDUSD,
        # === CROSS PAIRS ===
        "EURGBP": Symbol.EURGBP,
        "EURGBPm": Symbol.EURGBP,
        "EURJPY": Symbol.EURJPY,
        "EURJPYm": Symbol.EURJPY,
        "GBPJPY": Symbol.GBPJPY,
        "GBPJPYm": Symbol.GBPJPY,
        "AUDJPY": Symbol.AUDJPY,
        "AUDJPYm": Symbol.AUDJPY,
        "CHFJPY": Symbol.CHFJPY,
        "CHFJPYm": Symbol.CHFJPY,
        "EURAUD": Symbol.EURAUD,
        "EURAUDm": Symbol.EURAUD,
        "EURNZD": Symbol.EURNZD,
        "EURNZDm": Symbol.EURNZD,
        # === OIL VARIATIONS ===
        "WTIUSD": Symbol.WTIUSD,
        "WTI": Symbol.WTIUSD,
        "CRUDE": Symbol.WTIUSD,
        "USOIL": Symbol.WTIUSD,
        "CL": Symbol.WTIUSD,
        "BRENTUSD": Symbol.BRENTUSD,
        "BRENT": Symbol.BRENTUSD,
        "UKOIL": Symbol.BRENTUSD,
        # === INDICES ===
        "US30": Symbol.US30,
        "US30.": Symbol.US30,
        "US30USD": Symbol.US30,
        "DOW": Symbol.US30,
        "DOWJONES": Symbol.US30,
        "YM": Symbol.US30,
        "NAS100": Symbol.NAS100,
        "NAS100.": Symbol.NAS100,
        "US100": Symbol.NAS100,
        "US100.": Symbol.NAS100,
        "NASDAQ": Symbol.NAS100,
        "NDX": Symbol.NAS100,
        "NQ": Symbol.NAS100,
        "USTEC": Symbol.NAS100,
        "SPX500": Symbol.SPX500,
        "SPX500.": Symbol.SPX500,
        "SP500": Symbol.SPX500,
        "US500": Symbol.SPX500,
        "US500.": Symbol.SPX500,
        "SPX": Symbol.SPX500,
        "ES": Symbol.SPX500,
        "GER40": Symbol.GER40,
        "GER40.": Symbol.GER40,
        "DAX": Symbol.GER40,
        "GDAXI": Symbol.GER40,
        "UK100": Symbol.UK100,
        "UK100.": Symbol.UK100,
        "FTSE": Symbol.UK100,
        "FTSE100": Symbol.UK100,
        "JPN225": Symbol.JPN225,
        "JPN225.": Symbol.JPN225,
        "NIKKEI": Symbol.JPN225,
        "N225": Symbol.JPN225,
        # === CRYPTOCURRENCIES ===
        "BTCUSD": Symbol.BTCUSD,
        "BTCUSDm": Symbol.BTCUSD,
        "BTCUSD.": Symbol.BTCUSD,
        "BITCOIN": Symbol.BTCUSD,
        "BTC": Symbol.BTCUSD,
        "ETHUSD": Symbol.ETHUSD,
        "ETHUSDm": Symbol.ETHUSD,
        "ETHUSD.": Symbol.ETHUSD,
        "ETHEREUM": Symbol.ETHUSD,
        "ETH": Symbol.ETHUSD,
        "LTCUSD": Symbol.LTCUSD,
        "LTCUSDm": Symbol.LTCUSD,
        "LITECOIN": Symbol.LTCUSD,
        "LTC": Symbol.LTCUSD,
        "XRPUSD": Symbol.XRPUSD,
        "XRPUSDm": Symbol.XRPUSD,
        "RIPPLE": Symbol.XRPUSD,
        "XRP": Symbol.XRPUSD,
    }

    COMMON_SUFFIXES: ClassVar[list[str]] = ["m", ".", "_", "_ECN", "_Pro", "_RAW", "_ZERO"]

    def __init__(self):
        self.unmapped_symbols = set()

    def normalize(self, symbol: str, broker: str | None = None) -> str:
        """
        Normalize a symbol name to internal Symbol enum value

        Args:
            symbol: Raw symbol name from external source
            broker: Optional broker context for specific mappings

        Returns:
            Normalized symbol name (Symbol enum value)

        Raises:
            ValueError: If symbol cannot be normalized
        """
        if not symbol:
            raise ValueError("Empty symbol provided")

        cleaned_symbol = symbol.strip().upper()

        if cleaned_symbol in self.SYMBOL_MAPPINGS:
            return self.SYMBOL_MAPPINGS[cleaned_symbol].value

        if broker:
            broker_specific = f"{cleaned_symbol}_{broker.upper()}"
            if broker_specific in self.SYMBOL_MAPPINGS:
                return self.SYMBOL_MAPPINGS[broker_specific].value

        for suffix in self.COMMON_SUFFIXES:
            if cleaned_symbol.endswith(suffix):
                base_symbol = cleaned_symbol[: -len(suffix)]
                if base_symbol in self.SYMBOL_MAPPINGS:
                    logger.debug(
                        f"Mapped '{symbol}' -> '{base_symbol}' -> "
                        f"'{self.SYMBOL_MAPPINGS[base_symbol].value}'"
                    )
                    return self.SYMBOL_MAPPINGS[base_symbol].value

        normalized = self._pattern_match(cleaned_symbol)
        if normalized:
            return normalized.value

        self.unmapped_symbols.add(cleaned_symbol)
        raise ValueError(
            f"Cannot normalize symbol '{symbol}'. "
            f"Symbol not found in mappings. "
            f"Add mapping for '{cleaned_symbol}' to SYMBOL_MAPPINGS or use add_symbol_mapping()."
        )

    def _pattern_match(self, symbol: str) -> Symbol | None:
        """
        Use pattern matching for complex symbol variations

        Args:
            symbol: Cleaned symbol string

        Returns:
            Matched Symbol enum or None
        """
        if re.match(r"^XAU.*USD.*$", symbol) or "GOLD" in symbol:
            return Symbol.XAUUSD

        if re.match(r"^XAG.*USD.*$", symbol) or "SILVER" in symbol:
            return Symbol.XAGUSD

        if any(oil in symbol for oil in ["WTI", "CRUDE", "USOIL", "CL"]):
            return Symbol.WTIUSD

        if any(brent in symbol for brent in ["BRENT", "UKOIL"]):
            return Symbol.BRENTUSD

        if any(us30 in symbol for us30 in ["US30", "DOW", "YM"]):
            return Symbol.US30

        if any(nas in symbol for nas in ["NAS", "NASDAQ", "NDX", "NQ"]):
            return Symbol.NAS100

        if any(sp in symbol for sp in ["SPX", "SP500", "ES"]):
            return Symbol.SPX500

        forex_pairs = [
            ("EUR", "USD", Symbol.EURUSD),
            ("GBP", "USD", Symbol.GBPUSD),
            ("USD", "JPY", Symbol.USDJPY),
            ("USD", "CHF", Symbol.USDCHF),
            ("USD", "CAD", Symbol.USDCAD),
            ("AUD", "USD", Symbol.AUDUSD),
            ("NZD", "USD", Symbol.NZDUSD),
        ]

        for base, quote, enum_val in forex_pairs:
            if base in symbol and quote in symbol:
                return enum_val

        return None

    def get_unmapped_symbols(self) -> set:
        """Get set of symbols that couldn't be mapped"""
        return self.unmapped_symbols.copy()

    def add_symbol_mapping(self, external_symbol: str, internal_symbol: Symbol) -> None:
        """
        Add a new symbol mapping dynamically

        Args:
            external_symbol: External symbol identifier
            internal_symbol: Internal Symbol enum value
        """
        self.SYMBOL_MAPPINGS[external_symbol.upper()] = internal_symbol
        logger.info(f"Added symbol mapping: '{external_symbol}' -> '{internal_symbol.value}'")

    def get_supported_symbols(self) -> dict[str, Symbol]:
        """Get all currently supported symbol mappings"""
        return self.SYMBOL_MAPPINGS.copy()

    def validate_symbol(self, symbol: str) -> bool:
        """
        Check if a symbol can be successfully normalized

        Args:
            symbol: Symbol to validate

        Returns:
            True if symbol can be normalized
        """
        try:
            normalized = self.normalize(symbol)
            return normalized in [s.value for s in Symbol]
        except Exception:
            return False
