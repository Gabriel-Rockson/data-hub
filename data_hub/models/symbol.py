from enum import Enum


class Symbol(str, Enum):
    # Major Forex Pairs
    EURUSD = "EURUSD"
    GBPUSD = "GBPUSD"
    USDJPY = "USDJPY"
    USDCHF = "USDCHF"
    USDCAD = "USDCAD"
    AUDUSD = "AUDUSD"
    NZDUSD = "NZDUSD"

    # Minor/Cross Forex Pairs
    EURGBP = "EURGBP"
    EURJPY = "EURJPY"
    GBPJPY = "GBPJPY"
    AUDJPY = "AUDJPY"
    CHFJPY = "CHFJPY"
    EURAUD = "EURAUD"
    EURNZD = "EURNZD"

    # Commodities
    XAUUSD = "XAUUSD"  # Gold
    XAGUSD = "XAGUSD"  # Silver
    WTIUSD = "WTIUSD"  # Oil (West Texas Intermediate)
    BRENTUSD = "BRENTUSD"  # Brent Oil

    # Indices
    US30 = "US30"  # Dow Jones
    NAS100 = "NAS100"  # Nasdaq
    SPX500 = "SPX500"  # S&P 500
    GER40 = "GER40"  # DAX
    UK100 = "UK100"  # FTSE 100
    JPN225 = "JPN225"  # Nikkei

    # Cryptocurrencies (spot symbols)
    BTCUSD = "BTCUSD"
    ETHUSD = "ETHUSD"
    LTCUSD = "LTCUSD"
    XRPUSD = "XRPUSD"

    def __str__(self):
        return self.value
