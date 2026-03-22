from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class TickData:
    timestamp: datetime
    broker: str
    symbol: str
    bid: float
    ask: float
