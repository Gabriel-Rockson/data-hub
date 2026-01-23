from sqlalchemy import BigInteger, Column, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MarketData(Base):
    __tablename__ = "market_data"

    time = Column(TIMESTAMP, primary_key=True, nullable=False)
    broker = Column(String(20), primary_key=True, nullable=False)
    symbol = Column(String(20), primary_key=True, nullable=False)
    timeframe = Column(String(10), primary_key=True, nullable=False)
    open = Column(Numeric(12, 6), nullable=False)
    high = Column(Numeric(12, 6), nullable=False)
    low = Column(Numeric(12, 6), nullable=False)
    close = Column(Numeric(12, 6), nullable=False)
    volume = Column(BigInteger, default=0)
    tick_volume = Column(BigInteger, default=0)
    spread = Column(Integer, default=0)
