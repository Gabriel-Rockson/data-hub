import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(
        self,
        database_url: str,
        pool_size: int = 20,
        max_overflow: int = 50,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
    ):
        self.database_url = database_url

        self.engine = create_engine(
            self.database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
            echo=False,
        )

        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        """Create all tables defined in models"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def create_hypertable(self) -> None:
        """Create TimescaleDB hypertable after table creation"""
        try:
            with self.engine.connect() as conn:
                check_hypertable = text("""
                    SELECT EXISTS (
                        SELECT 1 FROM timescaledb_information.hypertables
                        WHERE hypertable_name = 'market_data'
                    );
                """)

                result = conn.execute(check_hypertable).scalar()

                if not result:
                    create_hypertable_sql = text("""
                        SELECT create_hypertable('market_data', 'time',
                            chunk_time_interval => INTERVAL '1 week',
                            if_not_exists => TRUE
                        );
                    """)
                    conn.execute(create_hypertable_sql)
                    logger.info("Hypertable created successfully")

                    add_dimension_sql = text("""
                        SELECT add_dimension('market_data', 'symbol',
                            number_partitions => 4,
                            if_not_exists => TRUE
                        );
                    """)
                    conn.execute(add_dimension_sql)
                    logger.info("Space partitioning added successfully")
                else:
                    logger.info("Hypertable already exists")

                create_indexes_sql = text("""
                    CREATE INDEX IF NOT EXISTS idx_market_data_broker_symbol_tf
                    ON market_data (broker, symbol, timeframe, time DESC);
                """)
                conn.execute(create_indexes_sql)

                create_symbol_index_sql = text("""
                    CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time
                    ON market_data (symbol, time DESC);
                """)
                conn.execute(create_symbol_index_sql)

                conn.commit()
                logger.info("Indexes created successfully")

        except Exception as e:
            logger.error(f"Error creating hypertable: {e}")
            raise

    def setup_compression(self) -> None:
        """Setup TimescaleDB compression for better storage efficiency"""
        try:
            with self.engine.connect() as conn:
                compression_sql = text("""
                    ALTER TABLE market_data SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'broker,symbol,timeframe',
                        timescaledb.compress_orderby = 'time DESC'
                    );
                """)
                conn.execute(compression_sql)

                compression_policy_sql = text("""
                    SELECT add_compression_policy('market_data', INTERVAL '3 days');
                """)
                conn.execute(compression_policy_sql)

                conn.commit()
                logger.info("Compression setup completed")

        except Exception as e:
            logger.warning(f"Compression setup failed (this is normal if already configured): {e}")

    @contextmanager
    def get_session(self) -> Generator[sessionmaker]:
        """Get a database session with automatic cleanup"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_pool_status(self) -> dict:
        """Get connection pool status for monitoring"""
        pool = self.engine.pool
        return {
            "size": getattr(pool, "size", lambda: "unknown")(),
            "checked_in": getattr(pool, "checkedin", lambda: "unknown")(),
            "checked_out": getattr(pool, "checkedout", lambda: "unknown")(),
            "overflow": getattr(pool, "overflow", lambda: "unknown")(),
            "invalid": getattr(pool, "invalid", lambda: "unavailable")(),
        }

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                return result == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def close_connections(self) -> None:
        """Close engine connections"""
        self.engine.dispose()
        logger.info("Database connections closed")
