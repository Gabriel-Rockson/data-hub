from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    postgres_db: str = "market_data"
    postgres_user: str = "postgres"
    postgres_password: str = "password"  # noqa: S105
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    db_pool_size: int = 20
    db_max_overflow: int = 50
    db_pool_timeout: int = 30
    db_pool_recycle: int = 3600
    db_pool_pre_ping: bool = True

    csv_data_root: str = str(PROJECT_ROOT / "data")

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()  # type: ignore
