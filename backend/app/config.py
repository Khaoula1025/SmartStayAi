"""
app/config.py
=============
All settings loaded from environment variables or a .env file.
Uses pydantic-settings so every value is typed and validated at startup.

Create a .env file in the backend/ directory:
  DB_HOST=localhost
  DB_PORT=5432
  DB_NAME=smartstay
  DB_USER=smartstay
  DB_PASSWORD=yourpassword
  JWT_SECRET_KEY=change-this-in-production
"""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Application ───────────────────────────────────────────────────────
    APP_NAME:    str = 'SmartStay Intelligence API'
    APP_VERSION: str = '1.0.0'
    DEBUG:       bool = False
    HOTEL:       str = 'hickstead'
    TOT_ROOMS:   int = 52

    # ── Database ──────────────────────────────────────────────────────────
    DB_HOST:     str = 'localhost'
    DB_PORT:     int = 5432
    DB_NAME:     str = 'smartstay'
    DB_USER:     str = 'smartstay'
    DB_PASSWORD: str = ''

    @property
    def DATABASE_URL(self) -> str:
        return (
            f'postgresql://{self.DB_USER}:{self.DB_PASSWORD}'
            f'@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}'
        )

    # ── JWT ───────────────────────────────────────────────────────────────
    JWT_SECRET_KEY:          str = 'smartstay-dev-secret-change-in-prod'
    JWT_ALGORITHM:           str = 'HS256'
    JWT_EXPIRE_MINUTES:      int = 480   # 8 hours

    # ── CORS ──────────────────────────────────────────────────────────────
    CORS_ORIGINS: list[str] = [
        'http://localhost:3000',
        'http://localhost:5173',
    ]

    # ── Data paths ────────────────────────────────────────────────────────
    # Root of the smartstay-intelligence project (one level up from backend/)
    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

    @property
    def PREDICTION_CSV(self) -> Path:
        return self.PROJECT_ROOT / 'data' / 'prediction' / 'predictions_2026.csv'

    @property
    def METRICS_JSON(self) -> Path:
        return self.PROJECT_ROOT / 'data' / 'prediction' / 'model_metrics.json'

    @property
    def COMPARISON_JSON(self) -> Path:
        return self.PROJECT_ROOT / 'data' / 'prediction' / 'model_comparison.json'

    # ── Airflow (for pipeline trigger) ────────────────────────────────────
    AIRFLOW_BASE_URL: str = 'http://localhost:8080'
    AIRFLOW_USER:     str = 'admin'
    AIRFLOW_PASSWORD: str = 'admin'
    AIRFLOW_DAG_ID:   str = 'smartstay_hickstead_daily'

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent / '.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance — import this everywhere."""
    return Settings()