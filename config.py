import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # Database
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "timescaledb")
    DATABASE_PORT: int = int(os.getenv("DATABASE_PORT", "5432"))
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "timeseries")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "tsuser")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "ts_password")
    
    # Services
    INGESTION_SERVICE_URL: str = os.getenv("INGESTION_SERVICE_URL", "http://ingestion:8000")
    PREPROCESSING_SERVICE_URL: str = os.getenv("PREPROCESSING_SERVICE_URL", "http://preprocessing:8000")
    FORECASTING_SERVICE_URL: str = os.getenv("FORECASTING_SERVICE_URL", "http://forecasting:8001")
    ANOMALY_SERVICE_URL: str = os.getenv("ANOMALY_SERVICE_URL", "http://anomaly:8002")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: List[str] = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","))
    KAFKA_INGESTION_TOPIC: str = "data.ingestion.completed"
    KAFKA_PREPROCESSING_TOPIC: str = "data.preprocessing.completed"
    KAFKA_FORECAST_TOPIC: str = "data.forecast.completed"
    KAFKA_ANOMALY_TOPIC: str = "data.anomaly.completed"
    
    # Dashboard
    DASHBOARD_TITLE: str = "🔄 Event-Driven Time Series Pipeline"
    AUTO_REFRESH_INTERVAL: int = int(os.getenv("AUTO_REFRESH_INTERVAL", "5"))  # seconds
    JOB_HISTORY_LIMIT: int = int(os.getenv("JOB_HISTORY_LIMIT", "20"))


# Global config instance
config = Config()
