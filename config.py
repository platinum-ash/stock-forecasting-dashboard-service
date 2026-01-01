import os
from dataclasses import dataclass, field
from typing import List
from urllib.parse import urlparse

@dataclass
class Config:
    # Database URLs (PostgreSQL connection strings)
    STATUS_DATABASE_URL: str = os.getenv("STATUS_DATABASE_URL", "postgresql://tsuser:ts_password@timescaledb:5432/timeseries")
    INGESTION_DATABASE_URL: str = os.getenv("INGESTION_DATABASE_URL", "postgresql://tsuser:ts_password@ingestion-db:5432/ingestion")
    PREPROCESSING_DATABASE_URL: str = os.getenv("PREPROCESSING_DATABASE_URL", "postgresql://tsuser:ts_password@preprocessing-db:5432/preprocessing")
    FORECASTING_DATABASE_URL: str = os.getenv("FORECASTING_DATABASE_URL", "postgresql://tsuser:ts_password@forecasting-db:5432/forecasting")
    ANOMALY_DATABASE_URL: str = os.getenv("ANOMALY_DATABASE_URL", "postgresql://tsuser:ts_password@anomaly-db:5432/anomaly")
    
    # Services
    INGESTION_SERVICE_URL: str = os.getenv("INGESTION_SERVICE_URL", "http://ingestion:8009/yahoo")
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
    AUTO_REFRESH_INTERVAL: int = int(os.getenv("AUTO_REFRESH_INTERVAL", "5"))
    JOB_HISTORY_LIMIT: int = int(os.getenv("JOB_HISTORY_LIMIT", "20"))

config = Config()
