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
    PREPROCESSING_SERVICE_URL: str = os.getenv("PREPROCESSING_SERVICE_URL", "http://preprocessing:8000")
    FORECASTING_SERVICE_URL: str = os.getenv("FORECASTING_SERVICE_URL", "http://forecasting:8001")
    
    # Kafka - FIXED with default_factory
    KAFKA_BOOTSTRAP_SERVERS: List[str] = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","))
    KAFKA_INGESTION_TOPIC: str = "data.ingestion.completed"
    KAFKA_PREPROCESSING_TOPIC: str = "data.preprocessing.completed"
    
    # Dashboard
    DASHBOARD_TITLE: str = "🔄 Event-Driven Time Series Pipeline"

# Global config instance
config = Config()
