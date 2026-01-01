try:
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from config import config
import streamlit as st
from typing import Dict

def monitor_kafka_topics() -> Dict:
    """Monitor Kafka topic lag and message counts"""
    if not KAFKA_AVAILABLE:
        return {
            "ingestion_topic": config.KAFKA_INGESTION_TOPIC,
            "preprocessing_topic": config.KAFKA_PREPROCESSING_TOPIC,
            "status": "disabled",
            "requires": "pip install kafka-python"
        }
    
    try:
        consumer = KafkaConsumer(
            config.KAFKA_INGESTION_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='dashboard-monitor',
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        
        # Poll for recent messages
        poll_result = consumer.poll(timeout_ms=1000)
        message_count = sum(len(records) for records in poll_result.values())
        
        consumer.close()
        
        return {
            "ingestion_topic": config.KAFKA_INGESTION_TOPIC,
            "preprocessing_topic": config.KAFKA_PREPROCESSING_TOPIC,
            "recent_messages": message_count,
            "status": "active"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "topics": [config.KAFKA_INGESTION_TOPIC, config.KAFKA_PREPROCESSING_TOPIC]
        }
