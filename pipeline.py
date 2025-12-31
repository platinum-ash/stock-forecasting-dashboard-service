import requests
import streamlit as st
from config import config
from typing import Dict

def trigger_data_ingestion(series_id: str) -> bool:
    """Trigger Kafka ingestion for new series"""
    st.info(f"🚀 Triggering data ingestion for {series_id}...")
    
    payload = {
        "series_id": series_id,
        "action": "fetch_raw_data",
        "timestamp": pd.Timestamp.now().isoformat(),
        "source": "yahoo_finance"
    }
    
    # In production, this would POST to ingestion service
    # For now, simulate Kafka message
    try:
        response = requests.post(
            f"{config.PREPROCESSING_SERVICE_URL}/trigger-ingestion",
            json=payload,
            timeout=10
        )
        if response.status_code in [200, 202]:
            st.success(f"✅ Kafka message sent to {config.KAFKA_INGESTION_TOPIC}")
            st.balloons()
            return True
        else:
            st.error(f"❌ Service error: {response.status_code}")
    except requests.RequestException as e:
        st.warning(f"⚠️ Simulation mode: Kafka message for {series_id} → {config.KAFKA_INGESTION_TOPIC}")
        st.success("👈 Check Kafka-UI at http://localhost:8080")
    
    return False

def trigger_preprocessing(series_id: str) -> bool:
    """Trigger preprocessing pipeline"""
    payload = {
        "series_id": series_id,
        "action": "preprocess",
        "timestamp": pd.Timestamp.now().isoformat()
    }
    
    try:
        response = requests.post(
            f"{config.PREPROCESSING_SERVICE_URL}/trigger-preprocessing",
            json=payload,
            timeout=10
        )
        if response.status_code in [200, 202]:
            st.success(f"✅ Preprocessing triggered → {config.KAFKA_PREPROCESSING_TOPIC}")
            return True
    except:
        st.warning(f"⚠️ Simulated preprocessing trigger for {series_id}")
    
    return False
