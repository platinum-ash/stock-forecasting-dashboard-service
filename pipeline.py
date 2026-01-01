import requests
import streamlit as st
from config import config
from typing import Dict
import pandas as pd



def trigger_data_ingestion(series_id: str) -> bool:
    """Trigger Kafka ingestion for new series"""
    st.info(f"Triggering data ingestion for {series_id}...")
    
    params = {
        "interval": "30m",
        "period": "2mo"
    }
    
    try:
        response = requests.get(
            f"{config.INGESTION_SERVICE_URL}/{series_id}/fetch_store",
            params=params,
            timeout=10
        )
        if response.status_code in [200, 202]:
            st.success(f"Data ingestion triggered for {series_id}")
            st.balloons()
            return True
        else:
            st.error(f"Service error: {response.status_code}")
    except requests.RequestException as e:
        st.warning(f"Connection error: {e}")
    
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
            st.success(f"Preprocessing triggered → {config.KAFKA_PREPROCESSING_TOPIC}")
            return True
    except:
        st.warning(f"Simulated preprocessing trigger for {series_id}")
    
    return False
