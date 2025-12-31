import streamlit as st
from database import fetch_series_list
from pipeline import trigger_data_ingestion, trigger_preprocessing
from kafka_monitor import monitor_kafka_topics
from config import config

def render_sidebar():
    """Render enhanced sidebar with pipeline controls"""
    st.header("🚀 Pipeline Controls")
    
    # New series ingestion
    new_series_id = st.text_input(
        "📥 New Series ID",
        placeholder="AAPL, TSLA, BTC-USD",
        help="Trigger Kafka data ingestion"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📥 Fetch Data", type="primary", use_container_width=True) and new_series_id:
            trigger_data_ingestion(new_series_id)
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Available series
    st.header("📊 Available Series")
    series_list = fetch_series_list()
    
    if series_list:
        selected_series = st.selectbox("Select Series", series_list, index=0)
        
        if st.button("🔮 Preprocess Series", use_container_width=True):
            trigger_preprocessing(selected_series)
    else:
        st.info("⏳ No processed series yet")
        st.markdown(f"**Watch topics:** `{config.KAFKA_INGESTION_TOPIC}` → `{config.KAFKA_PREPROCESSING_TOPIC}`")
    
    st.markdown("---")
    
    # Kafka monitoring
    st.header("📨 Kafka Status")
    kafka_status = monitor_kafka_topics()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Ingestion Topic", kafka_status.get("ingestion_topic", "N/A"))
    with col2:
        st.metric("Preprocessing Topic", kafka_status.get("preprocessing_topic", "N/A"))
    
    if kafka_status.get("recent_messages", 0) > 0:
        st.success(f"🔔 {kafka_status['recent_messages']} recent messages!")
    
    st.markdown("---")
    st.caption("🔗 Kafka-UI: http://localhost:8080")
    
    return series_list[0] if series_list else None
