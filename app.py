import sys
import os
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from config import config
from components.sidebar import render_sidebar
from components.pipeline_status import render_pipeline_status
from database import (
    get_connection_pool, 
    fetch_series_list, 
    get_job_history,
    get_active_jobs,
    trigger_pipeline
)

# Page config
st.set_page_config(
    page_title=config.DASHBOARD_TITLE,
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.pipeline-status {
    padding: 1.5rem;
    border-radius: 12px;
    margin: 1rem 0;
    font-weight: 600;
}
.status-waiting { 
    background: linear-gradient(135deg, #fff3cd 0%, #ffeeba 100%); 
    border-left: 5px solid #ffc107; 
    color: #856404; 
}
.status-processing { 
    background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%); 
    border-left: 5px solid #17a2b8; 
    color: #0c5460; 
}
.status-ready { 
    background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); 
    border-left: 5px solid #28a745; 
    color: #155724; 
}
.status-error { 
    background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%); 
    border-left: 5px solid #dc3545; 
    color: #721c24; 
}
.metric-card {
    background: white;
    padding: 1rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
</style>
""", unsafe_allow_html=True)


def render_pipeline_trigger():
    """Render pipeline trigger section"""
    st.markdown("## 🚀 Start Pipeline")
    
    with st.form("trigger_pipeline_form"):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            series_id = st.text_input(
                "Series ID",
                placeholder="e.g., SERIES_001",
                help="Enter the time series identifier to process"
            )
        
        with col2:
            preprocessing_method = st.selectbox(
                "Preprocessing Method",
                ["auto", "seasonal_decompose", "differencing", "normalization"]
            )
        
        submitted = st.form_submit_button("▶️ Start Pipeline", use_container_width=True)
        
        if submitted and series_id:
            with st.spinner("Triggering pipeline..."):
                result = trigger_pipeline(
                    series_id=series_id,
                    config_data={"method": preprocessing_method}
                )
                
                if result["success"]:
                    st.success(f"✅ {result['message']}")
                    st.info(f"Job ID: `{result['job_id']}`")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ {result['message']}")


def render_active_jobs():
    """Render currently running jobs"""
    st.markdown("## 🔄 Active Jobs")
    
    active_df = get_active_jobs()
    
    if active_df.empty:
        st.info("No active jobs currently running")
    else:
        # Format duration
        active_df['duration'] = active_df['running_seconds'].apply(
            lambda x: f"{int(x // 60)}m {int(x % 60)}s" if pd.notna(x) else "N/A"
        )
        
        # Display active jobs
        for idx, row in active_df.iterrows():
            col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
            
            with col1:
                st.markdown(f"**{row['series_id']}**")
            with col2:
                st.markdown(f"🔄 `{row['stage']}`")
            with col3:
                st.markdown(f"⏱️ {row['duration']}")
            with col4:
                st.markdown(f"🆔 `{row['job_id'][:8]}...`")


def render_job_history():
    """Render job history table"""
    st.markdown("## 📊 Pipeline History")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        limit = st.selectbox("Show last", [10, 20, 50, 100], index=1)
    with col2:
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()
    
    history_df = get_job_history(limit=limit)
    
    if history_df.empty:
        st.info("No job history available")
    else:
        # Format duration
        history_df['duration'] = history_df['duration_seconds'].apply(
            lambda x: f"{int(x // 60)}m {int(x % 60)}s" if pd.notna(x) and x > 0 else "N/A"
        )
        
        # Status emoji mapping
        status_emoji = {
            'completed': '✅',
            'failed': '❌',
            'running': '🔄'
        }
        history_df['status_display'] = history_df['status'].map(
            lambda x: f"{status_emoji.get(x, '⚪')} {x}"
        )
        
        # Display table
        display_df = history_df[[
            'job_id', 'series_id', 'status_display', 'stage', 
            'duration', 'created_at', 'error_message'
        ]].rename(columns={
            'job_id': 'Job ID',
            'series_id': 'Series',
            'status_display': 'Status',
            'stage': 'Stage',
            'duration': 'Duration',
            'created_at': 'Started At',
            'error_message': 'Error'
        })
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Job ID": st.column_config.TextColumn(width="small"),
                "Error": st.column_config.TextColumn(width="medium")
            }
        )


def render_metrics_overview():
    """Render key metrics"""
    from database import get_pipeline_status
    
    status = get_pipeline_status()
    active_df = get_active_jobs()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📥 Raw Records",
            value=f"{status.get('raw_count', 0):,}"
        )
    
    with col2:
        st.metric(
            label="✅ Processed Records",
            value=f"{status.get('prep_count', 0):,}"
        )
    
    with col3:
        st.metric(
            label="📊 Available Series",
            value=status.get('series_count', 0)
        )
    
    with col4:
        st.metric(
            label="🔄 Active Jobs",
            value=len(active_df)
        )


def main():
    st.title(config.DASHBOARD_TITLE)
    st.markdown("### Kafka-powered: Ingestion → Preprocessing → Analytics")
    
    # Auto-refresh toggle
    col1, col2 = st.columns([4, 1])
    with col1:
        render_pipeline_status()
    with col2:
        auto_refresh = st.checkbox("Auto-refresh", value=False)
        if auto_refresh:
            import time
            time.sleep(5)
            st.rerun()
    
    # Metrics overview
    render_metrics_overview()
    
    st.markdown("---")
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🚀 Start Pipeline", 
        "🔄 Active Jobs", 
        "📊 Job History",
        "📈 Analytics"
    ])
    
    with tab1:
        render_pipeline_trigger()
        
        # Workflow diagram
        st.markdown("### Pipeline Workflow")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            #### 1. 📥 Ingestion
            ```
            Dashboard → Ingestion API
                 ↓
            Kafka: data.ingestion.completed
                 ↓
            time_series_raw populated
            ```
            """)
        
        with col2:
            st.markdown("""
            #### 2. 🔄 Preprocessing
            ```
            Listen: data.ingestion.completed
                 ↓
            Preprocessing service
                 ↓
            Kafka: data.preprocessing.completed
            ```
            """)
        
        with col3:
            st.markdown("""
            #### 3. 📊 Analytics
            ```
            Listen: data.preprocessing.completed
                 ↓
            Forecasting + Anomaly (parallel)
                 ↓
            Kafka: data.forecast.completed
            ```
            """)
    
    with tab2:
        render_active_jobs()
    
    with tab3:
        render_job_history()
    
    with tab4:
        # Sidebar for series selection
        selected_series = render_sidebar()
        
        if selected_series:
            st.success(f"✅ Analyzing **{selected_series}**")
            
            # Quick data preview
            connection_pool = get_connection_pool("preprocessing")
            if connection_pool:
                conn = None
                try:
                    conn = connection_pool.getconn()
                    query = """
                    SELECT * FROM time_series_preprocessed
                    WHERE series_id = %s
                    ORDER BY timestamp DESC
                    LIMIT 100
                    """
                    df = pd.read_sql(query, conn, params=(selected_series,))
                    
                    if not df.empty:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        # Plot closing price
                        st.line_chart(df.set_index('timestamp')[['open', 'high', 'low', 'close']])
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("No data available for this series")
                except Exception as e:
                    st.error(f"❌ Failed to fetch data: {e}")
                finally:
                    if conn:
                        connection_pool.putconn(conn)
        else:
            st.info("👈 Select a series from the sidebar to view analytics")


if __name__ == "__main__":
    main()
