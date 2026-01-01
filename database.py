import psycopg2
from psycopg2 import pool
import pandas as pd
from typing import List, Dict
from config import config
import streamlit as st
import logging

@st.cache_resource
def get_connection_pool(db_type: str = "status"):
    """Create persistent connection pools for different databases"""
    try:
        db_url_map = {
            "status": config.STATUS_DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://"),
            "ingestion": config.INGESTION_DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://"),
            "preprocessing": config.PREPROCESSING_DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://"),
            "forecasting": config.FORECASTING_DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://"),
            "anomaly": config.ANOMALY_DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://")
        }

        
        if db_type not in db_url_map:
            raise ValueError(f"Unknown database type: {db_type}")
        
        db_url = db_url_map[db_type]
        
        return pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=db_url,
            connect_timeout=10
        )
    except Exception as e:
        st.error(f"❌ Connection pool creation failed for {db_type}: {e}")
        return None

def get_pipeline_status() -> Dict:
    """Get comprehensive pipeline status from multiple databases"""
    status_pool = get_connection_pool("status")
    ingestion_pool = get_connection_pool("ingestion")
    preprocessing_pool = get_connection_pool("preprocessing")
    
    if not all([status_pool, ingestion_pool, preprocessing_pool]):
        return {"status": "error", "message": "One or more databases unavailable"}
    
    conn_raw = None
    conn_prep = None
    try:
        # Get raw data count from ingestion database
        conn_raw = ingestion_pool.getconn()
        cursor_raw = conn_raw.cursor()
        cursor_raw.execute("SELECT COUNT(*) FROM time_series_raw;")
        raw_count = cursor_raw.fetchone()[0] or 0
        cursor_raw.close()
        
        # Get preprocessed data from preprocessing database
        conn_prep = preprocessing_pool.getconn()
        cursor_prep = conn_prep.cursor()
        cursor_prep.execute("SELECT COUNT(*) FROM time_series_preprocessed;")
        prep_count = cursor_prep.fetchone()[0] or 0
        
        cursor_prep.execute("""
            SELECT COUNT(DISTINCT series_id) as series_count
            FROM time_series_preprocessed 
            WHERE series_id IS NOT NULL
        """)
        series_count = cursor_prep.fetchone()[0] or 0
        cursor_prep.close()
        
        if series_count > 0:
            status = "ready"
            message = f"✅ {series_count} series ready for analysis"
        elif raw_count > 0:
            status = "processing"
            message = f"🔄 {raw_count} raw records - preprocessing pending"
        else:
            status = "waiting"
            message = "⏳ Waiting for data ingestion"
            
        return {
            "status": status,
            "raw_count": raw_count,
            "prep_count": prep_count,
            "series_count": series_count,
            "message": message,
            "timestamp": pd.Timestamp.now().isoformat()
        }
    
    except Exception as e:
        logging.error(f"Status check failed: {e}")
        return {"status": "error", "message": f"Status check failed: {e}"}
    
    finally:
        if conn_raw:
            ingestion_pool.putconn(conn_raw)
        if conn_prep:
            preprocessing_pool.putconn(conn_prep)

@st.cache_data(ttl=30)
def fetch_series_list() -> List[str]:
    """Fetch available series from preprocessed database"""
    status = get_pipeline_status()
    
    if status.get("series_count", 0) == 0:
        return []
    
    preprocessing_pool = get_connection_pool("preprocessing")
    if not preprocessing_pool:
        return []
    
    conn = None
    try:
        conn = preprocessing_pool.getconn()
        query = """
            SELECT DISTINCT series_id 
            FROM time_series_preprocessed 
            WHERE series_id IS NOT NULL 
            ORDER BY series_id
        """
        df = pd.read_sql(query, conn)
        return df['series_id'].dropna().tolist()
    except Exception as e:
        st.error(f"❌ Failed to fetch series: {e}")
        return []
    finally:
        if conn:
            preprocessing_pool.putconn(conn)

def get_job_history(limit: int = 20) -> pd.DataFrame:
    """Fetch recent pipeline job history from status database"""
    status_pool = get_connection_pool("status")
    if not status_pool:
        return pd.DataFrame()
    
    conn = None
    try:
        conn = status_pool.getconn()
        query = """
            SELECT 
                job_id,
                series_id,
                status,
                stage,
                created_at,
                updated_at,
                EXTRACT(EPOCH FROM (updated_at - created_at)) as duration_seconds,
                error_message
            FROM pipeline_jobs
            ORDER BY created_at DESC
            LIMIT %s
        """
        df = pd.read_sql(query, conn, params=(limit,))
        return df
    except Exception as e:
        logging.error(f"Failed to fetch job history: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            status_pool.putconn(conn)

def get_active_jobs() -> pd.DataFrame:
    """Fetch currently running jobs from status database"""
    status_pool = get_connection_pool("status")
    if not status_pool:
        return pd.DataFrame()
    
    conn = None
    try:
        conn = status_pool.getconn()
        query = """
            SELECT 
                job_id,
                series_id,
                stage,
                created_at,
                EXTRACT(EPOCH FROM (NOW() - created_at)) as running_seconds
            FROM pipeline_jobs
            WHERE status = 'running'
            ORDER BY created_at DESC
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logging.error(f"Failed to fetch active jobs: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            status_pool.putconn(conn)

def trigger_pipeline(series_id: str, config_data: Dict = None) -> Dict:
    """Trigger pipeline via ingestion service API"""
    import requests
    
    try:

        params = {
            "interval": "5m",
            "period": "3mo"
        }
        
        response = requests.get(
            f"{config.INGESTION_SERVICE_URL}/{series_id}/fetch_store",
            params=params,
            timeout=10
        )
        response.raise_for_status()
        
        result = response.json()
        return {
            "success": True,
            "job_id": result.get("job_id"),
            "message": f"Pipeline started for {series_id}"
        }
    except Exception as e:
        logging.error(f"Pipeline trigger failed: {e}")
        return {
            "success": False,
            "message": f"Failed to trigger pipeline: {e}"
        }
