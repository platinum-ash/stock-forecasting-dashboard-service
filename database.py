import psycopg2
from psycopg2 import pool
import pandas as pd
from typing import List, Dict, Optional
from config import config
import streamlit as st
import logging


@st.cache_resource
def get_connection_pool():
    """Create a persistent connection pool"""
    try:
        return pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=config.DATABASE_HOST,
            port=config.DATABASE_PORT,
            database=config.DATABASE_NAME,
            user=config.DATABASE_USER,
            password=config.DATABASE_PASSWORD,
            connect_timeout=10
        )
    except Exception as e:
        st.error(f"❌ Connection pool creation failed: {e}")
        return None


def get_pipeline_status() -> Dict:
    """Get comprehensive pipeline status"""
    connection_pool = get_connection_pool()
    if not connection_pool:
        return {"status": "error", "message": "Database unavailable"}
    
    conn = None
    try:
        conn = connection_pool.getconn()
        cursor = conn.cursor()
        
        # Raw data status
        cursor.execute("SELECT COUNT(*) FROM time_series_raw;")
        raw_count = cursor.fetchone()[0] or 0
        
        # Preprocessed status
        cursor.execute("SELECT COUNT(*) FROM time_series_preprocessed;")
        prep_count = cursor.fetchone()[0] or 0
        
        # Series availability
        cursor.execute("""
            SELECT COUNT(DISTINCT series_id) as series_count
            FROM time_series_preprocessed 
            WHERE series_id IS NOT NULL
        """)
        series_count = cursor.fetchone()[0] or 0
        
        cursor.close()
        
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
        logging.error(e)
        return {"status": "error", "message": f"Status check failed: {e}"}
    
    finally:
        if conn:
            connection_pool.putconn(conn)


@st.cache_data(ttl=30)
def fetch_series_list() -> List[str]:
    """Fetch available series from preprocessed table"""
    status = get_pipeline_status()
    
    if status.get("series_count", 0) == 0:
        return []
    
    connection_pool = get_connection_pool()
    if not connection_pool:
        return []
    
    conn = None
    try:
        conn = connection_pool.getconn()
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
            connection_pool.putconn(conn)


def get_job_history(limit: int = 20) -> pd.DataFrame:
    """Fetch recent pipeline job history"""
    connection_pool = get_connection_pool()
    if not connection_pool:
        return pd.DataFrame()
    
    conn = None
    try:
        conn = connection_pool.getconn()
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
            connection_pool.putconn(conn)


def get_active_jobs() -> pd.DataFrame:
    """Fetch currently running jobs"""
    connection_pool = get_connection_pool()
    if not connection_pool:
        return pd.DataFrame()
    
    conn = None
    try:
        conn = connection_pool.getconn()
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
            connection_pool.putconn(conn)


def trigger_pipeline(series_id: str, config_data: Dict = None) -> Dict:
    """Trigger pipeline via ingestion service API"""
    import requests
    
    try:
        payload = {
            "series_id": series_id,
            "preprocessing_config": config_data or {}
        }
        
        response = requests.post(
            f"{config.INGESTION_SERVICE_URL}/api/ingest",
            json=payload,
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
