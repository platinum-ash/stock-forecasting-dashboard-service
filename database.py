import psycopg2
import pandas as pd
from typing import List, Dict, Optional
from config import config
import streamlit as st

@st.cache_resource(ttl=300)
def get_db_connection():
    """Database connection with retry logic"""
    try:
        conn = psycopg2.connect(
            host=config.DATABASE_HOST,
            port=config.DATABASE_PORT,
            database=config.DATABASE_NAME,
            user=config.DATABASE_USER,
            password=config.DATABASE_PASSWORD,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None

def get_pipeline_status() -> Dict:
    """Get comprehensive pipeline status"""
    conn = get_db_connection()
    if not conn:
        return {"status": "error", "message": "Database unavailable"}
    
    try:
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
        conn.close()
        
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
        return {"status": "error", "message": f"Status check failed: {e}"}

@st.cache_data(ttl=30)
def fetch_series_list() -> List[str]:
    """Fetch available series from preprocessed table"""
    status = get_pipeline_status()
    
    # Sicheren Zugriff mit .get()
    if status.get("series_count", 0) == 0:
        return []
    
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
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
