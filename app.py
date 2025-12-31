import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


import streamlit as st
from config import config
from components.sidebar import render_sidebar
from components.pipeline_status import render_pipeline_status
from database import get_db_connection, fetch_series_list

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
</style>
""", unsafe_allow_html=True)

def main():
    st.title(config.DASHBOARD_TITLE)
    st.markdown("### Kafka-powered: Ingestion → Preprocessing → Analytics")
    
    # Pipeline status header
    col1, col2 = st.columns([3, 1])
    with col1:
        render_pipeline_status()
    with col2:
        if st.button("🔄 Refresh All", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Sidebar
    selected_series = render_sidebar()
    
    # Main content
    if selected_series:
        st.success(f"✅ Analyzing **{selected_series}**")
        st.markdown("### 📈 Analytics Dashboard (Coming Soon)")
        st.info("Full analytics tabs will be implemented here")
        
        # Quick data preview
        conn = get_db_connection()
        if conn:
            query = f"""
            SELECT * FROM time_series_preprocessed 
            WHERE series_id = %s 
            ORDER BY timestamp DESC LIMIT 10
            """
            df = pd.read_sql(query, conn, params=(selected_series,))
            st.dataframe(df, use_container_width=True)
    else:
        st.markdown("## 🚀 Pipeline Workflow")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 1. 📥 **Data Ingestion**
            ```
            User → Kafka: data.ingestion.completed
                   ↓
            time_series_raw populated
            ```
            """)
        
        with col2:
            st.markdown("""
            ### 2. 🔄 **Preprocessing**  
            ```
            Kafka: data.preprocessing.completed
                   ↓  
            time_series_preprocessed ready
            ```
            """)
        
        with col3:
            st.markdown("""
            ### 3. 📊 **Analytics**
            ```
            Processed data → Charts/Forecasts
                   ↓
            LSTM/Prophet predictions
            ```
            """)
        
        st.markdown("---")
        st.info("👆 Enter a series ID above to start the pipeline!")

if __name__ == "__main__":
    main()
