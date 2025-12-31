import streamlit as st
from database import get_pipeline_status
from datetime import datetime

def render_pipeline_status():
    """Render real-time pipeline status"""
    status = get_pipeline_status()
    
    status_class = status['status']
    
    st.markdown(f'''
<div class="pipeline-status status-{status.get('status', 'error')}">
    <h2>Pipeline Status</h2>
    <div style="font-size: 1.2em; margin: 1rem 0;">
        <strong>{status.get('message', 'Status unavailable')}</strong>
    </div>
    <div style="display: flex; gap: 2rem; font-size: 1.1em;">
        <span>📥 Raw: <strong>{status.get('raw_count', 0):,}</strong></span>
        <span>🔄 Processed: <strong>{status.get('prep_count', 0):,}</strong></span>
        <span>📊 Series: <strong>{status.get('series_count', 0)}</strong></span>
    </div>
    <small>Updated: {datetime.now().strftime('%H:%M:%S')}</small>
</div>
''', unsafe_allow_html=True)

