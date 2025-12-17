import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import json
import requests

# Page configuration
st.set_page_config(
    page_title="Time Series Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f0f2f6;
        border-radius: 10px 10px 0 0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4CAF50;
        color: white;
    }
    h1 {
        color: #1f77b4;
        font-weight: 700;
    }
    h2, h3 {
        color: #2c3e50;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    </style>
""", unsafe_allow_html=True)

# Configuration
PREPROCESSING_API_URL = "http://preprocessing:8000"

# Database connection
@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="timescaledb",
            port=5432,
            database="timeseries",
            user="tsuser",
            password="ts_password"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Data fetching functions
@st.cache_data(ttl=60)
def fetch_series_list():
    conn = get_db_connection()
    if not conn:
        return []
    query = "SELECT DISTINCT series_id FROM time_series_preprocessed ORDER BY series_id"
    df = pd.read_sql(query, conn)
    return df['series_id'].tolist()

@st.cache_data(ttl=60)
def fetch_time_series_data(series_id, table='time_series_preprocessed', hours=168):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = f"""
        SELECT timestamp, open, high, low, close, volume, features
        FROM {table}
        WHERE series_id = %s 
        AND timestamp >= NOW() - INTERVAL '{hours} hours'
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, conn, params=(series_id,))
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@st.cache_data(ttl=60)
def calculate_statistics(df):
    if df.empty:
        return {}
    
    stats = {
        'mean_close': df['close'].mean(),
        'std_close': df['close'].std(),
        'min_close': df['close'].min(),
        'max_close': df['close'].max(),
        'total_volume': df['volume'].sum(),
        'avg_volume': df['volume'].mean(),
        'price_change': ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0] * 100) if len(df) > 0 else 0,
        'volatility': df['close'].pct_change().std() * 100
    }
    return stats

# API interaction functions
def validate_series(series_id):
    """Call preprocessing API to validate series"""
    try:
        response = requests.get(f"{PREPROCESSING_API_URL}/validate/{series_id}", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"API returned status {response.status_code}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def preprocess_series(series_id, config):
    """Call preprocessing API to preprocess series"""
    try:
        response = requests.post(
            f"{PREPROCESSING_API_URL}/preprocess",
            json={
                "series_id": series_id,
                "interpolation_method": config.get("interpolation_method", "linear"),
                "outlier_method": config.get("outlier_method", "zscore"),
                "outlier_threshold": config.get("outlier_threshold", 3.0),
                "resample_frequency": config.get("resample_frequency"),
                "aggregation_method": config.get("aggregation_method", "mean")
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"API returned status {response.status_code}: {response.text}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def create_features(series_id, config):
    """Call preprocessing API to create features"""
    try:
        response = requests.post(
            f"{PREPROCESSING_API_URL}/features",
            json={
                "series_id": series_id,
                "lag_features": config.get("lag_features"),
                "rolling_window_sizes": config.get("rolling_window_sizes")
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"API returned status {response.status_code}: {response.text}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

# Visualization functions
def create_candlestick_chart(df):
    fig = go.Figure(data=[go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC'
    )])
    
    fig.update_layout(
        title='Price Movement (Candlestick)',
        yaxis_title='Price',
        xaxis_title='Time',
        template='plotly_white',
        height=500,
        hovermode='x unified',
        xaxis_rangeslider_visible=False
    )
    return fig

def create_volume_chart(df):
    colors = ['red' if close < open else 'green' 
              for close, open in zip(df['close'], df['open'])]
    
    fig = go.Figure(data=[go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        marker_color=colors,
        name='Volume'
    )])
    
    fig.update_layout(
        title='Trading Volume',
        yaxis_title='Volume',
        xaxis_title='Time',
        template='plotly_white',
        height=300,
        hovermode='x unified'
    )
    return fig

def create_correlation_heatmap(df):
    corr_data = df[['open', 'high', 'low', 'close', 'volume']].corr()
    
    fig = go.Figure(data=go.Heatmap(
        z=corr_data.values,
        x=corr_data.columns,
        y=corr_data.columns,
        colorscale='RdBu',
        zmid=0,
        text=corr_data.values.round(2),
        texttemplate='%{text}',
        textfont={"size": 12},
        colorbar=dict(title="Correlation")
    ))
    
    fig.update_layout(
        title='Feature Correlation Matrix',
        template='plotly_white',
        height=400
    )
    return fig

def create_price_distribution(df):
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df['close'],
        nbinsx=50,
        name='Price Distribution',
        marker_color='#1f77b4'
    ))
    
    fig.update_layout(
        title='Price Distribution',
        xaxis_title='Close Price',
        yaxis_title='Frequency',
        template='plotly_white',
        height=400,
        showlegend=False
    )
    return fig

def create_moving_averages(df):
    df['MA_7'] = df['close'].rolling(window=7).mean()
    df['MA_30'] = df['close'].rolling(window=30).mean()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], 
                             name='Close Price', line=dict(color='blue', width=1)))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['MA_7'], 
                             name='7-Period MA', line=dict(color='orange', width=2)))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['MA_30'], 
                             name='30-Period MA', line=dict(color='red', width=2)))
    
    fig.update_layout(
        title='Price with Moving Averages',
        yaxis_title='Price',
        xaxis_title='Time',
        template='plotly_white',
        height=500,
        hovermode='x unified'
    )
    return fig

# Main app
def main():
    st.title("📊 Time Series Analytics Dashboard")
    st.markdown("### Real-time monitoring and statistical analysis")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        series_list = fetch_series_list()
        if not series_list:
            st.error("No time series data available")
            return
        
        selected_series = st.selectbox(
            "Select Time Series",
            series_list,
            index=0
        )
        
        time_range = st.selectbox(
            "Time Range",
            ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "Last 90 Days"],
            index=1
        )
        
        time_mapping = {
            "Last 24 Hours": 24,
            "Last 7 Days": 168,
            "Last 30 Days": 720,
            "Last 90 Days": 2160
        }
        
        table_choice = st.radio(
            "Data Source",
            ["Preprocessed", "Raw"],
            index=0
        )
        
        table_name = "time_series_preprocessed" if table_choice == "Preprocessed" else "time_series_raw"
        
        st.markdown("---")
        st.subheader("🔧 Actions")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        if st.button("✅ Validate Series", use_container_width=True):
            with st.spinner("Validating..."):
                result = validate_series(selected_series)
                if "error" in result:
                    st.error(f"Validation failed: {result['error']}")
                else:
                    st.session_state['validation_result'] = result
                    st.success("Validation complete!")
    
    # Fetch data
    hours = time_mapping[time_range]
    df = fetch_time_series_data(selected_series, table_name, hours)
    
    if df.empty:
        st.warning(f"No data available for {selected_series}")
        return
    
    # Calculate statistics
    stats = calculate_statistics(df)
    
    # Metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Current Price",
            f"${stats['mean_close']:.2f}",
            f"{stats['price_change']:.2f}%"
        )
    
    with col2:
        st.metric(
            "Volatility",
            f"{stats['volatility']:.2f}%"
        )
    
    with col3:
        st.metric(
            "Min Price",
            f"${stats['min_close']:.2f}"
        )
    
    with col4:
        st.metric(
            "Max Price",
            f"${stats['max_close']:.2f}"
        )
    
    with col5:
        st.metric(
            "Avg Volume",
            f"{stats['avg_volume']:,.0f}"
        )
    
    st.markdown("---")
    
    # Tabs for different visualizations
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Price Charts", 
        "📊 Statistical Analysis", 
        "🔍 Correlation", 
        "🛠️ Preprocessing",
        "📋 Data Table"
    ])
    
    with tab1:
        st.plotly_chart(create_candlestick_chart(df), use_container_width=True)
        st.plotly_chart(create_volume_chart(df), use_container_width=True)
        st.plotly_chart(create_moving_averages(df), use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(create_price_distribution(df), use_container_width=True)
            
            st.subheader("📊 Summary Statistics")
            stats_df = pd.DataFrame({
                'Metric': ['Mean', 'Std Dev', 'Min', 'Max', 'Range'],
                'Close': [
                    f"${stats['mean_close']:.2f}",
                    f"${stats['std_close']:.2f}",
                    f"${stats['min_close']:.2f}",
                    f"${stats['max_close']:.2f}",
                    f"${stats['max_close'] - stats['min_close']:.2f}"
                ],
                'Volume': [
                    f"{stats['avg_volume']:,.0f}",
                    f"{df['volume'].std():,.0f}",
                    f"{df['volume'].min():,.0f}",
                    f"{df['volume'].max():,.0f}",
                    f"{df['volume'].max() - df['volume'].min():,.0f}"
                ]
            })
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
        
        with col2:
            # Price change over time
            df_temp = df.copy()
            df_temp['price_change_pct'] = df_temp['close'].pct_change() * 100
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_temp['timestamp'],
                y=df_temp['price_change_pct'],
                mode='lines',
                name='Price Change %',
                line=dict(color='purple', width=2)
            ))
            fig.update_layout(
                title='Price Change % Over Time',
                yaxis_title='Change %',
                xaxis_title='Time',
                template='plotly_white',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Daily range analysis
            df_temp['daily_range'] = df_temp['high'] - df_temp['low']
            
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=df_temp['timestamp'],
                y=df_temp['daily_range'],
                name='Daily Range',
                marker_color='lightblue'
            ))
            fig2.update_layout(
                title='Daily Price Range',
                yaxis_title='Range',
                xaxis_title='Time',
                template='plotly_white',
                height=400
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        st.plotly_chart(create_correlation_heatmap(df), use_container_width=True)
        
        # Show validation results if available
        if 'validation_result' in st.session_state:
            st.subheader("✅ Validation Results")
            val_result = st.session_state['validation_result']
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Points", val_result.get('total_points', 'N/A'))
            with col2:
                st.metric("Missing Values", val_result.get('missing_values', 'N/A'))
            with col3:
                st.metric("Missing %", f"{val_result.get('missing_percentage', 0):.2f}%")
            
            if 'date_range' in val_result:
                st.write("**Date Range:**")
                st.json(val_result['date_range'])
            
            if 'value_stats' in val_result:
                st.write("**Value Statistics:**")
                st.json(val_result['value_stats'])
        
        st.subheader("📈 Feature Insights")
        col1, col2 = st.columns(2)
        
        with col1:
            if 'features' in df.columns:
                st.write("**Available Features:**")
                sample_features = df['features'].iloc[0] if not df.empty else {}
                if isinstance(sample_features, str):
                    try:
                        sample_features = json.loads(sample_features)
                    except:
                        sample_features = {}
                
                if sample_features:
                    st.json(sample_features)
                else:
                    st.info("No additional features available")
        
        with col2:
            st.write("**Correlation Insights:**")
            corr_data = df[['open', 'high', 'low', 'close', 'volume']].corr()
            high_corr = []
            for i in range(len(corr_data.columns)):
                for j in range(i+1, len(corr_data.columns)):
                    if abs(corr_data.iloc[i, j]) > 0.8:
                        high_corr.append(f"{corr_data.columns[i]} ↔ {corr_data.columns[j]}: {corr_data.iloc[i, j]:.3f}")
            
            if high_corr:
                for item in high_corr:
                    st.write(f"• {item}")
            else:
                st.info("No strong correlations (>0.8) found")
    
    with tab4:
        st.header("🛠️ Data Preprocessing & Feature Engineering")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Preprocessing Configuration")
            
            interp_method = st.selectbox(
                "Interpolation Method",
                ["linear", "polynomial", "spline", "forward_fill", "backward_fill"],
                help="Method for handling missing values"
            )
            
            outlier_method = st.selectbox(
                "Outlier Detection",
                ["zscore", "iqr", "isolation_forest"],
                help="Method for detecting outliers"
            )
            
            outlier_threshold = st.slider(
                "Outlier Threshold",
                min_value=1.0,
                max_value=5.0,
                value=3.0,
                step=0.1,
                help="Threshold for outlier detection"
            )
            
            resample_freq = st.selectbox(
                "Resample Frequency (optional)",
                [None, "H", "D", "W", "M"],
                format_func=lambda x: "None" if x is None else {"H": "Hourly", "D": "Daily", "W": "Weekly", "M": "Monthly"}[x],
                help="Frequency for resampling data"
            )
            
            agg_method = st.selectbox(
                "Aggregation Method",
                ["mean", "median", "sum", "min", "max"],
                help="Method for aggregating resampled data"
            )
            
            if st.button("▶️ Run Preprocessing", use_container_width=True, type="primary"):
                with st.spinner("Processing..."):
                    config = {
                        "interpolation_method": interp_method,
                        "outlier_method": outlier_method,
                        "outlier_threshold": outlier_threshold,
                        "resample_frequency": resample_freq,
                        "aggregation_method": agg_method
                    }
                    
                    result = preprocess_series(selected_series, config)
                    
                    if "error" in result:
                        st.markdown(f'<div class="error-box">❌ Error: {result["error"]}</div>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="success-box">✅ Preprocessing complete!<br>Series ID: {result.get("series_id")}<br>Data Points: {result.get("data_points")}</div>', unsafe_allow_html=True)
                        st.json(result.get('metadata', {}))
                        st.cache_data.clear()
        
        with col2:
            st.subheader("Feature Engineering")
            
            lag_features_input = st.text_input(
                "Lag Features (comma-separated)",
                value="1,7,30",
                help="Create lag features (e.g., 1,7,30 for 1-day, 7-day, 30-day lags)"
            )
            
            rolling_windows_input = st.text_input(
                "Rolling Window Sizes (comma-separated)",
                value="7,14,30",
                help="Create rolling statistics (e.g., 7,14,30 for 7-day, 14-day, 30-day windows)"
            )
            
            st.markdown("**Generated Features:**")
            st.info("""
            - Lag features: Previous values at specified intervals
            - Rolling mean: Average over window
            - Rolling std: Standard deviation over window
            - Rolling min/max: Min/max over window
            - Time features: Hour, day, month, etc.
            """)
            
            if st.button("▶️ Create Features", use_container_width=True, type="primary"):
                with st.spinner("Creating features..."):
                    try:
                        lag_features = [int(x.strip()) for x in lag_features_input.split(",")] if lag_features_input else None
                        rolling_windows = [int(x.strip()) for x in rolling_windows_input.split(",")] if rolling_windows_input else None
                        
                        config = {
                            "lag_features": lag_features,
                            "rolling_window_sizes": rolling_windows
                        }
                        
                        result = create_features(selected_series, config)
                        
                        if "error" in result:
                            st.markdown(f'<div class="error-box">❌ Error: {result["error"]}</div>', unsafe_allow_html=True)
                        else:
                            st.markdown(f'<div class="success-box">✅ Features created!<br>Total Features: {len(result.get("features", []))}<br>Rows: {result.get("rows")}</div>', unsafe_allow_html=True)
                            st.write("**Created Features:**")
                            st.write(", ".join(result.get('features', [])))
                            st.cache_data.clear()
                    except ValueError as e:
                        st.markdown(f'<div class="error-box">❌ Invalid input: {str(e)}</div>', unsafe_allow_html=True)
    
    with tab5:
        st.subheader("📋 Raw Data View")
        st.dataframe(
            df.style.format({
                'open': '${:.2f}',
                'high': '${:.2f}',
                'low': '${:.2f}',
                'close': '${:.2f}',
                'volume': '{:,.0f}'
            }),
            use_container_width=True,
            height=600
        )
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{selected_series}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()