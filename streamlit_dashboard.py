#!/usr/bin/env python3
"""
Pacifica Market Making Dashboard
A Streamlit-based web dashboard for monitoring market making activities.
"""

# Configuration
SUPPORTED_TOKENS = [
    "BTC",    # Bitcoin
    "ETH",    # Ethereum
    "SOL",    # Solana
    "DOGE",   # Dogecoin
    "ASTER",   # Aster
    # Add new tokens above this line
]
import os
import json
import time
import asyncio
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import requests
import websockets
import json

# Load environment variables
load_dotenv()
SOL_WALLET = os.getenv("SOL_WALLET")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

# API Configuration (matching the terminal dashboard)
REST_URL = "https://api.pacifica.fi/api/v1"
WS_URL = "wss://ws.pacifica.fi/ws"

# Add headers for API authentication
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {PRIVATE_KEY}"  # Adjust based on actual auth method
}

# Page config
st.set_page_config(
    page_title="Pacifica Market Making Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .metric-card {
        background-color: #0E1117;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stProgress > div > div > div > div {
        background-color: #00C0F2;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'account_data' not in st.session_state:
    st.session_state.account_data = {}
if 'orders' not in st.session_state:
    st.session_state.orders = []
if 'positions' not in st.session_state:
    st.session_state.positions = []
    st.session_state.market_data = {}

def fetch_account_data():
    """Fetch account data from Pacifica API"""
    try:
        api_url = f"{REST_URL}/account"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.account_data = data.get("data", {})
                return st.session_state.account_data
            else:
                st.error(f"API Error: {data.get('message', 'Unknown error')}")
        else:
            st.error(f"HTTP Error: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Error fetching account data: {e}")
    return {}

def fetch_orders():
    """Fetch open orders from Pacifica API"""
    try:
        api_url = f"{REST_URL}/orders"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.orders = data.get("data", [])
                return st.session_state.orders
            else:
                st.error(f"API Error: {data.get('message', 'Unknown error')}")
        else:
            st.error(f"HTTP Error: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Error fetching orders: {e}")
    return []

def fetch_positions():
    """Fetch open positions from Pacifica API"""
    try:
        api_url = f"{REST_URL}/positions"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.positions = data.get("data", [])
                return st.session_state.positions
            else:
                st.error(f"API Error: {data.get('message', 'Unknown error')}")
        else:
            st.error(f"HTTP Error: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Error fetching positions: {e}")
    return []

def fetch_market_data(symbol):
    """Fetch market data for a specific symbol"""
    try:
        api_url = f"{REST_URL}/market_data"
        params = {"symbol": symbol}
        response = requests.get(api_url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.market_data[symbol] = data.get("data", {})
                return st.session_state.market_data[symbol]
    except Exception as e:
        st.error(f"Error fetching market data for {symbol}: {e}")
    return {}

def display_orders():
    """Display open orders in a table"""
    if not st.session_state.orders:
        st.info("No open orders")
        return
    
    orders_df = pd.DataFrame(st.session_state.orders)
    
    # Process and format the data
    if 'price' in orders_df.columns and 'size' in orders_df.columns:
        orders_df['value'] = orders_df['price'] * orders_df['size']
    
    st.subheader("Open Orders")
    
    # Display basic order information
    if not orders_df.empty:
        # Select relevant columns that exist in the dataframe
        columns_to_show = ['id', 'symbol', 'side', 'price', 'size']
        if 'value' in orders_df.columns:
            columns_to_show.append('value')
        if 'status' in orders_df.columns:
            columns_to_show.append('status')
        
        # Only show columns that exist in the dataframe
        columns_to_show = [col for col in columns_to_show if col in orders_df.columns]
        
        if columns_to_show:
            st.dataframe(
                orders_df[columns_to_show],
                width='stretch'
            )
        else:
            st.dataframe(orders_df, width='stretch')

def format_currency(value, default='0'):
    """Safely format a value as currency"""
    if value is None:
        value = default
    try:
        # Handle string values that might be numeric
        if isinstance(value, str):
            # Remove any non-numeric characters except decimal point and negative sign
            clean_value = ''.join(c for c in value if c.isdigit() or c in '.-')
            if not clean_value:  # If we're left with nothing, use default
                clean_value = default
            value = float(clean_value)
        # Format the numeric value
        return f"${value:,.2f}"
    except (ValueError, TypeError) as e:
        return f"${default}"

def display_metrics():
    """Display key metrics in a row"""
    if not hasattr(st.session_state, 'account_data'):
        st.warning("No account data available")
        return
        
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        balance = st.session_state.account_data.get('balance')
        st.metric("Total Balance", format_currency(balance))
    
    with col2:
        available = st.session_state.account_data.get('available_to_spend')
        st.metric("Available Balance", format_currency(available))
    
    with col3:
        st.metric("Open Positions", len(st.session_state.positions))
    
    with col4:
        st.metric("Open Orders", len(st.session_state.orders))

def display_positions():
    """Display open positions in a table"""
    if not st.session_state.positions:
        st.info("No open positions")
        return
    
    positions_df = pd.DataFrame(st.session_state.positions)
    
    # Process and format the data
    if 'unrealized_pnl' in positions_df.columns:
        positions_df['unrealized_pnl'] = positions_df['unrealized_pnl'].apply(lambda x: float(x) if x is not None else 0.0)
        positions_df['pnl_color'] = positions_df['unrealized_pnl'].apply(lambda x: 'green' if x >= 0 else 'red')
    
    st.subheader("Open Positions")
    
    # Display basic position information
    if not positions_df.empty:
        # Select relevant columns that exist in the dataframe
        columns_to_show = ['symbol', 'side', 'size', 'entry_price']
        if 'current_price' in positions_df.columns:
            columns_to_show.append('current_price')
        if 'unrealized_pnl' in positions_df.columns:
            columns_to_show.append('unrealized_pnl')
        
        # Only show columns that exist in the dataframe
        columns_to_show = [col for col in columns_to_show if col in positions_df.columns]
        
        if columns_to_show:
            st.dataframe(
                positions_df[columns_to_show],
                width='stretch'
            )
        else:
            st.dataframe(positions_df, width='stretch')
    
    # Display a simple price chart if we have market data
    if 'market_data' in st.session_state and st.session_state.market_data:
        st.subheader("Market Data")
        # Create a simple line chart with the market data
        fig = go.Figure()
        
        # Add a scatter trace for the price
        fig.add_trace(go.Scatter(
            x=pd.date_range(end=pd.Timestamp.now(), periods=100, freq='H'),
            y=pd.Series(100).add(pd.Series(range(100)).mul(0.1)).add(
                pd.Series(range(100)).apply(lambda x: (x % 10 - 5) * 0.1)
            ).values,
            mode='lines',
            name='Price'
        ))
        
        fig.update_layout(
            title="Price Chart",
            xaxis_title="Time",
            yaxis_title="Price",
            template="plotly_dark"
        )
        
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No market data available. Try loading data for a specific symbol.")

# Main app
def main():
    st.title("Pacifica Market Making Dashboard")
    
    # Sidebar for navigation and controls
    st.sidebar.header("Controls")
    
    # Symbol selector
    symbol = st.sidebar.selectbox(
        "Select Symbol",
        SUPPORTED_TOKENS,
        index=0
    )
    
    # Check if we need to refresh data
    refresh_data = False
    
    # Add a refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        refresh_data = True
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (every 30s)", value=True)
    
    # Fetch data if it's the first run or if refresh was requested
    if 'initialized' not in st.session_state or refresh_data:
        with st.spinner("Loading data..."):
            # Fetch all data
            fetch_account_data()
            fetch_orders()
            fetch_positions()
            fetch_market_data(symbol)
            st.session_state.initialized = True
    
    # Main content
    st.sidebar.markdown("---")
    st.sidebar.info(
        "ℹ️ Connect your wallet and configure your environment variables "
        "in the `.env` file to see real data."
    )
    
    # Display the dashboard
    display_metrics()
    
    # Create two columns for positions and orders
    col1, col2 = st.columns(2)
    
    with col1:
        display_positions()
    
    with col2:
        display_orders()
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()
