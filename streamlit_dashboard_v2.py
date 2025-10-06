"""
Streamlit Dashboard for Pacifica Market Making

A real-time dashboard to monitor Pacifica account status including:
- Account balance and margin information
- Open positions with PnL
- Active orders
- Recent trading activity
"""
import streamlit as st
import asyncio
import json
import time
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import requests
from dotenv import load_dotenv
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from pacifica_sdk.common.constants import REST_URL, WS_URL

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('streamlit_dashboard.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
SOL_WALLET = os.getenv("SOL_WALLET")

# Streamlit page config
st.set_page_config(
    page_title="Pacifica Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {font-size:24px; color: #1f77b4; font-weight: bold;}
    .section-header {font-size:20px; color: #2e86c1; margin-top: 20px;}
    .positive {color: #27ae60;}
    .negative {color: #e74c3c;}
    .metric-value {font-size: 18px; font-weight: bold;}
    .stDataFrame {font-size: 14px;}
    </style>
""", unsafe_allow_html=True)

# Initialize session state for persistent data
if 'account_info' not in st.session_state:
    st.session_state.account_info = {}
if 'open_orders' not in st.session_state:
    st.session_state.open_orders = []
if 'positions' not in st.session_state:
    st.session_state.positions = []
if 'recent_events' not in st.session_state:
    st.session_state.recent_events = []
if 'mid_prices' not in st.session_state:
    st.session_state.mid_prices = {}

def fetch_account_info():
    """Fetch account information from the REST API"""
    try:
        api_url = f"{REST_URL}/account"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.account_info = data.get("data", {})
                logger.info("Account info updated")
            else:
                logger.warning(f"Account API returned success=false: {data}")
    except Exception as e:
        logger.error(f"Error fetching account info: {e}")

def fetch_open_orders():
    """Fetch open orders from the REST API"""
    try:
        api_url = f"{REST_URL}/orders"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.open_orders = data.get("data", [])
                logger.info(f"Fetched {len(st.session_state.open_orders)} open orders")
            else:
                logger.warning(f"Orders API returned success=false: {data}")
    except Exception as e:
        logger.error(f"Error fetching open orders: {e}")

def fetch_positions():
    """Fetch positions from the REST API"""
    try:
        api_url = f"{REST_URL}/positions"
        params = {"account": SOL_WALLET}
        response = requests.get(api_url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                st.session_state.positions = data.get("data", [])
                logger.info(f"Fetched {len(st.session_state.positions)} positions")
            else:
                logger.warning(f"Positions API returned success=false: {data}")
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")

def format_currency(value):
    """Format currency values with 2 decimal places"""
    return f"${float(value or 0):,.2f}"

def format_pnl(value):
    """Format PnL values with color coding"""
    value = float(value or 0)
    color = "positive" if value >= 0 else "negative"
    sign = "+" if value >= 0 else ""
    return f"<span class='{color}'>{sign}${abs(value):,.2f}</span>"

def render_metrics():
    """Render account metrics at the top of the dashboard"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Balance", 
                 format_currency(st.session_state.account_info.get('balance', 0)))
    
    with col2:
        st.metric("Available Balance", 
                 format_currency(st.session_state.account_info.get('available_to_spend', 0)))
    
    with col3:
        st.metric("Margin Used", 
                 format_currency(st.session_state.account_info.get('total_margin_used', 0)))
    
    with col4:
        # Calculate total unrealized PnL from positions
        total_pnl = sum(float(pos.get('unrealized_pnl', 0)) for pos in st.session_state.positions)
        st.metric("Unrealized PnL", 
                 format_currency(total_pnl),
                 delta=format_currency(total_pnl - st.session_state.get('last_pnl', 0)),
                 delta_color="normal")
        st.session_state.last_pnl = total_pnl

def render_positions():
    """Render positions table"""
    st.subheader("Positions")
    
    if not st.session_state.positions:
        st.info("No open positions")
        return
    
    # Prepare positions data for display
    positions_data = []
    for pos in st.session_state.positions:
        symbol = pos.get('symbol', 'N/A')
        side = pos.get('side', '').capitalize()
        size = float(pos.get('size', 0))
        entry_price = float(pos.get('entry_price', 0))
        mark_price = float(pos.get('mark_price', 0) or entry_price)
        unrealized_pnl = float(pos.get('unrealized_pnl', 0))
        
        positions_data.append({
            'Symbol': symbol,
            'Side': side,
            'Size': size,
            'Entry Price': entry_price,
            'Mark Price': mark_price,
            'Unrealized PnL': unrealized_pnl,
            'P&L %': (unrealized_pnl / (size * entry_price) * 100) if size * entry_price > 0 else 0
        })
    
    # Create and display DataFrame with formatting
    if positions_data:
        df = pd.DataFrame(positions_data)
        
        # Format the DataFrame
        format_dict = {
            'Entry Price': '{:,.2f}',
            'Mark Price': '{:,.2f}',
            'Unrealized PnL': '{:+,.2f}',
            'P&L %': '{:+.2f}%'
        }
        
        # Apply styling
        def color_pnl(val):
            color = 'green' if val >= 0 else 'red'
            return f'color: {color}'
        
        styled_df = df.style\
            .format(format_dict)\
            .map(color_pnl, subset=['Unrealized PnL', 'P&L %'])
        
        st.dataframe(styled_df, width='stretch')

def render_orders():
    """Render open orders table"""
    st.subheader("Open Orders")
    
    if not st.session_state.open_orders:
        st.info("No open orders")
        return
    
    # Prepare orders data for display
    orders_data = []
    for order in st.session_state.open_orders:
        orders_data.append({
            'Order ID': order.get('order_id', 'N/A'),
            'Symbol': order.get('symbol', 'N/A'),
            'Side': order.get('side', '').capitalize(),
            'Price': float(order.get('price', 0)),
            'Size': float(order.get('size', 0)),
            'Filled': float(order.get('filled_size', 0)),
            'Type': order.get('order_type', '').capitalize(),
            'Time': datetime.fromtimestamp(order.get('timestamp', 0)/1000).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Create and display DataFrame with formatting
    if orders_data:
        df = pd.DataFrame(orders_data)
        
        # Format the DataFrame
        format_dict = {
            'Price': '{:,.2f}',
            'Size': '{:,.4f}',
            'Filled': '{:,.4f}'
        }
        
        # Color code buy/sell orders
        def color_side(val):
            color = 'green' if val.lower() == 'buy' else 'red'
            return f'color: {color}'
        
        styled_df = df.style\
            .format(format_dict)\
            .map(color_side, subset=['Side'])
        
        st.dataframe(styled_df, width='stretch')

def render_recent_activity():
    """Render recent trading activity"""
    st.subheader("Recent Activity")
    
    if not st.session_state.recent_events:
        st.info("No recent activity")
        return
    
    # Display recent events (last 10)
    for event in st.session_state.recent_events[-10:]:
        event_type = event.get('type', '').replace('_', ' ').title()
        event_time = datetime.fromtimestamp(event.get('timestamp', 0)/1000).strftime('%H:%M:%S')
        
        # Different styling based on event type
        if 'filled' in event_type.lower():
            st.success(f"{event_time} - {event_type}: {event.get('symbol')} {event.get('side')} {event.get('filled_size')} @ {event.get('price')}")
        elif 'cancel' in event_type.lower():
            st.warning(f"{event_time} - {event_type}: {event.get('order_id')}")
        else:
            st.info(f"{event_time} - {event_type}")

def main():
    """Main function to run the Streamlit app"""
    st.title("📊 Pacifica Trading Dashboard")
    st.markdown(f"*Account: `{SOL_WALLET[:8]}...{SOL_WALLET[-8:]}` | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    
    # Add a refresh button
    if st.button("🔄 Refresh Data"):
        with st.spinner("Refreshing data..."):
            fetch_account_info()
            fetch_positions()
            fetch_open_orders()
    
    # Display metrics
    render_metrics()
    
    # Two-column layout for positions and orders
    col1, col2 = st.columns([2, 1])
    
    with col1:
        render_positions()
    
    with col2:
        render_orders()
    
    # Recent activity at the bottom
    render_recent_activity()
    
    # Auto-refresh every 5 seconds
    time.sleep(5)  # Wait for 5 seconds before next refresh
    st.rerun()

if __name__ == "__main__":
    # Initial data fetch
    if not st.session_state.account_info:
        with st.spinner("Loading initial data..."):
            fetch_account_info()
            fetch_positions()
            fetch_open_orders()
    
    # Run the main app
    main()
    
    # Add a small delay to prevent excessive API calls
    time.sleep(1)
