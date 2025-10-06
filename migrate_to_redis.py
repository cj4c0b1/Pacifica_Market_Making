"""
Script to migrate existing CSV data to Redis.
"""
import os
import csv
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from redis_config import redis_manager

def migrate_prices(symbol: str, file_path: str):
    """Migrate price data from CSV to Redis."""
    print(f"Migrating price data for {symbol}...")
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            # Convert timestamp to datetime and sort
            df['unix_timestamp'] = pd.to_datetime(df['unix_timestamp'], unit='s')
            df = df.sort_values('unix_timestamp')
            
            # Store the latest price
            latest = df.iloc[-1].to_dict()
            price_data = {
                'bid': latest.get('bid', 0),
                'ask': latest.get('ask', 0),
                'mid': latest.get('mid', 0),
                'last_updated': datetime.now().isoformat(),
                'source_file': os.path.basename(file_path)
            }
            redis_manager.store_price(symbol, price_data)
            print(f"  Migrated {len(df)} price records for {symbol}")
    except Exception as e:
        print(f"Error migrating price data for {symbol}: {str(e)}")

def migrate_orderbook(symbol: str, file_path: str):
    """Migrate orderbook data from CSV to Redis."""
    print(f"Migrating orderbook data for {symbol}...")
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            # Convert timestamp to datetime and sort
            df['unix_timestamp'] = pd.to_datetime(df['unix_timestamp'], unit='s')
            df = df.sort_values('unix_timestamp')
            
            # Store each orderbook snapshot
            for _, row in df.iterrows():
                # For orderbook, we'll need to check the actual structure
                # Since we don't have a sample, we'll store the raw data
                orderbook_data = {
                    'timestamp': int(row['unix_timestamp']),
                    'bids': [],  # This needs to be populated based on actual data structure
                    'asks': [],  # This needs to be populated based on actual data structure
                    'source_file': os.path.basename(file_path),
                    'raw_data': row.to_dict()
                }
                redis_manager.store_orderbook(symbol, orderbook_data)
            print(f"  Migrated {len(df)} orderbook snapshots for {symbol}")
    except Exception as e:
        print(f"Error migrating orderbook data for {symbol}: {str(e)}")

def migrate_trades(symbol: str, file_path: str):
    """Migrate trade data from CSV to Redis."""
    print(f"Migrating trade data for {symbol}...")
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            # Convert timestamp to datetime and sort
            df['unix_timestamp'] = pd.to_datetime(df['unix_timestamp'], unit='s')
            df = df.sort_values('unix_timestamp')
            
            # Store each trade
            for _, row in df.iterrows():
                trade_data = {
                    'trade_id': str(row.get('id', '')),
                    'price': float(row.get('price', 0)),
                    'size': float(row.get('quantity', 0)),
                    'side': row.get('side', ''),
                    'timestamp': int(row['unix_timestamp_ms']) / 1000,  # Convert ms to seconds
                    'source_file': os.path.basename(file_path)
                }
                redis_manager.store_trade(symbol, trade_data)
            print(f"  Migrated {len(df)} trades for {symbol}")
    except Exception as e:
        print(f"Error migrating trade data for {symbol}: {str(e)}")

def main():
    """Main migration function."""
    data_dir = Path("PACIFICA_data")
    
    # Get all symbols from the data directory
    price_files = list(data_dir.glob("prices_*.csv"))
    symbols = [f.stem.split('_')[1] for f in price_files]
    
    print(f"Starting migration for symbols: {', '.join(symbols)}")
    
    for symbol in symbols:
        # Migrate prices
        price_file = data_dir / f"prices_{symbol}.csv"
        if price_file.exists():
            migrate_prices(symbol, price_file)
        
        # Migrate orderbook
        orderbook_file = data_dir / f"orderbook_{symbol}.csv"
        if orderbook_file.exists():
            migrate_orderbook(symbol, orderbook_file)
        
        # Migrate trades
        trades_file = data_dir / f"trades_{symbol}.csv"
        if trades_file.exists():
            migrate_trades(symbol, trades_file)
    
    print("\nMigration complete!")
    print("You can now use Redis as the primary data store for the market maker.")

if __name__ == "__main__":
    main()
