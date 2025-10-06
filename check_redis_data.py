#!/usr/bin/env python3
"""
Check data stored in Redis.
"""
import sys
from redis_config import redis_manager

def check_redis_data():
    """Check data stored in Redis."""
    try:
        redis_client = redis_manager.redis_client
        
        # Get all keys
        all_keys = redis_client.keys('*')
        print(f"Total keys in Redis: {len(all_keys) if all_keys else 0}")
        
        if not all_keys:
            print("No data found in Redis.")
            return False
        
        # Group keys by type
        key_types = {}
        for key in all_keys:
            key_type = key.split(':')[0] if ':' in key else 'other'
            key_types[key_type] = key_types.get(key_type, 0) + 1
        
        print("\nKey types and counts:")
        for k_type, count in sorted(key_types.items()):
            print(f"- {k_type}: {count}")
        
        # Show sample data for each symbol
        symbols = ['BTC', 'ETH', 'UNI', 'BNB']
        
        for symbol in symbols:
            print(f"\n--- {symbol} Data ---")
            
            # Latest price
            price_key = f"price:{symbol}:latest"
            price_data = redis_client.hgetall(price_key)
            if price_data:
                print(f"Latest price: {price_data}")
            
            # Latest order book
            ob_key = f"orderbook:{symbol}:best"
            ob_data = redis_client.hgetall(ob_key)
            if ob_data:
                print(f"Best bid/ask: {ob_data}")
            
            # Latest trade
            trade_key = f"trades:{symbol}:latest"
            trade_data = redis_client.hgetall(trade_key)
            if trade_data:
                print(f"Latest trade: {trade_data}")
            
            # Count of price records
            price_count = redis_client.zcard(f"prices:{symbol}:timestamps")
            print(f"Price records: {price_count}")
            
            # Count of order book snapshots
            ob_count = redis_client.zcard(f"orderbook:{symbol}:timestamps")
            print(f"Order book snapshots: {ob_count}")
            
            # Count of trades
            trade_count = redis_client.zcard(f"trades:{symbol}:timestamps")
            print(f"Trade records: {trade_count}")
        
        return True
        
    except Exception as e:
        print(f"Error checking Redis data: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    if check_redis_data():
        print("\n✅ Redis data check completed successfully!")
    else:
        print("\n❌ Redis data check failed.")
        sys.exit(1)
