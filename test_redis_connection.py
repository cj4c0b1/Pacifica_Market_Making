#!/usr/bin/env python3
"""
Test Redis connection and basic operations.
"""
import os
import sys
import time
from redis_config import redis_manager

def test_redis_connection():
    """Test Redis connection and basic operations."""
    try:
        # Test connection
        redis_client = redis_manager.redis_client
        print("Testing Redis connection...")
        
        # Ping test
        print(f"PING: {redis_client.ping()}")
        
        # Set a test key
        test_key = "test:connection"
        redis_client.set(test_key, "Hello, Redis!")
        print(f"Set test key '{test_key}': {redis_client.get(test_key)}")
        
        # Set expiration
        redis_client.expire(test_key, 10)  # 10 seconds
        print(f"Set expiration on test key. TTL: {redis_client.ttl(test_key)} seconds")
        
        # Test hash operations
        test_hash = "test:hash"
        redis_client.hset(test_hash, mapping={
            "field1": "value1",
            "field2": "42",
            "timestamp": str(time.time())
        })
        print(f"Test hash: {redis_client.hgetall(test_hash)}")
        
        # Clean up
        redis_client.delete(test_key, test_hash)
        print("Test keys cleaned up.")
        
        # Test data storage methods
        symbol = "BTCUSDT"
        price_data = {
            "bid": "50000.00",
            "ask": "50001.00",
            "last": "50000.50",
            "volume": "1000.5",
            "timestamp": str(time.time())
        }
        
        # Store price data
        redis_manager.store_price(symbol, price_data)
        print(f"Stored price data for {symbol}")
        
        # Retrieve price data
        latest_price = redis_manager.get_latest_price(symbol)
        print(f"Retrieved price data: {latest_price}")
        
        return True
        
    except Exception as e:
        print(f"Error testing Redis connection: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    if test_redis_connection():
        print("\n✅ Redis connection test successful!")
    else:
        print("\n❌ Redis connection test failed.")
        sys.exit(1)
