import os
import time
from redis_config import redis_manager

def test_redis_connection():
    """Test Redis connection and basic operations."""
    try:
        # Test connection
        print("Testing Redis connection...")
        print(f"Redis server: {redis_manager.redis_client}")
        
        # Ping test
        print("\nPinging Redis server...")
        ping_response = redis_manager.redis_client.ping()
        print(f"Ping response: {ping_response}")
        
        if not ping_response:
            print("❌ Failed to connect to Redis")
            return False
        
        # Basic set/get test
        print("\nTesting basic key-value operations...")
        test_key = "pacificat:test:connection"
        test_value = f"Test successful at {time.ctime()}"
        
        # Set a test key
        redis_manager.redis_client.set(test_key, test_value)
        
        # Get the test key
        retrieved_value = redis_manager.redis_client.get(test_key)
        print(f"Set value: {test_value}")
        print(f"Got value: {retrieved_value}")
        
        if retrieved_value == test_value:
            print("✅ Redis connection test passed!")
            return True
        else:
            print("❌ Redis connection test failed: Retrieved value doesn't match")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Redis: {str(e)}")
        return False

if __name__ == "__main__":
    test_redis_connection()
