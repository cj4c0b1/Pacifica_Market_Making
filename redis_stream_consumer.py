#!/usr/bin/env python3
"""
Redis Stream Consumer for Market Data

This script reads and displays data from Redis streams created by data_collector_redis.py
"""
import redis
import json
import time
from datetime import datetime

def format_timestamp(ts):
    """Convert timestamp to human-readable format."""
    try:
        return datetime.fromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    except (ValueError, TypeError):
        return ts

def print_stream_data(stream_name, data):
    """Print stream data in a readable format."""
    print(f"\n📊 Stream: {stream_name}")
    print("-" * 50)
    
    for stream_id, messages in data:
        print(f"\n🆔 {stream_id.decode() if isinstance(stream_id, bytes) else stream_id}")
        for message_id, message in messages:
            print(f"\n💬 Message ID: {message_id.decode() if isinstance(message_id, bytes) else message_id}")
            for key, value in message.items():
                key = key.decode() if isinstance(key, bytes) else key
                value = value.decode() if isinstance(value, bytes) else value
                print(f"   {key}: {value}")

def main():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    print("🔍 Redis Stream Consumer")
    print("Press Ctrl+C to exit\n")
    
    # Last ID for each stream
    last_ids = {}
    
    try:
        while True:
            streams = {
                'market:BTC:prices': last_ids.get('prices', '0-0'),
                'market:BTC:orderbook': last_ids.get('orderbook', '0-0'),
                'market:BTC:trades': last_ids.get('trades', '0-0')
            }
            
            # Only read new messages
            messages = r.xread(streams, count=5, block=5000)
            
            if messages:
                for stream_name, stream_messages in messages:
                    stream_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    if stream_messages:
                        # Update last seen ID
                        last_id = stream_messages[-1][0]
                        stream_type = stream_name.split(':')[-1]
                        last_ids[stream_type] = last_id
                        
                        # Print the data
                        print_stream_data(stream_name, [(last_id, stream_messages[-1][1])])
            else:
                print("⏳ Waiting for data... (Press Ctrl+C to exit)")
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Exiting consumer...")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
