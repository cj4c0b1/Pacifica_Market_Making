"""
Redis configuration and connection management for Pacifica Market Making.
"""
import os
import json
import redis
from typing import Optional, Dict, List, Any, Union
from datetime import datetime, timedelta

class RedisManager:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, **kwargs):
        """Initialize Redis connection.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            **kwargs: Additional Redis connection parameters
        """
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            **kwargs
        )
        
        # Key prefixes for different data types
        self.prefixes = {
            'price': 'prices',
            'orderbook': 'orderbook',
            'trades': 'trades',
            'parameters': 'params',
            'state': 'state'
        }
    
    def _get_key(self, data_type: str, symbol: str, timestamp: Optional[float] = None) -> str:
        """Generate a Redis key for the given data type and symbol."""
        if timestamp is None:
            return f"{self.prefixes[data_type]}:{symbol}"
        return f"{self.prefixes[data_type]}:{symbol}:{int(timestamp)}"
    
    def store_price(self, symbol: str, price_data: Dict[str, float]) -> bool:
        """Store price data in Redis."""
        key = self._get_key('price', symbol)
        return self.redis_client.hset(key, mapping=price_data) > 0
    
    def get_latest_price(self, symbol: str) -> Dict[str, float]:
        """Get the latest price data for a symbol."""
        key = self._get_key('price', symbol)
        return {k: float(v) for k, v in self.redis_client.hgetall(key).items()}
    
    def store_orderbook(self, symbol: str, orderbook_data: Dict[str, Any]) -> bool:
        """Store orderbook data in Redis with expiration."""
        key = self._get_key('orderbook', symbol, orderbook_data.get('timestamp'))
        # Store with 1-hour expiration
        return self.redis_client.setex(
            key,
            timedelta(hours=1),
            json.dumps(orderbook_data)
        )
    
    def get_recent_orderbooks(self, symbol: str, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get orderbook data for the last N minutes."""
        pattern = f"{self.prefixes['orderbook']}:{symbol}:*"
        keys = self.redis_client.keys(pattern)
        recent_keys = []
        now = datetime.now().timestamp()
        
        for key in keys:
            # Extract timestamp from key
            key_timestamp = int(key.split(':')[-1])
            if now - key_timestamp <= minutes * 60:  # Within last N minutes
                recent_keys.append(key)
        
        # Get and return the orderbook data
        return [json.loads(self.redis_client.get(key)) for key in recent_keys]
    
    def store_trade(self, symbol: str, trade_data: Dict[str, Any]) -> bool:
        """Store trade data in Redis."""
        key = self._get_key('trades', symbol, trade_data.get('timestamp'))
        # Store with 24-hour expiration
        return self.redis_client.setex(
            key,
            timedelta(hours=24),
            json.dumps(trade_data)
        )
    
    def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get the most recent trades for a symbol."""
        pattern = f"{self.prefixes['trades']}:{symbol}:*"
        keys = sorted(self.redis_client.keys(pattern), reverse=True)[:limit]
        return [json.loads(self.redis_client.get(key)) for key in keys]
    
    def store_parameters(self, symbol: str, params: Dict[str, float]) -> bool:
        """Store calculated parameters in Redis."""
        key = self._get_key('parameters', symbol)
        return self.redis_client.hset(key, mapping=params) > 0
    
    def get_parameters(self, symbol: str) -> Dict[str, float]:
        """Get stored parameters for a symbol."""
        key = self._get_key('parameters', symbol)
        return {k: float(v) for k, v in self.redis_client.hgetall(key).items()}
    
    def set_state(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a state value in Redis."""
        full_key = f"{self.prefixes['state']}:{key}"
        if ttl:
            return self.redis_client.setex(full_key, ttl, json.dumps(value))
        return self.redis_client.set(full_key, json.dumps(value))
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """Get a state value from Redis."""
        full_key = f"{self.prefixes['state']}:{key}"
        result = self.redis_client.get(full_key)
        return json.loads(result) if result else default
    
    def publish(self, channel: str, message: Dict[str, Any]) -> int:
        """Publish a message to a Redis channel."""
        return self.redis_client.publish(channel, json.dumps(message))
    
    def subscribe(self, channel: str, handler):
        """Subscribe to a Redis channel and process messages with the given handler."""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(**{channel: handler})
        return pubsub.run_in_thread(sleep_time=0.1)

# Singleton instance
redis_manager = RedisManager(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    password=os.getenv('REDIS_PASSWORD', None),
    socket_timeout=5,
    socket_connect_timeout=5,
    retry_on_timeout=True
)
