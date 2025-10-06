#!/usr/bin/env python3
"""
Redis Stream Data Collector for Pacifica Market Data

This module collects real-time market data from Pacifica's WebSocket
and streams it to Redis with configurable retention policies.
"""
import os
import json
import time
import signal
import asyncio
import websockets
import redis
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

def get_env_int(key: str, default: int) -> int:
    """Safely get an integer from environment variables, handling comments and whitespace."""
    value = os.getenv(key, str(default))
    # Remove any trailing comments and whitespace
    value = value.split('#')[0].strip()
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

# Configuration
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost').split('#')[0].strip(),
    'port': get_env_int('REDIS_PORT', 6379),
    'db': get_env_int('REDIS_DB', 0),
    'decode_responses': True,
    'socket_connect_timeout': 5,
    'retry_on_timeout': True
}

# WebSocket configuration
WEBSOCKET_URL = "wss://ws.pacifica.fi/ws"
API_BASE_URL = "https://api.pacifica.fi/api/v1"

# Redis stream configuration
STREAM_MAX_LEN = 1000  # Maximum number of entries per stream
DATA_RETENTION = {
    'prices': timedelta(hours=24),        # 24 hours for price data
    'orderbook': timedelta(hours=1),      # 1 hour for order book data
    'trades': timedelta(days=7)           # 7 days for trade data
}

class RedisStreamCollector:
    def __init__(self, symbols: List[str], redis_config=None, debug=False, verbose=False):
        """Initialize the Redis Stream Collector.
        
        Args:
            symbols: List of trading symbols to collect data for (e.g., ['BTC', 'ETH']).
                    Use ['ALL'] to subscribe to all available markets.
            redis_config: Dictionary containing Redis connection parameters
            debug: Enable debug logging
            verbose: Log all WebSocket messages (very verbose)
        """
        self.debug = debug
        self.verbose = verbose
        if 'ALL' in [s.upper() for s in symbols]:
            # Will be populated in get_available_markets()
            self.symbols = []
            self.subscribe_all = True
        else:
            self.symbols = [s.upper() for s in symbols]
            self.subscribe_all = False
        self.running = False
        
        # Initialize Redis connection
        self.redis_config = redis_config or REDIS_CONFIG
        self.redis = redis.Redis(**self.redis_config)
        
        # Test Redis connection
        try:
            self.redis.ping()
            print("✅ Successfully connected to Redis")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise
    
    async def initialize_streams(self):
        """Initialize Redis streams and set up retention policies."""
        for symbol in self.symbols:
            # Create streams if they don't exist
            for stream_type in ['prices', 'orderbook', 'trades']:
                stream_key = f"market:{symbol}:{stream_type}"
                # Set stream max length and retention time
                self.redis.xadd(stream_key, {"init": "stream_initialized"}, maxlen=STREAM_MAX_LEN, approximate=True)
                self.redis.expire(stream_key, int(DATA_RETENTION[stream_type].total_seconds()))
    
    async def process_price_update(self, symbol: str, data: Dict[str, Any]):
        """Process and store price update in Redis stream."""
        if not data:
            print("⚠️ No data provided for price update")
            return
            
        stream_key = f"market:{symbol}:prices"
        
        try:
            # Prepare the price data
            price_data = {
                'bid': str(data.get('bid', data.get('b', '0'))),
                'ask': str(data.get('ask', data.get('a', '0'))),
                'mid': str(data.get('mid', (float(data.get('bid', data.get('b', 0))) + float(data.get('ask', data.get('a', 0)))) / 2)),
                'timestamp': str(data.get('timestamp', datetime.utcnow().timestamp()))
            }
            
            print(f"💾 Storing price data in {stream_key}: {price_data}")
            
            # Add to Redis stream
            msg_id = self.redis.xadd(stream_key, price_data, maxlen=STREAM_MAX_LEN)
            print(f"✅ Stored price update {msg_id}")
            
            # Update latest price
            latest_key = f"market:{symbol}:latest:price"
            self.redis.hset(latest_key, mapping=price_data)
            
        except Exception as e:
            print(f"❌ Error writing to {stream_key}: {e}")
            import traceback
            traceback.print_exc()
    
    async def process_orderbook_update(self, symbol: str, data: Dict[str, Any]):
        """Process and store order book update in Redis stream."""
        if not data:
            print("⚠️ No data provided for order book update")
            return
            
        stream_key = f"market:{symbol}:orderbook"
        
        try:
            # Get bids and asks, handling different possible field names
            bids = data.get('bids', data.get('b', []))
            asks = data.get('asks', data.get('a', []))
            
            # Prepare the order book data
            orderbook_data = {
                'bids': json.dumps(bids[:10]),  # Only keep top 10 levels
                'asks': json.dumps(asks[:10]),  # Only keep top 10 levels
                'timestamp': str(data.get('timestamp', datetime.utcnow().timestamp()))
            }
            
            print(f"💾 Storing order book update in {stream_key}")
            
            # Add to Redis stream
            msg_id = self.redis.xadd(stream_key, orderbook_data, maxlen=STREAM_MAX_LEN)
            print(f"✅ Stored order book update {msg_id}")
            
            # Update best bid/ask
            if bids and asks:
                best_bid = float(bids[0][0]) if isinstance(bids[0], list) else float(bids[0].get('price', 0))
                best_ask = float(asks[0][0]) if isinstance(asks[0], list) else float(asks[0].get('price', 0))
                
                self.redis.hset(f"market:{symbol}:best", mapping={
                    'bid': str(best_bid),
                    'ask': str(best_ask),
                    'spread': str(best_ask - best_bid if best_bid and best_ask else 0),
                    'timestamp': str(datetime.utcnow().timestamp())
                })
            
        except Exception as e:
            print(f"❌ Error writing to {stream_key}: {e}")
            import traceback
            traceback.print_exc()
    
    async def process_trade_update(self, symbol: str, data: Dict[str, Any]):
        """Process and store trade update in Redis stream."""
        if not data:
            print("⚠️ No data provided for trade update")
            return
            
        stream_key = f"market:{symbol}:trades"
        
        try:
            # Handle different possible field names
            price = data.get('price', data.get('p', '0'))
            quantity = data.get('quantity', data.get('q', '0'))
            side = data.get('side', data.get('s', 'unknown'))
            
            # Generate a unique trade ID if not provided
            trade_id = data.get('id', f"{int(time.time()*1000)}_{price}_{quantity}")
            
            # Prepare the trade data
            trade_data = {
                'id': trade_id,
                'price': str(price),
                'quantity': str(quantity),
                'side': side.lower(),
                'timestamp': str(data.get('timestamp', datetime.utcnow().timestamp())),
                'value': str(float(price) * float(quantity))
            }
            
            print(f"💾 Storing trade in {stream_key}: {trade_data}")
            
            # Add to Redis stream
            msg_id = self.redis.xadd(stream_key, trade_data, maxlen=STREAM_MAX_LEN)
            print(f"✅ Stored trade {msg_id}")
            
            # Update latest trade
            self.redis.hset(f"market:{symbol}:latest:trade", mapping=trade_data)
            
            # Update 24h volume
            volume_key = f"market:{symbol}:24h:volume"
            self.redis.hincrbyfloat(volume_key, 'total_volume', float(quantity))
            
        except Exception as e:
            print(f"❌ Error writing to {stream_key}: {e}")
            import traceback
            traceback.print_exc()
    
    async def get_available_markets(self, websocket) -> List[str]:
        """Fetch available markets from the Pacifica REST API.
        
        Returns:
            List of available market symbols (e.g., ['BTC', 'ETH', 'SOL'])
        """
        print("🔍 Fetching available markets from the exchange...")
        try:
            import aiohttp
            
            # Use the REST API to get market info
            async with aiohttp.ClientSession() as session:
                url = f"{API_BASE_URL}/info"
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Failed to fetch markets: {response.status}")
                        return ['BTC', 'ETH', 'UNI', 'BNB']  # Fallback to default symbols
                        
                    data = await response.json()
                    
                    if not data.get('success', False):
                        print(f"⚠️ API error: {data.get('error', 'Unknown error')}")
                        return ['BTC', 'ETH', 'UNI', 'BNB']  # Fallback to default symbols
                    
                    # Extract unique symbols from the response
                    markets = set()
                    for market in data.get('data', []):
                        symbol = market.get('symbol')
                        if symbol:
                            base = market['symbol'].split('/')[0]
                            markets.add(base)
            
            if not markets:
                print("⚠️ Could not parse market data. Using default symbols.")
                return ['BTC', 'ETH', 'UNI', 'BNB']
                
            print(f"✅ Found {len(markets)} available markets")
            return sorted(list(markets))
            
        except (asyncio.TimeoutError, json.JSONDecodeError, KeyError) as e:
            print(f"⚠️ Error fetching markets: {e}")
            return ['BTC', 'ETH', 'UNI', 'BNB']
    
    async def websocket_handler(self):
        """Handle WebSocket connection and message processing with detailed logging."""
        while self.running:
            try:
                print(f"\n{'='*80}")
                print(f"🔄 [1/3] Connecting to WebSocket at {WEBSOCKET_URL}...")
                
                # Add timeout and additional connection parameters
                async with websockets.connect(
                    WEBSOCKET_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,  # 10MB max message size
                ) as websocket:
                    print("✅ [2/3] Connected to Pacifica WebSocket")
                    
                    # Generate a unique client ID for this connection
                    client_id = f"pacificaredis_{int(time.time())}"
                    print(f"🔑 Client ID: {client_id}")
                    
                    # Get available markets if ALL was specified
                    if self.subscribe_all:
                        self.symbols = await self.get_available_markets(websocket)
                        if not self.symbols:
                            print("⚠️ Could not fetch available markets. Using default symbols.")
                            self.symbols = ['BTC', 'ETH', 'UNI', 'BNB']
                    
                    # First, subscribe to prices stream for all symbols
                    prices_sub = {
                        "method": "subscribe",
                        "params": {
                            "source": "prices"
                        }
                    }
                    await websocket.send(json.dumps(prices_sub))
                    print("✅ Subscribed to prices stream")
                    
                    # Then subscribe to orderbook and trades for each symbol
                    for symbol in self.symbols:
                        print(f"\n📡 [3/3] Setting up subscriptions for {symbol}...")
                        
                        # 1. Subscribe to order book updates
                        book_sub = {
                            "method": "subscribe",
                            "params": {
                                "source": "book",
                                "symbol": symbol
                            }
                        }
                        await websocket.send(json.dumps(book_sub))
                        print(f"  - 📚 Sent order book subscription for {symbol}")
                        
                        # 2. Subscribe to trade updates
                        trade_sub = {
                            "method": "SUBSCRIBE",
                            "params": [f"{symbol.lower()}@trade"],
                            "id": int(time.time() * 1000) + 2
                        }
                        await websocket.send(json.dumps(trade_sub))
                        print(f"  - 💱 Sent trade subscription for {symbol}")
                        
                        # Small delay between subscriptions to avoid rate limiting
                        await asyncio.sleep(0.1)
                        
                    # Send initial ping to verify connection
                    ping_msg = {
                        "method": "ping",
                        "id": int(time.time() * 1000)
                    }
                    await websocket.send(json.dumps(ping_msg))
                    print("\n💓 Sent initial ping to verify connection...")
                    
                    print("\n📥 Listening for WebSocket messages...")
                    print("   (Press Ctrl+C to stop the data collector)\n")
                    
                    # Track last activity and ping time
                    last_activity = time.time()
                    last_ping = time.time()
                    
                    # Main message processing loop
                    while self.running:
                        try:
                            # Set a timeout for receiving messages
                            message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                            last_activity = time.time()
                            
                            # Log raw message (truncated for readability)
                            if self.verbose:
                                if len(message) > 200:
                                    print(f"🔍 [VERBOSE] Received message ({len(message)} bytes): {message[:200]}...")
                                else:
                                    print(f"🔍 [VERBOSE] Received message ({len(message)} bytes): {message}")
                            elif self.debug:
                                if len(message) > 200:
                                    print(f"🐛 [DEBUG] Received message ({len(message)} bytes): {message[:200]}...")
                                else:
                                    print(f"🐛 [DEBUG] Received message ({len(message)} bytes): {message}")
                                print("📝 Parsed data structure:")
                                print(json.dumps(data, indent=2))
                                
                                # Handle ping/pong - Pacifica uses 'op' instead of 'method'
                                if data.get('op') == 'pong':
                                    print("💓 Received pong response")
                                    last_ping = time.time()
                                    continue
                                
                                # Handle subscription responses
                                if data.get('type') == 'subscribed':
                                    channel = data.get('channel', 'unknown')
                                    market = data.get('market', 'unknown')
                                    print(f"✅ Successfully subscribed to {channel} for market: {market}")
                                    continue
                                
                                # Handle error responses
                                if data.get('type') == 'error':
                                    error_msg = data.get('error', 'Unknown error')
                                    print(f"❌ Error from WebSocket: {error_msg}")
                                    if 'code' in data:
                                        print(f"   Error code: {data['code']}")
                                    continue
                                
                                # Extract data based on Pacifica's message format
                                channel = data.get('channel')
                                market = data.get('market', '')
                                
                                # Extract symbol from market (format: "BTC/USDC")
                                symbol = market.split('/')[0].upper() if market and '/' in market else ''
                                
                                print(f"  - Channel: {channel}")
                                print(f"  - Market: {market}")
                                print(f"  - Extracted symbol: {symbol}")
                                
                                if not symbol or symbol not in self.symbols:
                                    print(f"⚠️ Unexpected symbol in market: {market}")
                                    continue
                                
                                # Process based on channel type
                                try:
                                    if channel == 'ticker':
                                        print("  - Processing price update")
                                        await self.process_price_update(symbol, data)
                                    elif channel == 'orderbook':
                                        print("  - Processing order book update")
                                        await self.process_orderbook_update(symbol, data)
                                    elif channel == 'trades':
                                        print("  - Processing trade update")
                                        await self.process_trade_update(symbol, data)
                                    else:
                                        print(f"⚠️ Unknown channel: {channel}")
                                except Exception as proc_err:
                                    print(f"❌ Error processing {channel} data: {proc_err}")
                                    import traceback
                                    traceback.print_exc()
                                
                        except (ValueError, json.JSONDecodeError) as json_err:
                            print(f"❌ Failed to parse message: {json_err}")
                            print(f"   Raw message: {message[:200]}...")
                            
                            # Check if we need to send a ping (every 25 seconds)
                            if time.time() - last_activity > 25:
                                print("⚠️ No messages received for 25 seconds. Sending ping...")
                                try:
                                    ping_msg = {
                                        "method": "PING",
                                        "id": int(time.time() * 1000)
                                    }
                                    await websocket.send(json.dumps(ping_msg))
                                    last_ping = time.time()
                                    last_activity = time.time()
                                    print("💓 Sent ping")
                                except Exception as ping_err:
                                    print(f"❌ Failed to send ping: {ping_err}")
                            
                            # Check for connection timeout (no messages for 60 seconds)
                            if time.time() - last_activity > 60:
                                print("⚠️ No messages received for 60 seconds. Reconnecting...")
                                break
                                
                        except asyncio.TimeoutError:
                            print("⚠️ No messages received for 30 seconds. Sending ping...")
                            try:
                                ping_msg = {
                                    "method": "PING",
                                    "id": int(time.time() * 1000)
                                }
                                await websocket.send(json.dumps(ping_msg))
                                last_ping = time.time()
                            except Exception as ping_err:
                                print(f"❌ Failed to send ping: {ping_err}")
                                break
                            
                        except websockets.exceptions.ConnectionClosed as cc_err:
                            print(f"❌ WebSocket connection closed: {cc_err}")
                            if cc_err.code == 1006:
                                print("  - Connection was closed abnormally")
                            break
                            
                        except Exception as msg_err:
                            print(f"❌ Error in message loop: {msg_err}")
                            import traceback
                            traceback.print_exc()
                            break
                
                # If we get here, the connection was closed
                print("\n🔌 WebSocket connection closed. Waiting 5 seconds before reconnecting...")
                await asyncio.sleep(5)
                
            except websockets.exceptions.WebSocketException as ws_err:
                print(f"\n❌ WebSocket error: {ws_err}")
                print("   Waiting 5 seconds before reconnecting...")
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                import traceback
                traceback.print_exc()
                print("   Waiting 5 seconds before reconnecting...")
                await asyncio.sleep(5)
            
            except websockets.exceptions.ConnectionClosed as e:
                print(f"\n❌ WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except ConnectionError as e:
                print(f"\n❌ Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                import traceback
                traceback.print_exc()
                print("Retrying in 5 seconds...")
                await asyncio.sleep(5)
    
    async def run(self):
        """Start the data collection process."""
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        
        try:
            # Initialize Redis streams
            await self.initialize_streams()
            
            # Start WebSocket handler
            await self.websocket_handler()
            
        except asyncio.CancelledError:
            print("Shutting down gracefully...")
        except Exception as e:
            print(f"Fatal error: {e}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shut down the collector."""
        if self.running:
            print("Shutting down data collector...")
            self.running = False
            # Add any cleanup code here


def setup_logging(debug=False):
    """Set up logging configuration."""
    import os
    import logging
    from logging.handlers import RotatingFileHandler
    
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            print(f"Created log directory: {os.path.abspath(log_dir)}")
        
        # Set up formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Set up file handler with rotation (10MB per file, keep 5 backups)
        log_file = os.path.join(log_dir, 'data_collector.log')
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        # Set up console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
        
        # Clear any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add our handlers
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        logger = logging.getLogger(__name__)
        logger.info("Logging initialized successfully")
        return logger
        
    except Exception as e:
        # Fallback to basic config if file logging fails
        logging.basicConfig(
            level=logging.DEBUG if debug else logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to set up file logging: {e}")
        logger.info("Falling back to console logging only")
        return logger

def main():
    import argparse
    import sys
    import traceback
    import logging
    
    # Set up argument parser first to handle debug flag for logging
    parser = argparse.ArgumentParser(description='Pacifica Market Data Collector (Redis Streams)')
    parser.add_argument('symbols', nargs='+', 
                      help='List of trading symbols (e.g., BTC ETH) or "ALL" for all available markets')
    parser.add_argument('--redis-host', default=REDIS_CONFIG['host'], 
                      help=f'Redis host (default: {REDIS_CONFIG["host"]})')
    parser.add_argument('--redis-port', type=int, default=REDIS_CONFIG['port'], 
                      help=f'Redis port (default: {REDIS_CONFIG["port"]})')
    parser.add_argument('--redis-db', type=int, default=REDIS_CONFIG['db'], 
                      help=f'Redis database number (default: {REDIS_CONFIG["db"]})')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--verbose', action='store_true', 
                      help='Enable verbose logging (shows all WebSocket messages)')
    
    args = parser.parse_args()
    
    # Set up logging with appropriate level
    logger = setup_logging(debug=args.debug)
    logger.info("Starting Pacifica Market Data Collector...")
    
    if args.debug:
        logger.debug("Debug logging enabled")
    if args.verbose:
        logger.info("Verbose logging enabled (all WebSocket messages will be shown)")
        if not args.debug:
            logger.setLevel(logging.DEBUG)
    
    logger.info(f"Command line arguments: {args}")
    
    # Create Redis config
    redis_config = {
        'host': args.redis_host,
        'port': args.redis_port,
        'db': args.redis_db,
        'decode_responses': True,
        'socket_connect_timeout': 5,
        'retry_on_timeout': True
    }
    
    logger.info(f"Redis configuration: {redis_config}")
    
    # Check if user wants to subscribe to all available markets
    if 'ALL' in [s.upper() for s in args.symbols]:
        symbols = ['ALL']
        logger.info("Will subscribe to ALL available markets")
    else:
        symbols = [s.upper() for s in args.symbols]
        logger.info(f"Will subscribe to specific markets: {', '.join(symbols)}")
    
    try:
        logger.info("Initializing RedisStreamCollector...")
        collector = RedisStreamCollector(
            symbols, 
            redis_config=redis_config,
            debug=args.debug,
            verbose=args.verbose
        )
        
        logger.info(f"Starting data collector for: {', '.join(symbols) if symbols[0] != 'ALL' else 'ALL available markets'}")
        asyncio.run(collector.run())
        
    except KeyboardInterrupt:
        logger.info("\nReceived stop signal. Shutting down...")
        if 'collector' in locals():
            collector.stop()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        if 'collector' in locals():
            collector.stop()
        raise
    finally:
        logger.info("Data collector stopped")

if __name__ == "__main__":
    main()
