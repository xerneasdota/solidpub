"""
WebSocket client for the Binance Trading Analysis System.
Handles real-time data streaming from Binance.
"""
import json
import logging
import threading
import time
import websocket
from datetime import datetime

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BINANCE_API

logger = logging.getLogger(__name__)

class BinanceWebSocket:
    """WebSocket client for streaming data from Binance."""
    
    def __init__(self, use_testnet=False, callback=None):
        """
        Initialize the WebSocket client.
        
        Args:
            use_testnet: Whether to use the testnet WebSocket
            callback: Function to call when a message is received
        """
        self.use_testnet = use_testnet
        self.callback = callback
        self.ws = None
        self.running = False
        self.thread = None
        
        # Select the appropriate WebSocket URL
        if self.use_testnet:
            self.base_url = 'wss://stream.binancefuture.com/stream'
        else:
            self.base_url = 'wss://fstream.binance.com/stream'
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Handle ping/pong messages
            if 'ping' in data:
                self._send_pong(data['ping'])
                return
            
            # Forward to callback if provided
            if self.callback:
                self.callback(data)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode WebSocket message: {message}")
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {str(e)}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {str(error)}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        logger.info(f"WebSocket closed: {close_msg} (Code: {close_status_code})")
        self.running = False
        
        # Attempt to reconnect after a delay
        if self.running:
            time.sleep(5)
            self.connect()
    
    def _on_open(self, ws):
        """Handle WebSocket connection open."""
        logger.info("WebSocket connection opened")
        self.running = True
    
    def _send_pong(self, ping_id):
        """Send a pong response to keep the connection alive."""
        if self.ws and self.ws.sock and self.ws.sock.connected:
            pong_data = json.dumps({"pong": ping_id})
            self.ws.send(pong_data)
    
    def _format_stream_names(self, symbols, intervals):
        """Format stream names for klines."""
        streams = []
        
        # Add kline streams for each symbol and interval
        for symbol in symbols:
            lower_symbol = symbol.lower()
            for interval in intervals:
                streams.append(f"{lower_symbol}@kline_{interval}")
        
        # Add trade streams for real-time price updates
        for symbol in symbols:
            lower_symbol = symbol.lower()
            streams.append(f"{lower_symbol}@trade")
        
        return streams
    
    def connect(self, symbols=None, intervals=None):
        """
        Connect to the WebSocket.
        
        Args:
            symbols: List of symbols to subscribe to (e.g., ["BTCUSDT", "ETHUSDT"])
            intervals: List of intervals to subscribe to (e.g., ["1m", "5m"])
        """
        if not symbols or not intervals:
            logger.error("Symbols and intervals are required")
            return False
        
        # Format stream names
        streams = self._format_stream_names(symbols, intervals)
        
        # Create WebSocket URL
        url = f"{self.base_url}?streams={'/'.join(streams)}"
        
        # Initialize WebSocket
        self.ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # Start WebSocket connection in a new thread
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.daemon = True
        self.thread.start()
        
        # Wait for connection to be established
        timeout = 5
        start_time = time.time()
        while not self.running and time.time() - start_time < timeout:
            time.sleep(0.1)
        
        return self.running
    
    def disconnect(self):
        """Disconnect from the WebSocket."""
        if self.ws:
            self.running = False
            self.ws.close()
            logger.info("WebSocket disconnected")

class DataStreamProcessor:
    """Processes data streams from Binance WebSocket."""
    
    def __init__(self, db_manager=None, on_kline_update=None, on_trade_update=None):
        """
        Initialize the data stream processor.
        
        Args:
            db_manager: Database manager instance
            on_kline_update: Callback for kline updates
            on_trade_update: Callback for trade updates
        """
        self.db_manager = db_manager
        self.on_kline_update = on_kline_update
        self.on_trade_update = on_trade_update
        self.latest_prices = {}
        self.latest_klines = {}
    
    def process_message(self, message):
        """Process a WebSocket message."""
        try:
            # Skip if no data
            if 'data' not in message:
                return
            
            data = message['data']
            stream = message.get('stream', '')
            
            # Process based on stream type
            if 'kline' in stream:
                self._process_kline(data)
            elif 'trade' in stream:
                self._process_trade(data)
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {str(e)}")
    
    def _process_kline(self, data):
        """Process a kline update."""
        try:
            # Extract kline data
            symbol = data['s']
            kline = data['k']
            
            # Create structured kline data
            kline_data = {
                'symbol': symbol,
                'interval': kline['i'],
                'timestamp': kline['t'],
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'volume': float(kline['v']),
                'close_time': kline['T'],
                'quote_asset_volume': float(kline['q']),
                'number_of_trades': int(kline['n']),
                'taker_buy_base_asset_volume': float(kline['V']),
                'taker_buy_quote_asset_volume': float(kline['Q']),
                'is_closed': kline['x']
            }
            
            # Store the latest kline
            key = f"{symbol}_{kline['i']}"
            self.latest_klines[key] = kline_data
            
            # Store in database if kline is closed
            if kline_data['is_closed'] and self.db_manager:
                self.db_manager.price_data.create(
                    symbol=symbol,
                    timeframe=kline['i'],
                    timestamp=kline['t'],
                    open_price=float(kline['o']),
                    high=float(kline['h']),
                    low=float(kline['l']),
                    close=float(kline['c']),
                    volume=float(kline['v']),
                    quote_asset_volume=float(kline['q']),
                    number_of_trades=int(kline['n']),
                    taker_buy_base_asset_volume=float(kline['V']),
                    taker_buy_quote_asset_volume=float(kline['Q'])
                )
            
            # Call update callback if provided
            if self.on_kline_update:
                self.on_kline_update(kline_data)
        except Exception as e:
            logger.error(f"Error processing kline data: {str(e)}")
    
    def _process_trade(self, data):
        """Process a trade update."""
        try:
            # Extract trade data
            symbol = data['s']
            price = float(data['p'])
            quantity = float(data['q'])
            timestamp = data['T']
            
            # Store the latest price
            self.latest_prices[symbol] = price
            
            # Update the latest kline with the new price
            for interval in ['1m', '5m', '15m', '1h', '4h', '1d']:
                key = f"{symbol}_{interval}"
                if key in self.latest_klines:
                    kline = self.latest_klines[key]
                    
                    # Update close price
                    kline['close'] = price
                    
                    # Update high/low if needed
                    if price > kline['high']:
                        kline['high'] = price
                    if price < kline['low']:
                        kline['low'] = price
            
            # Prepare trade data
            trade_data = {
                'symbol': symbol,
                'price': price,
                'quantity': quantity,
                'timestamp': timestamp
            }
            
            # Call update callback if provided
            if self.on_trade_update:
                self.on_trade_update(trade_data)
        except Exception as e:
            logger.error(f"Error processing trade data: {str(e)}")
    
    def get_latest_price(self, symbol):
        """Get the latest price for a symbol."""
        return self.latest_prices.get(symbol)
    
    def get_latest_kline(self, symbol, interval):
        """Get the latest kline for a symbol and interval."""
        key = f"{symbol}_{interval}"
        return self.latest_klines.get(key)
    
    def get_all_latest_prices(self):
        """Get all latest prices."""
        return self.latest_prices
