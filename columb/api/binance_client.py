"""
Binance API client for the Trading Analysis System.
Handles API calls to Binance for futures data.
"""
import time
import logging
import hmac
import hashlib
import requests
from urllib.parse import urlencode
import json
from datetime import datetime

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_CONFIG, BINANCE_API, TIMEFRAMES

logger = logging.getLogger(__name__)

class BinanceAPIError(Exception):
    """Exception raised for Binance API errors."""
    pass

class BinanceClient:
    """Client for interacting with the Binance Futures API."""
    
    def __init__(self, api_key=None, api_secret=None, use_testnet=None):
        """Initialize the Binance API client."""
        self.api_key = api_key or API_CONFIG['KEY']
        self.api_secret = api_secret or API_CONFIG['SECRET']
        self.use_testnet = use_testnet if use_testnet is not None else API_CONFIG['USE_TESTNET']
        
        # Set the base URL based on testnet setting
        if self.use_testnet:
            self.base_url = 'https://testnet.binancefuture.com'
            self.wss_url = 'wss://stream.binancefuture.com/stream'
        else:
            self.base_url = 'https://fapi.binance.com'
            self.wss_url = 'wss://fstream.binance.com/stream'
        
        # Initialize session
        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        })
    
    def _get_timestamp(self):
        """Get current timestamp in milliseconds."""
        return int(time.time() * 1000)
    
    def _generate_signature(self, query_string):
        """Generate HMAC-SHA256 signature for the query string."""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _handle_response(self, response):
        """Handle API response and error cases."""
        try:
            response_json = response.json()
            
            # Check for error response
            if response.status_code != 200:
                error_msg = response_json.get('msg', str(response_json))
                logger.error(f"API error: {error_msg} (Code: {response.status_code})")
                raise BinanceAPIError(f"API error: {error_msg} (Code: {response.status_code})")
            
            return response_json
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON response: {response.text}")
            raise BinanceAPIError(f"Failed to decode JSON response: {response.text}")
    
    def _create_request(self, method, endpoint, signed=False, **kwargs):
        """Create and send a request to the Binance API."""
        url = f"{self.base_url}{endpoint}"
        
        # For GET requests, parameters go in the query string
        params = kwargs.get('params', {})
        
        # Add timestamp for signed requests
        if signed:
            params['timestamp'] = self._get_timestamp()
            query_string = urlencode(params)
            params['signature'] = self._generate_signature(query_string)
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params)
            elif method == 'POST':
                response = self.session.post(url, json=kwargs.get('data', {}), params=params)
            elif method == 'DELETE':
                response = self.session.delete(url, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            return self._handle_response(response)
        except requests.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise BinanceAPIError(f"Request error: {str(e)}")
    
    def get_exchange_info(self):
        """Get exchange information including trading pairs."""
        return self._create_request('GET', BINANCE_API['futures']['exchangeInfo'])
    
    def get_trading_pairs(self):
        """Get futures trading pairs."""
        exchange_info = self.get_exchange_info()
        
        # Filter for perpetual futures trading pairs
        pairs = [
            s for s in exchange_info.get('symbols', [])
            if s.get('status') == 'TRADING' and s.get('contractType') == 'PERPETUAL'
        ]
        
        return pairs
    
    def get_klines(self, symbol, interval, limit=500, start_time=None, end_time=None):
        """
        Get kline/candlestick data for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Kline interval (e.g., "1m", "5m", "1h")
            limit: Number of klines to retrieve (max 1500)
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            
        Returns:
            List of klines
        """
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1500)  # API limit is 1500
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        return self._create_request('GET', BINANCE_API['futures']['klines'], params=params)
    
    def get_historical_data(self, symbol, interval, limit=1000):
        """Get historical kline data for a symbol and interval."""
        # Validate interval
        if interval not in TIMEFRAMES:
            raise ValueError(f"Invalid interval: {interval}. Must be one of {list(TIMEFRAMES.keys())}")
        
        # Ensure limit doesn't exceed API maximum
        limit = min(limit, 1500)
        
        # Get klines
        klines = self.get_klines(symbol, interval, limit)
        
        # Convert to more usable format
        formatted_data = []
        for k in klines:
            formatted_data.append({
                'timestamp': k[0],
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'close_time': k[6],
                'quote_asset_volume': float(k[7]),
                'number_of_trades': int(k[8]),
                'taker_buy_base_asset_volume': float(k[9]),
                'taker_buy_quote_asset_volume': float(k[10])
            })
        
        return formatted_data
    
    def get_all_historical_data(self, symbols, interval, limit=1000):
        """Get historical data for multiple symbols."""
        results = {}
        
        for symbol in symbols:
            try:
                data = self.get_historical_data(symbol, interval, limit)
                results[symbol] = data
                # Sleep to avoid API rate limits
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                results[symbol] = []
        
        return results
    
    def get_ticker_price(self, symbol=None):
        """
        Get current price for a symbol or all symbols.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT"), or None for all symbols
            
        Returns:
            Price or list of prices
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        return self._create_request('GET', '/fapi/v1/ticker/price', params=params)
    
    def get_ticker_24hr(self, symbol=None):
        """
        Get 24hr ticker statistics for a symbol or all symbols.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT"), or None for all symbols
            
        Returns:
            Ticker statistics
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        return self._create_request('GET', '/fapi/v1/ticker/24hr', params=params)
    
    def get_mark_price(self, symbol=None):
        """
        Get mark price for a symbol or all symbols.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT"), or None for all symbols
            
        Returns:
            Mark price
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        
        return self._create_request('GET', '/fapi/v1/premiumIndex', params=params)
    
    def get_account_info(self):
        """Get account information (requires API key with trading permission)."""
        return self._create_request('GET', '/fapi/v2/account', signed=True)
    
    def get_wss_url(self, streams):
        """
        Get WebSocket URL for specified streams.
        
        Args:
            streams: List of stream names (e.g., ["btcusdt@kline_1m", "ethusdt@kline_1m"])
            
        Returns:
            WebSocket URL
        """
        if isinstance(streams, list):
            stream_path = '/'.join(streams)
        else:
            stream_path = streams
        
        return f"{self.wss_url}?streams={stream_path}"
