#!/usr/bin/env python3
"""
Configuration settings for the Binance Trading Analysis System.
"""
import os
import json
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent

# Database settings
DATABASE = {
    'name': os.environ.get('DB_NAME', os.path.join(BASE_DIR, 'data', 'trading.db')),
    'backup_dir': os.path.join(BASE_DIR, 'data', 'backups'),
}

# Ensure data directories exist
os.makedirs(os.path.dirname(DATABASE['name']), exist_ok=True)
os.makedirs(DATABASE['backup_dir'], exist_ok=True)

# API Configuration
API_CONFIG = {
    'KEY': os.environ.get('BINANCE_API_KEY', ''),
    'SECRET': os.environ.get('BINANCE_API_SECRET', ''),
    'USE_TESTNET': os.environ.get('USE_TESTNET', 'False').lower() == 'true',
}

# Binance API endpoints
BINANCE_API = {
    'futures': {
        'base': 'https://fapi.binance.com' if not API_CONFIG['USE_TESTNET'] else 'https://testnet.binancefuture.com',
        'exchangeInfo': '/fapi/v1/exchangeInfo',
        'klines': '/fapi/v1/klines',
        'websocket': 'wss://fstream.binance.com/stream' if not API_CONFIG['USE_TESTNET'] else 'wss://stream.binancefuture.com/stream',
    }
}

# Data Collection Configuration
TIMEFRAMES = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}
DEFAULT_TIMEFRAME = "1m"
HISTORY_LIMIT = 1000

# Metric Configuration
METRIC_CONFIG = {
    'VOLUME_BASELINE_PERIOD': 20,
    'MOMENTUM_PERIOD': 14,
    'ZSCORE_PERIOD': 20,
    
    # Supertrend Configuration
    'SUPERTREND_PERIOD': 10,
    'SUPERTREND_MULTIPLIER': 3.0,
}

# UI Configuration
UI_CONFIG = {
    'UPDATE_INTERVAL': 5000,  # in milliseconds
    'MAX_DISPLAYED_PAIRS': 450,
}

# Opportunity Detection Configuration
OPPORTUNITY_THRESHOLD = {
    'volume': 2.0,
    'momentum': 0.8,
    'zscore': 1.5,
    'rank_change': 3,
    'price': 1.5
}

# Backtest Configuration
BACKTEST_CONFIG = {
    'TAKE_PROFIT': 3.0,  # 3% profit
    'STOP_LOSS': 1.5,    # 1.5% loss
    'MAX_BARS': 20,      # Maximum 20 candles (timeframe dependent)
}

# Slot Machine Configuration
SLOT_MACHINE_CONFIG = {
    'REQUIRED_MATCHING_METRICS': 3,   # Number of metrics that need to match for highlighting
    'MAX_SLOT_MACHINE_ROWS': 50       # Maximum number of rows to display in slot machine view
}

# Web server configuration
WEB_CONFIG = {
    'HOST': os.environ.get('HOST', '0.0.0.0'),
    'PORT': int(os.environ.get('PORT', 5000)),
    'DEBUG': os.environ.get('DEBUG', 'False').lower() == 'true',
}

# Export configuration for Orange3
EXPORT_CONFIG = {
    'export_dir': os.path.join(BASE_DIR, 'data', 'exports'),
    'default_format': 'csv',  # csv, xlsx, json
}

# Create export directory
os.makedirs(EXPORT_CONFIG['export_dir'], exist_ok=True)

def save_user_config(config_dict, filename='user_config.json'):
    """Save user configuration to a JSON file."""
    config_path = os.path.join(BASE_DIR, 'data', filename)
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    with open(config_path, 'w') as f:
        json.dump(config_dict, f, indent=4)
    
    return True

def load_user_config(filename='user_config.json'):
    """Load user configuration from a JSON file."""
    config_path = os.path.join(BASE_DIR, 'data', filename)
    
    if not os.path.exists(config_path):
        return {}
    
    with open(config_path, 'r') as f:
        return json.load(f)

# Override defaults with user configuration if available
user_config = load_user_config()
if user_config:
    # Update configurations with user-defined values
    # This allows flexible customization without editing this file
    if 'METRIC_CONFIG' in user_config:
        METRIC_CONFIG.update(user_config['METRIC_CONFIG'])
    if 'OPPORTUNITY_THRESHOLD' in user_config:
        OPPORTUNITY_THRESHOLD.update(user_config['OPPORTUNITY_THRESHOLD'])
    if 'BACKTEST_CONFIG' in user_config:
        BACKTEST_CONFIG.update(user_config['BACKTEST_CONFIG'])
