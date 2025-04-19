"""
Metrics calculations for the Binance Trading Analysis System.
"""
import logging
import numpy as np
from typing import Dict, List, Any, Optional, Tuple

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import METRIC_CONFIG
from .indicators import (
    calculate_supertrend, calculate_rsi, calculate_zscore, 
    calculate_price_change_percentage, calculate_momentum
)

logger = logging.getLogger(__name__)

def calculate_volume_metric(data: List[Dict]) -> float:
    """
    Calculate volume-based metric.
    Higher value indicates stronger buying pressure.
    
    Args:
        data: List of price data dictionaries
        
    Returns:
        Volume metric value
    """
    if len(data) < METRIC_CONFIG['VOLUME_BASELINE_PERIOD']:
        logger.warning(f"Not enough data for volume metric calculation. Need {METRIC_CONFIG['VOLUME_BASELINE_PERIOD']}, got {len(data)}")
        return 0.0
    
    cumulative_buy = 0.0
    cumulative_sell = 0.0
    
    # Process each candle
    for candle in data:
        high = candle['high']
        low = candle['low']
        close = candle['close']
        volume = candle['volume']
        
        # Skip if high equals low to avoid division by zero
        if high == low:
            continue
        
        # Estimate buy and sell volumes based on price position within the candle
        buy_vol = volume * (close - low) / (high - low)
        sell_vol = volume * (high - close) / (high - low)
        
        # Accumulate
        cumulative_buy += buy_vol
        cumulative_sell += sell_vol
    
    # Calculate ratio (avoid division by zero)
    if cumulative_sell > 0:
        ratio_volume = cumulative_buy / cumulative_sell
    else:
        ratio_volume = 1.0
    
    return ratio_volume

def calculate_momentum_metric(data: List[Dict]) -> float:
    """
    Calculate momentum-based metric.
    Positive value indicates bullish momentum, negative indicates bearish.
    
    Args:
        data: List of price data dictionaries
        
    Returns:
        Momentum metric value
    """
    if len(data) < METRIC_CONFIG['MOMENTUM_PERIOD']:
        logger.warning(f"Not enough data for momentum metric calculation. Need {METRIC_CONFIG['MOMENTUM_PERIOD']}, got {len(data)}")
        return 0.0
    
    # Calculate total percentage change for the entire period
    total_pct_change = calculate_total_pct_change(data)
    
    # Calculate RSI on the price data
    rsi_values = calculate_rsi(data, METRIC_CONFIG['MOMENTUM_PERIOD'])
    
    # Get the last two valid RSI values
    valid_rsi = [v for v in rsi_values if v is not None]
    if len(valid_rsi) < 2:
        return 0.0
    
    latest_rsi = valid_rsi[-1]
    previous_rsi = valid_rsi[-2]
    
    # Calculate momentum change
    momentum_change = latest_rsi - previous_rsi
    
    # Calculate momentum percentage change
    if previous_rsi != 0:
        momentum_pct_change = (momentum_change / previous_rsi) * 100
    else:
        momentum_pct_change = 0.0
    
    # Normalize to a value between -1 and 1 using tanh
    normalized_momentum = np.tanh(momentum_pct_change / 10)
    
    return normalized_momentum

def calculate_total_pct_change(data: List[Dict]) -> float:
    """
    Calculate total percentage change from first to last candle.
    
    Args:
        data: List of price data dictionaries
        
    Returns:
        Total percentage change
    """
    if len(data) < 2:
        logger.warning("Not enough data for percentage change calculation. Need at least 2 candles.")
        return 0.0
    
    # Calculate start price as average of high and low of first candle
    start_price = (data[0]['high'] + data[0]['low']) / 2
    
    # Calculate current price as average of high and low of last candle
    current_price = (data[-1]['high'] + data[-1]['low']) / 2
    
    # Avoid division by zero
    if start_price == 0:
        return 0.0
    
    # Calculate total percentage change
    total_pct_change = ((current_price - start_price) / start_price) * 100
    
    return total_pct_change

def calculate_zscore_metric(data: List[Dict]) -> float:
    """
    Calculate Z-Score based metric.
    
    Args:
        data: List of price data dictionaries
        
    Returns:
        Z-Score metric value
    """
    if len(data) < METRIC_CONFIG['ZSCORE_PERIOD']:
        logger.warning(f"Not enough data for Z-Score metric calculation. Need {METRIC_CONFIG['ZSCORE_PERIOD']}, got {len(data)}")
        return 0.0
    
    # Calculate Z-Score on closing prices
    zscore_values = calculate_zscore(data, METRIC_CONFIG['ZSCORE_PERIOD'])
    
    # Get the last valid Z-Score
    valid_zscores = [z for z in zscore_values if z is not None]
    if not valid_zscores:
        return 0.0
    
    return valid_zscores[-1]

def calculate_price_metric(data: List[Dict], symbol_info: Dict) -> float:
    """
    Calculate price-based metric using Supertrend signals.
    
    Args:
        data: List of price data dictionaries
        symbol_info: Dictionary containing signal prices and information
        
    Returns:
        Price metric value
    """
    # Calculate Supertrend
    supertrend = calculate_supertrend(
        data, 
        period=METRIC_CONFIG['SUPERTREND_PERIOD'], 
        multiplier=METRIC_CONFIG['SUPERTREND_MULTIPLIER']
    )
    
    # Initialize info if needed
    if 'long_pullback_level' not in symbol_info:
        symbol_info['long_pullback_level'] = None
    if 'short_pullback_level' not in symbol_info:
        symbol_info['short_pullback_level'] = None
    if 'signal_type' not in symbol_info:
        symbol_info['signal_type'] = None
    if 'signal_bar_idx' not in symbol_info:
        symbol_info['signal_bar_idx'] = None
    
    # Find the latest signal
    latest_buy_signal_idx = None
    latest_sell_signal_idx = None
    
    for i in range(len(supertrend) - 1, -1, -1):
        if supertrend[i].get('buy_signal') and latest_buy_signal_idx is None:
            latest_buy_signal_idx = i
        if supertrend[i].get('sell_signal') and latest_sell_signal_idx is None:
            latest_sell_signal_idx = i
        if latest_buy_signal_idx is not None and latest_sell_signal_idx is not None:
            break
    
    # Determine which signal is more recent
    latest_signal = None
    signal_type = None
    
    if latest_buy_signal_idx is not None and latest_sell_signal_idx is not None:
        if latest_buy_signal_idx > latest_sell_signal_idx:
            latest_signal = latest_buy_signal_idx
            signal_type = 'buy'
        else:
            latest_signal = latest_sell_signal_idx
            signal_type = 'sell'
    elif latest_buy_signal_idx is not None:
        latest_signal = latest_buy_signal_idx
        signal_type = 'buy'
    elif latest_sell_signal_idx is not None:
        latest_signal = latest_sell_signal_idx
        signal_type = 'sell'
    
    # No signals found
    if latest_signal is None:
        symbol_info['in_uptrend'] = supertrend[-1]['in_uptrend'] if supertrend else True
        return 0.0
    
    # Get the data for the same index
    if latest_signal >= len(data):
        symbol_info['in_uptrend'] = supertrend[-1]['in_uptrend'] if supertrend else True
        return 0.0
    
    signal_bar = data[latest_signal]
    
    # Update signal price if this is a new signal
    stored_signal_bar_idx = symbol_info['signal_bar_idx']
    
    if stored_signal_bar_idx != latest_signal:
        if signal_type == 'buy':
            # For buy signals, use the low of the signal bar
            symbol_info['long_pullback_level'] = signal_bar['low']
        else:
            # For sell signals, use the high of the signal bar
            symbol_info['short_pullback_level'] = signal_bar['high']
        
        symbol_info['signal_type'] = signal_type
        symbol_info['signal_bar_idx'] = latest_signal
    
    # Get stored values
    stored_signal_type = symbol_info['signal_type']
    long_pullback_level = symbol_info['long_pullback_level']
    short_pullback_level = symbol_info['short_pullback_level']
    
    # Set uptrend status from last Supertrend value
    symbol_info['in_uptrend'] = supertrend[-1]['in_uptrend'] if supertrend else True
    
    # Get current price
    current_price = data[-1]['close']
    
    # Calculate percentage change based on signal type
    diff_percent = 0.0
    
    if stored_signal_type == 'buy':
        if long_pullback_level is not None and long_pullback_level > 0:
            diff_percent = ((current_price - long_pullback_level) / long_pullback_level) * 100
    else:
        if short_pullback_level is not None and short_pullback_level > 0:
            diff_percent = ((short_pullback_level - current_price) / short_pullback_level) * 100
    
    return diff_percent

def calculate_all_metrics(
    historical_data: Dict[str, List[Dict]]
) -> Dict[str, Dict[str, Any]]:
    """
    Calculate all metrics for all trading pairs.
    
    Args:
        historical_data: Dictionary mapping symbols to lists of price data
        
    Returns:
        Dictionary of metrics for each symbol
    """
    logger.info("Calculating metrics for all pairs...")
    result = {}
    signal_prices = {}
    
    for symbol, data in historical_data.items():
        try:
            # Skip pairs with insufficient data
            min_required_data = max(
                METRIC_CONFIG['VOLUME_BASELINE_PERIOD'],
                METRIC_CONFIG['MOMENTUM_PERIOD'],
                METRIC_CONFIG['ZSCORE_PERIOD'],
                METRIC_CONFIG['SUPERTREND_PERIOD']
            )
            
            if len(data) < min_required_data:
                logger.warning(f"Skipping {symbol}: insufficient data ({len(data)} < {min_required_data})")
                continue
            
            # Initialize signal_prices for this symbol if needed
            if symbol not in signal_prices:
                signal_prices[symbol] = {
                    'long_pullback_level': None,
                    'short_pullback_level': None,
                    'signal_type': None,
                    'signal_bar_idx': None
                }
            
            # Calculate individual metrics
            volume_metric = calculate_volume_metric(data)
            momentum_metric = calculate_momentum_metric(data)
            total_pct_change = calculate_total_pct_change(data)
            zscore_metric = calculate_zscore_metric(data)
            price_metric = calculate_price_metric(data, signal_prices[symbol])
            
            # Get in_uptrend from signal_prices
            in_uptrend = signal_prices[symbol].get('in_uptrend', True)
            
            # Store metrics in result
            result[symbol] = {
                'symbol': symbol,
                'volume_metric': volume_metric,
                'momentum_metric': momentum_metric,
                'total_pct_change': total_pct_change,
                'zscore_metric': zscore_metric,
                'price_metric': price_metric,
                'price': data[-1]['close'],
                'in_uptrend': in_uptrend
            }
        except Exception as e:
            logger.error(f"Error calculating metrics for {symbol}: {str(e)}")
    
    logger.info(f"Calculated metrics for {len(result)} pairs")
    return result
