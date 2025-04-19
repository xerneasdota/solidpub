"""
Technical indicators for the Binance Trading Analysis System.
"""
import logging
import numpy as np
from typing import List, Dict, Union, Tuple, Optional

logger = logging.getLogger(__name__)

def calculate_atr(data: List[Dict], period: int = 14) -> List[float]:
    """
    Calculate Average True Range (ATR).
    
    Args:
        data: List of price data dictionaries with 'high', 'low', 'close' keys
        period: ATR period
    
    Returns:
        List of ATR values
    """
    if len(data) < period:
        logger.warning(f"Not enough data for ATR calculation. Need {period}, got {len(data)}")
        return [0] * len(data)
    
    high_prices = np.array([candle['high'] for candle in data])
    low_prices = np.array([candle['low'] for candle in data])
    close_prices = np.array([candle['close'] for candle in data])
    
    # Calculate True Range
    tr1 = high_prices - low_prices
    tr2 = np.abs(high_prices - np.roll(close_prices, 1))
    tr3 = np.abs(low_prices - np.roll(close_prices, 1))
    
    # Handle first element (no previous close)
    tr2[0] = 0
    tr3[0] = 0
    
    # True Range is the maximum of the three
    tr = np.maximum(np.maximum(tr1, tr2), tr3)
    
    # Calculate ATR
    atr = np.zeros(len(data))
    atr[0] = tr[0]  # First ATR is just the first TR
    
    # Calculate subsequent ATR values
    for i in range(1, len(data)):
        if i < period:
            # Simple average until we have enough data
            atr[i] = np.mean(tr[:i+1])
        else:
            # Smoothed average afterward
            atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    
    return atr.tolist()

def calculate_rsi(data: List[Union[Dict, float]], period: int = 14) -> List[Optional[float]]:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        data: List of price data dictionaries with 'close' key or list of values
        period: RSI period
    
    Returns:
        List of RSI values (None for values before period)
    """
    if len(data) <= period:
        logger.warning(f"Not enough data for RSI calculation. Need > {period}, got {len(data)}")
        return [None] * len(data)
    
    # Extract close prices if dictionaries are provided
    if isinstance(data[0], dict):
        prices = np.array([candle['close'] for candle in data])
    else:
        prices = np.array(data)
    
    # Calculate price changes
    changes = np.diff(prices)
    
    # Create arrays of gains and losses
    gains = np.zeros_like(changes)
    losses = np.zeros_like(changes)
    
    # Populate gains and losses
    gains[changes > 0] = changes[changes > 0]
    losses[changes < 0] = -changes[changes < 0]
    
    # Prepend a None to match original data length
    result = [None]
    
    # Not enough data yet
    if len(gains) < period:
        result.extend([None] * len(gains))
        return result
    
    # Calculate first average gain and loss
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    # Calculate first RSI
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
    
    result.append(rsi)
    
    # Calculate RSI for remaining data
    for i in range(period, len(changes)):
        # Update average gain and loss using smoothing formula
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        
        # Calculate RS and RSI
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        
        result.append(rsi)
    
    # Fill any additional None values if needed
    while len(result) < len(data):
        result.append(None)
    
    return result

def calculate_supertrend(
    data: List[Dict],
    period: int = 10,
    multiplier: float = 3.0
) -> List[Dict]:
    """
    Calculate Supertrend indicator.
    
    Args:
        data: List of price data dictionaries with 'high', 'low', 'close' keys
        period: ATR period
        multiplier: ATR multiplier
    
    Returns:
        List of Supertrend dictionaries with upperband, lowerband, in_uptrend, etc.
    """
    if len(data) < period:
        logger.warning(f"Not enough data for Supertrend calculation. Need {period}, got {len(data)}")
        return [{'upperband': None, 'lowerband': None, 'in_uptrend': None, 'signal_change': False}] * len(data)
    
    # Calculate ATR
    atr = calculate_atr(data, period)
    
    # Initialize Supertrend
    supertrend = []
    for i in range(len(data)):
        if i < period:
            # Add placeholder values for the first 'period' elements
            supertrend.append({
                'upperband': None,
                'lowerband': None,
                'in_uptrend': True,  # Default to uptrend
                'signal_change': False,
                'buy_signal': False,
                'sell_signal': False
            })
            continue
        
        high = data[i]['high']
        low = data[i]['low']
        close = data[i]['close']
        current_atr = atr[i]
        
        # Calculate basic upper and lower bands
        basic_upperband = ((high + low) / 2) + (multiplier * current_atr)
        basic_lowerband = ((high + low) / 2) - (multiplier * current_atr)
        
        # Previous values
        prev_upperband = supertrend[i-1]['upperband']
        prev_lowerband = supertrend[i-1]['lowerband']
        prev_in_uptrend = supertrend[i-1]['in_uptrend']
        
        # First valid Supertrend calculation
        if i == period:
            upperband = basic_upperband
            lowerband = basic_lowerband
            in_uptrend = True
            signal_change = False
            buy_signal = False
            sell_signal = False
        else:
            # Adjust upper and lower bands based on previous trend
            if prev_in_uptrend:
                upperband = basic_upperband
                lowerband = max(basic_lowerband, prev_lowerband)
                
                # Check if still in uptrend
                in_uptrend = close >= lowerband
            else:
                upperband = min(basic_upperband, prev_upperband)
                lowerband = basic_lowerband
                
                # Check if still in downtrend
                in_uptrend = close >= upperband
            
            # Detect signal changes
            signal_change = in_uptrend != prev_in_uptrend
            buy_signal = in_uptrend and signal_change
            sell_signal = not in_uptrend and signal_change
        
        # Add to result
        supertrend.append({
            'upperband': upperband,
            'lowerband': lowerband,
            'in_uptrend': in_uptrend,
            'signal_change': signal_change,
            'buy_signal': buy_signal,
            'sell_signal': sell_signal
        })
    
    return supertrend

def calculate_zscore(data: List[Union[Dict, float]], period: int = 20) -> List[Optional[float]]:
    """
    Calculate Z-Score for a time series.
    
    Args:
        data: List of price data dictionaries with 'close' key or list of values
        period: Z-Score period
    
    Returns:
        List of Z-Score values (None for values before period)
    """
    if len(data) < period:
        logger.warning(f"Not enough data for Z-Score calculation. Need {period}, got {len(data)}")
        return [None] * len(data)
    
    # Extract close prices if dictionaries are provided
    if isinstance(data[0], dict):
        prices = np.array([candle['close'] for candle in data])
    else:
        prices = np.array(data)
    
    # Initialize result with None values
    result = [None] * len(data)
    
    # Calculate Z-Score for each window
    for i in range(period - 1, len(data)):
        window = prices[i - period + 1:i + 1]
        mean = np.mean(window)
        std = np.std(window)
        
        if std > 0:
            # Calculate Z-Score
            zscore = (prices[i] - mean) / std
            result[i] = zscore
        else:
            # If standard deviation is zero, Z-Score is undefined
            result[i] = 0.0
    
    return result

def calculate_moving_average(data: List[Union[Dict, float]], period: int = 20) -> List[Optional[float]]:
    """
    Calculate Simple Moving Average (SMA).
    
    Args:
        data: List of price data dictionaries with 'close' key or list of values
        period: SMA period
    
    Returns:
        List of SMA values (None for values before period)
    """
    if len(data) < period:
        logger.warning(f"Not enough data for SMA calculation. Need {period}, got {len(data)}")
        return [None] * len(data)
    
    # Extract close prices if dictionaries are provided
    if isinstance(data[0], dict):
        prices = np.array([candle['close'] for candle in data])
    else:
        prices = np.array(data)
    
    # Initialize result with None values
    result = [None] * (period - 1)
    
    # Calculate SMA for each window
    for i in range(period, len(data) + 1):
        sma = np.mean(prices[i - period:i])
        result.append(sma)
    
    return result

def calculate_ema(data: List[Union[Dict, float]], period: int = 20) -> List[Optional[float]]:
    """
    Calculate Exponential Moving Average (EMA).
    
    Args:
        data: List of price data dictionaries with 'close' key or list of values
        period: EMA period
    
    Returns:
        List of EMA values (None for values before period)
    """
    if len(data) < period:
        logger.warning(f"Not enough data for EMA calculation. Need {period}, got {len(data)}")
        return [None] * len(data)
    
    # Extract close prices if dictionaries are provided
    if isinstance(data[0], dict):
        prices = np.array([candle['close'] for candle in data])
    else:
        prices = np.array(data)
    
    # Calculate multiplier
    multiplier = 2 / (period + 1)
    
    # Initialize EMA with SMA for the first 'period' elements
    ema = np.zeros_like(prices)
    ema[:period] = np.nan
    ema[period - 1] = np.mean(prices[:period])
    
    # Calculate EMA for remaining prices
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * multiplier + ema[i - 1]
    
    # Convert to list and replace NaN with None
    result = []
    for value in ema:
        if np.isnan(value):
            result.append(None)
        else:
            result.append(float(value))
    
    return result

def calculate_price_change_percentage(
    data: List[Dict],
    period: int = 1,
    mode: str = 'close-to-close'
) -> List[Optional[float]]:
    """
    Calculate price change percentage.
    
    Args:
        data: List of price data dictionaries with 'open', 'high', 'low', 'close' keys
        period: Period for percentage change
        mode: 'close-to-close', 'open-to-close', or 'high-to-low'
    
    Returns:
        List of percentage changes
    """
    if len(data) < period + 1:
        logger.warning(f"Not enough data for price change calculation. Need {period + 1}, got {len(data)}")
        return [None] * len(data)
    
    result = [None] * period
    
    for i in range(period, len(data)):
        if mode == 'close-to-close':
            start_price = data[i - period]['close']
            end_price = data[i]['close']
        elif mode == 'open-to-close':
            start_price = data[i]['open']
            end_price = data[i]['close']
        elif mode == 'high-to-low':
            start_price = data[i]['high']
            end_price = data[i]['low']
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'close-to-close', 'open-to-close', or 'high-to-low'")
        
        if start_price > 0:
            pct_change = ((end_price - start_price) / start_price) * 100
            result.append(pct_change)
        else:
            result.append(0.0)
    
    return result

def calculate_momentum(data: List[Union[Dict, float]], period: int = 14) -> List[Optional[float]]:
    """
    Calculate momentum indicator.
    
    Args:
        data: List of price data dictionaries with 'close' key or list of values
        period: Momentum period
        
    Returns:
        List of momentum values (None for values before period)
    """
    if len(data) <= period:
        logger.warning(f"Not enough data for momentum calculation. Need > {period}, got {len(data)}")
        return [None] * len(data)
    
    # Extract close prices if dictionaries are provided
    if isinstance(data[0], dict):
        prices = np.array([candle['close'] for candle in data])
    else:
        prices = np.array(data)
    
    # Calculate momentum (current price - price 'period' periods ago)
    momentum = np.zeros_like(prices)
    momentum[:period] = np.nan
    
    for i in range(period, len(prices)):
        momentum[i] = prices[i] - prices[i - period]
    
    # Convert to list and replace NaN with None
    result = []
    for value in momentum:
        if np.isnan(value):
            result.append(None)
        else:
            result.append(float(value))
    
    return result

def identify_support_resistance(
    data: List[Dict],
    window: int = 5,
    threshold: float = 0.03
) -> Tuple[List[float], List[float]]:
    """
    Identify support and resistance levels.
    
    Args:
        data: List of price data dictionaries with 'high', 'low', 'close' keys
        window: Window size for local minima/maxima
        threshold: Threshold for grouping levels (as percentage)
        
    Returns:
        Tuple of (support_levels, resistance_levels)
    """
    if len(data) < window * 2 + 1:
        logger.warning(f"Not enough data for support/resistance calculation. Need {window * 2 + 1}, got {len(data)}")
        return [], []
    
    # Extract price data
    highs = np.array([candle['high'] for candle in data])
    lows = np.array([candle['low'] for candle in data])
    
    support_levels = []
    resistance_levels = []
    
    # Find local minima (support) and maxima (resistance)
    for i in range(window, len(data) - window):
        # Check if this is a local minimum
        if all(lows[i] <= lows[i - j] for j in range(1, window + 1)) and all(lows[i] <= lows[i + j] for j in range(1, window + 1)):
            support_levels.append(lows[i])
        
        # Check if this is a local maximum
        if all(highs[i] >= highs[i - j] for j in range(1, window + 1)) and all(highs[i] >= highs[i + j] for j in range(1, window + 1)):
            resistance_levels.append(highs[i])
    
    # Group nearby levels
    support_levels = group_nearby_levels(support_levels, threshold)
    resistance_levels = group_nearby_levels(resistance_levels, threshold)
    
    return support_levels, resistance_levels

def group_nearby_levels(levels: List[float], threshold: float) -> List[float]:
    """
    Group nearby price levels.
    
    Args:
        levels: List of price levels
        threshold: Threshold for grouping levels (as percentage)
        
    Returns:
        List of grouped price levels
    """
    if not levels:
        return []
    
    # Sort levels
    sorted_levels = sorted(levels)
    
    # Group nearby levels
    grouped = []
    current_group = [sorted_levels[0]]
    
    for level in sorted_levels[1:]:
        # Calculate percentage difference
        prev_level = current_group[-1]
        diff_pct = abs((level - prev_level) / prev_level) if prev_level > 0 else 0
        
        if diff_pct <= threshold:
            # Add to current group
            current_group.append(level)
        else:
            # Take average of current group and start a new group
            grouped.append(sum(current_group) / len(current_group))
            current_group = [level]
    
    # Add the last group
    if current_group:
        grouped.append(sum(current_group) / len(current_group))
    
    return grouped
