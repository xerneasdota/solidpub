"""
Ranking calculations for the Binance Trading Analysis System.
"""
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def calculate_rankings(metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Calculate rankings for all metrics and total score.
    
    Args:
        metrics: Dictionary of metrics for each symbol
        
    Returns:
        Dictionary of rankings for each symbol
    """
    logger.info("Calculating rankings...")
    
    # Convert metrics to list for easier ranking
    metrics_list = list(metrics.values())
    
    if not metrics_list:
        logger.warning("No metrics available for ranking calculation")
        return {}
    
    # Calculate ranks for each metric
    volume_ranks = rank_by_metric(metrics_list, 'volume_metric', ascending=False)
    momentum_ranks = rank_by_metric(metrics_list, 'momentum_metric', ascending=False)
    total_pct_ranks = rank_by_metric(metrics_list, 'total_pct_change', ascending=False)
    zscore_ranks = rank_by_metric(metrics_list, 'zscore_metric', ascending=False)
    price_ranks = rank_by_metric(metrics_list, 'price_metric', ascending=False)
    
    # Create rankings dictionary
    rankings = {}
    
    for metric in metrics_list:
        symbol = metric['symbol']
        
        # Get ranks for this symbol
        volume_rank = volume_ranks.get(symbol, 999)
        momentum_rank = momentum_ranks.get(symbol, 999)
        total_pct_rank = total_pct_ranks.get(symbol, 999)
        zscore_rank = zscore_ranks.get(symbol, 999)
        price_rank = price_ranks.get(symbol, 999)
        
        # Calculate total score (sum of all ranks - lower is better)
        total_score = volume_rank + momentum_rank + total_pct_rank + zscore_rank + price_rank
        
        # Store in rankings dictionary
        rankings[symbol] = {
            'symbol': symbol,
            'price': metric['price'],
            'volume_metric': metric['volume_metric'],
            'volume_rank': volume_rank,
            'momentum_metric': metric['momentum_metric'],
            'momentum_rank': momentum_rank,
            'total_pct_change': metric['total_pct_change'],
            'total_pct_rank': total_pct_rank,
            'zscore_metric': metric['zscore_metric'],
            'zscore_rank': zscore_rank,
            'price_metric': metric['price_metric'],
            'price_rank': price_rank,
            'total_score': total_score,
            'in_uptrend': metric.get('in_uptrend', True)
        }
    
    # Calculate overall ranks based on total score
    overall_ranks = rank_by_metric(list(rankings.values()), 'total_score', ascending=True)
    
    # Add overall ranks to rankings dictionary
    for symbol in rankings:
        rankings[symbol]['overall_rank'] = overall_ranks.get(symbol, 999)
    
    logger.info(f"Ranking calculation completed for {len(rankings)} pairs")
    return rankings

def rank_by_metric(
    items: List[Dict[str, Any]],
    metric_key: str,
    ascending: bool = True
) -> Dict[str, int]:
    """
    Rank items by a specific metric.
    
    Args:
        items: List of items to rank
        metric_key: Key for the metric to rank by
        ascending: True for ascending order (lower is better), False for descending (higher is better)
        
    Returns:
        Dictionary mapping symbols to ranks
    """
    # Sort items by the metric
    sorted_items = sorted(items, key=lambda x: _safe_value(x, metric_key), reverse=not ascending)
    
    # Assign ranks (1 = best)
    ranks = {}
    current_rank = 1
    prev_value = None
    
    for idx, item in enumerate(sorted_items):
        symbol = item['symbol']
        value = _safe_value(item, metric_key)
        
        # If this value is different from the previous, assign a new rank
        # Otherwise, use the same rank (ties)
        if idx > 0 and value != prev_value:
            current_rank = idx + 1
        
        ranks[symbol] = current_rank
        prev_value = value
    
    return ranks

def _safe_value(item: Dict[str, Any], key: str) -> float:
    """Get a value safely, handling None or missing keys."""
    value = item.get(key)
    if value is None:
        return 0.0
    return value

def calculate_ranking_changes(
    current_rankings: Dict[str, Dict[str, Any]],
    previous_rankings: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Calculate changes in rankings between current and previous.
    
    Args:
        current_rankings: Current rankings dictionary
        previous_rankings: Previous rankings dictionary (if None, all changes will be 0)
        
    Returns:
        Dictionary of ranking changes for each symbol
    """
    logger.info("Calculating ranking changes...")
    
    changes = {}
    
    # If no previous rankings, set all changes to 0
    if not previous_rankings or not current_rankings:
        for symbol in current_rankings:
            changes[symbol] = {
                'symbol': symbol,
                'volume_rank_change': 0,
                'momentum_rank_change': 0,
                'total_pct_rank_change': 0,
                'zscore_rank_change': 0,
                'price_rank_change': 0,
                'overall_rank_change': 0
            }
        return changes
    
    # Calculate changes for each symbol
    for symbol in current_rankings:
        current = current_rankings[symbol]
        previous = previous_rankings.get(symbol)
        
        if current and previous:
            # Calculate changes in each metric's rank
            # Positive change means improvement (higher rank), negative means decline
            volume_change = previous['volume_rank'] - current['volume_rank']
            momentum_change = previous['momentum_rank'] - current['momentum_rank']
            total_pct_change = previous['total_pct_rank'] - current['total_pct_rank']
            zscore_change = previous['zscore_rank'] - current['zscore_rank']
            price_change = previous['price_rank'] - current['price_rank']
            overall_change = previous['overall_rank'] - current['overall_rank']
            
            changes[symbol] = {
                'symbol': symbol,
                'volume_rank_change': volume_change,
                'momentum_rank_change': momentum_change,
                'total_pct_rank_change': total_pct_change,
                'zscore_rank_change': zscore_change,
                'price_rank_change': price_change,
                'overall_rank_change': overall_change
            }
        else:
            # New symbol, no previous data
            changes[symbol] = {
                'symbol': symbol,
                'volume_rank_change': 0,
                'momentum_rank_change': 0,
                'total_pct_rank_change': 0,
                'zscore_rank_change': 0,
                'price_rank_change': 0,
                'overall_rank_change': 0
            }
    
    return changes

def store_rankings_history(
    history: List[Dict[str, Any]],
    rankings: Dict[str, Dict[str, Any]],
    max_history: int = 10
) -> List[Dict[str, Any]]:
    """
    Store current rankings in history and limit history size.
    
    Args:
        history: List of historical rankings
        rankings: Current rankings to store
        max_history: Maximum number of historical entries to keep
        
    Returns:
        Updated history list
    """
    # Create a timestamp
    timestamp = datetime.now()
    
    # Store rankings with timestamp (deep copy)
    history.append({
        'timestamp': timestamp,
        'rankings': {symbol: ranking.copy() for symbol, ranking in rankings.items()}
    })
    
    # Limit history size
    if len(history) > max_history:
        history = history[-max_history:]
    
    return history
