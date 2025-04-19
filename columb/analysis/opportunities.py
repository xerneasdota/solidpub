"""
Opportunity detection for the Binance Trading Analysis System.
"""
import logging
from typing import Dict, List, Any, Set, Tuple
from datetime import datetime

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import OPPORTUNITY_THRESHOLD

logger = logging.getLogger(__name__)

class OpportunityDetector:
    """Detects trading opportunities based on metrics and rankings."""
    
    def __init__(self):
        """Initialize the opportunity detector."""
        # Track detected opportunities to avoid duplicates
        self.detected_opportunities = {
            'long': set(),
            'short': set()
        }  # type: Dict[str, Set[str]]
        
        # Track when opportunities were last detected
        self.last_detection_time = None  # type: datetime
        
        # Track ranking history for movement detection
        self.ranking_history = []  # type: List[Dict[str, Any]]
    
    def add_ranking_history(self, rankings, timestamp=None):
        """
        Add rankings to history for movement detection.
        
        Args:
            rankings: Dictionary of rankings for each symbol
            timestamp: Optional timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        self.ranking_history.append({
            'timestamp': timestamp,
            'rankings': rankings
        })
        
        # Limit history size to 10 entries
        if len(self.ranking_history) > 10:
            self.ranking_history.pop(0)
    
    def reset_detection_state(self):
        """Reset detection state to allow re-detection of opportunities."""
        current_time = datetime.now()
        
        # Reset detected opportunities every hour
        if (self.last_detection_time is None or 
            (current_time - self.last_detection_time).total_seconds() > 3600):
            self.detected_opportunities = {
                'long': set(),
                'short': set()
            }
            self.last_detection_time = current_time
    
    def detect_opportunities(
        self,
        rankings: Dict[str, Dict[str, Any]],
        ranking_changes: Dict[str, Dict[str, Any]],
        metrics: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Detect trading opportunities based on metrics and rankings.
        
        Args:
            rankings: Dictionary of rankings for each symbol
            ranking_changes: Dictionary of ranking changes for each symbol
            metrics: Dictionary of metrics for each symbol
            
        Returns:
            Dictionary with 'current' and 'historical' opportunities
        """
        logger.info("Detecting trading opportunities...")
        
        # Add current rankings to history
        self.add_ranking_history(rankings)
        
        # Reset detection state if needed
        self.reset_detection_state()
        
        # Current opportunities
        current_long = []
        current_short = []
        
        # Detect opportunities for each symbol
        for symbol, ranking in rankings.items():
            try:
                changes = ranking_changes.get(symbol, {})
                metric = metrics.get(symbol, {})
                
                # Check for long opportunities
                if self._is_long_opportunity(symbol, ranking, changes, metric):
                    strength = self._calculate_opportunity_strength(ranking, changes, metric, "long")
                    
                    current_long.append({
                        'symbol': symbol,
                        'current_price': metric.get('price', 0.0),
                        'volume_metric': metric.get('volume_metric', 0.0),
                        'momentum_metric': metric.get('momentum_metric', 0.0),
                        'zscore_metric': metric.get('zscore_metric', 0.0),
                        'price_metric': metric.get('price_metric', 0.0),
                        'overall_rank': ranking.get('overall_rank', 999),
                        'rank_change': changes.get('overall_rank_change', 0),
                        'opportunity_strength': strength,
                        'detection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    
                    # Remember this opportunity
                    self.detected_opportunities['long'].add(symbol)
                
                # Check for short opportunities
                if self._is_short_opportunity(symbol, ranking, changes, metric):
                    strength = self._calculate_opportunity_strength(ranking, changes, metric, "short")
                    
                    current_short.append({
                        'symbol': symbol,
                        'current_price': metric.get('price', 0.0),
                        'volume_metric': metric.get('volume_metric', 0.0),
                        'momentum_metric': metric.get('momentum_metric', 0.0),
                        'zscore_metric': metric.get('zscore_metric', 0.0),
                        'price_metric': metric.get('price_metric', 0.0),
                        'overall_rank': ranking.get('overall_rank', 999),
                        'rank_change': changes.get('overall_rank_change', 0),
                        'opportunity_strength': strength,
                        'detection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    
                    # Remember this opportunity
                    self.detected_opportunities['short'].add(symbol)
            except Exception as e:
                logger.error(f"Error detecting opportunities for {symbol}: {str(e)}")
        
        # Sort opportunities by strength (descending)
        current_long.sort(key=lambda x: x.get('opportunity_strength', 0), reverse=True)
        current_short.sort(key=lambda x: x.get('opportunity_strength', 0), reverse=True)
        
        logger.info(f"Found {len(current_long)} long and {len(current_short)} short opportunities")
        
        return {
            'current': {
                'long': current_long,
                'short': current_short
            },
            'historical': {
                'long': current_long,  # In this version, historical is the same as current
                'short': current_short  # In production, you'd maintain a historical list
            }
        }
    
    def _is_long_opportunity(
        self,
        symbol: str,
        ranking: Dict[str, Any],
        changes: Dict[str, Any],
        metrics: Dict[str, Any]
    ) -> bool:
        """
        Check if symbol represents a long opportunity.
        
        Args:
            symbol: Trading pair symbol
            ranking: Ranking data for the symbol
            changes: Ranking changes for the symbol
            metrics: Metrics for the symbol
            
        Returns:
            True if this is a long opportunity
        """
        # Check if we already detected this symbol
        if symbol in self.detected_opportunities['long']:
            return False
        
        # Check if symbol moved from outside top 20 to inside top 20 in last 3 minutes
        return self._check_ranking_movement_for_long(symbol)
    
    def _is_short_opportunity(
        self,
        symbol: str,
        ranking: Dict[str, Any],
        changes: Dict[str, Any],
        metrics: Dict[str, Any]
    ) -> bool:
        """
        Check if symbol represents a short opportunity.
        
        Args:
            symbol: Trading pair symbol
            ranking: Ranking data for the symbol
            changes: Ranking changes for the symbol
            metrics: Metrics for the symbol
            
        Returns:
            True if this is a short opportunity
        """
        # Check if we already detected this symbol
        if symbol in self.detected_opportunities['short']:
            return False
        
        # Check if symbol moved from inside top 20 to outside top 20 in last 3 minutes
        return self._check_ranking_movement_for_short(symbol)
    
    def _check_ranking_movement_for_long(self, symbol: str) -> bool:
        """
        Check if symbol moved from outside top 20 to inside top 20 in last 3 minutes.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if criteria is met
        """
        # Need at least 2 history points
        if len(self.ranking_history) < 2:
            return False
        
        # Get current rankings
        current_rankings = self.ranking_history[-1]['rankings']
        current_overall_rank = current_rankings.get(symbol, {}).get('overall_rank', 999)
        
        # Current rank must be in top 20
        if current_overall_rank > 20:
            return False
        
        # Check previous rankings from recent history (within 3 minutes)
        current_time = self.ranking_history[-1]['timestamp']
        
        for i in range(len(self.ranking_history) - 2, -1, -1):
            history_entry = self.ranking_history[i]
            time_diff = (current_time - history_entry['timestamp']).total_seconds()
            
            # Check if this entry is within the last 3 minutes
            if time_diff <= 3 * 60:  # 3 minutes in seconds
                previous_rank = history_entry['rankings'].get(symbol, {}).get('overall_rank', 999)
                
                # Previous rank must be between 21-40 (was outside top 20 but in top 40)
                if previous_rank > 20 and previous_rank <= 40:
                    # Also check total rank improvement across all metrics
                    total_rank_improvement = 0
                    metrics = ['volume_rank', 'momentum_rank', 'total_pct_rank', 'zscore_rank', 'price_rank']
                    
                    for metric in metrics:
                        prev_metric_rank = history_entry['rankings'].get(symbol, {}).get(metric, 0)
                        curr_metric_rank = current_rankings.get(symbol, {}).get(metric, 0)
                        total_rank_improvement += (prev_metric_rank - curr_metric_rank)
                    
                    # Return true if total rank improvement is > 50
                    return total_rank_improvement > 50
        
        return False
    
    def _check_ranking_movement_for_short(self, symbol: str) -> bool:
        """
        Check if symbol moved from inside top 20 to outside top 20 in last 3 minutes.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if criteria is met
        """
        # Need at least 2 history points
        if len(self.ranking_history) < 2:
            return False
        
        # Get current rankings
        current_rankings = self.ranking_history[-1]['rankings']
        current_overall_rank = current_rankings.get(symbol, {}).get('overall_rank', 999)
        
        # Current rank must be between 21-40 (outside top 20 but in top 40)
        if current_overall_rank <= 20 or current_overall_rank > 40:
            return False
        
        # Check previous rankings from recent history (within 3 minutes)
        current_time = self.ranking_history[-1]['timestamp']
        
        for i in range(len(self.ranking_history) - 2, -1, -1):
            history_entry = self.ranking_history[i]
            time_diff = (current_time - history_entry['timestamp']).total_seconds()
            
            # Check if this entry is within the last 3 minutes
            if time_diff <= 3 * 60:  # 3 minutes in seconds
                previous_rank = history_entry['rankings'].get(symbol, {}).get('overall_rank', 999)
                
                # Previous rank must be in top 20
                if previous_rank <= 20:
                    # Also check total rank degradation across all metrics
                    total_rank_degradation = 0
                    metrics = ['volume_rank', 'momentum_rank', 'total_pct_rank', 'zscore_rank', 'price_rank']
                    
                    for metric in metrics:
                        prev_metric_rank = history_entry['rankings'].get(symbol, {}).get(metric, 0)
                        curr_metric_rank = current_rankings.get(symbol, {}).get(metric, 0)
                        total_rank_degradation += (curr_metric_rank - prev_metric_rank)
                    
                    # Return true if total rank degradation is > 50
                    return total_rank_degradation > 50
        
        return False
    
    def _calculate_opportunity_strength(
        self,
        ranking: Dict[str, Any],
        changes: Dict[str, Any],
        metrics: Dict[str, Any],
        direction: str
    ) -> float:
        """
        Calculate the strength of an opportunity.
        
        Args:
            ranking: Ranking data for the symbol
            changes: Ranking changes for the symbol
            metrics: Metrics for the symbol
            direction: 'long' or 'short'
            
        Returns:
            Opportunity strength score (0-100)
        """
        # Base score starts at 50
        score = 50.0
        
        # Add points for volume (0-15 points)
        volume_metric = metrics.get('volume_metric', 0.0)
        volume_score = 0.0
        
        if direction == "long":
            # For longs, higher volume ratio is better
            volume_score = min(volume_metric / OPPORTUNITY_THRESHOLD['volume'] * 7.5, 15.0)
        else:
            # For shorts, lower volume ratio is better
            inverse_ratio = 1.0 / max(volume_metric, 0.01)  # Avoid division by zero
            volume_score = min(inverse_ratio / OPPORTUNITY_THRESHOLD['volume'] * 7.5, 15.0)
        
        score += volume_score
        
        # Add points for momentum (0-15 points)
        momentum_metric = metrics.get('momentum_metric', 0.0)
        momentum_score = min(
            abs(momentum_metric) / OPPORTUNITY_THRESHOLD['momentum'] * 7.5, 
            15.0
        )
        
        # Only add momentum score if the sign matches the direction
        if (direction == "long" and momentum_metric > 0) or (direction == "short" and momentum_metric < 0):
            score += momentum_score
        
        # Add points for price metric (0-20 points)
        price_metric = metrics.get('price_metric', 0.0)
        price_score = min(price_metric / OPPORTUNITY_THRESHOLD['price'] * 10.0, 20.0)
        score += price_score
        
        # Add points for rank change (0-15 points)
        rank_change = changes.get('overall_rank_change', 0)
        rank_change_multiplier = 1 if direction == "long" else -1
        
        rank_change_value = rank_change * rank_change_multiplier
        if rank_change_value > 0:
            rank_score = min(rank_change_value / OPPORTUNITY_THRESHOLD['rank_change'] * 7.5, 15.0)
            score += rank_score
        
        # Add points for Z-score (0-15 points)
        zscore = metrics.get('zscore_metric', 0.0)
        zscore_multiplier = 1 if direction == "long" else -1
        
        zscore_value = zscore * zscore_multiplier
        if zscore_value > 0:
            zscore_score = min(zscore_value / OPPORTUNITY_THRESHOLD['zscore'] * 7.5, 15.0)
            score += zscore_score
        
        # Add points for trend direction (0-20 points)
        in_uptrend = ranking.get('in_uptrend')
        if in_uptrend is not None:
            if (direction == "long" and in_uptrend) or (direction == "short" and not in_uptrend):
                score += 20.0
        
        # Cap the score at 100
        return min(score, 100.0)

def prepare_slot_machine_data(
    rankings: Dict[str, Dict[str, Any]],
    ranking_changes: Dict[str, Dict[str, Any]],
    algorithm: str = "consistent",
    max_rows: int = 50
) -> Dict[str, Any]:
    """
    Prepare data for slot machine view.
    
    Args:
        rankings: Dictionary of rankings for each symbol
        ranking_changes: Dictionary of ranking changes for each symbol
        algorithm: 'consistent' or 'momentum'
        max_rows: Maximum number of rows to display
        
    Returns:
        Dictionary with columns and matches
    """
    logger.info(f"Preparing slot machine data using {algorithm} algorithm...")
    
    # Skip if no rankings are available
    if not rankings:
        return {'columns': [], 'matches': []}
    
    # Define metrics for slot machine columns
    slot_machine_metrics = [
        {'id': 'overall_rank', 'name': 'Overall', 'key': 'overall_rank', 'format': None, 'sortAscending': True},
        {'id': 'volume_rank', 'name': 'Volume', 'key': 'volume_rank', 'format': None, 'sortAscending': True},
        {'id': 'momentum_rank', 'name': 'Momentum', 'key': 'momentum_rank', 'format': None, 'sortAscending': True},
        {'id': 'price_rank', 'name': 'Price%', 'key': 'price_rank', 'format': None, 'sortAscending': True},
        {'id': 'total_pct_rank', 'name': 'Total%', 'key': 'total_pct_rank', 'format': None, 'sortAscending': True},
        {'id': 'zscore_rank', 'name': 'Z-Score', 'key': 'zscore_rank', 'format': None, 'sortAscending': True},
        {'id': 'in_uptrend', 'name': 'Trend', 'key': 'in_uptrend', 'format': 'trend', 'sortAscending': False}
    ]
    
    # Create slot machine data object
    slot_machine_data = {
        'columns': [],
        'matches': []
    }
    
    # Get all rankings as list for processing
    rankings_list = list(rankings.values())
    
    # Create columns for each metric
    for metric in slot_machine_metrics:
        if metric['id'] == 'overall_rank':
            # Sort by pre-calculated overall_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('overall_rank', 999))
        elif metric['id'] == 'volume_rank':
            # Sort by pre-calculated volume_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('volume_rank', 999))
        elif metric['id'] == 'momentum_rank':
            # Sort by pre-calculated momentum_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('momentum_rank', 999))
        elif metric['id'] == 'price_rank':
            # Sort by pre-calculated price_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('price_rank', 999))
        elif metric['id'] == 'total_pct_rank':
            # Sort by pre-calculated total_pct_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('total_pct_rank', 999))
        elif metric['id'] == 'zscore_rank':
            # Sort by pre-calculated zscore_rank
            sorted_items = sorted(rankings_list, key=lambda x: x.get('zscore_rank', 999))
        elif metric['id'] == 'in_uptrend':
            # For trend, true (uptrend) is sorted higher than false (downtrend)
            sorted_items = sorted(
                rankings_list,
                key=lambda x: 1 if x.get('in_uptrend', True) else 0,
                reverse=metric['sortAscending']
            )
        else:
            # Default sorting by symbol
            sorted_items = sorted(rankings_list, key=lambda x: x['symbol'])
        
        # Limit to max rows
        limited_sorted = sorted_items[:max_rows]
        
        # Add to columns
        slot_machine_data['columns'].append({
            'id': metric['id'],
            'name': metric['name'],
            'items': limited_sorted
        })
    
    # Choose which matching algorithm to use
    if algorithm == "consistent":
        slot_machine_data['matches'] = find_consistent_top_rankings(rankings)
    else:
        slot_machine_data['matches'] = find_momentum_breakthroughs(rankings, ranking_changes)
    
    logger.info(f"Found {len(slot_machine_data['matches'])} slot machine matches")
    return slot_machine_data

def find_consistent_top_rankings(rankings: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Find symbols with consistently high rankings across metrics.
    
    Args:
        rankings: Dictionary of rankings for each symbol
        
    Returns:
        List of match dictionaries
    """
    matches = []
    
    # Process each symbol
    for symbol, data in rankings.items():
        # Get all metric ranks
        metric_ranks = [
            data.get('volume_rank', 999),
            data.get('momentum_rank', 999),
            data.get('price_rank', 999),
            data.get('total_pct_rank', 999),
            data.get('zscore_rank', 999)
        ]
        
        # Check if all metrics are in top 40
        all_in_top_40 = all(rank <= 40 for rank in metric_ranks)
        
        # Count metrics in top 20
        count_in_top_20 = sum(1 for rank in metric_ranks if rank <= 20)
        
        # Check overall rank is in top 20
        overall_in_top_20 = data.get('overall_rank', 999) <= 20
        
        # Match if all criteria met
        if all_in_top_40 and count_in_top_20 >= 4 and overall_in_top_20:
            # Get names of metrics in top 20 for display
            top_metrics = []
            if data.get('volume_rank', 999) <= 20:
                top_metrics.append('volume_rank')
            if data.get('momentum_rank', 999) <= 20:
                top_metrics.append('momentum_rank')
            if data.get('price_rank', 999) <= 20:
                top_metrics.append('price_rank')
            if data.get('total_pct_rank', 999) <= 20:
                top_metrics.append('total_pct_rank')
            if data.get('zscore_rank', 999) <= 20:
                top_metrics.append('zscore_rank')
            
            matches.append({
                'symbol': symbol,
                'rank': data.get('overall_rank', 999),
                'matchCount': count_in_top_20,
                'columns': top_metrics,
                'matchType': "consistent"
            })
    
    # Sort matches by overall rank (ascending)
    matches.sort(key=lambda x: x['rank'])
    return matches

def find_momentum_breakthroughs(
    rankings: Dict[str, Dict[str, Any]],
    ranking_changes: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Find symbols with significant ranking improvements.
    
    Args:
        rankings: Dictionary of rankings for each symbol
        ranking_changes: Dictionary of ranking changes for each symbol
        
    Returns:
        List of match dictionaries
    """
    matches = []
    
    # Process each symbol
    for symbol, data in rankings.items():
        changes = ranking_changes.get(symbol, {})
        
        # Get all metric rank changes
        metric_changes = {
            'volume_rank': changes.get('volume_rank_change', 0),
            'momentum_rank': changes.get('momentum_rank_change', 0),
            'price_rank': changes.get('price_rank_change', 0),
            'total_pct_rank': changes.get('total_pct_rank_change', 0),
            'zscore_rank': changes.get('zscore_rank_change', 0)
        }
        
        # Count significant improvements (positive changes of 5+ positions)
        improved_metrics = [
            metric for metric, change in metric_changes.items()
            if change >= 5
        ]
        
        # Check overall rank improvement
        overall_improvement = changes.get('overall_rank_change', 0)
        
        # Current overall rank is in top 30
        current_rank_good = data.get('overall_rank', 999) <= 30
        
        # Match if criteria met: at least 3 metrics improved significantly, overall improved, and good current rank
        if len(improved_metrics) >= 3 and overall_improvement >= 5 and current_rank_good:
            matches.append({
                'symbol': symbol,
                'rank': data.get('overall_rank', 999),
                'matchCount': len(improved_metrics),
                'overallImprovement': overall_improvement,
                'columns': improved_metrics,
                'matchType': "momentum"
            })
    
    # Sort by overall improvement (descending)
    matches.sort(key=lambda x: x['overallImprovement'], reverse=True)
    return matches
