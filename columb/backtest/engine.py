"""
Backtesting engine for the Binance Trading Analysis System.
"""
import logging
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import numpy as np

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BACKTEST_CONFIG
from ..database.manager import DatabaseManager
from ..analysis.metrics import calculate_all_metrics
from ..analysis.rankings import calculate_rankings, calculate_ranking_changes

logger = logging.getLogger(__name__)

class BacktestEngine:
    """Engine for backtesting trading strategies based on metrics and rankings."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the backtest engine.
        
        Args:
            db_manager: Optional database manager for storing results
        """
        self.db_manager = db_manager
        self.results = {
            'long': [],
            'short': []
        }
        self.summary = None
        self.backtest_id = self._generate_backtest_id()
    
    def _generate_backtest_id(self) -> str:
        """Generate a unique backtest ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"backtest_{timestamp}_{uuid.uuid4().hex[:8]}"
    
    def run_backtest(
        self,
        historical_data: Dict[str, List[Dict[str, Any]]],
        timeframe: str,
        start_idx: int = 0,
        end_idx: Optional[int] = None,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        max_bars: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run a backtest on historical data.
        
        Args:
            historical_data: Dictionary mapping symbols to lists of price data
            timeframe: Timeframe string (e.g., "1m", "1h")
            start_idx: Start index for the backtest
            end_idx: End index for the backtest (None for all data)
            take_profit: Take profit percentage
            stop_loss: Stop loss percentage
            max_bars: Maximum number of bars to hold a position
            
        Returns:
            Dictionary of backtest results
        """
        # Use default values from config if not specified
        take_profit = take_profit or BACKTEST_CONFIG['TAKE_PROFIT']
        stop_loss = stop_loss or BACKTEST_CONFIG['STOP_LOSS']
        max_bars = max_bars or BACKTEST_CONFIG['MAX_BARS']
        
        logger.info(f"Running backtest: {self.backtest_id} for timeframe {timeframe}")
        
        # Determine the maximum length of historical data
        max_len = max(len(data) for data in historical_data.values()) if historical_data else 0
        
        if end_idx is None:
            end_idx = max_len - 1
        
        if start_idx >= end_idx or start_idx < 0 or end_idx >= max_len:
            logger.error(f"Invalid start/end indices: {start_idx}/{end_idx} (max: {max_len-1})")
            return {'long': [], 'short': [], 'summary': None}
        
        # Store opportunities detected at each bar
        detected_opportunities = {
            'long': {},
            'short': {}
        }
        
        # Store previous rankings for change calculation
        previous_rankings = None
        
        # Process each bar
        for idx in range(start_idx, end_idx + 1):
            try:
                # Extract data up to current bar for all symbols
                current_data = {
                    symbol: data[:idx+1]
                    for symbol, data in historical_data.items()
                    if len(data) > idx
                }
                
                # Calculate metrics
                metrics = calculate_all_metrics(current_data)
                
                # Calculate rankings
                rankings = calculate_rankings(metrics)
                
                # Calculate ranking changes
                ranking_changes = calculate_ranking_changes(rankings, previous_rankings)
                
                # Detect opportunities
                opportunities = self._detect_opportunities(rankings, ranking_changes, metrics, idx)
                
                # Store detected opportunities
                for direction in ['long', 'short']:
                    for opp in opportunities[direction]:
                        symbol = opp['symbol']
                        detected_opportunities[direction][f"{symbol}_{idx}"] = opp
                
                # Update previous rankings
                previous_rankings = rankings
                
                # Update progress every 100 bars
                if idx % 100 == 0:
                    logger.info(f"Processed {idx-start_idx+1}/{end_idx-start_idx+1} bars")
            
            except Exception as e:
                logger.error(f"Error processing bar {idx}: {str(e)}")
        
        logger.info(f"Detected {len(detected_opportunities['long'])} long and "
                   f"{len(detected_opportunities['short'])} short opportunities")
        
        # Simulate trades for detected opportunities
        self._simulate_trades(
            historical_data,
            detected_opportunities, 
            take_profit,
            stop_loss, 
            max_bars
        )
        
        # Calculate summary statistics
        self._calculate_summary_statistics(timeframe)
        
        # Store results in database if available
        if self.db_manager:
            self._store_results_in_db(timeframe)
        
        return {
            'long': self.results['long'],
            'short': self.results['short'],
            'summary': self.summary
        }
    
    def _detect_opportunities(
        self,
        rankings: Dict[str, Dict[str, Any]],
        ranking_changes: Dict[str, Dict[str, Any]],
        metrics: Dict[str, Dict[str, Any]],
        bar_idx: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect trading opportunities at a specific bar.
        
        Args:
            rankings: Dictionary of rankings for each symbol
            ranking_changes: Dictionary of ranking changes for each symbol
            metrics: Dictionary of metrics for each symbol
            bar_idx: Current bar index
            
        Returns:
            Dictionary of opportunities by direction
        """
        long_opportunities = []
        short_opportunities = []
        
        # Process each symbol
        for symbol, ranking in rankings.items():
            changes = ranking_changes.get(symbol, {})
            metric = metrics.get(symbol, {})
            
            # Long opportunities: good metrics and improving rank
            if (ranking.get('overall_rank', 999) <= 20 and
                changes.get('overall_rank_change', 0) > 0 and
                metric.get('momentum_metric', 0) > 0):
                
                # Calculate opportunity strength
                strength = self._calculate_opportunity_strength(ranking, changes, metric, "long")
                
                long_opportunities.append({
                    'symbol': symbol,
                    'bar_idx': bar_idx,
                    'current_price': metric.get('price', 0.0),
                    'volume_metric': metric.get('volume_metric', 0.0),
                    'momentum_metric': metric.get('momentum_metric', 0.0),
                    'zscore_metric': metric.get('zscore_metric', 0.0),
                    'price_metric': metric.get('price_metric', 0.0),
                    'overall_rank': ranking.get('overall_rank', 999),
                    'rank_change': changes.get('overall_rank_change', 0),
                    'opportunity_strength': strength,
                })
            
            # Short opportunities: declining metrics and worsening rank
            if (20 < ranking.get('overall_rank', 999) <= 40 and
                changes.get('overall_rank_change', 0) < 0 and
                metric.get('momentum_metric', 0) < 0):
                
                # Calculate opportunity strength
                strength = self._calculate_opportunity_strength(ranking, changes, metric, "short")
                
                short_opportunities.append({
                    'symbol': symbol,
                    'bar_idx': bar_idx,
                    'current_price': metric.get('price', 0.0),
                    'volume_metric': metric.get('volume_metric', 0.0),
                    'momentum_metric': metric.get('momentum_metric', 0.0),
                    'zscore_metric': metric.get('zscore_metric', 0.0),
                    'price_metric': metric.get('price_metric', 0.0),
                    'overall_rank': ranking.get('overall_rank', 999),
                    'rank_change': changes.get('overall_rank_change', 0),
                    'opportunity_strength': strength,
                })
        
        # Sort opportunities by strength
        long_opportunities.sort(key=lambda x: x['opportunity_strength'], reverse=True)
        short_opportunities.sort(key=lambda x: x['opportunity_strength'], reverse=True)
        
        return {
            'long': long_opportunities,
            'short': short_opportunities
        }
    
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
        
        # Add points for overall rank (0-20 points)
        overall_rank = ranking.get('overall_rank', 999)
        if direction == "long":
            # For longs, better rank (lower number) gives more points
            if overall_rank <= 10:
                score += 20.0
            elif overall_rank <= 20:
                score += 10.0
            elif overall_rank <= 30:
                score += 5.0
        else:
            # For shorts, rank falling out of top 20 gives points
            if 20 < overall_rank <= 25:
                score += 20.0
            elif 25 < overall_rank <= 30:
                score += 15.0
            elif 30 < overall_rank <= 40:
                score += 10.0
        
        # Add points for momentum (0-15 points)
        momentum = metrics.get('momentum_metric', 0.0)
        if (direction == "long" and momentum > 0) or (direction == "short" and momentum < 0):
            momentum_abs = abs(momentum)
            if momentum_abs > 0.5:
                score += 15.0
            elif momentum_abs > 0.3:
                score += 10.0
            elif momentum_abs > 0.1:
                score += 5.0
        
        # Add points for rank change (0-15 points)
        rank_change = changes.get('overall_rank_change', 0)
        if (direction == "long" and rank_change > 0) or (direction == "short" and rank_change < 0):
            rank_change_abs = abs(rank_change)
            if rank_change_abs >= 10:
                score += 15.0
            elif rank_change_abs >= 5:
                score += 10.0
            elif rank_change_abs >= 2:
                score += 5.0
        
        # Add points for trend direction (0-10 points)
        in_uptrend = ranking.get('in_uptrend', True)
        if (direction == "long" and in_uptrend) or (direction == "short" and not in_uptrend):
            score += 10.0
        
        return min(score, 100.0)
    
    def _simulate_trades(
        self,
        historical_data: Dict[str, List[Dict[str, Any]]],
        detected_opportunities: Dict[str, Dict[str, Dict[str, Any]]],
        take_profit: float,
        stop_loss: float,
        max_bars: int
    ) -> None:
        """
        Simulate trades for detected opportunities.
        
        Args:
            historical_data: Dictionary mapping symbols to lists of price data
            detected_opportunities: Dictionary of detected opportunities
            take_profit: Take profit percentage
            stop_loss: Stop loss percentage
            max_bars: Maximum number of bars to hold a position
        """
        logger.info("Simulating trades...")
        
        # Clear previous results
        self.results = {
            'long': [],
            'short': []
        }
        
        # Process long opportunities
        for key, opportunity in detected_opportunities['long'].items():
            symbol = opportunity['symbol']
            entry_bar_idx = opportunity['bar_idx']
            
            # Skip if not enough data for this symbol
            if symbol not in historical_data or len(historical_data[symbol]) <= entry_bar_idx:
                continue
            
            # Get entry data
            entry_bar = historical_data[symbol][entry_bar_idx]
            entry_price = entry_bar['close']
            entry_time = entry_bar['timestamp']
            
            # Simulate the trade
            result = self._simulate_single_trade(
                historical_data[symbol],
                symbol,
                'long',
                entry_bar_idx,
                entry_price,
                entry_time,
                take_profit,
                stop_loss,
                max_bars
            )
            
            if result:
                self.results['long'].append(result)
        
        # Process short opportunities
        for key, opportunity in detected_opportunities['short'].items():
            symbol = opportunity['symbol']
            entry_bar_idx = opportunity['bar_idx']
            
            # Skip if not enough data for this symbol
            if symbol not in historical_data or len(historical_data[symbol]) <= entry_bar_idx:
                continue
            
            # Get entry data
            entry_bar = historical_data[symbol][entry_bar_idx]
            entry_price = entry_bar['close']
            entry_time = entry_bar['timestamp']
            
            # Simulate the trade
            result = self._simulate_single_trade(
                historical_data[symbol],
                symbol,
                'short',
                entry_bar_idx,
                entry_price,
                entry_time,
                take_profit,
                stop_loss,
                max_bars
            )
            
            if result:
                self.results['short'].append(result)
        
        logger.info(f"Simulated {len(self.results['long'])} long and {len(self.results['short'])} short trades")
    
    def _simulate_single_trade(
        self,
        symbol_data: List[Dict[str, Any]],
        symbol: str,
        direction: str,
        entry_bar_idx: int,
        entry_price: float,
        entry_time: int,
        take_profit: float,
        stop_loss: float,
        max_bars: int
    ) -> Optional[Dict[str, Any]]:
        """
        Simulate a single trade.
        
        Args:
            symbol_data: List of price data for the symbol
            symbol: Trading pair symbol
            direction: 'long' or 'short'
            entry_bar_idx: Index of entry bar
            entry_price: Entry price
            entry_time: Entry timestamp
            take_profit: Take profit percentage
            stop_loss: Stop loss percentage
            max_bars: Maximum number of bars to hold the position
            
        Returns:
            Dictionary with trade result or None if no exit
        """
        # Find future bars to check for exit
        future_bars = symbol_data[entry_bar_idx + 1:entry_bar_idx + max_bars + 1]
        
        # No future bars, no trade
        if not future_bars:
            return None
        
        exit_bar = None
        exit_price = None
        exit_time = None
        exit_reason = None
        bars_held = 0
        
        # Check each future bar for exit conditions
        for i, bar in enumerate(future_bars):
            current_price = bar['close']
            
            # Calculate PnL percentage
            if direction == 'long':
                pnl_pct = ((current_price - entry_price) / entry_price) * 100
            else:  # short
                pnl_pct = ((entry_price - current_price) / entry_price) * 100
            
            # Check exit conditions
            if pnl_pct >= take_profit:
                exit_bar = bar
                exit_price = current_price
                exit_time = bar['timestamp']
                exit_reason = 'take_profit'
                bars_held = i + 1
                break
            
            elif pnl_pct <= -stop_loss:
                exit_bar = bar
                exit_price = current_price
                exit_time = bar['timestamp']
                exit_reason = 'stop_loss'
                bars_held = i + 1
                break
            
            # If this is the last bar we can check, exit at current price
            if i == len(future_bars) - 1:
                exit_bar = bar
                exit_price = current_price
                exit_time = bar['timestamp']
                exit_reason = 'max_duration'
                bars_held = i + 1
        
        # If no exit was found
        if not exit_bar:
            return None
        
        # Calculate final PnL
        if direction == 'long':
            pnl = ((exit_price - entry_price) / entry_price) * 100
        else:  # short
            pnl = ((entry_price - exit_price) / entry_price) * 100
        
        return {
            'symbol': symbol,
            'direction': direction,
            'entryPrice': entry_price,
            'entryTime': entry_time,
            'exitPrice': exit_price,
            'exitTime': exit_time,
            'exitReason': exit_reason,
            'pnl': pnl,
            'bars_held': bars_held
        }
    
    def _calculate_summary_statistics(self, timeframe: str) -> None:
        """
        Calculate summary statistics for backtest results.
        
        Args:
            timeframe: Timeframe string (e.g., "1m", "1h")
        """
        start_time = min(
            [r.get('entryTime', 0) for r in self.results['long'] + self.results['short']] or [0]
        )
        
        end_time = max(
            [r.get('exitTime', 0) for r in self.results['long'] + self.results['short']] or [0]
        )
        
        # Calculate long summary
        long_results = self.results['long']
        long_summary = self._calculate_direction_summary(long_results, start_time, end_time)
        
        # Calculate short summary
        short_results = self.results['short']
        short_summary = self._calculate_direction_summary(short_results, start_time, end_time)
        
        # Calculate combined summary
        combined_results = long_results + short_results
        combined_summary = self._calculate_direction_summary(combined_results, start_time, end_time)
        
        # Store summary
        self.summary = {
            'backtest_id': self.backtest_id,
            'timeframe': timeframe,
            'long': long_summary,
            'short': short_summary,
            'combined': combined_summary
        }
    
    def _calculate_direction_summary(
        self,
        results: List[Dict[str, Any]],
        start_time: int,
        end_time: int
    ) -> Dict[str, Any]:
        """
        Calculate summary statistics for a specific direction.
        
        Args:
            results: List of trade results
            start_time: Start timestamp of backtest
            end_time: End timestamp of backtest
            
        Returns:
            Dictionary of summary statistics
        """
        if not results:
            return {
                'totalTrades': 0,
                'winningTrades': 0,
                'losingTrades': 0,
                'winRate': 0.0,
                'averagePnl': 0.0,
                'totalPnl': 0.0,
                'maxProfit': 0.0,
                'maxLoss': 0.0,
                'avgBarsHeld': 0.0,
                'startTimestamp': start_time,
                'endTimestamp': end_time
            }
        
        # Extract PnL values
        pnl_values = [r.get('pnl', 0.0) for r in results]
        
        # Count winning and losing trades
        winning_trades = sum(1 for pnl in pnl_values if pnl > 0)
        losing_trades = sum(1 for pnl in pnl_values if pnl <= 0)
        
        # Calculate statistics
        win_rate = (winning_trades / len(results)) * 100 if results else 0.0
        average_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0.0
        total_pnl = sum(pnl_values)
        max_profit = max(pnl_values) if pnl_values else 0.0
        max_loss = min(pnl_values) if pnl_values else 0.0
        avg_bars_held = sum(r.get('bars_held', 0) for r in results) / len(results) if results else 0.0
        
        return {
            'totalTrades': len(results),
            'winningTrades': winning_trades,
            'losingTrades': losing_trades,
            'winRate': win_rate,
            'averagePnl': average_pnl,
            'totalPnl': total_pnl,
            'maxProfit': max_profit,
            'maxLoss': max_loss,
            'avgBarsHeld': avg_bars_held,
            'startTimestamp': start_time,
            'endTimestamp': end_time
        }
    
    def _store_results_in_db(self, timeframe: str) -> None:
        """
        Store backtest results and summary in the database.
        
        Args:
            timeframe: Timeframe string (e.g., "1m", "1h")
        """
        if not self.db_manager:
            logger.warning("No database manager available to store results")
            return
        
        try:
            # Store results
            self.db_manager.save_backtest_results(self.backtest_id, timeframe, self.results['long'])
            self.db_manager.save_backtest_results(self.backtest_id, timeframe, self.results['short'])
            
            # Store summary
            self.db_manager.save_backtest_summary(self.backtest_id, timeframe, self.summary)
            
            logger.info(f"Backtest results stored in database with ID: {self.backtest_id}")
        except Exception as e:
            logger.error(f"Error storing backtest results in database: {str(e)}")
