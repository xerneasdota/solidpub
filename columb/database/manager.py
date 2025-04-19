"""
Database manager for the Binance Trading Analysis System.
Provides a high-level interface for database operations.
"""

import logging
import sqlite3
import os
from datetime import datetime
import uuid

# Fix relative import
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE
from database.schema import init_db, backup_db
from database.models import (
    TradingPair,
    PriceData,
    Metrics,
    Rankings,
    Opportunity,
    BacktestResult,
    BacktestSummary,
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database manager handling connections and providing access to models."""

    def __init__(self, db_path=None):
        """Initialize the database manager."""
        self.db_path = db_path or DATABASE["name"]

        # Ensure the database exists and has the required schema
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        db_exists = os.path.exists(self.db_path)

        if not db_exists:
            init_db(self.db_path)

        # Set up the connection
        self.conn = None
        self._connect()

        # Create model instances
        self.trading_pairs = TradingPair(self.conn)
        self.price_data = PriceData(self.conn)
        self.metrics = Metrics(self.conn)
        self.rankings = Rankings(self.conn)
        self.opportunities = Opportunity(self.conn)
        self.backtest_results = BacktestResult(self.conn)
        self.backtest_summary = BacktestSummary(self.conn)

    def _connect(self):
        """Connect to the database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            logger.debug(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.debug("Database connection closed")

    def backup(self, backup_name=None):
        """Create a backup of the database."""
        self.conn.commit()  # Ensure all changes are committed
        return backup_db(self.db_path, backup_name)

    def create_backtest_id(self):
        """Generate a unique backtest ID."""
        return f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    # Rest of the file continues with all the methods...

    def save_trading_pairs(self, pairs_data):
        """Save or update trading pairs from API data."""
        pairs_to_insert = []

        for pair in pairs_data:
            pairs_to_insert.append((
                pair['symbol'],
                pair.get('baseAsset', ''),
                pair.get('quoteAsset', ''),
                pair.get('status', 'TRADING')
            ))

        if pairs_to_insert:
            return self.trading_pairs.bulk_insert(pairs_to_insert)
        return False

    def save_price_data(self, symbol, timeframe, data):
        """Save price data for a symbol and timeframe."""
        data_to_insert = []

        for item in data:
            # Handle different data formats (API response vs dictionary)
            if isinstance(item, list):
                # API response format (array)
                data_to_insert.append((
                    symbol,
                    timeframe,
                    item[0],  # timestamp
                    float(item[1]),  # open
                    float(item[2]),  # high
                    float(item[3]),  # low
                    float(item[4]),  # close
                    float(item[5]),  # volume
                    float(item[7]) if len(item) > 7 else None,  # quote_asset_volume
                    int(item[8]) if len(item) > 8 else None,  # number_of_trades
                    float(item[9]) if len(item) > 9 else None,  # taker_buy_base_asset_volume
                    float(item[10]) if len(item) > 10 else None  # taker_buy_quote_asset_volume
                ))
            elif isinstance(item, dict):
                # Dictionary format
                data_to_insert.append((
                    symbol,
                    timeframe,
                    item.get('timestamp'),
                    item.get('open'),
                    item.get('high'),
                    item.get('low'),
                    item.get('close'),
                    item.get('volume'),
                    item.get('quote_asset_volume'),
                    item.get('number_of_trades'),
                    item.get('taker_buy_base_asset_volume'),
                    item.get('taker_buy_quote_asset_volume')
                ))

        if data_to_insert:
            return self.price_data.bulk_insert(data_to_insert)
        return False

    def get_price_history(self, symbol, timeframe, limit=1000):
        """Get historical price data for a symbol and timeframe."""
        return self.price_data.get_history(symbol, timeframe, limit)

    def save_metrics(self, timeframe, metrics_data):
        """Save calculated metrics for all symbols at a timeframe."""
        metrics_to_insert = []
        timestamp = int(datetime.now().timestamp() * 1000)

        for symbol, metrics in metrics_data.items():
            metrics_to_insert.append((
                symbol,
                timeframe,
                timestamp,
                metrics.get('volume_metric'),
                metrics.get('momentum_metric'),
                metrics.get('total_pct_change'),
                metrics.get('zscore_metric'),
                metrics.get('price_metric'),
                metrics.get('in_uptrend')
            ))

        if metrics_to_insert:
            return self.metrics.bulk_insert(metrics_to_insert)
        return False

    def save_rankings(self, timeframe, rankings_data):
        """Save calculated rankings for all symbols at a timeframe."""
        rankings_to_insert = []
        timestamp = int(datetime.now().timestamp() * 1000)

        for symbol, ranking in rankings_data.items():
            rankings_to_insert.append((
                symbol,
                timeframe,
                timestamp,
                ranking.get('volume_rank'),
                ranking.get('momentum_rank'),
                ranking.get('total_pct_rank'),
                ranking.get('zscore_rank'),
                ranking.get('price_rank'),
                ranking.get('overall_rank'),
                ranking.get('total_score')
            ))

        if rankings_to_insert:
            return self.rankings.bulk_insert(rankings_to_insert)
        return False

    def save_opportunities(self, timeframe, opportunities_data, direction='long'):
        """Save detected opportunities."""
        opportunities_to_insert = []
        timestamp = int(datetime.now().timestamp() * 1000)

        for opportunity in opportunities_data:
            opportunities_to_insert.append((
                opportunity.get('symbol'),
                timeframe,
                timestamp,
                direction,
                opportunity.get('current_price'),
                opportunity.get('volume_metric'),
                opportunity.get('momentum_metric'),
                opportunity.get('zscore_metric'),
                opportunity.get('price_metric'),
                opportunity.get('overall_rank'),
                opportunity.get('rank_change'),
                opportunity.get('opportunity_strength')
            ))

        if opportunities_to_insert:
            return self.opportunities.bulk_insert(opportunities_to_insert)
        return False

    def save_backtest_results(self, backtest_id, timeframe, results):
        """Save backtest results."""
        results_to_insert = []

        for result in results:
            results_to_insert.append((
                backtest_id,
                result.get('symbol'),
                timeframe,
                result.get('direction'),
                result.get('entryTime', 0),
                result.get('entryPrice'),
                result.get('exitTime'),
                result.get('exitPrice'),
                result.get('exitReason'),
                result.get('pnl'),
                result.get('bars_held')
            ))

        if results_to_insert:
            return self.backtest_results.bulk_insert(results_to_insert)
        return False

    def save_backtest_summary(self, backtest_id, timeframe, summary_data):
        """Save backtest summary statistics."""
        # Save long direction summary
        if 'long' in summary_data:
            long_summary = summary_data['long']
            self.backtest_summary.create(
                backtest_id=backtest_id,
                timeframe=timeframe,
                direction='long',
                total_trades=long_summary.get('totalTrades', 0),
                winning_trades=long_summary.get('winningTrades', 0),
                losing_trades=long_summary.get('losingTrades', 0),
                win_rate=long_summary.get('winRate', 0),
                average_pnl=long_summary.get('averagePnl', 0),
                total_pnl=long_summary.get('totalPnl', 0),
                max_profit=long_summary.get('maxProfit', 0),
                max_loss=long_summary.get('maxLoss', 0),
                avg_bars_held=long_summary.get('avgBarsHeld', 0),
                start_timestamp=long_summary.get('startTimestamp', 0),
                end_timestamp=long_summary.get('endTimestamp', 0)
            )

        # Save short direction summary
        if 'short' in summary_data:
            short_summary = summary_data['short']
            self.backtest_summary.create(
                backtest_id=backtest_id,
                timeframe=timeframe,
                direction='short',
                total_trades=short_summary.get('totalTrades', 0),
                winning_trades=short_summary.get('winningTrades', 0),
                losing_trades=short_summary.get('losingTrades', 0),
                win_rate=short_summary.get('winRate', 0),
                average_pnl=short_summary.get('averagePnl', 0),
                total_pnl=short_summary.get('totalPnl', 0),
                max_profit=short_summary.get('maxProfit', 0),
                max_loss=short_summary.get('maxLoss', 0),
                avg_bars_held=short_summary.get('avgBarsHeld', 0),
                start_timestamp=short_summary.get('startTimestamp', 0),
                end_timestamp=short_summary.get('endTimestamp', 0)
            )

        # Save combined summary
        if 'combined' in summary_data:
            combined_summary = summary_data['combined']
            self.backtest_summary.create(
                backtest_id=backtest_id,
                timeframe=timeframe,
                direction='combined',
                total_trades=combined_summary.get('totalTrades', 0),
                winning_trades=combined_summary.get('winningTrades', 0),
                losing_trades=combined_summary.get('losingTrades', 0),
                win_rate=combined_summary.get('winRate', 0),
                average_pnl=combined_summary.get('averagePnl', 0),
                total_pnl=combined_summary.get('totalPnl', 0),
                max_profit=combined_summary.get('maxProfit', 0),
                max_loss=combined_summary.get('maxLoss', 0),
                avg_bars_held=combined_summary.get('avgBarsHeld', 0),
                start_timestamp=combined_summary.get('startTimestamp', 0),
                end_timestamp=combined_summary.get('endTimestamp', 0)
            )

        return True

    def get_backtest_data_for_export(self, backtest_id):
        """Get complete backtest data for export to Orange3."""
        # Get backtest summary
        summary = self.backtest_summary.get_by_backtest_id(backtest_id)

        # Get detailed results
        results = self.backtest_results.get_by_backtest_id(backtest_id)

        return {
            'summary': summary,
            'results': results
        }

    def get_data_for_machine_learning(self, timeframe, start_time=None, end_time=None):
        """
        Get a dataset suitable for machine learning analysis in Orange3.
        Combines metrics, rankings, and price data.
        """
        if end_time is None:
            end_time = int(datetime.now().timestamp() * 1000)

        if start_time is None:
            # Default to 30 days of data
            start_time = end_time - (30 * 24 * 60 * 60 * 1000)

        query = """
            SELECT 
                m.symbol,
                m.timestamp,
                m.volume_metric,
                m.momentum_metric,
                m.total_pct_change,
                m.zscore_metric,
                m.price_metric,
                m.in_uptrend,
                r.volume_rank,
                r.momentum_rank,
                r.total_pct_rank,
                r.zscore_rank,
                r.price_rank,
                r.overall_rank,
                r.total_score,
                p.open,
                p.high,
                p.low,
                p.close,
                p.volume
            FROM metrics m
            JOIN rankings r ON m.symbol = r.symbol AND m.timestamp = r.timestamp
            JOIN price_data p ON m.symbol = p.symbol AND m.timestamp = p.timestamp
            WHERE 
                m.timeframe = ? AND
                m.timestamp >= ? AND
                m.timestamp <= ?
            ORDER BY m.timestamp ASC, r.overall_rank ASC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (timeframe, start_time, end_time))
        return cursor.fetchall()
