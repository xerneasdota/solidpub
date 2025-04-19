"""
Database models for the Binance Trading Analysis System.
These models provide a simple ORM-like interface to the database.
"""
import sqlite3
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseModel:
    """Base class for all models providing common functionality."""
    
    table_name = None
    
    def __init__(self, conn=None):
        self.conn = conn
        self._cursor = None
    
    @property
    def cursor(self):
        if self._cursor is None and self.conn is not None:
            self._cursor = self.conn.cursor()
        return self._cursor
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
    
    def commit(self):
        """Commit the current transaction."""
        if self.conn:
            self.conn.commit()
    
    def rollback(self):
        """Rollback the current transaction."""
        if self.conn:
            self.conn.rollback()
    
    @classmethod
    def dict_factory(cls, cursor, row):
        """Convert a row to a dictionary."""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
    def execute(self, query, params=None):
        """Execute a query with parameters."""
        try:
            if params:
                return self.cursor.execute(query, params)
            else:
                return self.cursor.execute(query)
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.rollback()
            raise
    
    def executemany(self, query, params_list):
        """Execute a query with multiple parameter sets."""
        try:
            return self.cursor.executemany(query, params_list)
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.rollback()
            raise
    
    def fetchall(self):
        """Fetch all rows from the last query."""
        return self.cursor.fetchall()
    
    def fetchone(self):
        """Fetch one row from the last query."""
        return self.cursor.fetchone()
    
    def get_by_id(self, id):
        """Get a record by its ID."""
        query = f"SELECT * FROM {self.table_name} WHERE id = ?"
        self.execute(query, (id,))
        return self.fetchone()
    
    def get_all(self, limit=None, offset=None, order_by=None):
        """Get all records with optional pagination and ordering."""
        query = f"SELECT * FROM {self.table_name}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
        
        self.execute(query)
        return self.fetchall()
    
    def count(self, where=None, params=None):
        """Count records with optional filtering."""
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        self.execute(query, params)
        return self.fetchone()[0]
    
    def delete(self, id):
        """Delete a record by its ID."""
        query = f"DELETE FROM {self.table_name} WHERE id = ?"
        self.execute(query, (id,))
        self.commit()
        return self.cursor.rowcount


class TradingPair(BaseModel):
    """Model for trading pairs."""
    
    table_name = 'trading_pairs'
    
    def create(self, symbol, base_asset, quote_asset, status):
        """Create a new trading pair record."""
        query = """
            INSERT OR REPLACE INTO trading_pairs (symbol, base_asset, quote_asset, status)
            VALUES (?, ?, ?, ?)
        """
        self.execute(query, (symbol, base_asset, quote_asset, status))
        self.commit()
        return True
    
    def get_by_symbol(self, symbol):
        """Get a trading pair by its symbol."""
        query = "SELECT * FROM trading_pairs WHERE symbol = ?"
        self.execute(query, (symbol,))
        return self.fetchone()
    
    def get_active_pairs(self):
        """Get all active trading pairs."""
        query = "SELECT * FROM trading_pairs WHERE status = 'TRADING'"
        self.execute(query)
        return self.fetchall()
    
    def bulk_insert(self, pairs):
        """Insert multiple trading pairs at once."""
        query = """
            INSERT OR REPLACE INTO trading_pairs (symbol, base_asset, quote_asset, status)
            VALUES (?, ?, ?, ?)
        """
        self.executemany(query, pairs)
        self.commit()
        return True


class PriceData(BaseModel):
    """Model for price/OHLCV data."""
    
    table_name = 'price_data'
    
    def create(self, symbol, timeframe, timestamp, open_price, high, low, close, volume, 
               quote_asset_volume=None, number_of_trades=None, 
               taker_buy_base_asset_volume=None, taker_buy_quote_asset_volume=None):
        """Create a new price data record."""
        query = """
            INSERT OR REPLACE INTO price_data 
            (symbol, timeframe, timestamp, open, high, low, close, volume, 
             quote_asset_volume, number_of_trades, 
             taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            symbol, timeframe, timestamp, open_price, high, low, close, volume,
            quote_asset_volume, number_of_trades,
            taker_buy_base_asset_volume, taker_buy_quote_asset_volume
        ))
        self.commit()
        return True
    
    def bulk_insert(self, data_list):
        """Insert multiple price data records at once."""
        query = """
            INSERT OR REPLACE INTO price_data 
            (symbol, timeframe, timestamp, open, high, low, close, volume, 
             quote_asset_volume, number_of_trades, 
             taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(query, data_list)
        self.commit()
        return True
    
    def get_latest(self, symbol, timeframe, limit=1):
        """Get the latest price data for a symbol and timeframe."""
        query = """
            SELECT * FROM price_data 
            WHERE symbol = ? AND timeframe = ? 
            ORDER BY timestamp DESC LIMIT ?
        """
        self.execute(query, (symbol, timeframe, limit))
        if limit == 1:
            return self.fetchone()
        return self.fetchall()
    
    def get_history(self, symbol, timeframe, limit=1000, end_time=None):
        """Get historical price data for a symbol and timeframe."""
        if end_time is None:
            end_time = int(datetime.now().timestamp() * 1000)
        
        query = """
            SELECT * FROM price_data 
            WHERE symbol = ? AND timeframe = ? AND timestamp <= ? 
            ORDER BY timestamp DESC LIMIT ?
        """
        self.execute(query, (symbol, timeframe, end_time, limit))
        return self.fetchall()
    
    def get_timerange(self, symbol, timeframe, start_time, end_time=None):
        """Get price data for a symbol and timeframe within a time range."""
        if end_time is None:
            end_time = int(datetime.now().timestamp() * 1000)
        
        query = """
            SELECT * FROM price_data 
            WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ? 
            ORDER BY timestamp ASC
        """
        self.execute(query, (symbol, timeframe, start_time, end_time))
        return self.fetchall()


class Metrics(BaseModel):
    """Model for calculated metrics."""
    
    table_name = 'metrics'
    
    def create(self, symbol, timeframe, timestamp, volume_metric=None, momentum_metric=None,
               total_pct_change=None, zscore_metric=None, price_metric=None, in_uptrend=None):
        """Create a new metrics record."""
        query = """
            INSERT OR REPLACE INTO metrics 
            (symbol, timeframe, timestamp, volume_metric, momentum_metric, 
             total_pct_change, zscore_metric, price_metric, in_uptrend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            symbol, timeframe, timestamp, volume_metric, momentum_metric,
            total_pct_change, zscore_metric, price_metric, 
            1 if in_uptrend else 0 if in_uptrend is not None else None
        ))
        self.commit()
        return True
    
    def bulk_insert(self, metrics_list):
        """Insert multiple metrics records at once."""
        query = """
            INSERT OR REPLACE INTO metrics 
            (symbol, timeframe, timestamp, volume_metric, momentum_metric, 
             total_pct_change, zscore_metric, price_metric, in_uptrend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # Convert boolean in_uptrend to integer
        processed_list = []
        for item in metrics_list:
            processed_item = list(item)
            if len(processed_item) >= 9 and processed_item[8] is not None:
                processed_item[8] = 1 if processed_item[8] else 0
            processed_list.append(tuple(processed_item))
        
        self.executemany(query, processed_list)
        self.commit()
        return True
    
    def get_latest(self, symbol, timeframe):
        """Get the latest metrics for a symbol and timeframe."""
        query = """
            SELECT * FROM metrics 
            WHERE symbol = ? AND timeframe = ? 
            ORDER BY timestamp DESC LIMIT 1
        """
        self.execute(query, (symbol, timeframe))
        return self.fetchone()
    
    def get_all_latest(self, timeframe):
        """Get the latest metrics for all symbols at a specific timeframe."""
        query = """
            SELECT m.* FROM metrics m
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_timestamp
                FROM metrics
                WHERE timeframe = ?
                GROUP BY symbol
            ) m2 ON m.symbol = m2.symbol AND m.timestamp = m2.max_timestamp
            WHERE m.timeframe = ?
        """
        self.execute(query, (timeframe, timeframe))
        return self.fetchall()
    
    def get_history(self, symbol, timeframe, limit=100):
        """Get historical metrics for a symbol and timeframe."""
        query = """
            SELECT * FROM metrics 
            WHERE symbol = ? AND timeframe = ? 
            ORDER BY timestamp DESC LIMIT ?
        """
        self.execute(query, (symbol, timeframe, limit))
        return self.fetchall()


class Rankings(BaseModel):
    """Model for calculated rankings."""
    
    table_name = 'rankings'
    
    def create(self, symbol, timeframe, timestamp, volume_rank=None, momentum_rank=None,
               total_pct_rank=None, zscore_rank=None, price_rank=None, overall_rank=None,
               total_score=None):
        """Create a new ranking record."""
        query = """
            INSERT OR REPLACE INTO rankings 
            (symbol, timeframe, timestamp, volume_rank, momentum_rank, 
             total_pct_rank, zscore_rank, price_rank, overall_rank, total_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            symbol, timeframe, timestamp, volume_rank, momentum_rank,
            total_pct_rank, zscore_rank, price_rank, overall_rank, total_score
        ))
        self.commit()
        return True
    
    def bulk_insert(self, rankings_list):
        """Insert multiple ranking records at once."""
        query = """
            INSERT OR REPLACE INTO rankings 
            (symbol, timeframe, timestamp, volume_rank, momentum_rank, 
             total_pct_rank, zscore_rank, price_rank, overall_rank, total_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(query, rankings_list)
        self.commit()
        return True
    
    def get_latest(self, timeframe):
        """Get the latest rankings for all symbols at a specific timeframe."""
        query = """
            SELECT r.* FROM rankings r
            INNER JOIN (
                SELECT MAX(timestamp) as max_timestamp
                FROM rankings
                WHERE timeframe = ?
            ) r2 ON r.timestamp = r2.max_timestamp
            WHERE r.timeframe = ?
            ORDER BY r.overall_rank ASC
        """
        self.execute(query, (timeframe, timeframe))
        return self.fetchall()
    
    def get_top_n(self, timeframe, n=20):
        """Get the top N ranked symbols at a specific timeframe."""
        query = """
            SELECT r.* FROM rankings r
            INNER JOIN (
                SELECT MAX(timestamp) as max_timestamp
                FROM rankings
                WHERE timeframe = ?
            ) r2 ON r.timestamp = r2.max_timestamp
            WHERE r.timeframe = ?
            ORDER BY r.overall_rank ASC
            LIMIT ?
        """
        self.execute(query, (timeframe, timeframe, n))
        return self.fetchall()
    
    def get_history(self, symbol, timeframe, limit=100):
        """Get historical rankings for a symbol and timeframe."""
        query = """
            SELECT * FROM rankings 
            WHERE symbol = ? AND timeframe = ? 
            ORDER BY timestamp DESC LIMIT ?
        """
        self.execute(query, (symbol, timeframe, limit))
        return self.fetchall()


class Opportunity(BaseModel):
    """Model for detected trading opportunities."""
    
    table_name = 'opportunities'
    
    def create(self, symbol, timeframe, timestamp, direction, entry_price, volume_metric=None,
               momentum_metric=None, zscore_metric=None, price_metric=None, overall_rank=None,
               rank_change=None, opportunity_strength=None):
        """Create a new opportunity record."""
        query = """
            INSERT OR REPLACE INTO opportunities 
            (symbol, timeframe, timestamp, direction, entry_price, volume_metric, 
             momentum_metric, zscore_metric, price_metric, overall_rank, rank_change, opportunity_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            symbol, timeframe, timestamp, direction, entry_price, volume_metric,
            momentum_metric, zscore_metric, price_metric, overall_rank, rank_change, opportunity_strength
        ))
        self.commit()
        return True
    
    def bulk_insert(self, opportunities_list):
        """Insert multiple opportunity records at once."""
        query = """
            INSERT OR REPLACE INTO opportunities 
            (symbol, timeframe, timestamp, direction, entry_price, volume_metric, 
             momentum_metric, zscore_metric, price_metric, overall_rank, rank_change, opportunity_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(query, opportunities_list)
        self.commit()
        return True
    
    def get_latest(self, timeframe, direction=None, limit=20):
        """Get the latest opportunities for a specific timeframe and direction."""
        query = """
            SELECT o.* FROM opportunities o
            INNER JOIN (
                SELECT MAX(timestamp) as max_timestamp
                FROM opportunities
                WHERE timeframe = ?
            ) o2 ON o.timestamp = o2.max_timestamp
            WHERE o.timeframe = ?
        """
        params = [timeframe, timeframe]
        
        if direction:
            query += " AND o.direction = ?"
            params.append(direction)
        
        query += " ORDER BY o.opportunity_strength DESC LIMIT ?"
        params.append(limit)
        
        self.execute(query, params)
        return self.fetchall()
    
    def get_historical(self, timeframe, direction=None, limit=100):
        """Get historical opportunities for a specific timeframe and direction."""
        query = """
            SELECT * FROM opportunities 
            WHERE timeframe = ?
        """
        params = [timeframe]
        
        if direction:
            query += " AND direction = ?"
            params.append(direction)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        self.execute(query, params)
        return self.fetchall()


class BacktestResult(BaseModel):
    """Model for backtest results."""
    
    table_name = 'backtest_results'
    
    def create(self, backtest_id, symbol, timeframe, direction, entry_timestamp, entry_price,
               exit_timestamp=None, exit_price=None, exit_reason=None, pnl=None, bars_held=None):
        """Create a new backtest result record."""
        query = """
            INSERT OR REPLACE INTO backtest_results 
            (backtest_id, symbol, timeframe, direction, entry_timestamp, entry_price,
             exit_timestamp, exit_price, exit_reason, pnl, bars_held)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            backtest_id, symbol, timeframe, direction, entry_timestamp, entry_price,
            exit_timestamp, exit_price, exit_reason, pnl, bars_held
        ))
        self.commit()
        return True
    
    def bulk_insert(self, results_list):
        """Insert multiple backtest result records at once."""
        query = """
            INSERT OR REPLACE INTO backtest_results 
            (backtest_id, symbol, timeframe, direction, entry_timestamp, entry_price,
             exit_timestamp, exit_price, exit_reason, pnl, bars_held)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(query, results_list)
        self.commit()
        return True
    
    def get_by_backtest_id(self, backtest_id):
        """Get all results for a specific backtest ID."""
        query = """
            SELECT * FROM backtest_results 
            WHERE backtest_id = ? 
            ORDER BY entry_timestamp ASC
        """
        self.execute(query, (backtest_id,))
        return self.fetchall()
    
    def get_summary_stats(self, backtest_id, direction=None):
        """Calculate summary statistics for a backtest."""
        query = """
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losing_trades,
                AVG(pnl) as average_pnl,
                SUM(pnl) as total_pnl,
                MAX(pnl) as max_profit,
                MIN(pnl) as max_loss,
                AVG(bars_held) as avg_bars_held
            FROM backtest_results 
            WHERE backtest_id = ?
        """
        params = [backtest_id]
        
        if direction:
            query += " AND direction = ?"
            params.append(direction)
        
        self.execute(query, params)
        return self.fetchone()


class BacktestSummary(BaseModel):
    """Model for backtest summary statistics."""
    
    table_name = 'backtest_summary'
    
    def create(self, backtest_id, timeframe, direction, total_trades, winning_trades, losing_trades,
               win_rate, average_pnl, total_pnl, max_profit, max_loss, avg_bars_held,
               start_timestamp, end_timestamp):
        """Create a new backtest summary record."""
        query = """
            INSERT OR REPLACE INTO backtest_summary 
            (backtest_id, timeframe, direction, total_trades, winning_trades, losing_trades,
             win_rate, average_pnl, total_pnl, max_profit, max_loss, avg_bars_held,
             start_timestamp, end_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute(query, (
            backtest_id, timeframe, direction, total_trades, winning_trades, losing_trades,
            win_rate, average_pnl, total_pnl, max_profit, max_loss, avg_bars_held,
            start_timestamp, end_timestamp
        ))
        self.commit()
        return True
    
    def get_all_summaries(self):
        """Get all backtest summaries."""
        query = """
            SELECT * FROM backtest_summary 
            ORDER BY backtest_id, direction
        """
        self.execute(query)
        return self.fetchall()
    
    def get_by_backtest_id(self, backtest_id):
        """Get summary statistics for a specific backtest ID."""
        query = """
            SELECT * FROM backtest_summary 
            WHERE backtest_id = ? 
            ORDER BY direction
        """
        self.execute(query, (backtest_id,))
        return self.fetchall()
