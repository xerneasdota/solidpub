"""
Database schema for the Binance Trading Analysis System.
Defines the SQLite tables and relationships.
"""

import sqlite3
import os
import logging
from pathlib import Path

# Fix import - use absolute import instead of relative
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE

logger = logging.getLogger(__name__)

# SQL statements for table creation
TABLES = {
    # Table to store trading pairs
    "trading_pairs": """
        CREATE TABLE IF NOT EXISTS trading_pairs (
            symbol TEXT PRIMARY KEY,
            base_asset TEXT NOT NULL,
            quote_asset TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    # Table to store price data (OHLCV)
    "price_data": """
        CREATE TABLE IF NOT EXISTS price_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            quote_asset_volume REAL,
            number_of_trades INTEGER,
            taker_buy_base_asset_volume REAL,
            taker_buy_quote_asset_volume REAL,
            UNIQUE(symbol, timeframe, timestamp)
        )
    """,
    # Table to store calculated metrics
    "metrics": """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            volume_metric REAL,
            momentum_metric REAL,
            total_pct_change REAL,
            zscore_metric REAL,
            price_metric REAL,
            in_uptrend INTEGER,
            UNIQUE(symbol, timeframe, timestamp)
        )
    """,
    # Table to store rankings
    "rankings": """
        CREATE TABLE IF NOT EXISTS rankings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            volume_rank INTEGER,
            momentum_rank INTEGER,
            total_pct_rank INTEGER,
            zscore_rank INTEGER,
            price_rank INTEGER,
            overall_rank INTEGER,
            total_score REAL,
            UNIQUE(symbol, timeframe, timestamp)
        )
    """,
    # Table to store detected opportunities
    "opportunities": """
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            volume_metric REAL,
            momentum_metric REAL,
            zscore_metric REAL,
            price_metric REAL,
            overall_rank INTEGER,
            rank_change INTEGER,
            opportunity_strength REAL,
            UNIQUE(symbol, timeframe, timestamp, direction)
        )
    """,
    # Table to store backtest results
    "backtest_results": """
        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backtest_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_timestamp INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            exit_timestamp INTEGER,
            exit_price REAL,
            exit_reason TEXT,
            pnl REAL,
            bars_held INTEGER,
            UNIQUE(backtest_id, symbol, entry_timestamp, direction)
        )
    """,
    # Table to store backtest summary statistics
    "backtest_summary": """
        CREATE TABLE IF NOT EXISTS backtest_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backtest_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            total_trades INTEGER,
            winning_trades INTEGER,
            losing_trades INTEGER,
            win_rate REAL,
            average_pnl REAL,
            total_pnl REAL,
            max_profit REAL,
            max_loss REAL,
            avg_bars_held REAL,
            start_timestamp INTEGER,
            end_timestamp INTEGER,
            UNIQUE(backtest_id, timeframe, direction)
        )
    """,
}

# Indexes for performance
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_price_data_symbol ON price_data(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_price_data_timeframe ON price_data(timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_price_data_timestamp ON price_data(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_symbol ON metrics(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_timeframe ON metrics(timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_rankings_symbol ON rankings(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_rankings_timeframe ON rankings(timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_rankings_timestamp ON rankings(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_rankings_overall ON rankings(overall_rank)",
    "CREATE INDEX IF NOT EXISTS idx_opportunities_symbol ON opportunities(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_opportunities_timestamp ON opportunities(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_opportunities_direction ON opportunities(direction)",
    "CREATE INDEX IF NOT EXISTS idx_backtest_results_backtest_id ON backtest_results(backtest_id)",
    "CREATE INDEX IF NOT EXISTS idx_backtest_results_symbol ON backtest_results(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_backtest_summary_backtest_id ON backtest_summary(backtest_id)",
]


def init_db(db_path=None):
    """Initialize the database with required tables and indexes."""
    if db_path is None:
        db_path = DATABASE["name"]

    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create tables
        for table_name, create_statement in TABLES.items():
            cursor.execute(create_statement)
            logger.info(f"Created table: {table_name}")

        # Create indexes
        for index_statement in INDEXES:
            cursor.execute(index_statement)

        conn.commit()
        logger.info(f"Database initialized successfully at {db_path}")
        return True
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        return False
    finally:
        if conn:
            conn.close()


def backup_db(source_path=None, backup_name=None):
    """Create a backup of the database."""
    if source_path is None:
        source_path = DATABASE["name"]

    if backup_name is None:
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"trading_backup_{timestamp}.db"

    backup_path = os.path.join(DATABASE["backup_dir"], backup_name)

    try:
        # Ensure backup directory exists
        os.makedirs(DATABASE["backup_dir"], exist_ok=True)

        # Connect to the source database
        source_conn = sqlite3.connect(source_path)

        # Create a backup connection
        backup_conn = sqlite3.connect(backup_path)

        # Copy the database
        source_conn.backup(backup_conn)

        # Close connections
        source_conn.close()
        backup_conn.close()

        logger.info(f"Database backed up successfully to {backup_path}")
        return True
    except sqlite3.Error as e:
        logger.error(f"Database backup error: {e}")
        return False


def reset_db(db_path=None, confirm=False):
    """Reset the database by dropping all tables."""
    if not confirm:
        logger.warning("Database reset not confirmed. Set confirm=True to proceed.")
        return False

    if db_path is None:
        db_path = DATABASE["name"]

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Drop all tables
        for table in tables:
            if table[0] != "sqlite_sequence":  # Skip the sqlite_sequence table
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")

        conn.commit()
        logger.info(f"Database reset successfully at {db_path}")

        # Reinitialize the database
        init_db(db_path)

        return True
    except sqlite3.Error as e:
        logger.error(f"Database reset error: {e}")
        return False
    finally:
        if conn:
            conn.close()
