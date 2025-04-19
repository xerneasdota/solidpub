#!/usr/bin/env python3
"""
Binance Trading Analysis System - Main Application

This is the main entry point for the Binance Trading Analysis System.
It provides a command-line interface to run various components of the system.
"""
import os
import sys
import time
import logging
import argparse
import signal
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trading_analysis.log')
    ]
)
logger = logging.getLogger(__name__)

# Create argument parser
parser = argparse.ArgumentParser(description='Binance Trading Analysis System')

# Add arguments
parser.add_argument(
    '--mode',
    type=str,
    choices=['web', 'backtest', 'data', 'analyze'],
    default='web',
    help='Operating mode (default: web)'
)

parser.add_argument(
    '--timeframe',
    type=str,
    default='1h',
    help='Timeframe to use (default: 1h)'
)

parser.add_argument(
    '--symbols',
    type=str,
    help='Comma-separated list of symbols to use (default: top 20 by volume)'
)

parser.add_argument(
    '--db-path',
    type=str,
    help='Path to the SQLite database (default: data/trading.db)'
)

parser.add_argument(
    '--api-key',
    type=str,
    help='Binance API key (default: from config or environment)'
)

parser.add_argument(
    '--api-secret',
    type=str,
    help='Binance API secret (default: from config or environment)'
)

parser.add_argument(
    '--use-testnet',
    action='store_true',
    help='Use Binance testnet instead of production API'
)

parser.add_argument(
    '--web-host',
    type=str,
    default='0.0.0.0',
    help='Host to bind the web server to (default: 0.0.0.0)'
)

parser.add_argument(
    '--web-port',
    type=int,
    default=5000,
    help='Port to bind the web server to (default: 5000)'
)

parser.add_argument(
    '--debug',
    action='store_true',
    help='Enable debug mode'
)

parser.add_argument(
    '--no-websocket',
    action='store_true',
    help='Disable WebSocket for live data'
)

parser.add_argument(
    '--backtest-id',
    type=str,
    help='Backtest ID to use (for backtest mode)'
)

parser.add_argument(
    '--export-format',
    type=str,
    choices=['csv', 'xlsx', 'json', 'tab'],
    default='csv',
    help='Export format (default: csv)'
)

parser.add_argument(
    '--export-path',
    type=str,
    help='Path to export data to (default: data/exports)'
)

args = parser.parse_args()

# Apply command-line arguments to configuration
if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled")

# Setup signal handling for graceful shutdown
running = True
def signal_handler(signum, frame):
    global running
    logger.info(f"Received signal {signum}, shutting down...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main function to run the application."""
    logger.info("Starting Binance Trading Analysis System")
    
    try:
        # Initialize configuration
        from config import (
            API_CONFIG, DATABASE, TIMEFRAMES, DEFAULT_TIMEFRAME,
            WEB_CONFIG, EXPORT_CONFIG
        )
        
        # Override configuration with command-line arguments
        if args.db_path:
            DATABASE['name'] = args.db_path
        
        if args.api_key:
            API_CONFIG['KEY'] = args.api_key
        
        if args.api_secret:
            API_CONFIG['SECRET'] = args.api_secret
        
        if args.use_testnet:
            API_CONFIG['USE_TESTNET'] = True
        
        if args.web_host:
            WEB_CONFIG['HOST'] = args.web_host
        
        if args.web_port:
            WEB_CONFIG['PORT'] = args.web_port
        
        if args.debug:
            WEB_CONFIG['DEBUG'] = True
        
        if args.export_path:
            EXPORT_CONFIG['export_dir'] = args.export_path
        
        # Ensure necessary directories exist
        os.makedirs(os.path.dirname(DATABASE['name']), exist_ok=True)
        os.makedirs(DATABASE['backup_dir'], exist_ok=True)
        os.makedirs(EXPORT_CONFIG['export_dir'], exist_ok=True)
        
        # Initialize database
        logger.info(f"Initializing database at {DATABASE['name']}")
        from database.schema import init_db
        init_db(DATABASE['name'])
        
        # Create database manager
        from database.manager import DatabaseManager
        db_manager = DatabaseManager(DATABASE['name'])
        
        # Create API client
        logger.info("Initializing Binance API client")
        from api.binance_client import BinanceClient
        api_client = BinanceClient(
            api_key=API_CONFIG['KEY'],
            api_secret=API_CONFIG['SECRET'],
            use_testnet=API_CONFIG['USE_TESTNET']
        )
        
        # Process symbols argument
        symbols = []
        if args.symbols:
            symbols = args.symbols.split(',')
        
        # Get all symbols if none provided
        if not symbols:
            try:
                logger.info("Fetching available trading pairs")
                pairs = api_client.get_trading_pairs()
                db_manager.save_trading_pairs(pairs)
                
                # Use top symbols by volume
                symbols = [p['symbol'] for p in pairs[:20]]
                logger.info(f"Using top {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"Error fetching trading pairs: {str(e)}")
                # Try to get symbols from database
                pairs = db_manager.trading_pairs.get_active_pairs()
                if pairs:
                    symbols = [p['symbol'] for p in pairs[:20]]
                    logger.info(f"Using {len(symbols)} symbols from database")
                else:
                    logger.error("No symbols available, using defaults")
                    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        
        # Get timeframe
        timeframe = args.timeframe if args.timeframe in TIMEFRAMES else DEFAULT_TIMEFRAME
        
        # Run appropriate mode
        if args.mode == 'web':
            run_web_mode(db_manager, api_client, symbols, timeframe)
        elif args.mode == 'backtest':
            run_backtest_mode(db_manager, symbols, timeframe, args.backtest_id)
        elif args.mode == 'data':
            run_data_mode(db_manager, api_client, symbols, timeframe)
        elif args.mode == 'analyze':
            run_analysis_mode(db_manager, symbols, timeframe)
        else:
            logger.error(f"Invalid mode: {args.mode}")
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.exception(f"Unhandled error: {str(e)}")
    finally:
        logger.info("Shutting down...")
        # Clean up resources
        try:
            if 'db_manager' in locals():
                db_manager.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        
        logger.info("Binance Trading Analysis System shut down")

def run_web_mode(db_manager, api_client, symbols, timeframe):
    """
    Run the web application mode.
    
    Args:
        db_manager: Database manager instance
        api_client: Binance API client instance
        symbols: List of symbols to use
        timeframe: Timeframe to use
    """
    logger.info(f"Starting web mode with {len(symbols)} symbols, timeframe {timeframe}")
    
    try:
        from web.server import init_app, start_server, socketio, app, app_state, broadcast_update
        
        # Create data processor and WebSocket handler if enabled
        data_processor = None
        websocket_handler = None
        
        if not args.no_websocket:
            logger.info("Initializing WebSocket handler")
            from web.websocket import WebUIWebSocketHandler
            
            # Create WebSocket handler
            websocket_handler = WebUIWebSocketHandler(
                broadcast_callback=broadcast_update,
                db_manager=db_manager
            )
            
            # Start WebSocket handler
            websocket_handler.start(
                symbols=symbols,
                timeframe=timeframe,
                update_interval=10,
                use_testnet=API_CONFIG['USE_TESTNET']
            )
            
            # Store in app state
            app_state['websocket_active'] = True
            app_state['data_processor'] = websocket_handler.data_processor
        
        # Create backtest engine
        logger.info("Initializing backtest engine")
        from backtest.engine import BacktestEngine
        backtest_engine = BacktestEngine(db_manager)
        
        # Initialize web application
        init_app(
            db_manager=db_manager,
            api_client=api_client,
            data_processor=data_processor,
            backtest_engine=backtest_engine
        )
        
        # Make broadcast_update available to app_state
        app_state['broadcast_update'] = broadcast_update
        
        # Set current timeframe
        app_state['current_timeframe'] = timeframe
        
        # Start the web server
        start_server()
        
        # Wait for shutdown signal
        while running:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.exception(f"Error in web mode: {str(e)}")
    finally:
        # Clean up WebSocket handler
        if 'websocket_handler' in locals() and websocket_handler:
            websocket_handler.stop()

def run_backtest_mode(db_manager, symbols, timeframe, backtest_id=None):
    """
    Run the backtest mode.
    
    Args:
        db_manager: Database manager instance
        symbols: List of symbols to use
        timeframe: Timeframe to use
        backtest_id: Optional backtest ID to use
    """
    logger.info(f"Starting backtest mode with {len(symbols)} symbols, timeframe {timeframe}")
    
    try:
        # Create backtest engine
        from backtest.engine import BacktestEngine
        backtest_engine = BacktestEngine(db_manager)
        
        if backtest_id:
            # Load and display existing backtest
            logger.info(f"Loading backtest {backtest_id}")
            results = db_manager.backtest_results.get_by_backtest_id(backtest_id)
            summary = db_manager.backtest_summary.get_by_backtest_id(backtest_id)
            
            print("\n========== Backtest Summary ==========")
            for row in summary:
                direction = row['direction']
                print(f"\n--- {direction.upper()} ---")
                print(f"Total Trades: {row['total_trades']}")
                print(f"Win Rate: {row['win_rate']:.2f}%")
                print(f"Total PnL: {row['total_pnl']:.2f}%")
                print(f"Average PnL: {row['average_pnl']:.2f}%")
                print(f"Max Profit: {row['max_profit']:.2f}%")
                print(f"Max Loss: {row['max_loss']:.2f}%")
                print(f"Average Bars Held: {row['avg_bars_held']:.1f}")
            
            print("\n\nExport backtest results for Orange3? (y/n)")
            choice = input().lower()
            
            if choice == 'y':
                from backtest.exporter import Orange3Exporter
                exporter = Orange3Exporter(db_manager)
                
                print("Export format: (1) CSV, (2) XLSX, (3) JSON, (4) TAB (Orange)")
                format_choice = input()
                
                if format_choice == '1':
                    export_format = 'csv'
                elif format_choice == '2':
                    export_format = 'xlsx'
                elif format_choice == '3':
                    export_format = 'json'
                elif format_choice == '4':
                    export_format = 'tab'
                else:
                    export_format = 'csv'
                
                file_paths = exporter.export_backtest_results(
                    backtest_id,
                    export_format
                )
                
                print(f"Results exported to: {file_paths}")
        else:
            # Run new backtest
            logger.info("Running new backtest")
            
            # Get historical data from database
            historical_data = {}
            for symbol in symbols:
                price_data = db_manager.get_price_history(symbol, timeframe)
                if price_data:
                    historical_data[symbol] = price_data
            
            if not historical_data:
                logger.error("No historical data available")
                return
            
            # Get backtest parameters
            from config import BACKTEST_CONFIG
            take_profit = BACKTEST_CONFIG['TAKE_PROFIT']
            stop_loss = BACKTEST_CONFIG['STOP_LOSS']
            max_bars = BACKTEST_CONFIG['MAX_BARS']
            
            # Run backtest
            result = backtest_engine.run_backtest(
                historical_data,
                timeframe,
                take_profit=take_profit,
                stop_loss=stop_loss,
                max_bars=max_bars
            )
            
            # Print summary
            print("\n========== Backtest Summary ==========")
            print(f"Backtest ID: {backtest_engine.backtest_id}")
            print(f"Timeframe: {timeframe}")
            
            for direction, summary in result['summary'].items():
                print(f"\n--- {direction.upper()} ---")
                print(f"Total Trades: {summary['totalTrades']}")
                print(f"Win Rate: {summary['winRate']:.2f}%")
                print(f"Total PnL: {summary['totalPnl']:.2f}%")
                print(f"Average PnL: {summary['averagePnl']:.2f}%")
                print(f"Max Profit: {summary['maxProfit']:.2f}%")
                print(f"Max Loss: {summary['maxLoss']:.2f}%")
                print(f"Average Bars Held: {summary['avgBarsHeld']:.1f}")
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.exception(f"Error in backtest mode: {str(e)}")

def run_data_mode(db_manager, api_client, symbols, timeframe):
    """
    Run the data collection mode.
    
    Args:
        db_manager: Database manager instance
        api_client: Binance API client instance
        symbols: List of symbols to use
        timeframe: Timeframe to use
    """
    logger.info(f"Starting data mode with {len(symbols)} symbols, timeframe {timeframe}")
    
    try:
        # Fetch exchange info first to update the trading pairs
        logger.info("Fetching exchange info")
        exchange_info = api_client.get_exchange_info()
        
        # Save trading pairs
        if 'symbols' in exchange_info:
            db_manager.save_trading_pairs(exchange_info['symbols'])
            logger.info(f"Saved {len(exchange_info['symbols'])} trading pairs")
        
        # Fetch historical data for each symbol
        logger.info(f"Fetching historical data for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"Fetching data for {symbol} ({i+1}/{len(symbols)})")
                
                # Fetch data from Binance
                klines = api_client.get_historical_data(symbol, timeframe)
                
                # Save to database
                db_manager.save_price_data(symbol, timeframe, klines)
                
                logger.info(f"Saved {len(klines)} klines for {symbol}")
                
                # Sleep to avoid rate limits
                if i < len(symbols) - 1:
                    time.sleep(0.5)
            
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        logger.info("Data collection complete")
        
        # Ask if user wants to record historical metrics
        print("\nRecord historical metrics and rankings? (y/n)")
        choice = input().lower()
        
        if choice == 'y':
            from backtest.recorder import MetricsRecorder
            recorder = MetricsRecorder(db_manager)
            
            # Get historical data from database
            historical_data = {}
            for symbol in symbols:
                price_data = db_manager.get_price_history(symbol, timeframe)
                if price_data:
                    historical_data[symbol] = price_data
            
            # Record historical metrics and rankings
            print("Enter step size (e.g., 10 = record every 10th bar):")
            step = int(input() or "10")
            
            recorder.record_historical_metrics_and_rankings(
                historical_data, timeframe, step=step
            )
            
            logger.info("Historical metrics recording complete")
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.exception(f"Error in data mode: {str(e)}")

def run_analysis_mode(db_manager, symbols, timeframe):
    """
    Run the analysis mode.
    
    Args:
        db_manager: Database manager instance
        symbols: List of symbols to use
        timeframe: Timeframe to use
    """
    logger.info(f"Starting analysis mode with {len(symbols)} symbols, timeframe {timeframe}")
    
    try:
        # Get historical data from database
        historical_data = {}
        for symbol in symbols:
            price_data = db_manager.get_price_history(symbol, timeframe)
            if price_data:
                historical_data[symbol] = price_data
        
        if not historical_data:
            logger.error("No historical data available")
            return
        
        # Calculate metrics
        logger.info("Calculating metrics")
        from analysis.metrics import calculate_all_metrics
        metrics = calculate_all_metrics(historical_data)
        
        # Calculate rankings
        logger.info("Calculating rankings")
        from analysis.rankings import calculate_rankings
        rankings = calculate_rankings(metrics)
        
        # Get previous rankings for change calculation
        previous_rankings = None
        try:
            # Get the second most recent rankings
            prev_rankings_data = db_manager.rankings.get_latest(timeframe)
            
            # Group by timestamp and get the second most recent
            timestamps = {}
            for row in prev_rankings_data:
                ts = row['timestamp']
                if ts not in timestamps:
                    timestamps[ts] = []
                timestamps[ts].append(dict(row))
            
            # Sort timestamps in descending order and get the second one if available
            sorted_ts = sorted(timestamps.keys(), reverse=True)
            if len(sorted_ts) > 1:
                prev_rankings_data = timestamps[sorted_ts[1]]
                previous_rankings = {row['symbol']: row for row in prev_rankings_data}
        except Exception as e:
            logger.warning(f"Error getting previous rankings: {str(e)}")
        
        # Calculate ranking changes
        logger.info("Calculating ranking changes")
        from analysis.rankings import calculate_ranking_changes
        ranking_changes = calculate_ranking_changes(rankings, previous_rankings)
        
        # Detect opportunities
        logger.info("Detecting opportunities")
        from analysis.opportunities import OpportunityDetector
        detector = OpportunityDetector()
        opportunities = detector.detect_opportunities(rankings, ranking_changes, metrics)
        
        # Save metrics and rankings to database
        db_manager.save_metrics(timeframe, metrics)
        db_manager.save_rankings(timeframe, rankings)
        
        # Save opportunities to database
        db_manager.save_opportunities(timeframe, opportunities['current']['long'], 'long')
        db_manager.save_opportunities(timeframe, opportunities['current']['short'], 'short')
        
        logger.info("Analysis complete")
        
        # Print top opportunities
        print("\n========== Long Opportunities ==========")
        for opp in opportunities['current']['long'][:10]:
            print(f"{opp['symbol']} - Strength: {opp['opportunity_strength']:.2f}% - Rank: {opp['overall_rank']}")
        
        print("\n========== Short Opportunities ==========")
        for opp in opportunities['current']['short'][:10]:
            print(f"{opp['symbol']} - Strength: {opp['opportunity_strength']:.2f}% - Rank: {opp['overall_rank']}")
        
        # Generate slot machine view
        print("\nGenerate Slot Machine View? (y/n)")
        choice = input().lower()
        
        if choice == 'y':
            logger.info("Generating slot machine view")
            from analysis.opportunities import prepare_slot_machine_data
            slot_machine_data = prepare_slot_machine_data(
                rankings, ranking_changes, 'consistent', 20
            )
            
            print("\n========== Slot Machine Matches ==========")
            for match in slot_machine_data['matches'][:10]:
                print(f"{match['symbol']} - Rank: {match['rank']} - Match Count: {match['matchCount']}")
        
        # Export data for Orange3
        print("\nExport data for Orange3? (y/n)")
        choice = input().lower()
        
        if choice == 'y':
            from backtest.exporter import Orange3Exporter
            exporter = Orange3Exporter(db_manager)
            
            print("Export format: (1) CSV, (2) XLSX, (3) JSON, (4) TAB (Orange)")
            format_choice = input()
            
            if format_choice == '1':
                export_format = 'csv'
            elif format_choice == '2':
                export_format = 'xlsx'
            elif format_choice == '3':
                export_format = 'json'
            elif format_choice == '4':
                export_format = 'tab'
            else:
                export_format = 'csv'
            
            file_paths = exporter.export_metrics_rankings_data(
                timeframe,
                export_format=export_format
            )
            
            print(f"Data exported to: {file_paths}")
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.exception(f"Error in analysis mode: {str(e)}")

if __name__ == "__main__":
    main()
