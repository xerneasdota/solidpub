"""
Routes for the Binance Trading Analysis System web application.
This module contains route definitions that complement those in server.py.
"""
import logging
import os
import json
from typing import Dict, Any, Optional
from datetime import datetime
from flask import request, jsonify, Blueprint

logger = logging.getLogger(__name__)

# Create blueprint for API routes
api = Blueprint('api', __name__, url_prefix='/api')

# Initialize blueprint with application state
app_state = None

def init_blueprint(state: Dict[str, Any]):
    """
    Initialize the blueprint with application state.
    
    Args:
        state: Application state dictionary
    """
    global app_state
    app_state = state
    return api

@api.route('/config', methods=['GET'])
def get_config():
    """Get application configuration."""
    # Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
        TIMEFRAMES, DEFAULT_TIMEFRAME, METRIC_CONFIG, 
        OPPORTUNITY_THRESHOLD, BACKTEST_CONFIG, SLOT_MACHINE_CONFIG
    )
    
    return jsonify({
        'timeframes': TIMEFRAMES,
        'default_timeframe': DEFAULT_TIMEFRAME,
        'metrics': METRIC_CONFIG,
        'opportunity_threshold': OPPORTUNITY_THRESHOLD,
        'backtest': BACKTEST_CONFIG,
        'slot_machine': SLOT_MACHINE_CONFIG
    })

@api.route('/config', methods=['POST'])
def update_config():
    """Update application configuration."""
    # Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import save_user_config
    
    try:
        data = request.json
        
        if not data or not isinstance(data, dict):
            return jsonify({'error': 'Invalid configuration data'}), 400
        
        # Save configuration
        success = save_user_config(data)
        
        if success:
            return jsonify({'status': 'success'})
        else:
            return jsonify({'error': 'Failed to save configuration'}), 500
    
    except Exception as e:
        logger.error(f"Error updating configuration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/fetch/historical', methods=['POST'])
def fetch_historical_data():
    """Fetch historical data from Binance API."""
    if not app_state or not app_state.get('api_client'):
        return jsonify({'error': 'API client not initialized'}), 500
    
    if not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        data = request.json
        
        symbols = data.get('symbols', [])
        timeframe = data.get('timeframe', '1h')
        limit = data.get('limit', 1000)
        
        # If no symbols specified, get all active pairs
        if not symbols:
            pairs = app_state['db_manager'].trading_pairs.get_active_pairs()
            symbols = [p['symbol'] for p in pairs]
        
        # Fetch exchange info first to update the trading pairs
        exchange_info = app_state['api_client'].get_exchange_info()
        
        # Save trading pairs
        if 'symbols' in exchange_info:
            app_state['db_manager'].save_trading_pairs(exchange_info['symbols'])
        
        # Fetch historical data for each symbol
        results = {}
        for symbol in symbols:
            try:
                # Fetch data from Binance
                klines = app_state['api_client'].get_historical_data(symbol, timeframe, limit)
                
                # Save to database
                app_state['db_manager'].save_price_data(symbol, timeframe, klines)
                
                results[symbol] = len(klines)
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                results[symbol] = f"Error: {str(e)}"
        
        return jsonify({
            'status': 'success',
            'results': results
        })
    
    except Exception as e:
        logger.error(f"Error fetching historical data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/analyze', methods=['POST'])
def analyze_data():
    """Analyze historical data and calculate metrics and rankings."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        data = request.json
        
        timeframe = data.get('timeframe', '1h')
        
        # Get historical data from database
        symbols = []
        if data.get('symbols'):
            symbols = data['symbols']
        else:
            # Get all active pairs
            pairs = app_state['db_manager'].trading_pairs.get_active_pairs()
            symbols = [p['symbol'] for p in pairs]
        
        # Retrieve historical data for each symbol
        historical_data = {}
        for symbol in symbols:
            price_data = app_state['db_manager'].get_price_history(symbol, timeframe)
            
            if price_data:
                # Convert SQLite Row objects to dictionaries if needed
                if hasattr(price_data, 'keys'):  
                    price_data = [dict(row) for row in price_data]
                historical_data[symbol] = price_data
        
        if not historical_data:
            return jsonify({'error': 'No historical data available'}), 400
        
        # Calculate metrics
        from ..analysis.metrics import calculate_all_metrics
        metrics = calculate_all_metrics(historical_data)
        
        # Calculate rankings
        from ..analysis.rankings import calculate_rankings
        rankings = calculate_rankings(metrics)
        
        # Save metrics and rankings to database
        app_state['db_manager'].save_metrics(timeframe, metrics)
        app_state['db_manager'].save_rankings(timeframe, rankings)
        
        # Detect opportunities
        from ..analysis.opportunities import OpportunityDetector
        detector = OpportunityDetector()
        
        # Get previous rankings for change calculation
        previous_rankings = None
        try:
            # Get the second most recent rankings
            prev_rankings_data = app_state['db_manager'].rankings.get_latest(timeframe)
            
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
        from ..analysis.rankings import calculate_ranking_changes
        ranking_changes = calculate_ranking_changes(rankings, previous_rankings)
        
        # Detect opportunities
        opportunities = detector.detect_opportunities(rankings, ranking_changes, metrics)
        
        # Save opportunities to database
        app_state['db_manager'].save_opportunities(timeframe, opportunities['current']['long'], 'long')
        app_state['db_manager'].save_opportunities(timeframe, opportunities['current']['short'], 'short')
        
        # Prepare response
        result = {
            'timeframe': timeframe,
            'metrics_count': len(metrics),
            'rankings_count': len(rankings),
            'long_opportunities': len(opportunities['current']['long']),
            'short_opportunities': len(opportunities['current']['short'])
        }
        
        # Notify connected clients if WebSocket is active
        if app_state.get('websocket_active') and hasattr(app_state, 'broadcast_update'):
            app_state['broadcast_update']('data_update', {
                'type': 'analysis',
                'timeframe': timeframe,
                'rankings': rankings,
                'opportunities': opportunities['current']
            })
        
        return jsonify({
            'status': 'success',
            'result': result
        })
    
    except Exception as e:
        logger.error(f"Error analyzing data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/backtest/quick', methods=['POST'])
def quick_backtest():
    """Run a quick backtest on a specific symbol and timeframe."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        data = request.json
        
        symbol = data.get('symbol')
        timeframe = data.get('timeframe', '1h')
        direction = data.get('direction', 'long')
        
        if not symbol:
            return jsonify({'error': 'Symbol is required'}), 400
        
        # Get historical data from database
        price_data = app_state['db_manager'].get_price_history(symbol, timeframe)
        
        if not price_data:
            return jsonify({'error': f'No historical data available for {symbol} at {timeframe}'}), 400
        
        # Convert SQLite Row objects to dictionaries if needed
        if hasattr(price_data, 'keys'):
            price_data = [dict(row) for row in price_data]
        
        # Use backtest engine or create a simple one if not available
        if app_state.get('backtest_engine'):
            # Initialize a new backtest engine for this quick test
            from ..backtest.engine import BacktestEngine
            engine = BacktestEngine(app_state['db_manager'])
            
            # Run backtest
            result = engine.run_backtest(
                {symbol: price_data},
                timeframe,
                take_profit=data.get('take_profit'),
                stop_loss=data.get('stop_loss'),
                max_bars=data.get('max_bars')
            )
            
            return jsonify({
                'status': 'success',
                'backtest_id': engine.backtest_id,
                'result': {
                    'summary': result['summary'][direction],
                    'trades': result[direction][:10]  # Limit to first 10 trades for quick view
                }
            })
        else:
            # Simple backtest without saving to database
            from ..backtest.engine import BacktestEngine
            engine = BacktestEngine()
            
            # Run backtest
            result = engine.run_backtest(
                {symbol: price_data},
                timeframe,
                take_profit=data.get('take_profit'),
                stop_loss=data.get('stop_loss'),
                max_bars=data.get('max_bars')
            )
            
            return jsonify({
                'status': 'success',
                'result': {
                    'summary': result['summary'][direction],
                    'trades': result[direction][:10]  # Limit to first 10 trades for quick view
                }
            })
    
    except Exception as e:
        logger.error(f"Error running quick backtest: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/backtest/list', methods=['GET'])
def list_backtests():
    """List all available backtests."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        # Get all backtest summaries
        summaries = app_state['db_manager'].backtest_summary.get_all_summaries()
        
        # Convert SQLite Row objects to dictionaries if needed
        if hasattr(summaries, 'keys'):
            summaries = [dict(row) for row in summaries]
        
        # Group by backtest_id
        grouped = {}
        for summary in summaries:
            backtest_id = summary['backtest_id']
            if backtest_id not in grouped:
                grouped[backtest_id] = {
                    'backtest_id': backtest_id,
                    'timeframe': summary['timeframe'],
                    'start_timestamp': summary['start_timestamp'],
                    'end_timestamp': summary['end_timestamp'],
                    'directions': []
                }
            
            grouped[backtest_id]['directions'].append({
                'direction': summary['direction'],
                'total_trades': summary['total_trades'],
                'win_rate': summary['win_rate'],
                'total_pnl': summary['total_pnl']
            })
        
        return jsonify({
            'status': 'success',
            'backtests': list(grouped.values())
        })
    
    except Exception as e:
        logger.error(f"Error listing backtests: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/data/record', methods=['POST'])
def record_historical_metrics():
    """Record historical metrics and rankings for machine learning."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        data = request.json
        
        timeframe = data.get('timeframe', '1h')
        step = data.get('step', 10)
        symbols = data.get('symbols', [])
        
        # If no symbols specified, get all active pairs
        if not symbols:
            pairs = app_state['db_manager'].trading_pairs.get_active_pairs()
            symbols = [p['symbol'] for p in pairs]
        
        # Get historical data from database
        historical_data = {}
        for symbol in symbols:
            price_data = app_state['db_manager'].get_price_history(symbol, timeframe)
            
            if price_data:
                # Convert SQLite Row objects to dictionaries if needed
                if hasattr(price_data, 'keys'):
                    price_data = [dict(row) for row in price_data]
                historical_data[symbol] = price_data
        
        if not historical_data:
            return jsonify({'error': 'No historical data available'}), 400
        
        # Record historical metrics and rankings
        from ..backtest.recorder import MetricsRecorder
        recorder = MetricsRecorder(app_state['db_manager'])
        
        # Start recording in a background thread
        import threading
        recorder_thread = threading.Thread(
            target=recorder.record_historical_metrics_and_rankings,
            args=(historical_data, timeframe),
            kwargs={'step': step}
        )
        recorder_thread.daemon = True
        recorder_thread.start()
        
        return jsonify({
            'status': 'success',
            'message': f'Recording started with {len(historical_data)} symbols, step size {step}'
        })
    
    except Exception as e:
        logger.error(f"Error recording historical metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/data/export/orange', methods=['POST'])
def export_orange_data():
    """Export data in Orange3-compatible format."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        data = request.json
        
        export_type = data.get('type', 'metrics')
        timeframe = data.get('timeframe', '1h')
        format = data.get('format', 'tab')  # Use Orange3's tab format by default
        
        from ..backtest.exporter import Orange3Exporter
        exporter = Orange3Exporter(app_state['db_manager'])
        
        if export_type == 'metrics':
            # Export metrics data
            file_path = exporter.export_metrics_rankings_data(
                timeframe, 
                data.get('start_time'),
                data.get('end_time'),
                format,
                data.get('filename_prefix')
            )
            
            if not file_path:
                return jsonify({'error': 'Export failed. Check logs for details.'}), 500
            
            return jsonify({
                'status': 'success',
                'file_path': os.path.basename(file_path)
            })
        
        elif export_type == 'backtest':
            # Export backtest results
            backtest_id = data.get('backtest_id')
            if not backtest_id:
                return jsonify({'error': 'Backtest ID is required'}), 400
            
            file_paths = exporter.export_backtest_results(
                backtest_id,
                format,
                data.get('separate_directions', True),
                data.get('filename_prefix')
            )
            
            if not file_paths:
                return jsonify({'error': 'Export failed. Check logs for details.'}), 500
            
            return jsonify({
                'status': 'success',
                'file_paths': {k: os.path.basename(v) for k, v in file_paths.items()}
            })
        
        elif export_type == 'symbol':
            # Export time series data for a specific symbol
            symbol = data.get('symbol')
            if not symbol:
                return jsonify({'error': 'Symbol is required'}), 400
            
            file_path = exporter.export_time_series_data(
                symbol,
                timeframe,
                data.get('metrics', ['all']),
                data.get('start_time'),
                data.get('end_time'),
                format,
                data.get('filename_prefix')
            )
            
            if not file_path:
                return jsonify({'error': 'Export failed. Check logs for details.'}), 500
            
            return jsonify({
                'status': 'success',
                'file_path': os.path.basename(file_path)
            })
        
        else:
            return jsonify({'error': f'Unsupported export type: {export_type}'}), 400
    
    except Exception as e:
        logger.error(f"Error exporting Orange data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@api.route('/slot-machine/<timeframe>', methods=['GET'])
def get_slot_machine_data(timeframe):
    """Get slot machine data for a specific timeframe."""
    if not app_state or not app_state.get('db_manager'):
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        algorithm = request.args.get('algorithm', 'consistent')
        max_rows = request.args.get('max_rows', 50, type=int)
        
        # Get current rankings from database
        rankings_data = app_state['db_manager'].rankings.get_latest(timeframe)
        
        # Convert to dictionary format
        rankings = {}
        if hasattr(rankings_data, 'keys'):
            rankings_data = [dict(row) for row in rankings_data]
        
        for row in rankings_data:
            symbol = row['symbol']
            rankings[symbol] = row
            
            # Convert in_uptrend from integer to boolean if needed
            if 'in_uptrend' in row and isinstance(row['in_uptrend'], int):
                rankings[symbol]['in_uptrend'] = bool(row['in_uptrend'])
        
        # Get previous rankings for change calculation
        previous_rankings = None
        try:
            # Get the second most recent rankings
            prev_rankings_data = app_state['db_manager'].rankings.get_latest(timeframe)
            
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
        from ..analysis.rankings import calculate_ranking_changes
        ranking_changes = calculate_ranking_changes(rankings, previous_rankings)
        
        # Prepare slot machine data
        from ..analysis.opportunities import prepare_slot_machine_data
        slot_machine_data = prepare_slot_machine_data(
            rankings, ranking_changes, algorithm, max_rows
        )
        
        return jsonify(slot_machine_data)
    
    except Exception as e:
        logger.error(f"Error getting slot machine data: {str(e)}")
        return jsonify({'error': str(e)}), 500
