"""
Web server for the Binance Trading Analysis System.
"""
import logging
import os
import json
from typing import Dict, Any, Optional
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import WEB_CONFIG

logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__, static_folder='static')
CORS(app)

# Configure app
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'trading_analysis_secret')
app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB max upload size

# Create SocketIO instance
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Store application state
app_state = {
    'running': False,
    'websocket_active': False,
    'connected_clients': 0,
    'current_timeframe': None,
    'db_manager': None,
    'api_client': None,
    'data_processor': None,
    'backtest_engine': None
}

def init_app(
    db_manager,
    api_client,
    data_processor=None,
    backtest_engine=None
) -> None:
    """
    Initialize the application with necessary components.
    
    Args:
        db_manager: Database manager instance
        api_client: Binance API client instance
        data_processor: Optional data processor instance
        backtest_engine: Optional backtest engine instance
    """
    app_state['db_manager'] = db_manager
    app_state['api_client'] = api_client
    app_state['data_processor'] = data_processor
    app_state['backtest_engine'] = backtest_engine
    app_state['running'] = True
    
    logger.info("Web application initialized")

def shutdown_app() -> None:
    """Shutdown the application and clean up resources."""
    app_state['running'] = False
    
    # Close database connection
    if app_state['db_manager']:
        app_state['db_manager'].close()
    
    logger.info("Web application shut down")

# Default route
@app.route('/')
def index():
    """Render the main index.html page."""
    return render_template('index.html')

# Static files route
@app.route('/static/<path:path>')
def serve_static(path):
    """Serve static files."""
    return send_from_directory(app.static_folder, path)

# API status route
@app.route('/api/status')
def api_status():
    """Return API status."""
    return jsonify({
        'status': 'running' if app_state['running'] else 'stopped',
        'websocket_active': app_state['websocket_active'],
        'connected_clients': app_state['connected_clients'],
        'current_timeframe': app_state['current_timeframe'],
        'has_db': app_state['db_manager'] is not None,
        'has_api': app_state['api_client'] is not None
    })

# Get available trading pairs
@app.route('/api/pairs')
def get_trading_pairs():
    """Return available trading pairs."""
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        pairs = app_state['db_manager'].trading_pairs.get_active_pairs()
        return jsonify(pairs)
    except Exception as e:
        logger.error(f"Error getting trading pairs: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get price data for a symbol
@app.route('/api/price/<symbol>/<timeframe>')
def get_price_data(symbol, timeframe):
    """
    Return price data for a symbol.
    
    Args:
        symbol: Trading pair symbol
        timeframe: Timeframe string
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        limit = request.args.get('limit', 1000, type=int)
        data = app_state['db_manager'].get_price_history(symbol, timeframe, limit)
        
        # Ensure proper list format
        if isinstance(data, dict):
            data = [dict(row) for row in data.values()]
        elif hasattr(data, 'keys'):  # Handle SQLite Row objects
            data = [dict(row) for row in data]
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting price data: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get metrics for a symbol
@app.route('/api/metrics/<timeframe>')
def get_metrics(timeframe):
    """
    Return metrics for all symbols at a specific timeframe.
    
    Args:
        timeframe: Timeframe string
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        metrics = app_state['db_manager'].metrics.get_all_latest(timeframe)
        
        # Ensure proper format
        if hasattr(metrics, 'keys'):
            metrics = [dict(row) for row in metrics]
        
        return jsonify(metrics)
    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get rankings for a timeframe
@app.route('/api/rankings/<timeframe>')
def get_rankings(timeframe):
    """
    Return rankings for all symbols at a specific timeframe.
    
    Args:
        timeframe: Timeframe string
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        rankings = app_state['db_manager'].rankings.get_latest(timeframe)
        
        # Ensure proper format
        if hasattr(rankings, 'keys'):
            rankings = [dict(row) for row in rankings]
        
        return jsonify(rankings)
    except Exception as e:
        logger.error(f"Error getting rankings: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get opportunities
@app.route('/api/opportunities/<timeframe>')
def get_opportunities(timeframe):
    """
    Return trading opportunities for a specific timeframe.
    
    Args:
        timeframe: Timeframe string
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        direction = request.args.get('direction')
        
        if direction:
            opportunities = app_state['db_manager'].opportunities.get_latest(timeframe, direction)
        else:
            # Get both directions
            long_opps = app_state['db_manager'].opportunities.get_latest(timeframe, 'long')
            short_opps = app_state['db_manager'].opportunities.get_latest(timeframe, 'short')
            
            opportunities = {
                'long': [dict(row) for row in long_opps] if hasattr(long_opps, 'keys') else long_opps,
                'short': [dict(row) for row in short_opps] if hasattr(short_opps, 'keys') else short_opps
            }
        
        # Ensure proper format for single direction
        if direction and hasattr(opportunities, 'keys'):
            opportunities = [dict(row) for row in opportunities]
        
        return jsonify(opportunities)
    except Exception as e:
        logger.error(f"Error getting opportunities: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get historical opportunities
@app.route('/api/opportunities/historical/<timeframe>')
def get_historical_opportunities(timeframe):
    """
    Return historical trading opportunities for a specific timeframe.
    
    Args:
        timeframe: Timeframe string
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        direction = request.args.get('direction')
        limit = request.args.get('limit', 100, type=int)
        
        if direction:
            opportunities = app_state['db_manager'].opportunities.get_historical(timeframe, direction, limit)
        else:
            # Get both directions
            long_opps = app_state['db_manager'].opportunities.get_historical(timeframe, 'long', limit)
            short_opps = app_state['db_manager'].opportunities.get_historical(timeframe, 'short', limit)
            
            opportunities = {
                'long': [dict(row) for row in long_opps] if hasattr(long_opps, 'keys') else long_opps,
                'short': [dict(row) for row in short_opps] if hasattr(short_opps, 'keys') else short_opps
            }
        
        # Ensure proper format for single direction
        if direction and hasattr(opportunities, 'keys'):
            opportunities = [dict(row) for row in opportunities]
        
        return jsonify(opportunities)
    except Exception as e:
        logger.error(f"Error getting historical opportunities: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Run backtest
@app.route('/api/backtest', methods=['POST'])
def run_backtest():
    """Run a backtest with specified parameters."""
    if not app_state['backtest_engine']:
        return jsonify({'error': 'Backtest engine not initialized'}), 500
    
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        # Get parameters from request
        data = request.json
        timeframe = data.get('timeframe', '1h')
        symbols = data.get('symbols', [])
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        take_profit = data.get('take_profit')
        stop_loss = data.get('stop_loss')
        max_bars = data.get('max_bars')
        
        # Validate parameters
        if not symbols:
            # Use all symbols if none specified
            pairs = app_state['db_manager'].trading_pairs.get_active_pairs()
            symbols = [p['symbol'] for p in pairs]
        
        # Get historical data
        historical_data = {}
        for symbol in symbols:
            price_data = app_state['db_manager'].get_price_history(symbol, timeframe)
            if price_data:
                historical_data[symbol] = price_data
        
        if not historical_data:
            return jsonify({'error': 'No historical data available for selected symbols'}), 400
        
        # Run backtest
        result = app_state['backtest_engine'].run_backtest(
            historical_data,
            timeframe,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_bars=max_bars
        )
        
        # Format response
        response = {
            'backtest_id': app_state['backtest_engine'].backtest_id,
            'timeframe': timeframe,
            'summary': result['summary'],
            'long_count': len(result['long']),
            'short_count': len(result['short'])
        }
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error running backtest: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Get backtest results
@app.route('/api/backtest/<backtest_id>')
def get_backtest_results(backtest_id):
    """
    Return results for a specific backtest.
    
    Args:
        backtest_id: ID of the backtest
    """
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        results = app_state['db_manager'].backtest_results.get_by_backtest_id(backtest_id)
        summary = app_state['db_manager'].backtest_summary.get_by_backtest_id(backtest_id)
        
        # Ensure proper format
        if hasattr(results, 'keys'):
            results = [dict(row) for row in results]
        
        if hasattr(summary, 'keys'):
            summary = [dict(row) for row in summary]
        
        response = {
            'backtest_id': backtest_id,
            'results': results,
            'summary': summary
        }
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error getting backtest results: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Export data for Orange3
@app.route('/api/export', methods=['POST'])
def export_data():
    """Export data for Orange3 analysis."""
    if not app_state['db_manager']:
        return jsonify({'error': 'Database not initialized'}), 500
    
    try:
        from ..backtest.exporter import Orange3Exporter
        
        data = request.json
        export_type = data.get('type', 'metrics')
        format = data.get('format', 'csv')
        
        exporter = Orange3Exporter(app_state['db_manager'])
        
        if export_type == 'backtest':
            backtest_id = data.get('backtest_id')
            if not backtest_id:
                return jsonify({'error': 'Backtest ID is required for backtest export'}), 400
            
            result = exporter.export_backtest_results(backtest_id, format)
        elif export_type == 'metrics':
            timeframe = data.get('timeframe', '1h')
            start_time = data.get('start_time')
            end_time = data.get('end_time')
            
            result = exporter.export_metrics_rankings_data(timeframe, start_time, end_time, format)
        elif export_type == 'timeseries':
            symbol = data.get('symbol')
            timeframe = data.get('timeframe', '1h')
            metrics = data.get('metrics', ['all'])
            start_time = data.get('start_time')
            end_time = data.get('end_time')
            
            if not symbol:
                return jsonify({'error': 'Symbol is required for time series export'}), 400
            
            result = exporter.export_time_series_data(
                symbol, timeframe, metrics, start_time, end_time, format
            )
        else:
            return jsonify({'error': f'Unsupported export type: {export_type}'}), 400
        
        if not result:
            return jsonify({'error': 'Export failed, check logs for details'}), 500
        
        return jsonify({'status': 'success', 'result': result})
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        return jsonify({'error': str(e)}), 500

# WebSocket connection event
@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    app_state['connected_clients'] += 1
    logger.info(f"Client connected. Total: {app_state['connected_clients']}")
    
    # Send initial state to the client
    socketio.emit('status', {
        'status': 'running' if app_state['running'] else 'stopped',
        'websocket_active': app_state['websocket_active'],
        'current_timeframe': app_state['current_timeframe']
    })

# WebSocket disconnection event
@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    app_state['connected_clients'] = max(0, app_state['connected_clients'] - 1)
    logger.info(f"Client disconnected. Total: {app_state['connected_clients']}")

# Request for initial data
@socketio.on('request_data')
def handle_request_data(data):
    """
    Handle client request for initial data.
    
    Args:
        data: Dictionary with request parameters
    """
    try:
        timeframe = data.get('timeframe', app_state['current_timeframe'] or '1h')
        app_state['current_timeframe'] = timeframe
        
        # Send response with requested data
        socketio.emit('initial_data', {
            'timeframe': timeframe,
            'status': 'loading'
        })
        
        # If we have a database, fetch the data
        if app_state['db_manager']:
            # Get rankings
            rankings = app_state['db_manager'].rankings.get_latest(timeframe)
            
            # Get opportunities
            long_opps = app_state['db_manager'].opportunities.get_latest(timeframe, 'long')
            short_opps = app_state['db_manager'].opportunities.get_latest(timeframe, 'short')
            
            # Format response
            response = {
                'timeframe': timeframe,
                'status': 'success',
                'rankings': [dict(row) for row in rankings] if hasattr(rankings, 'keys') else rankings,
                'opportunities': {
                    'long': [dict(row) for row in long_opps] if hasattr(long_opps, 'keys') else long_opps,
                    'short': [dict(row) for row in short_opps] if hasattr(short_opps, 'keys') else short_opps
                }
            }
            
            socketio.emit('initial_data', response)
        else:
            socketio.emit('initial_data', {
                'timeframe': timeframe,
                'status': 'error',
                'message': 'Database not initialized'
            })
    except Exception as e:
        logger.error(f"Error handling data request: {str(e)}")
        socketio.emit('initial_data', {
            'timeframe': app_state['current_timeframe'] or '1h',
            'status': 'error',
            'message': str(e)
        })

# Change timeframe
@socketio.on('change_timeframe')
def handle_change_timeframe(data):
    """
    Handle client request to change timeframe.
    
    Args:
        data: Dictionary with timeframe
    """
    try:
        timeframe = data.get('timeframe', '1h')
        app_state['current_timeframe'] = timeframe
        
        # Emit status update
        socketio.emit('status', {
            'status': 'running' if app_state['running'] else 'stopped',
            'websocket_active': app_state['websocket_active'],
            'current_timeframe': timeframe
        })
        
        # Also request updated data
        handle_request_data({'timeframe': timeframe})
    except Exception as e:
        logger.error(f"Error changing timeframe: {str(e)}")

# Toggle WebSocket
@socketio.on('toggle_websocket')
def handle_toggle_websocket(data):
    """
    Handle client request to toggle WebSocket.
    
    Args:
        data: Dictionary with toggle state
    """
    try:
        active = data.get('active', not app_state['websocket_active'])
        app_state['websocket_active'] = active
        
        # Emit status update
        socketio.emit('status', {
            'status': 'running' if app_state['running'] else 'stopped',
            'websocket_active': active,
            'current_timeframe': app_state['current_timeframe']
        })
        
        # TODO: Actually start/stop the WebSocket client
    except Exception as e:
        logger.error(f"Error toggling WebSocket: {str(e)}")

def start_server():
    """Start the web server."""
    host = WEB_CONFIG['HOST']
    port = WEB_CONFIG['PORT']
    debug = WEB_CONFIG['DEBUG']
    
    logger.info(f"Starting web server on {host}:{port} (debug: {debug})")
    
    try:
        socketio.run(app, host=host, port=port, debug=debug)
    except Exception as e:
        logger.error(f"Error starting web server: {str(e)}")
    
    return app, socketio

# Broadcast updates to all clients
def broadcast_update(event, data):
    """
    Broadcast an update to all connected clients.
    
    Args:
        event: Event name
        data: Event data
    """
    try:
        socketio.emit(event, data)
    except Exception as e:
        logger.error(f"Error broadcasting update: {str(e)}")

# Handle data updates (called from WebSocket client)
def handle_data_update(data):
    """
    Handle data updates from WebSocket client.
    
    Args:
        data: Updated data
    """
    try:
        # Broadcast the update to all clients
        broadcast_update('data_update', data)
    except Exception as e:
        logger.error(f"Error handling data update: {str(e)}")

if __name__ == '__main__':
    # Start the server if run directly
    start_server()
