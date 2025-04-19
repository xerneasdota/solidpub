"""
WebSocket handler for the Binance Trading Analysis System web application.
Handles real-time data streaming to the web UI.
"""
import logging
import json
import threading
import time
from typing import Dict, Any, Optional, List, Callable

# Fix relative import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.websocket import BinanceWebSocket, DataStreamProcessor
from ..analysis.metrics import calculate_all_metrics
from ..analysis.rankings import calculate_rankings, calculate_ranking_changes
from ..analysis.opportunities import OpportunityDetector

logger = logging.getLogger(__name__)

class WebUIWebSocketHandler:
    """WebSocket handler for streaming data to the web UI."""
    
    def __init__(
        self,
        broadcast_callback: Callable[[str, Dict[str, Any]], None],
        db_manager=None
    ):
        """
        Initialize the WebSocket handler.
        
        Args:
            broadcast_callback: Function to broadcast updates to web clients
            db_manager: Optional database manager for storing data
        """
        self.broadcast_callback = broadcast_callback
        self.db_manager = db_manager
        
        self.binance_ws = None
        self.data_processor = None
        self.running = False
        self.thread = None
        
        # Keep track of latest data
        self.latest_metrics = {}
        self.latest_rankings = {}
        self.ranking_history = []
        self.previous_rankings = None
        
        # Opportunity detector
        self.opportunity_detector = OpportunityDetector()
        
        # Configuration
        self.current_timeframe = '1m'
        self.current_symbols = []
        self.update_interval = 10  # seconds
    
    def start(
        self,
        symbols: List[str],
        timeframe: str,
        update_interval: int = 10,
        use_testnet: bool = False
    ) -> bool:
        """
        Start the WebSocket handler.
        
        Args:
            symbols: List of symbols to monitor
            timeframe: Timeframe to use
            update_interval: Interval between updates in seconds
            use_testnet: Whether to use the testnet
            
        Returns:
            True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("WebSocket handler is already running")
            return False
        
        # Store configuration
        self.current_symbols = symbols
        self.current_timeframe = timeframe
        self.update_interval = update_interval
        
        try:
            # Create data processor
            self.data_processor = DataStreamProcessor(
                db_manager=self.db_manager,
                on_kline_update=self._handle_kline_update,
                on_trade_update=self._handle_trade_update
            )
            
            # Create Binance WebSocket
            self.binance_ws = BinanceWebSocket(
                use_testnet=use_testnet,
                callback=self._handle_websocket_message
            )
            
            # Connect to WebSocket
            success = self.binance_ws.connect(symbols, [timeframe])
            
            if not success:
                logger.error("Failed to connect to Binance WebSocket")
                return False
            
            # Start update thread
            self.running = True
            self.thread = threading.Thread(target=self._update_loop)
            self.thread.daemon = True
            self.thread.start()
            
            logger.info(f"WebSocket handler started with {len(symbols)} symbols, timeframe {timeframe}")
            return True
        
        except Exception as e:
            logger.error(f"Error starting WebSocket handler: {str(e)}")
            self.stop()
            return False
    
    def stop(self) -> None:
        """Stop the WebSocket handler."""
        self.running = False
        
        if self.binance_ws:
            self.binance_ws.disconnect()
            self.binance_ws = None
        
        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None
        
        logger.info("WebSocket handler stopped")
    
    def _handle_websocket_message(self, message: Dict[str, Any]) -> None:
        """
        Handle incoming WebSocket messages.
        
        Args:
            message: WebSocket message
        """
        if self.data_processor:
            self.data_processor.process_message(message)
    
    def _handle_kline_update(self, kline_data: Dict[str, Any]) -> None:
        """
        Handle kline updates.
        
        Args:
            kline_data: Kline data
        """
        # Store kline data in database if available and kline is closed
        if self.db_manager and kline_data.get('is_closed'):
            try:
                self.db_manager.price_data.create(
                    symbol=kline_data['symbol'],
                    timeframe=kline_data['interval'],
                    timestamp=kline_data['timestamp'],
                    open_price=kline_data['open'],
                    high=kline_data['high'],
                    low=kline_data['low'],
                    close=kline_data['close'],
                    volume=kline_data['volume'],
                    quote_asset_volume=kline_data.get('quote_asset_volume'),
                    number_of_trades=kline_data.get('number_of_trades'),
                    taker_buy_base_asset_volume=kline_data.get('taker_buy_base_asset_volume'),
                    taker_buy_quote_asset_volume=kline_data.get('taker_buy_quote_asset_volume')
                )
            except Exception as e:
                logger.error(f"Error storing kline data: {str(e)}")
        
        # Broadcast update to web clients
        self.broadcast_callback('kline_update', kline_data)
    
    def _handle_trade_update(self, trade_data: Dict[str, Any]) -> None:
        """
        Handle trade updates.
        
        Args:
            trade_data: Trade data
        """
        # Broadcast update to web clients
        self.broadcast_callback('trade_update', trade_data)
    
    def _update_loop(self) -> None:
        """Background thread for periodic updates."""
        while self.running:
            try:
                # Calculate metrics and rankings
                self._calculate_and_broadcast_updates()
                
                # Sleep until next update
                for _ in range(self.update_interval):
                    if not self.running:
                        break
                    time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in update loop: {str(e)}")
                time.sleep(5)  # Sleep before retrying
    
    def _calculate_and_broadcast_updates(self) -> None:
        """Calculate metrics and rankings and broadcast updates."""
        try:
            # Check if we have data processor and klines
            if not self.data_processor or not self.data_processor.latest_klines:
                return
            
            # Build historical data from latest klines
            historical_data = {}
            
            for key, kline in self.data_processor.latest_klines.items():
                if '_' not in key:
                    continue
                
                symbol, interval = key.split('_', 1)
                
                if interval != self.current_timeframe:
                    continue
                
                # Build data array with this kline
                if symbol not in historical_data:
                    historical_data[symbol] = []
                
                historical_data[symbol].append(kline)
            
            if not historical_data:
                logger.warning("No kline data available for metrics calculation")
                return
            
            # Calculate metrics
            metrics = calculate_all_metrics(historical_data)
            self.latest_metrics = metrics
            
            # Calculate rankings
            rankings = calculate_rankings(metrics)
            
            # Store rankings history
            timestamp = int(time.time() * 1000)
            self.ranking_history.append({
                'timestamp': timestamp,
                'rankings': rankings
            })
            
            # Limit history size
            max_history_size = 10
            if len(self.ranking_history) > max_history_size:
                self.ranking_history.pop(0)
            
            # Calculate ranking changes
            ranking_changes = calculate_ranking_changes(rankings, self.previous_rankings)
            
            # Update previous rankings for next iteration
            self.previous_rankings = rankings
            self.latest_rankings = rankings
            
            # Add rankings to opportunity detector
            self.opportunity_detector.add_ranking_history(rankings)
            
            # Detect opportunities
            opportunities = self.opportunity_detector.detect_opportunities(
                rankings, ranking_changes, metrics
            )
            
            # Store in database if available
            if self.db_manager:
                try:
                    # Save metrics
                    self.db_manager.save_metrics(self.current_timeframe, metrics)
                    
                    # Save rankings
                    self.db_manager.save_rankings(self.current_timeframe, rankings)
                    
                    # Save opportunities
                    if opportunities['current']['long']:
                        self.db_manager.save_opportunities(
                            self.current_timeframe, 
                            opportunities['current']['long'], 
                            'long'
                        )
                    
                    if opportunities['current']['short']:
                        self.db_manager.save_opportunities(
                            self.current_timeframe, 
                            opportunities['current']['short'], 
                            'short'
                        )
                except Exception as e:
                    logger.error(f"Error saving to database: {str(e)}")
            
            # Broadcast updates
            self.broadcast_callback('analysis_update', {
                'timestamp': timestamp,
                'timeframe': self.current_timeframe,
                'metrics_count': len(metrics),
                'rankings': rankings,
                'opportunities': opportunities['current']
            })
            
            # Prepare and broadcast slot machine data
            from ..analysis.opportunities import prepare_slot_machine_data
            slot_machine_data = prepare_slot_machine_data(
                rankings, ranking_changes, 'consistent', 50
            )
            
            self.broadcast_callback('slot_machine_update', {
                'timestamp': timestamp,
                'timeframe': self.current_timeframe,
                'slot_machine': slot_machine_data
            })
        
        except Exception as e:
            logger.error(f"Error calculating and broadcasting updates: {str(e)}")
    
    def change_timeframe(self, timeframe: str) -> bool:
        """
        Change the timeframe.
        
        Args:
            timeframe: New timeframe
            
        Returns:
            True if changed successfully, False otherwise
        """
        if not self.running:
            logger.warning("WebSocket handler is not running")
            return False
        
        try:
            # Stop current websocket
            if self.binance_ws:
                self.binance_ws.disconnect()
            
            # Create new websocket with new timeframe
            self.current_timeframe = timeframe
            self.binance_ws = BinanceWebSocket(
                callback=self._handle_websocket_message
            )
            
            # Connect to WebSocket
            success = self.binance_ws.connect(self.current_symbols, [timeframe])
            
            if not success:
                logger.error("Failed to connect to Binance WebSocket with new timeframe")
                return False
            
            # Reset data
            self.latest_metrics = {}
            self.latest_rankings = {}
            self.ranking_history = []
            self.previous_rankings = None
            
            logger.info(f"Timeframe changed to {timeframe}")
            return True
        
        except Exception as e:
            logger.error(f"Error changing timeframe: {str(e)}")
            return False
    
    def get_latest_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get the latest metrics."""
        return self.latest_metrics
    
    def get_latest_rankings(self) -> Dict[str, Dict[str, Any]]:
        """Get the latest rankings."""
        return self.latest_rankings
