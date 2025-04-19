# Binance Trading Analysis System

A modular Python application for technical analysis, trading opportunity detection, and backtesting on the Binance futures market.

## Key Features

- **Data Collection**: Fetch and store historical price data from Binance API
- **Technical Analysis**: Calculate various metrics and indicators (volume, momentum, Z-score, etc.)
- **Ranking System**: Rank trading pairs based on multiple metrics
- **Opportunity Detection**: Identify potential long/short trading opportunities
- **Real-time Updates**: Stream live market data via WebSocket
- **Backtesting**: Test trading strategies on historical data
- **Machine Learning Integration**: Export data to Orange3 for analysis
- **Web Interface**: Interactive UI for monitoring and analysis

## Directory Structure

```
binance_analysis/
├── app.py                  # Main entry point
├── config.py               # Configuration and settings
├── database/               # Database module
│   ├── models.py           # SQLite models
│   ├── schema.py           # Database schema
│   └── manager.py          # Database operations
├── api/                    # Data collection module
│   ├── binance_client.py   # Binance API client
│   └── websocket.py        # WebSocket client
├── analysis/               # Analysis module
│   ├── indicators.py       # Technical indicators
│   ├── metrics.py          # Metric calculations
│   ├── rankings.py         # Ranking algorithms
│   └── opportunities.py    # Opportunity detection
├── backtest/               # Backtesting module
│   ├── engine.py           # Backtesting engine
│   ├── recorder.py         # Historical data recorder
│   └── exporter.py         # Data export for Orange3
├── web/                    # Web interface
│   ├── server.py           # Flask web server
│   ├── routes.py           # API endpoints
│   ├── websocket.py        # WebSocket server
│   └── static/             # Static assets
├── utils/                  # Utility functions
└── requirements.txt        # Dependencies
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/binance-trading-analysis.git
   cd binance-trading-analysis
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure your Binance API credentials:
   - Set environment variables:
     ```bash
     export BINANCE_API_KEY="your_api_key"
     export BINANCE_API_SECRET="your_api_secret"
     ```
   - Or modify the `config.py` file directly.

## Usage

### Web Interface

The web interface provides an interactive dashboard to monitor metrics, rankings, and detected opportunities:

```bash
python app.py --mode web
```

Then open your browser and navigate to: http://localhost:5000

### Data Collection

To fetch and store historical data:

```bash
python app.py --mode data --timeframe 1h
```

### Analysis

To run analysis on existing data:

```bash
python app.py --mode analyze --timeframe 1h
```

### Backtesting

To run backtests on historical data:

```bash
python app.py --mode backtest --timeframe 1h
```

### Common Options

- `--timeframe`: Specify data timeframe (1m, 5m, 15m, 1h, 4h, 1d)
- `--symbols`: Comma-separated list of trading pairs
- `--db-path`: Custom database path
- `--api-key` and `--api-secret`: Binance API credentials
- `--use-testnet`: Use Binance testnet instead of production API
- `--export-format`: Format for exports (csv, xlsx, json, tab)

## Web Interface Features

- **Normal View**: Traditional table view with rankings and metrics
- **Slot Machine View**: Visualization to identify coinciding trends across metrics
- **Opportunity Detection**: Automated identification of trading opportunities
- **Backtesting**: Test strategies with adjustable parameters
- **Keyboard Shortcuts**: Quick access to common functions:
  - `r`: Refresh data
  - `t`: Change timeframe
  - `s`: Sort by different metrics
  - `m`: Toggle view mode
  - `w`: Toggle WebSocket
  - `o`: Toggle opportunity display
  - `b`: Run backtest

## Machine Learning Integration

The system can export data for analysis in Orange3:

1. Collect and analyze data
2. Export using the Orange3 exporter
3. Import the data into Orange3 for visualization and machine learning analysis

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes only. Do not use it to make financial decisions. Trading cryptocurrencies involves significant risk of loss. The author is not responsible for any financial losses incurred using this system.
