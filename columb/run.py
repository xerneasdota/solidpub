#!/usr/bin/env python3
"""
Wrapper script to run the Binance Trading Analysis System
This script handles the import paths correctly
"""
import os
import sys
from pathlib import Path

# Get the absolute path to the project root directory
project_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(project_dir))

# Add all subdirectories to the path
for subdir in ['database', 'api', 'analysis', 'backtest', 'web', 'utils']:
    subdir_path = project_dir / subdir
    if subdir_path.exists() and subdir_path.is_dir():
        sys.path.insert(0, str(subdir_path))

# Now that paths are set up, import and run the main function
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("dotenv not installed. Environment variables from .env won't be loaded.")
    print("Install with: pip install python-dotenv")

# Import the main app and run it
from app import main, parser, args

if __name__ == "__main__":
    # Run the main function
    main()
