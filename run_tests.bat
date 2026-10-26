@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
pip install pytest 2>nul
cd clickhouse
python -m pytest detector\test_detector.py -v --tb=short
