# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## System Overview

**S12 Trading System** is an automated Korean stock trading signal system based on 20-day moving average envelopes. It tracks high-volume stocks (5000억+ daily turnover), generates buy/sell signals with 3-level split entry/exit strategies, and sends real-time alerts via Telegram.

### Core Architecture

```
Daily_Turnover_Tracker.py → turnover_universe.xlsx
                                      ↓
                          Trading_Signal_System.py → trading_signals.xlsx
                                      ↓
                          Real_Time_Monitor.py (optional)
```

**Data Flow:**
1. **Daily collection** (20:10): Track high-turnover stocks → Generate signals → Send Telegram report
2. **Real-time monitoring** (08:00-20:00): Monitor Summary tab stocks → Alert on buy/sell opportunities

### Trading Strategy

**Buy Levels (3-stage split buying):**
- Level 1: Envelope + 1 tick
- Level 2: Level 1 - 10% + 1 tick
- Level 3: Level 2 - 10% + 1 tick

**Sell Levels:**
- +3%: First sell target (re-touch sell)
- +5%: Second sell target (re-touch sell)
- +7%: Third sell target (immediate full exit)

## Commands

### Daily Operations

```bash
# Manual daily update (same as 20:10 auto-run)
RUN_DAILY_SYSTEM.bat

# Update Excel files only (no Telegram)
UPDATE_EXCEL_NOW.bat

# Individual Python script execution
python Daily_Turnover_Tracker.py --appkey [KEY] --secret [SECRET]
python Trading_Signal_System.py --appkey [KEY] --secret [SECRET] --alert-threshold 10.0
python Real_Time_Monitor.py --appkey [KEY] --secret [SECRET]
```

### Real-Time Monitoring

```bash
# Start real-time monitor (08:00-20:00, trading days only)
run_real_time_monitor.bat
# Or:
start_realtime_monitor.bat

# Monitor all running programs (NEW)
run_dashboard.bat
```

### Upbit Crypto Alert System

```bash
# Local execution (30-min intervals)
run_upbit_optimized.bat

# Deploy to Google Cloud
deploy_to_google_cloud.bat
setup_cloud_scheduler.bat
```

### Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Check trading day status
python trading_day_utils.py

# Test individual components
python test_upbit_alert.py
python test_cloud_function.py
```

## Key Files

### Core Python Scripts

- **Daily_Turnover_Tracker.py**: Fetches daily turnover rankings from Kiwoom API, filters stocks with 5000억+ turnover, excludes ETF/ETN, accumulates results in `turnover_universe.xlsx`
- **Trading_Signal_System.py**: Analyzes stocks from universe file, calculates 20-day MA envelopes, generates 3-level buy/sell signals, sends daily Telegram report at 20:10
- **Real_Time_Monitor.py**: Monitors Summary tab stocks during trading hours (08:00-20:00), sends proximity alerts (5% threshold), one alert per status per day to prevent spam
- **monitor_dashboard.py**: Interactive dashboard for monitoring and controlling all real-time programs, shows process status, log activity, supports one-click start/stop
- **telegram_notifier.py**: Telegram notification module, supports 4 recipients (me, yoonjoo, minjeong, jumeoni), HTML/Markdown formatting
- **trading_day_utils.py**: Trading day utilities using `holidays` library, checks weekends and Korean public holidays, provides next trading day calculation
- **upbit_alert_optimized.py**: Monitors Upbit crypto market, alerts when 15+ coins drop 15%+, runs every 30 minutes, Google Cloud Functions compatible

### Batch Files

- **RUN_DAILY_SYSTEM.bat**: Main daily execution wrapper
- **UPDATE_EXCEL_NOW.bat**: Manual Excel update without scheduler
- **run_trading_signal.bat**: Called by Windows Task Scheduler at 20:10 daily
- **run_real_time_monitor.bat** / **start_realtime_monitor.bat**: Real-time monitoring launcher
- **run_dashboard.bat**: Launches monitoring dashboard for all real-time programs

### Data Files (output/)

- **turnover_universe.xlsx**: Accumulated high-turnover stock universe (persistent history)
- **trading_signals.xlsx**: Current analysis results with two tabs:
  - **Summary**: Active monitoring stocks
  - **History**: Completed trades with realized returns
- **market_cap_universe.xlsx**: Market cap filtered results

### Logs

- **logs/s12_daily_YYYYMMDD.log**: Daily system execution logs
- **realtime_monitor_YYYYMMDD.log**: Real-time monitoring logs
- **alert_history.json**: Alert deduplication tracking

## API Integration

### Kiwoom REST API

- **Base URL**: `https://api.kiwoom.com`
- **Authentication**: Bearer token via OAuth2 client credentials flow
- **Key endpoints**:
  - `/oauth2/token`: Token acquisition
  - `/api/dostk/rkinfo` (ka10032): Turnover rankings
  - `/api/dostk/chart` (ka10081): Chart data (20-day MA calculation)
- **Important**: Use `_AL` suffix for KRX+NXT integrated charts
- **API keys**: Stored in `.env` file and hardcoded in batch files

### Telegram Bot API

- **Configuration**: `.env` file with `TELEGRAM_TOKEN` and 4 `TELEGRAM_CHAT_ID_*` variables
- **Message format**: HTML with structured formatting (emoji indicators, table-like alignment)
- **Send pattern**: Broadcast to all 4 recipients for daily reports, selective sending for alerts

## Technical Details

### Tick Price Calculation

Korean stock market has non-uniform tick sizes:
- < 2,000: 1원
- 2,000-5,000: 5원
- 5,000-20,000: 10원
- 20,000-50,000: 50원
- 50,000-200,000: 100원
- 200,000-500,000: 500원
- ≥ 500,000: 1,000원

Functions `get_tick_unit()` and `get_nearest_tick_price()` in Trading_Signal_System.py handle this logic. Always round up to valid tick prices for buy/sell orders.

### Real-Time Alert Logic

- **Proximity alerts**: Only trigger when getting closer (progressive alerting)
  - 1% proximity: 🔴 alert
  - 3% proximity: 🟠 alert (only if no 1% alert sent)
  - 5% proximity: 🟡 alert (only if no 1%/3% alert sent)
- **Execution alerts**: 🎯 always trigger when low price touches buy level
- **Deduplication**: `alert_history.json` prevents same-status duplicate alerts on same day
- **Dynamic intervals**: Monitoring frequency adjusts based on proximity (1min for <1%, 30min for >10%)

### Excel Formatting

- **Percentage**: `0.00"%"` format (values already multiplied by 100)
- **Currency**: `#,##0` format with thousand separators
- **Dates**: `YYYY-MM-DD` format
- **Conditional highlighting**: Red for buy signals, green for sell signals

### Trading Day Detection

- Uses `holidays` library for Korean public holidays
- Checks weekends (Saturday/Sunday) via `weekday() >= 5`
- Both systems (daily and real-time) skip execution on non-trading days
- See `trading_day_utils.py` for implementation details

## Important Conventions

### File Organization

- **All Excel outputs**: Must be saved in `output/` directory only
- **Test files**: Keep in `test/` directory
- **S1 related files**: Keep in `s1/` directory (separate legacy system)
- **Logs**: Save to `logs/` directory or root with dated filenames

### Data Persistence

- **turnover_universe.xlsx**: Accumulative - preserves historical records with (date, ticker) deduplication
- **trading_signals.xlsx**: Current state only - fully replaced on each run, not accumulative
- **History tab**: Stores completed trades for reference

### Automated Execution

- **Windows Task Scheduler**: Configured to run `run_trading_signal.bat` daily at 20:10
- **Task name**: `S12_Trading_Signal_Daily` or `S12_Debug_Test`
- **Persistence**: Survives reboots
- **Python path**: Hardcoded as `C:\Program Files (x86)\Python311\python.exe` in batch files

### Code Modification Guidelines

- **Before modifying core logic**: Confirm with user first
- **API key handling**: Keys are hardcoded in batch files and `.env` - do not commit changes
- **ETF/ETN exclusion**: Maintain EXCLUDE_KEYWORDS list in Daily_Turnover_Tracker.py (현대, KODEX, TIGER, etc.)
- **Backwards compatibility**: Keep existing Excel structure to avoid breaking downstream dependencies

## Known Issues & Solutions

### Resolved Issues

1. **Duplicate entries**: Fixed by removing accumulative logic in Trading_Signal_System.py
2. **Percentage formatting**: Excel displays correctly with `0.00"%"` format (values pre-multiplied)
3. **Telegram sorting**: Reports now sort by lowest gap percentage (most urgent first)
4. **Low/High price data**: Fixed API response key mapping error (volume→low confusion)
5. **Alert progression**: Only sends new alerts when price moves closer to target
6. **Real-time stability**: Dynamic interval system prevents API spam and improves performance

### Current Stable State

- ✅ No duplicate data issues
- ✅ Accurate percentage display in Excel
- ✅ Proper Telegram message sorting
- ✅ Clean directory structure
- ✅ Correct low/high price data from API
- ✅ Smart alert progression logic
- ✅ Stable real-time monitoring system

## Upbit Crypto Alert System

Separate subsystem for monitoring Upbit cryptocurrency market crashes:

- **Trigger condition**: 15+ coins with 15%+ daily drop
- **Execution**: Every 30 minutes during 09:00-18:00 weekdays
- **Deployment options**: Local (run_upbit_optimized.bat) or Google Cloud Functions
- **Cost optimization**: Batch API calls (100 coins/request), 75% reduction in API calls
- **Expected monthly cost**: < $0.50 on Google Cloud

See [UPBIT_ALERT_GUIDE.md](UPBIT_ALERT_GUIDE.md) for detailed setup.

## Dependencies

```
requests==2.31.0
python-dotenv==1.0.0
pandas
openpyxl
holidays  # For Korean public holiday detection
```

Install with: `pip install -r requirements.txt`

## Architecture Notes

### Separation of Concerns

- **Data collection**: Daily_Turnover_Tracker.py (independent, reusable universe)
- **Signal generation**: Trading_Signal_System.py (reads universe, generates signals, sends reports)
- **Real-time monitoring**: Real_Time_Monitor.py (optional, reads signals, monitors market)
- **Notifications**: telegram_notifier.py (shared module with recipient management)

### API Token Management

- Tokens expire - scripts re-authenticate on each run
- No persistent token storage (stateless execution)
- Token acquisition happens in each script independently

### Excel as Data Store

The system uses Excel files as the primary data store (no database). This design choice:
- ✅ Makes data immediately accessible to non-technical users
- ✅ Allows manual inspection and verification
- ✅ Provides built-in visualization (conditional formatting, charts)
- ⚠️ Requires careful handling of file locking (close Excel before running scripts)
- ⚠️ No concurrent write support (sequential execution enforced by batch files)

## Troubleshooting

### Common Issues

**Excel files not updating:**
- Check if Excel is open (file locking issue)
- Verify Python packages installed: `pip install -r requirements.txt`
- Check API keys in batch files and `.env`
- Verify internet connection
- Check logs: `logs/s12_daily_YYYYMMDD.log`

**Telegram messages not sending:**
- Verify `.env` file exists with correct TELEGRAM_TOKEN
- Check TELEGRAM_CHAT_ID_* values are set
- Test with test_upbit_alert_demo.py
- Check Telegram bot is not blocked

**Scheduler not running:**
- Open Windows Task Scheduler
- Search for task name: `S12_Trading_Signal_Daily` or `S12*`
- Check last run result and next scheduled time
- Verify task is enabled
- Check Python path: `C:\Program Files (x86)\Python311\python.exe`

**Non-trading day execution:**
- System automatically skips weekends and Korean holidays
- Check `trading_day_utils.py` for holiday detection logic
- Run `python trading_day_utils.py` to see current status

**Real-time programs not running:**
- Use `run_dashboard.bat` to check all program statuses
- Dashboard shows process status, PID, and log activity
- One-click start/stop for individual or all programs
- See [MONITOR_DASHBOARD_GUIDE.md](MONITOR_DASHBOARD_GUIDE.md) for details

## Related Documentation

- [S12_SYSTEM_GUIDE.md](S12_SYSTEM_GUIDE.md): Comprehensive system guide with detailed logic explanations
- [README_DAILY_SYSTEM.md](README_DAILY_SYSTEM.md): Quick start guide for daily operations
- [TEXTBOOK_SYSTEM.md](TEXTBOOK_SYSTEM.md): FAQ and common operations reference
- [MONITOR_DASHBOARD_GUIDE.md](MONITOR_DASHBOARD_GUIDE.md): Real-time program monitoring dashboard guide
- [UPBIT_ALERT_GUIDE.md](UPBIT_ALERT_GUIDE.md): Cryptocurrency alert system setup
- [google_cloud_setup.md](google_cloud_setup.md): Google Cloud deployment instructions
