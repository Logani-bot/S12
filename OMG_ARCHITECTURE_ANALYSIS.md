# OMG Trading Project - Complete Architecture Analysis

Project Location: C:/Coding/omg/  
Status: Phase 1.5 Core Complete + Real-time Monitoring Active

---

## EXECUTIVE SUMMARY

OMG is a multi-layered cryptocurrency trading system combining:
1. Phase 1.5 core (High/Wait state machine with 7-level pyramids)
2. Envelope alert system (45-day MA boundaries)
3. Upbit crisis monitor (Market crash detection)
4. Analysis tools (Excel generation + CSV simulation)

**Key Logic**: 
- Wait→High transition: L +98.5% rebound
- Buy levels: H × (0.56, 0.52, 0.46, 0.41, 0.35, 0.28, 0.21)
- Sell thresholds: +7.7%, +17.3%, +24.4%, +37.4%, +52.7%, +79.9%, +98.5%

---

## 1. COMPONENT ARCHITECTURE

### Files & Roles

**Core Engine**:
- core/phase1_5_core.py (514 lines) - Main state machine + simulation
- phase1_5_rules.py (40 lines) - Buy constraint rules
- config/adapters.py (97 lines) - Binance API client

**Analysis Tools**:
- universe_selector.py - Top 100 coin selection from CoinGecko
- auto_debug_builder.py - Batch Phase 1.5 CSV generation
- coin_analysis_excel.py - Current trading readiness report

**Real-Time Monitoring (Red S/)**:
- envelope_alert.py (408 lines) - 45-day MA envelope proximity alerts
- upbit_monitor.py (278 lines) - Upbit KRW market crisis detection
- run_monitor.py - Launch wrapper
- telegram_notifier.py - Message sending

---

## 2. PHASE 1.5 STRATEGY

### State Machine

Two modes: **high** and **wait**

HIGH:
- Tracks H (cycle high)
- Entry: Mode starts here or after restart
- Exit: low ≤ H × 0.56 (-44%) → switch to wait

WAIT:
- Tracks L (cycle low)
- Entry: Triggered by -44% drop from H
- Buy/Add: Only in wait mode
- Exit: high ≥ L × 1.985 (+98.5%) → restart to high

### Buy Levels (B1-B7) from H

B1 = H × 0.56 (-44%)  ← Shallowest (first buy)
B2 = H × 0.52 (-48%)
B3 = H × 0.46 (-54%)
B4 = H × 0.41 (-59%)
B5 = H × 0.35 (-65%)
B6 = H × 0.28 (-72%)
B7 = H × 0.21 (-79%)  ← Deepest (7th buy)
Stop = H × 0.19 (-81%) ← Stop loss

### Sell Thresholds (Rebound % from L)

Stage 1: +7.7%
Stage 2: +17.3%
Stage 3: +24.4%
Stage 4: +37.4%
Stage 5: +52.7%
Stage 6: +79.9%
Stage 7: +98.5%

---

## 3. REAL-TIME SYSTEMS

### Envelope Alert (10-min intervals)

Config: 45-day MA ±45%, alert if ≤5% from lower band
Scope: Top 30 Binance coins
Trigger: current_price ≤ SMA45 × 0.55 × 1.05
Output: Telegram alerts + Excel files

### Upbit Crisis Monitor (1-hour intervals)

Config: -15% daily drop threshold, 15+ coins trigger
Scope: All KRW-pair coins on Upbit
Logic: 
- START alert when ≥15 coins in free-fall (once per day)
- END alert when <15 coins (once per day after START)
Output: Telegram alerts + status tracking

---

## 4. ANALYSIS PIPELINE

1. **universe_selector.py**: Get Top 100 coins from CoinGecko
2. **auto_debug_builder.py**: 
   - Fetch 5Y OHLCV from Binance per symbol
   - Run Phase 1.5 simulation
   - Save debug/{SYMBOL}_debug.csv
3. **coin_analysis_excel.py**:
   - Read latest debug CSV
   - Get current prices from CoinGecko
   - Calculate next buy target + distance %
   - Output: coin_analysis_{timestamp}.xlsx

---

## 5. KEY COMMANDS

**Manual**:
python auto_debug_builder.py       (generate simulations)
python coin_analysis_excel.py      (generate analysis excel)

**Continuous**:
python Red S/envelope_alert.py     (10-min monitoring)
python Red S/run_monitor.py        (1-hour monitoring)

---

## 6. OUTPUT FILES

**Simulations**: debug/{SYMBOL}_debug.csv (27 columns, 1500+ rows)
**Analysis**: output/coin_analysis_{timestamp}.xlsx
**Alerts**: output/envelope_alerts_{timestamp}.xlsx
**Logs**: Red S/upbit_monitor_{YYYYMMDD}.log

---

## 7. CONFIGURATION

**.env** (Envelope):
TELEGRAM_BOT_TOKEN=xxx
TELEGRAM_CHAT_ID=xxx

**Red S/config.json** (Upbit):
telegram_bot_token, telegram_chat_id

---

## 8. TECHNICAL NOTES

- Binance API: USDT pairs only, daily OHLCV (1500 bars max)
- CoinGecko: Market cap ranking, price, 24h change
- Upbit API: Real-time KRW-pair tickers
- Telegram: Message sending (HTML format)
- Timezone: UTC→KST conversion (UTC+9)
- Precision: 10 decimals for calculations
- Constraints: Daily candles, no partial fills, single position per symbol

---

**Version**: 1.0
**Created**: 2025-11-02
**Status**: Production-ready (core components) + Real-time monitoring active
