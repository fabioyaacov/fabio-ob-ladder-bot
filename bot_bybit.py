"""
Fabio OB Ladder Strategy - Bybit Testnet Bot (Autonomous)
Smart OB Ladder Engine: detects order blocks from Bybit OHLCV data,
places 4 Long + 4 Short limit orders with SL and 3 TPs in premium/discount zones.

Architecture:
  Bybit API -> Bot Python (VPS) -> Smart OB Ladder Engine -> Long / Short

No TradingView webhook needed. Runs fully autonomous on Render free tier.
"""

import os
import math
import threading
import time
import requests
from datetime import datetime
from flask import Flask, request, jsonify
import ccxt

app = Flask(__name__)

# ============================================================================
# CONFIGURATION - Environment variables (set on Render)
# ============================================================================
API_KEY = os.environ.get('BYBIT_TESTNET_API_KEY', 'YOUR_API_KEY_HERE')
API_SECRET = os.environ.get('BYBIT_TESTNET_API_SECRET', 'YOUR_API_SECRET_HERE')

SYMBOL = os.environ.get('SYMBOL', 'BTCUSDT')
ACCOUNT_BALANCE = float(os.environ.get('ACCOUNT_BALANCE', '1000'))
RISK_PER_TRADE_PCT = float(os.environ.get('RISK_PER_TRADE_PCT', '1.0'))
POLL_INTERVAL_SEC = int(os.environ.get('POLL_INTERVAL_SEC', '60'))
SWING_LENGTH = int(os.environ.get('SWING_LENGTH', '10'))
MAX_ATR_MULT = float(os.environ.get('MAX_ATR_MULT', '3.5'))
NUM_ENTRIES = 4
PRIMARY_TIMEFRAME = os.environ.get('PRIMARY_TIMEFRAME', '30m')

# ============================================================================
# EXCHANGE SETUP (synchronous CCXT - more reliable in threads)
# ============================================================================
exchange = ccxt.bybit({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'enableRateLimit': True,
    'timeout': 30000,
    'options': {
        'defaultType': 'future',
        'defaultMarginMode': 'isolated',
        'defaultHedgeMode': True,
    }
})

# Force testnet endpoint
exchange.set_sandbox_mode(True)

# ============================================================================
# GLOBAL STATE
# ============================================================================
bot_state = {
    'running': False,
    'engine_step': 'not started',
    'last_cycle': None,
    'current_price': None,
    'mid_range': None,
    'nearest_bull_ob': None,
    'nearest_bear_ob': None,
    'active_orders': {},
    'fired_entries': [],
    'last_bull_ob_start': None,
    'last_bear_ob_start': None,
    'cycle_count': 0,
    'errors': [],
}

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)

# ============================================================================
# OB LADDER ENGINE - Order Block Detection
# ============================================================================

def calculate_atr(highs, lows, closes, period=10):
    if len(highs) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0.0
    return sum(trs[-period:]) / period

def find_pivots(highs, lows, length=10):
    pivots = []
    for i in range(length, len(highs) - length):
        is_high = all(highs[i] >= highs[i-j] for j in range(1, length+1)) and \
                  all(highs[i] >= highs[i+j] for j in range(1, length+1))
        is_low = all(lows[i] <= lows[i-j] for j in range(1, length+1)) and \
                 all(lows[i] <= lows[i+j] for j in range(1, length+1))
        if is_high:
            pivots.append((i, 'high', highs[i]))
        if is_low:
            pivots.append((i, 'low', lows[i]))
    return pivots

def detect_order_blocks(highs, lows, opens, closes, volumes, timestamps, swing_length=10, atr_val=0, max_atr_mult=3.5):
    if len(highs) < swing_length * 2 + 1:
        return [], []

    bullish_obs = []
    bearish_obs = []

    pivots = find_pivots(highs, lows, swing_length)
    pivot_indices = {p[0]: p[1] for p in pivots}

    for i in range(swing_length, len(highs) - swing_length):
        is_pivot_high = pivot_indices.get(i) == 'high'
        is_pivot_low = pivot_indices.get(i) == 'low'

        if is_pivot_high:
            box_bottom = min(lows[max(0, i-3):i])
            box_top = max(opens[i-1], closes[i-1])
            actual_bottom = lows[i-1]
            actual_top = highs[i-1]
            actual_time = timestamps[i-1]
            for j in range(1, min(i, 20)):
                if lows[i-j] < actual_bottom:
                    actual_bottom = lows[i-j]
                    actual_top = max(opens[i-j], closes[i-j])
                    actual_time = timestamps[i-j]
            ob_size = abs(actual_top - actual_bottom)
            if atr_val == 0 or ob_size <= atr_val * max_atr_mult:
                bullish_obs.append({
                    'top': actual_top, 'bottom': actual_bottom,
                    'volume': volumes[i] + (volumes[i-1] if i > 0 else 0),
                    'ob_type': 'Bull', 'start_time': actual_time,
                    'breaker': False, 'break_time': None,
                })

        if is_pivot_low:
            actual_top = highs[i-1]
            actual_bottom = lows[i-1]
            actual_time = timestamps[i-1]
            for j in range(1, min(i, 20)):
                if highs[i-j] > actual_top:
                    actual_top = highs[i-j]
                    actual_bottom = min(opens[i-j], closes[i-j])
                    actual_time = timestamps[i-j]
            ob_size = abs(actual_top - actual_bottom)
            if atr_val == 0 or ob_size <= atr_val * max_atr_mult:
                bearish_obs.append({
                    'top': actual_top, 'bottom': actual_bottom,
                    'volume': volumes[i] + (volumes[i-1] if i > 0 else 0),
                    'ob_type': 'Bear', 'start_time': actual_time,
                    'breaker': False, 'break_time': None,
                })

    return bullish_obs, bearish_obs

def update_breaker_status(obs, highs, lows, timestamps):
    for ob in obs:
        if ob['breaker']:
            continue
        for i in range(len(highs)):
            if timestamps[i] < ob['start_time']:
                continue
            if ob['ob_type'] == 'Bull' and lows[i] < ob['bottom']:
                ob['breaker'] = True
                ob['break_time'] = timestamps[i]
                break
            elif ob['ob_type'] == 'Bear' and highs[i] > ob['top']:
                ob['breaker'] = True
                ob['break_time'] = timestamps[i]
                break
    return obs

def find_nearest_obs(bullish_obs, bearish_obs, current_price):
    nearest_bull = None
    nearest_bear = None
    nearest_bull_dist = float('inf')
    nearest_bear_dist = float('inf')

    for ob in bullish_obs:
        if ob['breaker']:
            continue
        ob_mid = (ob['top'] + ob['bottom']) / 2.0
        ob_range = abs(ob['top'] - ob['bottom'])
        dist = abs(current_price - ob_mid)
        proximity_limit = ob['bottom'] - ob_range * 2.0
        if current_price >= proximity_limit and dist < nearest_bull_dist:
            nearest_bull_dist = dist
            nearest_bull = ob

    for ob in bearish_obs:
        if ob['breaker']:
            continue
        ob_mid = (ob['top'] + ob['bottom']) / 2.0
        ob_range = abs(ob['top'] - ob['bottom'])
        dist = abs(current_price - ob_mid)
        proximity_limit = ob['top'] + ob_range * 2.0
        if current_price <= proximity_limit and dist < nearest_bear_dist:
            nearest_bear_dist = dist
            nearest_bear = ob

    return nearest_bull, nearest_bear

def calc_mid_range(bull_ob, bear_ob):
    if bull_ob is None or bear_ob is None:
        return None
    low_ref = bull_ob['top']
    high_ref = bear_ob['bottom']
    if high_ref > low_ref:
        return (low_ref + high_ref) / 2.0
    return None

def calculate_ladder_entries(ob, direction, mid_range, opposite_ob):
    ob_top = ob['top']
    ob_bottom = ob['bottom']
    ob_range = abs(ob_top - ob_bottom)
    if ob_range <= 0:
        return []

    spacing = ob_range / (NUM_ENTRIES + 1)
    entry_prices = []

    if direction == 'Long':
        for i in range(1, NUM_ENTRIES + 1):
            entry_price = ob_bottom + (spacing * i)
            if entry_price < ob_top:
                entry_prices.append(entry_price)
    else:
        for i in range(1, NUM_ENTRIES + 1):
            entry_price = ob_top - (spacing * i)
            if entry_price > ob_bottom:
                entry_prices.append(entry_price)

    if not entry_prices:
        return []

    avg_entry = sum(entry_prices) / len(entry_prices)
    risk_amount = ACCOUNT_BALANCE * RISK_PER_TRADE_PCT / 100.0

    if direction == 'Long':
        stop_loss = ob_bottom * 0.998
    else:
        stop_loss = ob_top * 1.002

    risk_per_unit = abs(avg_entry - stop_loss)
    if risk_per_unit <= 0:
        return []

    position_size = risk_amount / (risk_per_unit * len(entry_prices))
    tp_zone_start = mid_range
    entries = []

    for i, entry_price in enumerate(entry_prices):
        tp1, tp2, tp3 = None, None, None

        if opposite_ob and mid_range:
            if direction == 'Long':
                tp_zone_end = opposite_ob['bottom']
                if tp_zone_end > tp_zone_start:
                    zone_range = abs(tp_zone_end - tp_zone_start)
                    tp1 = tp_zone_start + zone_range * 0.25
                    tp2 = tp_zone_start + zone_range * 0.50
                    tp3 = tp_zone_start + zone_range * 0.75
                    if tp3 > opposite_ob['bottom']:
                        tp3 = opposite_ob['bottom']
            else:
                tp_zone_end = opposite_ob['top']
                if tp_zone_end < tp_zone_start:
                    zone_range = abs(tp_zone_end - tp_zone_start)
                    tp1 = tp_zone_start - zone_range * 0.25
                    tp2 = tp_zone_start - zone_range * 0.50
                    tp3 = tp_zone_start - zone_range * 0.75
                    if tp3 < opposite_ob['top']:
                        tp3 = opposite_ob['top']

        if tp1 is None:
            tp1_reward = ACCOUNT_BALANCE * 1.0 / 100.0
            tp2_reward = ACCOUNT_BALANCE * 2.0 / 100.0
            tp3_reward = ACCOUNT_BALANCE * 3.0 / 100.0
            tp1_dist = tp1_reward / (position_size * len(entry_prices))
            tp2_dist = tp2_reward / (position_size * len(entry_prices))
            tp3_dist = tp3_reward / (position_size * len(entry_prices))
            if direction == 'Long':
                tp1 = entry_price + tp1_dist
                tp2 = entry_price + tp2_dist
                tp3 = entry_price + tp3_dist
            else:
                tp1 = entry_price - tp1_dist
                tp2 = entry_price - tp2_dist
                tp3 = entry_price - tp3_dist

        entries.append({
            'entry_id': f"{direction}_{i+1}",
            'direction': direction,
            'price': entry_price,
            'size': position_size,
            'sl': stop_loss,
            'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
            'risk_amount': risk_amount,
            'avg_entry': avg_entry,
        })

    return entries

# ============================================================================
# BYBIT ORDER MANAGEMENT (synchronous)
# ============================================================================

def setup_hedge_mode(symbol):
    try:
        exchange.set_position_mode(True, symbol)
        log(f'Hedge mode enabled for {symbol}')
    except Exception as e:
        if 'not modified' in str(e).lower() or 'same' in str(e).lower():
            log(f'Hedge mode already enabled for {symbol}')
        else:
            log(f'Warning setting hedge mode: {e}')

def cancel_open_orders(symbol):
    try:
        orders = exchange.fetch_open_orders(symbol)
        for o in orders:
            try:
                exchange.cancel_order(o['id'], symbol)
                log(f'Cancelled order {o["id"]}')
            except Exception as e:
                log(f'Failed to cancel order {o["id"]}: {e}')
    except Exception as e:
        log(f'Error fetching open orders: {e}')

def place_entry_orders(symbol, entries):
    setup_hedge_mode(symbol)
    min_qty = 0.01
    placed = []

    for entry in entries:
        entry_id = entry['entry_id']
        direction = entry['direction']
        price = entry['price']
        qty = max(entry['size'], min_qty)
        sl = entry['sl']
        tp1, tp2, tp3 = entry['tp1'], entry['tp2'], entry['tp3']

        if entry_id in bot_state['fired_entries']:
            log(f'{entry_id} already fired, skipping')
            continue

        order_side = 'buy' if direction.lower() == 'long' else 'sell'
        close_side = 'sell' if order_side == 'buy' else 'buy'
        pos_idx = 1 if order_side == 'buy' else 2

        tp1_qty = round(qty * 0.3333, 4)
        tp2_qty = round(qty * 0.3333, 4)
        tp3_qty = round(qty - tp1_qty - tp2_qty, 4)

        try:
            log(f'Placing {direction} limit: {symbol} qty={qty} @ {price}')
            main_order = exchange.create_order(
                symbol=symbol, type='limit', side=order_side,
                amount=qty, price=float(price),
                params={'positionIdx': pos_idx, 'timeInForce': 'GTC'}
            )
            log(f'Limit order placed: ID={main_order["id"]}')

            try:
                exchange.create_order(
                    symbol=symbol, type='market', side=close_side, amount=qty,
                    params={'stopPrice': float(sl), 'positionIdx': pos_idx,
                            'reduceOnly': True, 'trigger': 'last_price',
                            'triggerDirection': 'descending', 'slOrderType': 'market'}
                )
                log(f'SL placed for {entry_id}')
            except Exception as e:
                log(f'SL error (non-fatal): {e}')

            try:
                exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp1_qty,
                    price=float(tp1), params={'positionIdx': pos_idx, 'reduceOnly': True,
                    'timeInForce': 'GTC', 'trigger': 'last_price', 'triggerDirection': 'ascending'}
                )
                log(f'TP1 placed for {entry_id}')
            except Exception as e:
                log(f'TP1 error (non-fatal): {e}')

            try:
                exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp2_qty,
                    price=float(tp2), params={'positionIdx': pos_idx, 'reduceOnly': True,
                    'timeInForce': 'GTC', 'trigger': 'last_price', 'triggerDirection': 'ascending'}
                )
                log(f'TP2 placed for {entry_id}')
            except Exception as e:
                log(f'TP2 error (non-fatal): {e}')

            try:
                exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp3_qty,
                    price=float(tp3), params={'positionIdx': pos_idx, 'reduceOnly': True,
                    'timeInForce': 'GTC', 'trigger': 'last_price', 'triggerDirection': 'ascending'}
                )
                log(f'TP3 placed for {entry_id}')
            except Exception as e:
                log(f'TP3 error (non-fatal): {e}')

            bot_state['fired_entries'].append(entry_id)
            bot_state['active_orders'][entry_id] = {
                'direction': direction, 'price': price, 'qty': qty,
                'sl': sl, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
                'timestamp': datetime.now().isoformat(),
            }
            placed.append(entry_id)

        except Exception as e:
            log(f'ERROR placing {entry_id}: {e}')

    return placed

def fetch_ohlcv(timeframe, limit=500):
    """Fetch OHLCV directly from Bybit v5 API (more reliable than CCXT for testnet)."""
    try:
        log(f'fetch_ohlcv: START {SYMBOL} {timeframe} limit={limit}')

        # Test basic connectivity first
        try:
            log('fetch_ohlcv: testing connectivity to api-testnet.bybit.com...')
            test_resp = requests.get('https://api-testnet.bybit.com/v5/market/time', timeout=10)
            log(f'fetch_ohlcv: connectivity test HTTP {test_resp.status_code}')
        except Exception as ce:
            log(f'fetch_ohlcv: CONNECTIVITY FAILED: {type(ce).__name__}: {ce}')
            bot_state['errors'].append(f'CONNECTIVITY: {type(ce).__name__}: {str(ce)}')
            return []

        url = 'https://api-testnet.bybit.com/v5/market/kline'
        params = {
            'category': 'linear',
            'symbol': SYMBOL,
            'interval': timeframe,
            'limit': str(limit),
        }
        log(f'fetch_ohlcv: calling {url} with params={params}')
        resp = requests.get(url, params=params, timeout=15)
        log(f'fetch_ohlcv: HTTP {resp.status_code}')
        if resp.status_code != 200:
            log(f'fetch_ohlcv: HTTP error {resp.status_code}: {resp.text[:200]}')
            bot_state['errors'].append(f'fetch_ohlcv HTTP {resp.status_code}: {resp.text[:200]}')
            return []
        result = resp.json()
        if result.get('retCode') != 0:
            log(f'fetch_ohlcv: Bybit error: {result.get("retMsg")}')
            bot_state['errors'].append(f'fetch_ohlcv Bybit: {result.get("retMsg")}')
            return []
        klines = result.get('result', {}).get('list', [])
        klines.reverse()
        data = []
        for k in klines:
            data.append([
                int(k[0]),    # timestamp
                float(k[1]),  # open
                float(k[2]),  # high
                float(k[3]),  # low
                float(k[4]),  # close
                float(k[5]),  # volume
            ])
        log(f'fetch_ohlcv: got {len(data)} bars, DONE')
        return data
    except requests.exceptions.Timeout:
        log('fetch_ohlcv: TIMEOUT')
        bot_state['errors'].append('fetch_ohlcv: TIMEOUT')
        return []
    except Exception as e:
        log(f'fetch_ohlcv ERROR: {type(e).__name__}: {e}')
        bot_state['errors'].append(f'fetch_ohlcv: {type(e).__name__}: {str(e)}')
        return []

def parse_ohlcv(data):
    timestamps = [c[0] for c in data]
    opens = [c[1] for c in data]
    highs = [c[2] for c in data]
    lows = [c[3] for c in data]
    closes = [c[4] for c in data]
    volumes = [c[5] for c in data]
    return timestamps, opens, highs, lows, closes, volumes

# ============================================================================
# MAIN ENGINE LOOP (synchronous, runs in background thread)
# ============================================================================

def engine_cycle():
    log(f'--- Engine cycle #{bot_state["cycle_count"]} ---')
    bot_state['engine_step'] = 'cycle started'
    bot_state['engine_step'] = 'about to fetch ohlcv'

    try:
        bot_state['engine_step'] = 'calling fetch_ohlcv'
        data = fetch_ohlcv(PRIMARY_TIMEFRAME, limit=500)
        if not data or len(data) < SWING_LENGTH * 2 + 1:
            log(f'Not enough data ({len(data) if data else 0} bars)')
            return

        timestamps, opens, highs, lows, closes, volumes = parse_ohlcv(data)
        current_price = closes[-1]
        log(f'Got {len(data)} bars, price={current_price}')
        atr_val = calculate_atr(highs, lows, closes, period=10)
        bot_state['current_price'] = current_price

        bot_state['engine_step'] = 'detecting order blocks'
        bullish_obs, bearish_obs = detect_order_blocks(
            highs, lows, opens, closes, volumes, timestamps,
            swing_length=SWING_LENGTH, atr_val=atr_val, max_atr_mult=MAX_ATR_MULT
        )
        log(f'Found {len(bullish_obs)} Bull OBs, {len(bearish_obs)} Bear OBs')

        bot_state['engine_step'] = 'updating breaker status'
        bullish_obs = update_breaker_status(bullish_obs, highs, lows, timestamps)
        bearish_obs = update_breaker_status(bearish_obs, highs, lows, timestamps)

        bot_state['engine_step'] = 'finding nearest OBs'
        nearest_bull, nearest_bear = find_nearest_obs(bullish_obs, bearish_obs, current_price)
        bot_state['nearest_bull_ob'] = nearest_bull
        bot_state['nearest_bear_ob'] = nearest_bear

        bot_state['engine_step'] = 'calculating mid-range'
        mid_range = calc_mid_range(nearest_bull, nearest_bear)
        bot_state['mid_range'] = mid_range

        log(f'Price={current_price} | Nearest Bull={nearest_bull["top"] if nearest_bull else "none"} '
            f'Nearest Bear={nearest_bear["bottom"] if nearest_bear else "none"} | MidRange={mid_range}')

        bot_state['engine_step'] = 'checking OB changes'
        bull_changed = False
        bear_changed = False

        if nearest_bull:
            bull_start = nearest_bull['start_time']
            if bot_state['last_bull_ob_start'] is None or bull_start != bot_state['last_bull_ob_start']:
                bull_changed = True
                bot_state['last_bull_ob_start'] = bull_start
                bot_state['fired_entries'] = [e for e in bot_state['fired_entries'] if not e.startswith('Long')]
                log('Bull OB changed -> cleared Long fired entries')

        if nearest_bear:
            bear_start = nearest_bear['start_time']
            if bot_state['last_bear_ob_start'] is None or bear_start != bot_state['last_bear_ob_start']:
                bear_changed = True
                bot_state['last_bear_ob_start'] = bear_start
                bot_state['fired_entries'] = [e for e in bot_state['fired_entries'] if not e.startswith('Short')]
                log('Bear OB changed -> cleared Short fired entries')

        if bull_changed or bear_changed:
            bot_state['engine_step'] = 'cancelling old orders'
            cancel_open_orders(SYMBOL)

        bot_state['engine_step'] = 'calculating ladder entries'
        long_entries = []
        short_entries = []
        if nearest_bull:
            long_entries = calculate_ladder_entries(nearest_bull, 'Long', mid_range, nearest_bear)
        if nearest_bear:
            short_entries = calculate_ladder_entries(nearest_bear, 'Short', mid_range, nearest_bull)

        all_entries = long_entries + short_entries
        new_entries = [e for e in all_entries if e['entry_id'] not in bot_state['fired_entries']]

        if new_entries:
            bot_state['engine_step'] = f'placing {len(new_entries)} orders'
            log(f'Placing {len(new_entries)} new entries...')
            placed = place_entry_orders(SYMBOL, new_entries)
            log(f'Placed {len(placed)} entries: {placed}')
        else:
            log('No new entries to place')

        bot_state['engine_step'] = 'cycle complete'
        bot_state['last_cycle'] = datetime.now().isoformat()
        bot_state['cycle_count'] += 1
        log(f'Cycle #{bot_state["cycle_count"]} complete')

    except Exception as e:
        import traceback
        err_msg = f'{datetime.now().isoformat()} - {type(e).__name__}: {str(e)}'
        bot_state['errors'].append(err_msg)
        bot_state['engine_step'] = f'ERROR: {type(e).__name__}: {str(e)}'
        log(f'Engine cycle error: {e}')
        log(traceback.format_exc())

def engine_loop():
    bot_state['running'] = True
    log('Smart OB Ladder Engine started')
    log(f'Symbol={SYMBOL} Balance={ACCOUNT_BALANCE} Risk={RISK_PER_TRADE_PCT}% Interval={POLL_INTERVAL_SEC}s')
    log(f'Timeframe={PRIMARY_TIMEFRAME} SwingLength={SWING_LENGTH}')

    while bot_state['running']:
        engine_cycle()
        time.sleep(POLL_INTERVAL_SEC)

    log('Engine stopped')

def start_engine_thread():
    def run_loop():
        try:
            log('Engine thread starting...')
            engine_loop()
        except Exception as e:
            bot_state['errors'].append(f'THREAD FATAL: {str(e)}')
            log(f'Engine thread FATAL: {e}')

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    return thread

# ============================================================================
# FLASK ENDPOINTS
# ============================================================================

@app.route('/status', methods=['GET'])
def status():
    return jsonify({
        'status': 'running' if bot_state['running'] else 'stopped',
        'engine_step': bot_state['engine_step'],
        'symbol': SYMBOL,
        'current_price': bot_state['current_price'],
        'mid_range': bot_state['mid_range'],
        'nearest_bull_ob': bot_state['nearest_bull_ob'],
        'nearest_bear_ob': bot_state['nearest_bear_ob'],
        'active_orders': bot_state['active_orders'],
        'fired_entries': bot_state['fired_entries'],
        'cycle_count': bot_state['cycle_count'],
        'last_cycle': bot_state['last_cycle'],
        'errors': bot_state['errors'][-10:],
        'api_key_set': API_KEY != 'YOUR_API_KEY_HERE',
    })

@app.route('/test-net', methods=['GET'])
def test_net():
    """Test network connectivity to Bybit testnet from Flask context."""
    try:
        resp = requests.get('https://api-testnet.bybit.com/v5/market/time', timeout=10)
        return jsonify({
            'status': 'ok',
            'http_code': resp.status_code,
            'content_type': resp.headers.get('content-type', 'unknown'),
            'raw_text': resp.text[:500],
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error_type': type(e).__name__,
            'error': str(e),
        }), 500

@app.route('/test-ohlcv', methods=['GET'])
def test_ohlcv():
    """Test fetching OHLCV data from Bybit testnet from Flask context."""
    try:
        url = 'https://api-testnet.bybit.com/v5/market/kline'
        params = {'category': 'linear', 'symbol': SYMBOL, 'interval': '30m', 'limit': '5'}
        resp = requests.get(url, params=params, timeout=15)
        return jsonify({
            'status': 'ok',
            'http_code': resp.status_code,
            'content_type': resp.headers.get('content-type', 'unknown'),
            'raw_text': resp.text[:500],
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error_type': type(e).__name__,
            'error': str(e),
        }), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'engine_running': bot_state['running']}), 200

@app.route('/cancel', methods=['POST'])
def cancel_orders():
    symbol = request.json.get('symbol', SYMBOL)
    cancel_open_orders(symbol)
    bot_state['fired_entries'].clear()
    bot_state['active_orders'].clear()
    log(f'Manual cancel: all orders for {symbol} cancelled')
    return jsonify({'status': 'success', 'message': f'Cancelled all orders for {symbol}'}), 200

@app.route('/reset', methods=['POST'])
def reset_engine():
    bot_state['fired_entries'].clear()
    bot_state['active_orders'].clear()
    bot_state['last_bull_ob_start'] = None
    bot_state['last_bear_ob_start'] = None
    log('Engine reset: cleared all tracking state')
    return jsonify({'status': 'success', 'message': 'Engine state reset'}), 200

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'name': 'Fabio OB Ladder - Autonomous Bybit Testnet Bot',
        'architecture': 'Bybit API -> Bot Python (VPS) -> Smart OB Ladder Engine -> Long / Short',
        'config': {
            'symbol': SYMBOL,
            'account_balance': ACCOUNT_BALANCE,
            'risk_per_trade_pct': RISK_PER_TRADE_PCT,
            'poll_interval_sec': POLL_INTERVAL_SEC,
            'swing_length': SWING_LENGTH,
            'primary_timeframe': PRIMARY_TIMEFRAME,
            'num_entries_per_ob': NUM_ENTRIES,
            'api_key_set': API_KEY != 'YOUR_API_KEY_HERE',
            'testnet': True,
            'hedge_mode': True,
            'margin_mode': 'isolated',
        },
        'endpoints': {
            '/status': 'GET - Full bot status',
            '/health': 'GET - Health check',
            '/cancel': 'POST - Cancel all open orders',
            '/reset': 'POST - Reset tracking state',
        }
    })

# ============================================================================
# STARTUP - Engine starts on import (works with gunicorn)
# ============================================================================

if API_KEY != 'YOUR_API_KEY_HERE':
    log('Starting Smart OB Ladder Engine thread...')
    start_engine_thread()
else:
    log('WARNING: API keys not set. Engine will not start.')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log(f'Starting Fabio OB Ladder Bot on port {port}')
    app.run(host='0.0.0.0', port=port, debug=False)
