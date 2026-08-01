"""
Fabio OB Ladder Strategy - Bybit Testnet Bot (Autonomous)
Smart OB Ladder Engine: detects order blocks from Bybit OHLCV data,
places 4 Long + 4 Short limit orders with SL and 3 TPs in premium/discount zones.

Architecture:
  Bybit API -> Bot Python (VPS) -> Smart OB Ladder Engine -> Long / Short

No TradingView webhook needed. Runs fully autonomous on Render free tier.
"""

import os
import json
import math
import asyncio
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
import ccxt.async_support as ccxt

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
TIMEFRAMES = ['1d', '240', '60', '30']

# ============================================================================
# EXCHANGE SETUP
# ============================================================================
exchange = ccxt.bybit({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'test': True,
    'options': {
        'defaultType': 'future',
        'defaultMarginMode': 'isolated',
        'defaultHedgeMode': True,
    }
})

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
    print(f'[{ts}] {msg}')

# ============================================================================
# OB LADDER ENGINE - Order Block Detection
# ============================================================================

def calculate_atr(highs, lows, closes, period=10):
    """Calculate ATR (Average True Range)."""
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
    """Find pivot highs and lows in the price series.
    Returns list of (index, type, price) where type is 'high' or 'low'."""
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
    """Detect bullish and bearish order blocks from OHLCV data.
    Mimics the Pine Script findOBSwings + findOrderBlocks logic."""
    if len(highs) < swing_length * 2 + 1:
        return [], []

    bullish_obs = []
    bearish_obs = []

    # Find pivots
    pivots = find_pivots(highs, lows, swing_length)

    # Track swing state (like Pine Script swingType)
    swing_type = 0  # 0 = looking for high, 1 = looking for low
    last_swing_high_idx = None
    last_swing_high_price = None
    last_swing_low_idx = None
    last_swing_low_price = None
    crossed_high = False
    crossed_low = False

    for i in range(swing_length, len(highs) - swing_length):
        # Check if this bar is a pivot high
        is_pivot_high = i in [p[0] for p in pivots if p[1] == 'high']
        is_pivot_low = i in [p[0] for p in pivots if p[1] == 'low']

        # Update swing state
        upper = max(highs[max(0, i-swing_length):i+1]) if i >= swing_length else highs[i]
        lower = min(lows[max(0, i-swing_length):i+1]) if i >= swing_length else lows[i]

        if highs[i] > upper:
            swing_type = 0
        elif lows[i] < lower:
            swing_type = 1

        # Detect bullish OB (price breaks above swing high)
        if swing_type == 0 and is_pivot_high and not crossed_high:
            if last_swing_high_idx is not None and i > last_swing_high_idx:
                crossed_high = True
                # Find the lowest point between last swing low and this breakout
                box_bottom = min(lows[last_swing_low_idx:i+1]) if last_swing_low_idx else min(lows[:i+1])
                # Find box top: the highest high of the candle before the move up
                box_top = max(opens[max(0, i-1)], closes[max(0, i-1)])
                box_top = min(box_top, box_bottom)  # Use min[max] logic

                # Search for actual box bottom (lowest low looking back)
                actual_bottom = lows[i-1]
                actual_top = highs[i-1]
                actual_time = timestamps[i-1]
                for j in range(1, min(i, 20)):
                    if i - j < 0:
                        break
                    if lows[i-j] < actual_bottom:
                        actual_bottom = lows[i-j]
                        actual_top = max(opens[i-j], closes[i-j])
                        actual_time = timestamps[i-j]

                ob_size = abs(actual_top - actual_bottom)
                if atr_val == 0 or ob_size <= atr_val * max_atr_mult:
                    ob = {
                        'top': actual_top,
                        'bottom': actual_bottom,
                        'volume': volumes[i] + (volumes[i-1] if i > 0 else 0),
                        'ob_type': 'Bull',
                        'start_time': actual_time,
                        'breaker': False,
                        'break_time': None,
                    }
                    bullish_obs.append(ob)

        # Detect bearish OB (price breaks below swing low)
        if swing_type == 1 and is_pivot_low and not crossed_low:
            if last_swing_low_idx is not None and i > last_swing_low_idx:
                crossed_low = True
                # Find the highest point between last swing high and this breakdown
                box_top = max(highs[last_swing_high_idx:i+1]) if last_swing_high_idx else max(highs[:i+1])
                box_bottom = min(opens[max(0, i-1)], closes[max(0, i-1)])
                box_bottom = max(box_bottom, box_top)

                actual_top = highs[i-1]
                actual_bottom = lows[i-1]
                actual_time = timestamps[i-1]
                for j in range(1, min(i, 20)):
                    if i - j < 0:
                        break
                    if highs[i-j] > actual_top:
                        actual_top = highs[i-j]
                        actual_bottom = min(opens[i-j], closes[i-j])
                        actual_time = timestamps[i-j]

                ob_size = abs(actual_top - actual_bottom)
                if atr_val == 0 or ob_size <= atr_val * max_atr_mult:
                    ob = {
                        'top': actual_top,
                        'bottom': actual_bottom,
                        'volume': volumes[i] + (volumes[i-1] if i > 0 else 0),
                        'ob_type': 'Bear',
                        'start_time': actual_time,
                        'breaker': False,
                        'break_time': None,
                    }
                    bearish_obs.append(ob)

        # Track last swing points
        if is_pivot_high:
            last_swing_high_idx = i
            last_swing_high_price = highs[i]
            crossed_high = False
        if is_pivot_low:
            last_swing_low_idx = i
            last_swing_low_price = lows[i]
            crossed_low = False

    return bullish_obs, bearish_obs

def update_breaker_status(obs, highs, lows, timestamps):
    """Mark OBs as breaker if price has broken through them."""
    for ob in obs:
        if ob['breaker']:
            continue
        # Check if any candle broke below a Bull OB or above a Bear OB
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
    """Find nearest non-broken Bull OB (support) and Bear OB (resistance) to current price."""
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
        # Allow price to be above, inside, or slightly below the OB
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
        # Allow price to be below, inside, or slightly above the OB
        proximity_limit = ob['top'] + ob_range * 2.0
        if current_price <= proximity_limit and dist < nearest_bear_dist:
            nearest_bear_dist = dist
            nearest_bear = ob

    return nearest_bull, nearest_bear

def calc_mid_range(bull_ob, bear_ob):
    """Calculate the 50% mid-range line between nearest support and resistance OBs."""
    if bull_ob is None or bear_ob is None:
        return None
    low_ref = bull_ob['top']
    high_ref = bear_ob['bottom']
    if high_ref > low_ref:
        return (low_ref + high_ref) / 2.0
    return None

def calculate_ladder_entries(ob, direction, mid_range, opposite_ob):
    """Calculate 4 entry prices, SL, and 3 TPs for an OB.
    Returns list of dicts with entry details."""
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

    # TP placement: between mid-range and opposite OB
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

        # Fallback if no valid opposite OB
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
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'risk_amount': risk_amount,
            'avg_entry': avg_entry,
        })

    return entries

# ============================================================================
# BYBIT ORDER MANAGEMENT
# ============================================================================

async def setup_hedge_mode(symbol):
    """Enable hedge mode on Bybit for the given symbol."""
    try:
        await exchange.set_position_mode(True, symbol)
        log(f'Hedge mode enabled for {symbol}')
    except Exception as e:
        if 'not modified' in str(e).lower() or 'same' in str(e).lower():
            log(f'Hedge mode already enabled for {symbol}')
        else:
            log(f'Warning setting hedge mode: {e}')

async def cancel_open_orders(symbol):
    """Cancel all open orders for the symbol."""
    try:
        orders = await exchange.fetch_open_orders(symbol)
        for o in orders:
            try:
                await exchange.cancel_order(o['id'], symbol)
                log(f'Cancelled order {o["id"]}')
            except Exception as e:
                log(f'Failed to cancel order {o["id"]}: {e}')
    except Exception as e:
        log(f'Error fetching open orders: {e}')

async def place_entry_orders(symbol, entries):
    """Place limit + SL + 3 TPs for each entry on Bybit testnet."""
    await setup_hedge_mode(symbol)

    min_qty = 0.01
    placed = []

    for entry in entries:
        entry_id = entry['entry_id']
        direction = entry['direction']
        price = entry['price']
        qty = max(entry['size'], min_qty)
        sl = entry['sl']
        tp1 = entry['tp1']
        tp2 = entry['tp2']
        tp3 = entry['tp3']

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
            # Main limit order
            log(f'Placing {direction} limit: {symbol} qty={qty} @ {price}')
            main_order = await exchange.create_order(
                symbol=symbol, type='limit', side=order_side,
                amount=qty, price=float(price),
                params={'positionIdx': pos_idx, 'timeInForce': 'GTC'}
            )
            log(f'Limit order placed: ID={main_order["id"]}')

            # Stop Loss
            try:
                sl_order = await exchange.create_order(
                    symbol=symbol, type='market', side=close_side, amount=qty,
                    params={
                        'stopPrice': float(sl), 'positionIdx': pos_idx,
                        'reduceOnly': True, 'trigger': 'last_price',
                        'triggerDirection': 'descending', 'slOrderType': 'market',
                    }
                )
                log(f'SL placed: ID={sl_order["id"]}')
            except Exception as e:
                log(f'SL error (non-fatal): {e}')

            # TP1
            try:
                tp1_order = await exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp1_qty,
                    price=float(tp1), params={
                        'positionIdx': pos_idx, 'reduceOnly': True, 'timeInForce': 'GTC',
                        'trigger': 'last_price', 'triggerDirection': 'ascending',
                    }
                )
                log(f'TP1 placed: ID={tp1_order["id"]}')
            except Exception as e:
                log(f'TP1 error (non-fatal): {e}')

            # TP2
            try:
                tp2_order = await exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp2_qty,
                    price=float(tp2), params={
                        'positionIdx': pos_idx, 'reduceOnly': True, 'timeInForce': 'GTC',
                        'trigger': 'last_price', 'triggerDirection': 'ascending',
                    }
                )
                log(f'TP2 placed: ID={tp2_order["id"]}')
            except Exception as e:
                log(f'TP2 error (non-fatal): {e}')

            # TP3
            try:
                tp3_order = await exchange.create_order(
                    symbol=symbol, type='limit', side=close_side, amount=tp3_qty,
                    price=float(tp3), params={
                        'positionIdx': pos_idx, 'reduceOnly': True, 'timeInForce': 'GTC',
                        'trigger': 'last_price', 'triggerDirection': 'ascending',
                    }
                )
                log(f'TP3 placed: ID={tp3_order["id"]}')
            except Exception as e:
                log(f'TP3 error (non-fatal): {e}')

            bot_state['fired_entries'].append(entry_id)
            bot_state['active_orders'][entry_id] = {
                'direction': direction,
                'price': price,
                'qty': qty,
                'sl': sl,
                'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
                'timestamp': datetime.now().isoformat(),
            }
            placed.append(entry_id)

        except Exception as e:
            log(f'ERROR placing {entry_id}: {e}')

    return placed

async def fetch_ohlcv(timeframe, limit=500):
    """Fetch OHLCV data from Bybit."""
    try:
      data = await exchange.fetch_ohlcv(SYMBOL, timeframe=timeframe, limit=limit)
      return data
    except Exception as e:
        log(f'Error fetching OHLCV {timeframe}: {e}')
        return []

def parse_ohlcv(data):
    """Parse CCXT OHLCV list into separate arrays."""
    timestamps = [c[0] for c in data]
    opens = [c[1] for c in data]
    highs = [c[2] for c in data]
    lows = [c[3] for c in data]
    closes = [c[4] for c in data]
    volumes = [c[5] for c in data]
    return timestamps, opens, highs, lows, closes, volumes

# ============================================================================
# MAIN ENGINE LOOP
# ============================================================================

async def engine_cycle():
    """Run one cycle of the OB Ladder Engine."""
    log(f'--- Engine cycle #{bot_state["cycle_count"]} ---')
    bot_state['engine_step'] = 'starting cycle'

    try:
        primary_tf = TIMEFRAMES[-1]
        bot_state['engine_step'] = f'fetching OHLCV {primary_tf}'
        log(f'Step 1: Fetching OHLCV {primary_tf}...')
        data = await fetch_ohlcv(primary_tf, limit=500)
        if not data or len(data) < SWING_LENGTH * 2 + 1:
            log(f'Not enough data for {primary_tf} ({len(data) if data else 0} bars)')
            return

        timestamps, opens, highs, lows, closes, volumes = parse_ohlcv(data)
        current_price = closes[-1]
        log(f'Step 2: Got {len(data)} bars, current price={current_price}')
        atr_val = calculate_atr(highs, lows, closes, period=10)
        bot_state['current_price'] = current_price

        log(f'Step 3: Detecting order blocks (swing_length={SWING_LENGTH})...')
        bot_state['engine_step'] = 'detecting order blocks'
        bullish_obs, bearish_obs = detect_order_blocks(
            highs, lows, opens, closes, volumes, timestamps,
            swing_length=SWING_LENGTH, atr_val=atr_val, max_atr_mult=MAX_ATR_MULT
        )
        log(f'Step 4: Found {len(bullish_obs)} Bull OBs, {len(bearish_obs)} Bear OBs')

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

        log(f'Price={current_price} | Bull OBs={len(bullish_obs)} Bear OBs={len(bearish_obs)} | '
            f'Nearest Bull={nearest_bull["top"] if nearest_bull else "none"} '
            f'Nearest Bear={nearest_bear["bottom"] if nearest_bear else "none"} | '
            f'MidRange={mid_range}')

        bot_state['engine_step'] = 'checking OB changes'
        bull_changed = False
        bear_changed = False

        if nearest_bull:
            bull_start = nearest_bull['start_time']
            if bot_state['last_bull_ob_start'] is None or bull_start != bot_state['last_bull_ob_start']:
                bull_changed = True
                bot_state['last_bull_ob_start'] = bull_start
                bot_state['fired_entries'] = [e for e in bot_state['fired_entries'] if not e.startswith('Long')]
                log(f'Bull OB changed -> cleared Long fired entries')

        if nearest_bear:
            bear_start = nearest_bear['start_time']
            if bot_state['last_bear_ob_start'] is None or bear_start != bot_state['last_bear_ob_start']:
                bear_changed = True
                bot_state['last_bear_ob_start'] = bear_start
                bot_state['fired_entries'] = [e for e in bot_state['fired_entries'] if not e.startswith('Short')]
                log(f'Bear OB changed -> cleared Short fired entries')

        if bull_changed or bear_changed:
            bot_state['engine_step'] = 'cancelling old orders'
            await cancel_open_orders(SYMBOL)
            if bull_changed:
                bot_state['active_orders'] = {k: v for k, v in bot_state['active_orders'].items() if not k.startswith('Short')}
            if bear_changed:
                bot_state['active_orders'] = {k: v for k, v in bot_state['active_orders'].items() if not k.startswith('Long')}

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
            placed = await place_entry_orders(SYMBOL, new_entries)
            log(f'Placed {len(placed)} entries: {placed}')
        else:
            log(f'No new entries to place (all already fired)')

        bot_state['engine_step'] = 'cycle complete'
        bot_state['last_cycle'] = datetime.now().isoformat()
        bot_state['cycle_count'] += 1
        log(f'Cycle #{bot_state["cycle_count"]} complete')

    except Exception as e:
        import traceback
        err_msg = f'{datetime.now().isoformat()} - {str(e)}'
        bot_state['errors'].append(err_msg)
        bot_state['engine_step'] = f'ERROR: {str(e)}'
        log(f'Engine cycle error: {e}')
        log(traceback.format_exc())

async def engine_loop():
    """Main engine loop running in background thread."""
    bot_state['running'] = True
    log('Smart OB Ladder Engine started')
    log(f'Symbol={SYMBOL} Balance={ACCOUNT_BALANCE} Risk={RISK_PER_TRADE_PCT}% Interval={POLL_INTERVAL_SEC}s')
    log(f'Timeframes={TIMEFRAMES} SwingLength={SWING_LENGTH}')

    while bot_state['running']:
        try:
            await engine_cycle()
        except Exception as e:
            err_msg = f'{datetime.now().isoformat()} - {str(e)}'
            bot_state['errors'].append(err_msg)
            log(f'Engine cycle error: {e}')
        await asyncio.sleep(POLL_INTERVAL_SEC)

    log('Engine stopped')

def start_engine_thread():
    """Start the engine in a background thread with its own event loop."""
    def run_loop():
        try:
            log('Engine thread: creating event loop...')
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            log('Engine thread: event loop created, starting engine_loop...')
            loop.run_until_complete(engine_loop())
            loop.close()
        except Exception as e:
            err_msg = f'{datetime.now().isoformat()} - THREAD FATAL: {str(e)}'
            bot_state['errors'].append(err_msg)
            log(f'Engine thread FATAL error: {e}')

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    return thread

# ============================================================================
# FLASK ENDPOINTS (for monitoring)
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

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'engine_running': bot_state['running']}), 200

@app.route('/cancel', methods=['POST'])
def cancel_orders():
    symbol = request.json.get('symbol', SYMBOL)
    asyncio.run(cancel_open_orders(symbol))
    bot_state['fired_entries'].clear()
    bot_state['active_orders'].clear()
    log(f'Manual cancel: all orders for {symbol} cancelled, fired entries cleared')
    return jsonify({'status': 'success', 'message': f'Cancelled all orders for {symbol}'}), 200

@app.route('/reset', methods=['POST'])
def reset_engine():
    """Reset fired entries and active orders tracking without cancelling on exchange."""
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
            'timeframes': TIMEFRAMES,
            'num_entries_per_ob': NUM_ENTRIES,
            'api_key_set': API_KEY != 'YOUR_API_KEY_HERE',
            'testnet': True,
            'hedge_mode': True,
            'margin_mode': 'isolated',
        },
        'endpoints': {
            '/status': 'GET - Full bot status (price, OBs, orders, errors)',
            '/health': 'GET - Health check',
            '/cancel': 'POST - Cancel all open orders and clear tracking',
            '/reset': 'POST - Reset tracking state (no exchange cancel)',
        }
    })

# ============================================================================
# STARTUP - Engine starts on import (works with gunicorn)
# ============================================================================

# Start engine thread on module load (gunicorn imports the module, so this runs)
if API_KEY != 'YOUR_API_KEY_HERE':
    log('Starting Smart OB Ladder Engine thread...')
    start_engine_thread()
else:
    log('WARNING: API keys not set. Engine will not start. Configure BYBIT_TESTNET_API_KEY and BYBIT_TESTNET_API_SECRET.')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log(f'Starting Fabio OB Ladder Bot on port {port}')
    app.run(host='0.0.0.0', port=port, debug=False)
