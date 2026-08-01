"""
Fabio OB Ladder Strategy - Bybit Testnet Webhook Bot
Receives webhook alerts from TradingView and sends orders to Bybit testnet API.
Supports: limit orders, stop loss, take profit (3 levels), hedge mode.
"""

import os
import json
import asyncio
from datetime import datetime
from flask import Flask, request, jsonify
import ccxt.async_support as ccxt

app = Flask(__name__)

# ============================================================================
# CONFIGURATION - Set your Bybit testnet API keys here or via environment variables
# ============================================================================
API_KEY = os.environ.get('BYBIT_TESTNET_API_KEY', 'YOUR_API_KEY_HERE')
API_SECRET = os.environ.get('BYBIT_TESTNET_API_SECRET', 'YOUR_API_SECRET_HERE')
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'your_webhook_secret_here')

# Initialize Bybit testnet exchange
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

# Track active orders to avoid duplicates
active_orders = {}

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{timestamp}] {msg}')

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

async def cancel_existing_orders(symbol, side):
    """Cancel all existing open orders for the symbol and side."""
    try:
        open_orders = await exchange.fetch_open_orders(symbol)
        for order in open_orders:
            try:
                await exchange.cancel_order(order['id'], symbol)
                log(f'Cancelled order {order["id"]}')
            except Exception as e:
                log(f'Failed to cancel order {order["id"]}: {e}')
    except Exception as e:
        log(f'Error fetching open orders: {e}')

async def place_limit_order(symbol, side, price, qty, sl, tp1, tp2, tp3, entry_id):
    """Place a limit order with SL and 3 TP orders on Bybit testnet."""
    try:
        # Enable hedge mode
        await setup_hedge_mode(symbol)

        # Determine buy/sell
        order_side = 'buy' if side.lower() == 'long' else 'sell'
        close_side = 'sell' if order_side == 'buy' else 'buy'
        position_idx = 1 if order_side == 'buy' else 2

        # Ensure minimum quantity for BTCUSDT (0.01) or other symbols
        min_qty = 0.01
        qty = max(float(qty), min_qty)
        tp1_qty = round(qty * 0.3333, 4)
        tp2_qty = round(qty * 0.3333, 4)
        tp3_qty = round(qty - tp1_qty - tp2_qty, 4)

        # ---- 1. Place main limit order ----
        log(f'Placing {order_side.upper()} limit order: {symbol} qty={qty} price={price}')
        main_order = await exchange.create_order(
            symbol=symbol,
            type='limit',
            side=order_side,
            amount=qty,
            price=float(price),
            params={
                'positionIdx': position_idx,
                'timeInForce': 'GTC',
            }
        )
        order_id = main_order['id']
        log(f'Limit order placed: ID={order_id}')

        # ---- 2. Place Stop Loss order (trigger order) ----
        log(f'Placing SL order: trigger price={sl}')
        try:
            sl_order = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=qty,
                params={
                    'stopPrice': float(sl),
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'trigger': 'last_price',
                    'triggerDirection': 'descending',
                    'tpMode': 'partial',
                    'slOrderType': 'market',
                }
            )
            sl_order_id = sl_order['id']
            log(f'SL order placed: ID={sl_order_id}')
        except Exception as e:
            log(f'SL order error (non-fatal): {e}')
            sl_order_id = None

        # ---- 3. Place TP1 (33% of position) ----
        log(f'Placing TP1 order: price={tp1} qty={tp1_qty}')
        try:
            tp1_order = await exchange.create_order(
                symbol=symbol,
                type='limit',
                side=close_side,
                amount=tp1_qty,
                price=float(tp1),
                params={
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'timeInForce': 'GTC',
                    'trigger': 'last_price',
                    'triggerDirection': 'ascending',
                }
            )
            tp1_order_id = tp1_order['id']
            log(f'TP1 order placed: ID={tp1_order_id}')
        except Exception as e:
            log(f'TP1 order error (non-fatal): {e}')
            tp1_order_id = None

        # ---- 4. Place TP2 (33% of position) ----
        log(f'Placing TP2 order: price={tp2} qty={tp2_qty}')
        try:
            tp2_order = await exchange.create_order(
                symbol=symbol,
                type='limit',
                side=close_side,
                amount=tp2_qty,
                price=float(tp2),
                params={
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'timeInForce': 'GTC',
                    'trigger': 'last_price',
                    'triggerDirection': 'ascending',
                }
            )
            tp2_order_id = tp2_order['id']
            log(f'TP2 order placed: ID={tp2_order_id}')
        except Exception as e:
            log(f'TP2 order error (non-fatal): {e}')
            tp2_order_id = None

        # ---- 5. Place TP3 (remaining ~34% of position) ----
        log(f'Placing TP3 order: price={tp3} qty={tp3_qty}')
        try:
            tp3_order = await exchange.create_order(
                symbol=symbol,
                type='limit',
                side=close_side,
                amount=tp3_qty,
                price=float(tp3),
                params={
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'timeInForce': 'GTC',
                    'trigger': 'last_price',
                    'triggerDirection': 'ascending',
                }
            )
            tp3_order_id = tp3_order['id']
            log(f'TP3 order placed: ID={tp3_order_id}')
        except Exception as e:
            log(f'TP3 order error (non-fatal): {e}')
            tp3_order_id = None

        # Track the order
        active_orders[entry_id] = {
            'main_order_id': order_id,
            'sl_order_id': sl_order_id,
            'tp1_order_id': tp1_order_id,
            'tp2_order_id': tp2_order_id,
            'tp3_order_id': tp3_order_id,
            'symbol': symbol,
            'side': order_side,
            'price': price,
            'qty': qty,
            'timestamp': datetime.now().isoformat(),
        }

        return True, 'Orders placed successfully'

    except Exception as e:
        log(f'ERROR placing orders: {e}')
        return False, str(e)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Receive webhook from TradingView and place orders on Bybit testnet."""
    try:
        # Get the webhook data
        data = request.get_data(as_text=True)
        log(f'Received webhook: {data}')

        # Parse JSON
        alert = json.loads(data)

        # Validate webhook secret (optional - uncomment to enable)
        # if alert.get('secret') != WEBHOOK_SECRET:
        #     log('Invalid webhook secret')
        #     return jsonify({'error': 'Invalid secret'}), 403

        # Extract order details
        action = alert.get('action', '')
        symbol = alert.get('symbol', 'BTCUSDT')
        direction = alert.get('direction', '')
        price = float(alert.get('price', 0))
        size = float(alert.get('size', 0))
        sl = float(alert.get('sl', 0))
        tp1 = float(alert.get('tp1', 0))
        tp2 = float(alert.get('tp2', 0))
        tp3 = float(alert.get('tp3', 0))
        entry_id = alert.get('entry_id', '')

        log(f'Parsed: action={action} symbol={symbol} dir={direction} price={price} size={size} SL={sl} TP1={tp1} TP2={tp2} TP3={tp3} id={entry_id}')

        # Check if already processed
        if entry_id in active_orders:
            log(f'Order {entry_id} already active, skipping')
            return jsonify({'status': 'duplicate', 'message': 'Order already placed'}), 200

        # Place orders asynchronously
        success, message = asyncio.run(
            place_limit_order(symbol, direction, price, size, sl, tp1, tp2, tp3, entry_id)
        )

        if success:
            log(f'SUCCESS: {message}')
            return jsonify({'status': 'success', 'message': message, 'entry_id': entry_id}), 200
        else:
            log(f'FAILED: {message}')
            return jsonify({'status': 'error', 'message': message}), 500

    except json.JSONDecodeError as e:
        log(f'JSON parse error: {e}')
        return jsonify({'error': 'Invalid JSON'}), 400
    except Exception as e:
        log(f'Webhook error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/cancel', methods=['POST'])
def cancel_orders():
    """Cancel all open orders for a symbol."""
    try:
        symbol = request.json.get('symbol', 'BTCUSDT')
        asyncio.run(cancel_existing_orders(symbol, ''))
        # Clear active_orders tracking
        cleared = len(active_orders)
        active_orders.clear()
        log(f'Cancelled all orders for {symbol}, cleared {cleared} tracked entries')
        return jsonify({'status': 'success', 'message': f'Cancelled all orders for {symbol}'}), 200
    except Exception as e:
        log(f'Cancel error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def status():
    """Check bot status and active orders."""
    return jsonify({
        'status': 'running',
        'active_orders': len(active_orders),
        'orders': active_orders,
        'api_key_set': API_KEY != 'YOUR_API_KEY_HERE',
        'timestamp': datetime.now().isoformat(),
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'}), 200

@app.route('/', methods=['GET'])
def home():
    """Home endpoint with instructions."""
    return jsonify({
        'name': 'Fabio OB Ladder - Bybit Testnet Webhook Bot',
        'endpoints': {
            '/webhook': 'POST - Receive TradingView webhook alerts',
            '/status': 'GET - Check bot status and active orders',
            '/cancel': 'POST - Cancel all open orders (body: {"symbol": "BTCUSDT"})',
            '/health': 'GET - Health check',
        },
        'config': {
            'api_key_set': API_KEY != 'YOUR_API_KEY_HERE',
            'testnet': True,
            'hedge_mode': True,
            'margin_mode': 'isolated',
        }
    })

if __name__ == '__main__':
    # Use PORT environment variable for cloud deployment (Render, Railway, etc.)
    port = int(os.environ.get('PORT', 5000))
    log(f'Starting Fabio OB Ladder Bot on port {port}')
    log(f'API Key configured: {API_KEY != "YOUR_API_KEY_HERE"}')
    app.run(host='0.0.0.0', port=port, debug=False)
