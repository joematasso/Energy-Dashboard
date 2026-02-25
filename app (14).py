#!/usr/bin/env python3
"""
Energy Desk v3.0 — Backend Server
Flask + SQLite (WAL) + WebSocket (flask-socketio)
"""

import os
import sys
import json
import time
import random
import string
import sqlite3
import hashlib
import csv
import io
import logging
import math
from datetime import datetime, timedelta
from functools import wraps
from threading import Lock

import requests
import feedparser
from flask import Flask, request, jsonify, send_from_directory, Response, g
from flask_socketio import SocketIO, emit

# ---------------------------------------------------------------------------
# App Setup
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder='.', static_url_path='')
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'energydesk-v3-secret')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'energydesk.db')
EIA_API_KEY = os.environ.get('EIA_API_KEY', '')

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Caches
news_cache = {}
news_cache_lock = Lock()
NEWS_CACHE_TTL = 900  # 15 minutes

eia_cache = {}
eia_cache_lock = Lock()
EIA_CACHE_TTL = 3600  # 1 hour

# Active connections
active_connections = set()
connections_lock = Lock()

# ---------------------------------------------------------------------------
# Database Helpers
# ---------------------------------------------------------------------------
def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def get_db_standalone():
    """Get database connection outside of request context."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    """Initialize database schema."""
    conn = get_db_standalone()
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT DEFAULT '',
            color TEXT DEFAULT '#22d3ee',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS traders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_name TEXT UNIQUE NOT NULL,
            real_name TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL,
            firm TEXT DEFAULT '',
            pin TEXT NOT NULL,
            team_id INTEGER,
            status TEXT DEFAULT 'PENDING',
            starting_balance REAL DEFAULT 1000000,
            photo_url TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id)
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_name TEXT NOT NULL,
            trade_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (trader_name) REFERENCES traders(trader_name)
        );

        CREATE TABLE IF NOT EXISTS pins (
            pin TEXT PRIMARY KEY,
            status TEXT DEFAULT 'AVAILABLE',
            claimed_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS performance_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_name TEXT NOT NULL,
            snapshot_date DATE NOT NULL,
            equity REAL,
            realized_pnl REAL,
            unrealized_pnl REAL,
            trade_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS admin_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            type TEXT NOT NULL,
            team_id INTEGER,
            avatar TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS conversation_members (
            conversation_id INTEGER NOT NULL,
            trader_name TEXT NOT NULL,
            last_read TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (conversation_id, trader_name)
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS trade_feed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_name TEXT NOT NULL,
            action TEXT NOT NULL,
            summary TEXT NOT NULL,
            team_name TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS message_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            trader_name TEXT NOT NULL,
            emoji TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(message_id, trader_name, emoji)
        );

        CREATE TABLE IF NOT EXISTS pinned_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            pinned_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(conversation_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS admin_broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'normal',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Insert default admin PIN if not exists
    cur.execute("INSERT OR IGNORE INTO admin_config (key, value) VALUES ('admin_pin', 'admin123')")
    cur.execute("INSERT OR IGNORE INTO admin_config (key, value) VALUES ('censored_words', '[]')")

    # Migration: add real_name column if it doesn't exist
    try:
        cur.execute("SELECT real_name FROM traders LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE traders ADD COLUMN real_name TEXT NOT NULL DEFAULT ''")
        cur.execute("UPDATE traders SET real_name = display_name WHERE real_name = ''")

    # Migration: add otc_available column
    try:
        cur.execute("SELECT otc_available FROM traders LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE traders ADD COLUMN otc_available INTEGER DEFAULT 1")

    # Migration: add avatar column to conversations
    try:
        cur.execute("SELECT avatar FROM conversations LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE conversations ADD COLUMN avatar TEXT DEFAULT ''")

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")

# ---------------------------------------------------------------------------
# Auth Helpers
# ---------------------------------------------------------------------------
def verify_admin_pin(pin):
    """Verify admin PIN against database."""
    db = get_db()
    row = db.execute("SELECT value FROM admin_config WHERE key='admin_pin'").fetchone()
    if row and row['value'] == pin:
        return True
    return False

def admin_required(f):
    """Decorator to require admin PIN in X-Admin-Pin header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        pin = request.headers.get('X-Admin-Pin', '')
        if not verify_admin_pin(pin):
            return jsonify({'success': False, 'error': 'Invalid admin PIN'}), 403
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# Static File Routes
# ---------------------------------------------------------------------------
@app.route('/')
def serve_index():
    return send_from_directory('.', 'index.html')

@app.route('/admin')
def serve_admin():
    return send_from_directory('.', 'admin.html')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory('.', 'manifest.json')

@app.route('/icon.svg')
def serve_icon():
    return send_from_directory('.', 'icon.svg')

# ---------------------------------------------------------------------------
# Public API Endpoints
# ---------------------------------------------------------------------------
@app.route('/api/status')
def api_status():
    db = get_db()
    active = db.execute("SELECT COUNT(*) as c FROM traders WHERE status='ACTIVE'").fetchone()['c']
    with connections_lock:
        ws_count = len(active_connections)
    return jsonify({
        'success': True,
        'status': 'online',
        'active_traders': active,
        'connected_clients': ws_count,
        'server_time': datetime.utcnow().isoformat(),
        'version': '3.0'
    })

@app.route('/api/traders/register', methods=['POST'])
def register_trader():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    real_name = (data.get('real_name') or data.get('display_name') or '').strip()
    display_name = (data.get('display_name') or real_name).strip()
    firm = (data.get('firm') or '').strip()
    pin = (data.get('pin') or '').strip()

    if not real_name:
        return jsonify({'success': False, 'error': 'Name is required'}), 400
    if not pin or len(pin) != 4 or not pin.isdigit():
        return jsonify({'success': False, 'error': 'A valid 4-digit PIN is required'}), 400

    # Generate trader_name from real_name
    trader_name = real_name.lower().replace(' ', '_')

    db = get_db()

    # Check if PIN exists and is available
    pin_row = db.execute("SELECT * FROM pins WHERE pin=?", (pin,)).fetchone()
    if pin_row:
        if pin_row['status'] != 'AVAILABLE':
            return jsonify({'success': False, 'error': 'PIN already claimed or disabled'}), 400
    # If no PINs exist in the system, allow registration without PIN validation
    total_pins = db.execute("SELECT COUNT(*) as c FROM pins").fetchone()['c']
    if total_pins > 0 and not pin_row:
        return jsonify({'success': False, 'error': 'Invalid PIN'}), 400

    # Check if trader already exists
    existing = db.execute("SELECT * FROM traders WHERE trader_name=?", (trader_name,)).fetchone()
    if existing:
        return jsonify({'success': False, 'error': 'Trader name already taken'}), 400

    try:
        db.execute(
            "INSERT INTO traders (trader_name, real_name, display_name, firm, pin, status) VALUES (?, ?, ?, ?, ?, 'ACTIVE')",
            (trader_name, real_name, display_name, firm, pin)
        )
        if pin_row:
            db.execute("UPDATE pins SET status='CLAIMED', claimed_by=? WHERE pin=?", (trader_name, pin))
        db.commit()

        socketio.emit('trader_registered', {
            'trader_name': trader_name,
            'display_name': display_name,
            'real_name': real_name,
            'firm': firm
        })

        return jsonify({
            'success': True,
            'trader_name': trader_name,
            'display_name': display_name,
            'real_name': real_name,
            'status': 'ACTIVE'
        })
    except sqlite3.IntegrityError as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/traders/login', methods=['POST'])
def login_trader():
    """Login with real name and PIN. Admin must have created the account first."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    name = (data.get('name') or '').strip()
    pin = (data.get('pin') or '').strip()

    if not name:
        return jsonify({'success': False, 'error': 'Name is required'}), 400
    if not pin or len(pin) != 4 or not pin.isdigit():
        return jsonify({'success': False, 'error': 'A valid 4-digit PIN is required'}), 400

    db = get_db()

    # Match by real_name (case-insensitive) and PIN
    trader = db.execute(
        "SELECT * FROM traders WHERE LOWER(real_name)=LOWER(?) AND pin=?",
        (name, pin)
    ).fetchone()

    if not trader:
        return jsonify({'success': False, 'error': 'Invalid name or PIN. Contact your admin for access.'}), 401

    if trader['status'] == 'DISABLED':
        return jsonify({'success': False, 'error': 'Your account has been disabled. Contact your admin.'}), 403

    # Update last_seen
    db.execute("UPDATE traders SET last_seen=CURRENT_TIMESTAMP WHERE id=?", (trader['id'],))
    if trader['status'] == 'PENDING':
        db.execute("UPDATE traders SET status='ACTIVE' WHERE id=?", (trader['id'],))
    db.commit()

    # Get team info
    team_info = None
    if trader['team_id']:
        team = db.execute("SELECT name, color FROM teams WHERE id=?", (trader['team_id'],)).fetchone()
        if team:
            team_info = {'name': team['name'], 'color': team['color']}

    return jsonify({
        'success': True,
        'trader_name': trader['trader_name'],
        'real_name': (trader['real_name'] if 'real_name' in trader.keys() else trader['display_name']),
        'display_name': trader['display_name'],
        'firm': trader['firm'],
        'status': 'ACTIVE',
        'starting_balance': trader['starting_balance'],
        'photo_url': trader['photo_url'],
        'team': team_info
    })

@app.route('/api/traders/heartbeat/<trader>', methods=['POST'])
def trader_heartbeat(trader):
    db = get_db()
    db.execute("UPDATE traders SET last_seen=CURRENT_TIMESTAMP WHERE trader_name=?", (trader,))
    db.commit()
    return jsonify({'success': True})


@app.route('/api/traders/display-name/<trader>', methods=['POST'])
def update_display_name(trader):
    """Let a trader update their own display name."""
    data = request.get_json()
    new_name = (data.get('display_name') or '').strip()
    if not new_name:
        return jsonify({'success': False, 'error': 'Display name cannot be empty'}), 400
    if len(new_name) > 30:
        return jsonify({'success': False, 'error': 'Display name must be 30 characters or less'}), 400

    db = get_db()
    db.execute("UPDATE traders SET display_name=? WHERE trader_name=?", (new_name, trader))
    db.commit()
    return jsonify({'success': True, 'display_name': new_name})

@app.route('/api/trades/<trader>', methods=['GET'])
def get_trades(trader):
    db = get_db()
    rows = db.execute(
        "SELECT id, trade_data, created_at FROM trades WHERE trader_name=? ORDER BY created_at DESC",
        (trader,)
    ).fetchall()
    trades = []
    for row in rows:
        td = json.loads(row['trade_data'])
        td['id'] = row['id']
        td['server_created_at'] = row['created_at']
        trades.append(td)
    return jsonify({'success': True, 'trades': trades})

@app.route('/api/trades/<trader>', methods=['POST'])
def submit_trade(trader):
    """Submit a trade with server-side validation."""
    db = get_db()

    # 1. Validate trader status
    trader_row = db.execute("SELECT * FROM traders WHERE trader_name=?", (trader,)).fetchone()
    if not trader_row:
        return jsonify({'success': False, 'error': 'Trader not found'}), 404
    if trader_row['status'] != 'ACTIVE':
        return jsonify({'success': False, 'error': f'Trader status is {trader_row["status"]}. Must be ACTIVE to trade.'}), 403

    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No trade data provided'}), 400

    # 2. Validate required fields
    required = ['type', 'direction', 'hub', 'volume', 'entryPrice']
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({'success': False, 'error': f'Missing required fields: {", ".join(missing)}'}), 400

    # 3. Volume limits
    volume = float(data.get('volume', 0))
    trade_type = data.get('type', '')
    is_crude = trade_type.startswith('CRUDE') or trade_type in ('EFP', 'OPTION_CL')
    max_volume = 50000 if is_crude else 500000
    unit = 'BBL' if is_crude else 'MMBtu'
    if volume <= 0:
        return jsonify({'success': False, 'error': 'Volume must be positive'}), 400
    if volume > max_volume:
        return jsonify({'success': False, 'error': f'Volume exceeds maximum of {max_volume:,.0f} {unit}'}), 400

    # Validate trade type against commodity sector
    SECTOR_TRADE_TYPES = {
        'ng': {'PHYS_FIXED', 'PHYS_INDEX', 'BASIS_SWAP', 'FIXED_FLOAT', 'SPREAD', 'BALMO', 'OPTION_NG', 'TAS', 'MULTILEG'},
        'crude': {'CRUDE_PHYS', 'CRUDE_SWAP', 'CRUDE_DIFF', 'OPTION_CL', 'EFP', 'TAS'},
        'power': {'PHYS_FIXED', 'PHYS_INDEX', 'FIXED_FLOAT', 'SPREAD', 'BALMO', 'TAS'},
        'freight': {'FREIGHT_FFA', 'FREIGHT_PHYS'},
        'ag': {'AG_FUTURES', 'AG_OPTIONS', 'AG_SPREAD'},
        'metals': {'METALS_FUTURES', 'METALS_OPTIONS', 'METALS_SPREAD'},
    }
    hub_name = data.get('hub', '')
    trade_sector = data.get('sector', '')
    # Infer sector from trade type if not provided
    if not trade_sector:
        if is_crude:
            trade_sector = 'crude'
        elif trade_type.startswith('FREIGHT'):
            trade_sector = 'freight'
        elif trade_type.startswith('AG'):
            trade_sector = 'ag'
        elif trade_type.startswith('METALS'):
            trade_sector = 'metals'
        elif trade_type in ('OPTION_NG', 'BASIS_SWAP'):
            trade_sector = 'ng'
    if trade_sector and trade_sector in SECTOR_TRADE_TYPES:
        allowed = SECTOR_TRADE_TYPES[trade_sector]
        if trade_type not in allowed:
            return jsonify({'success': False, 'error': f'Trade type {trade_type} is not valid for {trade_sector} sector'}), 400

    # 4. Price validation
    entry_price = float(data.get('entryPrice', 0))
    is_basis = trade_type == 'BASIS_SWAP'
    if not is_basis and entry_price <= 0:
        return jsonify({'success': False, 'error': 'Entry price must be positive'}), 400

    # Validate price vs direction: BUY >= spot, SELL <= spot (skip for basis trades)
    spot_ref = float(data.get('spotRef', entry_price))
    direction = data.get('direction', '')
    if not is_basis:
        if direction == 'BUY' and entry_price < spot_ref * 0.999:
            return jsonify({'success': False, 'error': 'BUY price must be at or above spot'}), 400
        if direction == 'SELL' and entry_price > spot_ref * 1.001:
            return jsonify({'success': False, 'error': 'SELL price must be at or below spot'}), 400

    starting_balance = trader_row['starting_balance']
    existing_trades = db.execute(
        "SELECT trade_data FROM trades WHERE trader_name=?", (trader,)
    ).fetchall()

    used_margin = 0
    realized_pnl = 0
    for row in existing_trades:
        td = json.loads(row['trade_data'])
        if td.get('status') == 'CLOSED':
            realized_pnl += float(td.get('realizedPnl', 0))
        elif td.get('status') == 'OPEN':
            used_margin += _calc_margin(td)

    new_margin = _calc_margin(data)
    equity = starting_balance + realized_pnl
    buying_power = equity - used_margin
    if new_margin > buying_power:
        return jsonify({
            'success': False,
            'error': f'Insufficient buying power. Required: ${new_margin:,.0f}, Available: ${buying_power:,.0f}'
        }), 400

    # 5. Duplicate prevention (same trade within 5 seconds)
    recent = db.execute(
        "SELECT trade_data FROM trades WHERE trader_name=? AND created_at > datetime('now', '-5 seconds')",
        (trader,)
    ).fetchall()
    for row in recent:
        td = json.loads(row['trade_data'])
        if (td.get('type') == data.get('type') and
            td.get('direction') == data.get('direction') and
            td.get('hub') == data.get('hub') and
            abs(float(td.get('volume', 0)) - volume) / max(volume, 1) < 0.05 and
            abs(float(td.get('entryPrice', 0)) - entry_price) / max(abs(entry_price), 0.01) < 0.02):
            return jsonify({'success': False, 'error': 'Duplicate trade detected (within 5 seconds)'}), 400

    # Store trade
    data['status'] = 'OPEN'
    data['timestamp'] = datetime.utcnow().isoformat()
    trade_json = json.dumps(data)

    cur = db.execute(
        "INSERT INTO trades (trader_name, trade_data) VALUES (?, ?)",
        (trader, trade_json)
    )
    db.commit()
    trade_id = cur.lastrowid

    db.execute("UPDATE traders SET last_seen=CURRENT_TIMESTAMP WHERE trader_name=?", (trader,))
    db.commit()

    socketio.emit('trade_submitted', {
        'trader_name': trader,
        'trade_id': trade_id,
        'type': data.get('type'),
        'direction': data.get('direction'),
        'hub': data.get('hub'),
        'volume': volume
    })
    socketio.emit('leaderboard_update', {'reason': 'trade_submitted'})

    # Log to trade feed
    try:
        me_row = db.execute("SELECT t.display_name, tm.name as team_name FROM traders t LEFT JOIN teams tm ON t.team_id=tm.id WHERE t.trader_name=?", (trader,)).fetchone()
        feed_sum = f"{me_row['display_name']} {data.get('direction')} {volume:,.0f} {data.get('hub','')} @ ${entry_price:.4f}"
        db.execute("INSERT INTO trade_feed (trader_name, action, summary, team_name) VALUES (?,?,?,?)",
                   (trader, 'TRADE', feed_sum, me_row['team_name'] or ''))
        db.commit()
        socketio.emit('trade_feed_update', {'summary': feed_sum})
    except Exception:
        pass

    return jsonify({'success': True, 'trade_id': trade_id})

@app.route('/api/trades/<trader>/<int:trade_id>', methods=['PUT'])
def update_trade(trader, trade_id):
    """Close a trade (or update trade data)."""
    db = get_db()
    row = db.execute("SELECT * FROM trades WHERE id=? AND trader_name=?", (trade_id, trader)).fetchone()
    if not row:
        return jsonify({'success': False, 'error': 'Trade not found'}), 404

    data = request.get_json()
    td = json.loads(row['trade_data'])

    # Validate close price if closing a non-OTC trade
    if data.get('status') == 'CLOSED' and td.get('venue') != 'OTC':
        close_price = float(data.get('closePrice', 0))
        spot_ref = float(data.get('spotRef', 0)) or float(td.get('spotRef', 0))
        if spot_ref > 0:
            # Allow close price within 5% of spot reference (generous for sim, prevents abuse)
            deviation = abs(close_price - spot_ref) / spot_ref
            if deviation > 0.05:
                return jsonify({'success': False, 'error': f'Close price ${close_price:.4f} deviates too far from market ${spot_ref:.4f}'}), 400

    td.update(data)
    db.execute("UPDATE trades SET trade_data=? WHERE id=?", (json.dumps(td), trade_id))
    db.commit()

    if data.get('status') == 'CLOSED':
        socketio.emit('trade_closed', {'trader_name': trader, 'trade_id': trade_id})
        socketio.emit('leaderboard_update', {'reason': 'trade_closed'})

    return jsonify({'success': True, 'trade_id': trade_id})

@app.route('/api/trades/<trader>/<int:trade_id>', methods=['DELETE'])
def delete_trade(trader, trade_id):
    """Delete a trade (only within 1-hour window)."""
    db = get_db()
    row = db.execute("SELECT * FROM trades WHERE id=? AND trader_name=?", (trade_id, trader)).fetchone()
    if not row:
        return jsonify({'success': False, 'error': 'Trade not found'}), 404

    created = datetime.fromisoformat(row['created_at'])
    if datetime.utcnow() - created > timedelta(hours=1):
        return jsonify({'success': False, 'error': 'Trade can only be deleted within 1 hour of placement'}), 400

    db.execute("DELETE FROM trades WHERE id=?", (trade_id,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/traders/photo/<trader>', methods=['POST'])
def upload_photo(trader):
    """Upload headshot photo (base64)."""
    data = request.get_json()
    photo = data.get('photo', '')
    if not photo:
        return jsonify({'success': False, 'error': 'No photo data'}), 400
    db = get_db()
    db.execute("UPDATE traders SET photo_url=? WHERE trader_name=?", (photo, trader))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/traders/photo/<trader>', methods=['GET'])
def get_photo(trader):
    db = get_db()
    row = db.execute("SELECT photo_url FROM traders WHERE trader_name=?", (trader,)).fetchone()
    if row:
        return jsonify({'success': True, 'photo': row['photo_url']})
    return jsonify({'success': False, 'error': 'Trader not found'}), 404

# ---------------------------------------------------------------------------
# Leaderboard API
# ---------------------------------------------------------------------------
@app.route('/api/leaderboard')
def get_leaderboard():
    """Server-calculated leaderboard."""
    db = get_db()
    traders = db.execute("SELECT * FROM traders WHERE status='ACTIVE'").fetchall()
    results = []
    # Accept current prices from client (optional query param)
    prices_json = request.args.get('prices', '{}')
    try:
        client_prices = json.loads(prices_json) if prices_json else {}
    except (json.JSONDecodeError, TypeError):
        client_prices = {}

    for t in traders:
        trades = db.execute("SELECT trade_data FROM trades WHERE trader_name=?", (t['trader_name'],)).fetchall()
        realized = 0
        unrealized = 0
        wins = 0
        losses = 0
        gross_wins = 0
        gross_losses = 0
        trade_count = len(trades)
        for row in trades:
            td = json.loads(row['trade_data'])
            if td.get('status') == 'CLOSED':
                pnl = float(td.get('realizedPnl', 0))
                realized += pnl
                if pnl > 0:
                    wins += 1
                    gross_wins += pnl
                elif pnl < 0:
                    losses += 1
                    gross_losses += abs(pnl)
            elif td.get('status') == 'OPEN':
                # Calculate unrealized P&L using client-provided prices or spotRef as fallback
                hub = td.get('hub', '')
                ep = float(td.get('entryPrice', 0))
                vol = float(td.get('volume', 0))
                direction = td.get('direction', '')
                is_basis_trade = td.get('type') == 'BASIS_SWAP'

                current_price = float(client_prices.get(hub, 0))
                if not current_price:
                    current_price = float(td.get('spotRef', ep))

                if is_basis_trade:
                    # For basis trades, use differential change
                    basis_ref = float(td.get('spotRef', ep))
                    diff_change = current_price - ep
                    trade_pnl = diff_change * vol if direction == 'BUY' else -diff_change * vol
                else:
                    d = 1 if direction == 'BUY' else -1
                    trade_pnl = (current_price - ep) * vol * d
                unrealized += trade_pnl

        equity = t['starting_balance'] + realized + unrealized
        ret = ((equity - t['starting_balance']) / t['starting_balance']) * 100 if t['starting_balance'] else 0
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
        pf = (gross_wins / gross_losses) if gross_losses > 0 else (999 if gross_wins > 0 else 0)

        team_info = None
        if t['team_id']:
            team = db.execute("SELECT name, color FROM teams WHERE id=?", (t['team_id'],)).fetchone()
            if team:
                team_info = {'name': team['name'], 'color': team['color']}

        results.append({
            'trader_name': t['trader_name'],
            'real_name': (t['real_name'] if 'real_name' in t.keys() else t['display_name']),
            'display_name': t['display_name'],
            'firm': t['firm'],
            'photo_url': t['photo_url'],
            'team': team_info,
            'equity': equity,
            'starting_balance': t['starting_balance'],
            'realized_pnl': realized,
            'unrealized_pnl': unrealized,
            'return_pct': round(ret, 2),
            'win_rate': round(win_rate, 1),
            'profit_factor': round(pf, 2),
            'trade_count': trade_count,
            'wins': wins,
            'losses': losses,
            'last_seen': t['last_seen']
        })

    results.sort(key=lambda x: x['return_pct'], reverse=True)
    for i, r in enumerate(results):
        r['rank'] = i + 1

    return jsonify({'success': True, 'leaderboard': results})

@app.route('/api/leaderboard/snapshots/<trader>')
def get_snapshots(trader):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM performance_snapshots WHERE trader_name=? ORDER BY snapshot_date ASC",
        (trader,)
    ).fetchall()
    snapshots = [dict(row) for row in rows]
    return jsonify({'success': True, 'snapshots': snapshots})

# ---------------------------------------------------------------------------
# News Proxy
# ---------------------------------------------------------------------------
@app.route('/api/news/<commodity>')
def get_news(commodity):
    with news_cache_lock:
        cached = news_cache.get(commodity)
        if cached and time.time() - cached['ts'] < NEWS_CACHE_TTL:
            return jsonify({'success': True, 'articles': cached['data']})

    # Per-commodity RSS feeds and keyword filters
    feed_config = {
        'ng': {
            'feeds': [
                ('https://oilprice.com/rss/main', 'OilPrice'),
                ('https://www.rigzone.com/news/rss/rigzone_latest.aspx', 'Rigzone'),
            ],
            'keywords': ['natural gas', 'lng', 'storage', 'henry hub', 'pipeline', 'gas storage',
                         'gas export', 'gas demand', 'gas production', 'gas price', 'mcf', 'bcf',
                         'marcellus', 'permian gas', 'freeport lng', 'sabine', 'cheniere']
        },
        'crude': {
            'feeds': [
                ('https://oilprice.com/rss/main', 'OilPrice'),
                ('https://www.rigzone.com/news/rss/rigzone_latest.aspx', 'Rigzone'),
            ],
            'keywords': ['crude', 'oil', 'opec', 'barrel', 'wti', 'brent', 'petroleum',
                         'refinery', 'gasoline', 'diesel', 'cushing', 'bakken', 'shale',
                         'oil price', 'oil production', 'oil demand', 'drilling', 'rig count']
        },
        'power': {
            'feeds': [
                ('https://www.utilitydive.com/feeds/news/', 'UtilityDive'),
                ('https://oilprice.com/rss/main', 'OilPrice'),
            ],
            'keywords': ['power', 'electric', 'grid', 'renewable', 'ercot', 'pjm', 'solar',
                         'wind', 'utility', 'generation', 'caiso', 'nuclear', 'battery',
                         'capacity', 'megawatt', 'blackout', 'transmission', 'energy storage']
        },
        'freight': {
            'feeds': [
                ('https://gcaptain.com/feed/', 'gCaptain'),
                ('https://www.hellenicshippingnews.com/feed/', 'Hellenic Shipping'),
            ],
            'keywords': ['shipping', 'freight', 'tanker', 'baltic', 'vessel', 'vlcc', 'cargo',
                         'maritime', 'bulk', 'container', 'charter', 'tonnage', 'port',
                         'suezmax', 'panamax', 'capesize', 'lng carrier', 'dry bulk']
        },
        'ag': {
            'feeds': [
                ('https://www.agweb.com/rss/news', 'AgWeb'),
                ('https://www.feedstuffs.com/rss.xml', 'Feedstuffs'),
            ],
            'keywords': ['corn', 'soybean', 'wheat', 'grain', 'crop', 'usda', 'cattle',
                         'hog', 'livestock', 'cotton', 'sugar', 'coffee', 'cocoa', 'harvest',
                         'planting', 'drought', 'yield', 'agriculture', 'farm', 'ethanol']
        },
        'metals': {
            'feeds': [
                ('https://www.kitco.com/rss/feed.xml', 'Kitco'),
                ('https://www.mining.com/feed/', 'Mining.com'),
            ],
            'keywords': ['gold', 'silver', 'copper', 'platinum', 'palladium', 'aluminum',
                         'nickel', 'iron ore', 'steel', 'zinc', 'metal', 'mining', 'bullion',
                         'comex', 'lme', 'precious', 'base metal', 'ore']
        }
    }

    config = feed_config.get(commodity, feed_config.get('crude'))
    articles = []

    for feed_url, source_name in config['feeds']:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:30]:
                title = entry.get('title', '').lower()
                summary = entry.get('summary', '').lower()
                combined = title + ' ' + summary
                if any(kw in combined for kw in config['keywords']):
                    # Parse time
                    pub = entry.get('published', entry.get('updated', ''))
                    try:
                        from email.utils import parsedate_to_datetime
                        dt = parsedate_to_datetime(pub)
                        age = datetime.now(dt.tzinfo) - dt if dt.tzinfo else datetime.utcnow() - dt
                        hrs = int(age.total_seconds() / 3600)
                        if hrs < 1:
                            time_label = f"{int(age.total_seconds()/60)}m ago"
                        elif hrs < 24:
                            time_label = f"{hrs}h ago"
                        else:
                            time_label = f"{hrs//24}d ago"
                    except Exception:
                        time_label = pub[:16] if pub else ''

                    articles.append({
                        'source': source_name,
                        'headline': entry.get('title', ''),
                        'description': entry.get('summary', '')[:200].replace('<p>', '').replace('</p>', ''),
                        'time': time_label,
                        'url': entry.get('link', '')
                    })
                if len(articles) >= 15:
                    break
        except Exception as e:
            logger.warning(f"RSS fetch failed for {source_name} ({commodity}): {e}")
            continue

    with news_cache_lock:
        news_cache[commodity] = {'data': articles, 'ts': time.time()}
    return jsonify({'success': True, 'articles': articles})

# ---------------------------------------------------------------------------
# EIA Proxy
# ---------------------------------------------------------------------------
@app.route('/api/eia-debug')
def eia_debug():
    """Debug endpoint — hit /api/eia-debug in your browser to check EIA status"""
    info = {
        'api_key_set': bool(EIA_API_KEY),
        'api_key_prefix': EIA_API_KEY[:6] + '...' if len(EIA_API_KEY) > 6 else '(empty)',
        'cache_entries': list(eia_cache.keys()),
        'test_result': None
    }
    if EIA_API_KEY:
        try:
            test_url = f"https://api.eia.gov/v2/natural-gas/stor/wkly/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&facets[process][]=SAT&sort[0][column]=period&sort[0][direction]=desc&length=2"
            resp = requests.get(test_url, timeout=15)
            info['test_status_code'] = resp.status_code
            info['test_response_preview'] = resp.text[:500]
            data = resp.json()
            rows = data.get('response', {}).get('data', [])
            info['test_rows_returned'] = len(rows)
            if rows:
                info['test_first_row'] = rows[0]
            info['test_result'] = 'OK' if rows else 'No data rows in response'
        except Exception as e:
            info['test_result'] = f'Error: {str(e)}'
    else:
        info['test_result'] = 'EIA_API_KEY environment variable not set. Run: set EIA_API_KEY=your_key_here before starting app.py'
    return jsonify(info)

@app.route('/api/eia/<eia_type>')
def get_eia(eia_type):
    with eia_cache_lock:
        cached = eia_cache.get(eia_type)
        if cached and time.time() - cached['ts'] < EIA_CACHE_TTL:
            return jsonify({'success': True, 'data': cached['data']})

    if not EIA_API_KEY:
        return jsonify({'success': False, 'error': 'EIA_API_KEY not configured'})

    # EIA v2 API routes and series IDs
    routes = {
        'ng_storage': {
            'url': f"https://api.eia.gov/v2/natural-gas/stor/wkly/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&facets[process][]=SAT&sort[0][column]=period&sort[0][direction]=desc&length=52",
            'label': 'NG Weekly Storage'
        },
        'crude_inventory': {
            'url': f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[duoarea][]=NUS&sort[0][column]=period&sort[0][direction]=desc&length=52",
            'label': 'Crude Weekly Inventory'
        },
        'crude_cushing': {
            'url': f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[duoarea][]=R20&sort[0][column]=period&sort[0][direction]=desc&length=52",
            'label': 'Cushing Stocks'
        }
    }

    route = routes.get(eia_type)
    if not route:
        return jsonify({'success': False, 'error': 'Unknown EIA type'}), 400

    try:
        resp = requests.get(route['url'], timeout=15)
        raw = resp.json()
        # Parse v2 response format
        rows = raw.get('response', {}).get('data', [])
        parsed = []
        for row in rows[:52]:
            parsed.append({
                'period': row.get('period', ''),
                'value': row.get('value'),
            })
        result = {
            'type': eia_type,
            'label': route['label'],
            'data': parsed,
            'total': raw.get('response', {}).get('total', 0)
        }
        with eia_cache_lock:
            eia_cache[eia_type] = {'data': result, 'ts': time.time()}
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logger.error(f"EIA fetch error for {eia_type}: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ---------------------------------------------------------------------------
# CFTC Commitment of Traders Proxy
# ---------------------------------------------------------------------------
cot_cache = {}
cot_cache_lock = Lock()
COT_CACHE_TTL = 7200  # 2 hours

@app.route('/api/cot/<commodity>')
def get_cot(commodity):
    with cot_cache_lock:
        cached = cot_cache.get(commodity)
        if cached and time.time() - cached['ts'] < COT_CACHE_TTL:
            return jsonify({'success': True, 'data': cached['data']})

    # CFTC contract codes
    contract_map = {
        'ng': '023651',       # Natural Gas (NYMEX)
        'crude': '067651',    # Crude Oil (NYMEX)
    }
    code = contract_map.get(commodity)
    if not code:
        return jsonify({'success': False, 'error': 'Unknown commodity'}), 400

    try:
        # CFTC Disaggregated Futures — Socrata Open Data API (no key needed)
        url = (
            f"https://publicreporting.cftc.gov/resource/72hh-3qpy.json?"
            f"$where=cftc_contract_market_code='{code}'"
            f"&$order=report_date_as_yyyy_mm_dd DESC"
            f"&$limit=12"
        )
        resp = requests.get(url, timeout=15)
        rows = resp.json()
        parsed = []
        for row in rows:
            parsed.append({
                'date': row.get('report_date_as_yyyy_mm_dd', ''),
                'market': row.get('market_and_exchange_names', ''),
                'oi': _safe_int(row.get('open_interest_all')),
                'prod_long': _safe_int(row.get('prod_merc_positions_long_all')),
                'prod_short': _safe_int(row.get('prod_merc_positions_short_all')),
                'swap_long': _safe_int(row.get('swap_positions_long_all')),
                'swap_short': _safe_int(row.get('swap__positions_short_all')),
                'mm_long': _safe_int(row.get('money_manager_positions_long')),
                'mm_short': _safe_int(row.get('money_manager_positions_short')),
                'other_long': _safe_int(row.get('other_rept_positions_long_all')),
                'other_short': _safe_int(row.get('other_rept_positions_short_all')),
                'nonrept_long': _safe_int(row.get('nonrept_positions_long_all')),
                'nonrept_short': _safe_int(row.get('nonrept_positions_short_all')),
            })
        result = {'commodity': commodity, 'data': parsed}
        with cot_cache_lock:
            cot_cache[commodity] = {'data': result, 'ts': time.time()}
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logger.error(f"CFTC COT fetch error for {commodity}: {e}")
        return jsonify({'success': False, 'error': str(e)})

def _safe_int(val):
    try:
        return int(val) if val else 0
    except (ValueError, TypeError):
        return 0

# ---------------------------------------------------------------------------
# Admin API Endpoints
# ---------------------------------------------------------------------------
@app.route('/api/admin/traders', methods=['GET'])
@admin_required
def admin_list_traders():
    db = get_db()
    traders = db.execute("""
        SELECT t.*, tm.name as team_name, tm.color as team_color,
               (SELECT COUNT(*) FROM trades WHERE trader_name=t.trader_name) as trade_count
        FROM traders t
        LEFT JOIN teams tm ON t.team_id = tm.id
        ORDER BY t.created_at DESC
    """).fetchall()

    results = []
    for t in traders:
        trades = db.execute("SELECT trade_data FROM trades WHERE trader_name=?", (t['trader_name'],)).fetchall()
        realized = sum(float(json.loads(r['trade_data']).get('realizedPnl', 0))
                       for r in trades if json.loads(r['trade_data']).get('status') == 'CLOSED')

        results.append({
            'id': t['id'],
            'trader_name': t['trader_name'],
            'real_name': t['real_name'] if 'real_name' in t.keys() else t['display_name'],
            'display_name': t['display_name'],
            'firm': t['firm'],
            'pin': t['pin'],
            'status': t['status'],
            'team_id': t['team_id'],
            'team_name': t['team_name'],
            'team_color': t['team_color'],
            'starting_balance': t['starting_balance'],
            'trade_count': t['trade_count'],
            'realized_pnl': realized,
            'photo_url': t['photo_url'],
            'created_at': t['created_at'],
            'last_seen': t['last_seen']
        })

    return jsonify({'success': True, 'traders': results})

@app.route('/api/admin/traders/approve/<int:tid>', methods=['POST'])
@admin_required
def admin_approve_trader(tid):
    db = get_db()
    db.execute("UPDATE traders SET status='ACTIVE' WHERE id=?", (tid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/traders/disable/<int:tid>', methods=['POST'])
@admin_required
def admin_disable_trader(tid):
    db = get_db()
    db.execute("UPDATE traders SET status='DISABLED' WHERE id=?", (tid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/traders/enable/<int:tid>', methods=['POST'])
@admin_required
def admin_enable_trader(tid):
    db = get_db()
    db.execute("UPDATE traders SET status='ACTIVE' WHERE id=?", (tid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/traders/reset/<int:tid>', methods=['POST'])
@admin_required
def admin_reset_individual(tid):
    """Reset a single trader's trades."""
    db = get_db()
    trader = db.execute("SELECT trader_name FROM traders WHERE id=?", (tid,)).fetchone()
    if not trader:
        return jsonify({'success': False, 'error': 'Trader not found'}), 404
    db.execute("DELETE FROM trades WHERE trader_name=?", (trader['trader_name'],))
    db.execute("DELETE FROM performance_snapshots WHERE trader_name=?", (trader['trader_name'],))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/traders/<int:tid>', methods=['DELETE'])
@admin_required
def admin_delete_trader(tid):
    db = get_db()
    trader = db.execute("SELECT trader_name FROM traders WHERE id=?", (tid,)).fetchone()
    if trader:
        db.execute("DELETE FROM trades WHERE trader_name=?", (trader['trader_name'],))
        db.execute("DELETE FROM performance_snapshots WHERE trader_name=?", (trader['trader_name'],))
    db.execute("DELETE FROM traders WHERE id=?", (tid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/traders/balance/<int:tid>', methods=['POST'])
@admin_required
def admin_set_balance(tid):
    data = request.get_json()
    balance = float(data.get('starting_balance', 1000000))
    db = get_db()
    db.execute("UPDATE traders SET starting_balance=? WHERE id=?", (balance, tid))
    db.commit()
    return jsonify({'success': True})


@app.route('/api/admin/traders/pin/<int:tid>', methods=['POST'])
@admin_required
def admin_change_trader_pin(tid):
    data = request.get_json()
    new_pin = data.get('pin', '').strip()
    if not new_pin or len(new_pin) != 4 or not new_pin.isdigit():
        return jsonify({'success': False, 'error': 'PIN must be exactly 4 digits'}), 400
    db = get_db()
    trader = db.execute("SELECT * FROM traders WHERE id=?", (tid,)).fetchone()
    if not trader:
        return jsonify({'success': False, 'error': 'Trader not found'}), 404
    db.execute("UPDATE traders SET pin=? WHERE id=?", (new_pin, tid))
    db.commit()
    return jsonify({'success': True, 'display_name': trader['display_name']})

# ---------------------------------------------------------------------------
# Admin Teams
# ---------------------------------------------------------------------------
@app.route('/api/admin/teams', methods=['GET'])
@admin_required
def admin_list_teams():
    db = get_db()
    teams = db.execute("SELECT * FROM teams ORDER BY name").fetchall()
    results = []
    for t in teams:
        members = db.execute(
            "SELECT id, trader_name, display_name, firm, status FROM traders WHERE team_id=?",
            (t['id'],)
        ).fetchall()
        results.append({
            'id': t['id'],
            'name': t['name'],
            'description': t['description'],
            'color': t['color'],
            'members': [dict(m) for m in members],
            'member_count': len(members)
        })
    return jsonify({'success': True, 'teams': results})

@app.route('/api/admin/teams', methods=['POST'])
@admin_required
def admin_create_team():
    data = request.get_json()
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Team name is required'}), 400
    db = get_db()
    try:
        cur = db.execute(
            "INSERT INTO teams (name, description, color) VALUES (?, ?, ?)",
            (name, data.get('description', ''), data.get('color', '#22d3ee'))
        )
        db.commit()
        return jsonify({'success': True, 'team_id': cur.lastrowid})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Team name already exists'}), 400

@app.route('/api/admin/teams/<int:tid>', methods=['PUT'])
@admin_required
def admin_update_team(tid):
    data = request.get_json()
    db = get_db()
    db.execute(
        "UPDATE teams SET name=?, description=?, color=? WHERE id=?",
        (data.get('name', ''), data.get('description', ''), data.get('color', '#22d3ee'), tid)
    )
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/teams/<int:tid>', methods=['DELETE'])
@admin_required
def admin_delete_team(tid):
    db = get_db()
    db.execute("UPDATE traders SET team_id=NULL WHERE team_id=?", (tid,))
    db.execute("DELETE FROM teams WHERE id=?", (tid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/teams/<int:tid>/assign', methods=['POST'])
@admin_required
def admin_assign_to_team(tid):
    data = request.get_json()
    trader_id = data.get('trader_id')
    db = get_db()
    db.execute("UPDATE traders SET team_id=? WHERE id=?", (tid, trader_id))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/teams/<int:tid>/remove', methods=['POST'])
@admin_required
def admin_remove_from_team(tid):
    data = request.get_json()
    trader_id = data.get('trader_id')
    db = get_db()
    db.execute("UPDATE traders SET team_id=NULL WHERE id=? AND team_id=?", (trader_id, tid))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/teams/transfer', methods=['POST'])
@admin_required
def admin_transfer_trader():
    """Transfer trader between teams."""
    data = request.get_json()
    trader_id = data.get('trader_id')
    to_team_id = data.get('to_team_id')
    db = get_db()
    db.execute("UPDATE traders SET team_id=? WHERE id=?", (to_team_id, trader_id))
    db.commit()
    return jsonify({'success': True})

# ---------------------------------------------------------------------------
# Admin PINs
# ---------------------------------------------------------------------------
@app.route('/api/admin/pins', methods=['GET'])
@admin_required
def admin_list_pins():
    db = get_db()
    pins = db.execute("SELECT * FROM pins ORDER BY created_at DESC").fetchall()
    results = [dict(p) for p in pins]
    return jsonify({'success': True, 'pins': results})

@app.route('/api/admin/pins/generate', methods=['POST'])
@admin_required
def admin_generate_pins():
    data = request.get_json()
    quantity = min(int(data.get('quantity', 10)), 50)
    db = get_db()
    generated = []
    for _ in range(quantity):
        while True:
            pin = ''.join(random.choices(string.digits, k=4))
            existing = db.execute("SELECT pin FROM pins WHERE pin=?", (pin,)).fetchone()
            if not existing:
                break
        db.execute("INSERT INTO pins (pin) VALUES (?)", (pin,))
        generated.append(pin)
    db.commit()
    return jsonify({'success': True, 'pins': generated, 'count': len(generated)})

@app.route('/api/admin/pins/revoke', methods=['POST'])
@admin_required
def admin_revoke_pin():
    data = request.get_json()
    pin = data.get('pin')
    db = get_db()
    db.execute("UPDATE pins SET status='DISABLED' WHERE pin=?", (pin,))
    db.commit()
    return jsonify({'success': True})

# ---------------------------------------------------------------------------
# Admin System
# ---------------------------------------------------------------------------
@app.route('/api/admin/reset-all', methods=['POST'])
@admin_required
def admin_reset_all():
    db = get_db()
    db.execute("DELETE FROM trades")
    db.execute("DELETE FROM performance_snapshots")
    db.commit()
    socketio.emit('leaderboard_update', {'reason': 'reset_all'})
    return jsonify({'success': True})

@app.route('/api/admin/export', methods=['GET'])
@admin_required
def admin_export():
    db = get_db()
    rows = db.execute(
        "SELECT t.trader_name, t.trade_data, t.created_at FROM trades t ORDER BY t.created_at DESC"
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Trader', 'Type', 'Direction', 'Hub', 'Volume', 'Entry Price', 'Status',
                     'Realized P&L', 'Close Price', 'Notes', 'Created At'])
    for row in rows:
        td = json.loads(row['trade_data'])
        writer.writerow([
            row['trader_name'],
            td.get('type', ''),
            td.get('direction', ''),
            td.get('hub', ''),
            td.get('volume', ''),
            td.get('entryPrice', ''),
            td.get('status', ''),
            td.get('realizedPnl', ''),
            td.get('closePrice', ''),
            td.get('notes', ''),
            row['created_at']
        ])

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=energy_desk_trades.csv'}
    )

@app.route('/api/admin/change-pin', methods=['POST'])
@admin_required
def admin_change_pin():
    """Change admin PIN."""
    data = request.get_json()
    new_pin = (data.get('new_pin') or '').strip()
    confirm_pin = (data.get('confirm_pin') or '').strip()
    if not new_pin:
        return jsonify({'success': False, 'error': 'New PIN is required'}), 400
    if new_pin != confirm_pin:
        return jsonify({'success': False, 'error': 'PINs do not match'}), 400
    if len(new_pin) < 4:
        return jsonify({'success': False, 'error': 'PIN must be at least 4 characters'}), 400

    db = get_db()
    db.execute("UPDATE admin_config SET value=? WHERE key='admin_pin'", (new_pin,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/admin/config', methods=['GET'])
@admin_required
def admin_get_config():
    db = get_db()
    rows = db.execute("SELECT * FROM admin_config").fetchall()
    config = {}
    for r in rows:
        if r['key'] == 'admin_pin':
            config['admin_pin'] = '****'
        else:
            config[r['key']] = r['value']
    config['eia_api_key'] = '****' if EIA_API_KEY else 'NOT SET'
    config['database'] = DATABASE
    config['news_cache_ttl'] = NEWS_CACHE_TTL
    return jsonify({'success': True, 'config': config})


# ---------------------------------------------------------------------------
# Censored Words (Admin)
# ---------------------------------------------------------------------------
@app.route('/api/admin/censored-words', methods=['GET'])
@admin_required
def get_censored_words():
    db = get_db()
    row = db.execute("SELECT value FROM admin_config WHERE key='censored_words'").fetchone()
    words = json.loads(row['value']) if row else []
    return jsonify({'success': True, 'words': words})

@app.route('/api/admin/censored-words', methods=['POST'])
@admin_required
def set_censored_words():
    db = get_db()
    data = request.get_json()
    words = data.get('words', [])
    # Normalize: lowercase, strip whitespace
    words = [w.strip().lower() for w in words if w.strip()]
    db.execute("INSERT OR REPLACE INTO admin_config (key, value) VALUES ('censored_words', ?)",
               (json.dumps(words),))
    db.commit()
    return jsonify({'success': True, 'words': words, 'count': len(words)})


def censor_text(text):
    """Replace censored words/phrases with asterisks."""
    db = get_db()
    row = db.execute("SELECT value FROM admin_config WHERE key='censored_words'").fetchone()
    if not row:
        return text
    words = json.loads(row['value'])
    if not words:
        return text
    result = text
    for word in words:
        if not word:
            continue
        # Case-insensitive replacement
        import re as _re
        pattern = _re.compile(_re.escape(word), _re.IGNORECASE)
        replacement = '*' * len(word)
        result = pattern.sub(replacement, result)
    return result


# ---------------------------------------------------------------------------
# Admin Broadcasts
# ---------------------------------------------------------------------------
@app.route('/api/admin/broadcasts', methods=['GET'])
@admin_required
def get_broadcasts():
    db = get_db()
    rows = db.execute("SELECT * FROM admin_broadcasts ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify({'success': True, 'broadcasts': [dict(r) for r in rows]})

@app.route('/api/admin/broadcast', methods=['POST'])
@admin_required
def send_broadcast():
    db = get_db()
    data = request.get_json()
    subject = data.get('subject', '').strip()
    body = data.get('body', '').strip()
    priority = data.get('priority', 'normal')
    if not body:
        return jsonify({'success': False, 'error': 'Message body required'}), 400
    if priority not in ('normal', 'urgent'):
        priority = 'normal'
    cur = db.execute("INSERT INTO admin_broadcasts (subject, body, priority) VALUES (?, ?, ?)",
                     (subject, body, priority))
    db.commit()
    broadcast_id = cur.lastrowid
    # Emit to all connected traders via socket
    socketio.emit('admin_broadcast', {
        'id': broadcast_id,
        'subject': subject,
        'body': body,
        'priority': priority,
        'created_at': datetime.utcnow().isoformat()
    })
    return jsonify({'success': True, 'id': broadcast_id})

@app.route('/api/admin/broadcasts/<int:bid>', methods=['DELETE'])
@admin_required
def delete_broadcast(bid):
    db = get_db()
    db.execute("DELETE FROM admin_broadcasts WHERE id=?", (bid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/broadcasts', methods=['GET'])
def get_trader_broadcasts():
    """Public endpoint for traders to fetch recent broadcasts."""
    db = get_db()
    limit = int(request.args.get('limit', 20))
    rows = db.execute("SELECT * FROM admin_broadcasts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return jsonify({'success': True, 'broadcasts': [dict(r) for r in rows]})

# ---------------------------------------------------------------------------
# Censored Word Individual Delete
# ---------------------------------------------------------------------------
@app.route('/api/admin/censored-words/<path:word>', methods=['DELETE'])
@admin_required
def delete_censored_word(word):
    db = get_db()
    row = db.execute("SELECT value FROM admin_config WHERE key='censored_words'").fetchone()
    words = json.loads(row['value']) if row else []
    target = word.strip().lower()
    words = [w for w in words if w != target]
    db.execute("INSERT OR REPLACE INTO admin_config (key, value) VALUES ('censored_words', ?)",
               (json.dumps(words),))
    db.commit()
    return jsonify({'success': True, 'words': words, 'count': len(words)})


# ---------------------------------------------------------------------------
# Chat Rename
# ---------------------------------------------------------------------------
@app.route('/api/chat/conversations/<int:conv_id>/rename', methods=['POST'])
def rename_conversation(conv_id):
    db = get_db()
    data = request.get_json()
    trader = data.get('trader', '')
    new_name = data.get('name', '').strip()
    if not new_name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    if len(new_name) > 50:
        return jsonify({'success': False, 'error': 'Name too long (max 50 chars)'}), 400
    # Verify membership
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?",
                        (conv_id, trader)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    # Only allow renaming group conversations
    conv = db.execute("SELECT * FROM conversations WHERE id=?", (conv_id,)).fetchone()
    if not conv or conv['type'] not in ('group',):
        return jsonify({'success': False, 'error': 'Can only rename group chats'}), 400
    db.execute("UPDATE conversations SET name=? WHERE id=?", (new_name, conv_id))
    db.commit()
    return jsonify({'success': True, 'name': new_name})


@app.route('/api/chat/conversations/<int:conv_id>/avatar', methods=['POST'])
def set_conversation_avatar(conv_id):
    """Upload and resize a group chat avatar to ~128x128, store as base64."""
    from PIL import Image
    import io, base64
    db = get_db()
    trader = request.form.get('trader', '')
    if not trader:
        return jsonify({'success': False, 'error': 'Trader required'}), 400
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?",
                        (conv_id, trader)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    conv = db.execute("SELECT * FROM conversations WHERE id=?", (conv_id,)).fetchone()
    if not conv or conv['type'] not in ('group', 'team'):
        return jsonify({'success': False, 'error': 'Avatars only for group/team chats'}), 400

    if 'image' not in request.files:
        return jsonify({'success': False, 'error': 'No image file provided'}), 400
    file = request.files['image']
    if not file.filename:
        return jsonify({'success': False, 'error': 'Empty file'}), 400

    try:
        img = Image.open(file.stream)
        img = img.convert('RGB')
        # Crop to square from center, then resize
        w, h = img.size
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
        img = img.resize((128, 128), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=80)
        b64 = base64.b64encode(buf.getvalue()).decode('ascii')
        avatar_data = f"data:image/jpeg;base64,{b64}"
    except Exception as e:
        return jsonify({'success': False, 'error': f'Image processing failed: {str(e)}'}), 400

    db.execute("UPDATE conversations SET avatar=? WHERE id=?", (avatar_data, conv_id))
    db.commit()
    return jsonify({'success': True, 'avatar': avatar_data})


@app.route('/api/chat/conversations/<int:conv_id>/members', methods=['GET'])
def get_conversation_members(conv_id):
    db = get_db()
    members = db.execute("""
        SELECT t.trader_name, t.display_name, tm.name as team_name, tm.color as team_color
        FROM conversation_members cm
        JOIN traders t ON cm.trader_name=t.trader_name
        LEFT JOIN teams tm ON t.team_id=tm.id
        WHERE cm.conversation_id=?
    """, (conv_id,)).fetchall()
    return jsonify({'success': True, 'members': [{
        'trader_name': m['trader_name'], 'display_name': m['display_name'],
        'team_name': m['team_name'] or '', 'team_color': m['team_color'] or '#888'
    } for m in members]})


@app.route('/api/chat/conversations/<int:conv_id>/members', methods=['POST'])
def add_conversation_members(conv_id):
    db = get_db()
    data = request.get_json()
    trader = data.get('trader', '')
    new_members = data.get('members', [])
    if not new_members:
        return jsonify({'success': False, 'error': 'No members specified'}), 400
    # Verify requester is a member
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?",
                        (conv_id, trader)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    # Only allow adding to group conversations
    conv = db.execute("SELECT * FROM conversations WHERE id=?", (conv_id,)).fetchone()
    if not conv or conv['type'] != 'group':
        return jsonify({'success': False, 'error': 'Can only add members to group chats'}), 400
    added = []
    for m in new_members:
        t = db.execute("SELECT trader_name FROM traders WHERE trader_name=? AND status='ACTIVE'", (m,)).fetchone()
        if t:
            db.execute("INSERT OR IGNORE INTO conversation_members (conversation_id, trader_name) VALUES (?, ?)",
                       (conv_id, m))
            added.append(m)
    db.commit()
    return jsonify({'success': True, 'added': added, 'count': len(added)})

# ---------------------------------------------------------------------------
# NYMEX Market Hours & Holidays
# ---------------------------------------------------------------------------
NYMEX_HOLIDAYS = {
    # 2025
    '2025-01-01','2025-01-20','2025-02-17','2025-04-18','2025-05-26',
    '2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    # 2026
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25',
    '2026-07-03','2026-09-07','2026-11-26','2026-12-25',
    # 2027
    '2027-01-01','2027-01-18','2027-02-15','2027-03-26','2027-05-31',
    '2027-07-05','2027-09-06','2027-11-25','2027-12-24',
    # 2028
    '2028-01-01','2028-01-17','2028-02-21','2028-04-14','2028-05-29',
    '2028-07-04','2028-09-04','2028-11-23','2028-12-25',
    # 2029
    '2029-01-01','2029-01-15','2029-02-19','2029-03-30','2029-05-28',
    '2029-07-04','2029-09-03','2029-11-22','2029-12-25',
    # 2030
    '2030-01-01','2030-01-21','2030-02-18','2030-04-19','2030-05-27',
    '2030-07-04','2030-09-02','2030-11-28','2030-12-25',
}

def is_market_open():
    """Check if market is open. Business hours: 8AM-5PM CT, Mon-Fri. Closed holidays."""
    try:
        import pytz
        ct = datetime.now(pytz.timezone('US/Central'))
    except Exception:
        ct = datetime.utcnow() - timedelta(hours=6)
    date_str = ct.strftime('%Y-%m-%d')
    if date_str in NYMEX_HOLIDAYS:
        return False, 'Holiday', ct
    dow = ct.weekday()
    t = ct.hour * 60 + ct.minute
    if dow >= 5:  # Saturday=5, Sunday=6
        return False, 'Weekend', ct
    if t < 8*60:  # Before 8AM CT
        return False, 'Pre-Market', ct
    if t >= 17*60:  # After 5PM CT
        return False, 'After Hours', ct
    return True, 'Open', ct


@app.route('/api/market-status')
def market_status():
    is_open, reason, ct = is_market_open()
    return jsonify({
        'open': is_open, 'reason': reason,
        'ct_time': ct.strftime('%H:%M:%S'), 'ct_date': ct.strftime('%Y-%m-%d'),
        'ct_dow': ct.strftime('%A'), 'holidays': sorted(NYMEX_HOLIDAYS)
    })


# ---------------------------------------------------------------------------
# OTC System
# ---------------------------------------------------------------------------
@app.route('/api/traders/otc-status/<trader>', methods=['GET'])
def get_otc_status(trader):
    db = get_db()
    row = db.execute("SELECT otc_available FROM traders WHERE trader_name=?", (trader,)).fetchone()
    if not row:
        return jsonify({'success': False, 'error': 'Not found'}), 404
    return jsonify({'success': True, 'otc_available': bool(row['otc_available'])})

@app.route('/api/traders/otc-status/<trader>', methods=['POST'])
def set_otc_status(trader):
    db = get_db()
    data = request.get_json()
    val = 1 if data.get('otc_available', True) else 0
    db.execute("UPDATE traders SET otc_available=? WHERE trader_name=?", (val, trader))
    db.commit()
    return jsonify({'success': True, 'otc_available': bool(val)})

@app.route('/api/traders/otc-counterparties/<trader>', methods=['GET'])
def get_otc_counterparties(trader):
    db = get_db()
    me = db.execute("SELECT * FROM traders WHERE trader_name=?", (trader,)).fetchone()
    if not me:
        return jsonify({'success': False, 'error': 'Not found'}), 404
    my_team_id = me['team_id']
    if my_team_id:
        rows = db.execute("""
            SELECT t.trader_name, t.display_name, t.real_name, t.firm, t.otc_available,
                   tm.name as team_name, tm.color as team_color
            FROM traders t LEFT JOIN teams tm ON t.team_id=tm.id
            WHERE t.status='ACTIVE' AND t.trader_name != ? AND (t.team_id IS NULL OR t.team_id != ?)
        """, (trader, my_team_id)).fetchall()
    else:
        rows = db.execute("""
            SELECT t.trader_name, t.display_name, t.real_name, t.firm, t.otc_available,
                   tm.name as team_name, tm.color as team_color
            FROM traders t LEFT JOIN teams tm ON t.team_id=tm.id
            WHERE t.status='ACTIVE' AND t.trader_name != ?
        """, (trader,)).fetchall()
    return jsonify({'success': True, 'counterparties': [{
        'trader_name': r['trader_name'], 'display_name': r['display_name'],
        'real_name': r['real_name'] if 'real_name' in r.keys() else r['display_name'],
        'firm': r['firm'], 'otc_available': bool(r['otc_available']),
        'team_name': r['team_name'] or '', 'team_color': r['team_color'] or '#888'
    } for r in rows]})

@app.route('/api/trades/otc/<trader>', methods=['POST'])
def submit_otc_trade(trader):
    db = get_db()
    data = request.get_json()
    me = db.execute("SELECT * FROM traders WHERE trader_name=? AND status='ACTIVE'", (trader,)).fetchone()
    if not me:
        return jsonify({'success': False, 'error': 'Trader not found'}), 404
    cpty_name = data.get('counterparty', '')
    cpty = db.execute("SELECT * FROM traders WHERE trader_name=? AND status='ACTIVE'", (cpty_name,)).fetchone()
    if not cpty:
        return jsonify({'success': False, 'error': 'Counterparty not found'}), 404
    if not cpty['otc_available']:
        return jsonify({'success': False, 'error': f'{cpty["display_name"]} is not accepting OTC trades'}), 400
    if me['team_id'] and cpty['team_id'] and me['team_id'] == cpty['team_id']:
        return jsonify({'success': False, 'error': 'OTC trades must be with a different team'}), 400

    entry_price = float(data.get('entryPrice', 0))
    volume = float(data.get('volume', 0))
    direction = data.get('direction', '')
    mirror_direction = 'SELL' if direction == 'BUY' else 'BUY'

    trade_data = {
        'type': data.get('type', 'SWAP'), 'direction': direction, 'hub': data.get('hub', ''),
        'volume': volume, 'entryPrice': entry_price, 'spotRef': float(data.get('spotRef', entry_price)),
        'venue': 'OTC', 'counterparty': cpty['display_name'], 'counterpartyTrader': cpty_name,
        'otcMirrorOf': None, 'deliveryMonth': data.get('deliveryMonth', ''),
        'notes': data.get('notes', ''), 'status': 'OPEN', 'timestamp': datetime.utcnow().isoformat(),
    }
    cur = db.execute("INSERT INTO trades (trader_name, trade_data) VALUES (?, ?)", (trader, json.dumps(trade_data)))
    db.commit()
    my_trade_id = cur.lastrowid

    mirror_data = dict(trade_data)
    mirror_data['direction'] = mirror_direction
    mirror_data['counterparty'] = me['display_name']
    mirror_data['counterpartyTrader'] = trader
    mirror_data['otcMirrorOf'] = my_trade_id
    mirror_data['notes'] = f'OTC mirror — initiated by {me["display_name"]}'
    cur2 = db.execute("INSERT INTO trades (trader_name, trade_data) VALUES (?, ?)", (cpty_name, json.dumps(mirror_data)))
    db.commit()
    mirror_id = cur2.lastrowid

    trade_data['otcMirrorOf'] = mirror_id
    db.execute("UPDATE trades SET trade_data=? WHERE id=?", (json.dumps(trade_data), my_trade_id))
    db.commit()

    # Trade feed
    me_team = db.execute("SELECT name FROM teams WHERE id=?", (me['team_id'],)).fetchone() if me['team_id'] else None
    feed_summary = f"{me['display_name']} {direction} {volume:,.0f} {data.get('hub','')} OTC w/ {cpty['display_name']} @ ${entry_price:.4f}"
    db.execute("INSERT INTO trade_feed (trader_name, action, summary, team_name) VALUES (?,?,?,?)",
               (trader, 'OTC_TRADE', feed_summary, me_team['name'] if me_team else ''))
    db.commit()

    socketio.emit('trade_submitted', {'trader_name': trader, 'trade_id': my_trade_id, 'otc': True})
    socketio.emit('trade_submitted', {'trader_name': cpty_name, 'trade_id': mirror_id, 'otc': True})
    socketio.emit('leaderboard_update', {'reason': 'otc_trade'})
    return jsonify({'success': True, 'trade_id': my_trade_id, 'mirror_id': mirror_id})


# ---- OTC Trade Close (auto-close mirror) ----
@app.route('/api/trades/otc-close/<trader>/<int:trade_id>', methods=['POST'])
def close_otc_trade(trader, trade_id):
    db = get_db()
    row = db.execute("SELECT * FROM trades WHERE id=? AND trader_name=?", (trade_id, trader)).fetchone()
    if not row:
        return jsonify({'success': False, 'error': 'Trade not found'}), 404
    td = json.loads(row['trade_data'])
    close_data = request.get_json()
    close_price = float(close_data.get('closePrice', 0))

    # Close initiator
    td['status'] = 'CLOSED'
    td['closePrice'] = close_price
    td['closeTimestamp'] = datetime.utcnow().isoformat()
    vol = float(td.get('volume', 0))
    ep = float(td.get('entryPrice', 0))
    is_basis_trade = td.get('type') == 'BASIS_SWAP'
    if is_basis_trade:
        # Basis P&L = change in differential × volume
        diff_change = close_price - ep
        pnl = diff_change * vol if td['direction'] == 'BUY' else -diff_change * vol
    else:
        pnl = (close_price - ep) * vol if td['direction'] == 'BUY' else (ep - close_price) * vol
    td['realizedPnl'] = pnl
    db.execute("UPDATE trades SET trade_data=? WHERE id=?", (json.dumps(td), trade_id))

    # Close mirror
    mirror_id = td.get('otcMirrorOf')
    if mirror_id:
        mrow = db.execute("SELECT * FROM trades WHERE id=?", (mirror_id,)).fetchone()
        if mrow:
            mtd = json.loads(mrow['trade_data'])
            mtd['status'] = 'CLOSED'
            mtd['closePrice'] = close_price
            mtd['closeTimestamp'] = datetime.utcnow().isoformat()
            m_is_basis = mtd.get('type') == 'BASIS_SWAP'
            if m_is_basis:
                m_diff_change = close_price - float(mtd['entryPrice'])
                mpnl = m_diff_change * float(mtd['volume']) if mtd['direction'] == 'BUY' else -m_diff_change * float(mtd['volume'])
            else:
                mpnl = (close_price - float(mtd['entryPrice'])) * float(mtd['volume']) if mtd['direction'] == 'BUY' else (float(mtd['entryPrice']) - close_price) * float(mtd['volume'])
            mtd['realizedPnl'] = mpnl
            db.execute("UPDATE trades SET trade_data=? WHERE id=?", (json.dumps(mtd), mirror_id))
            socketio.emit('trade_closed', {'trader_name': mrow['trader_name'], 'trade_id': mirror_id})

    db.commit()
    socketio.emit('trade_closed', {'trader_name': trader, 'trade_id': trade_id})
    socketio.emit('leaderboard_update', {'reason': 'otc_close'})
    return jsonify({'success': True})


# ---------------------------------------------------------------------------
# Trade Feed
# ---------------------------------------------------------------------------
@app.route('/api/trade-feed')
def get_trade_feed():
    db = get_db()
    rows = db.execute("SELECT * FROM trade_feed ORDER BY created_at DESC LIMIT 50").fetchall()
    return jsonify([{
        'id': r['id'], 'trader_name': r['trader_name'], 'action': r['action'],
        'summary': r['summary'], 'team_name': r['team_name'], 'created_at': r['created_at']
    } for r in rows])


# ---------------------------------------------------------------------------
# Chat System
# ---------------------------------------------------------------------------
@app.route('/api/chat/conversations/<trader>', methods=['GET'])
def get_conversations(trader):
    db = get_db()
    rows = db.execute("""
        SELECT c.id, c.name, c.type, c.team_id, c.avatar, cm.last_read,
            (SELECT COUNT(*) FROM messages m WHERE m.conversation_id=c.id AND m.created_at > cm.last_read AND m.sender != ?) as unread,
            (SELECT m.text FROM messages m WHERE m.conversation_id=c.id ORDER BY m.id DESC LIMIT 1) as last_msg,
            (SELECT m.sender FROM messages m WHERE m.conversation_id=c.id ORDER BY m.id DESC LIMIT 1) as last_sender,
            (SELECT m.created_at FROM messages m WHERE m.conversation_id=c.id ORDER BY m.id DESC LIMIT 1) as last_msg_time
        FROM conversations c
        JOIN conversation_members cm ON c.id=cm.conversation_id AND cm.trader_name=?
        ORDER BY last_msg_time DESC NULLS LAST
    """, (trader, trader)).fetchall()
    convos = []
    for r in rows:
        members = db.execute("""
            SELECT t.trader_name, t.display_name, tm.name as team_name, tm.color as team_color
            FROM conversation_members cm JOIN traders t ON cm.trader_name=t.trader_name
            LEFT JOIN teams tm ON t.team_id=tm.id WHERE cm.conversation_id=?
        """, (r['id'],)).fetchall()
        convos.append({
            'id': r['id'], 'name': r['name'], 'type': r['type'], 'team_id': r['team_id'],
            'avatar': r['avatar'] or '',
            'unread': r['unread'] or 0, 'last_msg': r['last_msg'], 'last_sender': r['last_sender'],
            'last_msg_time': r['last_msg_time'],
            'members': [{'trader_name': m['trader_name'], 'display_name': m['display_name'],
                         'team_name': m['team_name'] or '', 'team_color': m['team_color'] or '#888'} for m in members]
        })
    return jsonify({'success': True, 'conversations': convos})

@app.route('/api/chat/conversations', methods=['POST'])
def create_conversation():
    db = get_db()
    data = request.get_json()
    conv_type = data.get('type', 'dm')
    name = data.get('name', '')
    members = data.get('members', [])
    creator = data.get('creator', '')
    if not creator:
        return jsonify({'success': False, 'error': 'Creator required'}), 400
    if conv_type == 'dm' and len(members) == 1:
        other = members[0]
        existing = db.execute("""
            SELECT c.id FROM conversations c WHERE c.type='dm' AND
                EXISTS (SELECT 1 FROM conversation_members cm1 WHERE cm1.conversation_id=c.id AND cm1.trader_name=?) AND
                EXISTS (SELECT 1 FROM conversation_members cm2 WHERE cm2.conversation_id=c.id AND cm2.trader_name=?)
        """, (creator, other)).fetchone()
        if existing:
            return jsonify({'success': True, 'conversation_id': existing['id'], 'existing': True})
    cur = db.execute("INSERT INTO conversations (name, type) VALUES (?, ?)", (name, conv_type))
    conv_id = cur.lastrowid
    for m in set(members + [creator]):
        db.execute("INSERT OR IGNORE INTO conversation_members (conversation_id, trader_name) VALUES (?, ?)", (conv_id, m))
    db.commit()
    return jsonify({'success': True, 'conversation_id': conv_id})

@app.route('/api/chat/team-conversation/<trader>', methods=['POST'])
def ensure_team_conversation(trader):
    db = get_db()
    me = db.execute("SELECT * FROM traders WHERE trader_name=?", (trader,)).fetchone()
    if not me or not me['team_id']:
        return jsonify({'success': False, 'error': 'No team'}), 400
    team = db.execute("SELECT * FROM teams WHERE id=?", (me['team_id'],)).fetchone()
    existing = db.execute("SELECT id FROM conversations WHERE type='team' AND team_id=?", (me['team_id'],)).fetchone()
    if existing:
        db.execute("INSERT OR IGNORE INTO conversation_members (conversation_id, trader_name) VALUES (?, ?)", (existing['id'], trader))
        db.commit()
        return jsonify({'success': True, 'conversation_id': existing['id']})
    cur = db.execute("INSERT INTO conversations (name, type, team_id) VALUES (?, 'team', ?)", (team['name'], me['team_id']))
    conv_id = cur.lastrowid
    teammates = db.execute("SELECT trader_name FROM traders WHERE team_id=? AND status='ACTIVE'", (me['team_id'],)).fetchall()
    for t in teammates:
        db.execute("INSERT OR IGNORE INTO conversation_members (conversation_id, trader_name) VALUES (?, ?)", (conv_id, t['trader_name']))
    db.commit()
    return jsonify({'success': True, 'conversation_id': conv_id})

@app.route('/api/chat/messages/<int:conv_id>', methods=['GET'])
def get_messages(conv_id):
    db = get_db()
    trader = request.args.get('trader', '')
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?", (conv_id, trader)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    limit = int(request.args.get('limit', 100))
    rows = db.execute("""
        SELECT m.*, t.display_name, tm.name as team_name, tm.color as team_color
        FROM messages m JOIN traders t ON m.sender=t.trader_name
        LEFT JOIN teams tm ON t.team_id=tm.id
        WHERE m.conversation_id=? ORDER BY m.id DESC LIMIT ?
    """, (conv_id, limit)).fetchall()
    db.execute("UPDATE conversation_members SET last_read=CURRENT_TIMESTAMP WHERE conversation_id=? AND trader_name=?", (conv_id, trader))
    db.commit()

    # Fetch reactions and pins in batch for all returned messages
    msg_ids = [r['id'] for r in rows]
    reactions_map = {}
    pins_set = set()
    if msg_ids:
        placeholders = ','.join('?' * len(msg_ids))
        react_rows = db.execute(f"""
            SELECT message_id, emoji, GROUP_CONCAT(trader_name) as traders, COUNT(*) as count
            FROM message_reactions WHERE message_id IN ({placeholders}) GROUP BY message_id, emoji
        """, msg_ids).fetchall()
        for rr in react_rows:
            mid = rr['message_id']
            if mid not in reactions_map:
                reactions_map[mid] = []
            reactions_map[mid].append({'emoji': rr['emoji'], 'traders': rr['traders'].split(','), 'count': rr['count']})
        pin_rows = db.execute(f"SELECT message_id FROM pinned_messages WHERE conversation_id=? AND message_id IN ({placeholders})",
                              [conv_id] + msg_ids).fetchall()
        pins_set = {pr['message_id'] for pr in pin_rows}

    return jsonify({'success': True, 'messages': [{
        'id': r['id'], 'sender': r['sender'], 'display_name': r['display_name'],
        'team_name': r['team_name'] or '', 'team_color': r['team_color'] or '#888',
        'text': r['text'], 'created_at': r['created_at'],
        'reactions': reactions_map.get(r['id'], []),
        'pinned': r['id'] in pins_set
    } for r in reversed(rows)]})

@app.route('/api/chat/send/<int:conv_id>', methods=['POST'])
def send_message(conv_id):
    db = get_db()
    data = request.get_json()
    sender = data.get('sender', '')
    text = data.get('text', '').strip()
    if not text:
        return jsonify({'success': False, 'error': 'Empty message'}), 400
    if len(text) > 2000:
        return jsonify({'success': False, 'error': 'Too long'}), 400
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?", (conv_id, sender)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    # Apply word filter
    text = censor_text(text)
    cur = db.execute("INSERT INTO messages (conversation_id, sender, text) VALUES (?, ?, ?)", (conv_id, sender, text))
    db.commit()
    msg_id = cur.lastrowid
    db.execute("UPDATE conversation_members SET last_read=CURRENT_TIMESTAMP WHERE conversation_id=? AND trader_name=?", (conv_id, sender))
    db.commit()
    sender_info = db.execute("""
        SELECT t.display_name, tm.name as team_name, tm.color as team_color
        FROM traders t LEFT JOIN teams tm ON t.team_id=tm.id WHERE t.trader_name=?
    """, (sender,)).fetchone()
    socketio.emit('new_message', {
        'conversation_id': conv_id, 'id': msg_id, 'sender': sender,
        'display_name': sender_info['display_name'] if sender_info else sender,
        'team_name': sender_info['team_name'] if sender_info else '',
        'team_color': sender_info['team_color'] if sender_info else '#888',
        'text': text, 'created_at': datetime.utcnow().isoformat()
    })

    # Detect @mentions and emit notifications
    import re as _re
    mentions = _re.findall(r'@(\w[\w\s]*?)(?=\s|$|[.,!?])', text)
    if mentions:
        # Get all conversation members to match display names
        members = db.execute("""
            SELECT t.trader_name, t.display_name FROM conversation_members cm
            JOIN traders t ON cm.trader_name=t.trader_name
            WHERE cm.conversation_id=?
        """, (conv_id,)).fetchall()
        sender_display = sender_info['display_name'] if sender_info else sender
        for mention in mentions:
            mention_lower = mention.strip().lower()
            for member in members:
                if (member['display_name'].lower().startswith(mention_lower) or
                    member['trader_name'].lower() == mention_lower):
                    if member['trader_name'] != sender:
                        socketio.emit('mention_notification', {
                            'mentioned_trader': member['trader_name'],
                            'sender_display': sender_display,
                            'conversation_id': conv_id,
                            'text_preview': text[:100]
                        })

    return jsonify({'success': True, 'message_id': msg_id})

@app.route('/api/chat/mark-read/<int:conv_id>/<trader>', methods=['POST'])
def mark_read(conv_id, trader):
    db = get_db()
    db.execute("UPDATE conversation_members SET last_read=CURRENT_TIMESTAMP WHERE conversation_id=? AND trader_name=?", (conv_id, trader))
    db.commit()
    return jsonify({'success': True})


# ---------------------------------------------------------------------------
# Chat Reactions
# ---------------------------------------------------------------------------
@app.route('/api/chat/reactions/<int:message_id>', methods=['GET'])
def get_reactions(message_id):
    db = get_db()
    rows = db.execute("""
        SELECT emoji, GROUP_CONCAT(trader_name) as traders, COUNT(*) as count
        FROM message_reactions WHERE message_id=? GROUP BY emoji
    """, (message_id,)).fetchall()
    reactions = [{'emoji': r['emoji'], 'traders': r['traders'].split(','), 'count': r['count']} for r in rows]
    return jsonify({'success': True, 'reactions': reactions})


@app.route('/api/chat/reactions/<int:message_id>', methods=['POST'])
def toggle_reaction(message_id):
    db = get_db()
    data = request.get_json()
    trader = data.get('trader', '')
    emoji = data.get('emoji', '')
    if not trader or not emoji:
        return jsonify({'success': False, 'error': 'Trader and emoji required'}), 400
    # Check if reaction exists
    existing = db.execute(
        "SELECT id FROM message_reactions WHERE message_id=? AND trader_name=? AND emoji=?",
        (message_id, trader, emoji)
    ).fetchone()
    if existing:
        db.execute("DELETE FROM message_reactions WHERE id=?", (existing['id'],))
        action = 'removed'
    else:
        db.execute(
            "INSERT INTO message_reactions (message_id, trader_name, emoji) VALUES (?, ?, ?)",
            (message_id, trader, emoji)
        )
        action = 'added'
    db.commit()
    # Return updated reactions for this message
    rows = db.execute("""
        SELECT emoji, GROUP_CONCAT(trader_name) as traders, COUNT(*) as count
        FROM message_reactions WHERE message_id=? GROUP BY emoji
    """, (message_id,)).fetchall()
    reactions = [{'emoji': r['emoji'], 'traders': r['traders'].split(','), 'count': r['count']} for r in rows]
    # Broadcast reaction update
    socketio.emit('reaction_update', {'message_id': message_id, 'reactions': reactions})
    return jsonify({'success': True, 'action': action, 'reactions': reactions})


# Batch fetch reactions for multiple messages
@app.route('/api/chat/reactions-batch', methods=['POST'])
def get_reactions_batch():
    db = get_db()
    data = request.get_json()
    message_ids = data.get('message_ids', [])
    if not message_ids:
        return jsonify({'success': True, 'reactions': {}})
    placeholders = ','.join('?' * len(message_ids))
    rows = db.execute(f"""
        SELECT message_id, emoji, GROUP_CONCAT(trader_name) as traders, COUNT(*) as count
        FROM message_reactions WHERE message_id IN ({placeholders}) GROUP BY message_id, emoji
    """, message_ids).fetchall()
    result = {}
    for r in rows:
        mid = r['message_id']
        if mid not in result:
            result[mid] = []
        result[mid].append({'emoji': r['emoji'], 'traders': r['traders'].split(','), 'count': r['count']})
    return jsonify({'success': True, 'reactions': result})


# ---------------------------------------------------------------------------
# Chat Pinned Messages
# ---------------------------------------------------------------------------
@app.route('/api/chat/pins/<int:conv_id>', methods=['GET'])
def get_pinned_messages(conv_id):
    db = get_db()
    rows = db.execute("""
        SELECT p.message_id, p.pinned_by, p.created_at as pinned_at,
               m.text, m.sender, m.created_at as msg_time,
               t.display_name, tm.name as team_name, tm.color as team_color
        FROM pinned_messages p
        JOIN messages m ON p.message_id=m.id
        JOIN traders t ON m.sender=t.trader_name
        LEFT JOIN teams tm ON t.team_id=tm.id
        WHERE p.conversation_id=?
        ORDER BY p.created_at DESC
    """, (conv_id,)).fetchall()
    pins = [{
        'message_id': r['message_id'], 'pinned_by': r['pinned_by'],
        'pinned_at': r['pinned_at'], 'text': r['text'],
        'sender': r['sender'], 'display_name': r['display_name'],
        'team_name': r['team_name'] or '', 'team_color': r['team_color'] or '#888',
        'msg_time': r['msg_time']
    } for r in rows]
    return jsonify({'success': True, 'pins': pins})


@app.route('/api/chat/pins/<int:conv_id>/<int:message_id>', methods=['POST'])
def pin_message(conv_id, message_id):
    db = get_db()
    data = request.get_json()
    trader = data.get('trader', '')
    if not trader:
        return jsonify({'success': False, 'error': 'Trader required'}), 400
    # Verify membership
    member = db.execute("SELECT * FROM conversation_members WHERE conversation_id=? AND trader_name=?",
                        (conv_id, trader)).fetchone()
    if not member:
        return jsonify({'success': False, 'error': 'Not a member'}), 403
    # Toggle pin
    existing = db.execute("SELECT id FROM pinned_messages WHERE conversation_id=? AND message_id=?",
                          (conv_id, message_id)).fetchone()
    if existing:
        db.execute("DELETE FROM pinned_messages WHERE id=?", (existing['id'],))
        db.commit()
        socketio.emit('pin_update', {'conversation_id': conv_id, 'message_id': message_id, 'action': 'unpinned'})
        return jsonify({'success': True, 'action': 'unpinned'})
    else:
        # Limit to 25 pins per conversation
        count = db.execute("SELECT COUNT(*) as c FROM pinned_messages WHERE conversation_id=?", (conv_id,)).fetchone()['c']
        if count >= 25:
            return jsonify({'success': False, 'error': 'Maximum 25 pinned messages per conversation'}), 400
        db.execute("INSERT INTO pinned_messages (conversation_id, message_id, pinned_by) VALUES (?, ?, ?)",
                   (conv_id, message_id, trader))
        db.commit()
        socketio.emit('pin_update', {'conversation_id': conv_id, 'message_id': message_id, 'action': 'pinned'})
        return jsonify({'success': True, 'action': 'pinned'})


# ---------------------------------------------------------------------------
# WebSocket Events
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Weather Forecasts (Open-Meteo API + synthetic fallback)
# ---------------------------------------------------------------------------
import threading, time as _time

WEATHER_CITIES = [
    {'id': 'houston', 'name': 'Houston', 'state': 'TX', 'lat': 29.76, 'lon': -95.37,
     'hubs': ['Henry Hub', 'Waha', 'ERCOT Hub', 'ERCOT South'], 'normal_jan': 53, 'normal_jul': 95, 'sector': 'both'},
    {'id': 'chicago', 'name': 'Chicago', 'state': 'IL', 'lat': 41.88, 'lon': -87.63,
     'hubs': ['Chicago', 'MISO Illinois'], 'normal_jan': 26, 'normal_jul': 84, 'sector': 'both'},
    {'id': 'new_york', 'name': 'New York', 'state': 'NY', 'lat': 40.71, 'lon': -74.01,
     'hubs': ['Transco Zone 6', 'NYISO Zone J', 'Tetco M3'], 'normal_jan': 33, 'normal_jul': 85, 'sector': 'both'},
    {'id': 'boston', 'name': 'Boston', 'state': 'MA', 'lat': 42.36, 'lon': -71.06,
     'hubs': ['Algonquin', 'NEPOOL Mass'], 'normal_jan': 29, 'normal_jul': 82, 'sector': 'both'},
    {'id': 'pittsburgh', 'name': 'Pittsburgh', 'state': 'PA', 'lat': 40.44, 'lon': -79.99,
     'hubs': ['Dominion South', 'PJM West Hub'], 'normal_jan': 28, 'normal_jul': 83, 'sector': 'both'},
    {'id': 'dallas', 'name': 'Dallas', 'state': 'TX', 'lat': 32.78, 'lon': -96.80,
     'hubs': ['ERCOT North', 'ERCOT Hub'], 'normal_jan': 47, 'normal_jul': 96, 'sector': 'both'},
    {'id': 'los_angeles', 'name': 'Los Angeles', 'state': 'CA', 'lat': 34.05, 'lon': -118.24,
     'hubs': ['SoCal Gas', 'CAISO SP15', 'CAISO NP15'], 'normal_jan': 58, 'normal_jul': 84, 'sector': 'both'},
    {'id': 'denver', 'name': 'Denver', 'state': 'CO', 'lat': 39.74, 'lon': -104.98,
     'hubs': ['Opal', 'Kern River'], 'normal_jan': 32, 'normal_jul': 88, 'sector': 'ng'},
]

_weather_cache = {'data': None, 'timestamp': 0, 'source': 'none'}
_weather_lock = threading.Lock()
WEATHER_TTL = 6 * 3600  # 6 hours


def _get_normal_temp(city, day_of_year):
    """Sinusoidal normal temp based on Jan/Jul normals."""
    jan, jul = city['normal_jan'], city['normal_jul']
    mid = (jan + jul) / 2.0
    amp = (jul - jan) / 2.0
    # Peak around day 200 (mid-July), trough around day 15 (mid-Jan)
    return mid + amp * math.sin(2 * math.pi * (day_of_year - 105) / 365)


def _generate_synthetic_weather():
    """Generate plausible synthetic 14-day forecasts when API unavailable."""
    import random
    now = datetime.utcnow()
    doy = now.timetuple().tm_yday
    result = []
    for city in WEATHER_CITIES:
        days = []
        for d in range(14):
            normal = _get_normal_temp(city, doy + d)
            # Random anomaly: biased slightly for realism
            anomaly = random.gauss(0, 5)  # 5°F std dev
            high = normal + abs(random.gauss(5, 2)) + anomaly
            low = normal - abs(random.gauss(5, 2)) + anomaly
            avg = (high + low) / 2
            hdd = max(0, 65 - avg)
            cdd = max(0, avg - 65)
            date_str = (now + timedelta(days=d)).strftime('%Y-%m-%d')
            days.append({
                'date': date_str, 'high': round(high, 1), 'low': round(low, 1),
                'avg': round(avg, 1), 'normal': round(normal, 1),
                'anomaly': round(avg - normal, 1), 'hdd': round(hdd, 1), 'cdd': round(cdd, 1)
            })
        # Compute period aggregates
        hdd_6_10 = sum(d['hdd'] for d in days[5:10])
        cdd_6_10 = sum(d['cdd'] for d in days[5:10])
        hdd_8_14 = sum(d['hdd'] for d in days[7:14])
        cdd_8_14 = sum(d['cdd'] for d in days[7:14])
        normal_hdd_6_10 = sum(max(0, 65 - _get_normal_temp(city, doy + i)) for i in range(5, 10))
        normal_cdd_6_10 = sum(max(0, _get_normal_temp(city, doy + i) - 65) for i in range(5, 10))
        normal_hdd_8_14 = sum(max(0, 65 - _get_normal_temp(city, doy + i)) for i in range(7, 14))
        normal_cdd_8_14 = sum(max(0, _get_normal_temp(city, doy + i) - 65) for i in range(7, 14))
        result.append({
            'id': city['id'], 'name': city['name'], 'state': city['state'],
            'lat': city['lat'], 'lon': city['lon'],
            'hubs': city['hubs'], 'sector': city['sector'],
            'days': days,
            'summary': {
                'hdd_6_10': round(hdd_6_10, 1), 'cdd_6_10': round(cdd_6_10, 1),
                'hdd_8_14': round(hdd_8_14, 1), 'cdd_8_14': round(cdd_8_14, 1),
                'normal_hdd_6_10': round(normal_hdd_6_10, 1), 'normal_cdd_6_10': round(normal_cdd_6_10, 1),
                'normal_hdd_8_14': round(normal_hdd_8_14, 1), 'normal_cdd_8_14': round(normal_cdd_8_14, 1),
                'hdd_6_10_dev': round(hdd_6_10 - normal_hdd_6_10, 1),
                'cdd_6_10_dev': round(cdd_6_10 - normal_cdd_6_10, 1),
                'hdd_8_14_dev': round(hdd_8_14 - normal_hdd_8_14, 1),
                'cdd_8_14_dev': round(cdd_8_14 - normal_cdd_8_14, 1),
            }
        })
    return result


def _fetch_open_meteo_weather():
    """Fetch 14-day forecasts from Open-Meteo API, return structured data or None."""
    import urllib.request
    try:
        lats = ','.join(str(c['lat']) for c in WEATHER_CITIES)
        lons = ','.join(str(c['lon']) for c in WEATHER_CITIES)
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lats}&longitude={lons}"
               f"&daily=temperature_2m_max,temperature_2m_min"
               f"&temperature_unit=fahrenheit&forecast_days=14&timezone=America/Chicago")
        req = urllib.request.Request(url, headers={'User-Agent': 'EnergyDesk/3.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode())
        # Open-Meteo returns a list when multiple coords are sent
        if not isinstance(raw, list):
            raw = [raw]
        now = datetime.utcnow()
        doy = now.timetuple().tm_yday
        result = []
        for i, city in enumerate(WEATHER_CITIES):
            api_data = raw[i] if i < len(raw) else None
            if not api_data or 'daily' not in api_data:
                continue
            daily = api_data['daily']
            highs = daily.get('temperature_2m_max', [])
            lows = daily.get('temperature_2m_min', [])
            dates = daily.get('time', [])
            days = []
            for d in range(min(14, len(highs))):
                high = highs[d]; low = lows[d]
                avg = (high + low) / 2
                normal = _get_normal_temp(city, doy + d)
                hdd = max(0, 65 - avg); cdd = max(0, avg - 65)
                days.append({
                    'date': dates[d] if d < len(dates) else '',
                    'high': round(high, 1), 'low': round(low, 1),
                    'avg': round(avg, 1), 'normal': round(normal, 1),
                    'anomaly': round(avg - normal, 1), 'hdd': round(hdd, 1), 'cdd': round(cdd, 1)
                })
            hdd_6_10 = sum(d['hdd'] for d in days[5:10])
            cdd_6_10 = sum(d['cdd'] for d in days[5:10])
            hdd_8_14 = sum(d['hdd'] for d in days[7:14])
            cdd_8_14 = sum(d['cdd'] for d in days[7:14])
            normal_hdd_6_10 = sum(max(0, 65 - _get_normal_temp(city, doy + j)) for j in range(5, 10))
            normal_cdd_6_10 = sum(max(0, _get_normal_temp(city, doy + j) - 65) for j in range(5, 10))
            normal_hdd_8_14 = sum(max(0, 65 - _get_normal_temp(city, doy + j)) for j in range(7, 14))
            normal_cdd_8_14 = sum(max(0, _get_normal_temp(city, doy + j) - 65) for j in range(7, 14))
            result.append({
                'id': city['id'], 'name': city['name'], 'state': city['state'],
                'lat': city['lat'], 'lon': city['lon'],
                'hubs': city['hubs'], 'sector': city['sector'],
                'days': days,
                'summary': {
                    'hdd_6_10': round(hdd_6_10, 1), 'cdd_6_10': round(cdd_6_10, 1),
                    'hdd_8_14': round(hdd_8_14, 1), 'cdd_8_14': round(cdd_8_14, 1),
                    'normal_hdd_6_10': round(normal_hdd_6_10, 1), 'normal_cdd_6_10': round(normal_cdd_6_10, 1),
                    'normal_hdd_8_14': round(normal_hdd_8_14, 1), 'normal_cdd_8_14': round(normal_cdd_8_14, 1),
                    'hdd_6_10_dev': round(hdd_6_10 - normal_hdd_6_10, 1),
                    'cdd_6_10_dev': round(cdd_6_10 - normal_cdd_6_10, 1),
                    'hdd_8_14_dev': round(hdd_8_14 - normal_hdd_8_14, 1),
                    'cdd_8_14_dev': round(cdd_8_14 - normal_cdd_8_14, 1),
                }
            })
        if len(result) == len(WEATHER_CITIES):
            return result
        return None
    except Exception as e:
        logger.warning(f"Open-Meteo fetch failed: {e}")
        return None


@app.route('/api/weather/forecast', methods=['GET'])
def get_weather_forecast():
    """Return 14-day weather forecasts for energy hub cities."""
    global _weather_cache
    now_ts = _time.time()
    with _weather_lock:
        if _weather_cache['data'] and (now_ts - _weather_cache['timestamp']) < WEATHER_TTL:
            return jsonify({'success': True, 'source': _weather_cache['source'],
                            'cities': _weather_cache['data'],
                            'cached_at': _weather_cache['timestamp']})
    # Try live data first
    live = _fetch_open_meteo_weather()
    if live:
        with _weather_lock:
            _weather_cache = {'data': live, 'timestamp': now_ts, 'source': 'open-meteo'}
        return jsonify({'success': True, 'source': 'open-meteo', 'cities': live, 'cached_at': now_ts})
    # Fallback to synthetic
    synth = _generate_synthetic_weather()
    with _weather_lock:
        _weather_cache = {'data': synth, 'timestamp': now_ts, 'source': 'synthetic'}
    return jsonify({'success': True, 'source': 'synthetic', 'cities': synth, 'cached_at': now_ts})


@app.route('/api/weather/bias', methods=['GET'])
def get_weather_bias():
    """Return per-hub weather-driven price bias for the tick engine.
    Positive = bullish (cold in winter / hot in summer), negative = bearish."""
    global _weather_cache
    data = _weather_cache.get('data')
    if not data:
        # Generate on-the-fly
        data = _generate_synthetic_weather()
    now = datetime.utcnow()
    month = now.month
    is_heating = month in (1, 2, 3, 4, 10, 11, 12)
    hub_bias = {}
    for city in data:
        s = city['summary']
        if is_heating:
            # Colder than normal = more HDD = bullish gas/power
            dev = s['hdd_6_10_dev']
            # Scale: +10 HDD deviation → ~+2% bias
            bias = dev * 0.002
        else:
            # Hotter than normal = more CDD = bullish power
            dev = s['cdd_6_10_dev']
            bias = dev * 0.002
        for hub_name in city['hubs']:
            hub_bias[hub_name] = round(bias, 4)
    return jsonify({'success': True, 'is_heating_season': is_heating, 'bias': hub_bias})


# ---------------------------------------------------------------------------
# WebSocket Events
# ---------------------------------------------------------------------------
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    with connections_lock:
        active_connections.add(sid)
        count = len(active_connections)
    emit('connection_count', {'count': count}, broadcast=True)
    logger.info(f"Client connected: {sid} (total: {count})")

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    with connections_lock:
        active_connections.discard(sid)
        count = len(active_connections)
    emit('connection_count', {'count': count}, broadcast=True)
    logger.info(f"Client disconnected: {sid} (total: {count})")

@socketio.on('register_trader')
def handle_register_trader(data):
    trader_name = data.get('trader_name', '')
    if trader_name:
        conn = get_db_standalone()
        conn.execute("UPDATE traders SET last_seen=CURRENT_TIMESTAMP WHERE trader_name=?", (trader_name,))
        conn.commit()
        conn.close()
        logger.info(f"Trader registered on WS: {trader_name}")

@socketio.on('request_leaderboard')
def handle_leaderboard_request():
    emit('leaderboard_update', {'reason': 'requested'})

# ---------------------------------------------------------------------------
# Trade Margin Calculation Helper
# ---------------------------------------------------------------------------
def _calc_margin(td):
    """Calculate required margin for a trade."""
    volume = float(td.get('volume', 0))
    trade_type = td.get('type', '')
    is_crude = trade_type.startswith('CRUDE') or trade_type in ('EFP', 'OPTION_CL')

    # Spread and multileg trades get reduced margin (offsetting risk)
    is_spread = trade_type in ('SPREAD', 'MULTILEG', 'CRUDE_DIFF')
    spread_discount = 0.4 if is_spread else 1.0  # 60% margin reduction for spreads

    if is_crude:
        margin = (volume / 1000) * 5000
    elif trade_type == 'BASIS_SWAP':
        margin = (volume / 10000) * 800
    elif trade_type in ('OPTION_NG',):
        margin = (volume / 10000) * 1500 * 0.5
    elif trade_type == 'OPTION_CL':
        margin = (volume / 1000) * 5000 * 0.5
    else:
        margin = (volume / 10000) * 1500

    return margin * spread_discount

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    init_db()

    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'

    logger.info(f"Starting Energy Desk v3.0 on {host}:{port}")
    logger.info(f"Database: {DATABASE}")
    logger.info(f"EIA API Key: {'configured' if EIA_API_KEY else 'NOT SET'}")

    socketio.run(app, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)
