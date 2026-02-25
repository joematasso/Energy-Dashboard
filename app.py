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

    # 4. Price validation
    entry_price = float(data.get('entryPrice', 0))
    if entry_price <= 0:
        return jsonify({'success': False, 'error': 'Entry price must be positive'}), 400

    # Validate price vs direction: BUY >= spot, SELL <= spot
    spot_ref = float(data.get('spotRef', entry_price))
    direction = data.get('direction', '')
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
            float(td.get('volume', 0)) == volume and
            float(td.get('entryPrice', 0)) == entry_price):
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

    keywords = {
        'ng': ['natural gas', 'lng', 'storage', 'henry hub', 'pipeline'],
        'crude': ['crude', 'oil', 'opec', 'barrel', 'wti', 'brent', 'petroleum'],
        'power': ['power', 'electric', 'grid', 'renewable', 'ercot', 'pjm', 'solar', 'wind'],
        'freight': ['freight', 'shipping', 'tanker', 'baltic', 'tonnage', 'charter', 'vessel', 'vlcc']
    }.get(commodity, ['energy'])

    try:
        feed = feedparser.parse('https://oilprice.com/rss/main')
        articles = []
        for entry in feed.entries[:30]:
            title = entry.get('title', '').lower()
            summary = entry.get('summary', '').lower()
            combined = title + ' ' + summary
            if any(kw in combined for kw in keywords):
                articles.append({
                    'source': 'OilPrice.com',
                    'headline': entry.get('title', ''),
                    'description': entry.get('summary', '')[:200],
                    'time': entry.get('published', ''),
                    'url': entry.get('link', '')
                })
            if len(articles) >= 6:
                break

        with news_cache_lock:
            news_cache[commodity] = {'data': articles, 'ts': time.time()}
        return jsonify({'success': True, 'articles': articles})
    except Exception as e:
        logger.error(f"News fetch error: {e}")
        return jsonify({'success': True, 'articles': []})

# ---------------------------------------------------------------------------
# EIA Proxy
# ---------------------------------------------------------------------------
@app.route('/api/eia/<eia_type>')
def get_eia(eia_type):
    with eia_cache_lock:
        cached = eia_cache.get(eia_type)
        if cached and time.time() - cached['ts'] < EIA_CACHE_TTL:
            return jsonify({'success': True, 'data': cached['data']})

    if not EIA_API_KEY:
        return jsonify({'success': False, 'error': 'EIA_API_KEY not configured'})

    series_map = {
        'ng_storage': 'NG.NW2_EPG0_SWO_R48_BCF.W',
        'crude_inventory': 'PET.WCESTUS1.W'
    }
    series_id = series_map.get(eia_type)
    if not series_id:
        return jsonify({'success': False, 'error': 'Unknown EIA type'}), 400

    try:
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={EIA_API_KEY}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        result = {'series_id': series_id, 'raw': data}
        with eia_cache_lock:
            eia_cache[eia_type] = {'data': result, 'ts': time.time()}
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logger.error(f"EIA fetch error: {e}")
        return jsonify({'success': False, 'error': str(e)})

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
            'display_name': t['display_name'],
            'firm': t['firm'],
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
    """Check if NYMEX/CME energy markets are open."""
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
    if dow == 5:
        return False, 'Weekend', ct
    if dow == 6:
        return (True, 'Open', ct) if t >= 17*60 else (False, 'Weekend', ct)
    if dow == 4:
        return (True, 'Open', ct) if t < 16*60 else (False, 'Weekend', ct)
    if 16*60 <= t < 17*60:
        return False, 'Maintenance', ct
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
        SELECT c.id, c.name, c.type, c.team_id, cm.last_read,
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
    if not creator or not members:
        return jsonify({'success': False, 'error': 'Creator and members required'}), 400
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
    return jsonify({'success': True, 'messages': [{
        'id': r['id'], 'sender': r['sender'], 'display_name': r['display_name'],
        'team_name': r['team_name'] or '', 'team_color': r['team_color'] or '#888',
        'text': r['text'], 'created_at': r['created_at']
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
    return jsonify({'success': True, 'message_id': msg_id})

@app.route('/api/chat/mark-read/<int:conv_id>/<trader>', methods=['POST'])
def mark_read(conv_id, trader):
    db = get_db()
    db.execute("UPDATE conversation_members SET last_read=CURRENT_TIMESTAMP WHERE conversation_id=? AND trader_name=?", (conv_id, trader))
    db.commit()
    return jsonify({'success': True})


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

    return margin

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
