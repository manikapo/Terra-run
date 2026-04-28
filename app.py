"""
Infinite Me — Backend v2
8me.in | Run. Claim. Own your world.
pip install flask flask-cors requests python-dotenv gunicorn shapely pywebpush
"""

from flask import Flask, jsonify, request, redirect, session
import json, math, os, uuid, hashlib, sqlite3, urllib.parse
from datetime import datetime
import requests

try:
    from pywebpush import webpush, WebPushException
    WEBPUSH_AVAILABLE = True
    print("WebPush loaded OK")
except ImportError:
    WEBPUSH_AVAILABLE = False
    print("WebPush not available — push notifications disabled")

try:
    from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon
    from shapely.validation import make_valid
    SHAPELY_AVAILABLE = True
    print("Shapely loaded OK")
except ImportError:
    SHAPELY_AVAILABLE = False
    print("Shapely not available - using fallback")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "terra-run-secret-change-in-prod")

# Cookie settings for cross-domain
app.config["SESSION_COOKIE_SAMESITE"] = "None"
app.config["SESSION_COOKIE_SECURE"]   = True
app.config["SESSION_COOKIE_HTTPONLY"] = True

# Allowed origins — add any new frontend URLs here
ALLOWED_ORIGINS = [
    "https://play.8me.in",
    "http://play.8me.in",
    "https://8me.in",
    "http://8me.in",
    "https://api.8me.in",
    "https://web-production-4077c.up.railway.app",
    "http://localhost:5500",
    "http://localhost:3000",
    "http://127.0.0.1:5500",
    "capacitor://localhost",
    "ionic://localhost",
    "http://localhost",
]

def get_cors_origin(request_origin):
    """Return allowed origin for CORS header."""
    if not request_origin or request_origin == "null":
        # document.write() or no origin — allow as play.8me.in
        return "https://play.8me.in"
    if request_origin in ALLOWED_ORIGINS:
        return request_origin
    if request_origin.startswith("capacitor://") or request_origin.startswith("ionic://"):
        return request_origin
    return "https://play.8me.in"

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = app.make_response("")
        origin = get_cors_origin(request.headers.get("Origin"))
        res.headers["Access-Control-Allow-Origin"]      = origin
        res.headers["Access-Control-Allow-Headers"]     = "Content-Type,X-User-Id,Authorization,Accept,X-Admin-Token"
        res.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,DELETE,OPTIONS"
        res.headers["Access-Control-Allow-Credentials"] = "true"
        res.headers["Access-Control-Max-Age"]           = "3600"
        res.headers["Vary"]                             = "Origin"
        res.status_code = 204
        return res

@app.after_request
def after_request(response):
    request_origin = request.headers.get("Origin", "")
    origin = get_cors_origin(request_origin)
    response.headers["Access-Control-Allow-Origin"]      = origin
    response.headers["Access-Control-Allow-Headers"]     = "Content-Type,X-User-Id,Authorization,Accept,X-Admin-Token"
    response.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"]           = "3600"
    response.headers["Vary"]                             = "Origin"
    return response



# ── Config ────────────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID", "YOUR_STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "YOUR_STRAVA_CLIENT_SECRET")
STRAVA_REDIRECT_URI  = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:5000/strava/callback")
GOOGLE_CLIENT_ID         = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET     = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI      = os.getenv("GOOGLE_REDIRECT_URI", "https://web-production-4077c.up.railway.app/auth/google/callback")
GOOGLE_ANDROID_CLIENT_ID = os.getenv("GOOGLE_ANDROID_CLIENT_ID", "82989459925-fu2f0588kvhjep7m8tpu9u35lmr7274e.apps.googleusercontent.com")
# Native app uses the web callback URL but marks it with ?app=1 so we can redirect back to the app
GOOGLE_NATIVE_REDIRECT   = os.getenv("GOOGLE_NATIVE_REDIRECT", "https://play.8me.in/auth/google/callback")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "https://play.8me.in")
VAPID_PUBLIC_KEY     = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY    = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_EMAIL          = os.getenv("VAPID_EMAIL", "mailto:hello@8me.in")
DB_PATH              = os.getenv("DB_PATH", "terra_run.db")
ADMIN_PASSWORD       = os.getenv("ADMIN_PASSWORD", "infiniteme-admin-2025")  # change via Railway env var

COLOR_POOL = ["#00ff88","#ff6b35","#00cfff","#ff3cac","#ffd700","#a78bfa",
              "#ff6eb4","#4dffb4","#ff9500","#00e5ff"]

# ── SQLite helpers ────────────────────────────────────────────────────────────
def get_db():
    # Ensure the directory exists (important for Railway Volume)
    import os
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        print(f"Created directory: {db_dir}")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL mode allows multiple readers + one writer simultaneously
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def init_db():
    """Create tables and seed demo data if first run."""
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        avatar      TEXT,
        color       TEXT,
        total_km    REAL DEFAULT 0,
        zones       INTEGER DEFAULT 0,
        strava_token TEXT,
        created_at  TEXT
    );

    CREATE TABLE IF NOT EXISTS territories (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL,
        name        TEXT,
        polygon     TEXT NOT NULL,
        area_km2    REAL,
        captured_at TEXT,
        color       TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS push_subscriptions (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL,
        subscription TEXT NOT NULL,
        created_at  TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)

    # Migrate — add username column if not exists
    for col in ["username TEXT", "photo_url TEXT", "email TEXT"]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col}")
            conn.commit()
        except: pass

    # No demo seed — start clean for real users
    conn.commit()
    conn.close()
    print("Infinite Me — Database ready:", DB_PATH)

# ── Geo helpers ───────────────────────────────────────────────────────────────
def polygon_area_km2(coords):
    R = 6371.0; n = len(coords); area = 0.0
    for i in range(n):
        j = (i + 1) % n
        lat1,lon1 = math.radians(coords[i][0]),math.radians(coords[i][1])
        lat2,lon2 = math.radians(coords[j][0]),math.radians(coords[j][1])
        area += (lon2-lon1)*(2+math.sin(lat1)+math.sin(lat2))
    return abs(area * R * R / 2)

def convex_hull(points):
    pts = sorted(set(map(tuple,points)))
    if len(pts) <= 1: return pts
    def cross(o,a,b): return (a[0]-o[0])*(b[1]-o[1])-(a[1]-o[1])*(b[0]-o[0])
    lower,upper = [],[]
    for p in pts:
        while len(lower)>=2 and cross(lower[-2],lower[-1],p)<=0: lower.pop()
        lower.append(p)
    for p in reversed(pts):
        while len(upper)>=2 and cross(upper[-2],upper[-1],p)<=0: upper.pop()
        upper.append(p)
    return [[p[0],p[1]] for p in lower[:-1]+upper[:-1]]

def bbox_overlap(poly1, poly2, thr=0.0001):
    def bb(p):
        lats=[c[0] for c in p]; lons=[c[1] for c in p]
        return min(lats),max(lats),min(lons),max(lons)
    a=bb(poly1); b=bb(poly2)
    return not (a[1]<b[0]-thr or b[1]<a[0]-thr or a[3]<b[2]-thr or b[3]<a[2]-thr)

def point_in_polygon(point, polygon):
    x, y = point[0], point[1]
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i][0], polygon[i][1]
        xj, yj = polygon[j][0], polygon[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj-xi)*(y-yi)/(yj-yi+1e-12)+xi):
            inside = not inside
        j = i
    return inside

def polygons_truly_overlap(poly1, poly2):
    if not bbox_overlap(poly1, poly2):
        return False
    if SHAPELY_AVAILABLE:
        try:
            s1 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in poly1]))
            s2 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in poly2]))
            return s1.intersects(s2) and not s1.touches(s2)
        except: pass
    for pt in poly1:
        if point_in_polygon(pt, poly2): return True
    for pt in poly2:
        if point_in_polygon(pt, poly1): return True
    return False

def get_overlap_percentage(poly1, poly2):
    if not SHAPELY_AVAILABLE:
        # Fallback: use bounding box intersection ratio as estimate
        def bb(p):
            lats=[c[0] for c in p]; lons=[c[1] for c in p]
            return min(lats),max(lats),min(lons),max(lons)
        a = bb(poly1); b = bb(poly2)
        # Intersection bbox
        ilat1=max(a[0],b[0]); ilat2=min(a[1],b[1])
        ilon1=max(a[2],b[2]); ilon2=min(a[3],b[3])
        if ilat2 <= ilat1 or ilon2 <= ilon1: return 0.0
        inter_area = (ilat2-ilat1)*(ilon2-ilon1)
        poly1_area = (a[1]-a[0])*(a[3]-a[2])
        if poly1_area == 0: return 0.0
        return min(100.0, (inter_area/poly1_area)*100)
    try:
        s1 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in poly1]))
        s2 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in poly2]))
        if s1.area == 0: return 0
        return (s1.intersection(s2).area / s1.area) * 100
    except: return 100.0

def clip_polygon(original, cutter):
    if not SHAPELY_AVAILABLE:
        # Shapely not available - keep original zone intact (no clipping)
        return [original]
    try:
        s1 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in original]))
        s2 = make_valid(ShapelyPolygon([(p[0],p[1]) for p in cutter]))
        remaining = make_valid(s1.difference(s2))
        if remaining.is_empty: return []
        geoms = list(remaining.geoms) if remaining.geom_type == 'MultiPolygon' else [remaining]
        results = []
        for g in geoms:
            if g.geom_type == 'Polygon' and g.area > 0.000001:
                coords = [[c[0],c[1]] for c in list(g.exterior.coords)]
                if len(coords) >= 3:
                    results.append(coords)
        return results
    except Exception as e:
        print("clip_polygon error:", e)
        return []

def assign_color(user_count):
    return COLOR_POOL[user_count % len(COLOR_POOL)]

# ── Auth ──────────────────────────────────────────────────────────────────────
def get_uid():
    """Get user id from session, header, body, or query param — tries all methods."""    # 1. Session cookie (works when same domain)
    uid = session.get("user_id")
    if uid: return uid
    # 2. Custom header
    uid = request.headers.get("X-User-Id")
    if uid: return uid
    # 3. Inside JSON body (_uid field)
    try:
        data = request.get_json(silent=True) or {}
        uid = data.get("_uid")
        if uid: return uid
    except: pass
    # 4. Query parameter
    uid = request.args.get("_uid")
    if uid: return uid
    return None
@app.route("/api/guest-login", methods=["POST"])
def guest_login():
    data = request.json or {}
    name = data.get("name","Runner").strip() or "Runner"
    existing_uid = data.get("uid") or data.get("_uid")
    conn = get_db(); c = conn.cursor()
    # Ensure photo_url column exists
    try:
        c.execute("ALTER TABLE users ADD COLUMN photo_url TEXT")
        conn.commit()
    except: pass
    
    # Try to find user by existing uid first
    if existing_uid:
        user = c.execute("SELECT * FROM users WHERE id=?", (existing_uid,)).fetchone()
        if user:
            conn.close()
            session["user_id"] = existing_uid
            return jsonify({"ok": True, "user": dict(user)})
    
    # If no existing uid, always create a NEW unique user
    # Use random UUID so two people with same name get separate accounts
    uid = "u_" + uuid.uuid4().hex[:12]
    count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    color = assign_color(count)
    now   = datetime.utcnow().isoformat()+"Z"
    c.execute("""INSERT INTO users
                 (id, name, avatar, color, total_km, zones, strava_token, created_at)
                 VALUES (?,?,?,?,?,?,?,?)""",
              (uid, name, name[:2].upper(), color, 0.0, 0, None, now))
    conn.commit()
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    session["user_id"] = uid
    return jsonify({"ok": True, "user": dict(user)})

@app.route("/api/username/check")
def check_username():
    """Check if a username is available."""
    username = request.args.get("username", "").strip().lower()
    if not username:
        return jsonify({"ok": False, "error": "Username required"})
    if len(username) < 3:
        return jsonify({"ok": False, "error": "Too short — minimum 3 characters"})
    if len(username) > 20:
        return jsonify({"ok": False, "error": "Too long — maximum 20 characters"})
    if not username.replace("_","").replace(".","").isalnum():
        return jsonify({"ok": False, "error": "Only letters, numbers, _ and . allowed"})
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE LOWER(username)=?", (username,)).fetchone()
    conn.close()
    if existing:
        return jsonify({"ok": False, "error": "Username already taken"})
    return jsonify({"ok": True, "available": True})

@app.route("/api/username/set", methods=["POST"])
def set_username():
    """Set username for logged-in user."""
    uid = get_uid()
    if not uid: return jsonify({"ok": False, "error": "Not logged in"}), 401
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    if not username:
        return jsonify({"ok": False, "error": "Username required"})
    if len(username) < 3:
        return jsonify({"ok": False, "error": "Too short — minimum 3 characters"})
    if len(username) > 20:
        return jsonify({"ok": False, "error": "Too long — maximum 20 characters"})
    if not username.replace("_","").replace(".","").isalnum():
        return jsonify({"ok": False, "error": "Only letters, numbers, _ and . allowed"})
    conn = get_db(); c = conn.cursor()
    # Check not taken by someone else
    existing = c.execute("SELECT id FROM users WHERE LOWER(username)=? AND id!=?",
                         (username, uid)).fetchone()
    if existing:
        conn.close()
        return jsonify({"ok": False, "error": "Username already taken"})
    c.execute("UPDATE users SET username=? WHERE id=?", (username, uid))
    conn.commit()
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "user": dict(user)})

@app.route("/")
def index():
    return jsonify({
        "name": "Infinite Me API",
        "version": "2.0",
        "status": "running",
        "frontend": "https://play.8me.in",
        "docs": "https://8me.in/tutorial.html"
    })

# ── Push Notifications ────────────────────────────────────────────────────────
def send_push(user_id, title, body, url="/"):
    """Send a push notification to all subscriptions for a user."""
    if not WEBPUSH_AVAILABLE or not VAPID_PRIVATE_KEY:
        return
    conn = get_db()
    subs = conn.execute(
        "SELECT subscription FROM push_subscriptions WHERE user_id=?",
        (user_id,)
    ).fetchall()
    conn.close()
    payload = json.dumps({ "title": title, "body": body, "url": url })
    for row in subs:
        try:
            webpush(
                subscription_info=json.loads(row["subscription"]),
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_EMAIL}
            )
        except WebPushException as e:
            print(f"Push failed for {user_id}: {e}")
            # Remove dead subscriptions (410 = subscription expired)
            if "410" in str(e):
                conn2 = get_db()
                conn2.execute(
                    "DELETE FROM push_subscriptions WHERE subscription=?",
                    (row["subscription"],)
                )
                conn2.commit()
                conn2.close()
        except Exception as e:
            print(f"Push error: {e}")

@app.route("/api/push/debug")
def push_debug():
    """Debug push notification setup."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    conn = get_db()
    subs = conn.execute(
        "SELECT id, created_at FROM push_subscriptions WHERE user_id=?",
        (uid,)
    ).fetchall()
    conn.close()
    return jsonify({
        "ok": True,
        "webpush_available": WEBPUSH_AVAILABLE,
        "vapid_public_key_set": bool(VAPID_PUBLIC_KEY),
        "vapid_private_key_set": bool(VAPID_PRIVATE_KEY),
        "vapid_private_key_length": len(VAPID_PRIVATE_KEY) if VAPID_PRIVATE_KEY else 0,
        "vapid_email": VAPID_EMAIL,
        "subscriptions_count": len(subs),
        "subscriptions": [dict(s) for s in subs]
    })

@app.route("/api/push/vapid-public-key")
def get_vapid_public_key():
    """Return VAPID public key for frontend subscription."""
    return jsonify({"publicKey": VAPID_PUBLIC_KEY})

@app.route("/api/push/subscribe", methods=["POST"])
def push_subscribe():
    """Store a push subscription for the logged-in user."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    data = request.json or {}
    subscription = data.get("subscription")
    if not subscription:
        return jsonify({"ok": False, "error": "No subscription"}), 400
    sub_str = json.dumps(subscription)
    conn = get_db(); c = conn.cursor()
    # Remove ALL old subscriptions for this user first — keep only the latest
    c.execute("DELETE FROM push_subscriptions WHERE user_id=?", (uid,))
    # Save the new subscription
    c.execute(
        "INSERT INTO push_subscriptions VALUES (?,?,?,?)",
        (uuid.uuid4().hex, uid, sub_str, datetime.utcnow().isoformat()+"Z")
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/push/unsubscribe", methods=["POST"])
def push_unsubscribe():
    """Remove push subscription."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    conn = get_db()
    conn.execute("DELETE FROM push_subscriptions WHERE user_id=?", (uid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/push/test", methods=["POST"])
def push_test():
    """Send a test notification to the logged-in user."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    send_push(uid,
        "🏃 Infinite Me",
        "Push notifications are working! You'll be alerted when your territory is stolen.",
        "/"
    )
    return jsonify({"ok": True})

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/me")
def me():
    uid = get_uid()
    if not uid: return jsonify({"ok":False,"error":"not logged in"}),401
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    if not user: return jsonify({"ok":False,"error":"user not found"}),404
    return jsonify({"ok":True,"user":dict(user)})

# ── Speed Anti-Cheat ──────────────────────────────────────────────────────────
VEHICLE_SPEED_KMH    = 28.0   # max sustained human running speed
CHEAT_WINDOW_SECS    = 30     # rolling window to check for sustained over-speed
CHEAT_SEGMENTS_MIN   = 3      # need at least N consecutive fast segments to flag

def haversine_km(a, b):
    """Distance in km between two [lat,lng] points."""
    R = 6371.0
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    sin_dlat = math.sin(dlat / 2); sin_dlon = math.sin(dlon / 2)
    a2 = sin_dlat**2 + math.cos(lat1) * math.cos(lat2) * sin_dlon**2
    return R * 2 * math.atan2(math.sqrt(a2), math.sqrt(1 - a2))

def check_vehicle_speed(gps_points_with_ts):
    """
    Detect vehicular movement. Each point is [lat, lng, timestamp_ms].
    Returns (is_cheating: bool, max_speed_kmh: float, flagged_segments: int).
    If no timestamps provided, returns (False, 0, 0) — graceful fallback.
    """
    pts = []
    for p in gps_points_with_ts:
        if len(p) >= 3 and p[2] is not None:
            try:
                pts.append((float(p[0]), float(p[1]), float(p[2])))
            except (ValueError, TypeError):
                pass

    if len(pts) < 4:
        return False, 0.0, 0  # No timestamps — can't check, allow through

    max_speed = 0.0
    consecutive_fast = 0
    peak_consecutive  = 0

    for i in range(1, len(pts)):
        lat1, lng1, ts1 = pts[i-1]
        lat2, lng2, ts2 = pts[i]
        dt_secs = (ts2 - ts1) / 1000.0
        if dt_secs <= 0 or dt_secs > 120:  # skip bad/stale segments
            consecutive_fast = 0
            continue
        dist_km  = haversine_km([lat1, lng1], [lat2, lng2])
        speed_kmh = dist_km / (dt_secs / 3600.0)
        if speed_kmh > max_speed:
            max_speed = speed_kmh
        if speed_kmh > VEHICLE_SPEED_KMH:
            consecutive_fast += 1
            peak_consecutive = max(peak_consecutive, consecutive_fast)
        else:
            consecutive_fast = 0

    is_cheating = peak_consecutive >= CHEAT_SEGMENTS_MIN
    return is_cheating, round(max_speed, 1), peak_consecutive


# ── Territories ───────────────────────────────────────────────────────────────
@app.route("/api/territories")
def get_territories():
    try:
        conn = get_db()
        # Ensure all extra columns exist
        for col in ["gps_path TEXT", "total_km REAL", "avg_pace TEXT",
                    "duration TEXT", "straight_km REAL", "photo_url TEXT"]:
            try:
                conn.execute(f"ALTER TABLE territories ADD COLUMN {col}")
                conn.commit()
            except: pass
        # Ensure photo_url exists on users too
        try:
            conn.execute("ALTER TABLE users ADD COLUMN photo_url TEXT")
            conn.commit()
        except: pass

        rows = conn.execute("""
            SELECT t.*, u.name as owner_name, u.avatar as owner_avatar,
                   COALESCE(u.photo_url,'') as owner_photo,
                   COALESCE(u.username, u.name) as owner_display
            FROM territories t LEFT JOIN users u ON t.user_id = u.id
        """).fetchall()
        conn.close()
        result = []
        for r in rows:
            try:
                d = dict(r)
                d["polygon"] = json.loads(d["polygon"])
                if d.get("gps_path"):
                    try: d["gps_path"] = json.loads(d["gps_path"])
                    except: d["gps_path"] = None
                else:
                    d["gps_path"] = None
                result.append(d)
            except Exception as row_err:
                print(f"Skipping bad territory row: {row_err}")
                continue
        return jsonify(result)
    except Exception as e:
        import traceback
        print("get_territories ERROR:", traceback.format_exc())
        return jsonify([])  # return empty array so frontend doesn't crash

@app.route("/api/territories", methods=["POST"])
def create_territory():
    try:
        uid = get_uid()
        if not uid:
            return jsonify({"ok": False, "error": "not logged in"}), 401

        data     = request.json or {}
        gps      = data.get("gps_points", [])  # each point: [lat, lng] or [lat, lng, ts_ms]
        name     = data.get("name", f"Run {uuid.uuid4().hex[:4].upper()}")
        sim_mode = data.get("sim_mode", False)

        if len(gps) < 4:
            return jsonify({"ok": False, "error": "Need at least 4 GPS points"}), 400

        # ── Vehicle/Speed Anti-Cheat ──────────────────────────────────────
        if not sim_mode:
            is_cheating, max_speed, fast_segs = check_vehicle_speed(gps)
            if is_cheating:
                print(f"CHEAT DETECTED: uid={uid}, max_speed={max_speed} km/h, "
                      f"consecutive_fast_segments={fast_segs}")
                return jsonify({
                    "ok": False,
                    "error": "vehicle_speed",
                    "max_speed_kmh": max_speed,
                    "message": (
                        f"Run rejected — speeds up to {max_speed:.1f} km/h detected "
                        f"({fast_segs} consecutive segments over {VEHICLE_SPEED_KMH} km/h). "
                        "Only human-paced runs are allowed."
                    )
                }), 400

        pts = [[float(p[0]), float(p[1])] for p in gps]

        # Build polygon
        if sim_mode:
            sampled = pts
            polygon = pts + [pts[0]]
        else:
            step    = max(1, len(pts) // 300)
            sampled = pts[::step]
            if sampled[-1] != pts[-1]:
                sampled.append(pts[-1])
            polygon = sampled + [sampled[0]]
            # Shapely validation — optional, skip on error
            if SHAPELY_AVAILABLE:
                try:
                    from shapely.geometry import Polygon as ShapelyPoly
                    sp = make_valid(ShapelyPoly([(p[1], p[0]) for p in polygon]))
                    if not sp.is_empty and sp.area > 0:
                        polygon = [[c[1], c[0]] for c in sp.exterior.coords]
                except Exception as e:
                    print("Shapely validation skipped:", e)

        if len(polygon) < 3:
            return jsonify({"ok": False, "error": "Could not form polygon"}), 400

        area = polygon_area_km2(polygon)

        # Run stats
        dist = sum(
            math.sqrt((gps[i][0]-gps[i-1][0])**2 + (gps[i][1]-gps[i-1][1])**2) * 111
            for i in range(1, len(gps))
        )
        run_km   = round(data.get("total_km", dist), 2)
        avg_pace = data.get("avg_pace", None)
        duration = data.get("duration", None)

        # Straight-line A→B distance
        try:
            lat1, lon1 = math.radians(gps[0][0]),  math.radians(gps[0][1])
            lat2, lon2 = math.radians(gps[-1][0]), math.radians(gps[-1][1])
            dlat = lat2 - lat1; dlon = lon2 - lon1
            a_ = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
            straight_km = round(6371 * 2 * math.atan2(math.sqrt(a_), math.sqrt(1-a_)), 3)
        except:
            straight_km = 0.0

        conn = get_db(); c = conn.cursor()

        # Ensure user exists
        user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        if not user:
            conn.close()
            return jsonify({"ok": False, "error": "User not found"}), 404

        # Ensure all columns exist
        for col in ["gps_path TEXT", "total_km REAL", "avg_pace TEXT",
                    "duration TEXT", "straight_km REAL"]:
            try:
                c.execute(f"ALTER TABLE territories ADD COLUMN {col}")
                conn.commit()
            except: pass

        # ── Save the territory FIRST ──────────────────────────────────────
        tid   = "t_" + uuid.uuid4().hex[:8]
        now   = datetime.utcnow().isoformat() + "Z"
        color = user["color"]
        gps_path_json = json.dumps(sampled) if not sim_mode else json.dumps(polygon)

        c.execute("""INSERT INTO territories
                     (id, user_id, name, polygon, area_km2, captured_at, color,
                      gps_path, total_km, avg_pace, duration, straight_km)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (tid, uid, name, json.dumps(polygon), round(area, 4), now, color,
                   gps_path_json, run_km, avg_pace, duration, straight_km))
        c.execute("UPDATE users SET zones=zones+1, total_km=total_km+? WHERE id=?",
                  (run_km, uid))
        conn.commit()

        # ── Steal logic — run after save, skip on any error ───────────────
        stolen_names = []
        stolen_user_ids = []
        try:
            all_t = c.execute("SELECT * FROM territories WHERE user_id != ?", (uid,)).fetchall()
            for t in all_t:
                try:
                    t_poly = json.loads(t["polygon"])
                    if not polygons_truly_overlap(polygon, t_poly):
                        continue
                    overlap_pct = get_overlap_percentage(t_poly, polygon)
                    if overlap_pct >= 50.0:
                        c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
                        c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (t["user_id"],))
                        su = c.execute("SELECT name, username FROM users WHERE id=?", (t["user_id"],)).fetchone()
                        if su:
                            stolen_names.append(su["name"])
                            stolen_user_ids.append(t["user_id"])
                    else:
                        remaining = clip_polygon(t_poly, polygon)
                        if not remaining:
                            c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
                            c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (t["user_id"],))
                            su = c.execute("SELECT name, username FROM users WHERE id=?", (t["user_id"],)).fetchone()
                            if su:
                                stolen_names.append(su["name"])
                                stolen_user_ids.append(t["user_id"])
                        else:
                            largest  = max(remaining, key=lambda p: polygon_area_km2(p))
                            new_area = round(polygon_area_km2(largest), 4)
                            c.execute("UPDATE territories SET polygon=?, area_km2=? WHERE id=?",
                                      (json.dumps(largest), new_area, t["id"]))
                            for extra in remaining[1:]:
                                if polygon_area_km2(extra) > 0.0001:
                                    etid = "t_" + uuid.uuid4().hex[:8]
                                    c.execute("""INSERT INTO territories
                                                 (id, user_id, name, polygon, area_km2, captured_at, color)
                                                 VALUES (?,?,?,?,?,?,?)""",
                                              (etid, t["user_id"], t["name"]+" (part)",
                                               json.dumps(extra),
                                               round(polygon_area_km2(extra), 4),
                                               now, t["color"]))
                                    c.execute("UPDATE users SET zones=zones+1 WHERE id=?", (t["user_id"],))
                except Exception as steal_err:
                    print(f"Steal error for territory {t['id']}:", steal_err)
                    continue

            # Merge own overlapping zones — expand territory instead of delete+replace
            my_t = c.execute("SELECT * FROM territories WHERE user_id=? AND id!=?", (uid, tid)).fetchall()
            for t in my_t:
                try:
                    t_poly = json.loads(t["polygon"])
                    if not polygons_truly_overlap(polygon, t_poly):
                        continue
                    # Merge: combine all points from both polygons and compute new convex hull
                    try:
                        from shapely.geometry import MultiPoint
                        combined_pts = [(p[0], p[1]) for p in polygon] + [(p[0], p[1]) for p in t_poly]
                        merged_hull = MultiPoint(combined_pts).convex_hull
                        if merged_hull.geom_type == 'Polygon':
                            merged_coords = [[c[0], c[1]] for c in merged_hull.exterior.coords[:-1]]
                            merged_area = merged_hull.area
                            # Update current territory with merged polygon
                            c.execute("UPDATE territories SET polygon=?, area=? WHERE id=?",
                                     (json.dumps(merged_coords), merged_area, tid))
                            polygon = merged_coords  # use merged polygon going forward
                        # Delete the old overlapping territory (it's been merged in)
                        c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
                        c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (uid,))
                    except Exception as merge_err:
                        # On merge error — keep both territories, don't delete
                        # This ensures running over your own territory never removes it
                        print("Merge error — keeping both territories:", merge_err)
                except: pass

            conn.commit()

            # ── Send push notifications to stolen users ────────────────────
            if stolen_user_ids:
                attacker = c.execute("SELECT name, username FROM users WHERE id=?", (uid,)).fetchone()
                attacker_display = ('@' + attacker['username']) if attacker and attacker['username'] else (attacker['name'] if attacker else 'Someone')
                for victim_uid in set(stolen_user_ids):
                    send_push(
                        victim_uid,
                        "⚔️ Territory Stolen!",
                        f"{attacker_display} just stole your zone! Open the app to reclaim it.",
                        "/"
                    )

        except Exception as steal_outer_err:
            print("Steal outer error:", steal_outer_err)
            conn.commit()

        updated_user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        conn.close()

        msg = "Territory captured!"
        if stolen_names:
            msg += f" Stolen from: {', '.join(stolen_names)} 😈"

        return jsonify({
            "ok": True,
            "territory": {
                "id": tid, "user_id": uid, "name": name, "polygon": polygon,
                "gps_path": sampled if not sim_mode else polygon,
                "area_km2": round(area, 4), "captured_at": now, "color": color,
                "owner_name": updated_user["name"] if updated_user else name,
                "total_km": run_km, "avg_pace": avg_pace,
                "duration": duration, "straight_km": straight_km
            },
            "stolen_from": stolen_names,
            "message": msg,
            "user": dict(updated_user) if updated_user else {}
        })

    except Exception as e:
        import traceback
        print("create_territory ERROR:", traceback.format_exc())
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/territories/<tid>", methods=["DELETE"])
def delete_territory(tid):
    uid = get_uid()
    if not uid: return jsonify({"ok":False,"error":"not logged in"}),401
    conn = get_db(); c = conn.cursor()
    t = c.execute("SELECT * FROM territories WHERE id=? AND user_id=?", (tid,uid)).fetchone()
    if not t:
        conn.close()
        return jsonify({"ok":False,"error":"Not found or not yours"}),404
    c.execute("DELETE FROM territories WHERE id=?", (tid,))
    c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (uid,))
    conn.commit(); conn.close()
    return jsonify({"ok":True})

# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.route("/api/leaderboard")
def get_leaderboard():
    conn = get_db()
    users_rows = conn.execute("SELECT * FROM users").fetchall()
    board = []
    for u in users_rows:
        u = dict(u)
        zone_area = conn.execute(
            "SELECT COALESCE(SUM(area_km2),0) FROM territories WHERE user_id=?",
            (u["id"],)
        ).fetchone()[0]
        board.append({
            "id": u["id"], "name": u["name"], "avatar": u["avatar"],
            "color": u["color"], "zones": u["zones"],
            "total_km": round(u["total_km"] or 0, 1),
            "area_km2": round(zone_area or 0, 3),
            "score": (u["zones"] or 0)*100 + int(u["total_km"] or 0)
        })
    conn.close()
    board.sort(key=lambda x: x["score"], reverse=True)
    for i,e in enumerate(board): e["rank"] = i+1
    return jsonify(board)

# ── Ping + debug ─────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    """Instant health check."""
    import os
    db_exists = os.path.exists(DB_PATH)
    db_size   = os.path.getsize(DB_PATH) if db_exists else 0
    data_dir  = os.path.dirname(DB_PATH)
    dir_exists = os.path.exists(data_dir)
    return jsonify({
        "ok": True,
        "message": "TERRA RUN is alive",
        "version": "2.0",
        "shapely": SHAPELY_AVAILABLE,
        "db_path": DB_PATH,
        "db_exists": db_exists,
        "db_size_bytes": db_size,
        "data_dir_exists": dir_exists,
        "cwd": os.getcwd()
    })

@app.route("/test")
def test():
    """Browser-friendly test — open this URL directly to verify server + env vars."""
    strava_id  = STRAVA_CLIENT_ID
    strava_ok  = strava_id not in ("", "YOUR_STRAVA_CLIENT_ID")
    redirect_uri = STRAVA_REDIRECT_URI
    frontend   = os.getenv("FRONTEND_URL", "NOT SET")
    origin     = request.headers.get("Origin", "no origin header")
    return f"""
    <h2>✅ Infinite Me server is running</h2>
    <table border=1 cellpadding=8>
      <tr><td>STRAVA_CLIENT_ID set</td><td>{"✅ yes" if strava_ok else "❌ NO — missing!"}</td></tr>
      <tr><td>STRAVA_CLIENT_SECRET set</td><td>{"✅ yes" if os.getenv("STRAVA_CLIENT_SECRET") else "❌ NO — missing!"}</td></tr>
      <tr><td>STRAVA_REDIRECT_URI</td><td>{redirect_uri}</td></tr>
      <tr><td>FRONTEND_URL</td><td>{frontend}</td></tr>
      <tr><td>DB_PATH</td><td>{DB_PATH}</td></tr>
      <tr><td>DB exists</td><td>{"✅ yes" if os.path.exists(DB_PATH) else "❌ no"}</td></tr>
      <tr><td>Request Origin</td><td>{origin}</td></tr>
      <tr><td>PORT</td><td>{os.getenv("PORT","not set")}</td></tr>
    </table>
    <br><a href="/ping">→ /ping (JSON)</a>
    """

@app.route("/api/debug/whoami", methods=["GET","OPTIONS"])
def whoami():
    uid = get_uid()
    if not uid:
        return jsonify({"uid":None,"found":False,
                        "session":dict(session),
                        "header":request.headers.get("X-User-Id"),
                        "param":request.args.get("_uid")})
    conn = get_db()
    user = conn.execute("SELECT id,name,color,zones FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    return jsonify({"uid":uid,"found":bool(user),
                    "user":dict(user) if user else None})

@app.route("/api/admin/reset-all")
def reset_all():
    """Delete ALL territories and reset all user zone counts to 0."""
    conn = get_db(); c = conn.cursor()
    c.execute("DELETE FROM territories")
    c.execute("UPDATE users SET zones=0, total_km=0")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "All territories deleted, all users reset to 0"})

@app.route("/api/admin/cleanup-all")
def cleanup_all():
    """Remove ALL test/demo territories for every user and fix zone counts."""
    conn = get_db(); c = conn.cursor()
    # Delete test territories
    c.execute("DELETE FROM territories WHERE id LIKE 'test_%' OR name='Test Zone'")
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    # Fix zone counts for all users
    users = c.execute("SELECT id FROM users").fetchall()
    for u in users:
        real_count = c.execute("SELECT COUNT(*) FROM territories WHERE user_id=?",
                               (u["id"],)).fetchone()[0]
        c.execute("UPDATE users SET zones=? WHERE id=?", (real_count, u["id"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "deleted_test": deleted})

@app.route("/api/admin/list-users")
def list_users():
    """List all users and their territory counts."""
    conn = get_db()
    users = conn.execute("SELECT id, name, zones, total_km FROM users ORDER BY zones DESC").fetchall()
    conn.close()
    return jsonify([dict(u) for u in users])

@app.route("/api/admin/clear-demo")
def clear_demo():
    """Remove demo seed data. Visit once to clean the DB."""
    demo_uids = ['u1','u2','u3','u4','u5']
    demo_tids = ['t1','t2','t3','t4','t5']
    conn = get_db(); c = conn.cursor()
    for uid in demo_uids:
        c.execute("DELETE FROM users WHERE id=?", (uid,))
    for tid in demo_tids:
        c.execute("DELETE FROM territories WHERE id=?", (tid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Demo data removed"})

@app.route("/api/admin/cleanup")
def cleanup():
    """Delete test territories and fix zone counts."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "pass ?_uid=YOUR_UID"})
    conn = get_db(); c = conn.cursor()
    # Delete test territories for this user
    c.execute("DELETE FROM territories WHERE user_id=? AND (id LIKE 'test_%' OR name='Test Zone')", (uid,))
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    # Recount zones properly
    real_count = c.execute("SELECT COUNT(*) FROM territories WHERE user_id=?", (uid,)).fetchone()[0]
    c.execute("UPDATE users SET zones=? WHERE id=?", (real_count, uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "deleted_test": deleted, "zones_now": real_count})
def test_save():
    """Quick test: save a dummy territory for the current user and return result."""
    uid = get_uid()
    if not uid:
        return jsonify({"ok": False, "error": "not logged in — pass ?_uid=YOUR_UID"})
    conn = get_db(); c = conn.cursor()
    # Ensure columns exist
    for col in ["gps_path TEXT", "total_km REAL", "avg_pace TEXT", "duration TEXT", "straight_km REAL"]:
        try:
            c.execute(f"ALTER TABLE territories ADD COLUMN {col}")
            conn.commit()
        except: pass
    tid = "test_" + uuid.uuid4().hex[:6]
    try:
        c.execute("""INSERT INTO territories
                     (id, user_id, name, polygon, area_km2, captured_at, color,
                      gps_path, total_km, avg_pace, duration, straight_km)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (tid, uid, "Test Zone", "[[0,0],[1,0],[1,1]]", 0.01,
                   datetime.utcnow().isoformat()+"Z", "#00ff88",
                   None, 1.0, "5'00\"", "10m 00s", 0.5))
        c.execute("UPDATE users SET zones=zones+1 WHERE id=?", (uid,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "tid": tid, "message": "Test territory saved! Check map."})
    except Exception as e:
        import traceback
        conn.close()
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})
    conn = get_db()
    user = conn.execute("SELECT id,name,color,zones FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    return jsonify({"uid":uid,"found":bool(user),
                    "user":dict(user) if user else None})

# ── Google OAuth ──────────────────────────────────────────────────────────────
@app.route("/auth/google")
def google_connect():
    """Redirect user to Google OAuth consent screen."""
    uid   = request.args.get('_uid') or get_uid()
    state = uid or "new"
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         state,
        "access_type":   "offline",
        "prompt":        "select_account"
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return redirect(url)

@app.route("/auth/google/callback")
def google_callback():
    """Handle Google OAuth callback — works for both web and native app."""
    code  = request.args.get("code")
    state = request.args.get("state", "new")
    error = request.args.get("error")

    if error or not code:
        return redirect(f"{FRONTEND_URL}?google=error")

    # Exchange code for tokens
    token_res = requests.post("https://oauth2.googleapis.com/token", data={
        "code":          code,
        "client_id":     GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "grant_type":    "authorization_code"
    })
    token_data = token_res.json()
    if "access_token" not in token_data:
        print("Google token error:", token_data)
        return redirect(f"{FRONTEND_URL}?google=error")

    # Fetch user profile
    profile_res = requests.get("https://www.googleapis.com/oauth2/v3/userinfo",
                               headers={"Authorization": f"Bearer {token_data['access_token']}"})
    profile   = profile_res.json()
    google_id = profile.get("sub")
    email     = profile.get("email", "")
    name      = profile.get("name", email.split("@")[0] if email else "Runner")
    photo_url = profile.get("picture", "")
    avatar    = name[:2].upper()

    if not google_id:
        return redirect(f"{FRONTEND_URL}?google=error")

    uid = f"google_{google_id}"

    conn = get_db(); c = conn.cursor()
    try:    c.execute("ALTER TABLE users ADD COLUMN photo_url TEXT"); conn.commit()
    except: pass
    try:    c.execute("ALTER TABLE users ADD COLUMN email TEXT");     conn.commit()
    except: pass

    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        color = COLOR_POOL[count % len(COLOR_POOL)]
        now   = datetime.utcnow().isoformat() + "Z"
        c.execute("""INSERT OR IGNORE INTO users
                     (id, name, avatar, color, total_km, zones, strava_token, created_at, photo_url, email)
                     VALUES (?,?,?,?,?,?,?,?,?,?)""",
                  (uid, name, avatar, color, 0.0, 0, None, now, photo_url, email))
    else:
        c.execute("UPDATE users SET name=?, photo_url=?, email=? WHERE id=?",
                  (name, photo_url, email, uid))

    # Migrate guest territories if state was a guest uid
    if state and state != "new" and state != uid and state.startswith("u_"):
        c.execute("UPDATE territories SET user_id=? WHERE user_id=?", (uid, state))
        zone_count = c.execute("SELECT COUNT(*) FROM territories WHERE user_id=?", (uid,)).fetchone()[0]
        c.execute("UPDATE users SET zones=? WHERE id=?", (zone_count, uid))
        print(f"Migrated guest {state} → Google {uid}")

    conn.commit(); conn.close()
    session["user_id"] = uid
    encoded_name = urllib.parse.quote(name)
    return redirect(f"{FRONTEND_URL}?google=connected&uid={uid}&name={encoded_name}")




@app.route("/auth/google/native", methods=["POST"])
def google_native_login():
    """
    Native Android app sends Google ID token directly.
    We verify it with Google, then create/update the user and return uid+name.
    No browser redirect needed — pure API call.
    """
    data     = request.json or {}
    id_token = data.get("id_token", "").strip()

    if not id_token:
        return jsonify({"ok": False, "error": "id_token required"}), 400

    # Verify the ID token with Google
    try:
        verify_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={id_token}"
        r = requests.get(verify_url, timeout=10)
        token_info = r.json()
    except Exception as e:
        print("Google token verify error:", e)
        return jsonify({"ok": False, "error": "Token verification failed"}), 400

    # Check token is valid
    if "error_description" in token_info or "sub" not in token_info:
        print("Invalid Google token:", token_info)
        return jsonify({"ok": False, "error": "Invalid token"}), 401

    # Verify audience matches our Android or Web client ID
    aud = token_info.get("aud", "")
    valid_clients = [
        GOOGLE_ANDROID_CLIENT_ID,
        GOOGLE_CLIENT_ID,
    ]
    if aud not in valid_clients:
        print(f"Token audience mismatch: {aud}")
        return jsonify({"ok": False, "error": "Token audience mismatch"}), 401

    google_id = token_info.get("sub")
    email     = token_info.get("email", "")
    name      = token_info.get("name", email.split("@")[0] if email else "Runner")
    photo_url = token_info.get("picture", "")
    avatar    = name[:2].upper()
    uid       = f"google_{google_id}"

    conn = get_db(); c = conn.cursor()
    try:    c.execute("ALTER TABLE users ADD COLUMN photo_url TEXT"); conn.commit()
    except: pass
    try:    c.execute("ALTER TABLE users ADD COLUMN email TEXT");     conn.commit()
    except: pass

    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        color = COLOR_POOL[count % len(COLOR_POOL)]
        now   = datetime.utcnow().isoformat() + "Z"
        c.execute("""INSERT OR IGNORE INTO users
                     (id, name, avatar, color, total_km, zones, strava_token, created_at, photo_url, email)
                     VALUES (?,?,?,?,?,?,?,?,?,?)""",
                  (uid, name, avatar, color, 0.0, 0, None, now, photo_url, email))
    else:
        c.execute("UPDATE users SET name=?, photo_url=?, email=? WHERE id=?",
                  (name, photo_url, email, uid))

    conn.commit()
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    session["user_id"] = uid

    print(f"Native Google login: {uid} ({name})")
    return jsonify({"ok": True, "user": dict(user)})

@app.route("/strava/connect")
def strava_connect():
    uid = get_uid()
    # No login required — uid passed via query param or session
    # If none, use "new" and the callback will create the user fresh
    state = uid or "new"
    url = (f"https://www.strava.com/oauth/authorize"
           f"?client_id={STRAVA_CLIENT_ID}&response_type=code"
           f"&redirect_uri={STRAVA_REDIRECT_URI}&scope=activity:read_all&state={state}")
    return redirect(url)

@app.route("/strava/disconnect", methods=["POST"])
def strava_disconnect():
    uid = get_uid()
    if not uid: return jsonify({"ok": False, "error": "not logged in"}), 401
    conn = get_db(); c = conn.cursor()
    c.execute("UPDATE users SET strava_token=NULL WHERE id=?", (uid,))
    conn.commit(); conn.close()
    return jsonify({"ok": True, "message": "Strava disconnected"})


@app.route("/strava/sync")
def strava_sync():
    """Fetch recent Strava activities and create territories from them."""
    uid = get_uid()
    if not uid: return jsonify({"ok": False, "error": "not logged in"}), 401

    conn = get_db(); c = conn.cursor()
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()

    if not user or not user["strava_token"]:
        return jsonify({"ok": False, "error": "Strava not connected"}), 400

    # Fetch recent activities from Strava
    r = requests.get("https://www.strava.com/api/v3/athlete/activities",
                     headers={"Authorization": f"Bearer {user['strava_token']}"},
                     params={"per_page": 10, "page": 1})
    if r.status_code != 200:
        return jsonify({"ok": False, "error": "Strava API error", "status": r.status_code}), 400

    activities = r.json()
    synced = []
    for act in activities:
        # Only process runs with a map polyline
        if act.get("type") not in ["Run", "Walk", "Hike"]: continue
        polyline = act.get("map", {}).get("summary_polyline", "")
        if not polyline: continue

        # Decode polyline
        try:
            pts = decode_polyline(polyline)
        except:
            continue

        if len(pts) < 4: continue

        # Check if already synced (by Strava activity ID)
        act_id = str(act["id"])
        conn = get_db(); c = conn.cursor()
        existing = c.execute(
            "SELECT id FROM territories WHERE name LIKE ?",
            (f"%strava_{act_id}%",)
        ).fetchone()

        if existing:
            conn.close()
            continue  # Already imported

        # Build polygon from route
        if SHAPELY_AVAILABLE:
            try:
                from shapely.geometry import LineString
                line = LineString([(p[1],p[0]) for p in pts])
                buffered = make_valid(line.buffer(0.0003, cap_style=1, join_style=1))
                if not buffered.is_empty and buffered.geom_type == 'Polygon':
                    polygon = [[c[1],c[0]] for c in buffered.exterior.coords]
                else:
                    polygon = convex_hull(pts)
            except:
                polygon = convex_hull(pts)
        else:
            polygon = convex_hull(pts)

        area   = round(polygon_area_km2(polygon), 4)
        tid    = "t_" + uuid.uuid4().hex[:8]
        name   = act.get("name", "Strava Run") + f" (strava_{act_id})"
        now    = datetime.utcnow().isoformat() + "Z"
        color  = user["color"]

        user_row = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        c.execute("INSERT INTO territories VALUES (?,?,?,?,?,?,?)",
                  (tid, uid, name, json.dumps(polygon), area, now, color))
        c.execute("UPDATE users SET zones=zones+1, total_km=total_km+? WHERE id=?",
                  (round(act.get("distance", 0) / 1000, 2), uid))
        conn.commit()
        conn.close()
        synced.append(act.get("name", "Run"))

    return jsonify({"ok": True, "synced": len(synced), "activities": synced})


def decode_polyline(polyline_str):
    """Decode Google encoded polyline to list of [lat, lng]."""
    index, lat, lng = 0, 0, 0
    coordinates = []
    changes = {"latitude": 0, "longitude": 0}
    while index < len(polyline_str):
        for unit in ["latitude", "longitude"]:
            chunk, shift = 0, 0
            while True:
                b = ord(polyline_str[index]) - 63
                index += 1
                chunk |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20: break
            if chunk & 1:
                changes[unit] = ~(chunk >> 1)
            else:
                changes[unit] = chunk >> 1
        lat += changes["latitude"]
        lng += changes["longitude"]
        coordinates.append([lat / 100000.0, lng / 100000.0])
    return coordinates


@app.route("/strava/callback")
def strava_callback():
    code  = request.args.get("code")
    if not code: return "Missing code", 400

    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code"
    })
    data = r.json()
    print("Strava token response:", data)  # Log full response to Railway logs
    if "access_token" not in data:
        return f"Strava auth error: {data}", 400

    athlete   = data.get("athlete", {})
    strava_id = str(athlete.get("id", ""))
    firstname = athlete.get("firstname", "")
    lastname  = athlete.get("lastname", "")
    name      = (firstname + " " + lastname).strip() or "Strava Runner"
    avatar    = (firstname[:1] + lastname[:1]).upper() or "SR"
    photo_url = athlete.get("profile_medium","") or athlete.get("profile","")
    uid       = "strava_" + strava_id

    conn = get_db(); c = conn.cursor()

    # Add photo_url column if not exists
    try:
        c.execute("ALTER TABLE users ADD COLUMN photo_url TEXT")
        conn.commit()
    except: pass

    existing = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    now = datetime.utcnow().isoformat() + "Z"

    if existing:
        c.execute(
            "UPDATE users SET name=?, strava_token=?, avatar=?, photo_url=? WHERE id=?",
            (name, data["access_token"], avatar, photo_url, uid)
        )
    else:
        count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        color = assign_color(count)
        c.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?)",
                  (uid, name, avatar, color, 0.0, 0, data["access_token"], now))
        c.execute("UPDATE users SET photo_url=? WHERE id=?", (photo_url, uid))

    conn.commit()
    conn.close()
    session["user_id"] = uid

    frontend_url = os.getenv("FRONTEND_URL", "https://play.8me.in")
    return redirect(frontend_url + "/?uid=" + uid + "&strava=connected")

@app.route("/api/strava/activities")
def strava_activities():
    uid = get_uid()
    if not uid: return jsonify({"error":"not logged in"}),401
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    if not user or not user["strava_token"]:
        return jsonify({"error":"Strava not connected"}),400
    r = requests.get("https://www.strava.com/api/v3/athlete/activities",
                     headers={"Authorization":f"Bearer {user['strava_token']}"},
                     params={"per_page":10})
    result = []
    for a in r.json():
        if a.get("type") in ("Run","Walk","Hike"):
            result.append({"id":a["id"],"name":a["name"],
                           "distance_km":round(a.get("distance",0)/1000,2),
                           "date":a.get("start_date_local",""),
                           "moving_time_s":a.get("moving_time",0)})
    return jsonify(result)

# ── Admin Panel ───────────────────────────────────────────────────────────────
import functools, hashlib as _hl

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        # Accept token from header, cookie, or query param (for cross-domain reliability)
        token = (request.headers.get("X-Admin-Token")
                 or request.cookies.get("admin_token")
                 or request.args.get("_at"))
        expected = _hl.sha256(ADMIN_PASSWORD.encode()).hexdigest()
        if not token or token != expected:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

@app.route("/admin/login", methods=["POST"])
def admin_login():
    data = request.json or {}
    pw   = data.get("password", "")
    if pw != ADMIN_PASSWORD:
        return jsonify({"ok": False, "error": "Wrong password"}), 401
    token = _hl.sha256(ADMIN_PASSWORD.encode()).hexdigest()
    resp  = jsonify({"ok": True, "token": token})
    resp.set_cookie("admin_token", token, httponly=True, secure=True,
                    samesite="None", max_age=86400 * 7)
    return resp

@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    resp = jsonify({"ok": True})
    resp.delete_cookie("admin_token")
    return resp

@app.route("/admin/stats")
@admin_required
def admin_stats():
    conn = get_db()
    try:
        # Ensure columns exist
        for col in ["username TEXT", "photo_url TEXT", "email TEXT", "created_at TEXT"]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col}")
                conn.commit()
            except: pass

        users = conn.execute("""
            SELECT u.id, u.name, COALESCE(u.username,'') as username,
                   COALESCE(u.email,'') as email,
                   u.color, u.avatar,
                   u.total_km, u.zones,
                   COALESCE(u.created_at,'') as created_at,
                   COALESCE(u.photo_url,'') as photo_url,
                   COUNT(t.id) as territory_count,
                   COALESCE(SUM(t.area_km2),0) as total_area
            FROM users u
            LEFT JOIN territories t ON t.user_id = u.id
            GROUP BY u.id
            ORDER BY u.total_km DESC
        """).fetchall()

        total_territories = conn.execute("SELECT COUNT(*) FROM territories").fetchone()[0]
        total_users       = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_km          = conn.execute("SELECT COALESCE(SUM(total_km),0) FROM users").fetchone()[0]

        # Rank users by total_km
        user_list = []
        for i, u in enumerate(users):
            d = dict(u)
            d["rank"] = i + 1
            # Determine auth type from id prefix
            if d["id"].startswith("strava_"):
                d["auth_type"] = "Strava"
            elif d["id"].startswith("google_"):
                d["auth_type"] = "Google"
            else:
                d["auth_type"] = "Guest"
            user_list.append(d)

        return jsonify({
            "ok": True,
            "summary": {
                "total_users": total_users,
                "total_territories": total_territories,
                "total_km": round(total_km, 2),
            },
            "users": user_list
        })
    finally:
        conn.close()

@app.route("/admin/user/<uid>", methods=["DELETE"])
@admin_required
def admin_delete_user(uid):
    """Remove a user and all their territories."""
    conn = get_db(); c = conn.cursor()
    try:
        user = c.execute("SELECT name FROM users WHERE id=?", (uid,)).fetchone()
        if not user:
            conn.close()
            return jsonify({"ok": False, "error": "User not found"}), 404
        c.execute("DELETE FROM territories WHERE user_id=?", (uid,))
        c.execute("DELETE FROM push_subscriptions WHERE user_id=?", (uid,))
        c.execute("DELETE FROM users WHERE id=?", (uid,))
        conn.commit()
        print(f"ADMIN: Deleted user {uid} ({user['name']})")
        return jsonify({"ok": True, "deleted": uid, "name": user["name"]})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()

@app.route("/admin/user/<uid>/territories")
@admin_required
def admin_user_territories(uid):
    """Get all territories for a specific user."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT id, name, area_km2, captured_at,
                   total_km, avg_pace, duration
            FROM territories WHERE user_id=?
            ORDER BY captured_at DESC
        """, (uid,)).fetchall()
        return jsonify({"ok": True, "territories": [dict(r) for r in rows]})
    finally:
        conn.close()

@app.route("/admin/user/<uid>/notify", methods=["POST"])
@admin_required
def admin_notify_user(uid):
    """Send a push notification to a specific user from the admin panel."""
    if not WEBPUSH_AVAILABLE or not VAPID_PRIVATE_KEY:
        return jsonify({"ok": False, "error": "Push notifications not configured — check VAPID keys in Railway env vars"}), 400

    data  = request.json or {}
    title = data.get("title", "").strip()
    body  = data.get("body", "").strip()
    url   = data.get("url", "/").strip() or "/"

    if not title or not body:
        return jsonify({"ok": False, "error": "Title and message are required"}), 400

    # Check user exists
    conn = get_db()
    try:
        user = conn.execute("SELECT name FROM users WHERE id=?", (uid,)).fetchone()
        if not user:
            return jsonify({"ok": False, "error": "User not found"}), 404

        subs = conn.execute(
            "SELECT subscription FROM push_subscriptions WHERE user_id=?", (uid,)
        ).fetchall()
    finally:
        conn.close()

    if not subs:
        return jsonify({
            "ok": False,
            "error": f"{user['name']} has no push subscription — they may not have granted notification permission"
        }), 400

    # Fire the push using the existing send_push helper
    sent = 0; failed = 0
    payload = json.dumps({"title": title, "body": body, "url": url, "tag": "admin"})
    for row in subs:
        try:
            webpush(
                subscription_info=json.loads(row["subscription"]),
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_EMAIL}
            )
            sent += 1
        except WebPushException as e:
            print(f"Admin push failed for {uid}: {e}")
            if "410" in str(e):
                # Clean up expired subscription
                conn2 = get_db()
                conn2.execute("DELETE FROM push_subscriptions WHERE subscription=?", (row["subscription"],))
                conn2.commit(); conn2.close()
            failed += 1
        except Exception as e:
            print(f"Admin push error: {e}")
            failed += 1

    if sent == 0:
        return jsonify({"ok": False, "error": "All subscriptions expired or failed — user may need to re-enable notifications"}), 400

    print(f"ADMIN: Sent push to {uid} ({user['name']}): '{title}'")
    return jsonify({"ok": True, "sent": sent, "failed": failed, "user": user["name"]})


# ── Boot ──────────────────────────────────────────────────────────────────────
init_db()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"Infinite Me v2 — http://0.0.0.0:{port}")
    app.run(debug=False, port=port, host="0.0.0.0")
