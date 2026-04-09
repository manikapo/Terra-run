"""
Infinite Me — Backend v2
8me.in | Run. Claim. Own your world.
pip install flask flask-cors requests python-dotenv gunicorn shapely
"""

from flask import Flask, jsonify, request, redirect, session
import json, math, os, uuid, hashlib, sqlite3
from datetime import datetime
import requests

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
    "https://web-production-4077c.up.railway.app",
    "http://localhost:5500",
    "http://localhost:3000",
    "http://127.0.0.1:5500",
]

def get_cors_origin(request_origin):
    """Return the origin if allowed, else the primary frontend."""
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return "https://play.8me.in"

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = app.make_response("")
        origin = get_cors_origin(request.headers.get("Origin"))
        res.headers["Access-Control-Allow-Origin"]      = origin
        res.headers["Access-Control-Allow-Headers"]     = "Content-Type,X-User-Id,Authorization,Accept"
        res.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,DELETE,OPTIONS"
        res.headers["Access-Control-Allow-Credentials"] = "true"
        res.headers["Access-Control-Max-Age"]           = "3600"
        res.headers["Vary"]                             = "Origin"
        res.status_code = 204
        return res

@app.after_request
def after_request(response):
    origin = get_cors_origin(request.headers.get("Origin"))
    response.headers["Access-Control-Allow-Origin"]      = origin
    response.headers["Access-Control-Allow-Headers"]     = "Content-Type,X-User-Id,Authorization,Accept"
    response.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"]           = "3600"
    response.headers["Vary"]                             = "Origin"
    return response



# ── Config ────────────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID", "YOUR_STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "YOUR_STRAVA_CLIENT_SECRET")
STRAVA_REDIRECT_URI  = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:5000/strava/callback")
DB_PATH              = os.getenv("DB_PATH", "terra_run.db")

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
    """)

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
    
    # If client sends existing uid, try to restore that session first
    existing_uid = data.get("uid") or data.get("_uid")
    
    conn = get_db(); c = conn.cursor()
    
    # Try to find user by existing uid first
    if existing_uid:
        user = c.execute("SELECT * FROM users WHERE id=?", (existing_uid,)).fetchone()
        if user:
            conn.close()
            session["user_id"] = existing_uid
            return jsonify({"ok": True, "user": dict(user)})
    
    # Otherwise create/find by name hash
    uid = "u_" + hashlib.md5(name.lower().encode()).hexdigest()[:10]
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        color = assign_color(count)
        now   = datetime.utcnow().isoformat()+"Z"
        c.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?)",
                  (uid,name,name[:2].upper(),color,0.0,0,None,now))
        conn.commit()
        user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    session["user_id"] = uid
    return jsonify({"ok": True, "user": dict(user)})

@app.route("/api/me")
def me():
    uid = get_uid()
    if not uid: return jsonify({"ok":False,"error":"not logged in"}),401
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    if not user: return jsonify({"ok":False,"error":"user not found"}),404
    return jsonify({"ok":True,"user":dict(user)})

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
                   COALESCE(u.photo_url,'') as owner_photo
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
        gps      = data.get("gps_points", [])
        name     = data.get("name", f"Run {uuid.uuid4().hex[:4].upper()}")
        sim_mode = data.get("sim_mode", False)

        if len(gps) < 4:
            return jsonify({"ok": False, "error": "Need at least 4 GPS points"}), 400

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
                        su = c.execute("SELECT name FROM users WHERE id=?", (t["user_id"],)).fetchone()
                        if su: stolen_names.append(su["name"])
                    else:
                        remaining = clip_polygon(t_poly, polygon)
                        if not remaining:
                            c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
                            c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (t["user_id"],))
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

            # Remove own overlapping zones
            my_t = c.execute("SELECT * FROM territories WHERE user_id=? AND id!=?", (uid, tid)).fetchall()
            for t in my_t:
                try:
                    t_poly = json.loads(t["polygon"])
                    if polygons_truly_overlap(polygon, t_poly):
                        c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
                        c.execute("UPDATE users SET zones=MAX(0,zones-1) WHERE id=?", (uid,))
                except: pass

            conn.commit()
        except Exception as steal_outer_err:
            print("Steal outer error:", steal_outer_err)
            conn.commit()  # commit the save even if steal fails

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

@app.route("/api/admin/test-save")
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

# ── Strava OAuth ──────────────────────────────────────────────────────────────
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

# ── Boot ──────────────────────────────────────────────────────────────────────
init_db()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"Infinite Me v2 — http://0.0.0.0:{port}")
    app.run(debug=False, port=port, host="0.0.0.0")
