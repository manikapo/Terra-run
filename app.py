"""
TERRA RUN — Flask Backend v2
SQLite persistence + multiplayer ready
pip install flask flask-cors requests python-dotenv gunicorn
"""

from flask import Flask, jsonify, request, redirect, session
from flask_cors import CORS
import json, math, os, uuid, hashlib, sqlite3
from datetime import datetime
import requests

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "terra-run-secret-change-in-prod")

# Cookie settings for cross-domain (Netlify frontend → Railway backend)
app.config["SESSION_COOKIE_SAMESITE"] = "None"
app.config["SESSION_COOKIE_SECURE"]   = True
app.config["SESSION_COOKIE_HTTPONLY"] = True

# Disable flask-cors and handle CORS manually for full control
@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = app.make_response("")
        origin = request.headers.get("Origin", "*")
        res.headers["Access-Control-Allow-Origin"]      = origin
        res.headers["Access-Control-Allow-Headers"]     = "Content-Type,X-User-Id,Authorization,Accept"
        res.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,DELETE,OPTIONS"
        res.headers["Access-Control-Allow-Credentials"] = "true"
        res.headers["Access-Control-Max-Age"]           = "3600"
        res.status_code = 204
        return res

@app.after_request
def after_request(response):
    origin = request.headers.get("Origin", "*")
    response.headers["Access-Control-Allow-Origin"]  = origin
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,X-User-Id,Authorization,Accept"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"] = "3600"
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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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

    # Seed demo users only if table is empty
    count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        demo_users = [
            ("u1","Arjun Sharma","AS","#00ff88",142.3,7),
            ("u2","Priya Mehta","PM","#ff6b35",98.7,5),
            ("u3","Rohan Verma","RV","#00cfff",211.0,12),
            ("u4","Sneha Kapoor","SK","#ff3cac",76.4,3),
            ("u5","Dev Patel","DP","#ffd700",305.8,18),
        ]
        now = datetime.utcnow().isoformat() + "Z"
        for uid,name,av,color,km,zones in demo_users:
            c.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?)",
                      (uid,name,av,color,km,zones,None,now))

        demo_territories = [
            ("t1","u5","Connaught Place Loop",
             [[28.6328,77.2197],[28.6358,77.2227],[28.6338,77.2257],[28.6308,77.2237],[28.6298,77.2207]],
             0.42,"2025-03-28T06:12:00Z","#ffd700"),
            ("t2","u3","India Gate Circuit",
             [[28.6129,77.2295],[28.6159,77.2335],[28.6139,77.2375],[28.6109,77.2345],[28.6099,77.2305]],
             0.61,"2025-03-29T05:45:00Z","#00cfff"),
            ("t3","u1","Lodhi Garden Route",
             [[28.5934,77.2198],[28.5964,77.2238],[28.5944,77.2268],[28.5914,77.2238],[28.5904,77.2208]],
             0.38,"2025-03-30T06:00:00Z","#00ff88"),
            ("t4","u2","Humayun's Tomb Run",
             [[28.5933,77.2507],[28.5963,77.2547],[28.5943,77.2577],[28.5913,77.2547],[28.5903,77.2517]],
             0.29,"2025-03-30T07:30:00Z","#ff6b35"),
            ("t5","u5","Nehru Place Sprint",
             [[28.5491,77.2534],[28.5521,77.2564],[28.5501,77.2594],[28.5471,77.2564],[28.5461,77.2534]],
             0.51,"2025-03-27T06:00:00Z","#ffd700"),
        ]
        for tid,uid,name,poly,area,cap,color in demo_territories:
            c.execute("INSERT INTO territories VALUES (?,?,?,?,?,?,?)",
                      (tid,uid,name,json.dumps(poly),area,cap,color))

    conn.commit()
    conn.close()
    print("Database ready:", DB_PATH)

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
    uid  = "u_" + hashlib.md5(name.lower().encode()).hexdigest()[:10]
    conn = get_db(); c = conn.cursor()
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
    conn = get_db()
    rows = conn.execute("""
        SELECT t.*, u.name as owner_name, u.avatar as owner_avatar
        FROM territories t LEFT JOIN users u ON t.user_id = u.id
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["polygon"] = json.loads(d["polygon"])
        result.append(d)
    return jsonify(result)

@app.route("/api/territories", methods=["POST"])
def create_territory():
    uid = get_uid()
    if not uid: return jsonify({"ok":False,"error":"not logged in"}),401

    data     = request.json or {}
    gps      = data.get("gps_points",[])
    name     = data.get("name", f"Zone {uuid.uuid4().hex[:4].upper()}")
    sim_mode = data.get("sim_mode", False)

    if len(gps) < 4:
        return jsonify({"ok":False,"error":"Need at least 4 GPS points"}),400

    # Build polygon
    if sim_mode:
        polygon = [[float(p[0]),float(p[1])] for p in gps]
        if polygon[0] != polygon[-1]: polygon.append(polygon[0])
    else:
        polygon = convex_hull(gps)

    if len(polygon) < 3:
        return jsonify({"ok":False,"error":"Could not form polygon"}),400

    area = polygon_area_km2(polygon)

    conn = get_db(); c = conn.cursor()

    # Get current user
    user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        conn.close()
        return jsonify({"ok":False,"error":"User not found"}),404

    # Find and remove overlapping zones owned by OTHERS (steal mechanic)
    all_t = c.execute("SELECT * FROM territories WHERE user_id != ?", (uid,)).fetchall()
    stolen_from = []
    for t in all_t:
        t_poly = json.loads(t["polygon"])
        if bbox_overlap(polygon, t_poly):
            stolen_from.append(t["user_id"])
            c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
            c.execute("UPDATE users SET zones = MAX(0, zones-1) WHERE id=?", (t["user_id"],))

    # Get names of stolen-from users
    stolen_names = []
    for suid in stolen_from:
        su = c.execute("SELECT name FROM users WHERE id=?", (suid,)).fetchone()
        if su: stolen_names.append(su["name"])

    # Also remove any overlapping zones owned by THIS user (replace with new one)
    my_t = c.execute("SELECT * FROM territories WHERE user_id=?", (uid,)).fetchall()
    for t in my_t:
        t_poly = json.loads(t["polygon"])
        if bbox_overlap(polygon, t_poly):
            c.execute("DELETE FROM territories WHERE id=?", (t["id"],))
            c.execute("UPDATE users SET zones = MAX(0, zones-1) WHERE id=?", (uid,))

    # Distance estimate
    dist = sum(
        math.sqrt((gps[i][0]-gps[i-1][0])**2+(gps[i][1]-gps[i-1][1])**2)*111
        for i in range(1,len(gps))
    )

    # Insert new territory
    tid  = "t_" + uuid.uuid4().hex[:8]
    now  = datetime.utcnow().isoformat()+"Z"
    color = user["color"]
    c.execute("INSERT INTO territories VALUES (?,?,?,?,?,?,?)",
              (tid, uid, name, json.dumps(polygon), round(area,4), now, color))

    # Update user stats
    c.execute("UPDATE users SET zones=zones+1, total_km=total_km+? WHERE id=?",
              (round(dist,2), uid))

    conn.commit()

    # Fetch updated user
    updated_user = c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()

    msg = "Territory captured!"
    if stolen_names:
        msg += f" Stolen from: {', '.join(stolen_names)} 😈"

    return jsonify({
        "ok": True,
        "territory": {"id":tid,"user_id":uid,"name":name,"polygon":polygon,
                      "area_km2":round(area,4),"captured_at":now,"color":color,
                      "owner_name":updated_user["name"]},
        "stolen_from": stolen_names,
        "message": msg,
        "user": dict(updated_user)
    })

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
    conn = get_db()
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    zone_count = conn.execute("SELECT COUNT(*) FROM territories").fetchone()[0]
    conn.close()
    return jsonify({"ok":True,"message":"TERRA RUN is alive",
                    "users":user_count,"zones":zone_count})

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

# ── Strava OAuth ──────────────────────────────────────────────────────────────
@app.route("/strava/connect")
def strava_connect():
    uid = get_uid()
    if not uid: return jsonify({"error":"Login first"}),401
    url = (f"https://www.strava.com/oauth/authorize"
           f"?client_id={STRAVA_CLIENT_ID}&response_type=code"
           f"&redirect_uri={STRAVA_REDIRECT_URI}&scope=activity:read_all&state={uid}")
    return redirect(url)

@app.route("/strava/callback")
def strava_callback():
    code = request.args.get("code")
    uid  = request.args.get("state")
    if not code or not uid: return "Missing params",400
    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET,
        "code": code, "grant_type": "authorization_code"
    })
    data = r.json()
    if "access_token" not in data: return f"Strava error: {data}",400
    conn = get_db(); c = conn.cursor()
    name = data["athlete"].get("firstname","") + " " + data["athlete"].get("lastname","")
    c.execute("UPDATE users SET strava_token=?, name=? WHERE id=?",
              (data["access_token"], name.strip(), uid))
    conn.commit(); conn.close()
    session["user_id"] = uid
    return redirect("/?strava=connected")

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
    print("TERRA RUN v2 — http://localhost:5000")
    app.run(debug=True, port=5000, host="0.0.0.0")
