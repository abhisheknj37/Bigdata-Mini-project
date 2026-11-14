import os
from flask import Flask, jsonify, request, g
from flask_cors import CORS
from pathlib import Path
import subprocess, sys, sqlite3
from config import BASE_DIR, DB_PATH, SAMPLE_DIR, UPLOAD_DIR
from db import connect
from auth import init_auth, login_user, create_access_token, require_auth

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY") or "local-development-only-change-this-secret"
if os.getenv("APP_ENV") == "production" and not os.getenv("JWT_SECRET_KEY"):
    raise RuntimeError("JWT_SECRET_KEY must be configured in production")

origins = [x.strip() for x in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",") if x.strip()]
CORS(app, resources={r"/api/*": {"origins": origins}})
init_auth()


@app.get("/api/health")
def health():
    return jsonify(status="ok", database_exists=DB_PATH.exists(), mode="local-sqlite")


@app.post("/api/login")
def login():
    d = request.get_json(silent=True) or {}
    user = login_user(d.get("username", ""), d.get("password", ""))
    if not user:
        return jsonify(error="Invalid username or password"), 401
    return jsonify(user=user, token=create_access_token(user))


@app.get("/api/me")
@require_auth()
def me():
    return jsonify(g.user)


@app.post("/api/pipeline/run")
@require_auth("admin")
def run_pipeline():
    script = BASE_DIR / "scripts" / "run_local_warehouse.py"
    result = subprocess.run([sys.executable, str(script)], cwd=BASE_DIR, capture_output=True, text=True, timeout=180)
    return jsonify(success=result.returncode == 0, output=result.stdout[-10000:], error=result.stderr[-3000:]), (200 if result.returncode == 0 else 500)


@app.get("/api/overview")
@require_auth()
def overview():
    if not DB_PATH.exists():
        return jsonify(error="Warehouse not initialized. Admin must run the pipeline."), 404
    with connect() as c:
        def scalar(sql):
            try:
                return c.execute(sql).fetchone()[0] or 0
            except sqlite3.Error:
                return 0

        cards = {
            "users": scalar("SELECT COUNT(*) FROM dim_user"),
            "content": scalar("SELECT COUNT(*) FROM dim_content"),
            "streams": scalar("SELECT COUNT(*) FROM fact_content_streaming"),
            "watch_hours": round(scalar("SELECT SUM(duration_watched_minutes)/60.0 FROM fact_content_streaming"), 1),
            "revenue": scalar("SELECT SUM(CAST(amount_billed_usd AS REAL)) FROM stg_subscription_billing"),
        }
        top = [dict(r) for r in c.execute("SELECT c.title,c.genre,COUNT(*) views,ROUND(SUM(f.duration_watched_minutes)/60.0,1) watch_hours FROM fact_content_streaming f JOIN dim_content c USING(content_key) GROUP BY c.title,c.genre ORDER BY views DESC LIMIT 8")]
        genres = [dict(r) for r in c.execute("SELECT c.genre,COUNT(*) streams FROM fact_content_streaming f JOIN dim_content c USING(content_key) GROUP BY c.genre ORDER BY streams DESC")]
        tiers = [dict(r) for r in c.execute("SELECT u.subscription_tier tier,COUNT(*) streams,ROUND(AVG(f.quality_score),2) qoe,ROUND(AVG(f.buffering_events),2) avg_buffering FROM fact_content_streaming f JOIN dim_user u USING(user_key) GROUP BY u.subscription_tier ORDER BY streams DESC")]
        revenue = [dict(r) for r in c.execute("SELECT substr(billing_date,1,7) month,ROUND(SUM(CAST(amount_billed_usd AS REAL)),2) revenue FROM stg_subscription_billing GROUP BY month ORDER BY month")]
        recent = [dict(r) for r in c.execute("SELECT c.title,c.genre,f.duration_watched_minutes,f.completion_percentage,f.buffering_events,f.quality_score FROM fact_content_streaming f JOIN dim_content c USING(content_key) ORDER BY f.stream_key DESC LIMIT 12")]
    return jsonify(cards=cards, top_content=top, genres=genres, tiers=tiers, monthly_revenue=revenue, recent_activity=recent)


@app.post("/api/upload")
@require_auth("admin")
def upload():
    f = request.files.get("file")
    if not f:
        return jsonify(error="Select a CSV file"), 400
    if not f.filename.lower().endswith(".csv"):
        return jsonify(error="Only CSV files are accepted"), 400
    name = Path(f.filename).name
    allowed = {"staging_content.csv", "staging_viewing_activity.csv", "staging_subscription_billing.csv", "staging_users.csv", "staging_devices.csv", "staging_subscription_plans.csv", "staging_user_interactions.csv"}
    UPLOAD_DIR.mkdir(exist_ok=True)
    target = (SAMPLE_DIR / name) if name in allowed else (UPLOAD_DIR / name)
    target.parent.mkdir(parents=True, exist_ok=True)
    f.save(target)
    return jsonify(filename=name, status="staging dataset replaced" if name in allowed else "saved to uploads (not mapped to pipeline)")


@app.get("/api/hadoop/status")
@require_auth()
def hadoop_status():
    return jsonify(
        artifacts={x: (BASE_DIR / x).exists() for x in ["hive", "pig", "mapreduce", "sqoop", "oozie"]},
        mode="Local SQLite simulator",
        cluster="Not connected",
        note="Hadoop/Hive scripts are included, but a running cluster must be configured separately.",
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=os.getenv("APP_ENV") != "production")
