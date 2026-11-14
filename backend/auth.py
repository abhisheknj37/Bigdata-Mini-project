import os
from datetime import datetime, timedelta, timezone
from functools import wraps

import jwt
from flask import current_app, g, jsonify, request
from werkzeug.security import check_password_hash, generate_password_hash

from db import connect


def init_auth():
    production = os.getenv("APP_ENV") == "production"
    accounts = [
        (os.getenv("ADMIN_USERNAME", "admin"), os.getenv("ADMIN_PASSWORD"), "admin"),
        (os.getenv("ANALYST_USERNAME", "analyst"), os.getenv("ANALYST_PASSWORD"), "analyst"),
    ]

    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL
            )
        """)
        for username, password, role in accounts:
            if production and not password:
                raise RuntimeError(f"{role.upper()}_PASSWORD must be configured in production")
            if not password:
                password = "Admin@123" if role == "admin" else "Analyst@123"
            conn.execute(
                """INSERT INTO app_users(username, password_hash, role) VALUES(?,?,?)
                   ON CONFLICT(username) DO UPDATE SET
                   password_hash=excluded.password_hash, role=excluded.role""",
                (username, generate_password_hash(password), role),
            )


def login_user(username, password):
    with connect() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, role FROM app_users WHERE username=?",
            (username,),
        ).fetchone()
    if row and check_password_hash(row["password_hash"], password):
        return {"id": row["id"], "username": row["username"], "role": row["role"]}
    return None


def create_access_token(user):
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": str(user["id"]), "iat": now, "exp": now + timedelta(hours=8)},
        current_app.config["JWT_SECRET_KEY"],
        algorithm="HS256",
    )


def require_auth(*roles):
    def deco(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            try:
                payload = jwt.decode(
                    token,
                    current_app.config["JWT_SECRET_KEY"],
                    algorithms=["HS256"],
                )
            except jwt.InvalidTokenError:
                return jsonify(error="Authentication required or session expired"), 401

            with connect() as conn:
                row = conn.execute(
                    "SELECT id, username, role FROM app_users WHERE id=?",
                    (payload.get("sub"),),
                ).fetchone()
            if not row:
                return jsonify(error="User account not found"), 401

            g.user = {"id": row["id"], "username": row["username"], "role": row["role"]}
            if roles and g.user["role"] not in roles:
                return jsonify(error="Permission denied"), 403
            return fn(*args, **kwargs)
        return wrapped
    return deco
