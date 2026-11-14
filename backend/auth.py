from functools import wraps
from flask import request, jsonify, g
from werkzeug.security import check_password_hash, generate_password_hash
from db import connect

def init_auth():
    with connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS app_users (id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL)")
        for username, password, role in [('admin','Admin@123','admin'),('analyst','Analyst@123','analyst')]:
            if not conn.execute('SELECT id FROM app_users WHERE username=?',(username,)).fetchone():
                conn.execute('INSERT INTO app_users(username,password_hash,role) VALUES(?,?,?)',(username,generate_password_hash(password),role))

def login_user(username,password):
    with connect() as conn:
        row=conn.execute('SELECT id,username,password_hash,role FROM app_users WHERE username=?',(username,)).fetchone()
    if row and check_password_hash(row['password_hash'],password):
        return {'id':row['id'],'username':row['username'],'role':row['role']}
    return None

def require_auth(*roles):
    def deco(fn):
        @wraps(fn)
        def wrapped(*args,**kwargs):
            token=request.headers.get('Authorization','').removeprefix('Bearer ').strip()
            parts=token.split(':')
            if len(parts)!=3: return jsonify({'error':'Authentication required'}),401
            g.user={'id':parts[0],'username':parts[1],'role':parts[2]}
            if roles and g.user['role'] not in roles: return jsonify({'error':'Permission denied'}),403
            return fn(*args,**kwargs)
        return wrapped
    return deco
