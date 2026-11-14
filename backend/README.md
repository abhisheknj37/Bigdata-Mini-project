# Flask API

```powershell
cd backend
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

API: http://127.0.0.1:5000/api/health

Demo accounts: `admin / Admin@123` and `analyst / Analyst@123`.

This is a college-demo authentication implementation. Before production, replace the simple demo token with JWT or secure sessions, use HTTPS, change secrets/passwords, and restrict CORS.
