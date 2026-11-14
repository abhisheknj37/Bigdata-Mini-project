# Media & Entertainment Data Warehouse

A full-stack analytics dashboard built with React, Flask, SQLite and a set of Hadoop ecosystem artifacts. It demonstrates a media warehouse workflow with sample datasets, dimensional modelling, streaming-quality metrics and role-based access.

## Features
- Admin and Analyst access with signed, expiring JWT authentication.
- KPI dashboard, content and genre analytics, subscriptions, revenue and streaming QoE.
- Admin-only CSV upload and local ETL pipeline execution.
- Hadoop/Hive, Pig, Java MapReduce, Sqoop and Oozie artifacts.
- SQLite local simulator and sample CSV data.

## Tech stack
- Frontend: React 18, Vite, Axios, Chart.js.
- Backend: Python, Flask, Flask-CORS, PyJWT, Gunicorn.
- Warehouse: SQLite; sample ETL builds staging, dimension and fact tables.

## Run locally

Backend (terminal 1):
```bash
cd backend
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Frontend (terminal 2):
```bash
cd frontend
npm install
npm run dev
```
Open http://localhost:5173.

Local demo credentials: `admin / Admin@123` and `analyst / Analyst@123`. These are for local development only and are not used by the production deployment.

## Live links
- **Live dashboard:** https://mediavault-dashboard-a-star-codes.vercel.app
- **Portfolio:** https://abhisheknj37-portfolio.vercel.app

## Deploy

### 1. Flask API on Render
Create a Python web service connected to this repository:
- Build command: `pip install -r backend/requirements.txt`
- Start command: `cd backend && gunicorn app:app --bind 0.0.0.0:$PORT`
- Region: Singapore (or the nearest available region).
- Set environment variables:
  - `APP_ENV=production`
  - `JWT_SECRET_KEY` = a unique random secret (at least 32 random bytes).
  - `ADMIN_USERNAME=admin`
  - `ADMIN_PASSWORD` = a strong private password.
  - `ANALYST_USERNAME=analyst`
  - `ANALYST_PASSWORD` = a separate strong private password.
  - `CORS_ORIGINS` = the deployed Vercel frontend origin, e.g. `https://your-project.vercel.app`.
- The free Render filesystem is ephemeral. Uploaded files and changes to the SQLite database may be lost when the service restarts or redeploys. Use persistent storage or a managed database for production data.

### 2. React frontend on Vercel
- Import this GitHub repository: https://github.com/abhisheknj37/Bigdata-Mini-project
- Root Directory: `frontend`
- Framework: Vite
- Build command: `npm run build`
- Output directory: `dist`
- Add environment variable `VITE_API_URL` with the Render service origin (without `/api`).

## Data pipeline and Hadoop notes
The **Run pipeline** button executes `scripts/run_local_warehouse.py`, which rebuilds `data/media_warehouse.db` from `data/sample/*.csv`. This is a local SQLite demonstration, not distributed Hadoop execution.

The repository includes Hadoop/Hive, Pig, MapReduce, Sqoop and Oozie source artifacts. A real cluster must be configured and operated separately before claiming those distributed jobs have run.

## Security notes
- JWTs are signed with an environment-provided secret and expire after eight hours.
- Roles are loaded from the database on each authenticated request; role values in client tokens are not trusted.
- Production demo passwords must be supplied as private Render environment variables.
- Use a managed database, persistent storage, HTTPS and a review of upload limits/validation before using this as a real business service.
