# Media & Entertainment Data Warehouse — Full-stack mini project

A dark analytics dashboard with React frontend, Python Flask REST API, SQLite local warehouse mode, and the existing Hadoop/Hive ecosystem scripts from the supplied starter project.

## Features
- Role-based demo access: Admin and Analyst.
- KPI dashboard, content/genre analytics, subscription and revenue views, streaming QoE table.
- Admin-only CSV upload and local ETL pipeline execution.
- Hadoop artifact status page for Hive, Pig, Java MapReduce, Sqoop and Oozie.
- Includes original sample datasets, Hive DDL/ETL/analytics, Pig scripts, MapReduce Java source and Oozie workflow files.

## Requirements
- Python 3.10+ recommended
- Node.js 18+ and npm
- VS Code
- Optional: a separately configured Hadoop/Hive cluster for distributed execution

## Run in VS Code (Windows PowerShell)
Open this folder in VS Code. Use two terminals.

### Terminal 1 — backend
```powershell
cd backend
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```
Backend runs at `http://127.0.0.1:5000`.

### Terminal 2 — frontend
```powershell
cd frontend
npm install
npm run dev
```
Open `http://127.0.0.1:5173`.

### Demo accounts
| Role | Username | Password |
|---|---|---|
| Admin | `admin` | `Admin@123` |
| Analyst | `analyst` | `Analyst@123` |

Admin can run the pipeline and upload CSV files. Analyst can view dashboard analytics. After uploading one of the recognized staging CSV files, run the pipeline again to refresh the warehouse.

## Local data pipeline
The dashboard's **Run pipeline** button executes:
```powershell
python scripts\run_local_warehouse.py
```
It rebuilds `data/media_warehouse.db` from `data/sample/*.csv` and creates staging, dimension and fact tables. This is the runnable local demonstration mode.

## Hadoop / Hive mode
The project contains the Hadoop ecosystem artifacts in `config/`, `hive/`, `pig/`, `mapreduce/`, `sqoop/`, and `oozie/`. Review `docs/run_guide.md` and `docs/architecture.md` before cluster execution. The dashboard reports that scripts are present; it does **not** claim a cluster is connected. A real cluster requires Hadoop/HDFS, Hive Metastore/Server2, Java/Hadoop client configuration, HDFS paths, JDBC drivers/credentials for Sqoop, and Oozie configuration. Run the supplied HQL/Pig/MapReduce/Oozie commands in that configured environment.

## Architecture
```text
React + Chart.js (5173)
        | REST / JSON
        v
Flask API (5000) ---- Admin auth / upload / pipeline trigger
        |
        v
SQLite local warehouse <--- Python CSV ETL simulator
        ^
        |
Hadoop/HDFS + Hive + Pig + MapReduce + Sqoop + Oozie
(cluster mode; configure separately)
```

## Important academic / security notes
- Demo credentials and the simple bearer token are for a local mini-project demonstration only. Do not expose this build to the public internet as-is.
- Before real deployment, implement JWT or secure server sessions, password rotation, HTTPS, restrictive CORS, upload validation, and proper role persistence.
- The local mode is a functional simulator, not a substitute for distributed Hadoop execution. Do not claim Hive/MapReduce ran unless you execute them on a working cluster.
