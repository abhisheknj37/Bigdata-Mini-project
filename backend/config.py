from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
SAMPLE_DIR = DATA_DIR / 'sample'
DB_PATH = DATA_DIR / 'media_warehouse.db'
UPLOAD_DIR = BASE_DIR / 'uploads'
