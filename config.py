# config.py
DB_USER = "postgres"
DB_PASS = "postgres"      # change if different
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "retail_db"

from sqlalchemy.engine import URL
CONN_URL = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)
