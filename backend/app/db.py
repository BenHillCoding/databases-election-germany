import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10, future=True)


def init_db():
    with engine.begin() as conn:
        # Minimal table for demo
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """))
        # Seed a single row if table is empty
        count = conn.execute(text("SELECT COUNT(*) FROM items")).scalar_one()
        if count == 0:
            conn.execute(text("INSERT INTO items (name) VALUES (:name)"), {"name": "Hello from Postgres"})
