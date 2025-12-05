from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from .db import engine, init_db
import psycopg2
import os

app = FastAPI()

# Allow React dev server and containerized frontend
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://frontend:3000",  # container name within compose network
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

@app.on_event("startup")
def on_startup():
    init_db()

@app.get("/api/items")
def get_items():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM items ORDER BY id")).mappings().all()
        return {"items": [dict(r) for r in rows]}

@app.get("/national_summary_25") # Q1
def national_summary_25():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT partei_kuerzel, total_seats_nationwide FROM mv_national_summary_25;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"data": rows}

@app.get("/all_representatives") # Q2
def all_representatives():
    pass

@app.get("/wahlkreise") # Helper
def wahlkreise():
    pass

@app.get("/wahlkreisergebnis/{number}") # Q3, Q7
def wahlkreisergebnis(number: int):
    pass

@app.get("/wahlkreiswinner") # Q4
def wahlkreiswinner():
    pass

@app.get("/direktmandate_without_zweitstimmendeckung") # Q5
def direktmandate_without_zweitstimmendeckung():
    pass

@app.get("/closest_winners") # Q6
def closest_winners():
    pass