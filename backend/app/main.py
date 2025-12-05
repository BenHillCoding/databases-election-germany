from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from .db import engine, init_db

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

@app.on_event("startup")
def on_startup():
    init_db()

@app.get("/api/items")
def get_items():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM items ORDER BY id")).mappings().all()
        return {"items": [dict(r) for r in rows]}
