import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def db_connect():
    return psycopg2.connect(**DB_CONFIG)

def drop_all_tables(cur):
    # Drop in reverse dependency order to avoid FK issues
    tables = [
        "zweitstimme",
        "erststimme",
        "wahlkreisergebnis",
        "direktkandidatur",
        "listenplatz",
        "landesliste",
        "kandidat",
        "wahlkreis",
        "wahl",
        "bundesland",
        "partei"
    ]
    for t in tables:
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")
    print("✅ Dropped all existing tables.")

def create_core_schema(cur):
    # Partei
    cur.execute("""
        CREATE TABLE partei (
            id SERIAL PRIMARY KEY,
            kuerzel TEXT UNIQUE,
            name TEXT,
            nationale_minderheit BOOLEAN DEFAULT FALSE
        )
    """)

    # Bundesland
    cur.execute("""
        CREATE TABLE bundesland (
            id INT PRIMARY KEY,
            name TEXT UNIQUE
        )
    """)

    # Wahl
    cur.execute("""
        CREATE TABLE wahl (
            nummer INT PRIMARY KEY,
            datum DATE
        )
    """)

    # Wahlkreis
    cur.execute("""
        CREATE TABLE wahlkreis (
            id SERIAL PRIMARY KEY,
            nummer INT UNIQUE,
            name TEXT,
            bundesland_id INT REFERENCES bundesland(id)
        )
    """)

    # Kandidat
    cur.execute("""
        CREATE TABLE kandidat (
            id SERIAL PRIMARY KEY,
            vorname TEXT,
            nachname TEXT,
            geburtsjahr INT,
            partei_id INT REFERENCES partei(id),
            UNIQUE(vorname, nachname, geburtsjahr, partei_id)
        )
    """)

    # Landesliste
    cur.execute("""
        CREATE TABLE landesliste (
            id SERIAL PRIMARY KEY,
            partei_id INT REFERENCES partei(id),
            wahl_id INT REFERENCES wahl(nummer),
            bundesland_id INT REFERENCES bundesland(id),
            UNIQUE(partei_id, wahl_id, bundesland_id)
        )
    """)

    # Listenplatz
    cur.execute("""
        CREATE TABLE listenplatz (
            id SERIAL PRIMARY KEY,
            landesliste_id INT REFERENCES landesliste(id),
            kandidat_id INT REFERENCES kandidat(id),
            position INT,
            UNIQUE(landesliste_id, kandidat_id)
        )
    """)

    # Direktkandidatur
    cur.execute("""
        CREATE TABLE direktkandidatur (
            id SERIAL PRIMARY KEY,
            kandidat_id INT REFERENCES kandidat(id),
            wahl_id INT REFERENCES wahl(nummer),
            wahlkreis_id INT REFERENCES wahlkreis(id),
            partei_id INT REFERENCES partei(id),
            UNIQUE(kandidat_id, wahl_id, wahlkreis_id)
        )
    """)

    # Wahlkreisergebnis
    cur.execute("""
        CREATE TABLE wahlkreisergebnis (
            id SERIAL PRIMARY KEY,
            wahlkreis_id INT,
            wahl_id INT,
            wahlberechtigte INT,
            UNIQUE(wahlkreis_id, wahl_id)
        )
    """)

    # Erststimme (no constraints for speed)
    cur.execute("""
        CREATE TABLE erststimme (
            id SERIAL PRIMARY KEY,
            wahlkreisergebnis_id INT,
            gueltig BOOLEAN,
            direktkandidatur_id INT
        )
    """)

    # Zweitstimme (no constraints for speed)
    cur.execute("""
        CREATE TABLE zweitstimme (
            id SERIAL PRIMARY KEY,
            wahlkreisergebnis_id INT,
            gueltig BOOLEAN,
            partei_id INT
        )
    """)

def main():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            drop_all_tables(cur)
            create_core_schema(cur)
        conn.commit()
        print("Schema dropped and recreated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
