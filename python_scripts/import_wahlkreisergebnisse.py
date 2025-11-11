import psycopg2
import csv
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 


CSV_PATH = "python_scripts/daten/kerg2_00413.csv"

def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def get_wahl_id(cur, datum):
    cur.execute("SELECT nummer FROM wahl WHERE datum = %s", (datum,))
    row = cur.fetchone()
    return row[0] if row else None

def insert_wahlkreisergebnis(cur, wahl_id, wahlkreis_id, wahlberechtigte):
    cur.execute("""
        INSERT INTO wahlkreisergebnis (wahl_id, wahlkreis_id, wahlberechtigte)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """, (wahl_id, wahlkreis_id, wahlberechtigte))

# Fills wahlkreisergebnis table
def main():
    conn = db_connect()
    with conn.cursor() as cur, open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            # We only care about rows with Gruppenname == "Wahlberechtigte"
            if row.get("Gruppenname") != "Wahlberechtigte" or row.get("Gebietsart") != "Wahlkreis":
                continue

            datum = row.get("Wahltag")
            wahl_id = get_wahl_id(cur, datum)
            if not wahl_id:
                print(f"Wahl nicht gefunden für Datum {datum}")
                continue

            wahlkreis_id = int(row.get("Gebietsnummer"))
            # For 2025 use "Anzahl", for 2021 use "VorpAnzahl"
            try:
                wahlberechtigte_2025 = int(row.get("Anzahl"))
            except (TypeError, ValueError):
                wahlberechtigte_2025 = None
            try:
                wahlberechtigte_2021 = int(row.get("VorpAnzahl"))
            except (TypeError, ValueError):
                wahlberechtigte_2021 = None

            # Insert 2025 result
            if wahlberechtigte_2025 is not None:
                insert_wahlkreisergebnis(cur, wahl_id, wahlkreis_id, wahlberechtigte_2025)

            # Insert 2021 result (find wahl_id for 2021)
            if wahlberechtigte_2021 is not None:
                wahl_id_2021 = get_wahl_id(cur, "26.09.2021")  # adjust to actual 2021 date in your wahl table
                if wahl_id_2021:
                    insert_wahlkreisergebnis(cur, wahl_id_2021, wahlkreis_id, wahlberechtigte_2021)

    conn.commit()
    conn.close()
    print("Wahlkreisergebnis-Import abgeschlossen.")

if __name__ == "__main__":
    main()
