import psycopg2
import csv
import io
import os
from dotenv import load_dotenv

load_dotenv()
CSV_PATH = "/app/python_scripts/daten/kerg2_00413.csv"

def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )


def drop_constraints(cur):
    # Entferne PKs und Indizes für maximale Geschwindigkeit
    cur.execute("ALTER TABLE erststimme DROP CONSTRAINT IF EXISTS erststimme_pkey CASCADE;")
    cur.execute("ALTER TABLE zweitstimme DROP CONSTRAINT IF EXISTS zweitstimme_pkey CASCADE;")

def recreate_constraints(cur):
    # Nach dem Laden neu erstellen
    cur.execute("ALTER TABLE erststimme ADD COLUMN id SERIAL PRIMARY KEY;")
    cur.execute("ALTER TABLE zweitstimme ADD COLUMN id SERIAL PRIMARY KEY;")

def main():
    conn = db_connect()
    with conn.cursor() as cur, open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        # Wahl-ID für 2025 holen
        cur.execute("SELECT nummer FROM wahl WHERE datum = '2025-02-23'")
        row = cur.fetchone()
        if not row:
            print("Wahl 2025 nicht gefunden.")
            return
        wahl_id_2025 = row[0]

        # Constraints entfernen
        drop_constraints(cur)

        # Buffer für COPY
        erst_buffer = io.StringIO()
        zweit_buffer = io.StringIO()

        for row in reader:
            if row.get("Gebietsart") != "Wahlkreis":
                continue

            try:
                wahlkreis_id = int(row.get("Gebietsnummer"))
                anzahl = int(row.get("Anzahl"))
            except (TypeError, ValueError):
                continue
            if anzahl <= 0:
                continue

            stimme = row.get("Stimme")
            gruppenname = row.get("Gruppenname")
            kuerzel = row.get("Gruppenname")

            # Wahlkreisergebnis-ID holen
            cur.execute(
                "SELECT id FROM wahlkreisergebnis WHERE wahl_id = %s AND wahlkreis_id = %s",
                (wahl_id_2025, wahlkreis_id)
            )
            res = cur.fetchone()
            if not res:
                continue
            wahlkreisergebnis_id = res[0]

            if gruppenname == "Ungültige":
                if stimme == "1":
                    for _ in range(anzahl):
                        erst_buffer.write(f"{wahlkreisergebnis_id}\tfalse\t\\N\t1\n")
                elif stimme == "2":
                    for _ in range(anzahl):
                        zweit_buffer.write(f"{wahlkreisergebnis_id}\tfalse\t\\N\t1\n")
            else:
                cur.execute("SELECT id FROM partei WHERE kuerzel = %s", (kuerzel,))
                res = cur.fetchone()
                if not res:
                    continue
                partei_id = res[0]

                if stimme == "1":
                    cur.execute(
                        "SELECT id FROM direktkandidatur WHERE wahl_id = %s AND wahlkreis_id = %s AND partei_id = %s",
                        (wahl_id_2025, wahlkreis_id, partei_id)
                    )
                    res = cur.fetchone()
                    if not res:
                        continue
                    direkt_id = res[0]
                    for _ in range(anzahl):
                        erst_buffer.write(f"{wahlkreisergebnis_id}\ttrue\t{direkt_id}\t1\n")
                elif stimme == "2":
                    for _ in range(anzahl):
                        zweit_buffer.write(f"{wahlkreisergebnis_id}\ttrue\t{partei_id}\t1\n")

        # Bulk COPY
        print("Bulk Copy folgt.")
        erst_buffer.seek(0)
        zweit_buffer.seek(0)
        cur.copy_expert("COPY erststimme (wahlkreisergebnis_id, gueltig, direktkandidatur_id, anzahl) FROM STDIN", erst_buffer)
        cur.copy_expert("COPY zweitstimme (wahlkreisergebnis_id, gueltig, partei_id, anzahl) FROM STDIN", zweit_buffer)

        # Constraints neu erstellen
        # recreate_constraints(cur)

    conn.commit()
    conn.close()
    print("Votes 2025 erfolgreich geladen.")

if __name__ == "__main__":
    main()
