import csv
import psycopg2
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

import os

CSV_PATH_2025 = "/app/python_scripts/daten/btw25_bewerb_utf8.csv"
CSV_PATH_2021 = "/app/python_scripts/daten/btw21_kandidaturen_utf8.csv"


def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )


# --- Lookups ---

def get_wahl_nummer(cur, datum):
    cur.execute("""
        SELECT nummer FROM wahl
        WHERE datum = TO_DATE(%s, 'DD.MM.YYYY')
        """, (datum,))
    row = cur.fetchone()
    return row[0] if row else None

def get_partei_id(cur, partei_kuerzel):
    if not partei_kuerzel:
        return None
    cur.execute("""
        SELECT id FROM partei
        WHERE kuerzel = %s
        """, (partei_kuerzel,))
    row = cur.fetchone()
    if row is None:
        print("Partei fehlt: ", partei_kuerzel)
    return row[0] if row else None

def get_parteilos_id(cur):
    cur.execute("SELECT id FROM partei WHERE name = %s", ("Parteilos",))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Partei 'Parteilos' nicht in Tabelle vorhanden!")
    return row[0]

def get_bundesland_id(cur, bundesland_name):
    if not bundesland_name:
        return None
    cur.execute("""
        SELECT id FROM bundesland
        WHERE name = %s
        """, (bundesland_name,))
    row = cur.fetchone()
    return row[0] if row else None

def get_kandidat_id(cur, vorname, nachname, geburtsjahr, partei_id):
    cur.execute("""
        SELECT id FROM kandidat
        WHERE vorname = %s AND nachname = %s AND geburtsjahr = %s AND partei_id = %s
        """, (vorname, nachname, geburtsjahr, partei_id))
    row = cur.fetchone()
    return row[0] if row else None

# --- Inserts ---

def insert_kandidat(cur, vorname, nachname, geburtsjahr, partei_id):
    cur.execute("""
        INSERT INTO kandidat (vorname, nachname, geburtsjahr, partei_id)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """, (vorname, nachname, geburtsjahr, partei_id))
    return cur.fetchone()[0]

def get_or_create_landesliste(cur, partei_id, wahl_nummer, bundesland_id):
    cur.execute("""
        SELECT id FROM landesliste
        WHERE partei_id = %s AND wahl_id = %s AND bundesland_id = %s
        """, (partei_id, wahl_nummer, bundesland_id))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        INSERT INTO landesliste (partei_id, wahl_id, bundesland_id)
        VALUES (%s, %s, %s)
        RETURNING id
        """, (partei_id, wahl_nummer, bundesland_id))
    return cur.fetchone()[0]

def insert_listenplatz(cur, kandidat_id, landesliste_id, position):
    cur.execute("""
        INSERT INTO listenplatz (kandidat_id, landesliste_id, position)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """, (kandidat_id, landesliste_id, position))

def insert_direktkandidatur(cur, kandidat_id, wahl_nummer, wahlkreis_id, partei_id):
    cur.execute("""
        INSERT INTO direktkandidatur (kandidat_id, wahl_id, wahlkreis_id, partei_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """, (kandidat_id, wahl_nummer, wahlkreis_id, partei_id))

def read_file(cur, file_path):
    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        parteilos_id = get_parteilos_id(cur)

        for row in reader:
            datum = row.get("Wahltag")
            nachname = row.get("Nachname")
            vornamen = row.get("Vornamen")
            geburtsjahr = int(row.get("Geburtsjahr"))
            kennzeichen = row.get("Kennzeichen")
            gebietsname = row.get("Gebietsname")  # long name of Bundesland
            listenplatz_str = row.get("Listenplatz")
            listenplatz = int(listenplatz_str) if listenplatz_str and listenplatz_str.strip() else None
            wahlkreis_id = int(row.get("Gebietsnummer"))
            gruppen_kurz = row.get("GruppennameKurz")

            if not (datum and nachname and vornamen and geburtsjahr):
                continue

            wahl_nummer = get_wahl_nummer(cur, datum)
            if not wahl_nummer:
                print(f"Wahl nicht gefunden: {datum}")
                continue

            partei_id = get_partei_id(cur, gruppen_kurz)
            if not partei_id:
                partei_id = parteilos_id

            kandidat_id = get_kandidat_id(cur, vornamen, nachname, geburtsjahr, partei_id)
            if not kandidat_id:
                kandidat_id = insert_kandidat(cur, vornamen, nachname, geburtsjahr, partei_id)

            if kennzeichen == "Landesliste":
                bundesland_id = get_bundesland_id(cur, gebietsname)
                if partei_id and bundesland_id:
                    landesliste_id = get_or_create_landesliste(cur, partei_id, wahl_nummer, bundesland_id)
                    if listenplatz is not None:
                        insert_listenplatz(cur, kandidat_id, landesliste_id, listenplatz)

            if kennzeichen in ("Kreiswahlvorschlag", "anderer Kreiswahlvorschlag"):
                if wahlkreis_id is not None:
                    insert_direktkandidatur(cur, kandidat_id, wahl_nummer, wahlkreis_id, partei_id)

# Fills kandidat, direktkandidatur, listenplatz, landesliste tables
def main():
    conn = db_connect()
    with conn.cursor() as cur:
        read_file(cur, CSV_PATH_2025)
        read_file(cur, CSV_PATH_2021)

    conn.commit()
    conn.close()
    print("Kandidaten-Import abgeschlossen.")

if __name__ == "__main__":
    main()
