import psycopg2
import csv
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

RESULTS_FILE = "python_scripts/daten/kerg2_00413.csv"
WAHLKREIS = 1
WAHL_NUMMER_2021 = 20
WAHL_NUMMER_2025 = 21

# id
current_id = 0

# Verbindung zur Datenbank herstellen
def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

# Get both wahlkreisergebnis_ids for a specific wahlkreisnummer
def get_wahlkreisergebnis_ids(cur, wahlkreis_nummer):
    cur.execute("""
        SELECT id
        FROM wahlkreisergebnis we 
        JOIN wahl w ON we.wahl_id = w.nummer
        WHERE wahlkreis_id = %s
        ORDER BY datum
        """, (wahlkreis_nummer,))
    rows = cur.fetchall()
    return rows if rows else None

# Get partei_id for Partei
def get_partei_id(cur, partei_kuerzel):
    cur.execute("""
        SELECT id FROM partei
        WHERE kuerzel = %s
        """, (partei_kuerzel,))
    row = cur.fetchone()
    return row[0] if row else None

# Get direktkandidatur_id for partei in wahlkreis and wahl
def get_direktkandidatur_for_partei_wahlkreis_wahl(cur, partei_id, wahlkreis_id, wahl_nummer):
    cur.execute("""
        SELECT dk.id
        FROM direktkandidatur dk
        JOIN kandidat k ON dk.kandidat_id = k.id
        WHERE k.partei_id = %s
          AND dk.wahl_id = %s
          AND dk.wahlkreis_id = %s
        """, (partei_id, wahl_nummer, wahlkreis_id))
    row = cur.fetchone()
    return row[0] if row else None


# CSV-Datei öffnen
def insert_stimmen(conn, cur, results_file, wahlkreis = -1):
    global current_id
    with open(results_file, encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            if (row["Gebietsart"] == "Wahlkreis") and ((wahlkreis == -1) or int(row["Gebietsnummer"]) == wahlkreis):
                wahlkreis_nummer = int(row["Gebietsnummer"])
                wahlkreisergebnis_ids = get_wahlkreisergebnis_ids(cur, wahlkreis_nummer)
                wahlkreisergebnis_id_2021 = wahlkreisergebnis_ids[0]
                wahlkreisergebnis_id_2025 = wahlkreisergebnis_ids[1]
                # Ungültige Stimmen
                if row["Gruppenart"] == "System-Gruppe" and row["Gruppenname"] == "Ungültige":
                    if row["Stimme"] == "1":
                        number_of_invalid_erststimmen_2021 = int(row["VorpAnzahl"])
                        number_of_invalid_erststimmen_2025 = int(row["Anzahl"])
                        for i in range(number_of_invalid_erststimmen_2021):
                            cur.execute("""INSERT INTO erststimme(id, wahlkreisergebnis_id, gueltig, direktkandidatur_id) 
                                        values(%s, %s, false, null)""", (current_id, wahlkreisergebnis_id_2021))
                            current_id += 1
                        for i in range(number_of_invalid_erststimmen_2025):
                            cur.execute("""INSERT INTO erststimme(id, wahlkreisergebnis_id, gueltig, direktkandidatur_id) 
                                        values(%s, %s, false, null)""", (current_id, wahlkreisergebnis_id_2025))
                            current_id += 1
                    elif row["Stimme"] == "2":
                        number_of_invalid_zweitstimmen_2021 = int(row["VorpAnzahl"])
                        number_of_invalid_zweitstimmen_2025 = int(row["Anzahl"])
                        for i in range(number_of_invalid_zweitstimmen_2021):
                            cur.execute("""INSERT INTO zweitstimme(id, wahlkreisergebnis_id, gueltig, partei_id) 
                                        values(%s, %s, false, null)""", (current_id, wahlkreisergebnis_id_2021))
                            current_id += 1
                        for i in range(number_of_invalid_zweitstimmen_2025):
                            cur.execute("""INSERT INTO zweitstimme(id, wahlkreisergebnis_id, gueltig, partei_id) 
                                        values(%s, %s, false, null)""", (current_id, wahlkreisergebnis_id_2025))
                            current_id += 1
                elif row["Gruppenart"] in ["Partei", "Einzelbewerber"]:
                    partei_kuerzel = row.get("GruppennameKurz") or row.get("Gruppenname")
                    partei_id = get_partei_id(cur, partei_kuerzel)
                    if not partei_id:
                        print(f"Partei nicht gefunden: {partei_kuerzel}")
                        continue

                    stimme = row["Stimme"]  # "1" = Erststimme, "2" = Zweitstimme

                    # Stimmenzahlen
                    anzahl_2025 = int(row["Anzahl"]) if row.get("Anzahl") else None
                    anzahl_2021 = int(row["VorpAnzahl"]) if row.get("VorpAnzahl") else None

                    if stimme == "1":
                        dk_id_2021 = get_direktkandidatur_for_partei_wahlkreis_wahl(cur, partei_id, WAHL_NUMMER_2021, wahlkreis_nummer)
                        dk_id_2025 = get_direktkandidatur_for_partei_wahlkreis_wahl(cur, partei_id, WAHL_NUMMER_2025, wahlkreis_nummer)
                        if anzahl_2021 is not None:
                            for i in range(anzahl_2021):
                                cur.execute("""
                                    INSERT INTO erststimme (id, wahlkreisergebnis_id, gueltig, direktkandidatur_id)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                """, (current_id, wahlkreisergebnis_id_2021, True, dk_id_2021))
                                current_id += 1
                        if anzahl_2025 is not None:
                            for i in range(anzahl_2025):
                                cur.execute("""
                                    INSERT INTO erststimme (id, wahlkreisergebnis_id, gueltig, direktkandidatur_id)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                """, (current_id, wahlkreisergebnis_id_2025, True, dk_id_2025))
                                current_id += 1
                    elif stimme == "2":
                        if anzahl_2021 is not None:
                            for i in range(anzahl_2021):
                                cur.execute("""
                                        INSERT INTO zweitstimme (id, wahlkreisergebnis_id, gueltig, partei_id)
                                        VALUES (%s, %s, %s, %s)
                                        ON CONFLICT DO NOTHING
                                    """, (current_id, wahlkreisergebnis_id_2021, True, partei_id))
                                current_id += 1
                        if anzahl_2025 is not None:
                            for i in range(anzahl_2025):
                                cur.execute("""
                                        INSERT INTO zweitstimme (id, wahlkreisergebnis_id, gueltig, partei_id)
                                        VALUES (%s, %s, %s, %s)
                                        ON CONFLICT DO NOTHING
                                    """, (current_id, wahlkreisergebnis_id_2025, True, partei_id))
                                current_id += 1
            conn.commit()

# Fills partei table
def main():
    conn = db_connect()
    with conn.cursor() as cur:
        insert_stimmen(conn, cur, RESULTS_FILE, WAHLKREIS)

    # Änderungen speichern
    conn.commit()

    # Verbindung schließen
    cur.close()
    conn.close()
    print("Stimmen-Import abgeschlossen.")

if __name__ == "__main__":
    main()
