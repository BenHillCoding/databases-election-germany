import psycopg2
import csv
import os
from dotenv import load_dotenv

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

def get_wahlkreisergebnis_id(cur, wahl_id, wahlkreis_id):
    cur.execute(
        "SELECT id FROM wahlkreisergebnis WHERE wahl_id = %s AND wahlkreis_id = %s",
        (wahl_id, wahlkreis_id)
    )
    row = cur.fetchone()
    return row[0] if row else None

def get_partei_id(cur, kuerzel):
    cur.execute("SELECT id FROM partei WHERE kuerzel = %s", (kuerzel,))
    row = cur.fetchone()
    return row[0] if row else None

def get_or_create_direktkandidatur_id(cur, wahl_id, wahlkreis_id, partei_id):
    cur.execute(
        """SELECT id FROM direktkandidatur
           WHERE wahl_id = %s AND wahlkreis_id = %s AND partei_id = %s""",
        (wahl_id, wahlkreis_id, partei_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        name = "w" + str(wahl_id) + "-wk" + str(wahlkreis_id) + "-p" + str(partei_id)
        cur.execute("""INSERT INTO kandidat(vorname, nachname, geburtsjahr, partei_id) VALUES(%s,%s,%s,%s) RETURNING id"""
                    , (name, name, 1900, partei_id))
        new_id = cur.fetchone()[0]
        cur.execute("""INSERT INTO direktkandidatur(kandidat_id, wahl_id, wahlkreis_id, partei_id) VALUES(%s,%s,%s,%s) RETURNING id"""
                    , (new_id, wahl_id, wahlkreis_id, partei_id))
        new_id = cur.fetchone()[0]
        return new_id

def insert_zweitstimme(cur, wahlkreisergebnis_id, partei_id, anzahl):
    cur.execute(
        """INSERT INTO zweitstimme (wahlkreisergebnis_id, gueltig, partei_id, anzahl)
           VALUES (%s, %s, %s, %s);""",
        (wahlkreisergebnis_id, True, partei_id, anzahl)
    )

def insert_erststimme(cur, wahlkreisergebnis_id, direktkandidatur_id, anzahl):
    cur.execute(
        """INSERT INTO erststimme (wahlkreisergebnis_id, gueltig, direktkandidatur_id, anzahl)
           VALUES (%s, %s, %s, %s);""",
        (wahlkreisergebnis_id, True, direktkandidatur_id, anzahl)
    )

def main():
    conn = db_connect()
    with conn.cursor() as cur, open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        # Wahl-ID für 2021 holen
        wahl_id_2021 = get_wahl_id(cur, "26.09.2021")
        if not wahl_id_2021:
            print("Wahl 2021 nicht gefunden.")
            return

        for row in reader:
            if row.get("Gebietsart") != "Wahlkreis":
                continue

            wahlkreis_id = int(row.get("Gebietsnummer"))
            stimme = row.get("Stimme")
            vorp_anzahl = row.get("VorpAnzahl")

            try:
                anzahl = int(vorp_anzahl)
            except (TypeError, ValueError):
                continue

            if anzahl <= 0:
                continue

            wahlkreisergebnis_id = get_wahlkreisergebnis_id(cur, wahl_id_2021, wahlkreis_id)
            if not wahlkreisergebnis_id:
                print(f"Wahlkreisergebnis nicht gefunden für Wahlkreis {wahlkreis_id}, Wahl 2021")
                continue

            gruppenart = row.get("Gruppenart")
            kuerzel = row.get("Gruppenname")

            # Ungültige Stimmen
            if kuerzel == "Ungültige":
                if stimme == "1":
                    insert_erststimme(cur, wahlkreisergebnis_id, None, anzahl)
                elif stimme == "2":
                    insert_zweitstimme(cur, wahlkreisergebnis_id, None, anzahl)
                continue

            # Partei oder Einzelbewerber/Wählergruppe
            if gruppenart in ["Partei", "Einzelbewerber/Wählergruppe"]:
                partei_id = get_partei_id(cur, kuerzel)
                if not partei_id:
                    print(f"Partei mit Kürzel {kuerzel} nicht gefunden, überspringe.")
                    print(row)
                    continue

                if stimme == "1":  # Erststimme
                    direktkandidatur_id = get_or_create_direktkandidatur_id(cur, wahl_id_2021, wahlkreis_id, partei_id)
                    if direktkandidatur_id:
                        insert_erststimme(cur, wahlkreisergebnis_id, direktkandidatur_id, anzahl)
                    else:
                        print(f"Keine Direktkandidatur gefunden für {kuerzel} in Wahlkreis {wahlkreis_id}")
                elif stimme == "2":  # Zweitstimme
                    insert_zweitstimme(cur, wahlkreisergebnis_id, partei_id, anzahl)

    conn.commit()
    conn.close()
    print("2021 Stimmen-Import abgeschlossen.")

if __name__ == "__main__":
    main()
