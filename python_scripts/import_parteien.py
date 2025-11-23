import psycopg2
import csv
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 


FILE_2025 = "python_scripts/daten/btw25_parteien.csv"
FILE_2021 = "python_scripts/daten/btw21_parteien.csv"

# Verbindung zur Datenbank herstellen
def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


# CSV-Datei öffnen
def insert_parteien(cur, partei_file):
    with open(partei_file, encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            if row["Gruppenart_CSV"] in ["Partei", "Einzelbewerber/Wählergruppe"]:
                kuerzel = row["GruppennameKurz"].strip()
                name = row["Gruppenname"].strip()

                # Prüfen, ob Partei bereits existiert (per Kürzel oder Name)
                cur.execute(
                    "SELECT id FROM partei WHERE kuerzel = %s",
                    (kuerzel,)
                )
                exists = cur.fetchone()

                if not exists:
                    cur.execute(
                        "INSERT INTO partei (kuerzel, name) VALUES (%s, %s)",
                        (kuerzel, name)
                    )

# Fills partei table
def main():
    conn = db_connect()
    with conn.cursor() as cur:
        insert_parteien(cur, FILE_2025)
        insert_parteien(cur, FILE_2021)

    # SSW nationale Minderheit
    cur.execute("UPDATE partei SET nationale_minderheit = true WHERE kuerzel = 'SSW'")

    # Änderungen speichern
    conn.commit()

    # Verbindung schließen
    cur.close()
    conn.close()
    print("Parteien-Import abgeschlossen.")

if __name__ == "__main__":
    main()
