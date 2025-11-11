import psycopg2
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

# Deletes content of all tables
def main():
    conn = db_connect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE erststimme CASCADE")
        cur.execute("TRUNCATE TABLE zweitstimme CASCADE")
        cur.execute("DELETE FROM listenplatz")
        cur.execute("DELETE FROM landesliste")
        cur.execute("DELETE FROM direktkandidatur")
        cur.execute("DELETE FROM wahlkreisergebnis")
        cur.execute("DELETE FROM wahlkreis")
        cur.execute("DELETE FROM bundesland")
        cur.execute("DELETE FROM kandidat")
        cur.execute("DELETE FROM partei")
        cur.execute("DELETE FROM wahl")


    # Änderungen speichern
    conn.commit()

    # Verbindung schließen
    conn.close()
    print("Delete-All abgeschlossen.")

if __name__ == "__main__":
    main()
