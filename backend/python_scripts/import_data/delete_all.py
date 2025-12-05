import psycopg2
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

# Verbindung zur Datenbank herstellen
def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port="5432"
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
