import psycopg2
import subprocess
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

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
        cur.execute("DELETE FROM erststimme")
        cur.execute("DELETE FROM zweitstimme")

    # Änderungen speichern
    conn.commit()

    subprocess.run(["python", "python_scripts/import_stimmen.py"])

    # Verbindung schließen
    cur.close()
    conn.close()
    print("Create-Delete-Stimmen abgeschlossen.")

if __name__ == "__main__":
    main()
