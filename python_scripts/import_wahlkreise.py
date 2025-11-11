import psycopg2
import csv
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 


# Fills wahlkreis table

wahlkreis_file = "python_scripts/daten/btw25_wahlkreisnamen_utf8.csv"

# Verbindung zur Datenbank herstellen
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()

with open(wahlkreis_file, encoding="utf-8-sig") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=";")
    for row in reader:
        wkr_nr = int(row["WKR_NR"])
        name = row["WKR_NAME"].strip()
        land_nr = int(row["LAND_NR"])  # als Integer casten

        cur.execute(
            """
            INSERT INTO wahlkreis (nummer, name, bundesland_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (wkr_nr, name, land_nr)
        )

conn.commit()
cur.close()
conn.close()

print("Wahlkreis-Import abgeschlossen.")
