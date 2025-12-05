import psycopg2
import os
from dotenv import load_dotenv, dotenv_values 

load_dotenv() 


BUNDESLAND_LIST = [[1, "Schleswig-Holstein"], [2, "Hamburg"], [3, "Niedersachsen"], [4, "Bremen"], [5, "Nordrhein-Westfalen"], [6, "Hessen"], [7, "Rheinland-Pfalz"], [8, "Baden-Württemberg"], [9, "Bayern"], [10, "Saarland"], [11, "Berlin"], [12, "Brandenburg"], [13, "Mecklenburg-Vorpommern"], [14, "Sachsen"], [15, "Sachsen-Anhalt"], [16, "Thüringen"]]

def db_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )



# Fills bundesland and wahl table
def main():
    conn = db_connect()
    with conn.cursor() as cur:
        for bundesland in BUNDESLAND_LIST:
            cur.execute("""
                INSERT INTO bundesland(id, name) VALUES(%s, %s) ON CONFLICT DO NOTHING
                """, (bundesland[0], bundesland[1]))
        
        cur.execute("""INSERT INTO wahl(nummer, datum) VALUES(20, date '2021-09-26')""")
        cur.execute("""INSERT INTO wahl(nummer, datum) VALUES(21, date '2025-02-23')""")

    conn.commit()
    conn.close()
    print("Bundesländer-Import und Wahl-Import abgeschlossen.")

if __name__ == "__main__":
    main()
