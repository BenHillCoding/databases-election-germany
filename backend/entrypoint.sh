#!/bin/sh
set -e

# Wait for Postgres to accept connections
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  echo "Waiting for Postgres..."
  sleep 2
done

echo "Postgres is ready!"

# Run your schema/data scripts
python python_scripts/import_data/rename_parteien.py
python python_scripts/import_data/import_schema.py
python python_scripts/import_data/import_bundeslaender_wahl.py
python python_scripts/import_data/import_wahlkreise.py
python python_scripts/import_data/import_parteien.py
python python_scripts/import_data/import_kandidaten.py
python python_scripts/import_data/import_wahlkreisergebnisse.py
python python_scripts/import_data/import_stimmen_2021.py
python python_scripts/import_data/import_stimmen_2025.py

# Finally start FastAPI
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
