import subprocess

subprocess.run(["python", "python_scripts/rename_parteien.py"])
subprocess.run(["python", "python_scripts/delete_all.py"])
subprocess.run(["python", "python_scripts/import_schema.py"])
subprocess.run(["python", "python_scripts/import_bundeslaender_wahl.py"])
subprocess.run(["python", "python_scripts/import_wahlkreise.py"])
subprocess.run(["python", "python_scripts/import_parteien.py"])
subprocess.run(["python", "python_scripts/import_kandidaten.py"])
subprocess.run(["python", "python_scripts/import_wahlkreisergebnisse.py"])
# subprocess.run(["python", "python_scripts/import_stimmen.py"])

