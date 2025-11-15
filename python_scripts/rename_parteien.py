import os
import csv

# Define replacements
NEW_KUERZEL_LIST = [["GRÜNE/B 90", "GRÜNE"], ["DIE LINKE", "Die Linke"]]
NEW_GRUPPENNAME_LIST = []

# Directory containing CSV files
TARGET_DIR = "python_scripts/daten"

def replace_in_csv(filepath, rowname, replacements):
    """Replace values in GruppennameKurz column of a CSV file."""
    rows = []
    with open(filepath, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames
        for row in reader:
            old_value = row.get(rowname, "")
            for old, new in replacements:
                if old_value == old:
                    row[rowname] = new
            rows.append(row)

    # Write back the updated rows
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

def main():
    for root, dirs, files in os.walk(TARGET_DIR):
        for filename in files:
            if filename.lower().endswith(".csv"):
                file_path = os.path.join(root, filename)
                replace_in_csv(file_path, "GruppennameKurz", NEW_KUERZEL_LIST)
                replace_in_csv(file_path, "Gruppenname", NEW_GRUPPENNAME_LIST)
    print("Update abgeschlossen")

if __name__ == "__main__":
    main()
