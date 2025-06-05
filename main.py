# main.py

from xml_to_json import xml_to_json
from db_mysql import (
    create_database_if_not_exists,
    create_tables,
    import_json_to_mysql,
    export_mysql_to_json
)

# 1) Utworzenie bazy (jeśli jej nie ma)
create_database_if_not_exists()

# 2) Utworzenie tabel w bazie
create_tables()

# 3) Konwersja: XML → JSON
xml_to_json(
    "data/raw/covid_stats.xml",      # tu jest Twój plik XML
    "data/processed/covid_stats.json"  # tu zapisujemy wygenerowany JSON
)

# 4) Import JSON → MySQL (ORM)
import_json_to_mysql("data/processed/covid_stats.json")

# 5) Eksport z MySQL → JSON (ORM)
export_mysql_to_json("data/processed/covid_exported.json")
