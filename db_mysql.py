# db_mysql.py

import mysql.connector
import json
import os

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# --------------------------------------------------------------------------------
# 1. Funkcja tworząca bazę danych (jeśli nie istnieje) – korzystamy nadal z mysql.connector,
#    ponieważ SQLAlchemy nie zapewnia wprost CREATE DATABASE.
# --------------------------------------------------------------------------------

def create_database_if_not_exists():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234"
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS covid_stats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.close()
    conn.close()


# --------------------------------------------------------------------------------
# 2. Konfiguracja SQLAlchemy: silnik, sesja i klasa bazowa
# --------------------------------------------------------------------------------

# Uwaga: w łańcuchu połączenia używamy drivera mysqlconnector,
#       bo wcześniej instalowaliśmy `mysql-connector-python`.
engine = create_engine(
    "mysql+mysqlconnector://root:1234@localhost/covid_stats",
    echo=False,                           # można włączyć True dla logów SQL
    pool_pre_ping=True,
    future=True                           # tryb 2.0 API
)
Session = sessionmaker(bind=engine, future=True)
Base = declarative_base()


# --------------------------------------------------------------------------------
# 3. Modele ORM (klasy odpowiadające tabelom)
# --------------------------------------------------------------------------------

class Year(Base):
    __tablename__ = "years"
    id = Column(Integer, primary_key=True)
    value = Column(Integer, unique=True, nullable=False)

    # Relacja jeden-do-wielu do tabeli Unemployment
    unemployment = relationship(
        "Unemployment",
        back_populates="year",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Year(id={self.id}, value={self.value})>"


class Unemployment(Base):
    __tablename__ = "unemployment"
    id = Column(Integer, primary_key=True)
    year_id = Column(Integer, ForeignKey("years.id"), nullable=False)
    name = Column(String(50), nullable=False)
    national_unemployment = Column(Float, nullable=True)
    male_unemployment = Column(Float, nullable=True)
    female_unemployment = Column(Float, nullable=True)

    # Relacja do tabeli Year
    year = relationship("Year", back_populates="unemployment")
    # Relacja jeden-do-wielu do tabeli Death
    deaths = relationship(
        "Death",
        back_populates="month",
        cascade="all, delete-orphan"
    )

    # Chcemy, żeby w ramach jednego roku nazwa miesiąca była unikalna:
    __table_args__ = (
        UniqueConstraint("year_id", "name", name="uix_year_month_name"),
    )

    def __repr__(self):
        return (f"<Unemployment(id={self.id}, year_id={self.year_id}, name={self.name}, "
                f"nat={self.national_unemployment}, male={self.male_unemployment}, "
                f"female={self.female_unemployment})>")


class Death(Base):
    __tablename__ = "deaths"
    id = Column(Integer, primary_key=True)
    month_id = Column(Integer, ForeignKey("unemployment.id"), nullable=False)
    total_deaths = Column(Integer, nullable=True)
    covid_deaths = Column(Integer, nullable=True)
    other_deaths = Column(Integer, nullable=True)

    # Relacja do tabeli Unemployment (jeden miesiąc może mieć wiele rekordów, ale w praktyce
    # trzymamy tylko jeden rekord deaths na miesiąc, więc lista ma pojedynczy element)
    month = relationship("Unemployment", back_populates="deaths")

    def __repr__(self):
        return (f"<Death(id={self.id}, month_id={self.month_id}, total={self.total_deaths}, "
                f"covid={self.covid_deaths}, other={self.other_deaths})>")


# --------------------------------------------------------------------------------
# 4. Funkcja tworząca tabele zgodnie z modelami ORM
# --------------------------------------------------------------------------------

def create_tables():
    """
    Tworzy wszystkie tabele w bazie (jeśli ich jeszcze nie ma),
    używając deklaracji w klasach ORM.
    """
    Base.metadata.create_all(engine)
    print("✅ Tabele utworzone (lub już istniały).")


# --------------------------------------------------------------------------------
# 5. Import JSON → MySQL przez SQLAlchemy ORM
# --------------------------------------------------------------------------------

def import_json_to_mysql(json_path='data/processed/covid_stats.json'):
    if not os.path.exists(json_path):
        print(f"⚠️ Plik {json_path} nie istnieje. Import przerwany.")
        return

    # 1) Wczytanie surowego JSON-a
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 2) Jeśli mamy starszy format (klucz "data" zamiast "years"), przekształcamy:
    if "data" in data and "years" not in data:
        data["years"] = []
        for entry in data["data"]:
            # Każdy element ma postać: { "year": 2020, "months": [ ... ] }
            year_val = entry.get("year")
            months = entry.get("months", [])
            data["years"].append({
                "value": year_val,
                "months": months
            })

    # Jeśli wciąż nie ma "years", to nic nie robimy
    if "years" not in data:
        print("⚠️ Nie znaleziono klucza 'years' w JSON. Import przerwany.")
        return

    session = Session()

    try:
        for year_entry in data["years"]:
            year_val = year_entry.get("value")
            if year_val is None:
                continue

            # Sprawdzamy, czy taki rok już jest w bazie
            existing_year = session.query(Year).filter_by(value=year_val).one_or_none()
            if existing_year:
                year_obj = existing_year
            else:
                year_obj = Year(value=year_val)
                session.add(year_obj)
                session.flush()  # żeby otrzymać ID przed tworzeniem months

            for month in year_entry.get("months", []):
                month_name = month.get("name")
                unemp_data = month.get("unemployment", {})
                deaths_data = month.get("deaths", {})

                # Sprawdzamy, czy dla tego year_obj i nazwy miesiąca już mamy rekord
                existing_month = (
                    session.query(Unemployment)
                           .filter_by(year_id=year_obj.id, name=month_name)
                           .one_or_none()
                )
                if existing_month:
                    unemp_obj = existing_month
                    # Można tu zaktualizować wartości, jeśli chcemy nadpisać stare
                    unemp_obj.national_unemployment = unemp_data.get("national")
                    unemp_obj.male_unemployment = unemp_data.get("male")
                    unemp_obj.female_unemployment = unemp_data.get("female")
                else:
                    unemp_obj = Unemployment(
                        year=year_obj,
                        name=month_name,
                        national_unemployment=unemp_data.get("national"),
                        male_unemployment=unemp_data.get("male"),
                        female_unemployment=unemp_data.get("female")
                    )
                    session.add(unemp_obj)
                    session.flush()

                # Deaths: zakładamy dokładnie jeden wpis na miesiąc
                # Sprawdzamy, czy już istnieje Death dla tego month_id
                existing_death = (
                    session.query(Death)
                           .filter_by(month_id=unemp_obj.id)
                           .one_or_none()
                )
                if existing_death:
                    # Nadpisujemy wartości
                    existing_death.total_deaths = deaths_data.get("total")
                    existing_death.covid_deaths = deaths_data.get("COVID-19") or deaths_data.get("covid_deaths")
                    existing_death.other_deaths = deaths_data.get("other")
                else:
                    death_obj = Death(
                        month=unemp_obj,
                        total_deaths=deaths_data.get("total"),
                        covid_deaths=deaths_data.get("COVID-19") or deaths_data.get("covid_deaths"),
                        other_deaths=deaths_data.get("other")
                    )
                    session.add(death_obj)

        # Na koniec wykonujemy commit
        session.commit()
        print("✅ Dane zaimportowane (ORM).")

    except Exception as e:
        session.rollback()
        print("❌ Wystąpił błąd podczas importu ORM:", e)
    finally:
        session.close()


# --------------------------------------------------------------------------------
# 6. Eksport MySQL → JSON przez SQLAlchemy ORM
# --------------------------------------------------------------------------------

def export_mysql_to_json(json_path='data/processed/covid_exported.json'):
    """
    Eksportuje wszystkie dane z tabel:
      years, unemployment, deaths
    do struktury:
      {
        "years": [
          {
            "value": 2020,
            "months": [
              {
                "name": "January",
                "unemployment": {
                  "national": 5.2,
                  "male": 5.0,
                  "female": 5.4
                },
                "deaths": {
                  "total": 30000,
                  "COVID-19": 2000,
                  "other": 28000
                }
              },
              ...
            ]
          },
          ...
        ]
      }
    i zapisuje do pliku JSON pod podaną ścieżką.
    """

    session = Session()
    output = {"years": []}

    try:
        # Pobieramy wszystkie lata posortowane po wartości rocznej
        years = session.query(Year).order_by(Year.value).all()

        for year_obj in years:
            year_dict = {
                "value": year_obj.value,
                "months": []
            }
            # Iterujemy po wszystkich powiązanych rekordach Unemployment
            # Zamieniamy na listę dictów
            for unemp in (
                session.query(Unemployment)
                       .filter_by(year_id=year_obj.id)
                       .order_by(Unemployment.id)
                       .all()
            ):
                # Każdy unemp może mieć listę deaths, ale powinniśmy brać pierwszy element
                death_record = unemp.deaths[0] if unemp.deaths else None

                month_dict = {
                    "name": unemp.name,
                    "unemployment": {
                        "national": unemp.national_unemployment,
                        "male": unemp.male_unemployment,
                        "female": unemp.female_unemployment
                    },
                    "deaths": {
                        "total": death_record.total_deaths if death_record else None,
                        "COVID-19": death_record.covid_deaths if death_record else None,
                        "other": death_record.other_deaths if death_record else None
                    }
                }
                year_dict["months"].append(month_dict)

            output["years"].append(year_dict)

        # Zapis do pliku JSON
        # Upewniamy się, że katalog istnieje
        folder = os.path.dirname(json_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"📤 Dane wyeksportowane (ORM) do: {json_path}")

    except Exception as e:
        print("❌ Wystąpił błąd podczas eksportu ORM:", e)

    finally:
        session.close()
