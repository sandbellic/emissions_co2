import pandas as pd
import requests
from io import StringIO, BytesIO
import zipfile
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database


#------------------------------
# initialisation base de données
#------------------------------
# Paramètres de connexion à base de données PostgreSQL
database = "emissions_co2"
username = "postgres"
password = "SandPOST6642"
host = "localhost"
port = 5432

# On créée la connexion vers la base de données.  
# postgresql s'utilise avec le driver psycopg2 (et mysql avec le driver pymyslq)
DATABASE_URI = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(DATABASE_URI)

# On créée la base de données si elle n'existe pas.
if not database_exists(engine.url):
    create_database(engine.url)
#on crée le schéma associé dans lequel on va enregistrer nos tables et vues    
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS emissions_co2"))


#-------------
#fin initialisation base
#---------------

def boucle_API(url):
    # API utilisée pour le site open data de la SNCF, pas plus de 100 items récupérés à la fois
    # nécessité de boucler
    all_data = []
    offset = 0
    limit = 100   #on met 'limit' à la plus grande valeur autorisée par SNCF, soit 100
    while True:
        params = {"limit": limit,"offset": offset}
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Erreur API  {response.status_code}")
            break
        data = response.json()
        valeurs = data.get("results", [])
        all_data.extend(valeurs)
        offset += limit

    return all_data


url = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/records?select=code_uic%2C%20libelle%2Ccommune%2C%20c_geo&where=voyageurs%3D'O'"

df = pd.json_normalize(boucle_API(url))



print(df.head())