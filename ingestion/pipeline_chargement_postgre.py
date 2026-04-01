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



#----------------
#liste des url des données à charger
#-----------------
url_communes = "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"

url_cars = "https://www.data.gouv.fr/api/1/datasets/r/bc42c2e3-d24c-4499-a966-d35656c6cfc1"

url_trains = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/emission-co2-perimetre-complet/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
url_routes_train = " https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"

url_airports_fr = "https://ourairports.com/countries/FR/airports.csv"
url_routes_air = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

#----------------------


def load_url():
    #a partir des url de chaque éléments on va récupérer les données (format csv) sous-jacentes
    #on va stocker les résultats dans un dictionnaire liste_df:
    #  la clé va représenter le nom de la donnée qu'on va stocker 'cars', 'airports'
    #  et la valeur associée sera le dataframe équivalent du csv
    #Attention: cas particulier des voitures et des routes_trains: on récupère un zip
    liste_df={}

    #load de la liste des aéroports
    response = requests.get(url_airports_fr)
    if response.status_code == 200:
        liste_df['airports'] = pd.read_csv(StringIO(response.text))
    else:
        print("Erreur téléchargement liste aéroport:", response.status_code)

    #load de la liste des communes
    response = requests.get(url_communes)
    if response.status_code == 200:
        liste_df['communes'] = pd.read_csv(BytesIO(response.content),encoding='utf-8', low_memory=False)
    else:
        print("Erreur téléchargement liste communes:", response.status_code)

    #load trains source SNCF pour une liste de trajets courants
    response = requests.get(url_trains)
    if response.status_code == 200:
        liste_df['trains'] = pd.read_csv(StringIO(response.text), sep=";")
    else:
        print("Erreur téléchargement liste trains:", response.status_code)


    #load routes.txt : liste de toutes les routes SNCF (selon les différents moyen TGV, TER, intercités, ...)
    #dans un fichiers ZIP : on récupère uniquement routes.txt
    response = requests.get(url_routes_train)
    if response.status_code == 200:
         #ouvrir le zip, le fichier qui nous intéresse est dans le fichier de nom routes.txt
        z = zipfile.ZipFile(BytesIO(response.content))   #z représente l'archive zip
        # ouvrir le bon fichier
        with z.open("routes.txt") as f:
            liste_df['routes_train'] = pd.read_csv(f, sep=",")
    else:
        print("erreur téléchargement routes SNCF txt")

    #load cars
    response = requests.get(url_cars)
    if response.status_code == 200:
        #ouvrir le zip, le fichier qui nous intéresse est dans le 1er fichier
        z = zipfile.ZipFile(BytesIO(response.content))   #z représente l'archive zip
        with z.open(z.namelist()[0]) as f:
            liste_df['cars'] = pd.read_csv(f, sep=';', encoding='latin-1')
        #z.namelist()[0] => retourne dans la liste des fichiers dans le ZIP le 1er fichier
        #z.open() => ouvre ce fichier (sans l’extraire sur disque) et retourne un flux de données
    else:
        print("Erreur téléchargement cars:", response.status_code)

    #routes aériennes
    response = requests.get(url_routes_air)
    if response.status_code == 200:
        liste_df['routes_air'] = pd.read_csv(StringIO(response.text), header=None)
        liste_df["routes_air"].columns = ['Airline', 'Airline_ID', 'Source_airport', 'Source_airport_ID',
                'Destination_airport', 'Destination_airport_ID','Codeshare', 'Stops', 'Equipment']
    else:
        print("Erreur téléchargement routes aériennes:", response.status_code)

    return liste_df


# la source liste aéroports a été jugée non assez fiable par Karima => pas de webscraping 
def webscraping():
    liste_df = {}
    return liste_df


def boucle_API(url):
    # API utilisée pour le site open data de la SNCF, pas plus de 100 items récupérés à la fois
    # nécessité de boucler
    all_data = []
    offset = 0
    limit = 100   #on met 'limit' à la plus grande valeur autorisée par SNCF, soit 100
    while True:
        params = {
            "limit": limit,
            "offset": offset
        }
        response = requests.get(url, params=params)
        data = response.json()["results"]
        if not data:
            break
        all_data.extend(data)
        offset += limit

    return all_data


def API():

    liste_df = {}
    data_API =[]

    #API impact CO2 de Ademe : permettant de charger les émissions co2 selon différents moyens de transport
    # estimation faite pour 1000 km, avec intégration des émissions co2 liées à la construction
    url = "https://impactco2.fr/api/v1/transport?km=1000&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction=1&language=fr"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (avec), source ImpactCO2:", response.status_code)

    # même estimation faite pour 1000 km,mais ici SANS intégration des émissions co2 liées à la construction
    url = "https://impactco2.fr/api/v1/transport?km=1000&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction=0&language=fr"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2_sans'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (sans), source ImpactCO2:", response.status_code)

    #APIs liées à Open Data de la SNCF - URL de base identique
    url_base = "https://ressources.data.sncf.com"
    #open data SNCF donne des liaisons TGV (régularité mensuelle), ça permet d'obtenir des routes TGV / intercités
    #on a une limit=100 et offset comme autre paramètre

    #cette API porte sur des trajets TGV avec retards sur l'année N-1, elle comporte des durées de trajets
    # on a choisi de se limiter aux routes TGV nationales, sans "0" dans gare depart/arrivee et calculer la médiane des durées
    url_routes_tgv= "/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/records?select=median(duree_moyenne)&where=service%20%3D%20%22National%22%20and%20gare_depart%20!%3D%20%220%22%20and%20gare_arrivee%20!%3D%20%220%22&group_by=gare_depart%2C%20gare_arrivee"
    url = url_base + url_routes_tgv
    data_API = boucle_API(url)
    df = pd.DataFrame(data_API)
    df['transporteur']= 'TGV'
    liste_df['routes_tgv'] = df

    #pour les intercités, on va récupérer les gares départ et arrivée (pas de temps / km ...)
    url_routes_intercites = "/api/explore/v2.1/catalog/datasets/regularite-mensuelle-intercites/records?select=depart%2C%20arrivee&group_by=depart%2C%20arrivee"
    url = url_base + url_routes_intercites
    data_API = boucle_API(url)
    df = pd.DataFrame(data_API)
    df['transporteur'] = 'Intercités'
    liste_df['routes_intercites'] = df

    #liste de toutes les gares SNCF avec leur identifiant code_uic
    url_gares = "/api/explore/v2.1/catalog/datasets/liste-des-gares/records?select=code_uic%2Clibelle%2Ccommune%2Cc_geo&where=voyageurs%3D'O'"
    url = url_base + url_gares 
    data_API = boucle_API(url)
    df = pd.json_normalize(data_API)
    liste_df['gares'] = df
    
    return liste_df


def load_to_postgre(dico):
    #on part d'un dictionnaire contenant pour la clé un nom qui deviendra le nom de table et en valeur un dataframe
    for key, value in dico.items():
        # on récupère de df du dico et on crée une table de nom key dans PostgreSQL avec pour contenu df
        df = value
        df.to_sql(f"raw_{key}", engine, if_exists="replace", index=False, schema="emissions_co2")


def run_pipeline_chargement_postgre():
    #le pipeline va charger les fichiers .csv présents aux url définies dans des dataframes,  les API
    #puis création des tables correspondantes sous PostgreSQL
    dico_table = {}
    dico_table = load_url()
    #dico_table.update(webscraping())
    dico_table.update(API())
    load_to_postgre(dico_table)



#-----------------
#prog general
#-----------------

if __name__ == "__main__":
    run_pipeline_chargement_postgre()



