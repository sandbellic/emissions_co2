import pandas as pd
import requests
from io import StringIO, BytesIO
import csv

url_base = "https://impactco2.fr/api/v1"
km = 1000
include_construction = 1
# estimation faite pour 1000 km, avec intégration des émissions co2 liées à la construction
url_personnalise = f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction={include_construction}&language=fr"
response = requests.get(url_base + url_personnalise)
if response.status_code == 200:
        data = response.json()["data"]
        #liste_df['valeur_emissions_co2'] = pd.DataFrame(data)
        df= pd.DataFrame(data)
        print(df.head())
else:
    print("error")







def api():

    liste=[]

        #open data SNCF donne des liaisons TGV (régularité mensuelle), ça permet d'obtenir des routes TGV
        #on a une limit=100 et offset comme autre paramètre
    url_base = "https://data.ademe.fr/data-fair/api/v1/datasets/ademe-car-labelling/lines?" 
    url_criteres = "&select=Marque,Mod%C3%A8le,Energie,Poids_%C3%A0_vide,CO2_basse_vitesse_Min,CO2_basse_vitesse_Max,CO2_moyenne_vitesse_Min,CO2_moyenne_vitesse_Max,CO2_haute_vitesse_Min,CO2_haute_vitesse_Max,CO2_T-haute_vitesse_Min,CO2_T-haute_vitesse_Max,CO2_vitesse_mixte_Min,CO2_vitesse_mixte_Max&format=json&q_mode=simple"
    page = 1
    size = 10000
    url = url_base + url_criteres

    while True:
        url_variable = "page=" +str(page) + "&size=" +str(size)
        url = url_base + url_variable + url_criteres
        print (url)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()["results"]

            if not data:
             break
            liste.extend(data)
            page += size
        else:
           print("erreur lecture" + response.status_code)
        
        return liste
        exit

    #return liste






