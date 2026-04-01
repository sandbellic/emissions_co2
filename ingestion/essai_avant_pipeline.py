import pandas as pd
import requests
from io import StringIO, BytesIO
import csv



url = "https://impactco2.fr/api/v1/transport?km=1000&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction=1&language=fr"
response = requests.get(url)
data = response.json()["data"]
print(pd.DataFrame(data))



def api():

    liste=[]

        #open data SNCF donne des liaisons TGV (régularité mensuelle), ça permet d'obtenir des routes TGV
        #on a une limit=100 et offset comme autre paramètre
    url_base = "https://ressources.data.sncf.com"
    url_routes_tgv= "/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/records?select=median(duree_moyenne)&where=service%20%3D%20%22National%22%20and%20gare_depart%20!%3D%20%220%22%20and%20gare_arrivee%20!%3D%20%220%22&group_by=gare_depart%2C%20gare_arrivee"
    url = url_base + url_routes_tgv

    offset = 0
    limit = 100
    while True:
        params = {
            "limit": limit,
            "offset": offset
            }
        response = requests.get(url, params=params)
        data = response.json()["results"]

        if not data:
            break
        liste.extend(data)
        offset += limit

    df = pd.DataFrame(liste)
    print(df.head())