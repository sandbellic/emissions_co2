   #pour récupérer la listes des aéroports français les plus grands 
def webscraping():    
    url = "https://www.aeroports.org/france/#grands-aeroports"
    liste_df = {}
    #données 2023
    liste_villes = []
    liste_codes=[]
    response = requests.get(url)
    if response.status_code != 200:     #pb pour avoir les données de la page
        print("pb d'accès au site")
    else:
        soup = BeautifulSoup(response.text, 'html.parser')    #response.text => response.content.decode('utf-8') pour éviter d'avoir des caractères spéciaux
        villes = soup.find_all("a", class_="az-citylink")
        codes = soup.find_all("span", class_ = "az-code")
        for idx, (ville, code) in enumerate(zip(villes,codes)):
            liste_villes.append(ville.text)
            liste_codes.append(code.text)

    data={'code':liste_codes, 'ville':liste_villes}

    liste_df['aeroports'] = pd.DataFrame(data)

    return liste_df