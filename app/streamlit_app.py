import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="Calculateur CO2", layout="centered")

API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6ImI2MzVhNGU0NzAxOTRlNWFhNmJmYzAzODIwYTU0ODMwIiwiaCI6Im11cm11cjY0In0="

# -------------------------
# STYLE
# -------------------------
st.markdown("""
<style>
.block {
    background-color: white;
    padding: 20px;
    border-radius: 15px;
    margin-bottom: 15px;
    box-shadow: 0px 4px 10px rgba(0,0,0,0.05);
}
.bar-bg {
    background: #eee;
    border-radius: 10px;
}
.bar-fill {
    background: #4CAF50;
    padding: 5px;
    border-radius: 10px;
    color: white;
    text-align: right;
}
</style>
""", unsafe_allow_html=True)

# -------------------------
# SESSION
# -------------------------
if "calcul" not in st.session_state:
    st.session_state.calcul = False

# -------------------------
# VILLES
# -------------------------
villes = {
    "Paris": (2.3522, 48.8566),
    "Marseille": (5.3698, 43.2965),
    "Lyon": (4.8357, 45.7640),
    "Toulouse": (1.4442, 43.6047),
    "Nice": (7.2620, 43.7102),
    "Nantes": (-1.5536, 47.2184)
}

# -------------------------
# API
# -------------------------
def get_route(coord1, coord2):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"

    params = {
        "api_key": API_KEY,
        "start": f"{coord1[0]},{coord1[1]}",
        "end": f"{coord2[0]},{coord2[1]}"
    }

    headers = {"Accept": "application/geo+json"}

    r = requests.get(url, params=params, headers=headers)

    if r.status_code == 200:
        data = r.json()
        dist = data["features"][0]["properties"]["segments"][0]["distance"] / 1000
        coords = data["features"][0]["geometry"]["coordinates"]
        return dist, coords
    else:
        st.error("Erreur API")
        st.write(r.text)
        return None, None

# -------------------------
# FACTEURS
# -------------------------
FACTEURS = {
    "Voiture 🚗": 0.2,
    "Avion ✈️": 0.25,
    "Train 🚆": 0.01
}

# -------------------------
# UI
# -------------------------
st.title("🌍 Calculateur CO2 intelligent")
st.markdown("Comparez vos trajets avec des données réelles 🚀")

depart = st.selectbox("Ville de départ", list(villes.keys()))
arrivee = st.selectbox("Ville d’arrivée", list(villes.keys()))

col1, col2 = st.columns(2)

with col1:
    if st.button("Calculer le trajet"):
        st.session_state.calcul = True

with col2:
    if st.button("Reset"):
        st.session_state.calcul = False

# -------------------------
# LOGIQUE
# -------------------------
if depart != arrivee and st.session_state.calcul:

    with st.spinner("Calcul en cours..."):
        distance, coords = get_route(villes[depart], villes[arrivee])

    if distance:

        st.success(f"📏 Distance réelle : {distance:.1f} km")

        resultats = {
            transport: distance * facteur
            for transport, facteur in FACTEURS.items()
        }

        df = pd.DataFrame.from_dict(resultats, orient="index", columns=["CO2"])

        # -------------------------
        # CARTES
        # -------------------------
        st.subheader("📊 Comparaison")

        cols = st.columns(3)

        for i, t in enumerate(df.index):
            with cols[i]:
                st.markdown(f"""
                <div class="block">
                    <strong>{t}</strong><br><br>
                    <h2>{df.loc[t][0]:.1f} kg CO₂</h2>
                </div>
                """, unsafe_allow_html=True)

        # -------------------------
        # BARRES
        # -------------------------
        st.subheader("📊 Impact visuel")

        maxv = df["CO2"].max()

        for t in df.index:
            val = df.loc[t][0]
            width = (val / maxv) * 100

            st.markdown(f"""
            <div class="block">
                {t}<br>
                <div class="bar-bg">
                    <div class="bar-fill" style="width:{width}%;">
                        {val:.1f} kg
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # -------------------------
        # ÉQUIVALENT
        # -------------------------
        st.subheader("🌍 Équivalent")

        co2 = df["CO2"].min()

        arbres = co2 / 25
        km = co2 * 5

        st.info(f"""
🌳 {arbres:.1f} arbres nécessaires pour compenser  
🚗 équivaut à {km:.0f} km en voiture  
""")

        # -------------------------
        # CARTE
        # -------------------------
        st.subheader("🗺️ Trajet")

        coords_latlon = [(c[1], c[0]) for c in coords]

        m = folium.Map(location=coords_latlon[0], zoom_start=6)
        folium.PolyLine(coords_latlon, color="blue", weight=5).add_to(m)
        folium.Marker(coords_latlon[0], tooltip="Départ").add_to(m)
        folium.Marker(coords_latlon[-1], tooltip="Arrivée").add_to(m)

        st_folium(m, width=700, height=400)

else:
    if depart == arrivee:
        st.warning("Choisis deux villes différentes")