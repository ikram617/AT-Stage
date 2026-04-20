import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

st.set_page_config(page_title="FTTH Dataset Explorer", layout="wide", page_icon="📡")


# ====================== CHARGEMENT DU DATASET ======================
@st.cache_data
def load_data():
    df = pd.read_csv(r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_annaba4v2\dataset_fusionnee_final.csv")
    # Les identifiants générés par generer.py (ex: Bat-1, Bat-2) sont désormais strictement uniques.
    # On n'a plus besoin de séparer artificiellement par coordonnées GPS.
    df['building_uid'] = df['id_batiment']

    numeric_cols = ['lat_abonne', 'lon_abonne', 'etage', 'porte', 'lat_fat', 'lon_fat',
                    'distance_olt_m', 'nbr_etages', 'nbr_logements_par_etage',
                    'nbr_logements_total']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


df = load_data()

st.title("📡 FTTH Smart Planner - Dataset Explorer")
st.caption(f"Dataset chargé : **{len(df):,} lignes** | {df.shape[1]} colonnes")

# ====================== SIDEBAR ======================
st.sidebar.header("🔎 Filtres")

selected_uid = st.sidebar.selectbox(
    "Sélectionner un Bâtiment (Unique)",
    options=["Tous"] + sorted(df['building_uid'].unique()),
    index=0
)

# Filtrage
filtered_df = df.copy()
if selected_uid != "Tous":
    filtered_df = filtered_df[filtered_df['building_uid'] == selected_uid]

# ====================== TABS ======================
tab1, tab2, tab3, tab4 = st.tabs(
    ["📊 Overview & Stats", "📈 Distributions", "🏢 3D Building View", "🗺️ Carte Géographique"])

with tab1:
    st.subheader("📊 Statistiques Générales")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Abonnés", len(filtered_df))
    col2.metric("Blocs Physiques", filtered_df['building_uid'].nunique())
    col3.metric("FATs uniques", filtered_df['FAT_relative'].nunique())
    col4.metric("Quartiers", filtered_df['quartier'].nunique())

    st.subheader("Top 10 Batiments les plus peuplés")
    top_bat = filtered_df.groupby('id_batiment').size().nlargest(10)
    st.bar_chart(top_bat)

with tab2:
    st.subheader("Distribution des données")
    colA, colB = st.columns(2)

    with colA:
        fig = px.histogram(filtered_df, x="etage", nbins=30, title="Distribution par Étage")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.histogram(filtered_df, x="nbr_logements_total", title="Nombre total de logements par bâtiment")
        st.plotly_chart(fig2, use_container_width=True)

    with colB:
        fig3 = px.box(filtered_df, y="distance_olt_m", title="Distance OLT (mètres)")
        st.plotly_chart(fig3, use_container_width=True)

        fig4 = px.histogram(filtered_df, x="nbr_logements_par_etage", title="Logements par étage")
        st.plotly_chart(fig4, use_container_width=True)

with tab3:
    st.subheader("🏢 Visualisation 3D du Bâtiment (Abonnés + FATs)")

    if selected_uid == "Tous":
        st.info("Sélectionne un bâtiment spécifique dans la sidebar pour voir la vue 3D")
    else:
        # Vérification des doublons de porte
        dupes = filtered_df[filtered_df.duplicated(subset=['porte'], keep=False)]
        if not dupes.empty:
            st.warning(f"⚠️ Attention : {len(dupes)} abonnés partagent un numéro de porte identique dans ce bloc.")
            if st.checkbox("Voir les doublons"):
                st.write(dupes[['code_client', 'etage', 'porte', 'id_batiment']])

        bat_df = filtered_df # Déjà filtré par selected_uid

        if len(bat_df) == 0:
            st.warning("Aucune donnée pour ce bâtiment")
        else:
            # 3D Scatter : Abonnés (Z = étage)
            fig_3d = go.Figure()

            # Abonnés
            fig_3d.add_trace(go.Scatter3d(
                x=bat_df['lon_abonne'],
                y=bat_df['lat_abonne'],
                z=bat_df['etage'],
                mode='markers',
                marker=dict(size=4, color=bat_df['etage'], colorscale='Viridis', opacity=0.8),
                name='Abonnés'
            ))

            # FATs
            fat_df = bat_df.drop_duplicates(subset=['lat_fat', 'lon_fat'])
            fig_3d.add_trace(go.Scatter3d(
                x=fat_df['lon_fat'],
                y=fat_df['lat_fat'],
                z=fat_df['etage'] + 0.5,  # légèrement au-dessus
                mode='markers',
                marker=dict(size=8, color='red', symbol='diamond'),
                name='FATs'
            ))

            fig_3d.update_layout(
                title=f"3D - {selected_uid} ({len(bat_df)} abonnés)",
                scene=dict(
                    xaxis_title="Longitude",
                    yaxis_title="Latitude",
                    zaxis_title="Étage",
                    aspectmode="cube"
                ),
                height=700,
                margin=dict(l=0, r=0, b=0, t=40)
            )
            st.plotly_chart(fig_3d, use_container_width=True)

with tab4:
    st.subheader("🗺️ Carte Géographique des Abonnés & FATs")

    if len(filtered_df) > 5000:
        st.warning("Trop de points → affichage d’un échantillon de 5000 points")
        map_df = filtered_df.sample(5000)
    else:
        map_df = filtered_df

    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat_abonne",
        lon="lon_abonne",
        color="etage",
        hover_name="code_client",
        hover_data=["id_batiment", "porte", "FAT_relative"],
        zoom=15,
        height=700,
        mapbox_style="open-street-map"
    )
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

# ====================== INSIGHTS AUTOMATIQUES ======================
st.subheader("💡 Insights intéressants")
col_ins1, col_ins2 = st.columns(2)

with col_ins1:
    st.write("**Bâtiment le plus chargé**")
    max_bat = filtered_df.groupby('building_uid').size().idxmax()
    st.success(f"{max_bat} → {filtered_df[filtered_df['building_uid'] == max_bat].shape[0]} abonnés")

with col_ins2:
    st.write("**FAT le plus utilisé**")
    if 'FAT_relative' in filtered_df.columns:
        top_fat = filtered_df['FAT_relative'].value_counts().idxmax()
        st.info(f"FAT {top_fat} → {filtered_df[filtered_df['FAT_relative'] == top_fat].shape[0]} abonnés")

st.caption("Dataset finalisé avec suc"
           "cès ! Prêt pour analyse FTTH.")