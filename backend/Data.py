# Greedy_Vertical_Algorithm_hybride/dataset_v21.csv
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from Greedy_Vertical_Algorithm_hybride.config import settings

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG PAGE
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FAT Smart Planner — AT Oran",
    layout="wide",
    page_icon="📡",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────────────────
# STYLE CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #0a1628 0%, #0d2b4f 100%);
        padding: 2rem 2.5rem 1.5rem;
        border-radius: 12px;
        border-left: 5px solid #f97316;
        margin-bottom: 1.5rem;
        color: white;
    }
    .kpi-card {
        background: #0d1b2e;
        border: 1px solid #1e3a5f;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
    }
    .section-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.1rem;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: #f97316;
        border-bottom: 2px solid #1e3a5f;
        padding-bottom: 0.5rem;
        margin: 1.8rem 0 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
FAT_CAPACITY = 8


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DONNÉES
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_data(path: str):
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    numeric_cols = ["lat_abonne", "lon_abonne", "etage", "lat_fat", "lon_fat",
                    "distance_real_m", "distance_FAT_m", "occupe", "nb_etages_bat", "nb_log_etage"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>📡 FTTH Smart Planner — Analyse du Dataset</h1>
    <p>Algérie Télécom Oran • Visualisation Occupation Logements</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuration")
    dataset_path = st.text_input("Chemin du dataset",
                                 value="backend/Greedy_Vertical_Algorithm_hybride/dataset_v21.csv")

    if st.button("Charger Dataset"):
        try:
            df = load_data(dataset_path)
            st.success(f"✅ {len(df):,} lignes chargées")
            st.session_state.df = df
        except Exception as e:
            st.error(f"Erreur : {e}")

if 'df' not in st.session_state:
    st.warning("Veuillez charger le dataset depuis la sidebar")
    st.stop()

df = st.session_state.df.copy()

# ─────────────────────────────────────────────────────────────────────────────
# FILTRES
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("🔎 Filtres")
selected_bat = st.sidebar.selectbox("Bâtiment", ["Tous"] + sorted(df["id_batiment"].unique()))

fdf = df.copy()
if selected_bat != "Tous":
    fdf = fdf[fdf["id_batiment"] == selected_bat]

# ─────────────────────────────────────────────────────────────────────────────
# ONGLETS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "🏠 Vue Globale",
    "📊 Occupation & Vides",
    "🏢 Bâtiment 3D (Fond Blanc)",
    "🗺️ Carte Géographique"
])

# ===================================================================
# ONGLET 1 : VUE GLOBALE
# ===================================================================
with tab1:
    st.markdown('<div class="section-title">KPIs Globaux</div>', unsafe_allow_html=True)

    if "occupe" in fdf.columns:
        n_total = len(fdf)
        n_occupes = (fdf["occupe"] == 1).sum()
        n_vides = (fdf["occupe"] == 0).sum()
        pct_occup = n_occupes / n_total * 100 if n_total > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Logements", f"{n_total:,}")
        c2.metric("Occupés", f"{n_occupes:,}", f"{pct_occup:.1f}%")
        c3.metric("Vides", f"{n_vides:,}", f"{100 - pct_occup:.1f}%")
        c4.metric("Taux Occupation", f"{pct_occup:.1f}%", delta="Objectif AT")

    st.markdown('<div class="section-title">Distribution par Étage</div>', unsafe_allow_html=True)

    if "etage" in fdf.columns and "occupe" in fdf.columns:
        pivot = pd.crosstab(fdf["etage"], fdf["occupe"], normalize="index") * 100
        fig = px.bar(pivot, title="Taux Occupation par Étage (%)",
                     labels={"etage": "Étage", "value": "Pourcentage"},
                     color_discrete_map={0: "#ef4444", 1: "#22c55e"})
        st.plotly_chart(fig, use_container_width=True)

# ===================================================================
# ONGLET 2 : OCCUPATION & VIDES
# ===================================================================
with tab2:
    st.markdown('<div class="section-title">Analyse Détaillée Occupation</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        if "occupe" in fdf.columns:
            fig_pie = px.pie(
                names=["Occupés", "Vides"],
                values=[n_occupes, n_vides],
                color_discrete_sequence=["#22c55e", "#ef4444"],
                title="Répartition Occupés vs Vides"
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        if "type_batiment" in fdf.columns and "occupe" in fdf.columns:
            type_occ = fdf.groupby("type_batiment")["occupe"].mean() * 100
            fig_type = px.bar(type_occ, title="Taux d'Occupation par Type de Bâtiment",
                              color=type_occ.values, color_continuous_scale="RdYlGn")
            st.plotly_chart(fig_type, use_container_width=True)

# ===================================================================
# ONGLET 3 : BÂTIMENT 3D (Fond Blanc)
# ===================================================================
with tab3:
    st.markdown('<div class="section-title">Vue 3D du Bâtiment (Fond Blanc)</div>', unsafe_allow_html=True)

    if selected_bat == "Tous":
        st.info("Sélectionnez un bâtiment spécifique dans la barre latérale pour voir la vue 3D détaillée.")
    else:
        bat_df = fdf[fdf["id_batiment"] == selected_bat].copy()

        if bat_df.empty:
            st.error("Aucune donnée pour ce bâtiment.")
        else:
            # ─────────────────────────────────────────────────────────────
            # SÉPARER RÉSIDENTIELS ET COMMERCES
            # ─────────────────────────────────────────────────────────────
            if "usage" in bat_df.columns:
                bat_res = bat_df[bat_df["usage"] == "logements"].copy()
                bat_com = bat_df[bat_df["usage"] == "commerces"].copy()
            else:
                bat_res = bat_df[bat_df["etage"] > 0].copy()
                bat_com = bat_df[bat_df["etage"] == 0].copy()

            hauteur_m = bat_df["Hauteur par étage (m)"].iloc[0] if "Hauteur par étage (m)" in bat_df.columns else 3.0

            # ─────────────────────────────────────────────────────────────
            # HEADER INFORMATIONS BÂTIMENT
            # ─────────────────────────────────────────────────────────────
            st.markdown("### 🏢 Informations du Bâtiment")

            col1, col2, col3, col4, col5 = st.columns(5)

            # Nombre d'étages (résidentiels uniquement)
            n_etages = bat_df["nb_etages_bat"].iloc[0] if "nb_etages_bat" in bat_df.columns else int(bat_res["etage"].max())
            col1.metric("**Nombre d'étages**", f"{n_etages}")

            # Logements par étage
            if "nb_log_etage" in bat_df.columns:
                log_par_etage = int(bat_df["nb_log_etage"].iloc[0])
                col2.metric("**Logements / étage**", f"{log_par_etage}")
            else:
                log_par_etage = int(bat_res.groupby("etage").size().mode()[0])
                col2.metric("**Logements / étage**", f"{log_par_etage}")

            # Total logements résidentiels attendus
            total_attendu = n_etages * log_par_etage
            total_reel    = len(bat_res)
            col3.metric(
                "**Total logements résid.**",
                f"{total_reel}",
                delta=None if total_reel == total_attendu else f"⚠️ attendu {total_attendu}"
            )

            # Type de bâtiment
            if "type_batiment" in bat_df.columns:
                type_bat_val = bat_df["type_batiment"].iloc[0]
                col4.metric("**Type de bâtiment**", type_bat_val)

            # Présence commerce
            if "presence_de_commerce" in bat_df.columns:
                commerce = bat_df["presence_de_commerce"].iloc[0]
                has_com_display = commerce == 1 or str(commerce).lower() in ["true", "oui", "1"]
                col5.metric("**Commerce (RDC)**", "✅ Oui" if has_com_display else "❌ Non")
            else:
                col5.metric("**Commerce (RDC)**", "N/A")

            st.markdown("---")

            # ─────────────────────────────────────────────────────────────
            # VALIDATION COHÉRENCE DES DONNÉES
            # ─────────────────────────────────────────────────────────────
            if total_reel != total_attendu:
                st.warning(
                    f"⚠️ Incohérence détectée : {total_reel} logements résidentiels dans les données "
                    f"pour {n_etages} étages × {log_par_etage} logements/étage = {total_attendu} attendus. "
                    f"Vérifiez la génération (Generateur.py étape 5)."
                )

            # Vérifier que toutes les colonnes ont le bon nombre de points
            if not bat_res.empty:
                points_par_colonne = bat_res.groupby("appt_in_floor")["etage"].count() if "appt_in_floor" in bat_res.columns else None
                if points_par_colonne is not None:
                    min_pts = int(points_par_colonne.min())
                    max_pts = int(points_par_colonne.max())
                    if min_pts != max_pts:
                        st.warning(
                            f"⚠️ Colonnes de longueurs inégales : min={min_pts} pts, max={max_pts} pts "
                            f"(attendu {n_etages} partout). Régénérez le dataset après correction de Generateur.py."
                        )
                    else:
                        st.success(f"✅ {log_par_etage} colonnes × {min_pts} étages chacune — structure correcte.")

            # ─────────────────────────────────────────────────────────────
            # VUE 3D
            # ─────────────────────────────────────────────────────────────
            if all(col in bat_df.columns for col in ["lat_abonne", "lon_abonne", "etage"]):

                fig3d = go.Figure()

                color_map = {1: "#22c55e", 0: "#ef4444"}

                # ── Trace 1 : logements résidentiels ──────────────────────
                if not bat_res.empty:
                    colors_res = (
                        bat_res["occupe"].map(color_map).tolist()
                        if "occupe" in bat_res.columns
                        else ["#38bdf8"] * len(bat_res)
                    )
                    hover_res = (
                        bat_res.apply(
                            lambda r: f"{'Occupé' if r['occupe'] == 1 else 'Vide'} | Étage {int(r['etage'])} | Palier {int(r['appt_in_floor']) + 1 if 'appt_in_floor' in bat_res.columns else '?'}",
                            axis=1
                        ).tolist()
                        if "occupe" in bat_res.columns
                        else ["Logement"] * len(bat_res)
                    )

                    fig3d.add_trace(go.Scatter3d(
                        x=bat_res["lon_abonne"],
                        y=bat_res["lat_abonne"],
                        z=bat_res["etage"] * hauteur_m,   # étages 1..N → Z positif
                        mode="markers",
                        marker=dict(
                            size=6,
                            color=colors_res,
                            opacity=0.85,
                            line=dict(color="black", width=0.5)
                        ),
                        name="Logements résidentiels",
                        text=hover_res,
                        hovertemplate="<b>%{text}</b><br>Hauteur: %{z:.1f} m<br>Lon: %{x:.5f} | Lat: %{y:.5f}<extra></extra>"
                    ))

                # ── Trace 2 : commerces RDC ────────────────────────────────
                if not bat_com.empty:
                    hover_com = (
                        bat_com["occupe"].map({1: "Commerce occupé", 0: "Commerce vide"}).tolist()
                        if "occupe" in bat_com.columns
                        else ["Commerce RDC"] * len(bat_com)
                    )
                    colors_com = (
                        bat_com["occupe"].map(color_map).tolist()
                        if "occupe" in bat_com.columns
                        else ["#f97316"] * len(bat_com)
                    )

                    fig3d.add_trace(go.Scatter3d(
                        x=bat_com["lon_abonne"],
                        y=bat_com["lat_abonne"],
                        z=[0.0] * len(bat_com),           # RDC = niveau 0
                        mode="markers",
                        marker=dict(
                            size=8,
                            color=colors_com,
                            symbol="diamond",
                            opacity=0.9,
                            line=dict(color="black", width=0.5)
                        ),
                        name="Commerces RDC",
                        text=hover_com,
                        hovertemplate="<b>%{text}</b><br>Lon: %{x:.5f} | Lat: %{y:.5f}<extra></extra>"
                    ))

                # ── Légende manuelle occupation ────────────────────────────
                legend_items = [
                    ("Logement Occupé", "#22c55e", "circle"),
                    ("Logement Vide", "#ef4444", "circle"),
                    ("Commerce Occupé", "#22c55e", "diamond"),
                    ("Commerce Vide", "#ef4444", "diamond")
                ]
                for label, color, symbol in legend_items:
                    fig3d.add_trace(go.Scatter3d(
                        x=[None], y=[None], z=[None],
                        mode="markers",
                        marker=dict(size=7, color=color, symbol=symbol, line=dict(color="black", width=0.5)),
                        name=label,
                        showlegend=True
                    ))

                fig3d.update_layout(
                    title=dict(
                        text=f" {n_etages} étages × {log_par_etage} logements/étage",
                        font=dict(color="black", size=14)
                    ),
                    scene=dict(
                        xaxis_title="Longitude",
                        yaxis_title="Latitude",
                        zaxis_title="Hauteur (m)",
                        bgcolor="white",
                        xaxis=dict(gridcolor="lightgray", color="black", showbackground=True, backgroundcolor="white"),
                        yaxis=dict(gridcolor="lightgray", color="black", showbackground=True, backgroundcolor="white"),
                        zaxis=dict(
                            gridcolor="lightgray",
                            color="black",
                            showbackground=True,
                            backgroundcolor="white",
                            range=[0, (n_etages + 1) * hauteur_m]  # axe Z fixe = toutes colonnes visibles
                        ),
                        aspectmode="manual",
                        aspectratio=dict(x=1.5, y=1.5, z=1.0)
                    ),
                    paper_bgcolor="white",
                    font=dict(color="black"),
                    height=680,
                    legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor="lightgray", borderwidth=1)
                )

                st.plotly_chart(fig3d, use_container_width=True)

                # ── Tableau debug optionnel ────────────────────────────────
                with st.expander("🔎 Debug — Points par colonne (palier × étage)"):
                    if "appt_in_floor" in bat_res.columns:
                        pivot_debug = bat_res.pivot_table(
                            index="etage",
                            columns="appt_in_floor",
                            values="occupe",
                            aggfunc="count",
                            fill_value=0
                        )
                        pivot_debug.columns = [f"Palier {c+1}" for c in pivot_debug.columns]
                        pivot_debug.index   = [f"Étage {i}" for i in pivot_debug.index]
                        st.dataframe(pivot_debug, use_container_width=True)
                    else:
                        st.info("Colonne 'appt_in_floor' absente — debug non disponible.")
# ===================================================================
# ONGLET 4 : CARTE
# ===================================================================
with tab4:
    st.markdown('<div class="section-title">Carte Géographique — Occupés vs Vides</div>', unsafe_allow_html=True)

    map_filter = st.radio("Filtre :",
                          ["Tous", "Occupés seulement", "Vides seulement"],
                          horizontal=True)

    map_df = fdf.dropna(subset=["lat_abonne", "lon_abonne"]).copy()

    if map_filter == "Occupés seulement":
        map_df = map_df[map_df["occupe"] == 1]
    elif map_filter == "Vides seulement":
        map_df = map_df[map_df["occupe"] == 0]

    color_map = {1: "#22c55e", 0: "#ef4444"}

    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat_abonne", lon="lon_abonne",
        color="occupe" if "occupe" in map_df.columns else None,
        color_discrete_map=color_map,
        hover_name="code_client",
        hover_data=["etage", "type_batiment"],
        zoom=15,
        height=700,
        mapbox_style="carto-positron",  # fond clair
        title=f"Emplacements des logements — {map_filter}"
    )
    fig_map.update_traces(marker=dict(size=7, opacity=0.75))
    st.plotly_chart(fig_map, use_container_width=True)

st.caption("Dashboard FTTH Smart Plannergenerator")