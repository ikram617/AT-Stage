# =============================================================.
# FTTH Smart Planner — Dashboard d'Analyse & Rapport de Stage
# Algérie Télécom Oran — Stage ING4
#
# STRUCTURE DU DASHBOARD (6 onglets) :
#   1. Vue Réseau    → KPIs globaux, santé du dataset
#   2. Qualité Data  → conformité capacité FAT, GPS, étages
#   3. Bâtiment 3D   → scatter 3D par bâtiment, colonnes verticales
#   4. Modèle FAT    → distribution clusters, gaspillage câble, ARI
#   5. Carte Géo     → Mapbox abonnés + FATs + FDTs
#   6. Export        → tableaux prêts pour mémoire de stage
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG PAGE
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FTTH Smart Planner — AT Oran",
    layout="wide",
    page_icon="📡",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────────────────
# STYLE CSS  — thème industriel télécoms (bleu AT + orange accent)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }
    h1, h2, h3 {
        font-family: 'IBM Plex Mono', monospace;
        letter-spacing: -0.5px;
    }

    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, #0a1628 0%, #0d2b4f 60%, #0f3460 100%);
        padding: 2rem 2.5rem 1.5rem;
        border-radius: 12px;
        border-left: 4px solid #f97316;
        margin-bottom: 1.5rem;
    }
    .main-header h1 {
        color: #f0f4ff;
        font-size: 1.6rem;
        margin: 0;
        font-family: 'IBM Plex Mono', monospace;
    }
    .main-header p {
        color: #94a3b8;
        margin: 0.3rem 0 0;
        font-size: 0.85rem;
    }
    .orange-tag {
        background: #f97316;
        color: white;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-family: 'IBM Plex Mono', monospace;
        letter-spacing: 1px;
    }

    /* KPI cards */
    .kpi-card {
        background: #0d1b2e;
        border: 1px solid #1e3a5f;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        transition: border-color 0.2s;
    }
    .kpi-card:hover { border-color: #f97316; }
    .kpi-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 2rem;
        font-weight: 600;
        color: #38bdf8;
        line-height: 1;
    }
    .kpi-label {
        color: #64748b;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.4rem;
    }
    .kpi-delta {
        font-size: 0.75rem;
        margin-top: 0.3rem;
    }
    .delta-ok  { color: #22c55e; }
    .delta-warn{ color: #f97316; }
    .delta-err { color: #ef4444; }

    /* Section titles */
    .section-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: #f97316;
        border-bottom: 1px solid #1e3a5f;
        padding-bottom: 0.5rem;
        margin: 1.5rem 0 1rem;
    }

    /* Badge conformité */
    .badge-ok   { background:#166534; color:#bbf7d0; padding:2px 10px; border-radius:20px; font-size:0.8rem; }
    .badge-warn { background:#7c2d12; color:#fed7aa; padding:2px 10px; border-radius:20px; font-size:0.8rem; }
    .badge-err  { background:#7f1d1d; color:#fecaca; padding:2px 10px; border-radius:20px; font-size:0.8rem; }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #0a1628;
        border-radius: 8px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        color: #64748b;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.8rem;
    }
    .stTabs [aria-selected="true"] {
        background: #0f3460 !important;
        color: #38bdf8 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #060f1e;
        border-right: 1px solid #1e3a5f;
    }

    /* Plotly charts dark bg */
    .js-plotly-plot { border-radius: 8px; }

    /* Conformité table */
    .conf-table td, .conf-table th {
        padding: 8px 14px;
        font-size: 0.82rem;
        font-family: 'IBM Plex Mono', monospace;
    }
    .conf-table tr:nth-child(even) { background: #0d1b2e; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES MÉTIER
# ─────────────────────────────────────────────────────────────────────────────
FAT_CAPACITY = 8
CABLE_STANDARDS = [15, 20, 50, 80]  # mètres
TORTUOSITY = 1.3
MAX_DIST_OLT_M = 12_000

PLOTLY_DARK = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#0a1628",
    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
    margin=dict(l=20, r=20, t=40, b=20),
)


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DONNÉES
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Chargement dataset...")
def load_data(path: str) -> pd.DataFrame:
    """
    Charge et normalise le dataset fusionné produit par generer.py.

    Colonnes attendues (au minimum) :
        code_client, id_batiment, id_zone,
        lat_abonne, lon_abonne, etage, porte,
        FAT_relative, usage,
        lat_fdt, lon_fdt, nom_FDT,
        type_batiment, nbr_etages, nbr_logements_par_etage
    """
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    numeric = [
        "lat_abonne", "lon_abonne", "etage", "porte",
        "lat_fat", "lon_fat", "lat_fdt", "lon_fdt",
        "distance_olt_m", "nbr_etages", "nbr_logements_par_etage",
        "nbr_logements_total", "distance_FAT_m", "nb_abonnes_sim"
    ]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Colonne porte unifiée
    if "porte" not in df.columns and "numero_porte" in df.columns:
        df["porte"] = df["numero_porte"]

    # Snap câble drop
    if "distance_FAT_m" in df.columns:
        df["cable_snap"] = df["distance_FAT_m"].apply(
            lambda d: next((s for s in CABLE_STANDARDS if d <= s), CABLE_STANDARDS[-1])
            if pd.notna(d) else np.nan
        )
        df["cable_waste_m"] = df.apply(
            lambda r: max(0, r["cable_snap"] - r["distance_FAT_m"])
            if pd.notna(r.get("cable_snap")) else np.nan,
            axis=1
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px">
        <span class="orange-tag">STAGE ING4</span>
        <span class="orange-tag">AT ORAN</span>
    </div>
    <h1>📡 FTTH Smart Planner — Analyse du Dataset</h1>
    <p>Algérie Télécom · Planification FAT automatisée · Visualisation & Rapport</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — Chargement + Filtres
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Configuration")

    dataset_path = st.text_input(
        "Chemin CSV",
        value=r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v13_advanced\dataset_fusionnee_final.csv",
        help="Chemin absolu vers le fichier dataset_fusionnee_final.csv"
    )
    print(pd.read_csv(r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v13_advanced\dataset_fusionnee_final.csv").head(200).to_string())
    if not dataset_path:
        st.warning("Spécifie le chemin du dataset.")
        st.stop()

    try:
        df = load_data(dataset_path)
    except FileNotFoundError:
        st.error(f"Fichier introuvable :\n`{dataset_path}`")
        st.stop()
    except Exception as e:
        st.error(f"Erreur chargement : {e}")
        st.stop()

    st.success(f"✅ {len(df):,} lignes chargées")
    st.divider()

    st.markdown("### 🔎 Filtres")

    # Filtre type bâtiment
    types_bat = ["Tous"] + sorted(df["type_batiment"].dropna().unique().tolist()) \
        if "type_batiment" in df.columns else ["Tous"]
    selected_type = st.selectbox("Type bâtiment", types_bat)

    # Filtre bâtiment individuel
    all_bats = sorted(df["id_batiment"].dropna().unique().tolist())
    selected_bat = st.selectbox(
        "Bâtiment individuel",
        ["Tous"] + all_bats,
        index=0,
        help="Sélectionne un bâtiment pour la vue 3D détaillée"
    )

    # Filtre usage
    usages = ["Tous"] + sorted(df["usage"].dropna().unique().tolist()) \
        if "usage" in df.columns else ["Tous"]
    selected_usage = st.selectbox("Usage", usages)

    st.divider()
    st.markdown("### 📐 Paramètres AT")
    fat_cap_display = st.number_input("Capacité FAT (abonnés)", value=FAT_CAPACITY, disabled=True)
    hauteur_etage = st.number_input("Hauteur étage (m)", value=3.0, step=0.5)

# ─────────────────────────────────────────────────────────────────────────────
# FILTRAGE
# ─────────────────────────────────────────────────────────────────────────────
fdf = df.copy()
if selected_type != "Tous" and "type_batiment" in fdf.columns:
    fdf = fdf[fdf["type_batiment"] == selected_type]
if selected_bat != "Tous":
    fdf = fdf[fdf["id_batiment"] == selected_bat]
if selected_usage != "Tous" and "usage" in fdf.columns:
    fdf = fdf[fdf["usage"] == selected_usage]

# ─────────────────────────────────────────────────────────────────────────────
# CALCULS GLOBAUX
# ─────────────────────────────────────────────────────────────────────────────
n_abonnes = len(fdf)
n_batiments = fdf["id_batiment"].nunique()
n_fats = fdf["FAT_relative"].nunique() if "FAT_relative" in fdf.columns else 0
n_fdts = fdf["nom_FDT"].nunique() if "nom_FDT" in fdf.columns else 0

# Conformité capacité FAT
if "FAT_relative" in fdf.columns:
    fat_sizes = fdf.groupby("FAT_relative").size()
    n_fats_ok = (fat_sizes <= FAT_CAPACITY).sum()
    n_fats_over = (fat_sizes > FAT_CAPACITY).sum()
    pct_ok = n_fats_ok / len(fat_sizes) * 100 if len(fat_sizes) > 0 else 0
else:
    fat_sizes = pd.Series(dtype=int)
    pct_ok = 0
    n_fats_over = 0

# Abonnés par bâtiment
bat_sizes = fdf.groupby("id_batiment").size()

# ─────────────────────────────────────────────────────────────────────────────
# ONGLETS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Vue Réseau",
    "🔬 Qualité Données",
    "🏢 Bâtiment 3D",
    "📊 Analyse FAT",
    "🗺️ Carte Géo",
    "📋 Export Rapport"
])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — VUE RÉSEAU
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<div class="section-title">KPIs Réseau</div>', unsafe_allow_html=True)

    # ── KPI Cards ────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5, k6 = st.columns(6)


    def kpi(col, val, label, delta_html=""):
        col.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{val}</div>
            <div class="kpi-label">{label}</div>
            {delta_html}
        </div>""", unsafe_allow_html=True)


    kpi(k1, f"{n_abonnes:,}", "Abonnés")
    kpi(k2, f"{n_batiments:,}", "Bâtiments")
    kpi(k3, f"{n_fats:,}", "FATs")
    kpi(k4, f"{n_fdts:,}", "FDTs")

    avg_per_bat = n_abonnes / n_batiments if n_batiments > 0 else 0
    kpi(k5, f"{avg_per_bat:.1f}", "Moy. ab/bât")

    conf_color = "delta-ok" if pct_ok >= 99 else "delta-warn" if pct_ok >= 95 else "delta-err"
    kpi(k6, f"{pct_ok:.1f}%", "Conf. FAT",
        f'<div class="kpi-delta {conf_color}">≤8 ab/FAT</div>')

    st.markdown('<div class="section-title">Distribution des bâtiments</div>',
                unsafe_allow_html=True)

    c1, c2 = st.columns(2)

    with c1:
        # Abonnés par bâtiment — top 30
        top30 = bat_sizes.nlargest(30).reset_index()
        top30.columns = ["Bâtiment", "Abonnés"]
        top30["Bâtiment"] = top30["Bâtiment"].str[:35] + "..."

        fig = px.bar(
            top30, x="Abonnés", y="Bâtiment",
            orientation="h",
            title="Top 30 bâtiments par nombre d'abonnés",
            color="Abonnés",
            color_continuous_scale=["#0f3460", "#0ea5e9", "#38bdf8"],
        )
        fig.update_layout(**PLOTLY_DARK, height=500,
                          yaxis=dict(tickfont=dict(size=9)))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        # Distribution des tailles de bâtiment (histogramme)
        fig2 = px.histogram(
            bat_sizes.reset_index(), x=0, nbins=30,
            title="Distribution : abonnés / bâtiment",
            labels={0: "Abonnés"},
            color_discrete_sequence=["#f97316"],
        )
        fig2.add_vline(x=8, line_dash="dash", line_color="#38bdf8",
                       annotation_text="1 FAT", annotation_font_size=10)
        fig2.add_vline(x=FAT_CAPACITY, line_dash="dot", line_color="#22c55e",
                       annotation_text="Capacité max", annotation_font_size=10)
        fig2.update_layout(**PLOTLY_DARK, height=250)
        st.plotly_chart(fig2, use_container_width=True)

        # Type bâtiment pie
        if "type_batiment" in fdf.columns:
            type_counts = fdf["type_batiment"].value_counts().reset_index()
            type_counts.columns = ["Type", "Count"]
            fig3 = px.pie(
                type_counts, values="Count", names="Type",
                title="Répartition par type de bâtiment",
                color_discrete_sequence=px.colors.sequential.Blues_r,
                hole=0.4
            )
            fig3.update_layout(**PLOTLY_DARK, height=240,
                               showlegend=True,
                               legend=dict(font=dict(size=9)))
            st.plotly_chart(fig3, use_container_width=True)

    # ── Résumé textuel ───────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Résumé Dataset</div>', unsafe_allow_html=True)
    rc1, rc2, rc3 = st.columns(3)

    with rc1:
        st.markdown("**Colonnes disponibles**")
        col_status = {
            "etage": "etage" in fdf.columns,
            "porte": any(c in fdf.columns for c in ["porte", "numero_porte"]),
            "FAT_relative": "FAT_relative" in fdf.columns,
            "lat_fdt": "lat_fdt" in fdf.columns,
            "type_batiment": "type_batiment" in fdf.columns,
            "usage": "usage" in fdf.columns,
            "distance_FAT_m": "distance_FAT_m" in fdf.columns,
        }
        for col, ok in col_status.items():
            icon = "✅" if ok else "❌"
            st.markdown(f"{icon} `{col}`")

    with rc2:
        st.markdown("**Statistiques étages**")
        if "etage" in fdf.columns:
            st.markdown(f"- Étage min : **{int(fdf['etage'].min())}**")
            st.markdown(f"- Étage max : **{int(fdf['etage'].max())}**")
            st.markdown(f"- Étage moyen : **{fdf['etage'].mean():.1f}**")
            n_rdc = (fdf["etage"] == 0).sum()
            st.markdown(f"- Abonnés RDC (étage 0) : **{n_rdc:,}**")

    with rc3:
        st.markdown("**Santé données**")
        nulls = fdf[["lat_abonne", "lon_abonne", "etage", "FAT_relative"]].isnull().sum()
        for col, n in nulls.items():
            pct = n / len(fdf) * 100
            icon = "✅" if n == 0 else "⚠️" if pct < 5 else "❌"
            st.markdown(f"{icon} `{col}` : {n:,} NaN ({pct:.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 2 — QUALITÉ DONNÉES
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-title">Conformité Capacité FAT (≤8 abonnés)</div>',
                unsafe_allow_html=True)

    if len(fat_sizes) > 0:
        qa1, qa2, qa3 = st.columns(3)

        with qa1:
            # Gauge conformité
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=pct_ok,
                delta={"reference": 100, "valueformat": ".1f"},
                title={"text": "Conformité FAT (%)", "font": {"size": 13}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "#64748b"},
                    "bar": {"color": "#22c55e" if pct_ok >= 99 else "#f97316"},
                    "steps": [
                        {"range": [0, 90], "color": "#1e3a5f"},
                        {"range": [90, 98], "color": "#1e3a5f"},
                        {"range": [98, 100], "color": "#0f3460"},
                    ],
                    "threshold": {
                        "line": {"color": "#38bdf8", "width": 3},
                        "thickness": 0.8,
                        "value": 99
                    }
                }
            ))
            fig_gauge.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8"),
                height=260, margin=dict(l=30, r=30, t=50, b=10)
            )
            st.plotly_chart(fig_gauge, use_container_width=True)

        with qa2:
            # Distribution tailles FAT
            fat_df_qa = fat_sizes.reset_index()
            fat_df_qa.columns = ["FAT", "Abonnés"]
            fig_fat_dist = px.histogram(
                fat_df_qa, x="Abonnés", nbins=FAT_CAPACITY,
                title="Distribution taille des FATs",
                color_discrete_sequence=["#0ea5e9"],
            )
            fig_fat_dist.add_vline(
                x=FAT_CAPACITY, line_dash="dash", line_color="#ef4444",
                annotation_text=f"Limite {FAT_CAPACITY}", annotation_font_size=10
            )
            fig_fat_dist.update_layout(**PLOTLY_DARK, height=260)
            st.plotly_chart(fig_fat_dist, use_container_width=True)

        with qa3:
            # FATs surcapacité
            surcharge = fat_sizes[fat_sizes > FAT_CAPACITY]
            if len(surcharge) > 0:
                st.error(f"**{len(surcharge)} FATs en surcapacité !**")
                surcharge_df = surcharge.reset_index()
                surcharge_df.columns = ["FAT", "Abonnés"]
                surcharge_df["Dépassement"] = surcharge_df["Abonnés"] - FAT_CAPACITY
                st.dataframe(
                    surcharge_df.sort_values("Dépassement", ascending=False).head(20),
                    use_container_width=True, height=220
                )
            else:
                st.success("✅ Aucune FAT en surcapacité !")
                st.markdown(f"""
                <div style='text-align:center; padding:2rem;'>
                    <div style='font-size:3rem'>🎯</div>
                    <div style='color:#22c55e; font-family:IBM Plex Mono; margin-top:0.5rem;'>
                        100% conformité<br>capacité FAT
                    </div>
                </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">Analyse câbles drop (15/20/50/80m)</div>',
                unsafe_allow_html=True)

    if "cable_snap" in fdf.columns:
        cb1, cb2 = st.columns(2)

        with cb1:
            cable_dist = fdf["cable_snap"].value_counts().sort_index().reset_index()
            cable_dist.columns = ["Longueur (m)", "Fréquence"]
            cable_dist["Pct"] = (cable_dist["Fréquence"] / cable_dist["Fréquence"].sum() * 100).round(1)

            colors_cable = {15: "#22c55e", 20: "#84cc16", 50: "#f97316", 80: "#ef4444"}
            fig_cable = px.bar(
                cable_dist,
                x="Longueur (m)", y="Fréquence",
                text="Pct",
                title="Distribution des longueurs de câble drop utilisées",
                color="Longueur (m)",
                color_discrete_map=colors_cable,
            )
            fig_cable.update_traces(texttemplate="%{text}%", textposition="outside")
            fig_cable.update_layout(**PLOTLY_DARK, height=300,
                                    showlegend=False,
                                    xaxis=dict(type="category"))
            st.plotly_chart(fig_cable, use_container_width=True)

            # Insight câble
            pct_court = cable_dist[cable_dist["Longueur (m)"] <= 20]["Pct"].sum()
            st.markdown(f"""
            **💡 Lecture AT :** {pct_court:.1f}% des câbles drop utilisent ≤ 20m
            (câbles courts = **meilleure qualité signal + moindre coût**).
            Objectif ingénieur : maximiser cette proportion.
            """)

        with cb2:
            if "cable_waste_m" in fdf.columns:
                waste_per_bat = fdf.groupby("id_batiment")["cable_waste_m"].sum().reset_index()
                waste_per_bat.columns = ["Bâtiment", "Gaspillage (m)"]
                waste_per_bat["Bâtiment"] = waste_per_bat["Bâtiment"].str[:30] + "..."
                top_waste = waste_per_bat.nlargest(15, "Gaspillage (m)")

                fig_waste = px.bar(
                    top_waste,
                    x="Gaspillage (m)", y="Bâtiment",
                    orientation="h",
                    title="Top 15 bâtiments — gaspillage câble (m)",
                    color="Gaspillage (m)",
                    color_continuous_scale=["#0f3460", "#ef4444"],
                )
                fig_waste.update_layout(**PLOTLY_DARK, height=300,
                                        yaxis=dict(tickfont=dict(size=8)))
                st.plotly_chart(fig_waste, use_container_width=True)

    st.markdown('<div class="section-title">Cohérence GPS — Points hors polygone</div>',
                unsafe_allow_html=True)

    if all(c in fdf.columns for c in ["lat_abonne", "lon_abonne"]):
        # Calcul de la dispersion GPS par bâtiment
        # Un bon générateur produit des points dans le polygone OSM
        # On mesure l'écart-type GPS par bâtiment comme proxy de qualité
        gps_std = fdf.groupby("id_batiment").agg(
            std_lat=("lat_abonne", "std"),
            std_lon=("lon_abonne", "std"),
            n=("code_client", "count")
        ).reset_index()
        gps_std["spread_m"] = (
                np.sqrt(gps_std["std_lat"] ** 2 + gps_std["std_lon"] ** 2) * 111_000
        )

        gc1, gc2 = st.columns([2, 1])

        with gc1:
            fig_spread = px.histogram(
                gps_std, x="spread_m", nbins=40,
                title="Distribution de la dispersion GPS par bâtiment (mètres)",
                color_discrete_sequence=["#38bdf8"],
                labels={"spread_m": "Dispersion GPS (m, std)"}
            )
            fig_spread.add_vline(x=50, line_dash="dash", line_color="#f97316",
                                 annotation_text="50m — limite acceptable")
            fig_spread.update_layout(**PLOTLY_DARK, height=280)
            st.plotly_chart(fig_spread, use_container_width=True)

        with gc2:
            spread_median = gps_std["spread_m"].median()
            spread_max = gps_std["spread_m"].max()
            n_large = (gps_std["spread_m"] > 50).sum()

            st.markdown(f"""
            **Dispersion GPS médiane :**  
            `{spread_median:.1f} m`

            **Dispersion max :**  
            `{spread_max:.1f} m`

            **Bâtiments > 50m :**  
            `{n_large} / {len(gps_std)}`

            {"✅ Dispersion maîtrisée" if n_large == 0 else f"⚠️ {n_large} bâtiment(s) à vérifier"}
            """)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 3 — BÂTIMENT 3D
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-title">Visualisation 3D — Abonnés & FATs</div>',
                unsafe_allow_html=True)

    if selected_bat == "Tous":
        st.info("👆 Sélectionne un bâtiment spécifique dans la **sidebar** pour voir la vue 3D.")

        # Vue d'ensemble multi-bâtiments (2D scatter coloré par FAT)
        st.markdown("##### Aperçu global — distribution GPS des abonnés")
        sample = fdf.sample(min(3000, len(fdf)))
        if all(c in sample.columns for c in ["lon_abonne", "lat_abonne", "etage"]):
            fig_overview = px.scatter(
                sample,
                x="lon_abonne", y="lat_abonne",
                color="etage",
                title=f"Abonnés (échantillon {len(sample):,}) — colorés par étage",
                color_continuous_scale="Turbo",
                opacity=0.6,
                size_max=4,
            )
            fig_overview.update_traces(marker=dict(size=3))
            fig_overview.update_layout(**PLOTLY_DARK, height=450)
            st.plotly_chart(fig_overview, use_container_width=True)

    else:
        bat_df = fdf[fdf["id_batiment"] == selected_bat].copy()
        if bat_df.empty:
            st.warning("Aucune donnée pour ce bâtiment.")
        else:
            # ── Statistiques bâtiment ─────────────────────────────────────
            bi1, bi2, bi3, bi4, bi5, bi6 = st.columns(6)
            bi1.metric("Abonnés", len(bat_df))
            bi2.metric("Étages", f"{int(bat_df['etage'].min())}-{int(bat_df['etage'].max())}"
            if "etage" in bat_df.columns else "?")
            bi3.metric("FATs", bat_df["FAT_relative"].nunique()
            if "FAT_relative" in bat_df.columns else "?")

            if "nbr_logements_par_etage" in bat_df.columns:
                bi4.metric("Log/étage", int(bat_df["nbr_logements_par_etage"].iloc[0]))
            if "type_batiment" in bat_df.columns:
                bi5.metric("Type", bat_df["type_batiment"].iloc[0])
            
            if "presence_de_commerce" in bat_df.columns:
                has_comm = bat_df["presence_de_commerce"].iloc[0]
                bi6.metric("Commerce", "OUI" if has_comm == 1 else "NON")
            else:
                bi6.metric("Commerce", "?")
            # ── Vue 3D abonnés + FATs ─────────────────────────────────────
            fig3d = go.Figure()

            # --- Visualisation de l'emprise du bâtiment (RDC) ---
            # Utile pour voir l'étage 0 même s'il n'y a pas d'abonnés (commerce=0)
            if all(c in bat_df.columns for c in ["lon_abonne", "lat_abonne"]):
                lons, lats = bat_df["lon_abonne"], bat_df["lat_abonne"]
                # Marge pour englober les colonnes d'abonnés
                m_deg = 3.0 / 111000 
                x0, x1 = lons.min() - m_deg, lons.max() + m_deg
                y0, y1 = lats.min() - m_deg, lats.max() + m_deg
                
                fig3d.add_trace(go.Scatter3d(
                    x=[x0, x1, x1, x0, x0],
                    y=[y0, y0, y1, y1, y0],
                    z=[0, 0, 0, 0, 0],
                    mode="lines",
                    line=dict(color="rgba(148, 163, 184, 0.4)", width=3),
                    name="Socle RDC",
                    hoverinfo="skip"
                ))

            # Abonnés colorés par FAT_relative
            if "FAT_relative" in bat_df.columns:
                fat_codes = pd.Categorical(bat_df["FAT_relative"]).codes
                fig3d.add_trace(go.Scatter3d(
                    x=bat_df["lon_abonne"],
                    y=bat_df["lat_abonne"],
                    z=bat_df["etage"] * hauteur_etage,
                    mode="markers",
                    marker=dict(
                        size=5,
                        color=fat_codes,
                        colorscale="Turbo",
                        opacity=0.85,
                        colorbar=dict(title="FAT group", thickness=10)
                    ),
                    name="Abonnés",
                    text=bat_df["FAT_relative"],
                    hovertemplate=(
                        "<b>Abonné</b><br>"
                        "FAT: %{text}<br>"
                        "Étage: %{customdata[0]}<br>"
                        "Porte: %{customdata[1]}<extra></extra>"
                    ),
                    customdata=bat_df[["etage", "porte"]].values
                    if "porte" in bat_df.columns else bat_df[["etage"]].assign(p="?").values
                ))
            else:
                fig3d.add_trace(go.Scatter3d(
                    x=bat_df["lon_abonne"],
                    y=bat_df["lat_abonne"],
                    z=bat_df["etage"] * hauteur_etage,
                    mode="markers",
                    marker=dict(size=4, color=bat_df["etage"],
                                colorscale="Viridis", opacity=0.8),
                    name="Abonnés"
                ))

            # FATs (centroides réels si disponibles)
            fat_unique = bat_df.drop_duplicates(subset=["FAT_relative"]) \
                if "FAT_relative" in bat_df.columns else pd.DataFrame()

            if not fat_unique.empty and all(
                    c in fat_unique.columns for c in ["lon_fat", "lat_fat", "etage"]
            ):
                # Calcul dynamique des ports libres pour l'affichage 3D
                f_counts = bat_df.groupby("FAT_relative").size().reset_index(name="nb_abonnes_reel")
                fat_unique = fat_unique.merge(f_counts, on="FAT_relative", how="left")
                fat_unique["ports_libres"] = FAT_CAPACITY - fat_unique["nb_abonnes_reel"]

                fig3d.add_trace(go.Scatter3d(
                    x=fat_unique["lon_fat"],
                    y=fat_unique["lat_fat"],
                    z=fat_unique["etage"] * hauteur_etage,
                    mode="markers+text",
                    marker=dict(size=10, color="#ef4444", symbol="diamond",
                                line=dict(color="white", width=1)),
                    # Affichage ID court + ports libres en étiquette
                    text=fat_unique.apply(
                        lambda r: f"{r['FAT_relative'][-8:]}<br><span style='color:#38bdf8'>{int(r['ports_libres'])} libres</span>", 
                        axis=1
                    ),
                    textfont=dict(size=8, color="white"),
                    name="FATs",
                    customdata=fat_unique[["FAT_relative", "nb_abonnes_reel", "ports_libres"]].values,
                    hovertemplate=(
                        "<b>FAT ID</b>: %{customdata[0]}<br>"
                        "Abonnés raccordés: %{customdata[1]}<br>"
                        "Ports disponibles: <b>%{customdata[2]}</b>"
                        "<extra></extra>"
                    )
                ))

            # Lignes verticales par colonne (visualise la gaine technique)
            if all(c in bat_df.columns for c in ["lon_abonne", "lat_abonne", "etage"]):
                # Grouper par position GPS unique (= colonne verticale)
                bat_df["pos_key"] = (
                        bat_df["lon_abonne"].round(5).astype(str) + "_" +
                        bat_df["lat_abonne"].round(5).astype(str)
                )
                for pos_key, grp in bat_df.groupby("pos_key"):
                    if len(grp) > 1:
                        grp_sorted = grp.sort_values("etage")
                        fig3d.add_trace(go.Scatter3d(
                            x=grp_sorted["lon_abonne"],
                            y=grp_sorted["lat_abonne"],
                            z=grp_sorted["etage"] * hauteur_etage,
                            mode="lines",
                            line=dict(color="rgba(56,189,248,0.2)", width=1),
                            showlegend=False,
                            hoverinfo="skip"
                        ))

            fig3d.update_layout(
                title=f"Vue 3D — {selected_bat[:50]}",
                scene=dict(
                    xaxis_title="Longitude",
                    yaxis_title="Latitude",
                    zaxis_title="Hauteur (m)",
                    bgcolor="#060f1e",
                    xaxis=dict(gridcolor="#1e3a5f", color="#64748b"),
                    yaxis=dict(gridcolor="#1e3a5f", color="#64748b"),
                    zaxis=dict(gridcolor="#1e3a5f", color="#64748b"),
                    aspectmode="manual",
                    aspectratio=dict(x=1.5, y=1.5, z=0.8),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
                height=600,
                legend=dict(
                    bgcolor="rgba(10,22,40,0.8)",
                    bordercolor="#1e3a5f",
                    font=dict(size=10)
                ),
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig3d, use_container_width=True)

            # ── Vérification pureté étage ─────────────────────────────────
            if "FAT_relative" in bat_df.columns and "etage" in bat_df.columns:
                st.markdown('<div class="section-title">Pureté étage des FATs</div>',
                            unsafe_allow_html=True)

                fat_etage = bat_df.groupby("FAT_relative")["etage"].agg(
                    ["min", "max", "nunique", "count"]
                ).reset_index()
                fat_etage.columns = ["FAT", "étage_min", "étage_max",
                                     "n_étages_distincts", "n_abonnés"]
                fat_etage["pure"] = fat_etage["n_étages_distincts"] == 1
                fat_etage["status"] = fat_etage["pure"].map(
                    {True: " Mono-étage", False: " Multi-étages"}
                )

                pct_pure = fat_etage["pure"].mean() * 100
                pe1, pe2 = st.columns(2)

                with pe1:
                    color_purity = "#22c55e" if pct_pure >= 95 else "#f97316"
                    st.markdown(f"""
                    <div class="kpi-card" style="border-color:{color_purity}">
                        <div class="kpi-value" style="color:{color_purity}">
                            {pct_pure:.1f}%
                        </div>
                        <div class="kpi-label">FATs mono-étage (pureté)</div>
                    </div>""", unsafe_allow_html=True)

                    st.markdown(f"""
                    **Interprétation :**
                    -  > 95% → placement FAT conforme physiquement
                    -  < 80% → FATs qui mélangent les étages (anomalie)

                    Pour ce bâtiment : `{fat_etage['pure'].sum()}` / `{len(fat_etage)}` FATs pures
                    """)

                with pe2:
                    fig_purity = px.histogram(
                        fat_etage,
                        x="n_étages_distincts",
                        color="status",
                        title="Nombre d'étages distincts par FAT",
                        color_discrete_map={
                            "Mono-étage": "#22c55e",
                            "Multi-étages": "#f97316"
                        },
                        barmode="overlay"
                    )
                    fig_purity.update_layout(**PLOTLY_DARK, height=250)
                    st.plotly_chart(fig_purity, use_container_width=True)

                with st.expander("Détail par FAT — Raccordements Techniques"):
                    st.markdown("##### 1. Synthèse par FAT (Pureté & Capacité)")
                    st.dataframe(
                        fat_etage.sort_values("n_étages_distincts", ascending=False),
                        use_container_width=True, height=200
                    )
                    
                    st.markdown("---")
                    st.markdown("##### 2. Détails des raccordements par FAT")
                    st.markdown("Distance réelle (m) et Câble drop standard (15/20/50/80m)")
                    
                    # Groupement par FAT pour les tableaux séparés
                    for fat_id, group in bat_df.groupby("FAT_relative"):
                        st.info(f"**📍 FAT : `{fat_id}`**")
                        
                        # Détection flexible des colonnes de distance et câble
                        d_col = next((c for c in ["distance_real_m", "distance_FAT_m"] if c in group.columns), None)
                        c_col = next((c for c in ["cable_snap_m", "cable_snap", "cable_prefab_m", "distance_snap_m"] if c in group.columns), None)
                        
                        cols_to_show = ["code_client", "porte", "etage"]
                        if d_col: cols_to_show.append(d_col)
                        if c_col: cols_to_show.append(c_col)
                        
                        # Renommage pour un aspect professionnel
                        renames = {
                            "code_client": "ID Client",
                            "porte": "Porte",
                            "etage": "Étage",
                            "distance_real_m": "Distance (m)",
                            "distance_FAT_m": "Distance (m)",
                            "cable_snap_m": "Câble standard",
                            "cable_snap": "Câble standard",
                            "cable_prefab_m": "Câble standard",
                            "distance_snap_m": "Câble standard"
                        }
                        
                        df_display = group[cols_to_show].rename(columns=renames)
                        st.dataframe(df_display, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 4 — ANALYSE FAT
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-title">Distribution des clusters FAT</div>',
                unsafe_allow_html=True)

    if "FAT_relative" in fdf.columns:
        m1, m2 = st.columns(2)

        with m1:
            # Taille des FATs
            fat_size_df = fat_sizes.reset_index()
            fat_size_df.columns = ["FAT", "Abonnés"]

            fig_fat = px.histogram(
                fat_size_df, x="Abonnés", nbins=FAT_CAPACITY + 2,
                title="Taille des FATs — tous bâtiments",
                color_discrete_sequence=["#0ea5e9"],
                range_x=[0, FAT_CAPACITY + 2]
            )
            fig_fat.add_vline(x=FAT_CAPACITY, line_dash="dash", line_color="#ef4444",
                              annotation_text="Limite 8", annotation_font_size=11)
            fig_fat.update_layout(**PLOTLY_DARK, height=300)
            st.plotly_chart(fig_fat, use_container_width=True)

        with m2:
            # Heatmap étage × taille FAT
            if "etage" in fdf.columns:
                etage_fat = fdf.groupby(["etage", "FAT_relative"]).size().reset_index()
                etage_fat.columns = ["Étage", "FAT", "Abonnés"]
                etage_fat_pivot = etage_fat.groupby("Étage")["Abonnés"].agg(
                    ["mean", "max", "count"]
                ).reset_index()
                etage_fat_pivot.columns = ["Étage", "Moy/FAT", "Max/FAT", "N_FATs"]

                fig_heat = px.bar(
                    etage_fat_pivot,
                    x="Étage", y="Moy/FAT",
                    error_y=etage_fat_pivot["Max/FAT"] - etage_fat_pivot["Moy/FAT"],
                    title="Abonnés/FAT par étage (moyenne ± max)",
                    color="Moy/FAT",
                    color_continuous_scale=["#0f3460", "#22c55e", "#ef4444"],
                    range_color=[0, FAT_CAPACITY],
                )
                fig_heat.add_hline(y=FAT_CAPACITY, line_dash="dash",
                                   line_color="#ef4444", annotation_text="Limite 8")
                fig_heat.update_layout(**PLOTLY_DARK, height=300)
                st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown('<div class="section-title">Analyse câbles & linéaire fibre</div>',
                unsafe_allow_html=True)

    ca1, ca2, ca3 = st.columns(3)

    with ca1:
        if "cable_snap" in fdf.columns:
            cable_counts = fdf["cable_snap"].value_counts().sort_index()
            total_lineaire = (fdf["cable_snap"] * 1).sum()
            total_optimum = (fdf["cable_snap"].map({
                15: 15, 20: 20, 50: 50, 80: 80
            }) * 0.7).sum()  # hypothèse distances réelles = 70% du snap

            st.markdown(f"""
            **Linéaire câble total (snap) :**  
            `{total_lineaire / 1000:.2f} km`

            **Câbles courts (≤20m) :**  
            `{(fdf['cable_snap'] <= 20).sum():,}` / `{len(fdf):,}`
            = `{(fdf['cable_snap'] <= 20).mean() * 100:.1f}%`

            **Câbles longs (80m) :**  
            `{(fdf['cable_snap'] == 80).sum():,}`
            = `{(fdf['cable_snap'] == 80).mean() * 100:.1f}%`
            """)

    with ca2:
        if "distance_FAT_m" in fdf.columns and "cable_snap" in fdf.columns:
            waste_total = fdf["cable_waste_m"].sum() if "cable_waste_m" in fdf.columns else 0
            snap_total = fdf["cable_snap"].sum()
            eff_pct = (1 - waste_total / snap_total) * 100 if snap_total > 0 else 0

            fig_eff = go.Figure(go.Indicator(
                mode="gauge+number",
                value=eff_pct,
                title={"text": "Efficacité câble (%)", "font": {"size": 12}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#22c55e" if eff_pct >= 70 else "#f97316"},
                    "steps": [
                        {"range": [0, 50], "color": "#1e3a5f"},
                        {"range": [50, 80], "color": "#0f3460"},
                    ],
                }
            ))
            fig_eff.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#94a3b8"),
                height=230, margin=dict(l=20, r=20, t=40, b=10)
            )
            st.plotly_chart(fig_eff, use_container_width=True)

    with ca3:
        if "distance_olt_m" in fdf.columns:
            olt_df = fdf.groupby("id_batiment")["distance_olt_m"].first().dropna()
            n_hors = (olt_df > MAX_DIST_OLT_M).sum()

            fig_olt = px.histogram(
                olt_df.reset_index(), x="distance_olt_m",
                title="Distance OLT → abonné (m)",
                color_discrete_sequence=["#38bdf8"],
            )
            fig_olt.add_vline(x=MAX_DIST_OLT_M, line_dash="dash",
                              line_color="#ef4444",
                              annotation_text=f"Limite {MAX_DIST_OLT_M // 1000}km")
            fig_olt.update_layout(**PLOTLY_DARK, height=230)
            st.plotly_chart(fig_olt, use_container_width=True)
            if n_hors > 0:
                st.warning(f"⚠️ {n_hors} bâtiments hors limite OLT ({MAX_DIST_OLT_M // 1000}km)")

    # ── Variance intra/inter FAT ──────────────────────────────────────────
    st.markdown('<div class="section-title">Variance intra/inter cluster FAT</div>',
                unsafe_allow_html=True)

    st.markdown("""
    **Définition :**
    - **Variance intra-FAT** : dispersion GPS des abonnés *dans* un même cluster FAT.  
      Valeur faible = bonne cohésion spatiale.
    - **Variance inter-FAT** : distance entre les centroïdes FAT d'un même bâtiment.  
      Valeur élevée = bonne séparation entre clusters.
    - **Ratio inter/intra** : doit être > 10 pour un clustering de qualité.
    """)

    if all(c in fdf.columns for c in ["FAT_relative", "lat_abonne", "lon_abonne"]):
        # Calcul centroïdes FAT
        fat_centroids = fdf.groupby("FAT_relative").agg(
            clat=("lat_abonne", "mean"),
            clon=("lon_abonne", "mean"),
            n=("code_client", "count")
        ).reset_index()

        # Variance intra (std GPS dans chaque FAT, en mètres)
        intra_vars = fdf.groupby("FAT_relative").apply(
            lambda g: np.sqrt(
                np.var(g["lat_abonne"]) + np.var(g["lon_abonne"])
            ) * 111_000
        ).reset_index()
        intra_vars.columns = ["FAT_relative", "intra_std_m"]

        mean_intra = intra_vars["intra_std_m"].mean()

        # Variance inter (std des centroïdes, en mètres)
        mean_inter = np.sqrt(
            np.var(fat_centroids["clat"]) + np.var(fat_centroids["clon"])
        ) * 111_000

        ratio = mean_inter / mean_intra if mean_intra > 0 else 0

        vr1, vr2, vr3 = st.columns(3)
        color_ratio = "#22c55e" if ratio >= 10 else "#f97316" if ratio >= 5 else "#ef4444"

        vr1.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value" style="font-size:1.4rem">{mean_intra:.2f} m</div>
            <div class="kpi-label">Variance intra-FAT (std)</div>
        </div>""", unsafe_allow_html=True)

        vr2.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value" style="font-size:1.4rem">{mean_inter:.2f} m</div>
            <div class="kpi-label">Variance inter-FAT (std)</div>
        </div>""", unsafe_allow_html=True)

        vr3.markdown(f"""
        <div class="kpi-card" style="border-color:{color_ratio}">
            <div class="kpi-value" style="color:{color_ratio}">{ratio:.1f}</div>
            <div class="kpi-label">Ratio inter/intra</div>
            <div class="kpi-delta {'delta-ok' if ratio >= 10 else 'delta-warn'}">
                {'✅ Objectif atteint (>10)' if ratio >= 10 else '⚠️ Objectif : >10'}
            </div>
        </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 5 — CARTE GÉO
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div class="section-title">Carte géographique — Abonnés, FATs, FDTs</div>',
                unsafe_allow_html=True)

    map_mode = st.radio(
        "Couche à afficher",
        ["Abonnés (colorés par étage)", "Abonnés (colorés par FAT)", "FDTs"],
        horizontal=True
    )

    max_pts = st.slider("Nombre max de points", 500, 10_000, 3_000, step=500)

    map_df = fdf.dropna(subset=["lat_abonne", "lon_abonne"]).sample(
        min(max_pts, len(fdf))
    )

    if map_mode == "Abonnés (colorés par étage)" and "etage" in map_df.columns:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat_abonne", lon="lon_abonne",
            color="etage",
            color_continuous_scale="Turbo",
            opacity=0.7,
            zoom=14, height=650,
            mapbox_style="carto-darkmatter",
            hover_name="code_client",
            hover_data={c: True for c in
                        ["id_batiment", "etage", "FAT_relative", "usage"]
                        if c in map_df.columns},
            title="Abonnés colorés par étage"
        )

    elif map_mode == "Abonnés (colorés par FAT)" and "FAT_relative" in map_df.columns:
        fat_code_map = {f: i for i, f in enumerate(map_df["FAT_relative"].unique())}
        map_df["fat_code"] = map_df["FAT_relative"].map(fat_code_map)
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat_abonne", lon="lon_abonne",
            color="fat_code",
            color_continuous_scale="Turbo",
            opacity=0.7,
            zoom=14, height=650,
            mapbox_style="carto-darkmatter",
            hover_name="code_client",
            hover_data={c: True for c in
                        ["id_batiment", "etage", "FAT_relative"]
                        if c in map_df.columns},
            title="Abonnés colorés par FAT"
        )
        fig_map.update_coloraxes(showscale=False)

    elif map_mode == "FDTs" and all(
            c in fdf.columns for c in ["lat_fdt", "lon_fdt", "nom_FDT"]
    ):
        fdt_df = fdf.dropna(subset=["lat_fdt", "lon_fdt"]).drop_duplicates(
            subset=["nom_FDT"]
        )
        fdt_df["n_abonnes"] = fdf.groupby("nom_FDT").size().reindex(
            fdt_df["nom_FDT"]
        ).values

        fig_map = px.scatter_mapbox(
            fdt_df,
            lat="lat_fdt", lon="lon_fdt",
            size="n_abonnes",
            color="n_abonnes",
            color_continuous_scale=["#0f3460", "#f97316"],
            hover_name="nom_FDT",
            zoom=13, height=650,
            mapbox_style="carto-darkmatter",
            title="FDTs — taille proportionnelle au nombre d'abonnés"
        )
    else:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat_abonne", lon="lon_abonne",
            zoom=14, height=650,
            mapbox_style="carto-darkmatter",
        )

    fig_map.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono", color="#94a3b8"),
        margin=dict(l=0, r=0, t=40, b=0)
    )
    st.plotly_chart(fig_map, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 6 — EXPORT RAPPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.markdown('<div class="section-title">Tableaux pour Mémoire de Stage</div>',
                unsafe_allow_html=True)

    st.markdown("""
    Ces tableaux sont directement utilisables dans ton rapport de stage.
    Copie-les ou exporte-les en CSV.
    """)

    # ── Tableau 1 : Résumé dataset ────────────────────────────────────────
    st.markdown("#### 1. Statistiques descriptives du dataset")

    summary_data = {
        "Indicateur": [
            "Nombre total d'abonnés",
            "Nombre de bâtiments",
            "Nombre de FATs",
            "Nombre de FDTs",
            "Abonnés / bâtiment (moyenne)",
            "Abonnés / bâtiment (médiane)",
            "Abonnés / bâtiment (max)",
            "Conformité capacité FAT (≤8)",
            "FATs en surcapacité",
        ],
        "Valeur": [
            f"{n_abonnes:,}",
            f"{n_batiments:,}",
            f"{n_fats:,}",
            f"{n_fdts:,}",
            f"{bat_sizes.mean():.1f}",
            f"{bat_sizes.median():.1f}",
            f"{bat_sizes.max():,}",
            f"{pct_ok:.2f}%",
            f"{n_fats_over:,}",
        ],
        "Observation": [
            "Dataset synthétique généré via OSM + generer.py",
            "Bâtiments résidentiels/commerciaux Algérie",
            f"1 FAT = max {FAT_CAPACITY} abonnés (splitter N2)",
            "1 FDT = max 8 FATs (splitter N1)",
            "Indicateur de densité résidentielle",
            "Valeur centrale résistante aux outliers",
            "Bâtiment le plus dense du dataset",
            "Contrainte stricte AT — doit être 100%",
            "Anomalies de génération à corriger",
        ]
    }
    df_summary = pd.DataFrame(summary_data)
    st.dataframe(df_summary, use_container_width=True, hide_index=True, height=360)
    st.download_button(
        "📥 Télécharger (CSV)",
        df_summary.to_csv(index=False, encoding="utf-8-sig"),
        "stats_dataset.csv", "text/csv", key="dl_summary"
    )

    st.divider()

    # ── Tableau 2 : Distribution câbles ──────────────────────────────────
    if "cable_snap" in fdf.columns:
        st.markdown("#### 2. Distribution des longueurs de câble drop")

        cable_report = fdf["cable_snap"].value_counts().sort_index().reset_index()
        cable_report.columns = ["Longueur standard (m)", "Nombre"]
        cable_report["Pourcentage"] = (
                                              cable_report["Nombre"] / cable_report["Nombre"].sum() * 100
                                      ).round(2).astype(str) + "%"
        cable_report["Signification AT"] = cable_report["Longueur standard (m)"].map({
            15: "Câble court — distance < 15m",
            20: "Câble court — distance 15-20m",
            50: "Câble moyen — distance 20-50m",
            80: "Câble long — distance 50-80m (signal dégradé possible)"
        })
        st.dataframe(cable_report, use_container_width=True, hide_index=True)
        st.download_button(
            "📥 Télécharger (CSV)",
            cable_report.to_csv(index=False, encoding="utf-8-sig"),
            "cables_distribution.csv", "text/csv", key="dl_cable"
        )

    st.divider()

    # ── Tableau 3 : Stats par type bâtiment ──────────────────────────────
    if "type_batiment" in fdf.columns:
        st.markdown("#### 3. Statistiques par type de bâtiment")

        type_stats = fdf.groupby("type_batiment").agg(
            n_abonnes=("code_client", "count"),
            n_batiments=("id_batiment", "nunique"),
            n_fats=("FAT_relative", "nunique"),
            moy_etage=("etage", "mean"),
        ).reset_index()
        type_stats["moy_ab_bat"] = (
                type_stats["n_abonnes"] / type_stats["n_batiments"]
        ).round(1)
        type_stats["moy_etage"] = type_stats["moy_etage"].round(1)
        type_stats.columns = [
            "Type bâtiment", "Abonnés", "Bâtiments",
            "FATs", "Étage moyen", "Moy. ab/bât"
        ]
        st.dataframe(type_stats, use_container_width=True, hide_index=True)
        st.download_button(
            "📥 Télécharger (CSV)",
            type_stats.to_csv(index=False, encoding="utf-8-sig"),
            "stats_par_type.csv", "text/csv", key="dl_type"
        )

    st.divider()

    # ── Tableau 4 : Conformité par bâtiment ──────────────────────────────
    if "FAT_relative" in fdf.columns:
        st.markdown("#### 4. Rapport de conformité FAT par bâtiment")

        conf_bat = fdf.groupby(["id_batiment", "FAT_relative"]).size().reset_index()
        conf_bat.columns = ["Bâtiment", "FAT", "Abonnés"]
        conf_bat["Conforme"] = conf_bat["Abonnés"] <= FAT_CAPACITY
        conf_bat["Dépassement"] = (conf_bat["Abonnés"] - FAT_CAPACITY).clip(lower=0)

        conf_summary = conf_bat.groupby("Bâtiment").agg(
            n_fats=("FAT", "count"),
            fats_ok=("Conforme", "sum"),
            fats_nok=("Conforme", lambda x: (~x).sum()),
            max_depassement=("Dépassement", "max")
        ).reset_index()
        conf_summary["pct_ok"] = (
                                         conf_summary["fats_ok"] / conf_summary["n_fats"] * 100
                                 ).round(1).astype(str) + "%"
        conf_summary.columns = [
            "Bâtiment", "N FATs", "FATs OK", "FATs NOK",
            "Dépassement max", "% Conformité"
        ]

        # Ne garder que les bâtiments avec problème pour le rapport
        nok_only = conf_summary[conf_summary["FATs NOK"] > 0]
        if len(nok_only) > 0:
            st.warning(f"⚠️ {len(nok_only)} bâtiment(s) avec des FATs hors capacité")
            st.dataframe(nok_only, use_container_width=True, hide_index=True)
        else:
            st.success("✅ Tous les bâtiments sont conformes (capacité FAT ≤ 8)")
            st.dataframe(conf_summary.head(20), use_container_width=True, hide_index=True)

        st.download_button(
            "📥 Télécharger rapport complet (CSV)",
            conf_summary.to_csv(index=False, encoding="utf-8-sig"),
            "rapport_conformite.csv", "text/csv", key="dl_conf"
        )

    st.divider()
    st.markdown("""
    #### 📝 Note pour le mémoire

    Ces tableaux correspondent aux métriques de validation du système **FTTH Smart Planner**.
    Pour les intégrer dans ton rapport :
    1. Télécharge les CSV ci-dessus
    2. Ouvre dans Excel → Mise en forme tableau → Copie comme image dans Word
    3. Ou copie directement les valeurs dans le corps du texte

    **Références méthodologiques à citer :**
    - Données géospatiales : OpenStreetMap (OSM) via osmnx
    - Génération synthétique : `generer.py` — basé sur les normes AT Oran
    - Métriques clustering : ARI (Adjusted Rand Index), Silhouette Score
    - Norme câble drop : standards AT 15/20/50/80m
    """)