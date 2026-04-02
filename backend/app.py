from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
import numpy as np
import sys
import types
from io import BytesIO
from contextlib import asynccontextmanager

# ====================== CONFIG ======================
# On injecte un module config factice pour que model.py puisse faire
# `from config import settings` sans avoir le vrai config.py.
# Cela permet à l'API de fonctionner sans copier-coller la config partout.
config_module = types.ModuleType("config")

class DummySettings:
    FAT_CAPACITY = 8

config_module.settings = DummySettings()
sys.modules["config"] = config_module

# ====================== IMPORT MODÈLE ======================
from model import FATKMeansModel
from id_generator import ATIDGenerator

# ====================== ÉTAT GLOBAL ======================
# On stocke le modèle en mémoire globale.
# IMPORTANT : model est un objet STATELESS pour les hyperparamètres,
# mais STATEFUL après fit(). Ici on ne fait PAS de fit() global car
# on n'a pas de dataset permanent sur le serveur — le dataset est
# SYNTHÉTIQUE, généré à la volée depuis OSM.
model: FATKMeansModel = None


# ====================== LIFESPAN ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    POURQUOI ON N'UTILISE PAS joblib.load() :
    ──────────────────────────────────────────
    joblib/pickle enregistre les classes avec leur chemin MODULE au moment
    de la sauvegarde. Si tu lances `python model.py`, Python assigne
    __name__ = "__main__" → les classes sont sauvées sous "__main__.XXX".

    Quand FastAPI importe model.py, __name__ = "model".
    joblib.load() cherche "__main__.XXX" dans le contexte FastAPI où
    __main__ = app.py → la classe n'existe pas → KeyError/crash.

    SOLUTION : Instancier FATKMeansModel directement.
    Ce modèle n'a pas de "poids" à persister (pas de réseau de neurones).
    Il se re-fit sur les données synthétiques fraîches à chaque requête.
    """
    global model
    model = FATKMeansModel(random_state=2026, n_init=20)
    print("✅ FATKMeansModel initialisé (random_state=2026, n_init=20)")
    print("🚀 API prête")
    yield


# ====================== FASTAPI APP ======================
app = FastAPI(
    title="FTTH Smart Planner API - Algérie Télécom",
    version="3.0",
    description="Cascade simplifiée : Villes → Quartiers → Résidences (OSM)",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ====================== MODÈLE DE DONNÉES ======================
class PlanningInput(BaseModel):
    ville: str
    quartier: str
    residence: str
    nombre_etages: int = 8
    logements_par_etage: int = 12
    commerce: bool = False


# ====================== FONCTIONS UTILITAIRES ======================

def get_place_string(ville: str, quartier: str = None, residence: str = None) -> str:
    """Construit la chaîne de lieu OSM. Plus spécifique = meilleur résultat."""
    parts = [p for p in [residence, quartier, ville] if p]
    return ", ".join(parts) + ", Algeria"


def get_buildings_gdf(ville: str, quartier: str = None, residence: str = None) -> gpd.GeoDataFrame:
    """
    Récupère les bâtiments depuis OpenStreetMap pour un lieu donné.

    STRATÉGIE DE FALLBACK :
    1. Essayer avec résidence + quartier + ville (plus précis)
    2. Si vide, essayer sans la résidence
    3. Si toujours vide → lever HTTPException

    POURQUOI ox.features_from_place et pas ox.geometries_from_place ?
    features_from_place est l'API moderne (osmnx >= 1.0).
    geometries_from_place est dépréciée mais gardée en fallback.
    """
    place = get_place_string(ville, quartier, residence)
    try:
        gdf = ox.features_from_place(place, tags={"building": True})
        if gdf.empty:
            gdf = ox.geometries_from_place(place, tags={"building": True})
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
        if gdf.empty:
            raise ValueError("Aucun bâtiment trouvé")
        gdf = gdf.reset_index(drop=True)
        gdf["id_batiment"] = [f"{residence or quartier or ville}-B{i + 1}" for i in range(len(gdf))]
        return gdf[["id_batiment", "geometry"]]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OSM Error: {str(e)}")


def generate_random_point_in_polygon(poly, n: int):
    """
    Génère N points aléatoires à l'intérieur d'un polygone.

    ALGORITHME : Rejection sampling.
    1. Générer un point dans le bounding box
    2. Tester si le point est dans le polygone (contains)
    3. Répéter jusqu'à n points ou épuisement des tentatives

    LIMITE : 100 × n tentatives max → évite les boucles infinies
    pour les polygones très fins (bâtiments longs et étroits).
    """
    if n <= 0 or poly.is_empty:
        return []
    minx, miny, maxx, maxy = poly.bounds
    points = []
    attempts = 0
    while len(points) < n and attempts < n * 100:
        p = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        if poly.contains(p):
            points.append(p)
        attempts += 1
    return points


def generate_synthetic_dataset(
    buildings_gdf: gpd.GeoDataFrame,
    nombre_etages: int,
    logements_par_etage: int,
    has_commerce: bool
) -> pd.DataFrame:
    """
    Génère un dataset synthétique d'abonnés pour une zone OSM.

    POURQUOI SYNTHÉTIQUE ?
    ──────────────────────
    On n'a pas les vrais abonnés AT pour chaque ville/quartier/résidence.
    On les simule de manière réaliste :
    - Positions lat/lon : points aléatoires à l'intérieur du polygone bâtiment
    - Étages : de 1 à nombre_etages, logements_par_etage abonnés par étage
    - Commerces : 4 locaux au RDC (etage=0) si has_commerce=True

    LIMITATIONS CONNUES :
    - Le dataset synthétique n'a pas de colonne FAT_relative (pas de ground truth)
    - Le modèle peut donc calculer ARI=0 sur ces données (normal)
    - Les métriques retournées sont basées sur la qualité géométrique uniquement
    """
    rows = []
    code_client_counter = 1

    for _, building in buildings_gdf.iterrows():
        bat_id = building["id_batiment"]
        poly = building.geometry
        if not poly.is_valid or poly.area == 0:
            continue

        centroid = poly.centroid
        fdt_lat = round(centroid.y, 6)
        fdt_lon = round(centroid.x, 6)
        nom_fdt = "F310-001-01"
        id_zone = "Z310-001"

        total_logements = nombre_etages * logements_par_etage
        points_log = generate_random_point_in_polygon(poly, total_logements)

        for i in range(min(total_logements, len(points_log))):
            etage = (i // logements_par_etage) + 1
            porte = (etage * 100) + ((i % logements_par_etage) + 1)
            rows.append({
                "code_client":  f"AB{code_client_counter:06d}",
                "id_batiment":  bat_id,
                "id_zone":      id_zone,
                "lat_abonne":   round(points_log[i].y, 6),
                "lon_abonne":   round(points_log[i].x, 6),
                "etage":        etage,
                "porte":        porte,
                "usage":        "logements",
                "nom_FDT":      nom_fdt,
                "lat_fdt":      fdt_lat,
                "lon_fdt":      fdt_lon,
                "FAT_relative": None,  # Pas de ground truth pour les données synthétiques
            })
            code_client_counter += 1

        if has_commerce:
            points_com = generate_random_point_in_polygon(poly, 4)
            for i in range(min(4, len(points_com))):
                rows.append({
                    "code_client":  f"AB{code_client_counter:06d}",
                    "id_batiment":  bat_id,
                    "id_zone":      id_zone,
                    "lat_abonne":   round(points_com[i].y, 6),
                    "lon_abonne":   round(points_com[i].x, 6),
                    "etage":        0,
                    "porte":        100 + i,
                    "usage":        "commerces",
                    "nom_FDT":      nom_fdt,
                    "lat_fdt":      fdt_lat,
                    "lon_fdt":      fdt_lon,
                    "FAT_relative": None,
                })
                code_client_counter += 1

    return pd.DataFrame(rows)


# ====================== HELPER PARTAGÉ ======================

def _run_model_on_synthetic(data: PlanningInput):
    """
    Pipeline complet : OSM → dataset synthétique → modèle → IDs AT.

    ÉTAPES :
    1. Récupérer les bâtiments OSM pour la zone
    2. Générer le dataset synthétique d'abonnés
    3. Fitter un modèle KMeans local sur ces données
    4. Exporter les FAT candidates (to_dataframe)
    5. Générer les IDs AT normalisés (ATIDGenerator)
    6. Générer les placements optimaux (get_optimal_fat_placements)

    POURQUOI local_model et pas le model global ?
    ──────────────────────────────────────────────
    Le model global est partagé entre les requêtes (singleton).
    Si on fit() le global, une requête efface les résultats d'une autre.
    On crée donc un modèle LOCAL par requête → thread-safe.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Modèle non initialisé.")

    buildings_gdf = get_buildings_gdf(data.ville, data.quartier, data.residence)
    synthetic_df = generate_synthetic_dataset(
        buildings_gdf, data.nombre_etages, data.logements_par_etage, data.commerce
    )

    if len(synthetic_df) == 0:
        raise HTTPException(status_code=400, detail="Aucun abonné généré")

    # Modèle local par requête (thread-safe)
    local_model = FATKMeansModel(
        random_state=model.random_state,
        n_init=model.n_init
    )
    local_model.fit(synthetic_df)

    # ── CORRECTION BUG : ATIDGenerator utilisé correctement ──────────────────
    # Avant : df_with_ids = df_candidates (ATIDGenerator ignoré dans app.py !)
    # Maintenant : on génère les vrais IDs AT normalisés
    df_candidates = local_model.to_dataframe()
    generator = ATIDGenerator(wilaya_code="310")
    df_with_ids = generator.generate_for_candidates(df_candidates, synthetic_df)

    df_optimal = local_model.get_optimal_fat_placements(synthetic_df)

    return synthetic_df, df_with_ids, df_optimal, local_model


# ====================== ENDPOINTS ======================

@app.get("/villes")
async def get_villes():
    """
    Retourne la liste des villes d'Algérie depuis OSM.

    STRATÉGIE OSM :
    ──────────────
    On cherche les entités administratives de niveau 4 (wilayas/provinces).
    admin_level=4 correspond aux wilayas algériennes dans OSM.

    FALLBACK : Si OSM échoue (timeout, pas de connexion), on retourne
    une liste hardcodée des principales villes. Jamais de crash API.

    PROBLÈME ORIGINAL : La liste hardcodée avait ≤ 10 villes.
    CORRECTION : On retourne TOUTES les villes OSM, pas de limite artificielle.
    """
    try:
        gdf = ox.features_from_place(
            "Algeria",
            tags={"boundary": "administrative", "admin_level": "4"}
        )
        # Extraire les noms, supprimer les NaN, dédupliquer, trier
        villes = sorted(gdf["name"].dropna().unique().tolist())

        # Si OSM retourne trop peu de résultats (données incomplètes)
        # → compléter avec une liste de base mais garder TOUT ce qu'OSM a donné
        if len(villes) < 5:
            villes_base = [
                "Adrar", "Aïn Defla", "Aïn Témouchent", "Alger", "Annaba",
                "Batna", "Béchar", "Béjaïa", "Biskra", "Blida", "Bordj Bou Arréridj",
                "Bouira", "Boumerdès", "Chlef", "Constantine", "Djelfa", "El Bayadh",
                "El Oued", "El Tarf", "Ghardaïa", "Guelma", "Illizi", "Jijel",
                "Khenchela", "Laghouat", "M'Sila", "Mascara", "Médéa", "Mila",
                "Mostaganem", "Naâma", "Oran", "Ouargla", "Oum El Bouaghi",
                "Relizane", "Saïda", "Sétif", "Sidi Bel Abbès", "Skikda",
                "Souk Ahras", "Tamanrasset", "Tébessa", "Tiaret", "Tindouf",
                "Tipaza", "Tissemsilt", "Tizi Ouzou", "Tlemcen"
            ]
            # Fusionner sans doublons
            villes = sorted(list(set(villes + villes_base)))

        return {"villes": villes, "source": "osm", "count": len(villes)}

    except Exception as e:
        # Fallback : 48 wilayas algériennes hardcodées
        villes_fallback = [
            "Adrar", "Aïn Defla", "Aïn Témouchent", "Alger", "Annaba",
            "Batna", "Béchar", "Béjaïa", "Biskra", "Blida", "Bordj Bou Arréridj",
            "Bouira", "Boumerdès", "Chlef", "Constantine", "Djelfa", "El Bayadh",
            "El Oued", "El Tarf", "Ghardaïa", "Guelma", "Illizi", "Jijel",
            "Khenchela", "Laghouat", "M'Sila", "Mascara", "Médéa", "Mila",
            "Mostaganem", "Naâma", "Oran", "Ouargla", "Oum El Bouaghi",
            "Relizane", "Saïda", "Sétif", "Sidi Bel Abbès", "Skikda",
            "Souk Ahras", "Tamanrasset", "Tébessa", "Tiaret", "Tindouf",
            "Tipaza", "Tissemsilt", "Tizi Ouzou", "Tlemcen"
        ]
        return {
            "villes": villes_fallback,
            "source": "fallback",
            "count": len(villes_fallback),
            "warning": f"OSM indisponible : {str(e)}"
        }


@app.get("/quartiers")
async def get_quartiers(ville: str = Query(...)):
    """
    Retourne les quartiers/communes d'une ville depuis OSM.

    STRATÉGIE MULTI-NIVEAUX :
    admin_level=8 = communes (plus précis)
    Si vide → chercher place=neighbourhood/suburb/quarter
    Si toujours vide → retourner liste vide (pas de crash)
    """
    place = f"{ville}, Algeria"
    try:
        gdf = ox.features_from_place(
            place,
            tags={"boundary": "administrative", "admin_level": "8"}
        )
        if gdf.empty:
            gdf = ox.features_from_place(
                place,
                tags={"place": ["neighbourhood", "suburb", "quarter"]}
            )
        quartiers = sorted(set(gdf["name"].dropna().tolist()))
        return {"quartiers": quartiers, "count": len(quartiers)}
    except Exception as e:
        return {"quartiers": [], "count": 0, "error": str(e)}


@app.get("/residences")
async def get_residences(ville: str = Query(...), quartier: str = Query(...)):
    """
    Retourne les résidences/cités nommées dans un quartier.

    FILTRE PAR MOTS-CLÉS :
    On filtre les bâtiments qui ont un nom contenant des mots typiques
    des résidences algériennes : résidence, cité, lotissement, haï, bloc...
    Si le filtre ne trouve rien → on retourne les 80 premiers noms bruts.
    """
    place = f"{quartier}, {ville}, Algeria"
    try:
        gdf = ox.features_from_place(place, tags={"building": True})
        names = gdf.get("name", pd.Series([])).dropna().tolist()
        keywords = ["résidence", "cité", "lotissement", "hai", "rés", "bloc",
                    "complexe", "appartements", "tower", "immeuble"]
        filtered = [n for n in names if any(k in str(n).lower() for k in keywords)]
        if not filtered:
            filtered = names[:80]
        return {"residences": sorted(set(filtered))[:100], "count": len(filtered)}
    except Exception as e:
        return {"residences": [], "count": 0, "error": str(e)}


@app.get("/buildings")
async def get_buildings(
    ville: str = Query(...),
    quartier: str = Query(None),
    residence: str = Query(None)
):
    """Retourne les bâtiments OSM d'une zone en GeoJSON."""
    gdf = get_buildings_gdf(ville, quartier, residence)
    return JSONResponse(content={
        "geojson": gdf.to_json(),
        "buildings": gdf[["id_batiment"]].to_dict("records"),
        "count": len(gdf)
    })


@app.post("/run-planning")
async def run_planning(data: PlanningInput):
    """
    Endpoint principal : lance la planification complète pour une zone.

    PIPELINE :
    1. OSM → bâtiments
    2. Génération dataset synthétique
    3. KMeans par bâtiment → FAT candidates
    4. ATIDGenerator → IDs AT normalisés
    5. Calcul métriques globales
    6. Retour JSON structuré

    COLONNES RETOURNÉES dans fat_candidates_with_ids :
    id_batiment, id_zone, fat_id_AT (ID normalisé AT), fat_id (ID interne),
    usage, assigned_floor, n_subscribers, fdt_assigned

    NOTE : Les métriques Silhouette et ARI sont calculées sur les données
    synthétiques. Sans ground truth (FAT_relative = None), l'ARI retourne 0.
    C'est NORMAL pour des données synthétiques.
    """
    synthetic_df, df_with_ids, df_optimal, local_model = _run_model_on_synthetic(data)

    g = local_model.global_metrics_
    linear_fiber_m = float(df_optimal["linéaire_fibre_m"].sum()) if not df_optimal.empty else 0.0
    port_usage_pct = round(df_optimal["taux_occupation"].mean(), 1) if not df_optimal.empty else 0.0

    # Colonnes à exposer dans la réponse (vérification défensive)
    cols_wanted = [
        "id_batiment", "id_zone", "fat_id_AT", "fat_id",
        "usage", "assigned_floor", "n_subscribers", "fdt_assigned"
    ]
    cols_available = [c for c in cols_wanted if c in df_with_ids.columns]

    return {
        "estimated_abonnes": len(synthetic_df),
        "proposed_fats":     len(df_with_ids),
        "linear_fiber_m":    round(linear_fiber_m, 1),
        "port_usage_pct":    port_usage_pct,
        "metrics": {
            "silhouette":           round(g.mean_silhouette, 4),
            "ari":                  round(g.mean_ari, 4),
            "capacity_compliance":  round(g.mean_capacity_compliance, 2),
            "fat_count_match_rate": round(g.fat_count_match_rate, 2),
            "fdt_match_pct":        round(g.mean_fdt_match, 2),
        },
        "fat_candidates_with_ids": df_with_ids[cols_available].to_dict(orient="records"),
        "message": f"Sectorisation terminée pour {data.residence} ({data.quartier}, {data.ville})"
    }


@app.post("/export-project")
async def export_project(
    data: PlanningInput,
    format: str = Query("csv", enum=["csv", "excel"])
):
    """
    Exporte les résultats en CSV ou Excel.

    STRUCTURE EXPORT :
    - CSV : toutes les colonnes, encodage UTF-8-BOM (compatible Excel Windows)
    - Excel : 2 sheets → "FATs" (résultats principaux) + "Métriques" (df_optimal)

    MERGE : On joint les métriques de distance/câble depuis df_optimal
    sur les FAT candidates, en groupant par id_batiment (moyenne).
    """
    synthetic_df, df_with_ids, df_optimal, _ = _run_model_on_synthetic(data)

    merge_cols = ["id_batiment", "distance_m", "cable_snappe", "linéaire_fibre_m", "taux_occupation"]
    available_merge_cols = [c for c in merge_cols if c in df_optimal.columns]

    df_export = df_with_ids.copy()
    if len(available_merge_cols) > 1:
        df_opt_agg = (
            df_optimal[available_merge_cols]
            .groupby("id_batiment")
            .mean(numeric_only=True)
            .reset_index()
        )
        df_export = df_export.merge(df_opt_agg, on="id_batiment", how="left")

    filename_base = f"FTTH_{data.residence.replace(' ', '_')}"

    if format == "csv":
        buffer = BytesIO()
        df_export.to_csv(buffer, index=False, encoding="utf-8-sig")
        buffer.seek(0)
        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename_base}.csv"}
        )
    else:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="FATs")
            if not df_optimal.empty:
                df_optimal.to_excel(writer, index=False, sheet_name="Métriques")
        buffer.seek(0)
        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename_base}.xlsx"}
        )


# ====================== LANCEMENT ======================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)