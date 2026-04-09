from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
import numpy as np
import os
import joblib

# ====================== IMPORT ======================
from id_generator import ATIDGenerator

# Inclusion propre du fichier modèle pour désérialisation
import test
import sys
if '__main__' in sys.modules:
    setattr(sys.modules['__main__'], 'FATSmartPlanner', test.FATSmartPlanner)

# ====================== FASTAPI APP ======================
app = FastAPI(
    title="FTTH Smart Planner API - Algérie Télécom",
    version="4.0",
    description="API Découpée en 5 étapes (OSM GeoJSON, FAT Placement, AT ID)",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ====================== MODÈLES PYDANTIC ======================
class ImportOSMRequest(BaseModel):
    ville: str
    quartier: str
    residence: str
    nombre_etages: int = 8
    logements_par_etage: int = 12
    commerce: bool = False

class FATPlacementRequest(BaseModel):
    subscribers: List[Dict[str, Any]]

class NamingFATRequest(BaseModel):
    fat_candidates: List[Dict[str, Any]]
    subscribers: List[Dict[str, Any]]


# ====================== 1. ROUTES DÉCOUVERTE OSM ======================

@app.get("/api/ville")
async def get_ville():
    try:
        gdf = ox.features_from_place(
            "Algeria",
            tags={"boundary": "administrative", "admin_level": "4"}
        )
        villes = sorted(gdf["name"].dropna().unique().tolist())
        if len(villes) < 5: raise ValueError("OSM data incomplete")
        return {"villes": villes}
    except:
        villes_fallback = [
            "Adrar", "Aïn Defla", "Aïn Témouchent", "Alger", "Annaba", "Batna", "Béchar", "Béjaïa", "Biskra", 
            "Blida", "Bordj Bou Arréridj", "Bouira", "Boumerdès", "Chlef", "Constantine", "Djelfa", "El Bayadh",
            "El Oued", "El Tarf", "Ghardaïa", "Guelma", "Illizi", "Jijel", "Khenchela", "Laghouat", "M'Sila", 
            "Mascara", "Médéa", "Mila", "Mostaganem", "Naâma", "Oran", "Ouargla", "Oum El Bouaghi", "Relizane", 
            "Saïda", "Sétif", "Sidi Bel Abbès", "Skikda", "Souk Ahras", "Tamanrasset", "Tébessa", "Tiaret", 
            "Tindouf", "Tipaza", "Tissemsilt", "Tizi Ouzou", "Tlemcen"
        ]
        return {"villes": villes_fallback}


@app.get("/api/quartier")
async def get_quartier(ville: str = Query(...)):
    place = f"{ville}, Algeria"
    try:
        gdf = ox.features_from_place(place, tags={"boundary": "administrative", "admin_level": "8"})
        if gdf.empty:
            gdf = ox.features_from_place(place, tags={"place": ["neighbourhood", "suburb", "quarter"]})
        quartiers = sorted(set(gdf["name"].dropna().tolist()))
        return {"quartiers": quartiers}
    except:
        return {"quartiers": []}


@app.get("/api/residence")
async def get_residence(ville: str = Query(...), quartier: str = Query(...)):
    place = f"{quartier}, {ville}, Algeria"
    try:
        gdf = ox.features_from_place(place, tags={"building": True})
        names = gdf.get("name", pd.Series([])).dropna().tolist()
        keywords = ["résidence", "cité", "lotissement", "hai", "rés", "bloc", "complexe", "appartements", "tower", "immeuble"]
        filtered = [n for n in names if any(k in str(n).lower() for k in keywords)]
        if not filtered: filtered = names[:80]
        return {"residences": sorted(set(filtered))[:100]}
    except:
        return {"residences": []}


# ====================== 2. IMPORT OSM ET FORMATION DES BÂTIMENTS ======================

def generate_random_point_in_polygon(poly, n: int):
    if n <= 0 or poly.is_empty: return []
    minx, miny, maxx, maxy = poly.bounds
    points = []
    attempts = 0
    while len(points) < n and attempts < n * 100:
        p = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        if poly.contains(p): points.append(p)
        attempts += 1
    return points

@app.post("/api/importOSM")
async def import_osm(req: ImportOSMRequest):
    place = f"{req.residence}, {req.quartier}, {req.ville}, Algeria"
    try:
        gdf = ox.features_from_place(place, tags={"building": True})
        if gdf.empty: raise ValueError("Aucun bâtiment")
    except:
        try:
            place_fallback = f"{req.quartier}, {req.ville}, Algeria"
            gdf = ox.features_from_place(place_fallback, tags={"building": True})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Erreur OSM: Impossible de récupérer la zone. ({str(e)})")

    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if gdf.empty: raise HTTPException(status_code=400, detail="Aucun polygone de bâtiment trouvé.")
    gdf = gdf.reset_index(drop=True)

    # ────────────────────────────────────────────────────────
    # NOM FORMATÉ DU BÂTIMENT (Extraction OSM dynamique)
    # ────────────────────────────────────────────────────────
    formatted_ids = []
    for i, row in gdf.iterrows():
        # Extraction du bloc depuis `ref` ou de `name`
        bloc_val = "A"
        if "ref" in row and pd.notna(row["ref"]): 
            bloc_val = str(row["ref"]).upper()
        elif "name" in row and pd.notna(row["name"]) and "bloc" in str(row["name"]).lower():
            import re
            m = re.search(r'bloc\s+([a-zA-Z0-9]+)', str(row["name"]).lower())
            if m: bloc_val = m.group(1).upper()
            else: bloc_val = chr(65 + (i % 26)) # A, B, C si illisible
        else:
            bloc_val = chr(65 + (i % 26))

        # Extraction du numéro de logement/bâtiment
        num_val = str(i + 1)
        if "addr:housenumber" in row and pd.notna(row["addr:housenumber"]):
            num_val = str(row["addr:housenumber"])

        # Format Final Demandé
        bat_name = f"{req.ville}-{req.quartier}-{req.residence}-BLOC {bloc_val}-numéro {num_val}"
        formatted_ids.append(bat_name)
    
    gdf["id_batiment"] = formatted_ids

    # Génération synthétique géométrique
    rows = []
    cc_counter = 1
    for _, bldg in gdf.iterrows():
        bat_id = bldg["id_batiment"]
        poly = bldg.geometry
        if not poly.is_valid or poly.area == 0: continue

        fdt_lat_base = round(poly.centroid.y, 6)
        fdt_lon_base = round(poly.centroid.x, 6)

        tot_log = req.nombre_etages * req.logements_par_etage
        pts_log = generate_random_point_in_polygon(poly, tot_log)

        for i in range(len(pts_log)):
            etg = (i // req.logements_par_etage) + 1
            rows.append({
                "code_client": f"AB{cc_counter:06d}",
                "id_batiment": bat_id,
                "id_zone": "Z310-001",
                "lat_abonne": round(pts_log[i].y, 6),
                "lon_abonne": round(pts_log[i].x, 6),
                "etage": etg,
                "porte": (etg * 100) + ((i % req.logements_par_etage) + 1),
                "usage": "logements",
                "nom_FDT": "F310-001-01",
                "lat_fdt": fdt_lat_base,
                "lon_fdt": fdt_lon_base,
            })
            cc_counter += 1

    return JSONResponse(content={
        "buildings_geojson": gdf[["id_batiment", "geometry"]].to_json(),
        "subscribers": rows,
        "count": len(gdf)
    })


# ====================== 3. RUN MODEL (Emplacement FAT) ======================

@app.post("/api/emplacementFATs")
async def get_emplacement_fats(req: FATPlacementRequest):
    df_sub = pd.DataFrame(req.subscribers)
    if df_sub.empty: raise HTTPException(status_code=400, detail="Liste des abonnés vide")
    
    model_path = os.path.join(os.path.dirname(__file__), "model", "fat_kmeans_model_2d_constrained.joblib")
    if not os.path.exists(model_path):
        raise HTTPException(status_code=500, detail="Modèle non trouvé. 'python test.py' doit être lancé une fois.")
        
    try:
        model = joblib.load(model_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur chargement modèle : {str(e)}")

    # Le fit() recalcule l'emplacement spécifiquement pour la résidence affichée au Frontend
    model.fit(df_sub)
    
    output_rows = []
    for res in model.results_:
        for fat in res.fat_candidates:
            output_rows.append({
                "id_batiment": res.id_batiment,
                "id_zone": res.id_zone,
                "fat_id": fat.fat_id,
                "cluster_label": fat.cluster_label,
                "centroid_lat": fat.centroid_lat,
                "centroid_lon": fat.centroid_lon,
                "n_subscribers": fat.n_subscribers,
                "usage": fat.usage,
                "fdt_assigned": fat.fdt_assigned,
                "capacity_ok": fat.capacity_ok,
                "cable_m_to_fdt_real": fat.cable_m_to_fdt_real,
                "radius_deg": fat.radius_deg,
                "subscriber_ids": fat.subscriber_ids
            })
            
    return {"fat_candidates": output_rows}


# ====================== 4. NOM FAT ======================

@app.post("/api/nomFAT")
async def generate_noms_fat(req: NamingFATRequest):
    df_cands = pd.DataFrame(req.fat_candidates)
    df_subs = pd.DataFrame(req.subscribers)

    if df_cands.empty:
        raise HTTPException(status_code=400, detail="Liste des candidats FAT vide")

    generator = ATIDGenerator(wilaya_code="310")
    df_subs_indexed = df_subs.set_index("code_client")
    
    ids_at = []
    fat_seq_counter = {}

    for _, row in df_cands.iterrows():
        bat_id = row["id_batiment"]
        zone_id = row.get("id_zone", "Z310-001") 
        fdt_id = row.get("fdt_assigned", "F310-001-01")
        sub_ids = row.get("subscriber_ids", [])
        
        # Séquence incrémentale par Bloc
        fat_seq_counter[bat_id] = fat_seq_counter.get(bat_id, 0) + 1
        seq = fat_seq_counter[bat_id]

        # Déduire les numéros de portes pour le nommage FAT final (ex: Porte(4,5,6...))
        portes = []
        etq_min = 1
        if isinstance(sub_ids, list) and len(sub_ids) > 0:
            # Récupérer les abonnés correspondants dans le dataset Frontend
            valid_subs = [s for s in sub_ids if s in df_subs_indexed.index]
            if valid_subs:
                sub_rows = df_subs_indexed.loc[valid_subs]
                portes = sorted(sub_rows["porte"].tolist())
                if "etage" in sub_rows.columns:
                    etq_min = int(sub_rows["etage"].min())

        olt_num = generator._extract_olt_num(zone_id)
        fdt_num = generator._extract_fdt_num(fdt_id)
        adresse = generator._extract_adresse(bat_id)
        
        # Format "AT" validé
        id_at = generator._format_id(
            wilaya=generator.wilaya,
            olt_num=olt_num,
            fdt_num=fdt_num,
            fat_seq=seq,
            adresse=adresse,
            portes=portes if portes else [0],
            etage_depart=etq_min,
            sequence=1
        )
        ids_at.append(id_at)

    df_cands["fat_id_AT"] = ids_at
    
    return {"fat_candidates_with_ids": df_cands.to_dict(orient="records")}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)