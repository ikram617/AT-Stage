from contextlib import asynccontextmanager
import joblib
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
import types
import sys
import asyncio
import json
import re
import httpx
from pathlib import Path
from dataclasses import dataclass

from id_generator import ATIDGenerator

# ====================== CONFIG ======================
config_module = types.ModuleType("config")
FAT_CAPACITY = 8

class _Settings:
    FAT_CAPACITY = 8
    TORTUOSITY_TRUNK = 1.3
    AT_DROP_CABLE_STANDARDS_M = [10, 15, 20, 30, 50, 100]


config_module.settings = _Settings()
sys.modules["config"] = config_module


# ====================== INJECTION DES CLASSES ======================
@dataclass
class FATCandidate:
    fat_id: str
    cluster_label: int
    centroid_lat: float
    centroid_lon: float
    subscriber_ids: list
    n_subscribers: int
    usage: str
    fdt_assigned: str
    capacity_ok: bool = True
    cable_m_to_fdt_real: float = 0.0
    cable_snap: int = 0
    radius_deg: float = 0.0
    max_dist_to_sub_m: float = 0.0


@dataclass
class BuildingKMeansResult:
    id_batiment: str
    id_zone: str
    n_subscribers_total: int
    fat_candidates: list
    n_fats_proposed: int
    n_fats_ground_truth: int
    ari_score: float = 0.0
    silhouette_score: float = 0.0
    r2_score: float = 0.0
    capacity_compliance_pct: float = 100.0
    mae_m: float = 0.0
    mse_m2: float = 0.0
    rmse_m: float = 0.0
    max_distance_m: float = 0.0


sys.modules["__main__"].FATCandidate = FATCandidate
sys.modules["__main__"].BuildingKMeansResult = BuildingKMeansResult
sys.modules["__main__"].FATSmartPlanner = type("FATSmartPlanner", (), {})

# ====================== JOBLIB MODEL ======================
MODEL_PATH = Path("model/fat_pipeline_2d_annaba.joblib")
fat_model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global fat_model
    print("🔄 Démarrage - Chargement du modèle joblib...")
    if not MODEL_PATH.exists():
        print(f"⚠️ Modèle non trouvé : {MODEL_PATH}. Fonctionnement en mode Fallback K-Means.")
    else:
        try:
            fat_model = joblib.load(MODEL_PATH)
            print("✅ Modèle FAT 2D chargé avec succès !")
        except Exception as e:
            print(f"❌ Erreur de chargement modèle : {e}")
    yield
    print("🛑 Arrêt de l'application")


app = FastAPI(
    title="FTTH Smart Planner API - Algérie Télécom",
    version="5.0",
    description="Wilaya → Commune → Quartier → Résidence",
    lifespan=lifespan
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_CACHE_DIR = Path("osm_cache")
_CACHE_DIR.mkdir(exist_ok=True)
ox.settings.use_cache = True
ox.settings.cache_folder = str(_CACHE_DIR / "osmnx_http")

JSON_CACHE_DIR = Path("osm_json_cache")
JSON_CACHE_DIR.mkdir(exist_ok=True)

_WILAYAS_58 = {
    1: "Adrar", 2: "Chlef", 3: "Laghouat", 4: "Oum El Bouaghi", 5: "Batna", 6: "Béjaïa",
    7: "Biskra", 8: "Béchar", 9: "Blida", 10: "Bouira", 11: "Tamanrasset", 12: "Tébessa",
    13: "Tlemcen", 14: "Tiaret", 15: "Tizi Ouzou", 16: "Alger", 17: "Djelfa", 18: "Jijel",
    19: "Sétif", 20: "Saïda", 21: "Skikda", 22: "Sidi Bel Abbès", 23: "Annaba", 24: "Guelma",
    25: "Constantine", 26: "Médéa", 27: "Mostaganem", 28: "M'Sila", 29: "Mascara", 30: "Ouargla",
    31: "Oran", 32: "El Bayadh", 33: "Illizi", 34: "Bordj Bou Arréridj", 35: "Boumèrdès",
    36: "El Tarf", 37: "Tindouf", 38: "Tissemsilt", 39: "El Oued", 40: "Khenchela",
    41: "Souk Ahras", 42: "Tipaza", 43: "Mila", 44: "Aïn Defla", 45: "Naâma", 46: "Aïn Témouchent",
    47: "Ghardaïa", 48: "Relizane", 49: "Timimoun", 50: "Bordj Badji Mokhtar", 51: "Ouled Djellal",
    52: "Béni Abbès", 53: "In Salah", 54: "In Guezzam", 55: "Touggourt", 56: "Djanet",
    57: "El M'Ghair", 58: "El Meniaa"
}


def _load_wilaya_cache(ville_label: str) -> dict | None:
    parts = ville_label.split(" - ", 1)
    if len(parts) == 2:
        code_str, nom = parts[0].strip(), parts[1].strip()
    else:
        code_str, nom = None, ville_label.strip()
        for c, n in _WILAYAS_58.items():
            if n.lower() == nom.lower():
                code_str = f"{c:02d}"
                nom = n
                break
        if not code_str: return None
    safe_nom = nom.replace(" ", "_").replace("'", "").replace("\u2019", "")
    cache_file = JSON_CACHE_DIR / f"{code_str}-{safe_nom}.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def load_from_json_cache(key: str):
    path = JSON_CACHE_DIR / (key.replace("::", "__").replace(":", "_").replace(" ", "_") + ".json")
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


# ====================== OVERPASS (RÉSIDENTIEL STRICT) ======================
OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://api.openstreetmap.fr/oapi/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter"
]


async def _overpass_request(query: str, timeout: int = 50, retries: int = 3) -> dict:
    for attempt in range(retries):
        if attempt > 0:
            wait_time = 2 * attempt
            print(f"⏳ Attente de {wait_time}s avant nouvelle tentative...")
            await asyncio.sleep(wait_time)
            
        print(f"📡 Tentative Overpass {attempt + 1}/{retries}...")
        
        async def _try(url: str):
            try:
                async with httpx.AsyncClient(timeout=float(timeout)) as c:
                    resp = await c.post(url, data={"data": query.strip()})
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("elements"):
                            print(f"✅ {len(data['elements'])} éléments trouvés sur {url}")
                            return data
                    elif resp.status_code == 429:
                        print(f"⚠️ 429 Too Many Requests sur {url}")
            except Exception as e:
                # print(f"❌ Erreur sur {url}: {e}")
                pass
            return None

        tasks = [asyncio.create_task(_try(url)) for url in OVERPASS_SERVERS]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                for t in tasks: t.cancel()
                return result
        
        for t in tasks: t.cancel()
        if attempt < retries - 1:
            await asyncio.sleep(2)
            
    return {"elements": []}


def _format_building_name(tags: dict, index: int) -> dict:
    name = tags.get("name", "").strip()
    ref = tags.get("ref", "").strip()
    addr_nb = tags.get("addr:housenumber", "").strip()
    addr_st = tags.get("addr:street", "").strip()
    levels = tags.get("building:levels", "").strip()
    units = tags.get("building:units", "").strip() or tags.get("residential:units", "").strip()

    if name:
        display_name = name
    elif ref:
        display_name = f"Bât. {ref}"
    elif addr_nb and addr_st:
        display_name = f"N°{addr_nb} {addr_st}"
    elif addr_nb:
        display_name = f"N°{addr_nb}"
    else:
        display_name = f"Bâtiment {index}"

    return {
        "display_name": display_name,
        "levels": int(levels) if levels.isdigit() else None,
        "units": int(units) if units.isdigit() else None,
    }


async def fetch_residences_in_commune(commune: str, wilaya_name: str, lat_fallback: float = None, lon_fallback: float = None) -> List[Dict[str, Any]]:
    wilaya_pure = wilaya_name.split(" - ")[1] if " - " in wilaya_name else wilaya_name
    elements = []

    # STRATÉGIE 1 (RAPIDE) : Bounding Box si coordonnées dispo
    if lat_fallback and lon_fallback:
        print(f"⚡ Stratégie 1 (Rapide): Bounding Box autour de {lat_fallback}, {lon_fallback}")
        margin = 0.025 # ~2.5km
        s, w, n, e = lat_fallback - margin, lon_fallback - margin, lat_fallback + margin, lon_fallback + margin
        query_bbox = f"""
        [out:json][timeout:25];
        (
          nwr["building"]["name"]({s},{w},{n},{e});
          nwr["landuse"="residential"]["name"]({s},{w},{n},{e});
          nwr["place"~"neighbourhood|quarter|suburb"]["name"]({s},{w},{n},{e});
          nwr["addr:housename"]({s},{w},{n},{e});
        );
        out tags center;
        """
        data = await _overpass_request(query_bbox, timeout=25, retries=2)
        elements = data.get("elements", [])

    # STRATÉGIE 2 (LENTE / COMPLÈTE) : Recherche par Zone Administrative
    if not elements:
        print(f"🔍 Stratégie 2 (Complète): Recherche par zone administrative pour {commune}")
        query_area = f"""
        [out:json][timeout:50];
        area[name~"^{wilaya_pure}$",i][admin_level=4]->.w;
        (
          area[name~"^{commune}$",i][admin_level=8](area.w);
          area[name~"{commune}",i][admin_level=8](area.w);
          area[name~"^{commune}$",i](area.w);
        )->.searchArea;
        (
          nwr["building"]["name"](area.searchArea);
          nwr["landuse"="residential"]["name"](area.searchArea);
          nwr["place"~"neighbourhood|quarter|suburb"]["name"](area.searchArea);
          nwr["addr:housename"](area.searchArea);
        );
        out tags center;
        """
        data = await _overpass_request(query_area, timeout=50, retries=2)
        elements = data.get("elements", [])
    
    residences = []
    seen_names = set()

    for el in elements:
        tags = el.get("tags", {})
        # On récupère le nom le plus pertinent
        name = tags.get("name") or tags.get("addr:housename") or tags.get("official_name")

        if not name or len(name.strip()) < 2: continue
        
        # Filtre anti-digits simples (ex: "123")
        if name.strip().isdigit(): continue

        if name.lower() in seen_names: continue

        center = el.get("center") or {}
        # Fallback pour les nodes qui n'ont pas de champ "center" mais directement lat/lon
        lat = center.get("lat") or el.get("lat")
        lon = center.get("lon") or el.get("lon")
        
        if not lat: continue

        # Détection du type pour l'icône dans le frontend
        res_type = "Cité/Résidence"
        if tags.get("building"): res_type = "Bâtiment"
        if tags.get("place"): res_type = "Quartier/Zone"

        residences.append({
            "name": name,
            "osm_id": str(el.get("id", "")),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "levels": tags.get("building:levels") or tags.get("levels"),
            "units": tags.get("building:units") or tags.get("residential:units"),
            "type": res_type
        })
        seen_names.add(name.lower())

    print(f"✅ {len(residences)} résidences trouvées dans {commune}")
    # Tri par nom
    return sorted(residences, key=lambda x: x["name"])[:2000]

# ====================== PYDANTIC ======================
class ImportOSMRequest(BaseModel):
    ville: str
    commune: str
    residence: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    nombre_etages: int = 5
    logements_par_etage: int = 4
    commerce: bool = False


class FATPlacementRequest(BaseModel):
    subscribers: List[Dict[str, Any]]


class NamingFATRequest(BaseModel):
    fat_candidates: List[Dict[str, Any]]
    subscribers: List[Dict[str, Any]]


# ====================== ROUTES ======================
@app.get("/api/ville")
async def get_ville():
    print("GET /api/ville")
    cached = load_from_json_cache("villes")
    if cached:
        print(f"✅ Villes trouvées (cache): {len(cached)}")
        return {"villes": cached, "source": "json_cache", "count": len(cached)}
    print("⚠️ Aucune ville trouvée")
    return {"villes": [], "source": "empty", "message": "Lancez VilleData.py d'abord"}


@app.get("/api/commune")
async def get_commune(ville: str = Query(...)):
    print(f"GET /api/commune (ville={ville})")
    wilaya_data = _load_wilaya_cache(ville)
    if wilaya_data and wilaya_data.get("communes"):
        communes = [c["nom"] for c in wilaya_data["communes"]]
        print(f"✅ Communes trouvées (unified): {len(communes)}")
        return {"communes": communes, "source": "unified_cache", "count": len(communes)}
    key = f"c::{ville}"
    cached = load_from_json_cache(key)
    if cached:
        print(f"✅ Communes trouvées (legacy cache): {len(cached)}")
        return {"communes": cached, "source": "json_cache_legacy", "count": len(cached)}
    print("⚠️ Aucune commune trouvée")
    return {"communes": [], "source": "no_cache", "message": "Cache manquant."}


@app.get("/api/residence")
async def get_residence(ville: str = Query(...), commune: str = Query(...), search: str = Query(None)):
    print(f"GET /api/residence (ville={ville}, commune={commune}, search={search})")
    ville_pure = ville.split(" - ")[1] if " - " in ville else ville
    
    # Récupération des coordonnées de la commune depuis le cache pour le fallback
    lat_f, lon_f = None, None
    wilaya_data = _load_wilaya_cache(ville)
    if wilaya_data and wilaya_data.get("communes"):
        for c in wilaya_data["communes"]:
            if c["nom"] == commune:
                lat_f = c.get("lat")
                lon_f = c.get("lon")
                break
    
    residences = await fetch_residences_in_commune(commune, ville_pure, lat_f, lon_f)
    if search and search.strip():
        q = search.strip().lower()
        residences = [r for r in residences if q in r["name"].lower()]
    print(f"✅ Résidences trouvées: {len(residences)}")
    return {
        "residences": residences,
        "source": "overpass_live",
        "count": len(residences),
        "commune": commune,
    }


# ====================== IMPORT OSM ======================
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
    print(f"🔍 Import OSM contexte pour : {req.residence}")
    try:
        if req.lat and req.lon:
            # Récupère le contexte autour du bâtiment (150m)
            gdf = ox.features_from_point((req.lat, req.lon), dist=150, tags={"building": True})
        else:
            ville_pure = req.ville.split(" - ")[1] if " - " in req.ville else req.ville
            place = f"{req.residence}, {req.commune}, {ville_pure}, Algeria"
            gdf = ox.features_from_place(place, tags={"building": True})

        if gdf.empty:
            raise HTTPException(status_code=400, detail="Aucun bâtiment trouvé pour cette localisation.")

        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy().reset_index(drop=True)

        target_idx = 0
        if req.lat and req.lon:
            target_pt = Point(req.lon, req.lat)
            distances = gdf.geometry.distance(target_pt)
            target_idx = distances.idxmin()

        formatted_ids = []
        is_target_list = []
        for i, row in gdf.iterrows():
            is_target = (i == target_idx)
            is_target_list.append(is_target)
            bat_name = f"CIBLE-{req.residence[:15].upper()}" if is_target else f"VOISIN-B{i + 1}"
            formatted_ids.append(bat_name)

        gdf["id_batiment"] = formatted_ids
        gdf["is_target"] = is_target_list

        target_bldg = gdf.iloc[target_idx]
        osm_levels = target_bldg.get("building:levels")
        osm_units = target_bldg.get("building:units")

        etages = int(osm_levels) if pd.notna(osm_levels) else req.nombre_etages
        if pd.notna(osm_units) and int(osm_units) > 0:
            logements = max(1, int(osm_units) // max(1, etages))
        else:
            logements = req.logements_par_etage

        if etages <= 0: etages = 1
        if logements <= 0: logements = 1

        rows = []
        tot_log = etages * logements
        pts_log = generate_random_point_in_polygon(target_bldg.geometry, tot_log)

        cc_counter = 1
        for i in range(len(pts_log)):
            etg = (i // logements) + 1
            rows.append({
                "code_client": f"AB{cc_counter:06d}",
                "id_batiment": target_bldg["id_batiment"],
                "lat_abonne": round(pts_log[i].y, 6),
                "lon_abonne": round(pts_log[i].x, 6),
                "etage": etg,
                "porte": (etg * 100) + ((i % logements) + 1),
                "usage": "logements",
            })
            cc_counter += 1

        buildings_geojson = gdf[["id_batiment", "is_target", "geometry"]].to_json()

        print(f"✅ Import OSM réussi: {len(gdf)} bâtiments, {len(rows)} abonnés générés")
        return JSONResponse(content={
            "buildings_geojson": buildings_geojson,
            "subscribers": rows,
            "count": len(gdf),
            "residence": req.residence,
            "etages_detectes": etages,
            "logements_detectes": logements,
        })
    except Exception as e:
        print(f"❌ Erreur Import OSM: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Erreur d'import : {str(e)}")


# ====================== ALGO FAT & NOMMAGE ======================
def _fallback_clustering(df_sub: pd.DataFrame) -> list:
    from sklearn.cluster import KMeans
    output_rows = []
    bat_groups = df_sub.groupby("id_batiment") if "id_batiment" in df_sub.columns else [("BAT-001", df_sub)]
    for bat_id, bat_df in bat_groups:
        bat_df = bat_df.reset_index(drop=True)
        n_subs = len(bat_df)
        n_fats = max(1, int(np.ceil(n_subs / FAT_CAPACITY)))
        coords = bat_df[["lat_abonne", "lon_abonne"]].values
        labels = np.zeros(n_subs, dtype=int) if n_fats == 1 else KMeans(n_clusters=n_fats, n_init=5,
                                                                        random_state=42).fit_predict(coords)
        for cl in range(n_fats):
            mask = labels == cl
            cluster_df = bat_df[mask]
            if cluster_df.empty: continue
            output_rows.append({
                "id_batiment": str(bat_id), "id_zone": "Z310-001", "fat_id": f"FAT-{str(bat_id)[-6:]}-{cl + 1:02d}",
                "cluster_label": int(cl), "centroid_lat": float(cluster_df["lat_abonne"].mean()),
                "centroid_lon": float(cluster_df["lon_abonne"].mean()),
                "n_subscribers": int(mask.sum()), "usage": "logements", "fdt_assigned": "F310-001-01",
                "capacity_ok": bool(mask.sum() <= FAT_CAPACITY), "cable_m_to_fdt_real": 0.0, "radius_deg": 0.0,
                "subscriber_ids": cluster_df["code_client"].tolist() if "code_client" in cluster_df.columns else [],
            })
    return output_rows


@app.post("/api/emplacementFATs")
async def get_emplacement_fats(req: FATPlacementRequest):
    print(f"POST /api/emplacementFATs ({len(req.subscribers)} abonnés)")
    df_sub = pd.DataFrame(req.subscribers)
    if df_sub.empty:
        print("⚠️ Liste d'abonnés vide")
        raise HTTPException(status_code=400, detail="Liste d'abonnés vide")

    output_rows = []
    if fat_model is not None:
        try:
            print("🤖 Utilisation du modèle Fat Planner...")
            fat_model.fit(df_sub)
            results = getattr(fat_model, "results_", None) or []
            for res in results:
                for fat in (res.fat_candidates or []):
                    output_rows.append({
                        "id_batiment": getattr(res, "id_batiment", "BAT-001"),
                        "id_zone": getattr(res, "id_zone", "Z310-001"),
                        "fat_id": getattr(fat, "fat_id", ""), "cluster_label": getattr(fat, "cluster_label", 0),
                        "centroid_lat": getattr(fat, "centroid_lat", 0.0),
                        "centroid_lon": getattr(fat, "centroid_lon", 0.0),
                        "n_subscribers": getattr(fat, "n_subscribers", 0), "usage": getattr(fat, "usage", "logements"),
                        "fdt_assigned": getattr(fat, "fdt_assigned", "F310-001-01"),
                        "capacity_ok": getattr(fat, "capacity_ok", True),
                        "cable_m_to_fdt_real": getattr(fat, "cable_m_to_fdt_real", 0.0),
                        "radius_deg": getattr(fat, "radius_deg", 0.0),
                        "subscriber_ids": getattr(fat, "subscriber_ids", []),
                    })
            print(f"✅ {len(output_rows)} FATs identifiées par le modèle")
        except Exception as e:
            print(f"⚠️ Erreur modèle: {e}, fallback K-means...")
            pass
    if not output_rows:
        print("🔄 Exécution du fallback K-means...")
        output_rows = _fallback_clustering(df_sub)
        print(f"✅ {len(output_rows)} FATs identifiées (fallback)")
    return {"fat_candidates": output_rows}


@app.post("/api/nomFAT")
async def generate_noms_fat(req: NamingFATRequest):
    print(f"POST /api/nomFAT ({len(req.fat_candidates)} candidats)")
    df_cands = pd.DataFrame(req.fat_candidates)
    df_subs = pd.DataFrame(req.subscribers)
    if df_cands.empty:
        print("⚠️ Candidats FAT vides")
        raise HTTPException(status_code=400, detail="Candidats FAT vides")

    generator = ATIDGenerator(wilaya_code="310")
    df_subs_indexed = df_subs.set_index("code_client")
    ids_at, fat_seq_counter = [], {}

    for _, row in df_cands.iterrows():
        bat_id = row["id_batiment"]
        sub_ids = row.get("subscriber_ids", [])
        fat_seq_counter[bat_id] = fat_seq_counter.get(bat_id, 0) + 1
        portes, etq_min = [], 1

        if isinstance(sub_ids, list) and sub_ids:
            valid_subs = [s for s in sub_ids if s in df_subs_indexed.index]
            if valid_subs:
                sub_rows = df_subs_indexed.loc[valid_subs]
                portes = sorted(sub_rows["porte"].tolist())
                if "etage" in sub_rows.columns: etq_min = int(sub_rows["etage"].min())

        at_id = generator._format_id(
            wilaya=generator.wilaya,
            olt_num=generator._extract_olt_num(row.get("id_zone", "Z310-001")),
            fdt_num=generator._extract_fdt_num(row.get("fdt_assigned", "F310-001-01")),
            fat_seq=fat_seq_counter[bat_id],
            adresse=generator._extract_adresse(bat_id),
            portes=portes if portes else [0],
            etage_depart=etq_min, sequence=1
        )
        ids_at.append(at_id)
        print(f"🏷️ ID généré: {at_id}")

    df_cands["fat_id_AT"] = ids_at
    print("✅ Nommage FAT terminé")
    return {"fat_candidates_with_ids": df_cands.to_dict(orient="records")}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)