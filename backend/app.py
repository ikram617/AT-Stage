from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
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
import types
import sys
import asyncio
import json
import time
import re
import sqlite3
import threading
from pathlib import Path
import httpx

# ====================== IMPORT MODÈLE ======================
config_module = types.ModuleType("config")


class _Settings:
    FAT_CAPACITY = 8
    TORTUOSITY_TRUNK = 1.3
    AT_DROP_CABLE_STANDARDS_M = [10, 15, 20, 30, 50, 100]


config_module.settings = _Settings()
sys.modules["config"] = config_module

from model2D import FATSmartPlanner
from id_generator import ATIDGenerator

# ====================== CACHE SQLite + STALE-WHILE-REVALIDATE ======================
_CACHE_DIR = Path("osm_cache")
_CACHE_DIR.mkdir(exist_ok=True)
_CACHE_DB = _CACHE_DIR / "osm_cache.db"

ox.settings.use_cache = True
ox.settings.cache_folder = str(_CACHE_DIR / "osmnx_http")
ox.settings.timeout = 60
ox.settings.overpass_rate_limit = True

# TTL : 24h = frais, 7j = stale acceptable, > 7j = supprimé
_FRESH_TTL = 86400     # 24 heures
_STALE_TTL = 604800    # 7 jours

# Mémoire RAM (1er niveau, le plus rapide)
_mem_cache: dict = {}
_db_lock = threading.Lock()


def _init_cache_db():
    """Crée la table de cache SQLite si elle n'existe pas."""
    with _db_lock:
        with sqlite3.connect(str(_CACHE_DB)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    ts    REAL NOT NULL
                )
            """)
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_cache_ts ON cache(ts)""")


_init_cache_db()


def cache_get(key: str):
    """Lecture cache : RAM → SQLite. Retourne (value, needs_refresh)."""
    # 1) RAM (instantané)
    if key in _mem_cache:
        val, ts = _mem_cache[key]
        age = time.time() - ts
        if age < _FRESH_TTL:
            return val, False      # frais
        if age < _STALE_TTL:
            return val, True       # stale → répondre + refresh en background
        del _mem_cache[key]        # trop vieux

    # 2) SQLite (rapide, persistant)
    try:
        with _db_lock:
            with sqlite3.connect(str(_CACHE_DB)) as conn:
                row = conn.execute(
                    "SELECT value, ts FROM cache WHERE key = ?", (key,)
                ).fetchone()
        if row:
            value = json.loads(row[0])
            age = time.time() - row[1]
            _mem_cache[key] = (value, row[1])  # remplir RAM
            if age < _FRESH_TTL:
                return value, False
            if age < _STALE_TTL:
                return value, True
            # trop vieux → purger
            with _db_lock:
                with sqlite3.connect(str(_CACHE_DB)) as conn:
                    conn.execute("DELETE FROM cache WHERE key = ?", (key,))
    except Exception:
        pass

    return None, True  # pas de cache → refresh obligatoire


def cache_get_value(key: str):
    """Raccourci : retourne la valeur ou None (ignore needs_refresh)."""
    val, _ = cache_get(key)
    return val


def cache_set(key: str, value):
    """Écriture cache : RAM + SQLite."""
    now = time.time()
    _mem_cache[key] = (value, now)
    try:
        with _db_lock:
            with sqlite3.connect(str(_CACHE_DB)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cache (key, value, ts) VALUES (?, ?, ?)",
                    (key, json.dumps(value, ensure_ascii=False), now)
                )
    except Exception:
        pass


# ====================== FASTAPI APP ======================
app = FastAPI(
    title="FTTH Smart Planner API - Algérie Télécom",
    version="4.2",
    description="API Découpée en 5 étapes (OSM GeoJSON, FAT Placement, AT ID) — Overpass optimisé sans timeout",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Request
from fastapi.responses import JSONResponse as _JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


class CORSOnErrorMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
        except Exception as exc:
            response = _JSONResponse(status_code=500, content={"detail": str(exc)})
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response


app.add_middleware(CORSOnErrorMiddleware)


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


# ====================== LISTE WILAYAS ======================
WILAYAS: List[str] = [
    "01-Adrar", "02-Chlef", "03-Laghouat", "04-Oum El Bouaghi", "05-Batna",
    "06-Béjaïa", "07-Biskra", "08-Béchar", "09-Blida", "10-Bouira",
    "11-Tamanrasset", "12-Tébessa", "13-Tlemcen", "14-Tiaret", "15-Tizi Ouzou",
    "16-Alger", "17-Djelfa", "18-Jijel", "19-Sétif", "20-Saïda",
    "21-Skikda", "22-Sidi Bel Abbès", "23-Annaba", "24-Guelma", "25-Constantine",
    "26-Médéa", "27-Mostaganem", "28-M'Sila", "29-Mascara", "30-Ouargla",
    "31-Oran", "32-El Bayadh", "33-Illizi", "34-Bordj Bou Arréridj", "35-Boumèrdès",
    "36-El Tarf", "37-Tindouf", "38-Tissemsilt", "39-El Oued", "40-Khenchela",
    "41-Souk Ahras", "42-Tipaza", "43-Mila", "44-Aïn Defla", "45-Naâma",
    "46-Aïn Témouchent", "47-Ghardaïa", "48-Relizane",
    "49-Timimoun", "50-Bordj Badji Mokhtar", "51-Ouled Djellal", "52-Béni Abbès",
    "53-In Salah", "54-In Guezzam", "55-Touggourt", "56-Djanet",
    "57-El M'Ghair", "58-El Meniaa",
]


def _ville_sans_numero(ville: str) -> str:
    m = re.match(r"^\d{2}-(.+)$", ville.strip())
    return m.group(1) if m else ville.strip()


# ====================== OVERPASS CONFIG (sans timeout global) ======================
OVERPASS_SERVERS = [
    "https://lz4.overpass-api.de/api/interpreter",  # souvent le plus rapide
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]


async def _overpass_request(query: str) -> dict:
    """Requête Overpass avec mode parallèle (race) et timeout strict."""
    async def _try_server(url: str):
        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                resp = await client.post(url, data={"data": query.strip()})
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass
        return None

    tasks = [asyncio.create_task(_try_server(url)) for url in OVERPASS_SERVERS]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result and result.get("elements"):
            # On annule les autres requêtes dès qu'on a un résultat
            for t in tasks:
                t.cancel()
            return result
            
    # Si tous ont échoué ou timeout
    for t in tasks:
        t.cancel()
    return {"elements": []}


# ====================== FONCTIONS OVERPASS LÉGÈRES ======================
async def fetch_quartiers_overpass(ville_pure: str) -> List[str]:
    query = f"""
    [out:json][timeout:25];
    area["name"="{ville_pure}"]["admin_level"="4"]->.searchArea;
    (
      relation["boundary"="administrative"]["admin_level"="8"](area.searchArea);
      relation["place"~"neighbourhood|suburb|quarter"](area.searchArea);
    );
    out tags;
    """
    data = await _overpass_request(query)
    names = [el.get("tags", {}).get("name") for el in data.get("elements", [])
             if el.get("tags", {}).get("name")]
    return sorted(set(filter(None, names)))


async def fetch_residences_overpass(quartier: str, ville_pure: str) -> List[str]:
    """Version ultra-légère : seulement les noms (out tags)."""
    query = f"""
    [out:json][timeout:25];
    area[name="{quartier}"]->.a;
    (
      way["building"]["name"](area.a);
      relation["building"]["name"](area.a);
    );
    out tags;
    """
    data = await _overpass_request(query)
    elements = data.get("elements", [])

    names = [el.get("tags", {}).get("name", "") for el in elements if el.get("tags", {}).get("name")]
    keywords = ["résidence", "cité", "lotissement", "hai", "rés", "bloc",
                "complexe", "appartements", "tower", "immeuble", "el ", "hay "]
    filtered = [n for n in names if any(k in n.lower() for k in keywords)]
    return sorted(set(filtered or names))[:100]


# ====================== PREFETCH EN BACKGROUND ======================
async def _warm_quartiers(ville: str):
    key = f"q::{ville}"
    val, needs_refresh = cache_get(key)
    if val is not None and not needs_refresh:
        return
    ville_pure = _ville_sans_numero(ville)
    result = await fetch_quartiers_overpass(ville_pure)
    if result:
        cache_set(key, result)


async def _warm_residences(ville: str, quartier: str):
    key = f"r::{ville}::{quartier}"
    val, needs_refresh = cache_get(key)
    if val is not None and not needs_refresh:
        return
    ville_pure = _ville_sans_numero(ville)
    result = await fetch_residences_overpass(quartier, ville_pure)
    if result:
        cache_set(key, result)


# ====================== ROUTES DÉCOUVERTE OSM ======================
@app.get("/api/ville")
async def get_ville():
    return {"villes": WILAYAS}


@app.get("/api/ville/{ville}/prefetch")
async def prefetch_ville(ville: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(_warm_quartiers, ville)
    return {"status": "prefetching", "ville": ville}


@app.get("/api/quartier/{ville}/prefetch")
async def prefetch_quartier(ville: str, quartier: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(_warm_residences, ville, quartier)
    return {"status": "prefetching", "quartier": quartier}


@app.get("/api/quartier")
async def get_quartier(ville: str = Query(...), background_tasks: BackgroundTasks = None):
    key = f"q::{ville}"
    cached, needs_refresh = cache_get(key)

    if cached is not None:
        # Stale-While-Revalidate : répondre immédiatement, refresh en background
        if needs_refresh and background_tasks:
            background_tasks.add_task(_warm_quartiers, ville)
        return {"quartiers": cached, "source": "cache", "fresh": not needs_refresh}

    # Pas de cache du tout → fetch synchrone
    ville_pure = _ville_sans_numero(ville)
    result = await fetch_quartiers_overpass(ville_pure)
    quartiers = result or []
    if quartiers:
        cache_set(key, quartiers)
    return {"quartiers": quartiers, "source": "overpass", "fresh": True}


@app.get("/api/residence")
async def get_residence(ville: str = Query(...), quartier: str = Query(...), background_tasks: BackgroundTasks = None):
    key = f"r::{ville}::{quartier}"
    cached, needs_refresh = cache_get(key)

    if cached is not None:
        # Stale-While-Revalidate : répondre immédiatement, refresh en background
        if needs_refresh and background_tasks:
            background_tasks.add_task(_warm_residences, ville, quartier)
        return {"residences": cached, "source": "cache", "fresh": not needs_refresh}

    # Pas de cache du tout → fetch synchrone
    ville_pure = _ville_sans_numero(ville)
    result = await fetch_residences_overpass(quartier, ville_pure)
    residences = result or []
    if residences:
        cache_set(key, residences)
    return {"residences": residences, "source": "overpass", "fresh": True}


# ====================== IMPORT OSM (bâtiments live) ======================
def generate_random_point_in_polygon(poly, n: int):
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


@app.post("/api/importOSM")
async def import_osm(req: ImportOSMRequest):
    ville_pure = _ville_sans_numero(req.ville)
    place = f"{req.residence}, {req.quartier}, {ville_pure}, Algeria"

    # Désactivation cache pour bâtiments (toujours frais)
    _prev_cache = ox.settings.use_cache
    ox.settings.use_cache = False

    try:
        try:
            gdf = ox.features_from_place(place, tags={"building": True})
            if gdf.empty:
                raise ValueError("Aucun bâtiment")
        except Exception:
            try:
                place_fallback = f"{req.quartier}, {ville_pure}, Algeria"
                gdf = ox.features_from_place(place_fallback, tags={"building": True})
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Erreur OSM: Impossible de récupérer la zone. ({str(e)})")
    finally:
        ox.settings.use_cache = _prev_cache

    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if gdf.empty:
        raise HTTPException(status_code=400, detail="Aucun polygone de bâtiment trouvé.")
    gdf = gdf.reset_index(drop=True)

    # Nommage bâtiments
    formatted_ids = []
    for i, row in gdf.iterrows():
        bloc_val = "A"
        if "ref" in row and pd.notna(row["ref"]):
            bloc_val = str(row["ref"]).upper()
        elif "name" in row and pd.notna(row["name"]) and "bloc" in str(row["name"]).lower():
            m = re.search(r'bloc\s+([a-zA-Z0-9]+)', str(row["name"]).lower())
            bloc_val = m.group(1).upper() if m else chr(65 + (i % 26))
        else:
            bloc_val = chr(65 + (i % 26))

        num_val = str(i + 1)
        if "addr:housenumber" in row and pd.notna(row["addr:housenumber"]):
            num_val = str(row["addr:housenumber"])

        bat_name = f"{ville_pure}-{req.quartier}-{req.residence}-BLOC {bloc_val}-numéro {num_val}"
        formatted_ids.append(bat_name)

    gdf["id_batiment"] = formatted_ids

    # Génération abonnés
    rows = []
    cc_counter = 1
    for _, bldg in gdf.iterrows():
        bat_id = bldg["id_batiment"]
        poly = bldg.geometry
        if not poly.is_valid or poly.area == 0:
            continue

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


# ====================== EMPLACEMENT FATs & NOM FAT (inchangés) ======================
@app.post("/api/emplacementFATs")
async def get_emplacement_fats(req: FATPlacementRequest):
    df_sub = pd.DataFrame(req.subscribers)
    if df_sub.empty:
        raise HTTPException(status_code=400, detail="Liste des abonnés vide")

    try:
        planner = FATSmartPlanner()
        planner.fit(df_sub)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur modèle K-Means : {str(e)}")

    output_rows = []
    for res in planner.results_:
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

        fat_seq_counter[bat_id] = fat_seq_counter.get(bat_id, 0) + 1
        seq = fat_seq_counter[bat_id]

        portes = []
        etq_min = 1
        if isinstance(sub_ids, list) and len(sub_ids) > 0:
            valid_subs = [s for s in sub_ids if s in df_subs_indexed.index]
            if valid_subs:
                sub_rows = df_subs_indexed.loc[valid_subs]
                portes = sorted(sub_rows["porte"].tolist())
                if "etage" in sub_rows.columns:
                    etq_min = int(sub_rows["etage"].min())

        olt_num = generator._extract_olt_num(zone_id)
        fdt_num = generator._extract_fdt_num(fdt_id)
        adresse = generator._extract_adresse(bat_id)

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