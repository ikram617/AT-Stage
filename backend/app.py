from contextlib import asynccontextmanager
import hashlib
import unicodedata
import math
import time
import joblib
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from Greedy_Vertical_Algorithm_hybride.config import settings
import os
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
import numpy as np
import sys
import asyncio
import json
import re
import httpx
from pathlib import Path
from id_generator import ATIDGenerator
from Greedy_Vertical_Algorithm_hybride.fat_planner_hybride import (
    predire_k_depuis_bundle,
    generer_fats_depuis_k,
    load_k_predictor_model,
    load_k_predictor_optuna,
)

# Configuration OSMNX
ox.settings.use_cache = True
ox.settings.log_console = False
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def _update_env(key: str, value: str):
    # Toujours utiliser le chemin absolu du répertoire de app.py
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()

    found = False
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            found = True
        else:
            new_lines.append(line)

    if not found:
        new_lines.append(f"{key}={value}\n")

    with open(env_path, "w") as f:
        f.writelines(new_lines)
    print(f"[Config] .env mis à jour: {key}={value}")


# ====================== MODÈLES FAT PLANNER HYBRIDE ======================
def _base_path():
    if getattr(sys, 'frozen', False):
        return Path(sys._MEIPASS)
    return Path(os.path.dirname(os.path.abspath(__file__)))

BASE_DIR = _base_path()
K_PREDICTOR_OPTUNA_PATH = BASE_DIR / "Greedy_Vertical_Algorithm_hybride/models/k_predictor_osm_optuna.joblib"
K_PREDICTOR_OSM_PATH    = BASE_DIR / "Greedy_Vertical_Algorithm_hybride/models/k_predictor_osm.joblib"
K_PREDICTOR_PATH        = BASE_DIR / "Greedy_Vertical_Algorithm_hybride/models/k_predictor.joblib"
SNAP_RULES_PATH         = BASE_DIR / "Greedy_Vertical_Algorithm_hybride/models/snap_rules.joblib"
k_predictor_bundle = None
snap_rules_bundle = None

PREFAB_CABLES = [15, 20, 50, 80]
PALIER_FIXE_M = 4.0


def _snap_cable(distance: float, cables: list) -> int:
    for c in cables:
        if c >= distance:
            return c
    return 9999


def _run_hybride_pipeline(df_sub: pd.DataFrame, zone_id: str, fdt_nom: str, hauteur_etage_prev: float = 3.0) -> list:
    cables = settings.AT_DROP_CABLE_STANDARDS_M
    palier = 4.0
    capacity = settings.FAT_CAPACITY
    output_rows = []
    bat_groups = df_sub.groupby("id_batiment") if "id_batiment" in df_sub.columns else [("BAT-001", df_sub)]
    all_connections = []

    for bat_id, bat_df in bat_groups:
        bat_df = bat_df.reset_index(drop=True)
        for usage_type in ["logements", "commerces"]:
            grp_df = bat_df[bat_df["usage"] == usage_type].reset_index(
                drop=True) if "usage" in bat_df.columns else bat_df.reset_index(drop=True)
            if grp_df.empty: continue
            groups = [grp_df.iloc[i:i + capacity] for i in range(0, len(grp_df), capacity)]
            for cl_idx, group in enumerate(groups):
                if group.empty: continue
                sub_lats, sub_lons = group["lat_abonne"].values, group["lon_abonne"].values
                sub_etages = group["etage"].values if "etage" in group.columns else np.zeros(len(group))
                etage_fat = int(np.median(sub_etages))
                fat_lat, fat_lon = float(np.mean(sub_lats)), float(np.mean(sub_lons))
                hauteur_etage = hauteur_etage_prev
                if "Hauteur par étage (m)" in group.columns:
                    hauteur_etage = float(group["Hauteur par étage (m)"].iloc[0])
                elif "hauteur_etage" in group.columns:
                    hauteur_etage = float(group["hauteur_etage"].iloc[0])
                distances_real = []
                for _, row in group.iterrows():
                    ab_lat, ab_lon, et_ab = float(row["lat_abonne"]), float(row["lon_abonne"]), float(
                        row.get("etage", 0))
                    R = 6_371_000.0
                    la1, lo1 = math.radians(ab_lat), math.radians(ab_lon)
                    la2, lo2 = math.radians(fat_lat), math.radians(fat_lon)
                    a = (math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin(
                        (lo2 - lo1) / 2) ** 2)
                    dist_h = R * 2 * math.asin(math.sqrt(max(0.0, a)))
                    dist_v = abs(et_ab - etage_fat) * hauteur_etage
                    dist_real = round(dist_v + dist_h + palier, 2)
                    distances_real.append(dist_real)
                    all_connections.append({"code_client": row["code_client"], "distance_real_m": dist_real,
                                            "cable_snap_m": _snap_cable(dist_real, cables)})
                dist_moy = round(float(np.mean(distances_real)), 2)
                cable_snap = _snap_cable(dist_moy, cables)
                fat_id = f"FAT-{str(bat_id)[-6:]}-{cl_idx + 1:02d}"
                output_rows.append({
                    "id_batiment": str(bat_id), "id_zone": zone_id, "fat_id": fat_id, "cluster_label": cl_idx,
                    "centroid_lat": fat_lat, "centroid_lon": fat_lon, "etage_fat": etage_fat,
                    "n_subscribers": len(group), "usage": usage_type, "fdt_assigned": fdt_nom,
                    "capacity_ok": bool(len(group) <= capacity), "cable_m_to_fdt_real": dist_moy,
                    "cable_snap_m": cable_snap,
                    "radius_deg": 0.0,
                    "subscriber_ids": group["code_client"].tolist() if "code_client" in group.columns else [],
                })
    return output_rows, all_connections


@asynccontextmanager
async def lifespan(app: FastAPI):
    global k_predictor_bundle, snap_rules_bundle
    print("🚀 Démarrage - Chargement des modèles FAT Planner Hybride...")
    purged = 0
    for f in RESIDENCE_CACHE_DIR.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list) and len(data) == 0:
                f.unlink()
                purged += 1
        except Exception:
            f.unlink()
            purged += 1
    if purged:
        print(f"  🗑️  {purged} cache(s) vide(s) ou corrompu(s) supprimé(s) au démarrage")

    # Chargement K-Predictor : déléguer à load_k_predictor_model()
    # (Optuna > OSM v3 > Full — même logique que fat_planner_hybride.py)
    _model_dir = BASE_DIR / "Greedy_Vertical_Algorithm_hybride/models"
    try:
        k_predictor_bundle = load_k_predictor_model(_model_dir)
        m = k_predictor_bundle["metrics"]
        print(f"  ✅ K-Predictor chargé via load_k_predictor_model()")
        print(f"     source={Path(k_predictor_bundle.get('source_path', '?')).name}  "
              f"type={k_predictor_bundle['model_type']}  "
              f"features={len(k_predictor_bundle['feature_cols'])}  "
              f"MAE={m['MAE_fats']:.3f} FATs  "
              f"best_params={list(k_predictor_bundle.get('best_params', {}).keys()) or 'N/A'}")
    except FileNotFoundError as e:
        print(f"  ⚠️  {e}")

    if SNAP_RULES_PATH.exists():
        try:
            snap_rules_bundle = joblib.load(SNAP_RULES_PATH)
            snap_rules_bundle['prefab_cables'] = settings.AT_DROP_CABLE_STANDARDS_M
            print(f"  ✅ Snap rules chargées — câbles (actifs: {settings.AT_DROP_CABLE_STANDARDS_M})m")
        except Exception as e:
            print(f"  ❌ Erreur chargement Snap rules : {e}")
    yield
    print("🛑 Arrêt de l'application")


app = FastAPI(title="FTTH Smart Planner API", version="5.2", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

from fastapi.exceptions import RequestValidationError


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    print(f"DEBUG: Validation Error in {request.url.path}: {exc.errors()}")
    # try:
    #     body = await request.json()
    #     print(f"DEBUG: Request body (JSON): {body}")
    # except:
    #     body = await request.body()
    #     print(f"DEBUG: Request body (Raw): {body}")
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.get("/api/config")
async def get_config():
    return {"fat_capacity": settings.FAT_CAPACITY, "cable_standards": settings.AT_DROP_CABLE_STANDARDS_M}


@app.post("/api/config")
async def update_config(data: dict):
    import Greedy_Vertical_Algorithm_hybride.fat_planner_hybride as fph
    new_fat, new_cables = data.get("fat_capacity"), data.get("cable_standards")
    if new_fat is not None:
        val = int(new_fat)
        settings.FAT_CAPACITY = val
        fph.FAT_CAPACITY = val  # patch module-level constant
        _update_env("FAT_CAPACITY", str(val))
        print(f"[Config] FAT_CAPACITY → {val}")
    if new_cables is not None:
        cleaned = sorted([int(x) for x in new_cables])
        settings.AT_DROP_CABLE_STANDARDS_M = cleaned
        fph.PREFAB_CABLES = cleaned  # patch module-level constant
        _update_env("AT_DROP_CABLE_STANDARDS_M", json.dumps(cleaned))
        print(f"[Config] AT_DROP_CABLE_STANDARDS_M → {cleaned}")
    return {
        "status": "success",
        "config": {
            "fat_capacity": settings.FAT_CAPACITY,
            "cable_standards": settings.AT_DROP_CABLE_STANDARDS_M
        }
    }


_RUNTIME_DIR = Path(os.path.dirname(sys.executable)) if getattr(sys, 'frozen', False) else BASE_DIR
_CACHE_DIR = _RUNTIME_DIR / "osm_cache"
_CACHE_DIR.mkdir(exist_ok=True)
JSON_CACHE_DIR = BASE_DIR / "osm_json_cache"
JSON_CACHE_DIR.mkdir(exist_ok=True)
RESIDENCE_CACHE_DIR = _RUNTIME_DIR / "residence_cache"
RESIDENCE_CACHE_DIR.mkdir(exist_ok=True)
RESIDENCE_CACHE_TTL_SECONDS = 86400

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
    cache_file = JSON_CACHE_DIR / f"{code_str}-{nom.replace(' ', '_')}.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def load_from_json_cache(key: str):
    path = JSON_CACHE_DIR / (key.replace(":", "_").replace(" ", "_") + ".json")
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _residence_cache_key(commune: str, wilaya_name: str) -> str:
    raw = f"{wilaya_name.lower().strip()}::{commune.lower().strip()}"
    h = hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]
    safe = re.sub(r"[^\w]", "_", commune)[:20]
    return f"{safe}_{h}"


def _load_residence_cache(commune: str, wilaya_name: str) -> list | None:
    key = _residence_cache_key(commune, wilaya_name)
    path = RESIDENCE_CACHE_DIR / f"{key}.json"
    if not path.exists(): return None
    if time.time() - path.stat().st_mtime > RESIDENCE_CACHE_TTL_SECONDS:
        path.unlink(missing_ok=True)
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_residence_cache(commune: str, wilaya_name: str, residences: list) -> None:
    key = _residence_cache_key(commune, wilaya_name)
    path = RESIDENCE_CACHE_DIR / f"{key}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(residences, f, ensure_ascii=False)
    except Exception:
        pass


OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
    "https://api.openstreetmap.fr/oapi/interpreter",
]


async def _overpass_request(query: str, timeout: int = 60, retries: int = 2) -> dict:
    print(f"[Overpass] Sending query: {query[:100]}...")
    headers = {
        "User-Agent": "FTTH-Smart-Planner/1.0 (Algeria; contact@ftth-planner.dz)",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    for attempt in range(retries):
        for url in OVERPASS_SERVERS:
            try:
                async with httpx.AsyncClient(timeout=float(timeout)) as c:
                    resp = await c.post(url, data={"data": query.strip()}, headers=headers)
                    if resp.status_code == 200:
                        return resp.json()
                    else:
                        print(f"[Overpass] HTTP {resp.status_code} from {url}: {resp.text[:200]}")
            except Exception as e:
                print(f"[Overpass] Error from {url}: {e}")
                continue
    return {"elements": []}


def _classify_building(tags: dict) -> tuple[bool, str]:
    # Concatenate all relevant text fields for keyword matching
    name_fields = [tags.get("name", ""), tags.get("addr:housename", ""), tags.get("operator", ""),
                   tags.get("description", "")]
    raw_text = " ".join(filter(None, name_fields)).lower()

    # Normalization: remove accents for better matching
    def _norm(t: str) -> str:
        nfkd = unicodedata.normalize("NFD", t)
        return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

    text = _norm(raw_text)

    # Strict keywords requested by the user
    # cité, résidence, immeuble, ilot
    keywords = ["cite", "residence", "immeuble", "ilot"]

    # Veto for non-residential amenities
    if tags.get("amenity") in ["mosque", "school", "hospital", "university"]:
        return False, "amenity veto"

    if any(kw in text for kw in keywords):
        return True, "keyword match"

    # Fallback: if building tag is very specific (apartments) but has no name, we might keep it as "unnamed" for bloc grouping
    bt = tags.get("building", "").lower()
    if bt in ["apartments"] and not raw_text.strip():
        return True, "unnamed apartment"

    return False, "no keyword match"


def _build_display_name(tags: dict, osm_id: str, index: int) -> str:
    name = tags.get("name") or tags.get("addr:housename")
    if name: return name
    ref = tags.get("ref")
    if ref: return f"Bât. {ref}"
    return f"Bât. OSM-{str(osm_id)[-5:]}"


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bloc_letter(idx: int) -> str:
    return chr(65 + idx) if idx < 26 else chr(65 + idx // 26 - 1) + chr(65 + idx % 26)


def _compute_blocs(unnamed: list, commune: str) -> list:
    n = len(unnamed)
    if n == 0: return []
    parent = list(range(n))

    def find(x):
        while parent[x] != x: parent[x] = parent[parent[x]]; x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[ra] = rb

    # Optimization: Use a spatial grid (hashing) to avoid O(n^2)
    # 0.001 degrees is ~111m. Buildings within 100m will be in the same or adjacent cells.
    grid = {}
    GRID_SIZE = 0.001
    for i in range(n):
        gx = int(unnamed[i]["lat"] / GRID_SIZE)
        gy = int(unnamed[i]["lon"] / GRID_SIZE)
        grid.setdefault((gx, gy), []).append(i)

    # Check each cell and its 8 neighbors
    for (gx, gy), indices in grid.items():
        for dx in range(-1, 2):
            for dy in range(-1, 2):
                neighbor_indices = grid.get((gx + dx, gy + dy), [])
                for i in indices:
                    for j in neighbor_indices:
                        if i < j:
                            # Pre-check with a simple bounding box to avoid haversine overhead
                            if abs(unnamed[i]["lat"] - unnamed[j]["lat"]) < 0.001 and \
                                    abs(unnamed[i]["lon"] - unnamed[j]["lon"]) < 0.0015:
                                if _haversine_m(unnamed[i]["lat"], unnamed[i]["lon"], unnamed[j]["lat"],
                                                unnamed[j]["lon"]) <= 100:
                                    union(i, j)

    groups = {}
    for i in range(n): groups.setdefault(find(i), []).append(i)
    blocs = []
    for idx, (root, indices) in enumerate(groups.items()):
        letter = _bloc_letter(idx)
        clat = sum(unnamed[i]["lat"] for i in indices) / len(indices)
        clon = sum(unnamed[i]["lon"] for i in indices) / len(indices)
        buildings = []
        for b_idx, orig in enumerate(indices, 1):
            b = unnamed[orig]
            buildings.append({**b, "name": f"Cité {letter}-bloc{b_idx}", "bloc_letter": letter, "bloc_numero": b_idx})
        blocs.append({
            "name": f"Cité {letter}", "osm_id": buildings[0]["osm_id"], "lat": round(clat, 6), "lon": round(clon, 6),
            "type": "bloc", "is_bloc": True, "count": len(indices), "buildings": buildings
        })
    return blocs


async def fetch_residences_in_commune(commune, wilaya, lat_f=None, lon_f=None):
    print(f"[Residence] fetch_residences_in_commune called for {commune}, {wilaya} | lat_f={lat_f}, lon_f={lon_f}")
    cached = _load_residence_cache(commune, wilaya)
    if cached:
        print(f"[Residence] Returning cached result for {commune}")
        return cached

    elements = []

    # ── 1. Recherche par Aire Administrative ─────────────────────────────────
    q_area = f"""[out:json][timeout:90];
area["name"="{commune}"]["admin_level"~"8|7"]->.a;
(way["building"](area.a);relation["building"](area.a););
out tags center;"""
    data = await _overpass_request(q_area, timeout=90)
    elements = data.get("elements", [])
    print(f"[Residence] Area query → {len(elements)} éléments pour '{commune}'")

    # ── 2. Fallback Bounding Box ──────────────────────────────────────────────
    if not elements and lat_f and lon_f:
        m = 0.06
        q_bbox = (
            f'[out:json][timeout:60];'
            f'(way["building"]({lat_f - m},{lon_f - m},{lat_f + m},{lon_f + m});'
            f'relation["building"]({lat_f - m},{lon_f - m},{lat_f + m},{lon_f + m}););'
            f'out tags center;'
        )
        data = await _overpass_request(q_bbox, timeout=60)
        elements = data.get("elements", [])
        print(f"[Residence] BBox fallback → {len(elements)} éléments")

    def _norm(t: str) -> str:
        nfkd = unicodedata.normalize("NFD", t)
        return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

    # Keywords stricts (insensibles aux accents et à la casse)
    KEYWORDS = ["cite", "residence", "immeuble", "ilot"]

    AMENITY_VETO = {"mosque", "school", "hospital", "university", "bank", "restaurant", "cafe"}

    named, unnamed = [], []
    seen = set()

    for el in elements:
        oid = str(el["id"])
        if oid in seen: continue
        seen.add(oid)

        tags = el.get("tags", {})
        if tags.get("amenity") in AMENITY_VETO: continue

        lat = (el.get("center") or {}).get("lat") or el.get("lat")
        lon = (el.get("center") or {}).get("lon") or el.get("lon")
        if not lat or not lon: continue

        raw_name = (tags.get("name") or tags.get("addr:housename") or "").strip()
        has_name = bool(raw_name)

        entry = {
            "name": raw_name if has_name else f"Bât. OSM-{oid[-5:]}",
            "osm_id": oid,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "levels": int(tags.get("building:levels", 5)) if str(tags.get("building:levels", "")).isdigit() else 5,
            "units": int(tags.get("building:units", 0)) if str(tags.get("building:units", "")).isdigit() else 0,
            "has_official_name": has_name,
            "operator": tags.get("operator", ""),
        }

        if has_name:
            # Bâtiments NOMMÉS: garder uniquement si le nom contient un keyword
            if any(kw in _norm(raw_name) for kw in KEYWORDS):
                named.append(entry)
        else:
            # Bâtiments SANS NOM: garder uniquement les types résidentiels pour regroupement en blocs
            bt = tags.get("building", "").lower()
            if bt in ("apartments", "residential", "yes"):
                unnamed.append(entry)

    print(f"[Residence] {commune}: {len(named)} nommés filtrés, {len(unnamed)} sans nom → blocs")
    blocs = _compute_blocs(unnamed, commune)
    result = sorted(named, key=lambda x: x["name"]) + sorted(blocs, key=lambda x: x["name"])
    print(f"[Residence] Résultat final: {len(named)} résidences + {len(blocs)} blocs")
    if result: _save_residence_cache(commune, wilaya, result)
    return result


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
    hauteur_etage: float = 3.0


class NamingFATRequest(BaseModel):
    fat_candidates: List[Dict[str, Any]]
    subscribers: List[Dict[str, Any]]
    ville: Optional[str] = None
    commune: Optional[str] = None
    residence: Optional[str] = None


class KPredictorPlacementRequest(BaseModel):
    nb_etages: int
    nb_log_etage: int
    hauteur_etage: float = 3.0
    type_batiment: str = "AADL"
    presence_commerce: int = 0
    id_batiment: Any = "BAT-001"
    centroid_lat: float = 36.7
    centroid_lon: float = 3.0
    surface_m2: Optional[float] = None
    fat_balance_thr: float = 0.75
    ville: Optional[str] = None
    k_force: Optional[int] = None  # forcer K si besoin (bypass ML)
    taux_occupation: Optional[float] = None  # forcer taux occupation estimé


@app.get("/api/ville")
async def get_ville():
    cached = load_from_json_cache("villes")
    return {"villes": cached or []}


@app.get("/api/commune")
async def get_commune(ville: str = Query(...)):
    w = _load_wilaya_cache(ville)
    if w: return {"communes": [c["nom"] for c in w.get("communes", [])]}
    return {"communes": load_from_json_cache(f"c::{ville}") or []}


@app.get("/api/residence")
async def get_residence(ville: str = Query(...), commune: str = Query(...), search: str = Query(None)):
    v_pure = ville.split(" - ")[1] if " - " in ville else ville
    lat_f, lon_f = None, None
    w = _load_wilaya_cache(ville)
    if w:
        for c in w.get("communes", []):
            if c["nom"] == commune: lat_f, lon_f = c.get("lat"), c.get("lon"); break
    res = await fetch_residences_in_commune(commune, v_pure, lat_f, lon_f)
    if search:
        q = search.lower()
        res = [r for r in res if q in r["name"].lower()]
    return {"residences": res}


@app.post("/api/importOSM")
async def import_osm(req: ImportOSMRequest):
    _STRONG = ["résidence", "residence", "cité", "cite", "cit", "lotissement", "hai", "haï", "aadl", "lsp", "lpa",
               "cnep", "opgi", "enpi", "إقامة", "حي", "resid"]
    _WEAK = ["immeuble", "bloc", "bâtiment", "batiment", "tour", "villa", "appartement", "logement", "habitat", "ilot"]

    def _norm(t: str) -> str:
        nfkd = unicodedata.normalize("NFD", t)
        return re.sub(r"\s+", " ", re.sub(r"[^\w\s\u0600-\u06FF]", " ",
                                          "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower())).strip()

    def _text(row) -> str:
        parts = []
        for col in ["name", "addr:housename", "operator", "description"]:
            try:
                v = row.get(col)
                if v and str(v).strip() not in ("nan", ""): parts.append(str(v).strip())
            except Exception:
                pass
        return _norm(" ".join(parts))

    def _kw_match(txt: str):
        for kw in _STRONG:
            if kw in txt: return True
        for kw in _WEAK:
            if kw in txt and len(txt.replace(kw, "").strip()) >= 6: return True
        return False

    try:
        if req.lat and req.lon:
            gdf_all = ox.features_from_point((req.lat, req.lon), dist=800, tags={"building": True})
        else:
            v = req.ville.split(" - ")[1] if " - " in req.ville else req.ville
            gdf_all = ox.features_from_place(f"{req.commune}, {v}, Algeria", tags={"building": True})

        if gdf_all.empty:
            raise HTTPException(404, "Aucun bâtiment trouvé sur OpenStreetMap.")

        # Extract the real OSM ID from the index before dropping it
        osm_ids = []
        for idx in gdf_all.index:
            if isinstance(idx, tuple) and len(idx) > 1:
                osm_ids.append(str(idx[1]))
            else:
                osm_ids.append(str(idx))
        gdf_all["osm_id_real"] = osm_ids

        gdf_all = gdf_all[gdf_all.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy().reset_index(drop=True)

        # Identifier le bâtiment cible (le plus proche du clic)
        target_idx_orig = -1
        if req.lat and req.lon:
            target_idx_orig = int(gdf_all.geometry.distance(Point(req.lon, req.lat)).idxmin())

        target_norm = _norm(str(req.residence))

        # Filtrage : garder cible + bâtiments résidentiels (mots-clés ou tags OSM)
        kept = []
        for i, row in gdf_all.iterrows():
            txt = _text(row)
            is_target = (i == target_idx_orig)
            is_name_match = target_norm and (target_norm in txt or
                                             {t for t in target_norm.split() if len(t) >= 3}.issubset(
                                                 {t for t in txt.split() if len(t) >= 3}))

            # Vérifier si c'est un bâtiment résidentiel par tag ou par mot-clé
            building_tag = str(row.get("building", "")).lower()
            is_residential = building_tag in ("apartments", "residential") or _kw_match(txt)

            if is_target or is_name_match or is_residential:
                kept.append(i)

        if not kept:
            raise HTTPException(404, f"Aucun bâtiment résidentiel trouvé pour '{req.residence}'.")

        gdf = gdf_all.loc[kept].copy().reset_index(drop=True)

        # Recalculer target_idx dans le GDF filtré
        target_idx = kept.index(target_idx_orig) if (target_idx_orig != -1 and target_idx_orig in kept) else 0

        # Construire métadonnées
        formatted_ids, is_target_list, names_list, levels_list, units_list, lat_list, lon_list = [], [], [], [], [], [], []
        for i, row in gdf.iterrows():
            is_tgt = (i == target_idx)
            is_target_list.append(is_tgt)
            real_name = None
            for col in ["name", "addr:housename"]:
                try:
                    v = row.get(col)
                    if v and str(v).strip() not in ("nan", ""): real_name = str(v).strip(); break
                except Exception:
                    pass
            lettre = req.residence.replace("Cité ", "").replace("cité ", "").strip()
            if not lettre: lettre = req.residence
            names_list.append(req.residence if is_tgt else (real_name or f"Cité {lettre}-Bloc {i + 1}"))
            formatted_ids.append(
                f"CIBLE-{req.residence[:20].upper().replace(' ', '-')}" if is_tgt else f"Bloc-{i + 1:03d}")
            lvl = row.get("building:levels")
            levels_list.append(int(lvl) if pd.notna(lvl) and str(lvl).isdigit() else None)
            unt = row.get("building:units")
            units_list.append(int(unt) if pd.notna(unt) and str(unt).isdigit() else None)
            c = row.geometry.centroid
            lat_list.append(c.y);
            lon_list.append(c.x)

        gdf["id_batiment"] = formatted_ids
        gdf["is_target"] = is_target_list
        gdf["nom_batiment"] = names_list
        gdf["bat_levels"] = levels_list
        gdf["bat_units"] = units_list
        gdf["centroid_lat"] = lat_list
        gdf["centroid_lon"] = lon_list

        # Étages et logements pour la cible
        tb = gdf.iloc[target_idx]
        etages = int(tb["bat_levels"]) if pd.notna(tb["bat_levels"]) else req.nombre_etages
        osm_units = tb["bat_units"]
        logements = (max(1, int(osm_units) // max(1, etages)) if pd.notna(osm_units) and osm_units > 0
                     else req.logements_par_etage)
        etages, logements = max(1, etages), max(1, logements)
        gdf.loc[target_idx, "bat_levels"] = etages
        gdf.loc[target_idx, "bat_units"] = etages * logements

        # Générer positions abonnés (espacement basé sur surface réelle)
        centroid = tb.geometry.centroid
        surface_m2 = tb.geometry.area * (111000 ** 2)
        esp_deg = max(0.00005, np.sqrt(surface_m2 / max(logements, 1)) / 111000)
        start_lon = centroid.x - (logements - 1) * esp_deg / 2

        rows, cc = [], 1
        gp_com, gp_log = 1, 1
        if req.commerce:
            for i in range(logements):
                rows.append({"code_client": f"AB{cc:06d}", "id_batiment": tb["id_batiment"],
                             "lat_abonne": round(centroid.y, 6), "lon_abonne": round(start_lon + i * esp_deg, 6),
                             "etage": 0, "porte": gp_com, "usage": "commerces"})
                cc += 1;
                gp_com += 1
        for etg in range(1, etages + 1):
            for i in range(logements):
                rows.append({"code_client": f"AB{cc:06d}", "id_batiment": tb["id_batiment"],
                             "lat_abonne": round(centroid.y, 6), "lon_abonne": round(start_lon + i * esp_deg, 6),
                             "etage": etg, "porte": gp_log, "usage": "logements"})
                cc += 1;
                gp_log += 1

        cols = ["id_batiment", "is_target", "nom_batiment", "bat_levels", "bat_units", "centroid_lat", "centroid_lon",
                "osm_id_real", "geometry"]
        buildings_geojson = gdf[cols].to_json()

        print(f"  ImportOSM OK → {len(gdf)} bâtiments, {len(rows)} abonnés (cible: {etages}ét × {logements}log)")
        return JSONResponse(content={
            "buildings_geojson": buildings_geojson,
            "subscribers": rows,
            "count": len(gdf),
            "residence": req.residence,
            "etages_detectes": etages,
            "logements_detectes": logements,
        })
    except HTTPException:
        raise
    except Exception as e:
        print(f"  Erreur ImportOSM: {e}")
        raise HTTPException(400, f"Erreur d'import OSM : {str(e)}")


@app.post("/api/emplacementFATs")
async def get_emplacement_fats(req: FATPlacementRequest):
    df = pd.DataFrame(req.subscribers)
    if df.empty: raise HTTPException(400, "Vide")
    rows, conns = _run_hybride_pipeline(df, "Z310-001", "F310-001-01", req.hauteur_etage)
    return {"fat_candidates": rows, "connections": conns}


@app.post("/api/nomFAT")
async def generate_noms_fat(req: NamingFATRequest):
    w_code = ATIDGenerator.get_wilaya_code(req.ville)
    gen = ATIDGenerator(wilaya_code=w_code)
    results = []

    # Compteur par (bâtiment, étage, usage) pour la séquence finale (ex: -1, -2 au bout du nom)
    floor_usage_counters = {}

    for idx, f in enumerate(req.fat_candidates, 1):
        bat_id = f.get("id_batiment", "ADR")
        if req.residence:
            adresse = req.residence.replace(" ", "-")
        else:
            adresse = gen._extract_adresse(str(bat_id)).replace(" ", "-")
        etage = f.get("etage_fat", 1)
        usage = f.get("usage", "logements")

        # Calcul de la séquence par étage/usage pour le dernier chiffre du nom
        counter_key = (str(bat_id), etage, usage)
        floor_usage_counters[counter_key] = floor_usage_counters.get(counter_key, 0) + 1
        seq_in_floor = floor_usage_counters[counter_key]

        olt_num = "001"
        if "BAT-" in str(bat_id):
            olt_num = f"{int(str(bat_id).split('-')[-1]) % 999:03d}"

        client_ids = f.get("subscriber_ids", [])
        portes = []
        if client_ids and req.subscribers:
            sub_map = {s.get("code_client"): s.get("porte") for s in req.subscribers if s.get("code_client")}
            portes = list(dict.fromkeys(
                sub_map[cid] for cid in client_ids if cid in sub_map
            ))

        if not portes:
            n_subs = f.get("n_subscribers", 8)
            portes = list(range(1, n_subs + 1))

        f["fat_id_AT"] = gen._format_id(
            wilaya=w_code,
            olt_num=olt_num,
            fdt_num="01",
            fat_seq=idx,
            adresse=adresse,
            portes=portes,
            etage_depart=etage,
            sequence=seq_in_floor,
            usage=usage
        )
        if client_ids and req.subscribers:
            matched_ids = [cid for cid in client_ids if cid in sub_map]
            unmatched = len(client_ids) - len(matched_ids)
            if unmatched > 0:
                print(f"  ⚠️ nomFAT: {unmatched} subscriber_ids sans porte dans sub_map — vérifier le préfixe PRED-")
            f["subscriber_ids"] = matched_ids if matched_ids else client_ids
        results.append(f)

    return {"fat_candidates_with_ids": results}


@app.post("/api/emplacementFATs/predict")
async def get_emplacement_fats_predict(req: KPredictorPlacementRequest):
    if k_predictor_bundle is None:
        raise HTTPException(503, "Modèle K-Predictor non chargé — lancer model.py")

    w_code = ATIDGenerator.get_wilaya_code(req.ville)

    # ── Prédiction K via bundle (Optuna ou OSM) ───────────────────────────────
    taux_occ = req.taux_occupation if req.taux_occupation is not None else 0.70
    res = predire_k_depuis_bundle(
        nb_etages=req.nb_etages,
        nb_log_etage=req.nb_log_etage,
        hauteur_etage=req.hauteur_etage,
        type_batiment=req.type_batiment,
        presence_commerce=req.presence_commerce,
        k_bundle=k_predictor_bundle,
        snap_bundle=snap_rules_bundle or {},
        taux_occupation_estime=taux_occ,
        surface_m2=req.surface_m2,
    )

    # Permettre de forcer K depuis le frontend
    K = int(req.k_force) if req.k_force else res.get("K_predit_raw", res["K_predit"])

    # Récupérer nb_logements_habites_estime depuis le résultat du modèle
    # (clé principale v3.1 — fallback sur l'ancienne clé et calcul manuel)
    nb_total = req.nb_etages * req.nb_log_etage
    nb_habites_estime = res.get(
        "nb_logements_habites_estime",
        res.get("nb_logements_habites", max(1, round(nb_total * taux_occ)))
    )

    print(
        f"  [Mode Estimé] type={req.type_batiment}  "
        f"R+{req.nb_etages}  {req.nb_log_etage}log/ét  "
        f"→ K prédit={res['K_predit']} (raw={res.get('K_predit_raw', res['K_predit']):.2f})  "
        f"nb_habites_estimé={nb_habites_estime}/{nb_total}  "
        f"taux_occ={nb_habites_estime / max(nb_total, 1) * 100:.1f}%  "
        f"modèle={res.get('modele_type', '?')}  MAE={res.get('modele_mae_fats', 0.0):.3f} FATs"
    )

    # ── Génération des FATs sur les abonnés simulés ────────────────────────────
    gen = generer_fats_depuis_k(
        K=K,
        nb_etages=req.nb_etages,
        nb_log_etage=req.nb_log_etage,
        hauteur_etage=req.hauteur_etage,
        presence_commerce=req.presence_commerce,
        bat_id=str(req.id_batiment),
        centroid_lat=req.centroid_lat,
        centroid_lon=req.centroid_lon,
        fdt_nom=f"F{w_code}-001-01",
        snap_bundle=snap_rules_bundle or {},
        fat_balance_thr=req.fat_balance_thr,
        type_batiment=req.type_batiment,
    )

    return {
        "fat_candidates": gen["fat_candidates"],
        "subscribers": gen.get("subscribers", []),
        "nb_log_etage": req.nb_log_etage,
        "k_prediction": res,
        "nb_logements_habites_estime": nb_habites_estime,
        "K_utilise": K,
        "modele_type": res.get("modele_type", "?"),
        "mode": "prediction",
    }

if __name__ == "__main__":
    import uvicorn
    import multiprocessing
    multiprocessing.freeze_support()
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)