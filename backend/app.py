from contextlib import asynccontextmanager
import hashlib
import math
import time
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
import sys
import asyncio
import json
import re
import httpx
from pathlib import Path

from id_generator import ATIDGenerator

# Configuration OSMNX
ox.settings.use_cache = True
ox.settings.log_console = False

import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")

FAT_CAPACITY = 8

# ====================== MODÈLES FAT PLANNER HYBRIDE ======================
# Produits par fat_planner_hybride.py (P5 — Sauvegarde déploiement)
K_PREDICTOR_PATH = Path("Greedy Vertical Algorithm hybride/models/k_predictor.joblib")
SNAP_RULES_PATH = Path("Greedy Vertical Algorithm hybride/models/snap_rules.joblib")

k_predictor_bundle = None  # {"model", "feature_cols", "metrics", "type_bat_map"}
snap_rules_bundle = None  # {"prefab_cables", "palier_fixe_m", "fat_capacity"}

# Constantes physiques (valeurs par défaut — écrasées au chargement du modèle)
PREFAB_CABLES = [15, 20, 50, 80]
PALIER_FIXE_M = 4.0


def _snap_cable(distance: float, cables: list) -> int:
    """Règle AT déterministe : câble préfab >= distance réelle."""
    for c in cables:
        if c >= distance:
            return c
    return 9999


def _run_hybride_pipeline(df_sub: pd.DataFrame, zone_id: str, fdt_nom: str) -> list:
    """
    Pipeline Greedy+Médiane+Snap de fat_planner_hybride.py, adapté pour l'API.

    Étape 1 — Groupement Greedy séquentiel par FAT_CAPACITY (8)
    Étape 2 — Placement FAT à la médiane analytique des étages du groupe
    Étape 3 — Distance physique (haversine horizontal + vertical + palier)
    Étape 4 — Snap câble déterministe {15,20,50,80,9999}m

    Entrée  : df_sub avec colonnes [code_client, lat_abonne, lon_abonne, etage,
                                    porte, id_batiment, usage]
    Sortie  : liste de dicts fat_candidate (même format que l'ancien fallback)
    """
    cables = snap_rules_bundle["prefab_cables"] if snap_rules_bundle else PREFAB_CABLES
    palier = snap_rules_bundle["palier_fixe_m"] if snap_rules_bundle else PALIER_FIXE_M
    capacity = snap_rules_bundle["fat_capacity"] if snap_rules_bundle else FAT_CAPACITY

    output_rows = []

    bat_groups = (
        df_sub.groupby("id_batiment")
        if "id_batiment" in df_sub.columns
        else [("BAT-001", df_sub)]
    )

    for bat_id, bat_df in bat_groups:
        bat_df = bat_df.reset_index(drop=True)

        # Séparer logements et commerces
        for usage_type in ["logements", "commerces"]:
            grp_df = bat_df[bat_df["usage"] == usage_type].reset_index(drop=True) \
                if "usage" in bat_df.columns \
                else bat_df.reset_index(drop=True)
            if grp_df.empty:
                continue

            # Étape 1 — Greedy séquentiel par capacity
            groups = [grp_df.iloc[i:i + capacity] for i in range(0, len(grp_df), capacity)]

            for cl_idx, group in enumerate(groups):
                if group.empty:
                    continue

                sub_lats = group["lat_abonne"].values
                sub_lons = group["lon_abonne"].values
                sub_etages = group["etage"].values if "etage" in group.columns else np.zeros(len(group))

                # Étape 2 — FAT à la médiane analytique des étages
                etage_fat = int(np.median(sub_etages))
                fat_lat = float(np.mean(sub_lats))
                fat_lon = float(np.mean(sub_lons))

                # Étape 3 — Distance physique par abonné
                hauteur_etage = 3.0  # valeur par défaut si non fournie
                if "Hauteur par étage (m)" in group.columns:
                    hauteur_etage = float(group["Hauteur par étage (m)"].iloc[0])

                distances_real = []
                for _, row in group.iterrows():
                    ab_lat = float(row["lat_abonne"])
                    ab_lon = float(row["lon_abonne"])
                    et_ab = float(row.get("etage", 0))

                    R = 6_371_000.0
                    la1, lo1 = math.radians(ab_lat), math.radians(ab_lon)
                    la2, lo2 = math.radians(fat_lat), math.radians(fat_lon)
                    a = (math.sin((la2 - la1) / 2) ** 2
                         + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2)
                    dist_h = R * 2 * math.asin(math.sqrt(max(0.0, a)))

                    dist_v = abs(et_ab - etage_fat) * hauteur_etage
                    dist_real = round(dist_v + dist_h + palier, 2)
                    distances_real.append(dist_real)

                dist_moy = round(float(np.mean(distances_real)), 2)

                # Étape 4 — Snap câble déterministe
                cable_snap = _snap_cable(dist_moy, cables)

                fat_id = f"FAT-{str(bat_id)[-6:]}-{cl_idx + 1:02d}"

                output_rows.append({
                    "id_batiment": str(bat_id),
                    "id_zone": zone_id,
                    "fat_id": fat_id,
                    "cluster_label": cl_idx,
                    "centroid_lat": fat_lat,
                    "centroid_lon": fat_lon,
                    "etage_fat": etage_fat,
                    "n_subscribers": len(group),
                    "usage": usage_type,
                    "fdt_assigned": fdt_nom,
                    "capacity_ok": bool(len(group) <= capacity),
                    "cable_m_to_fdt_real": dist_moy,
                    "cable_snap_m": cable_snap,
                    "radius_deg": 0.0,
                    "subscriber_ids": group["code_client"].tolist()
                    if "code_client" in group.columns else [],
                })

    return output_rows


@asynccontextmanager
async def lifespan(app: FastAPI):
    global k_predictor_bundle, snap_rules_bundle
    print("🔄 Démarrage - Chargement des modèles FAT Planner Hybride...")

    # ── Nettoyage des caches vides au démarrage ─────────────────────────────
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
        print(f"🧹 {purged} cache(s) vide(s) ou corrompu(s) supprimé(s) au démarrage")

    # ── K-Predictor (fat_planner_hybride P5) ─────────────────────────────────
    if K_PREDICTOR_PATH.exists():
        try:
            k_predictor_bundle = joblib.load(K_PREDICTOR_PATH)
            m = k_predictor_bundle["metrics"]
            print(f"✅ K-Predictor chargé — R²={m['R2_pct']}%  MAE={m['MAE_fats']} FATs  Acc@1={m['Accuracy_1fat']}%")
        except Exception as e:
            print(f"❌ Erreur chargement K-Predictor : {e}")
    else:
        print(f"⚠️  K-Predictor non trouvé : {K_PREDICTOR_PATH}")
        print(f"   → Lancez fat_planner_hybride.py pour générer les modèles")

    # ── Snap rules ────────────────────────────────────────────────────────────
    if SNAP_RULES_PATH.exists():
        try:
            snap_rules_bundle = joblib.load(SNAP_RULES_PATH)
            print(f"✅ Snap rules chargées — câbles {snap_rules_bundle['prefab_cables']}m")
        except Exception as e:
            print(f"❌ Erreur chargement Snap rules : {e}")
    else:
        print(f"⚠️  Snap rules non trouvées : {SNAP_RULES_PATH} — utilisation des valeurs par défaut")

    yield
    print("🛑 Arrêt de l'application")


app = FastAPI(
    title="FTTH Smart Planner API - Algérie Télécom",
    version="5.1",
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

# Cache résidences sur disque — expire après 24h
RESIDENCE_CACHE_DIR = Path("residence_cache")
RESIDENCE_CACHE_DIR.mkdir(exist_ok=True)
RESIDENCE_CACHE_TTL_SECONDS = 86400  # 24 heures

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


# ====================== CACHE RÉSIDENCES DISQUE ======================
# Leçon : le cache disque est l'optimisation la plus importante pour l'UX.
# La 1ère visite d'une commune coûte 5-15s (Overpass).
# Toutes les visites suivantes = lecture fichier JSON = < 50ms.
# TTL de 24h : les données OSM ne changent pas à la minute.

def _residence_cache_key(commune: str, wilaya_name: str) -> str:
    """
    Génère une clé de cache stable pour une commune donnée.
    On utilise un hash MD5 tronqué pour éviter les problèmes
    de caractères spéciaux (accents, apostrophes) dans les noms de fichiers.
    """
    raw = f"{wilaya_name.lower().strip()}::{commune.lower().strip()}"
    h = hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]
    # On garde aussi un nom lisible pour le debug
    safe = re.sub(r"[^\w]", "_", commune)[:20]
    return f"{safe}_{h}"


def _load_residence_cache(commune: str, wilaya_name: str) -> list | None:
    """
    Charge les résidences depuis le cache disque si le fichier existe
    et n'a pas expiré (TTL = 24h).

    Retourne None si cache absent ou expiré → déclenche fetch Overpass.
    """
    key = _residence_cache_key(commune, wilaya_name)
    path = RESIDENCE_CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None

    # Vérification TTL : on compare mtime du fichier à maintenant
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > RESIDENCE_CACHE_TTL_SECONDS:
        print(f"⏰ Cache expiré ({age_seconds / 3600:.1f}h) pour {commune} — refresh Overpass")
        path.unlink(missing_ok=True)
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"⚡ Cache disque HIT : {len(data)} résidences pour {commune} ({age_seconds / 60:.0f}min)")
        return data
    except Exception as e:
        print(f"⚠️ Cache corrompu pour {commune}: {e}")
        return None


def _save_residence_cache(commune: str, wilaya_name: str, residences: list) -> None:
    """Persiste les résidences sur disque pour les requêtes futures."""
    key = _residence_cache_key(commune, wilaya_name)
    path = RESIDENCE_CACHE_DIR / f"{key}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(residences, f, ensure_ascii=False)
        print(f"💾 Cache disque sauvegardé : {len(residences)} résidences → {path.name}")
    except Exception as e:
        print(f"⚠️ Impossible de sauvegarder le cache: {e}")


# ====================== OVERPASS ======================
OVERPASS_SERVERS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://api.openstreetmap.fr/oapi/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter"
]


async def _overpass_request(query: str, timeout: int = 60, retries: int = 3) -> dict:
    for attempt in range(retries):
        if attempt > 0:
            wait_time = 3 * attempt  # 3s, puis 6s
            print(f"⏳ Attente de {wait_time}s avant nouvelle tentative...")
            await asyncio.sleep(wait_time)

        print(f"📡 Tentative Overpass {attempt + 1}/{retries}...")

        async def _try(url: str, jitter: float = 0.0):
            try:
                await asyncio.sleep(jitter)  # décale les requêtes pour éviter le burst
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
                print(f"⚠️ Erreur {url}: {type(e).__name__}")
            return None

        # On envoie les requêtes avec un jitter progressif (0s, 0.5s, 1s, 1.5s, 2s)
        # pour ne pas taper tous les serveurs au même instant → évite les 429 groupés
        tasks = [
            asyncio.create_task(_try(url, jitter=i * 0.5))
            for i, url in enumerate(OVERPASS_SERVERS)
        ]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                for t in tasks: t.cancel()
                return result

        for t in tasks: t.cancel()

    return {"elements": []}


# ====================== CLASSIFICATION RÉSIDENTIELLE ======================
# Leçon sur les frozenset vs list :
# frozenset → O(1) pour le test "x in S" (table de hachage)
# list      → O(n) pour le test "x in L" (scan linéaire)
# Pour 2000 bâtiments × N tags, ça compte.

# Tags OSM → bâtiment résidentiel CONFIRMÉ
_RESIDENTIAL_TAGS = frozenset({
    "apartments", "residential", "house", "detached", "semidetached_house",
    "terrace", "dormitory", "bungalow", "block", "flat",
    # "yes" = tag générique → ambigu, on garde (vaut mieux un faux positif)
    "yes",
})

# Tags OSM → bâtiment NON résidentiel CONFIRMÉ → à exclure
_NON_RESIDENTIAL_TAGS = frozenset({
    "mosque", "church", "cathedral", "temple", "synagogue", "chapel",
    "school", "university", "college", "kindergarten",
    "hospital", "clinic", "pharmacy", "doctors",
    "industrial", "warehouse", "factory", "storage", "manufacture",
    "retail", "supermarket", "mall", "kiosk", "shop", "commercial",
    "office", "government", "civic", "public",
    "stadium", "sports_hall", "grandstand", "sports_centre",
    "garage", "garages", "parking",
    "hotel", "hostel", "motel",
    "train_station", "bus_station", "terminal", "transportation",
    "power", "transformer_tower", "substation",
    "barn", "farm_auxiliary", "greenhouse",
    "construction",  # en construction → pas encore habitable
})

# Tags amenity= → signal fort NON résidentiel
_NON_RESIDENTIAL_AMENITY = frozenset({
    "place_of_worship", "school", "hospital", "clinic", "pharmacy",
    "university", "college", "police", "fire_station", "post_office",
    "bank", "marketplace", "fuel", "bus_station",
})

# Mots-clés dans le nom → CONFIRME caractère résidentiel
# Pourquoi tuple et pas frozenset ici ?
# → On itère séquentiellement avec `any()` et `break` implicite
# → L'ordre importe (on peut mettre les plus fréquents en premier)
_RESIDENTIAL_NAME_KW = (
    "résidence", "residence", "cité", "cite", "logements", "logement",
    "aadl", "opgi", "lpa", "enpi", "cnep",
    "bloc", "tour", "immeuble", "ilot",
    "villa", "appartement", "lotissement", "habitat",
    "haouch",  # termes arabes courants
)

# Mots-clés dans le nom → CONFIRME caractère NON résidentiel
_NON_RESIDENTIAL_NAME_KW = (
    "mosquée", "mosque", "masjid", "جامع",
    "église", "eglise",
    "école", "ecole", "lycée", "lycee", "cem ", "primaire", "secondaire",
    "hôpital", "hopital", "clinique", "pharmacie", "dispensaire",
    "mairie", " apc ", "daïra", "daira", "wilaya",
    "stade", "salle de sport", "piscine",
    "marché", "marche", "souk", "centre commercial",
    "gare", "aéroport", "aeroport",
    "caserne", "brigade", "commissariat",
)


# ====================== REGROUPEMENT EN BLOCS ======================

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en mètres entre deux points (Haversine). Précis pour < 500m."""
    R = 6_371_000
    dlat = (lat2 - lat1) * math.pi / 180
    dlon = (lon2 - lon1) * math.pi / 180
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * math.pi / 180) * math.cos(lat2 * math.pi / 180) * math.sin(
        dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bloc_letter(idx: int) -> str:
    """Convertit un index (0-based) en lettre(s) : 0→A, 25→Z, 26→AA…"""
    if idx < 26:
        return chr(65 + idx)
    return chr(65 + idx // 26 - 1) + chr(65 + idx % 26)


def _compute_blocs(unnamed: list, commune: str) -> list:
    """
    Regroupe les bâtiments sans nom officiel par proximité géographique (rayon ≤ 100m).
    Algorithme : Union-Find (DSU) — O(n²) en temps mais n ≤ 2000 en pratique.

    Retourne une liste de dicts représentant les BLOCS (pas les bâtiments individuels) :
    [
      {
        "name": "bir_el_djir-blocA",          ← affiché dans la liste
        "bloc_letter": "A",
        "osm_id": "<id du 1er bâtiment du bloc>",  ← pour la sélection
        "lat": ..., "lon": ...,               ← centroïde du bloc
        "buildings": [                         ← tous les bâtiments du bloc
          {"osm_id":..., "lat":..., "lon":..., "name":"bir_el_djir-blocA-numero1", ...}
        ],
        "count": 12,
        "has_official_name": False,
        "is_bloc": True,
        "type": "bloc",
        "operator": None,
        "levels": None, "units": None,
      },
      ...
    ]
    """
    import math as _math

    n = len(unnamed)
    if n == 0:
        return []

    # ── Union-Find ────────────────────────────────────────────────────────────
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    # ── PHASE 2 : Regroupement Spatial (Optimisé via Grille/Hashing) ──────────
    # Au lieu d'un O(n²) qui explose sur 15000 bâtiments, on utilise une grille.
    cell_size = 0.001  # ~110m : rayon de recherche sûr pour grouper à 100m
    grid: dict[tuple[int, int], list[int]] = {}

    for i in range(n):
        cx = int(unnamed[i]["lat"] / cell_size)
        cy = int(unnamed[i]["lon"] / cell_size)
        grid.setdefault((cx, cy), []).append(i)

    # On ne compare chaque bâtiment qu'avec ceux de sa cellule et des 8 voisines
    for (cx, cy), current_indices in grid.items():
        # Cellules à vérifier : (cx, cy) et ses voisines
        for dx in range(-1, 2):
            for dy in range(-1, 2):
                neighbor_key = (cx + dx, cy + dy)
                if neighbor_key not in grid:
                    continue

                neighbor_indices = grid[neighbor_key]
                for i in current_indices:
                    for j in neighbor_indices:
                        if i < j:  # Evite double comparaison et i == j
                            # Pré-calcul rapide (bounding box) avant Haversine
                            if abs(unnamed[i]["lat"] - unnamed[j]["lat"]) < 0.001 and \
                                    abs(unnamed[i]["lon"] - unnamed[j]["lon"]) < 0.001:

                                dist = _haversine_m(unnamed[i]["lat"], unnamed[i]["lon"],
                                                    unnamed[j]["lat"], unnamed[j]["lon"])
                                if dist <= 100:
                                    union(i, j)

    # ── Collecte des groupes ──────────────────────────────────────────────────
    groups: dict[int, list[int]] = {}
    for i in range(n):
        root = find(i)
        groups.setdefault(root, []).append(i)

    # ── Tri des groupes par latitude décroissante du centroïde (nord → sud) ──
    def group_centroid_lat(indices):
        return sum(unnamed[i]["lat"] for i in indices) / len(indices)

    sorted_groups = sorted(groups.values(), key=group_centroid_lat, reverse=True)

    # ── Nom de commune normalisé pour le label ────────────────────────────────
    commune_slug = (commune or "commune") \
        .lower() \
        .replace(" ", "_") \
        .replace("-", "_") \
        .replace("'", "") \
        .replace("ï", "i") \
        .replace("é", "e") \
        .replace("è", "e") \
        .replace("ê", "e") \
        .replace("â", "a") \
        .replace("î", "i") \
        .replace("ô", "o") \
        .replace("û", "u")

    blocs = []
    for group_idx, indices in enumerate(sorted_groups):
        letter = _bloc_letter(group_idx)
        bloc_key = f"{commune_slug}-bloc {letter}"

        # Tri des bâtiments du groupe ouest → est (longitude croissante)
        sorted_indices = sorted(indices, key=lambda i: unnamed[i]["lon"])

        buildings_in_bloc = []
        for num, orig_idx in enumerate(sorted_indices, start=1):
            b = unnamed[orig_idx]
            buildings_in_bloc.append({
                **b,
                "name": f"{bloc_key}-numero{num}",  # label carte
                "bloc_letter": letter,
                "bloc_numero": num,
                "bloc_key": bloc_key,
            })

        # Centroïde du bloc
        clat = sum(unnamed[i]["lat"] for i in indices) / len(indices)
        clon = sum(unnamed[i]["lon"] for i in indices) / len(indices)

        # On prend l'osm_id du bâtiment le plus à l'ouest comme représentant
        repr_building = buildings_in_bloc[0]

        blocs.append({
            "name": bloc_key,  # affiché dans la liste
            "osm_id": repr_building["osm_id"],  # pour la sélection
            "lat": round(clat, 6),
            "lon": round(clon, 6),
            "levels": None,
            "units": None,
            "type": "bloc",
            "operator": None,
            "has_official_name": False,
            "is_bloc": True,
            "is_apartment_block": True,
            "bloc_letter": letter,
            "count": len(indices),
            "buildings": buildings_in_bloc,  # liste complète pour la carte
        })

    return blocs


def _classify_building(tags: dict) -> tuple[bool, str]:
    """
    Classifie un élément OSM comme résidentiel ou non via un système de score.

    Retourne (is_residential: bool, reason: str)

    Architecture du score :
    ┌─────────────────────────────────────────────┬───────┐
    │ Condition                                   │ Score │
    ├─────────────────────────────────────────────┼───────┤
    │ amenity= non résidentiel (mosquée, école..) │  -10  │ ← veto immédiat
    │ building= non résidentiel                   │   -3  │
    │ nom contient mot-clé non résidentiel        │   -2  │
    │ building= résidentiel confirmé              │  +2   │
    │ nom contient mot-clé résidentiel            │  +1   │
    └─────────────────────────────────────────────┴───────┘

    Seuil de décision : score >= -1 → résidentiel (inclusif par défaut)

    Pourquoi inclusif par défaut ?
    En Algérie, ~60% des bâtiments ont building=yes (générique).
    Si on est strict, on perd ces 60%. On préfère quelques faux positifs
    (ex: un petit commerce tagué "yes") plutôt que rater des résidences.
    """
    building_tag = tags.get("building", "").lower().strip()
    amenity_tag = tags.get("amenity", "").lower().strip()
    landuse_tag = tags.get("landuse", "").lower().strip()

    # Concaténation des champs textuels pertinents pour la recherche de mots-clés
    # On met tout en minuscules une seule fois (pas à chaque comparaison)
    name_text = " ".join(filter(None, [
        tags.get("name", ""),
        tags.get("addr:housename", ""),
        tags.get("operator", ""),
        tags.get("description", ""),
    ])).lower()

    score = 0

    # ── Veto immédiat : amenity (signal le plus fiable d'OSM) ──────────────────
    # Les mosquées ont TOUJOURS amenity=place_of_worship
    # Les écoles ont TOUJOURS amenity=school
    # Ce tag est plus fiable que building= car les contributeurs OSM
    # oublient souvent de tagger building=mosque mais mettent amenity=
    if amenity_tag in _NON_RESIDENTIAL_AMENITY:
        return False, f"amenity={amenity_tag} [veto]"

    # ── Landuse résidentiel → signal fort positif ──────────────────────────────
    if landuse_tag == "residential":
        score += 3

    # ── Tag building= ─────────────────────────────────────────────────────────
    if building_tag in _NON_RESIDENTIAL_TAGS:
        score -= 3
    elif building_tag in _RESIDENTIAL_TAGS:
        score += 2
    # building="" ou valeur inconnue → score neutre (0)

    # ── Analyse du nom ────────────────────────────────────────────────────────
    # On utilise any() qui court-circuite dès le premier match → rapide
    if any(kw in name_text for kw in _NON_RESIDENTIAL_NAME_KW):
        score -= 2

    if any(kw in name_text for kw in _RESIDENTIAL_NAME_KW):
        score += 1

    is_residential = score >= -1
    return is_residential, f"building={building_tag or 'N/A'}, score={score}"


def _build_display_name(tags: dict, osm_id: str, index: int) -> str:
    """
    Génère le nom affiché à l'utilisateur en cascade de 6 niveaux.

    Niveau 1 → Nom officiel OSM         : "Résidence El Feth"
    Niveau 2 → Nom de maison (housename): "Bloc C AADL"
    Niveau 3 → Adresse reconstruite     : "N°12 Rue Larbi Ben M'hidi"
    Niveau 4 → Opérateur seul           : "AADL" (sans les niveaux pour éviter "Bât. 4 niv.")
    Niveau 5 → Ref OSM                  : "Bât. REF-A3"
    Niveau 6 → Code géo court           : "Bât. OSM-45678"

    NB v6 : On supprime le niveau intermédiaire "Bât. X niv." qui générait
    des noms peu lisibles dans la liste. Les bâtiments sans nom seront
    groupés en blocs nommés côté frontend ({commune}-blocX-numeroN).
    """
    name = tags.get("name", "").strip()
    housename = tags.get("addr:housename", "").strip()
    addr_nb = tags.get("addr:housenumber", "").strip()
    addr_st = tags.get("addr:street", "").strip()
    ref = tags.get("ref", "").strip()
    operator = tags.get("operator", "").strip().upper()

    # Niveau 1 & 2
    if name:      return name
    if housename: return housename

    # Niveau 3 : adresse
    if addr_nb and addr_st: return f"N°{addr_nb} {addr_st}"
    if addr_nb:             return f"Bât. N°{addr_nb}"

    # Niveau 4 : opérateur seul (SANS les niveaux — évite "Bât. 4 niv.")
    for op_keyword in ("AADL", "OPGI", "LPA", "ENPI", "CNEP"):
        if op_keyword in operator:
            return f"{op_keyword} {ref}".strip() if ref else op_keyword

    # Niveau 5 : ref seul
    if ref:
        return f"Bât. {ref}"

    # Niveau 6 : code OSM court (5 derniers chiffres = lisible, stable)


#   short_id = str(osm_id)[-5:] if osm_id else str(index)
#  return f"Bât. OSM-{short_id}"


def _detect_operator_badge(tags: dict) -> str | None:
    """
    Détecte AADL / OPGI / LPA / ENPI pour afficher un badge dans le frontend.
    Cherche dans operator=, name=, et addr:housename=
    """
    combined = " ".join(filter(None, [
        tags.get("operator", ""),
        tags.get("name", ""),
        tags.get("addr:housename", ""),
    ])).upper()

    for op in ("AADL", "OPGI", "LPA", "ENPI", "CNEP"):
        if op in combined:
            return op
    return None


def _get_building_type_label(tags: dict, operator_badge: str | None) -> str:
    """
    Retourne le label de type affiché dans la liste résidences.
    Ex: "Immeuble", "Maison", "AADL", "Résidence"
    """
    bt = tags.get("building", "").lower()

    if bt in ("apartments", "flat", "block"):  return "Immeuble"
    if bt in ("house", "detached", "bungalow"): return "Maison"
    if bt in ("semidetached_house", "terrace"): return "Maison jumelée"
    if bt == "dormitory":                       return "Résidence"
    if bt == "residential":                     return "Résidence"
    if operator_badge:                          return operator_badge
    return "Bâtiment"


# ====================== FETCH RÉSIDENCES (COEUR DU SYSTÈME) ======================

async def fetch_residences_in_commune(
        commune: str,
        wilaya_name: str,
        lat_fallback: float = None,
        lon_fallback: float = None
) -> List[Dict[str, Any]]:
    """
    Récupère tous les bâtiments résidentiels d'une commune.

    Architecture en 3 phases :
    ┌──────────────────────────────────────────────────────────┐
    │ Phase 0 : Cache disque                                   │
    │   → Si données < 24h : retour immédiat (< 50ms)         │
    │   → Sinon : continue vers Phase 1                        │
    ├──────────────────────────────────────────────────────────┤
    │ Phase 1 : Fetch Overpass LARGE (sans filtre name)        │
    │   → 1A : Bounding box (rapide, ~3-8s) si coords dispo   │
    │   → 1B : Zone admin (précis, ~8-20s) en fallback        │
    │   NB: "out tags center" = pas de géométrie complète     │
    │       → payload 10x plus petit → bien plus rapide       │
    ├──────────────────────────────────────────────────────────┤
    │ Phase 2 : Filtrage Python par score de tags              │
    │   → Classification résidentielle (O(n) tags)             │
    │   → Déduplication par osm_id                            │
    │   → Génération noms en cascade                          │
    ├──────────────────────────────────────────────────────────┤
    │ Phase 3 : Sauvegarde cache + tri + retour               │
    └──────────────────────────────────────────────────────────┘
    """
    wilaya_pure = wilaya_name.split(" - ")[1] if " - " in wilaya_name else wilaya_name

    # ── PHASE 0 : Cache disque ────────────────────────────────────────────────
    # On ne retourne le cache que s'il contient au moins 1 résidence.
    # Un cache vide (0 éléments) est considéré comme invalide → on re-fetch.
    cached = _load_residence_cache(commune, wilaya_pure)
    if cached is not None and len(cached) > 0:
        return cached

    # ── PHASE 1A : Query Overpass par bounding box (prioritaire si coords) ────
    # margin = 0.05° ≈ 5.5km — couvre les grandes communes comme Bir El Djir.
    # L'ancienne valeur (0.022°) était trop petite et ratait les bâtiments
    # en périphérie des communes étendues.
    elements = []

    if lat_fallback and lon_fallback:
        for margin in (0.05, 0.10):  # 2 tentatives : ~5.5km puis ~11km
            s = lat_fallback - margin
            w = lon_fallback - margin
            n = lat_fallback + margin
            e = lon_fallback + margin

            query_bbox = f"""
[out:json][timeout:60];
(
  way["building"]({s},{w},{n},{e});
  relation["building"]["type"="multipolygon"]({s},{w},{n},{e});
);
out tags center;
"""
            t0 = time.time()
            data = await _overpass_request(query_bbox, timeout=60, retries=3)
            elements = data.get("elements", [])
            print(f"📦 Phase 1A (bbox ±{margin}°): {len(elements)} éléments en {time.time() - t0:.1f}s")
            if elements:
                break  # On a des résultats → on arrête d'élargir

    # ── PHASE 1B : Fallback par zone administrative ────────────────────────────
    # Utilisé uniquement si la bbox n'a rien retourné (commune sans coords,
    # ou Overpass rate-limitée sur tous les serveurs).
    if not elements:
        # admin_level=4 en Algérie = wilaya
        # admin_level=8 ou 9 = commune (varie selon la source OSM)
        # On essaie les deux pour maximiser la chance de match
        query_area = f"""
[out:json][timeout:90];
area["name"~"^{wilaya_pure}$",i]["admin_level"~"4|6"]->.w;
(
  area["name"~"^{commune}$",i]["admin_level"~"8|9|10"](area.w);
  area["name"~"^{commune}$",i](area.w);
)->.searchArea;
(
  way["building"](area.searchArea);
  relation["building"]["type"="multipolygon"](area.searchArea);
);
out tags center;
"""
        t0 = time.time()
        data = await _overpass_request(query_area, timeout=90, retries=3)
        elements = data.get("elements", [])
        print(f"📦 Phase 1B (admin area): {len(elements)} éléments en {time.time() - t0:.1f}s")

    # ── PHASE 2 : Filtrage et transformation Python ────────────────────────────
    named_residences = []  # bâtiments avec nom officiel OSM
    unnamed_residences = []  # bâtiments sans nom → seront groupés en blocs
    seen_ids = set()
    stats = {"total": len(elements), "excluded": 0, "no_coords": 0}

    for el in elements:
        osm_id = str(el.get("id", ""))

        if osm_id in seen_ids:
            continue
        seen_ids.add(osm_id)

        tags = el.get("tags", {})

        # ── Filtrage résidentiel ──────────────────────────────────────────────
        is_residential, reason = _classify_building(tags)
        if not is_residential:
            stats["excluded"] += 1
            continue

        # ── Coordonnées ──────────────────────────────────────────────────────
        center = el.get("center") or {}
        lat = center.get("lat") or el.get("lat")
        lon = center.get("lon") or el.get("lon")
        if not lat or not lon:
            stats["no_coords"] += 1
            continue

        # ── Métadonnées ───────────────────────────────────────────────────────
        levels_raw = tags.get("building:levels", "")
        units_raw = tags.get("building:units", "") or tags.get("residential:units", "")
        operator_badge = _detect_operator_badge(tags)
        type_label = _get_building_type_label(tags, operator_badge)
        has_official = bool(tags.get("name") or tags.get("addr:housename"))
        bt = tags.get("building", "").lower()
        is_apt_block = bt in ("apartments", "flat", "block", "residential", "dormitory") or bool(operator_badge)

        display_name = _build_display_name(tags, osm_id, len(named_residences) + len(unnamed_residences) + 1)

        entry = {
            "name": display_name,
            "osm_id": osm_id,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "levels": int(levels_raw) if str(levels_raw).isdigit() else None,
            "units": int(units_raw) if str(units_raw).isdigit() else None,
            "type": type_label,
            "operator": operator_badge,
            "has_official_name": has_official,
            "is_apartment_block": is_apt_block,
        }

        if has_official:
            named_residences.append(entry)
        else:
            unnamed_residences.append(entry)

    print(
        f"✅ Résultat brut : {len(named_residences)} nommés | "
        f"{len(unnamed_residences)} sans nom | "
        f"{stats['excluded']} exclus | {stats['no_coords']} sans coordonnées"
    )

    # ── PHASE 3 : Regroupement des sans-nom en blocs géographiques ────────────
    # Les bâtiments sans nom sont groupés par proximité (≤100m) → un seul
    # représentant par bloc dans la liste (ex: "bir_el_djir-blocA").
    # Chaque bloc contient la liste complète de ses bâtiments pour la carte.
    blocs = _compute_blocs(unnamed_residences, commune)
    print(f"📦 {len(unnamed_residences)} bâtiments sans nom → {len(blocs)} blocs")

    # ── PHASE 4 : Tri + assemblage final ──────────────────────────────────────
    # Résidences nommées triées alphabétiquement en premier,
    # puis les blocs triés par lettre (A, B, C…).
    named_sorted = sorted(named_residences, key=lambda x: x["name"].lower())
    blocs_sorted = sorted(blocs, key=lambda x: x["name"].lower())

    result = named_sorted + blocs_sorted  # nommés d'abord, blocs ensuite
    result = result[:2000]

    if result:
        _save_residence_cache(commune, wilaya_pure, result)
    else:
        print(f"⚠️ Aucune résidence trouvée pour {commune} — cache NON sauvegardé")

    return result


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
    hauteur_etage: float = 3.0

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
async def get_residence(
        ville: str = Query(...),
        commune: str = Query(...),
        search: str = Query(None)
):
    print(f"GET /api/residence (ville={ville}, commune={commune}, search={search})")
    ville_pure = ville.split(" - ")[1] if " - " in ville else ville

    lat_f, lon_f = None, None
    wilaya_data = _load_wilaya_cache(ville)
    if wilaya_data and wilaya_data.get("communes"):
        for c in wilaya_data["communes"]:
            if c["nom"] == commune:
                lat_f = c.get("lat")
                lon_f = c.get("lon")
                break

    residences = await fetch_residences_in_commune(commune, ville_pure, lat_f, lon_f)

    # ── Filtrage par recherche textuelle ──────────────────────────────────────
    # Fonctionne sur les nommés (name, type, operator) ET les blocs (name = "commune-blocA")
    if search and search.strip():
        q = search.strip().lower()
        residences = [
            r for r in residences
            if q in r["name"].lower()
               or q in (r.get("type") or "").lower()
               or q in (r.get("operator") or "").lower()
        ]

    n_named = sum(1 for r in residences if r.get("has_official_name"))
    n_blocs = sum(1 for r in residences if r.get("is_bloc"))

    print(f"✅ Résidences retournées: {len(residences)} ({n_named} nommées + {n_blocs} blocs)")
    return {
        "residences": residences,
        "source": "overpass_live",
        "count": len(residences),
        "commune": commune,
        "stats": {
            "named": n_named,
            "blocs": n_blocs,
            # rétro-compatibilité
            "with_official_name": n_named,
            "without_official_name": n_blocs,
        }
    }


@app.delete("/api/residence/cache")
async def clear_residence_cache(commune: str = Query(None)):
    """
    Endpoint utilitaire pour vider le cache résidences.
    Utile quand les données OSM ont été mises à jour et qu'on veut forcer un refresh.

    Usage : DELETE /api/residence/cache?commune=Bir+El+Djir
            DELETE /api/residence/cache  (vide tout le cache)
    """
    if commune:
        deleted = 0
        for f in RESIDENCE_CACHE_DIR.glob("*.json"):
            if commune.lower().replace(" ", "_")[:5] in f.name.lower():
                f.unlink()
                deleted += 1
        return {"message": f"{deleted} fichier(s) cache supprimé(s) pour {commune}"}
    else:
        count = len(list(RESIDENCE_CACHE_DIR.glob("*.json")))
        for f in RESIDENCE_CACHE_DIR.glob("*.json"):
            f.unlink()
        return {"message": f"Cache complet vidé ({count} fichiers)"}


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
            gdf = ox.features_from_point((req.lat, req.lon), dist=100, tags={"building": True})
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
        names_list = []
        levels_list = []
        units_list = []
        lat_list = []
        lon_list = []

        for i, row in gdf.iterrows():
            is_target = (i == target_idx)
            is_target_list.append(is_target)

            real_name = None
            if "name" in gdf.columns and pd.notna(row.get("name")):
                real_name = row.get("name")
            elif "addr:housename" in gdf.columns and pd.notna(row.get("addr:housename")):
                real_name = row.get("addr:housename")

            bat_name = req.residence if is_target else (real_name if real_name else f"Bloc A-numero {i + 1}")
            names_list.append(bat_name)

            internal_id = f"CIBLE-{req.residence[:15].upper()}" if is_target else f"BLOC-A-N{i + 1}"
            formatted_ids.append(internal_id)

            lvl = row.get("building:levels") if "building:levels" in gdf.columns else None
            levels_list.append(int(lvl) if pd.notna(lvl) else None)

            unt = row.get("building:units") if "building:units" in gdf.columns else None
            units_list.append(int(unt) if pd.notna(unt) else None)

            centroid = row.geometry.centroid
            lat_list.append(centroid.y)
            lon_list.append(centroid.x)

        gdf["id_batiment"] = formatted_ids
        gdf["is_target"] = is_target_list
        gdf["nom_batiment"] = names_list
        gdf["bat_levels"] = levels_list
        gdf["bat_units"] = units_list
        gdf["centroid_lat"] = lat_list
        gdf["centroid_lon"] = lon_list

        target_bldg = gdf.iloc[target_idx]
        osm_levels = target_bldg["bat_levels"]
        osm_units = target_bldg["bat_units"]

        etages = int(osm_levels) if pd.notna(osm_levels) else req.nombre_etages
        if pd.notna(osm_units) and int(osm_units) > 0:
            logements = max(1, int(osm_units) // max(1, etages))
        else:
            logements = req.logements_par_etage

        if etages <= 0: etages = 1
        if logements <= 0: logements = 1

        gdf.loc[target_idx, "bat_levels"] = etages
        gdf.loc[target_idx, "bat_units"] = etages * logements

        # --- DISTRIBUTION LINÉAIRE (Ligne droite) & PORTE CONTINUE ---
        m = logements
        surface_deg2 = target_bldg.geometry.area
        surface_m2 = surface_deg2 * (111000 ** 2)
        taille_m2 = surface_m2 / m
        espacement_m = np.sqrt(taille_m2)
        espacement_deg = espacement_m / 111000
        centroid = target_bldg.geometry.centroid

        base_points = []
        start_lon = centroid.x - (m - 1) * espacement_deg / 2
        for i in range(m):
            base_points.append(Point(start_lon + i * espacement_deg, centroid.y))

        rows = []
        cc_counter = 1
        global_porte_idx = 1

        # --- RDC COMMERCIAL (étage 0) si commerce activé ---
        if req.commerce:
            for log_idx in range(logements):
                pt = base_points[log_idx]
                rows.append({
                    "code_client": f"AB{cc_counter:06d}",
                    "id_batiment": target_bldg["id_batiment"],
                    "lat_abonne": round(pt.y, 6),
                    "lon_abonne": round(pt.x, 6),
                    "etage": 0,
                    "porte": global_porte_idx,
                    "usage": "commerces",
                })
                cc_counter += 1
                global_porte_idx += 1

        # --- ÉTAGES RÉSIDENTIELS (étage 1+) ---
        for etg_idx in range(etages):
            etg = etg_idx + 1
            for log_idx in range(logements):
                # Réutilisation des points de base pour l'alignement vertical
                pt = base_points[log_idx]
                rows.append({
                    "code_client": f"AB{cc_counter:06d}",
                    "id_batiment": target_bldg["id_batiment"],
                    "lat_abonne": round(pt.y, 6),
                    "lon_abonne": round(pt.x, 6),
                    "etage": etg,
                    "porte": global_porte_idx,
                    "usage": "logements",
                })
                cc_counter += 1
                global_porte_idx += 1

        cols_to_keep = ["id_batiment", "is_target", "nom_batiment", "bat_levels", "bat_units", "centroid_lat",
                        "centroid_lon", "geometry"]
        buildings_geojson = gdf[cols_to_keep].to_json()

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
        raise HTTPException(status_code=400, detail="Liste d'abonnés vide")

    # Pipeline Greedy+Médiane+Snap de fat_planner_hybride.py
    # Utilise k_predictor_bundle et snap_rules_bundle chargés au démarrage.
    try:
        output_rows = _run_hybride_pipeline(df_sub, zone_id="Z310-001", fdt_nom="F310-001-01")
    except Exception as e:
        print(f"⚠️ Erreur pipeline hybride: {e}, fallback K-means...")
        output_rows = _fallback_clustering(df_sub)

    if not output_rows:
        output_rows = _fallback_clustering(df_sub)

    return {"fat_candidates": output_rows}


@app.post("/api/nomFAT")
async def generate_noms_fat(req: NamingFATRequest):
    print(f"POST /api/nomFAT ({len(req.fat_candidates)} candidats)")
    df_cands = pd.DataFrame(req.fat_candidates)
    df_subs = pd.DataFrame(req.subscribers)
    if df_cands.empty:
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

    df_cands["fat_id_AT"] = ids_at
    return {"fat_candidates_with_ids": df_cands.to_dict(orient="records")}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)