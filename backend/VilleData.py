"""
Le chargement de données dans la section "Localisation" repose désormais sur une architecture simplifiée "Wilaya → Commune → Résidence".
Cette structure remplace l'ancien système de quartiers par un cache direct Wilaya → Communes pour plus de performance.

Voici le détail technique du fonctionnement :

1. Niveau Wilaya (Initialisation)
Au chargement de l'application, le frontend appelle l'endpoint /api/ville.
Source : Un fichier statique villes.json (situé dans osm_json_cache/) contenant la liste officielle des 58 wilayas d'Algérie.

2. Niveau Commune (Liaison Hiérarchique)
Dès que vous sélectionnez une Wilaya, le frontend appelle /api/commune?ville=...
Source : Le Cache Unifié. Le backend cherche le fichier correspondant au code (ex: 31-Oran.json).
Logique : Ce fichier contient la liste des communes avec leurs coordonnées GPS pour permettre un ciblage précis.

3. Niveau Résidence (Enrichissement Dynamique)
Lorsque vous sélectionnez une commune, le backend interroge en temps réel l'API Overpass (OpenStreetMap) 
pour récupérer les bâtiments résidentiels dans la zone géographique de la commune.
"""

import asyncio
import json
import argparse
from pathlib import Path
import httpx
import time

# ====================== PATHS ======================
DATA_DIR = Path(__file__).parent / "data"
CACHE_DIR = Path(__file__).parent / "osm_json_cache"
CACHE_DIR.mkdir(exist_ok=True)

WILAYAS_FILE = DATA_DIR / "wilayas.json"
COMMUNES_FILE = DATA_DIR / "communes.json"

# ====================== CHARGEMENT DATASET LOCAL ======================
def load_wilayas() -> list:
    """Charge les 58+ wilayas depuis le dataset local."""
    with open(WILAYAS_FILE, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def load_communes() -> list:
    """Charge les 1541 communes depuis le dataset local."""
    with open(COMMUNES_FILE, "r", encoding="utf-8-sig") as f:
        return json.load(f)

# ====================== MAPPING WILAYAS (58 officielles) ======================
WILAYAS_58 = {
    1: "Adrar", 2: "Chlef", 3: "Laghouat", 4: "Oum El Bouaghi", 5: "Batna",
    6: "Béjaïa", 7: "Biskra", 8: "Béchar", 9: "Blida", 10: "Bouira",
    11: "Tamanrasset", 12: "Tébessa", 13: "Tlemcen", 14: "Tiaret", 15: "Tizi Ouzou",
    16: "Alger", 17: "Djelfa", 18: "Jijel", 19: "Sétif", 20: "Saïda",
    21: "Skikda", 22: "Sidi Bel Abbès", 23: "Annaba", 24: "Guelma", 25: "Constantine",
    26: "Médéa", 27: "Mostaganem", 28: "M'Sila", 29: "Mascara", 30: "Ouargla",
    31: "Oran", 32: "El Bayadh", 33: "Illizi", 34: "Bordj Bou Arréridj", 35: "Boumèrdès",
    36: "El Tarf", 37: "Tindouf", 38: "Tissemsilt", 39: "El Oued", 40: "Khenchela",
    41: "Souk Ahras", 42: "Tipaza", 43: "Mila", 44: "Aïn Defla", 45: "Naâma",
    46: "Aïn Témouchent", 47: "Ghardaïa", 48: "Relizane",
    49: "Timimoun", 50: "Bordj Badji Mokhtar", 51: "Ouled Djellal", 52: "Béni Abbès",
    53: "In Salah", 54: "In Guezzam", 55: "Touggourt", 56: "Djanet",
    57: "El M'Ghair", 58: "El Meniaa"
}

def build_wilaya_id_map(wilayas_data: list) -> dict:
    """Construit un mapping dataset_wilaya_id → code_officiel."""
    name_to_code = {}
    for code, name in WILAYAS_58.items():
        name_to_code[_normalize(name)] = code

    id_map = {}
    for w in wilayas_data:
        dataset_id = str(w["id"])
        dataset_name = w["name"]
        normalized = _normalize(dataset_name)

        if normalized in name_to_code:
            id_map[dataset_id] = name_to_code[normalized]
        else:
            code_val = int(w.get("code", 0))
            id_map[dataset_id] = code_val

    return id_map

def _normalize(name: str) -> str:
    """Normalise un nom pour le matching."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", name)
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    return clean.lower().strip().replace("'", "").replace("'", "").replace("-", " ")

def get_communes_for_wilaya(communes_data: list, wilaya_code: int, id_map: dict) -> list:
    """Retourne les communes d'une wilaya donnée depuis le dataset local."""
    matching_ids = [did for did, code in id_map.items() if code == wilaya_code]

    result = []
    seen = set()
    def _to_float(val):
        try:
            if not val: return 0.0
            # Nettoyage des caractères parasites (espaces, virgules au début, etc.)
            clean_val = str(val).strip().replace(",", ".")
            if clean_val.startswith("."): clean_val = "0" + clean_val
            return float(clean_val)
        except (ValueError, TypeError):
            return 0.0

    for com in communes_data:
        wid = str(com.get("wilaya_id", ""))
        if wid in matching_ids:
            name = com.get("name", "").strip()
            if name and name not in seen:
                seen.add(name)
                result.append({
                    "nom": name,
                    "lat": _to_float(com.get("longitude", 0)),
                    "lon": _to_float(com.get("latitude", 0)),
                    "post_code": com.get("post_code", ""),
                })

    return sorted(result, key=lambda x: x["nom"])

# ====================== GÉNÉRATION DU CACHE ======================
def wilaya_cache_path(code: int, nom: str) -> Path:
    """Path du fichier cache pour une wilaya."""
    safe = nom.replace(" ", "_").replace("'", "").replace("'", "")
    return CACHE_DIR / f"{code:02d}-{safe}.json"

def save_cache(path: Path, data: dict):
    """Sauvegarde un fichier JSON cache."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"   💾 Sauvegardé : {path.name}")

async def generate_wilaya_cache(
    wilaya_code: int,
    wilaya_nom: str,
    communes_data: list,
    id_map: dict,
):
    """Génère le fichier cache JSON pour une wilaya complète."""
    print(f"\n📍 [{wilaya_code:02d}/58] {wilaya_nom}")

    # 1. Récupérer les communes depuis le dataset local
    communes_local = get_communes_for_wilaya(communes_data, wilaya_code, id_map)
    print(f"   📋 {len(communes_local)} communes (dataset local)")

    # 2. Construire la hiérarchie (Wilaya -> Communes)
    communes_result = []
    for com in communes_local:
        communes_result.append({
            "nom": com["nom"],
            "lat": com["lat"],
            "lon": com["lon"]
        })

    # 3. Construire le JSON final
    data = {
        "code": wilaya_code,
        "nom": wilaya_nom,
        "label": f"{wilaya_code:02d} - {wilaya_nom}",
        "communes": communes_result
    }

    # 4. Sauvegarder
    path = wilaya_cache_path(wilaya_code, wilaya_nom)
    save_cache(path, data)

    print(f"   ✅ {len(communes_result)} communes enregistrées")
    return data

async def main(wilaya_filter: int = None):
    """Point d'entrée principal."""
    print("=" * 80)
    print("🚀 VilleData.py — Version optimisée (Wilaya → Commune uniquement)")
    print("=" * 80)

    if not WILAYAS_FILE.exists() or not COMMUNES_FILE.exists():
        print("❌ Fichiers de données manquants dans backend/data/")
        return

    # Charger les données
    print("\n📂 Chargement des données locales...")
    wilayas_data = load_wilayas()
    communes_data = load_communes()
    print(f"   ✅ {len(wilayas_data)} wilayas, {len(communes_data)} communes")

    # Construire le mapping d'IDs
    id_map = build_wilaya_id_map(wilayas_data)

    # Sauvegarder la liste des villes
    villes = [f"{str(code).zfill(2)} - {name}" for code, name in sorted(WILAYAS_58.items())]
    villes_path = CACHE_DIR / "villes.json"
    with open(villes_path, "w", encoding="utf-8") as f:
        json.dump(villes, f, ensure_ascii=False, indent=2)
    print(f"\n✅ {len(villes)} wilayas → villes.json")

    # Déterminer les wilayas à traiter
    if wilaya_filter:
        if wilaya_filter not in WILAYAS_58:
            print(f"❌ Wilaya {wilaya_filter} inconnue")
            return
        to_process = [(wilaya_filter, WILAYAS_58[wilaya_filter])]
    else:
        to_process = list(WILAYAS_58.items())

    # Générer le cache
    for code, nom in to_process:
        await generate_wilaya_cache(
            wilaya_code=code,
            wilaya_nom=nom,
            communes_data=communes_data,
            id_map=id_map
        )

    print("\n" + "=" * 80)
    print("🎉 Cache terminé ! Les communes sont prêtes.")
    print("🚀 Lance maintenant : python app.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère le cache hiérarchique Wilaya → Communes")
    parser.add_argument("--wilaya", type=int, help="Code wilaya (ex: 31 pour Oran)")
    args = parser.parse_args()
    asyncio.run(main(args.wilaya))
