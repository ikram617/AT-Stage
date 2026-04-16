"""

Le chargement de données dans la section "Localisation" repose désormais sur une architecture hybride "Wilaya → Commune → Quartier → Résidence" que nous venons de mettre en place. Cette structure remplace l'ancienne méthode instable par un système de cache hiérarchique robuste.

Voici le détail technique du fonctionnement étape par étape :

1. Niveau Wilaya (Initialisation)
Au chargement de l'application, le frontend appelle l'endpoint /api/ville.

Source : Un fichier statique villes.json (situé dans osm_json_cache/) contenant la liste officielle des 58 wilayas d'Algérie.
Action : Remplit la première liste déroulante.
2. Niveau Commune (Liaison Hiérarchique)
Dès que vous sélectionnez une Wilaya (ex: "31 - Oran"), le frontend appelle /api/commune?ville=31%20-%20Oran.

Source : Le Cache Unifié. Le backend cherche le fichier correspondant au code (ex: 31-Oran.json).
Logique : Ce fichier contient l'arborescence complète de la wilaya. Le backend extrait uniquement la liste des noms des communes pour la deuxième liste déroulante.
3. Niveau Quartier (Filtrage Imbriqué)
Une fois la commune choisie (ex: "Oran"), l'appel /api/quartier?ville=31...&commune=Oran est lancé.

Source : Le même fichier de Cache Unifié (31-Oran.json).
Logique : Au lieu de chercher dans un fichier séparé, le backend parcourt l'objet JSON de la wilaya, trouve l'entrée correspondant à la commune demandée, et renvoie la liste des quartiers qui lui sont rattachés. C'est ce qui garantit qu'un quartier appartient bien à sa commune.
4. Niveau Résidence (Enrichissement Dynamique)
C'est l'étape la plus avancée. Lorsque vous cliquez sur le champ "Résidence", le composant ResidenceSearchSelect s'active.

Appel : /api/residence?ville=...&quartier=...
Source Mixte :
Cache : Le backend regarde si des bâtiments sont déjà listés pour ce quartier dans le cache.
Overpass Live : Si vous faites une recherche, le backend interroge en temps réel l'API Overpass (OpenStreetMap) pour récupérer les polygones réels des bâtiments résidentiels (apartments, residential, house) dans la zone géographique précise du quartier.
Résultat : Vous obtenez une liste de bâtiments réels avec leurs coordonnées GPS, prêts à être importés sur la carte.
Résumé de la Hiérarchie de Données
mermaid
graph TD
    A[villes.json] -->|Sélection Wilaya| B[31-Oran.json]
    B -->|Extraction| C[Liste des Communes]
    C -->|Sélection| D[Navigation JSON]
    D -->|Extraction| E[Liste des Quartiers]
    E -->|Sélection + Live Search| F[Overpass API / OSM]
    F -->|Resultat| G[Bâtiments réels / Polygones]


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
OVERRIDES_FILE = DATA_DIR / "quartiers_overrides.json"

# ====================== CHARGEMENT DATASET LOCAL ======================
def load_wilayas() -> list:
    """Charge les 58+ wilayas depuis le dataset local."""
    with open(WILAYAS_FILE, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def load_communes() -> list:
    """Charge les 1541 communes depuis le dataset local."""
    with open(COMMUNES_FILE, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def load_overrides() -> dict:
    """Charge les quartiers manuels (overrides) depuis le fichier local."""
    if not OVERRIDES_FILE.exists():
        return {}
    with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# ====================== MAPPING WILAYAS (58 officielles) ======================
# On ne garde que les 58 wilayas officielles (codes 1-58)
# Le dataset 2026 contient aussi des wilayas déléguées (codes 49-69+)
# On mappe les deux systèmes
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
    """
    Construit un mapping dataset_wilaya_id → code_officiel.
    Le dataset utilise ses propres IDs (1-69), on mappe vers les 58 codes officiels.
    """
    # Mapping nom → code officiel
    name_to_code = {}
    for code, name in WILAYAS_58.items():
        # Normaliser le nom pour le matching
        name_to_code[_normalize(name)] = code

    # Construire le mapping dataset_id → code_officiel
    id_map = {}
    for w in wilayas_data:
        dataset_id = str(w["id"])
        dataset_name = w["name"]
        normalized = _normalize(dataset_name)

        # Chercher le code officiel par nom normalisé
        if normalized in name_to_code:
            id_map[dataset_id] = name_to_code[normalized]
        else:
            # Tentative avec le code direct si <= 58
            code_val = int(w.get("code", 0))
            if 1 <= code_val <= 58:
                id_map[dataset_id] = code_val
            else:
                # Wilayas déléguées ou nouvelles — on les intègre telles quelles
                id_map[dataset_id] = code_val

    return id_map

def _normalize(name: str) -> str:
    """Normalise un nom pour le matching (supprime accents, met en minuscule)."""
    import unicodedata
    # Supprimer les accents
    nfkd = unicodedata.normalize("NFKD", name)
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    return clean.lower().strip().replace("'", "").replace("'", "").replace("-", " ")


def get_communes_for_wilaya(communes_data: list, wilaya_code: int, id_map: dict) -> list:
    """Retourne les communes d'une wilaya donnée depuis le dataset local."""
    # Trouver tous les dataset_ids qui correspondent à ce code_officiel
    matching_ids = [did for did, code in id_map.items() if code == wilaya_code]

    result = []
    seen = set()
    for com in communes_data:
        wid = str(com.get("wilaya_id", ""))
        if wid in matching_ids:
            name = com.get("name", "").strip()
            if name and name not in seen:
                seen.add(name)
                result.append({
                    "nom": name,
                    "lat": float(com.get("longitude", 0)),  # Note: le dataset a lat/lon inversés
                    "lon": float(com.get("latitude", 0)),
                    "post_code": com.get("post_code", ""),
                })

    return sorted(result, key=lambda x: x["nom"])


# ====================== OVERPASS (pour quartiers uniquement) ======================
OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
    "https://api.openstreetmap.fr/oapi/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

BLACKLIST_QUARTIER = [
    "logement", "logements", "aadl", "opgi", "adl",
    "100 log", "200 log", "300 log", "400 log", "500 log",
    "سكن", "مسكن", " lpa ", " lpp ",
]


async def overpass_request(query: str, timeout: int = 60, retries: int = 2) -> dict:
    """Essaie plusieurs serveurs Overpass avec retry automatique."""
    for attempt in range(retries):
        for url in OVERPASS_SERVERS:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(url, data={"data": query.strip()})
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("elements"):
                            print(f"      ✅ Overpass OK ({url.split('/')[2]})")
                            return data
            except Exception as e:
                continue
            await asyncio.sleep(0.5)
        if attempt < retries - 1:
            await asyncio.sleep(2)

    return {"elements": []}


async def fetch_quartiers_overpass(commune_nom: str, lat: float, lon: float) -> list:
    """
    Récupère les quartiers d'une commune via Overpass API.
    Utilise les coordonnées GPS de la commune pour construire une bbox.
    """
    margin = 0.03  # ~3km autour du centre
    s, w, n, e = lat - margin, lon - margin, lat + margin, lon + margin

    query = f"""
[out:json][timeout:45];
(
  node["place"~"suburb|neighbourhood|quarter|city_district"]({s},{w},{n},{e});
  way["place"~"suburb|neighbourhood|quarter|city_district"]({s},{w},{n},{e});
  relation["place"~"suburb|neighbourhood|quarter|city_district"]({s},{w},{n},{e});
);
out tags;
"""
    data = await overpass_request(query)
    names = set()

    for el in data.get("elements", []):
        tags = el.get("tags", {})
        name = (tags.get("name:fr") or tags.get("name") or "").strip()
        if not name or len(name) < 3 or name.isdigit():
            continue
        # Appliquer la blacklist
        name_lower = name.lower()
        if any(bad in name_lower for bad in BLACKLIST_QUARTIER):
            continue
        names.add(name)

    return sorted(names)


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
    overrides: dict,
    with_quartiers: bool = False,
    delay: float = 1.5,
):
    """Génère le fichier cache JSON pour une wilaya complète."""
    print(f"\n📍 [{wilaya_code:02d}/58] {wilaya_nom}")

    # 1. Récupérer les communes depuis le dataset local
    communes_local = get_communes_for_wilaya(communes_data, wilaya_code, id_map)
    print(f"   📋 {len(communes_local)} communes (dataset local)")

    # 2. Récupérer les overrides quartiers pour cette wilaya
    wilaya_overrides = overrides.get(str(wilaya_code), {})
    if wilaya_overrides:
        print(f"   📝 Overrides quartiers : {len(wilaya_overrides)} communes avec quartiers manuels")

    # 3. Construire la hiérarchie
    communes_result = []
    for i, com in enumerate(communes_local, 1):
        nom = com["nom"]
        quartiers = []

        # Priorité 1 : overrides manuels
        manual_quartiers = wilaya_overrides.get(nom, [])
        if manual_quartiers:
            quartiers = list(manual_quartiers)
            print(f"   [{i}/{len(communes_local)}] {nom} → {len(quartiers)} quartiers (overrides)")
        elif with_quartiers:
            # Priorité 2 : Overpass API
            print(f"   [{i}/{len(communes_local)}] {nom} → ", end="", flush=True)
            overpass_quartiers = await fetch_quartiers_overpass(nom, com["lat"], com["lon"])
            quartiers = overpass_quartiers
            print(f"{len(quartiers)} quartiers (Overpass)")
            await asyncio.sleep(delay)
        else:
            print(f"   [{i}/{len(communes_local)}] {nom} → 0 quartiers (sans --with-quartiers)")

        communes_result.append({
            "nom": nom,
            "quartiers": quartiers
        })

    # 4. Construire le JSON final
    data = {
        "code": wilaya_code,
        "nom": wilaya_nom,
        "label": f"{wilaya_code:02d} - {wilaya_nom}",
        "communes": communes_result
    }

    # 5. Sauvegarder
    path = wilaya_cache_path(wilaya_code, wilaya_nom)
    save_cache(path, data)

    # Stats
    total_q = sum(len(c["quartiers"]) for c in communes_result)
    print(f"   ✅ {len(communes_result)} communes, {total_q} quartiers au total")

    return data


async def main(wilaya_filter: int = None, with_quartiers: bool = False, delay: float = 1.5):
    """Point d'entrée principal."""
    print("=" * 80)
    print("🚀 VilleData.py — Version hybride (dataset local + Overpass enrichi)")
    print("=" * 80)

    # Vérifier que les fichiers de données existent
    if not WILAYAS_FILE.exists():
        print(f"❌ Fichier manquant : {WILAYAS_FILE}")
        print("   Téléchargez-le depuis : https://github.com/Kenandarabeh/algeria-wilayas-communes-2026")
        return
    if not COMMUNES_FILE.exists():
        print(f"❌ Fichier manquant : {COMMUNES_FILE}")
        print("   Téléchargez-le depuis : https://github.com/Kenandarabeh/algeria-wilayas-communes-2026")
        return

    # Charger les données
    print("\n📂 Chargement des données locales...")
    wilayas_data = load_wilayas()
    communes_data = load_communes()
    overrides = load_overrides()
    print(f"   ✅ {len(wilayas_data)} wilayas, {len(communes_data)} communes")
    if overrides:
        total_override_communes = sum(len(v) for v in overrides.values())
        print(f"   ✅ {len(overrides)} wilayas avec overrides ({total_override_communes} communes)")

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

    # Mode d'exécution
    if with_quartiers:
        print("\n🔍 Mode : communes (local) + quartiers (overrides + Overpass)")
    else:
        print("\n⚡ Mode : communes uniquement (local, instantané)")

    # Générer le cache
    for code, nom in to_process:
        await generate_wilaya_cache(
            wilaya_code=code,
            wilaya_nom=nom,
            communes_data=communes_data,
            id_map=id_map,
            overrides=overrides,
            with_quartiers=with_quartiers,
            delay=delay,
        )

    print("\n" + "=" * 80)
    print("🎉 Cache terminé !")
    if not with_quartiers:
        print("💡 Pour enrichir avec les quartiers Overpass : python VilleData.py --with-quartiers")
    print("🚀 Lance maintenant : python app.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère le cache hiérarchique Wilaya → Communes → Quartiers")
    parser.add_argument("--wilaya", type=int, help="Code wilaya (ex: 31 pour Oran)")
    parser.add_argument("--with-quartiers", action="store_true", help="Active la récupération des quartiers via Overpass")
    parser.add_argument("--delay", type=float, default=1.5, help="Délai entre requêtes Overpass (secondes)")
    args = parser.parse_args()
    asyncio.run(main(args.wilaya, args.with_quartiers, args.delay))