"""
SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM ORAN (v11)
=====================================================
CHANGEMENTS v11 vs v8/v10 :
  SUPPRIMÉ  : tout ce qui dépend d'OSM (osmnx, charger_batiments, estimer_logements,
               charger_communes_oran, charger_quartiers_oran, MultiPolygon, etc.)
  REMPLACÉ  : par generer_batiments_oran() — génération 100% déterministe + aléatoire contrôlé

ARCHITECTURE D'UN BÂTIMENT v11 :
  - Un seul point GPS pour tout le bâtiment (plus de cages séparées)
  - Colonne verticale pure pour tous les appartements
  - nb_log_etage : nombre total d'appartements par étage (3-15, random)

PHYSIQUE DES COLONNES VERTICALES :
  - Le bâtiment a une position GPS FIXE (centroïde)
  - Tous les appartements du bâtiment ont la MÊME lat/lon (colonne verticale pure)
  - Seul l'étage change → Le groupement se fait par étage
  - nb_log_etage appartements par étage

CONTRAINTE FAT (inchangée) :
  - FAT_group = porte_rank // 8 PAR ÉTAGE (règle AT officielle)
  - 1 FAT = max 8 abonnés du MÊME étage

CONSERVÉ INTACT de v8/v10 :
  - assigner_fats_batiment() — logique FAT inchangée
  - merge_all_tables()        — fusion CSV inchangée
  - ResidenceNamer            — naming bâtiments
  - Toutes les constantes AT  — settings, PREFAB_LENGTHS, etc.
  - generer_tables()          — génération tables AT (légère adaptation)
  - Tous les utilitaires GPS  — haversine, offset_gps, rand_offset, etc.
"""

import os
import shutil
import sys
import warnings
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt, ceil
from shapely.geometry import Polygon, Point

# ─── Import config ──────────────────────────────────────────────────────────
try:
    from backend.config import settings
except ImportError:
    from backend.config import settings

PREFAB_LENGTHS = settings.AT_DROP_CABLE_STANDARDS_M

warnings.filterwarnings("ignore")
np.random.seed(2026)

# ─── Dossier de sortie ──────────────────────────────────────────────────────
OUTPUT_DIR = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v12"
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)  # supprime tout le dossier + contenu

os.makedirs(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
# CONSTANTES WILAYA ORAN (hardcodé, plus besoin d'OSM)
# =============================================================================
WILAYA        = 31
ZONE_CODE     = "310"
wilaya_nom    = "Oran"

# =============================================================================
# COMMUNES D'ORAN — centres GPS réels + rayon de dispersion des bâtiments
# =============================================================================
# lat/lon = centroïde réel de la commune (source : coordonnées publiques)
# radius_m = rayon dans lequel les bâtiments de cette commune sont dispersés
ORAN_COMMUNES = {
    "Oran":              {"lat": 35.6969, "lon": -0.6331, "radius_m": 2000},
    "Gdyel":             {"lat": 35.7836, "lon": -0.5139, "radius_m": 800},
    "Bir El Djir":       {"lat": 35.7300, "lon": -0.5800, "radius_m": 900},
    "Hassi Bounif":      {"lat": 35.7100, "lon": -0.6900, "radius_m": 700},
    "Es Senia":          {"lat": 35.6400, "lon": -0.6100, "radius_m": 800},
    "Arzew":             {"lat": 35.8333, "lon": -0.3167, "radius_m": 900},
    "Bethioua":          {"lat": 35.8167, "lon": -0.2667, "radius_m": 600},
    "Marsat El Hadjadj": {"lat": 35.8000, "lon": -0.2000, "radius_m": 500},
    "Ain El Turk":       {"lat": 35.7400, "lon": -0.7600, "radius_m": 700},
    "El Ancon":          {"lat": 35.7200, "lon": -0.7800, "radius_m": 600},
    "Oued Tlelat":       {"lat": 35.5500, "lon": -0.4700, "radius_m": 700},
    "Tafraoui":          {"lat": 35.5200, "lon": -0.5500, "radius_m": 600},
    "Sidi Chami":        {"lat": 35.6700, "lon": -0.6000, "radius_m": 600},
    "Boufatis":          {"lat": 35.6200, "lon": -0.5000, "radius_m": 500},
    "Mers El Kebir":     {"lat": 35.7333, "lon": -0.7167, "radius_m": 700},
    "Bousfer":           {"lat": 35.7167, "lon": -0.7500, "radius_m": 600},
    "El Kerma":          {"lat": 35.6500, "lon": -0.5700, "radius_m": 600},
    "El Braya":          {"lat": 35.7000, "lon": -0.6200, "radius_m": 500},
    "Hassi Ben Okba":    {"lat": 35.6800, "lon": -0.4800, "radius_m": 500},
    "Ben Freha":         {"lat": 35.6100, "lon": -0.4500, "radius_m": 500},
    "Hassi Mefsoukh":    {"lat": 35.5800, "lon": -0.4200, "radius_m": 500},
    "Sidi Benyebka":     {"lat": 35.5600, "lon": -0.4000, "radius_m": 400},
    "Misserghin":        {"lat": 35.6300, "lon": -0.7800, "radius_m": 600},
    "Boutlelis":         {"lat": 35.5900, "lon": -0.8200, "radius_m": 500},
    "Ain El Kerma":      {"lat": 35.5700, "lon": -0.3800, "radius_m": 400},
    "Ain El Bia":        {"lat": 35.8500, "lon": -0.2300, "radius_m": 500},
}

# Types de bâtiments possibles (pondérés : AADL dominant à Oran)
BUILDING_TYPES_WEIGHTS = {
    "AADL":  0.40,
    "HLM":   0.20,
    "LPP":   0.15,
    "LPA":   0.10,
    "LSL":   0.08,
    "CNEP":  0.04,
    "PRIVE": 0.03,
}

HEIGHT_RANGES = {
    "AADL":  (2.9, 3.2),
    "HLM":   (2.6, 2.9),
    "LPP":   (3.0, 3.4),
    "LPA":   (2.8, 3.1),
    "LSL":   (2.6, 2.8),
    "CNEP":  (3.0, 3.3),
    "PRIVE": (3.1, 3.6),
}


BUILDING_TYPES  = list(BUILDING_TYPES_WEIGHTS.keys())
BUILDING_PROBS  = list(BUILDING_TYPES_WEIGHTS.values())

# Paramètres géométrie bâtiment
CAGE_SPACING_M  = 12.0   # distance entre deux cages d'escalier (mètres)
                          # réaliste pour un bloc AADL : 10-15m
log_par_etage   = 4
MIN_ETAGES      = 3
MAX_ETAGES      = 10
MIN_LOG_ETAGE   = 4
MAX_LOG_ETAGE   = 14

# Bâtiments par commune (A-D blocs, 1-6 numéros)
BLOCS           = ["A", "B", "C", "D","E"]
NUMS_PAR_BLOC   = 8      # numéros 1 à 8 → 8 × 5 = 40 bâtiments par commune

# =============================================================================
# NOMS / PRÉNOMS (inchangés)
# =============================================================================
PRENOMS = [
    "Mohamed", "Ahmed", "Abdelkader", "Youcef", "Karim", "Rachid", "Hichem", "Sofiane",
    "Amir", "Bilal", "Yassine", "Omar", "Ali", "Hamza", "Ibrahim", "Zakaria", "Nadir",
    "Walid", "Fayçal", "Salim", "Redouane", "Mehdi", "Ismaïl", "Anis", "Lotfi", "Tarek",
    "Nabil", "Sami", "Khaled", "Mourad", "Aymen", "Badr", "Djamel", "Farid", "Hassan",
    "Fatima", "Amina", "Nadia", "Samira", "Meriem", "Lynda", "Dalila", "Sofia", "Leila",
    "Houda", "Yasmine", "Sara", "Nour", "Imane", "Kheira", "Zineb", "Rania", "Warda",
    "Amel", "Dounia", "Fatiha", "Ghania", "Hassiba", "Karima", "Lamia", "Mouna", "Nabila",
    "Ikram"
]
NOMS_FAM = [
    "KEBIR", "BENALI", "KHELIFI", "BOUDIAF", "MANSOURI", "ZERROUK", "HAMIDI", "BENSALEM",
    "RAHMANI", "BELARBI", "CHABANE", "MERAD", "GUERFI", "BOUCHENAK", "FERHAT", "HADJ",
    "SAIDI", "BOUALI", "BENDJEDDOU", "CHERIF", "DJABRI", "ELKHALFI", "GHERBI", "HOCINE",
    "IDIR", "KACI", "LAKHDARI", "MEZIANE", "NEMOUCHI", "OUALI", "REZGUI", "SAHRAOUI",
    "TAHRI", "YAHIA", "ZEMMOURI", "ABDELLI", "BENYAHIA", "BOUKHEZAR", "DAOUD", "ELAMRI",
]
OPERATEURS = {
    "Djezzy":  ["077", "078"],
    "Ooredoo": ["066", "069", "079"],
    "Mobilis": ["055", "056", "057"],
}


def haversine(la1, lo1, la2, lo2):
    R = 6_371_000
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return R * 2 * asin(sqrt(max(0, a)))


def offset_gps(lat, lon, dist_m, angle_deg):
    """Déplace un point GPS de dist_m mètres dans la direction angle_deg."""
    a    = radians(angle_deg)
    dlat = (dist_m * cos(a)) / 111_000
    dlon = (dist_m * sin(a)) / (111_000 * cos(radians(lat)))
    return round(lat + dlat, 6), round(lon + dlon, 6)


def rand_offset(lat, lon, dmin, dmax):
    return offset_gps(lat, lon,
                      np.random.uniform(dmin, dmax),
                      np.random.uniform(0, 360))


def fmt_zone_id(commune_code, seq):
    return f"Z{ZONE_CODE}-{seq:03d}"

def fmt_olt(seq, elot):
    return f"T{ZONE_CODE}-{seq:03d}-{elot}-AN6000-IN"

def fmt_fdt(olt_seq, fdt_seq):
    return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}"

def fmt_fat(olt_seq, fdt_seq_num, spl_seq, commune, bloc, num_bat, type_unit, portes, etage, local_idx):
    # Extraction des numéros uniquement (ex: "Porte 5" -> "5")
    p_list = ",".join(str(x).replace("Porte ", "").replace("Commerce ", "") for x in portes)
    comm_norm = str(commune).upper().replace(" ", "-")
    # Format: F310-634-01-07-AIN-EL-BIA-B-1-Porte(1,2,3,4,5,6,7,8)-5F-7
    return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq_num:02d}-{spl_seq:02d}-{comm_norm}-{bloc}-{num_bat}-{type_unit}({p_list})-{etage}F-{local_idx}"

def fmt_code_client(seq):
    return f"1000000271{1200 + seq:04d}"


# =============================================================================
# RÉSIDENCE NAMER (inchangé)
# =============================================================================
class ResidenceNamer:
    """
    Génère des noms lisibles et UNIQUES par bâtiment.
    Pattern : {TypeBat} - {Commune} {Bloc}-{Num}
    ex : "AADL - Oran Bloc-A-1", "HLM - Gdyel Bloc-C-3"
    """
    def __init__(self, seed=2026):
        self._rng = np.random.RandomState(seed)

    def get(self, commune: str, bloc: str, num: int,
            type_batiment: str) -> tuple[str, str, str]:
        nom = f"{type_batiment} - {commune} Bloc {bloc}-Num {num}"
        return nom, nom, type_batiment


_namer = ResidenceNamer(seed=2026)


# =============================================================================
# GÉNÉRATION BÂTIMENTS ORAN (remplace charger_batiments + estimer_logements)
# =============================================================================
def batiment_polygon_carre(lat_centre: float, lon_centre: float,
                            demi_cote_m: float = 15.0) -> Polygon:
    """
    Crée un polygone carré centré sur (lat_centre, lon_centre).

    La conversion mètres → degrés utilise :
        d_lat = d_m / 111_000
        d_lon = d_m / (111_000 × cos(lat))

    Un immeuble AADL typique à Oran : ~15-25m de côté.
    """
    cos_lat = cos(radians(lat_centre))
    d_lat = demi_cote_m / 111_000
    d_lon = demi_cote_m / (111_000 * cos_lat)

    corners = [
        (lon_centre - d_lon, lat_centre - d_lat),  # SW
        (lon_centre + d_lon, lat_centre - d_lat),  # SE
        (lon_centre + d_lon, lat_centre + d_lat),  # NE
        (lon_centre - d_lon, lat_centre + d_lat),  # NW
    ]
    return Polygon(corners)


def coins_carre(lat_centre: float, lon_centre: float,
                demi_cote_m: float = 6.0) -> list[tuple[float, float]]:
    """
    Retourne les 4 coins GPS d'un carré de côté 2×demi_cote_m.

    Ce sont les positions des 4 CAGES D'ESCALIER du bâtiment.
    demi_cote_m ≈ CAGE_SPACING_M / 2  (distance cage↔centre)

    Ordre : SW, SE, NE, NW  (sens trigonométrique)
    Chaque coin = une cage = une colonne verticale d'appartements.

    POURQUOI ces 4 positions distinctes ?
    → K-Means 3D a besoin de positions x,y différentes pour distinguer les cages.
    → Avec un espacement de ~12m entre cages, le signal spatial est clair.
    → Les abonnés d'une même cage ont la MÊME lat/lon — colonne verticale pure.
    """
    cos_lat = cos(radians(lat_centre))
    h = demi_cote_m / 2  # demi-côté en mètres
    d_lat = h / 111_000
    d_lon = h / (111_000 * cos_lat)

    return [
        (round(lat_centre - d_lat, 6), round(lon_centre - d_lon, 6)),  # cage SW
        (round(lat_centre - d_lat, 6), round(lon_centre + d_lon, 6)),  # cage SE
        (round(lat_centre + d_lat, 6), round(lon_centre + d_lon, 6)),  # cage NE
        (round(lat_centre + d_lat, 6), round(lon_centre - d_lon, 6)),  # cage NW
    ]


def generer_batiments_oran(
    n_blocs_per_commune: int = 4,   # A, B, C, D
    n_nums_per_bloc: int = 5,       # 1, 2, 3, 4, 5
    rng_seed: int = 2026,
) -> pd.DataFrame:
    """
    Génère le DataFrame `bats` qui remplace la sortie de charger_batiments()
    + estimer_logements() sans aucune dépendance OSM.

    STRUCTURE D'UN BÂTIMENT :
    ─────────────────────────
    Chaque bâtiment est un CARRÉ avec 4 cages d'escalier aux 4 coins.
    - lat, lon     : centroïde du bâtiment (pour FDT, OLT, câbles trunk)
    - cage_positions : liste des 4 (lat, lon) des cages — colonne verticale
    - nb_etages    : random [MIN_ETAGES, MAX_ETAGES]
    - log_par_etage: random [MIN_LOG_ETAGE, MAX_LOG_ETAGE] PER CAGE
                     nb_log_total = N_CAGES × log_par_etage × nb_etages
    - geometry     : polygone carré Shapely (pour compatibilité generer_tables)

    NAMING :
    ────────
    batiment_pav = "{TypeBat} - {Commune} Bloc-{X}-{N}"
    id unique garanti : commune × bloc × num = 26 × 4 × 5 = 520 bâtiments

    DISPERSION GPS :
    ────────────────
    Chaque bâtiment est placé aléatoirement dans un rayon radius_m autour
    du centroïde de sa commune. Les bâtiments d'un même (commune, bloc)
    sont regroupés (même direction depuis le centre, distance croissante).
    """
    rng  = np.random.RandomState(rng_seed)
    rows = []
    bat_idx = 0

    communes = list(ORAN_COMMUNES.keys())

    for commune in communes:
        info        = ORAN_COMMUNES[commune]
        c_lat       = info["lat"]
        c_lon       = info["lon"]
        radius_m    = info["radius_m"]

        # Chaque bloc (A-D) part dans une direction différente
        for b_i, bloc in enumerate(BLOCS[:n_blocs_per_commune]):
            # Direction de base pour ce bloc (90° d'écart entre blocs)
            dir_base = b_i * 90 + rng.uniform(-20, 20)

            for num in range(1, n_nums_per_bloc + 1):
                # Distance depuis le centre de la commune, croissante par numéro
                dist = rng.uniform(
                    radius_m * 0.1 * num,
                    radius_m * 0.2 * num
                )
                dist = min(dist, radius_m)  # ne pas sortir du rayon

                direction = dir_base + rng.uniform(-15, 15)
                bat_lat, bat_lon = offset_gps(c_lat, c_lon, dist, direction)

                # Type bâtiment
                type_bat = rng.choice(BUILDING_TYPES, p=BUILDING_PROBS)

                # Dimensions
                nb_etages    = int(rng.randint(MIN_ETAGES, MAX_ETAGES + 1))
                log_par_cage = int(rng.randint(MIN_LOG_ETAGE, MAX_LOG_ETAGE + 1))

                # log_par_etage dans le contexte de assigner_fats_batiment
                # = log_par_cage (appartements dans UNE cage par étage)
                # Le total = N_CAGES × log_par_cage × nb_etages
                nb_log_total = log_par_etage * nb_etages

                # Taille du polygone : demi-côté ≈ CAGE_SPACING_M / 2
                demi_cote = CAGE_SPACING_M / 2
                poly = batiment_polygon_carre(bat_lat, bat_lon, demi_cote)

                # Positions des 4 cages (coins du carré)
                cages = coins_carre(bat_lat, bat_lon, demi_cote)

                # Nom bâtiment
                batiment_pav, _, _ = _namer.get(commune, bloc, num, type_bat)

                # Commerce RDC : 40% des bâtiments
                comm_rdc = bool(rng.random() < 0.40)

                rows.append({
                    # Coordonnées centroïde (pour OLT, FDT)
                    "lat":            bat_lat,
                    "lon":            bat_lon,
                    # Géométrie
                    "geometry":       poly,
                    # Paramètres bâtiment
                    "nb_etages":      nb_etages,
                    "nb_logements":   nb_log_total,
                    "log_par_etage":  log_par_cage,   # par cage par étage
                    # Positions cages (liste de 4 tuples (lat, lon))
                    # Metadata
                    "commune":        commune,
                    "bloc":           bloc,
                    "num_bat":        num,
                    "batiment_pav":   batiment_pav,
                    "type_batiment":  type_bat,
                    "commerce_rdc":   comm_rdc,
                    "btype":          "apartments",
                    "nom_bat":        batiment_pav,
                    # Champs OSM vides (compatibilité avec generer_tables)
                    "etages_osm":     nb_etages,
                    "poly_area_m2":   (2 * demi_cote)**2,
                    "hauteur_etage":  round(rng.uniform(*HEIGHT_RANGES[type_bat]), 2),
                })
                bat_idx += 1

    import geopandas as gpd
    bats = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    bats = bats.reset_index(drop=True)

    print(f"  → {len(bats):,} bâtiments générés (sans OSM)")
    print(f"  → Communes : {len(communes)}")
    print(f"  → Par commune : {n_blocs_per_commune} blocs × {n_nums_per_bloc} numéros")
    print(f"  → Étages moyen      : {bats['nb_etages'].mean():.1f}")
    print(f"  → Log/cage/étage moy: {bats['log_par_etage'].mean():.1f}")
    print(f"  → Abonnés/bât moy   : {(bats['nb_logements']).mean():.1f}")
    print(f"  → Abonnés/bât max   : {bats['nb_logements'].max()}")
    print(f"  → Total abonnés est.: {bats['nb_logements'].sum():,}")

    return bats


# =============================================================================
# GÉNÉRATION DES POSITIONS DES ABONNÉS — GRILLE 2D À L'INTÉRIEUR DU BÂTIMENT
# =============================================================================
# Architecture :
#   - Le bâtiment est modélisé comme un carré de côté sqrt(poly_area_m2)
#   - Les nb_log_etage appartements/étage sont disposés en GRILLE 2D
#     à l'intérieur de la surface utile (15% de marge côté murs)
#   - n_cols = ceil(sqrt(nb_log_etage)), n_rows = ceil(nb_log_etage / n_cols)
#   - Chaque cellule de grille = 1 colonne verticale (même lat/lon tous étages)
#   - Résultat : alignement vertical parfait + couverture homogène du bâtiment
#
# FAT Assignment (inchangé dans assigner_fats_batiment) :
#   - commerce_rdc = True  → étage 0 = FAT commerce séparée
#   - commerce_rdc = False → RDC vide, logements démarrent à l'étage 1
# =============================================================================
def generer_positions_batiment_v11(lat: float, lon: float,
                                   nb_log_etage: int,
                                   poly_area_m2: float = 225.0) -> list:
    """
    Retourne une liste de nb_log_etage tuples (lat_col, lon_col).

    Chaque tuple est la position GPS FIXE d'une colonne verticale
    (représente un emplacement de cage d'escalier / gaine technique).

    DISPOSITION EN GRILLE 2D :
      - côté bâtiment = sqrt(poly_area_m2) [entre ~20m et ~45m typiquement]
      - zone utile    = 70% du côté (15% de marge de chaque côté)
      - n_cols x n_rows >= nb_log_etage — grille la plus carrée possible
      - positions numérotées ligne par ligne (raster order : col varie vite)

    ALIGNEMENT VERTICAL GARANTI :
      - L'index appt_in_floor (0 .. nb_log_etage-1) est stable entre étages
      - Tous les abonnés du même index -> même (lat, lon), étage différent
    """
    if nb_log_etage <= 1:
        return [(round(lat, 6), round(lon, 6))]

    # ── Dimensions du bâtiment (carré) ───────────────────────────────────────
    side_m  = max(10.0, poly_area_m2 ** 0.5)   # côté en mètres
    margin  = 0.15                              # 15% de marge / mur
    usable  = side_m * (1.0 - 2.0 * margin)    # zone utile centrée

    # ── Grille 2D : disposition la plus carrée possible ──────────────────────
    n_cols = ceil(nb_log_etage ** 0.5)
    n_rows = ceil(nb_log_etage / n_cols)

    # Espacement entre points de grille (en mètres)
    dx_m = usable / (n_cols - 1) if n_cols > 1 else 0.0
    dy_m = usable / (n_rows - 1) if n_rows > 1 else 0.0

    # Borne réaliste : min 2m (couloir étroit), max 12m entre deux cages
    dx_m = max(2.0, min(dx_m, 12.0))
    dy_m = max(2.0, min(dy_m, 12.0))

    # ── Conversion mètres → degrés (projection locale) ───────────────────────
    cos_lat       = cos(radians(lat))
    m_per_deg_lat = 111_000
    m_per_deg_lon = 111_000 * cos_lat

    # Coin SW de la grille (centré sur le centroïde du bâtiment)
    half_w    = ((n_cols - 1) * dx_m) / 2.0
    half_h    = ((n_rows - 1) * dy_m) / 2.0
    start_lat = lat - half_h / m_per_deg_lat
    start_lon = lon - half_w / m_per_deg_lon

    # ── Génération des positions (raster order) ───────────────────────────────
    positions = []
    for i in range(nb_log_etage):
        col   = i % n_cols
        row   = i // n_cols
        p_lat = start_lat + row * dy_m / m_per_deg_lat
        p_lon = start_lon + col * dx_m / m_per_deg_lon
        positions.append((round(p_lat, 6), round(p_lon, 6)))

    return positions


# =============================================================================
# FAT ASSIGNMENT (inchangé — copié de v8)
# =============================================================================
def assigner_fats_batiment(
    nb_et: int,
    nb_log_etage: int,        # Nombre total d'appartements par étage
    base_positions: list,     # liste de (nb_log_etage) tuples (lat, lon)
    comm_rdc: bool,
    olt_seq: int,
    fdt_seq_num: int,
    spl_seq_start: int,
    fat_num_start: int,
    elot: str,
    zone_id: str,
    bat_unique_id: str,
    olt_lat: float,
    olt_lon: float,
    fdt_nom: str,
    client_seq_start: int,
    numero_seq_start: int,
    batiment_pav: str,
    voie_osm: str,
    quartier_osm: str,
    commune: str,
    nbr_logements_total: int,
    type_batiment: str,
    presence_de_commerce: int,
    hauteur_etage: float,
    bloc: str,
    num_bat: int,
):
    """
    Génère tous les abonnés + FATs pour un bâtiment entier.

    Logique correcte :
    1) Le RDC est séparé s'il y a commerce.
    2) Tous les logements du bâtiment sont regroupés ensemble.
    3) Les FATs sont remplis par paquets de 8 au maximum.
    4) Un FAT peut contenir des abonnés venant de plusieurs étages.
    """
    fats_out, spl2_out, abonnes_out = [], [], []
    clients_out, adresses_out, numeros_out = [], [], []

    spl_seq    = spl_seq_start
    client_seq = client_seq_start
    numero_seq = numero_seq_start

    # Position GPS du FAT = centroïde du bâtiment
    fat_lat = olt_lat
    fat_lon = olt_lon

    def _creer_fat_et_abonnes(appts: list, usage: str):
        """
        Crée les FATs pour une liste d'appartements.
        Chaque FAT prend jusqu'à 8 abonnés.
        appts : liste de dicts {appt_in_floor, porte, etage}
        """
        nonlocal spl_seq, client_seq, numero_seq
        
        # Compteurs locaux par étage pour l'index final de la FAT (ex: ...-5F-1, ...-5F-2)
        if not hasattr(assigner_fats_batiment, "_floor_counters"):
            assigner_fats_batiment._floor_counters = {}
        
        counters = assigner_fats_batiment._floor_counters
        if not appts:
            return

        # Découpage simple et correct : paquets de 8 max
        groups = [appts[i:i + 8] for i in range(0, len(appts), 8)]

        for group in groups:
            if not group:
                continue

            portes_group = [apt["porte"] for apt in group]
            etages_group = [apt.get("etage", 0) for apt in group]

            # Label de l'étage (ex: "5" ou "5-6" si mélange)
            if len(set(etages_group)) == 1:
                etage_label = str(etages_group[0])
            else:
                etage_label = f"{min(etages_group)}-{max(etages_group)}"

            # Détermination de l'étage de référence pour l'indexation locale
            ref_etage = etages_group[0]
            counters[ref_etage] = counters.get(ref_etage, 0) + 1
            local_idx = counters[ref_etage]

            # Type de FAT: Porte ou Commerce
            type_unit = "Commerce" if usage == "commerces" else "Porte"

            fat_id = fmt_fat(
                olt_seq,
                fdt_seq_num,
                spl_seq,
                commune,
                bloc,
                num_bat,
                type_unit,
                portes_group,
                etage_label,
                local_idx
            )

            dist_fat = int(np.random.choice(settings.AT_DROP_CABLE_STANDARDS_M))

            fats_out.append({
                "id":             fat_id,
                "nom_FDT":        fdt_nom,
                "num_de_groupe":  local_idx,
                "latitude":       fat_lat,
                "longitude":      fat_lon,
                "usage":          usage,
                "nb_ports":       settings.FAT_CAPACITY,
                "nb_abonnes_sim": len(group),
                "nb_etages_bat":  nb_et,
                "nb_log_etage":   nb_log_etage,
                "zone_id":        zone_id,
                "distance_FAT_m": dist_fat,
            })

            # Splitter N2 : on garde la logique existante
            for dn in range(1, settings.FAT_CAPACITY + 1):
                spl2_out.append({
                    "id":                  f"{fat_id}-DOWN-{dn:02d}",
                    "nom_FAT":             fat_id,
                    "id_splitter1":        f"{fdt_nom}-S{spl_seq:02d}",
                    "rapport_de_division": f"1:{settings.SPLITTER_N2_RATIO}",
                    "port_splitter":       f"{fdt_nom}-S{spl_seq:02d}-DOWN-{dn}",
                    "etat":                "utilisé" if dn <= len(group) else "libre",
                    "zone_id":             zone_id,
                })

            for apt in group:
                appt_in_floor = apt["appt_in_floor"]
                porte         = apt["porte"]
                etage_apt     = apt.get("etage", 0)

                # Position GPS = position de la cage / colonne
                base_lat, base_lon = base_positions[appt_in_floor]

                cc      = fmt_code_client(client_seq)
                op      = np.random.choice(list(OPERATEURS.keys()))
                prefix  = np.random.choice(OPERATEURS[op])
                contact = int(prefix + f"{np.random.randint(1_000_000, 9_999_999):07d}")

                abonnes_out.append({
                    "code_client":             cc,
                    "latitude":                base_lat,
                    "longitude":               base_lon,
                    "etage":                   etage_apt,
                    "porte":                   porte,
                    "id_batiment":             batiment_pav,
                    "id_zone":                 zone_id,
                    "FAT_relative":            fat_id,
                    "distance_FAT_m":          dist_fat,
                    "nbr_etages":              nb_et,
                    "nbr_logements_par_etage": nb_log_etage,
                    "nbr_logements_total":     nbr_logements_total,
                    "type_batiment":           type_batiment,
                    "presence_de_commerce":    presence_de_commerce,
                    "Hauteur par étage (m)":   hauteur_etage,
                })

                clients_out.append({
                    "code_client": cc,
                    "contact":     contact,
                    "nom":         f"{np.random.choice(PRENOMS)} {np.random.choice(NOMS_FAM)}",
                })

                adresses_out.append({
                    "code_client":  cc,
                    "batiment_pav": batiment_pav,
                    "voie":         voie_osm if voie_osm else f"FAT{ZONE_CODE}-{elot}",
                    "quartier":     quartier_osm,
                    "commune":      commune,
                    "wilaya":       wilaya_nom,
                })

                numeros_out.append({
                    "num_de_groupe":   int(f"4{1_800_000 + numero_seq:07d}"),
                    "code_client":     cc,
                    "region_relative": zone_id,
                    "FAT_relative":    fat_id,
                })

                client_seq += 1
                numero_seq += 1

            spl_seq += 1


    # Reset des compteurs d'index FAT par étage pour ce bâtiment
    assigner_fats_batiment._floor_counters = {}
    
    # ── RDC (étage 0) ────────────────────────────────────────────────────────
    if presence_de_commerce == 1:
        nb_com = np.random.randint(2, 5)
        appts_rdc = []
        for i in range(nb_com):
            appts_rdc.append({
                "appt_in_floor": i, 
                "porte": f"Commerce {i+1}", 
                "etage": 0
            })
        # FAT(s) dédiée(s) aux commerces (usage séparé)
        _creer_fat_et_abonnes(appts_rdc, usage="commerces")

    # ── Étages résidentiels (1..nb_et) ───────────────────────────────────────
    appts_residentiels = []
    res_porte_seq = 1
    
    for et in range(1, nb_et + 1):
        for i in range(nb_log_etage):
            appts_residentiels.append({
                "appt_in_floor": i,
                "porte": f"Porte {res_porte_seq}",
                "etage": et
            })
            res_porte_seq += 1

    # Un seul flux résidentiel, ensuite découpé en paquets de 8
    _creer_fat_et_abonnes(appts_residentiels, usage="logements")

    return {
        "fats":       fats_out,
        "spl2":       spl2_out,
        "abonnes":    abonnes_out,
        "clients":    clients_out,
        "adresses":   adresses_out,
        "numeros":    numeros_out,
        "spl_seq":    spl_seq,
        "client_seq": client_seq,
        "numero_seq": numero_seq,
    }


# =============================================================================
# GÉNÉRATION TABLES AT (adapté de v8 — minimal changes)
# =============================================================================
def resoudre_elot(row, bat_idx):
    return f"{row['commune'].replace(' ','-').upper()}-{row['bloc']}"

def resoudre_voie(row):
    return f"Rue {row['commune']} {row['bloc']}-{row['num_bat']}"

def resoudre_quartier(row):
    return row["commune"]

def generer_tables(bats: pd.DataFrame) -> dict:
    nb_bat_total = len(bats)
    print(f"\n[2/2] Génération tables AT — {wilaya_nom} ({nb_bat_total} bâtiments)")

    zones, equipements, cartes, ports = [], [], [], []
    fdts, spl1_rows, fats, spl2      = [], [], [], []
    clients, adresses, numeros, abonnes = [], [], [], []

    client_seq = 0
    numero_seq = 0
    olt_abonnes_count = {}

    nb_commerce_rdc  = int(nb_bat_total * 0.40)
    commerce_indices = set(
        np.random.choice(nb_bat_total, nb_commerce_rdc, replace=False)
    )

    commune_seq_counter: dict[str, int] = {}

    for bat_idx, row in bats.iterrows():
        olt_seq  = bat_idx + 1
        lat      = float(row["lat"])
        lon      = float(row["lon"])
        nb_et    = int(row["nb_etages"])
        nb_log_etage = int(row.get("log_par_etage", 4))
        hauteur_etage = float(row["hauteur_etage"])
        comm_rdc = bool(row.get("commerce_rdc", bat_idx in commerce_indices))
        commune  = row["commune"]

        elot      = resoudre_elot(row, bat_idx)
        voie_osm  = resoudre_voie(row)
        quartier  = resoudre_quartier(row)

        commune_seq_counter[commune] = commune_seq_counter.get(commune, 0) + 1
        zone_seq = commune_seq_counter[commune]
        com_code = commune.replace(" ", "")[:4].upper()
        zone_id  = fmt_zone_id(com_code, zone_seq)

        batiment_pav  = row["batiment_pav"]
        type_batiment = row["type_batiment"]

        print(f" ⏳ [{bat_idx+1}/{nb_bat_total}] {batiment_pav}")
        print(f"    ➡️  Étages:{nb_et} | Log/étage:{nb_log_etage} | "
              f" | Commerce:{comm_rdc}")

        olt_nom = fmt_olt(olt_seq, elot)

        if zone_id not in olt_abonnes_count:
            olt_abonnes_count[zone_id] = 0

        zones.append({
            "id":               zone_id,
            "wilaya":           WILAYA,
            "wilaya_nom":       wilaya_nom,
            "commune":          commune,
            "zone_geographique":f"CECLI {commune.upper()}",
        })
        equipements.append({
            "id":        zone_id,
            "nom":       olt_nom,
            "ip":        f"100.{WILAYA}.{(olt_seq % 254)+1}.{np.random.randint(1,254)}",
            "type":      np.random.choice(["FIBERHOME","HUAWEI","ZTE"], p=[0.5,0.35,0.15]),
            "latitude":  lat,
            "longitude": lon,
        })
        for slot in [1, 2]:
            cartes.append({
                "id":           int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}"),
                "nom":          f"{olt_nom}_Frame:0/Slot:{slot}",
                "id_equipement":zone_id,
                "position":     f"0/{slot}",
                "type":         "gpon",
                "rack":         slot,
            })
            for p in range(1, 17):
                etat = "utilisé" if (slot == 1 and p <= 4) else "libre"
                ports.append({
                    "id":      int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}{p:02d}"),
                    "nom":     f"{olt_nom}-Frame:0/Slot:{slot}/Port:{p}",
                    "nomCarte":f"{olt_nom}_Frame:0/Slot:{slot}",
                    "position":p,
                    "etat":    etat,
                    "zone_id": zone_id,
                })

        # FDT : 2-4 boîtiers autour du bâtiment
        nb_fdts  = np.random.randint(2, 5)
        fdts_bat = []
        for i in range(nb_fdts):
            angle = i * (360 / nb_fdts) + np.random.uniform(-15, 15)
            dist  = np.random.uniform(80, 400)
            fl, flo = offset_gps(lat, lon, dist, angle)
            fdt_nom = f"F{ZONE_CODE}-{olt_seq:03d}-{i+1:02d}"
            fdts_bat.append({
                "id":             fdt_nom,
                "nom_equipement": olt_nom,
                "zone":           zone_id,
                "latitude":       fl,
                "longitude":      flo,
                "distance_olt_m": round(dist),
            })
            fdts.append(fdts_bat[-1])
            spl1_rows.append({
                "id":                    f"{fdt_nom}-S01",
                "nom_FDT":               fdt_nom,
                "rapport_de_division":   "1:8",
                "etat":                  "utilisé",
                "zone_id":               zone_id,
            })

        fdt_ref    = fdts_bat[0]
        fdt_nom    = fdt_ref["id"]
        fdt_seq_num= int(fdt_nom.split("-")[-1])

        # Positions des abonnés :  nb_log_par_etage colonnes verticales
        # espacées selon la surface du bâtiment (poly_area_m2 / nb_log_etage)
        poly_area_m2 = float(row.get("poly_area_m2", 225.0))
        base_positions = generer_positions_batiment_v11(lat, lon, nb_log_etage,poly_area_m2=poly_area_m2)

        result = assigner_fats_batiment(
            nb_et=nb_et,
            nb_log_etage=nb_log_etage,
            base_positions=base_positions,
            comm_rdc=comm_rdc,
            olt_seq=olt_seq,
            fdt_seq_num=fdt_seq_num,
            spl_seq_start=1,
            fat_num_start=1,
            elot=elot,
            zone_id=zone_id,
            bat_unique_id=zone_id,
            olt_lat=lat,
            olt_lon=lon,
            fdt_nom=fdt_nom,
            client_seq_start=client_seq,
            numero_seq_start=numero_seq,
            batiment_pav=batiment_pav,
            voie_osm=voie_osm,
            quartier_osm=quartier,
            commune=commune,
            nbr_logements_total=int(row["nb_logements"]),
            type_batiment=type_batiment,
            presence_de_commerce=1 if comm_rdc else 0,
            hauteur_etage=hauteur_etage,
            bloc=row["bloc"],
            num_bat=int(row["num_bat"]),
        )

        fats.extend(result["fats"])
        spl2.extend(result["spl2"])
        abonnes.extend(result["abonnes"])
        clients.extend(result["clients"])
        adresses.extend(result["adresses"])
        numeros.extend(result["numeros"])

        client_seq = result["client_seq"]
        numero_seq = result["numero_seq"]
        olt_abonnes_count[zone_id] = (
            olt_abonnes_count.get(zone_id, 0) + len(result["abonnes"])
        )

    return {
        "zone":       pd.DataFrame(zones),
        "equipement": pd.DataFrame(equipements),
        "carte":      pd.DataFrame(cartes),
        "port":       pd.DataFrame(ports),
        "fdt":        pd.DataFrame(fdts),
        "splitter_n1":pd.DataFrame(spl1_rows),
        "fat":        pd.DataFrame(fats),
        "splitter_n2":pd.DataFrame(spl2),
        "client":     pd.DataFrame(clients),
        "adresse":    pd.DataFrame(adresses),
        "numero":     pd.DataFrame(numeros),
        "abonnes":    pd.DataFrame(abonnes),
    }


# =============================================================================
# MERGE TABLES (inchangé de v8/v10)
# =============================================================================
def merge_all_tables():
    print("🔄 Lecture de TOUTES les tables depuis donnee_generee_v12/...")
    base = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v12"

    abonnes    = pd.read_csv(f"{base}/abonnes.csv")
    client     = pd.read_csv(f"{base}/client.csv")
    fat        = pd.read_csv(f"{base}/fat.csv")
    numero     = pd.read_csv(f"{base}/numero.csv")
    adresse    = pd.read_csv(f"{base}/adresse.csv")
    zone       = pd.read_csv(f"{base}/zone.csv")
    equipement = pd.read_csv(f"{base}/equipement.csv")
    fdt        = pd.read_csv(f"{base}/fdt.csv")
    carte      = pd.read_csv(f"{base}/carte.csv")
    port       = pd.read_csv(f"{base}/port.csv")
    splitter_n2 = pd.read_csv(f"{base}/splitter_n2.csv")

    print(f"   → {len(abonnes):,} abonnés chargés")

    # 🔹 1. BASE (OK)
    df = abonnes.merge(client, on="code_client", how="left")

    # 🔹 2. RENOMMER ABONNE
    df = df.rename(columns={"latitude": "lat_abonne", "longitude": "lon_abonne"})

    # 🔹 3. FAT (OK car clé correcte)
    df = df.merge(
        fat[["id", "latitude", "longitude", "usage", "nb_ports",
             "nb_abonnes_sim", "nom_FDT",
             "nb_etages_bat", "nb_log_etage"]],
        left_on="FAT_relative", right_on="id", how="left"
    ).drop(columns=["id"], errors="ignore")

    df = df.rename(columns={"latitude": "lat_fat", "longitude": "lon_fat"})

    # 🔹 4. NUMERO (OK)
    df = df.merge(numero[["code_client", "num_de_groupe"]],
                  on="code_client", how="left")

    # 🔹 5. ADRESSE (OK si 1 ligne / client)
    df = df.merge(adresse, on="code_client", how="left")

    # 🔹 6. ZONE (⚠️ sécuriser unicité)
    zone_unique = zone.drop_duplicates(subset="id")

    df = df.merge(
        zone_unique[["id", "commune", "zone_geographique"]],
        left_on="id_zone", right_on="id", how="left"
    ).drop(columns=["id"], errors="ignore")

    # 🔥 7. EQUIPEMENT (CORRECTION CRITIQUE)
    # ❌ avant : plusieurs équipements par zone → explosion
    # ✅ on garde UN seul équipement par zone

    equipement_unique = equipement.drop_duplicates(subset="id")

    df = df.merge(
        equipement_unique[["id", "nom", "type", "ip"]],
        left_on="id_zone", right_on="id", how="left"
    ).drop(columns=["id"], errors="ignore")

    df = df.rename(columns={"nom": "nom_OLT", "type": "type_OLT"})

    # 🔹 8. FDT (OK car clé unique)
    df = df.merge(
        fdt[["id", "latitude", "longitude", "distance_olt_m"]],
        left_on="nom_FDT", right_on="id", how="left"
    ).drop(columns=["id"], errors="ignore")

    df = df.rename(columns={"latitude": "lat_fdt", "longitude": "lon_fdt"})

    # 🔥 9. CARTE (déjà agrégé → OK mais sécuriser)
    if not carte.empty:
        carte_agg = carte.groupby("id_equipement").agg(
            nb_cartes=('id', 'count'),
            cartes_positions=('position', lambda x: ', '.join(sorted(set(x.astype(str)))))
        ).reset_index()

        df = df.merge(
            carte_agg,
            left_on="id_zone",
            right_on="id_equipement",
            how="left"
        ).drop(columns=["id_equipement"], errors="ignore")

    # 🔥 10. PORT (déjà agrégé → OK)
    if not port.empty:
        port_agg = port.groupby("zone_id").agg(
            nb_ports_total=('id', 'count'),
            nb_ports_utilises=('etat', lambda x: (x == 'utilisé').sum()),
            nb_ports_libres=('etat', lambda x: (x == 'libre').sum())
        ).reset_index()

        df = df.merge(
            port_agg,
            left_on="id_zone",
            right_on="zone_id",
            how="left"
        ).drop(columns=["zone_id"], errors="ignore")

    # 🔥 11. CHECK FINAL (ANTI-DUPLICATION)
    print(f"   → Lignes avant nettoyage: {len(df):,}")

    df = df.drop_duplicates(subset="code_client")

    print(f"   → Lignes après nettoyage: {len(df):,}")

    # 🔹 12. COLONNES
    colonnes = [
        "code_client", "id_batiment", "id_zone",
        "lat_abonne", "lon_abonne", "etage", "porte",
        "FAT_relative", "usage",
        "lat_fat", "lon_fat", "nb_abonnes_sim", "distance_FAT_m",
        "nom_FDT", "lat_fdt", "lon_fdt", "distance_olt_m",
        "nb_etages_bat", "nb_log_etage",
        "nbr_etages", "nbr_logements_par_etage", "nbr_logements_total",
        "type_batiment", "presence_de_commerce",
        "Hauteur par étage (m)",
        "num_de_groupe",
        "nom", "batiment_pav", "quartier", "commune"
    ]

    colonnes = [c for c in colonnes if c in df.columns]

    df_final = df[colonnes].sort_values(
        by=["id_batiment", "etage", "code_client"]
    ).reset_index(drop=True)

    df_final.to_csv(
        f"{base}/dataset_fusionnee_final.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\n✅ dataset_final.csv créé avec succès !")
    print(f"   → {len(df_final):,} lignes")
    print(f"   → Colonnes : {list(df_final.columns)}")

    return df_final
# =============================================================================
# POINT D'ENTRÉE
# =============================================================================
if __name__ == "__main__":
    print("=" * 70)
    print(f"SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM ORAN v11")
    print(f"Wilaya : {WILAYA} — {wilaya_nom}")
    print(f"Mode   : SANS OSM — génération déterministe par commune")
    print("=" * 70)

    # ── [1/2] Génération bâtiments ──────────────────────────────────────────
    print("\n[1/2] Génération bâtiments Oran (26 communes × 4 blocs × 5 numéros)...")
    bats = generer_batiments_oran(
        n_blocs_per_commune=5,   # A, B, C, D , E
        n_nums_per_bloc=5,       # 1 à 5
        rng_seed=2026,
    )
    print(f"\n  Total bâtiments : {len(bats)}")

    # Vérification rapide des tailles
    sizes = bats["nb_logements"]
    print(f"  Abonnés/bât : min={sizes.min()} | moy={sizes.mean():.1f} | max={sizes.max()}")
    assert sizes.max() <= MAX_LOG_ETAGE * MAX_ETAGES, \
        f"Bâtiment trop grand : {sizes.max()} abonnés"

    # ── [2/2] Génération tables AT ───────────────────────────────────────────
    tables = generer_tables(bats)

    # Sauvegarde CSV
    for nom, df_table in tables.items():
        path = os.path.join(OUTPUT_DIR, f"{nom}.csv")
        df_table.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  ✓ {nom:15s} → {len(df_table):7,} lignes")

    # Fusion finale
    df_final = merge_all_tables()

    print("\n" + "=" * 70)
    print("RÉSUMÉ FINAL")
    print("=" * 70)
    bat_sizes = df_final.groupby("id_batiment").size()
    print(f"  Abonnés total    : {len(df_final):,}")
    print(f"  Bâtiments        : {df_final['id_batiment'].nunique():,}")
    print(f"  FATs             : {df_final['FAT_relative'].nunique():,}")
    print(f"  Abonnés/bât max  : {bat_sizes.max()}")
    print(f"  Abonnés/bât moy  : {bat_sizes.mean():.1f}")
    print(f"  Fichier final    : {OUTPUT_DIR}\\dataset_fusionnee_final.csv")
    print("=" * 70)