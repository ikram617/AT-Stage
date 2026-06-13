"""
=====================================================
CHANGEMENTS v14 vs v13 :
  - Géométrie rectangulaire : aspect_ratio ∈ [1.0, 3.0]
    → bâtiments carrés, barres courtes, barres longues
    → surface sémantique ~25 m²/logement (norme F3/F4 algérienne)
    → nouvelles colonnes : bat_largeur_m, bat_longueur_m
  - Positions abonnés sur rectangle (generer_positions_batiment_v11 v14)
    → dx_m (axe couloir) et dy_m (axe profondeur) indépendants
    → distances horizontales plus grandes pour les barres → câbles 20m/50m
  - Palier par abonné (assigner_fats_batiment v14)
    → palier_i = f(position dans la grille, largeur bâtiment)
    → abonné proche cage : ~3m | abonné fond couloir : jusqu'à 40m
    → spread réaliste : 15m dominant, 20m fréquent, 50m pour barres longues
=====================================================
"""

import os
import shutil
import warnings
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt, ceil
from shapely.geometry import Polygon

try:
    from config import settings
except ImportError:
    from config import settings

PREFAB_LENGTHS  = settings.AT_DROP_CABLE_STANDARDS_M
PALIER_FIXE_M   =np.random.uniform(2, 8)  # distance horizontale fixe palier → porte (m) — règle AT

warnings.filterwarnings("ignore")
np.random.seed(2026)

# ─── Dossier de sortie ──────────────────────────────────────────────────────
OUTPUT_DIR = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v13_advanced"
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# CONSTANTES WILAYA ORAN
# =============================================================================
WILAYA        = 31
ZONE_CODE     = "310"
wilaya_nom    = "Oran"

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

BUILDING_TYPES_WEIGHTS = {
    "AADL":  0.40, "HLM": 0.20, "LPP": 0.15, "LPA": 0.10,
    "LSL": 0.08, "CNEP": 0.04, "PRIVE": 0.03,
}

HEIGHT_RANGES = {
    "AADL": (2.9, 3.2), "HLM": (2.6, 2.9), "LPP": (3.0, 3.4),
    "LPA": (2.8, 3.1), "LSL": (2.6, 2.8), "CNEP": (3.0, 3.3), "PRIVE": (3.1, 3.6),
}

BUILDING_TYPES = list(BUILDING_TYPES_WEIGHTS.keys())
BUILDING_PROBS = list(BUILDING_TYPES_WEIGHTS.values())

CAGE_SPACING_M = np.random.uniform(8, 25)
MIN_ETAGES = 3
MAX_ETAGES = 10
MIN_LOG_ETAGE = 4
MAX_LOG_ETAGE = 14
BLOCS = ["A", "B", "C", "D", "E"]
NUMS_PAR_BLOC = 5

PRENOMS = ["Mohamed", "Ahmed", "Abdelkader", "Youcef", "Karim", "Rachid", "Hichem", "Sofiane", "Amir", "Bilal", "Yassine", "Omar", "Ali", "Hamza", "Ibrahim", "Zakaria", "Nadir", "Walid", "Fayçal", "Salim", "Redouane", "Mehdi", "Ismaïl", "Anis", "Lotfi", "Tarek", "Nabil", "Sami", "Khaled", "Mourad", "Aymen", "Badr", "Djamel", "Farid", "Hassan", "Fatima", "Amina", "Nadia", "Samira", "Meriem", "Lynda", "Dalila", "Sofia", "Leila", "Houda", "Yasmine", "Sara", "Nour", "Imane", "Kheira", "Zineb", "Rania", "Warda", "Amel", "Dounia", "Fatiha", "Ghania", "Hassiba", "Karima", "Lamia", "Mouna", "Nabila", "Ikram"]
NOMS_FAM = ["KEBIR", "BENALI", "KHELIFI", "BOUDIAF", "MANSOURI", "ZERROUK", "HAMIDI", "BENSALEM", "RAHMANI", "BELARBI", "CHABANE", "MERAD", "GUERFI", "BOUCHENAK", "FERHAT", "HADJ", "SAIDI", "BOUALI", "BENDJEDDOU", "CHERIF", "DJABRI", "ELKHALFI", "GHERBI", "HOCINE", "IDIR", "KACI", "LAKHDARI", "MEZIANE", "NEMOUCHI", "OUALI", "REZGUI", "SAHRAOUI", "TAHRI", "YAHIA", "ZEMMOURI", "ABDELLI", "BENYAHIA", "BOUKHEZAR", "DAOUD", "ELAMRI"]

OPERATEURS = {"Djezzy": ["077", "078"], "Ooredoo": ["066", "069", "079"], "Mobilis": ["055", "056", "057"]}

# =============================================================================
# FONCTIONS GPS
# =============================================================================
def haversine(la1, lo1, la2, lo2):
    R = 6_371_000
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return R * 2 * asin(sqrt(max(0, a)))

def offset_gps(lat, lon, dist_m, angle_deg):
    a = radians(angle_deg)
    dlat = (dist_m * cos(a)) / 111_000
    dlon = (dist_m * sin(a)) / (111_000 * cos(radians(lat)))
    return round(lat + dlat, 6), round(lon + dlon, 6)

def rand_offset(lat, lon, dmin, dmax):
    return offset_gps(lat, lon, np.random.uniform(dmin, dmax), np.random.uniform(0, 360))

def fmt_zone_id(commune_code, seq):
    return f"Z{ZONE_CODE}-{seq:03d}"

def fmt_olt(seq, elot):
    return f"T{ZONE_CODE}-{seq:03d}-{elot}-AN6000-IN"

def fmt_fdt(olt_seq, fdt_seq):
    return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}"

def fmt_fat(olt_seq, fdt_seq_num, spl_seq, commune, bloc, num_bat, type_unit, portes, etage, local_idx):
    p_list = ",".join(str(x).replace("Porte ", "").replace("Commerce ", "") for x in portes)
    comm_norm = str(commune).upper().replace(" ", "-")
    return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq_num:02d}-{spl_seq:02d}-{comm_norm}-{bloc}-{num_bat}-{type_unit}({p_list})-{etage}F-{local_idx}"

def fmt_code_client(seq):
    return f"1000000271{1200 + seq:04d}"

# =============================================================================
# ResidenceNamer
# =============================================================================
class ResidenceNamer:
    def __init__(self, seed=2026):
        self._rng = np.random.RandomState(seed)
    def get(self, commune: str, bloc: str, num: int, type_batiment: str):
        nom = f"{type_batiment} - {commune} Bloc {bloc}-Num {num}"
        return nom, nom, type_batiment

_namer = ResidenceNamer(seed=2026)

# =============================================================================
# Génération bâtiments (identique à v11)
# =============================================================================
def batiment_polygon_carre(lat_centre, lon_centre, demi_cote_m=15.0):
    cos_lat = cos(radians(lat_centre))
    d_lat = demi_cote_m / 111_000
    d_lon = demi_cote_m / (111_000 * cos_lat)
    corners = [
        (lon_centre - d_lon, lat_centre - d_lat),
        (lon_centre + d_lon, lat_centre - d_lat),
        (lon_centre + d_lon, lat_centre + d_lat),
        (lon_centre - d_lon, lat_centre + d_lat),
    ]
    return Polygon(corners)

def coins_carre(lat_centre, lon_centre, demi_cote_m=6.0):
    cos_lat = cos(radians(lat_centre))
    h = demi_cote_m / 2
    d_lat = h / 111_000
    d_lon = h / (111_000 * cos_lat)
    return [
        (round(lat_centre - d_lat, 6), round(lon_centre - d_lon, 6)),
        (round(lat_centre - d_lat, 6), round(lon_centre + d_lon, 6)),
        (round(lat_centre + d_lat, 6), round(lon_centre + d_lon, 6)),
        (round(lat_centre + d_lat, 6), round(lon_centre - d_lon, 6)),
    ]

# ── Nouvelles fonctions géométriques rectangulaires (v14) ──────────────────
# On conserve les fonctions carrées ci-dessus pour ne rien casser.
# Ces deux fonctions acceptent deux demi-dimensions indépendantes :
#   demi_largeur_m = demi-côté sur l'axe Est-Ouest (couloir horizontal)
#   demi_longueur_m = demi-côté sur l'axe Nord-Sud (profondeur du bâtiment)

def batiment_polygon_rect(lat_centre, lon_centre, demi_largeur_m, demi_longueur_m):
    """Polygone rectangulaire. Remplace batiment_polygon_carre pour les bâtiments v14."""
    cos_lat = cos(radians(lat_centre))
    d_lat = demi_longueur_m / 111_000
    d_lon = demi_largeur_m  / (111_000 * cos_lat)
    corners = [
        (lon_centre - d_lon, lat_centre - d_lat),
        (lon_centre + d_lon, lat_centre - d_lat),
        (lon_centre + d_lon, lat_centre + d_lat),
        (lon_centre - d_lon, lat_centre + d_lat),
    ]
    return Polygon(corners)

def coins_rect(lat_centre, lon_centre, demi_largeur_m, demi_longueur_m):
    """Quatre coins d'un rectangle. Remplace coins_carre pour les bâtiments v14."""
    cos_lat = cos(radians(lat_centre))
    d_lat = demi_longueur_m / 111_000
    d_lon = demi_largeur_m  / (111_000 * cos_lat)
    return [
        (round(lat_centre - d_lat, 6), round(lon_centre - d_lon, 6)),
        (round(lat_centre - d_lat, 6), round(lon_centre + d_lon, 6)),
        (round(lat_centre + d_lat, 6), round(lon_centre + d_lon, 6)),
        (round(lat_centre + d_lat, 6), round(lon_centre - d_lon, 6)),
    ]

def generer_batiments_oran(n_blocs_per_commune=5, n_nums_per_bloc=5, rng_seed=2026):
    rng = np.random.RandomState(rng_seed)
    rows = []
    communes = list(ORAN_COMMUNES.keys())

    for commune in communes:
        info = ORAN_COMMUNES[commune]
        c_lat, c_lon, radius_m = info["lat"], info["lon"], info["radius_m"]

        for b_i, bloc in enumerate(BLOCS[:n_blocs_per_commune]):
            dir_base = b_i * 90 + rng.uniform(-20, 20)
            for num in range(1, n_nums_per_bloc + 1):
                dist = min(rng.uniform(radius_m * 0.1 * num, radius_m * 0.2 * num), radius_m)
                direction = dir_base + rng.uniform(-15, 15)
                bat_lat, bat_lon = offset_gps(c_lat, c_lon, dist, direction)

                type_bat = rng.choice(BUILDING_TYPES, p=BUILDING_PROBS)
                nb_etages = int(rng.randint(MIN_ETAGES, MAX_ETAGES + 1))
                log_par_cage = int(rng.randint(MIN_LOG_ETAGE, MAX_LOG_ETAGE + 1))
                nb_log_total = log_par_cage * nb_etages

                # ── Géométrie rectangulaire v14 ────────────────────────────
                # Surface sémantique : ~25 m² par logement (norme algérienne F3/F4),
                # contrainte entre 100 m² (petit immeuble) et 1200 m² (grande barre).
                # aspect_ratio = largeur/longueur :
                #   1.0 → carré (hall central, cage unique)
                #   2.0 → barre courte (2 cages, couloir ~20-30 m)
                #   3.0 → barre longue (3+ cages, couloir ~40 m+)
                # C'est l'axe large (Est-Ouest) qui détermine la distance
                # horizontale FAT→abonné et donc le type de câble snap.
                surface_m2   = float(np.clip(log_par_cage * 25.0, 100.0, 1200.0))
                aspect_ratio = float(rng.uniform(1.0, 3.0))
                largeur_m    = round(float((surface_m2 * aspect_ratio) ** 0.5), 1)
                longueur_m   = round(float((surface_m2 / aspect_ratio) ** 0.5), 1)

                demi_larg = largeur_m  / 2.0
                demi_long = longueur_m / 2.0
                poly  = batiment_polygon_rect(bat_lat, bat_lon, demi_larg, demi_long)
                cages = coins_rect(bat_lat, bat_lon, demi_larg, demi_long)

                batiment_pav, _, _ = _namer.get(commune, bloc, num, type_bat)
                comm_rdc = bool(rng.random() < 0.40)

                rows.append({
                    "lat": bat_lat, "lon": bat_lon, "geometry": poly,
                    "nb_etages": nb_etages, "nb_logements": nb_log_total,
                    "log_par_etage": log_par_cage,
                    "commune": commune, "bloc": bloc, "num_bat": num,
                    "batiment_pav": batiment_pav, "type_batiment": type_bat,
                    "commerce_rdc": comm_rdc, "btype": "apartments",
                    "nom_bat": batiment_pav, "etages_osm": nb_etages,
                    "poly_area_m2":   largeur_m * longueur_m,
                    "bat_largeur_m":  largeur_m,   # ← v14 : axe large (couloir)
                    "bat_longueur_m": longueur_m,  # ← v14 : axe profondeur
                    "hauteur_etage":  round(rng.uniform(*HEIGHT_RANGES[type_bat]), 2),
                })

    import geopandas as gpd
    bats = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326").reset_index(drop=True)
    print(f"  → {len(bats):,} bâtiments générés")
    print(f"  → Abonnés total estimés : {bats['nb_logements'].sum():,}")
    return bats

# =============================================================================
# Positions abonnés avec bruit (v12)
# =============================================================================
def generer_positions_batiment_v11(lat, lon, nb_log_etage, poly_area_m2=225.0,
                                   bat_largeur_m=None, bat_longueur_m=None):
    """
    Distribue nb_log_etage appartements sur la surface du bâtiment.

    v14 : accepte bat_largeur_m et bat_longueur_m séparément pour les bâtiments
    rectangulaires. Si non fournis, on déduit un carré depuis poly_area_m2
    (compatibilité ascendante avec les appels existants).

    La logique de grille est préservée (n_cols × n_rows), mais l'espacement
    dx_m (axe large, couloir) et dy_m (axe profondeur) sont indépendants.
    Cela produit des distances horizontales FAT→abonné réalistes :
      - Bâtiment carré 10×10 m  → dx≈3m,  distances ~4-8 m   → câble 15m
      - Barre courte 25×10 m    → dx≈8m,  distances ~10-16 m  → câble 20m
      - Barre longue 50×10 m    → dx≈18m, distances ~20-32 m  → câble 50m
    """
    if nb_log_etage <= 1:
        return [(round(lat, 6), round(lon, 6))]

    # ── Dimensions effectives ──────────────────────────────────────────────
    if bat_largeur_m is not None and bat_longueur_m is not None:
        usable_x = max(5.0, bat_largeur_m  * 0.80)   # 80% de la largeur utilisable
        usable_y = max(5.0, bat_longueur_m * 0.80)   # 80% de la longueur utilisable
    else:
        # compatibilité : carré déduit de l'aire
        side_m   = max(10.0, poly_area_m2 ** 0.5)
        usable_x = side_m * 0.70
        usable_y = side_m * 0.70

    n_cols = ceil(nb_log_etage ** 0.5)
    n_rows = ceil(nb_log_etage / n_cols)

    # Espacement max par axe, avec minimum 2 m pour que les positions
    # ne se superposent pas et minimum physique (épaisseur d'un appartement).
    dx_m = max(2.0, usable_x / max(n_cols - 1, 1))
    dy_m = max(2.0, usable_y / max(n_rows - 1, 1))

    cos_lat       = cos(radians(lat))
    m_per_deg_lat = 111_000
    m_per_deg_lon = 111_000 * cos_lat

    half_w   = ((n_cols - 1) * dx_m) / 2.0
    half_h   = ((n_rows - 1) * dy_m) / 2.0
    start_lat = lat - half_h / m_per_deg_lat
    start_lon = lon - half_w / m_per_deg_lon

    positions = []
    for i in range(nb_log_etage):
        col   = i % n_cols
        row   = i // n_cols
        p_lat = start_lat + row * dy_m / m_per_deg_lat
        p_lon = start_lon + col * dx_m / m_per_deg_lon
        positions.append((round(p_lat, 6), round(p_lon, 6)))
    return positions

def generer_positions_batiment_v12(lat, lon, nb_log_etage, poly_area_m2=225.0,
                                   bat_largeur_m=None, bat_longueur_m=None):
    positions = generer_positions_batiment_v11(
        lat, lon, nb_log_etage, poly_area_m2, bat_largeur_m, bat_longueur_m
    )
    noisy = []
    for p_lat, p_lon in positions:
        noisy.append((
            round(p_lat + np.random.normal(0, 1.5e-6), 6),
            round(p_lon + np.random.normal(0, 1.5e-6), 6)
        ))
    return noisy

# =============================================================================
# Assignation FATs (modifications minimales pour Pipeline 2)
# =============================================================================
# =============================================================================
# Assignation FATs — Version v13 optimisée (changements minimaux)
# =============================================================================
def assigner_fats_batiment(
    nb_et, nb_log_etage, base_positions, comm_rdc,
    olt_seq, fdt_seq_num, spl_seq_start, fat_num_start,
    elot, zone_id, bat_unique_id, olt_lat, olt_lon, fdt_nom,
    client_seq_start, numero_seq_start, batiment_pav, voie_osm,
    quartier_osm, commune, nbr_logements_total, type_batiment,
    presence_de_commerce, hauteur_etage, bloc, num_bat,
    bat_largeur_m=10.0,   # ← v14 : largeur bâtiment pour palier par abonné
):
    fats_out, spl2_out, abonnes_out = [], [], []
    clients_out, adresses_out, numeros_out = [], [], []
    spl_seq = spl_seq_start
    client_seq = client_seq_start
    numero_seq = numero_seq_start

    assigner_fats_batiment._floor_counters = {}

    def _creer_fat_et_abonnes(appts, usage):
        nonlocal spl_seq, client_seq, numero_seq
        if not appts:
            return

        # Groupement Greedy séquentiel par 8 (comportement réel observé dans V13)
        # Avec 12 log/étage → mélange inévitable sur 1-2 étages consécutifs
        groups = [appts[i:i + 8] for i in range(0, len(appts), 8)]

        for group in groups:
            if not group:
                continue

            portes_group = [apt["porte"] for apt in group]
            etages_group = [apt.get("etage", 0) for apt in group]
            etage_label = str(etages_group[0]) if len(set(etages_group)) == 1 else f"{min(etages_group)}-{max(etages_group)}"

            ref_etage = etages_group[0]
            assigner_fats_batiment._floor_counters[ref_etage] = assigner_fats_batiment._floor_counters.get(ref_etage, 0) + 1
            local_idx = assigner_fats_batiment._floor_counters[ref_etage]

            type_unit = "Commerce" if usage == "commerces" else "Porte"

            # Médiane d'étage analytique (meilleure solution mathématique)
            etage_fat = int(np.median(etages_group))

            # Position FAT = centroïde du groupe
            group_lats = [base_positions[apt["appt_in_floor"]][0] for apt in group]
            group_lons = [base_positions[apt["appt_in_floor"]][1] for apt in group]
            fat_lat = round(sum(group_lats) / len(group_lats), 6)
            fat_lon = round(sum(group_lons) / len(group_lons), 6)

            fat_id = fmt_fat(olt_seq, fdt_seq_num, spl_seq, commune, bloc, num_bat,
                             type_unit, portes_group, etage_label, local_idx)

            # Calculs Pipeline physique réel
            distances_real = []
            distances_snap = []
            wastes = []
            z_coords = []

            for apt in group:
                appt_in_floor = apt["appt_in_floor"]
                ab_lat, ab_lon = base_positions[appt_in_floor]
                etage_apt = apt.get("etage", 0)

                # ── Palier par abonné v14 ──────────────────────────────────
                # La distance de couloir (palier) varie selon la position de
                # l'appartement dans l'étage : les premiers indices sont proches
                # de la cage d'escalier centrale (palier court), les derniers
                # sont au fond du couloir (palier long).
                #
                # Modèle linéaire sur la position dans la grille :
                #   palier_min = 3m  (cage d'escalier adjacente)
                #   palier_max = min(bat_largeur_m * 0.45, 40m)
                #     → pour une barre de 50m : palier_max ≈ 22m
                #     → pour un carré de 10m  : palier_max ≈  4.5m
                #
                # t = position normalisée dans [0, 1] le long du couloir
                # On utilise la colonne GPS (indice dans la grille) comme proxy.
                n_positions  = max(len(base_positions) - 1, 1)
                t            = appt_in_floor / n_positions          # 0 = cage, 1 = fond
                palier_min   = 3.0
                palier_max   = min(bat_largeur_m * 0.45, 40.0)
                palier_i     = palier_min + t * (palier_max - palier_min)

                dist_vertical   = abs(etage_apt - etage_fat) * hauteur_etage
                dist_horizontal = haversine(ab_lat, ab_lon, fat_lat, fat_lon)
                dist_real       = round(dist_vertical + dist_horizontal + palier_i, 2)

                dist_snap = next(
                    (c for c in sorted(PREFAB_LENGTHS) if c >= dist_real),
                    9999
                )
                waste = dist_snap - dist_real
                z = etage_apt * hauteur_etage

                distances_real.append(dist_real)
                distances_snap.append(dist_snap)
                wastes.append(waste)
                z_coords.append(z)

            distance_fat_moy = round(sum(distances_real) / len(distances_real), 2)

            fats_out.append({
                "id": fat_id,
                "nom_FDT": fdt_nom,
                "num_de_groupe": local_idx,
                "latitude": fat_lat,
                "longitude": fat_lon,
                "etage_fat": etage_fat,
                "usage": usage,
                "nb_ports": settings.FAT_CAPACITY,
                "nb_abonnes_sim": len(group),
                "nb_etages_bat": nb_et,
                "nb_log_etage": nb_log_etage,
                "zone_id": zone_id,
                "distance_FAT_m": distance_fat_moy,
                "distance_real_m": distance_fat_moy,
                "cable_prefab_m": int(np.median(distances_snap)),
            })

            # Splitter N2
            for dn in range(1, settings.FAT_CAPACITY + 1):
                spl2_out.append({
                    "id": f"{fat_id}-DOWN-{dn:02d}",
                    "nom_FAT": fat_id,
                    "id_splitter1": f"{fdt_nom}-S{spl_seq:02d}",
                    "rapport_de_division": f"1:{settings.SPLITTER_N2_RATIO}",
                    "port_splitter": f"{fdt_nom}-S{spl_seq:02d}-DOWN-{dn}",
                    "etat": "utilisé" if dn <= len(group) else "libre",
                    "zone_id": zone_id,
                })

            # Abonnés
            for i, apt in enumerate(group):
                appt_in_floor = apt["appt_in_floor"]
                porte = apt["porte"]
                etage_apt = apt.get("etage", 0)
                base_lat, base_lon = base_positions[appt_in_floor]

                cc = fmt_code_client(client_seq)

                op = np.random.choice(list(OPERATEURS.keys()))
                prefix = np.random.choice(OPERATEURS[op])
                contact = int(prefix + f"{np.random.randint(1_000_000, 9_999_999):07d}")

                abonnes_out.append({
                    "code_client": cc,
                    "latitude": base_lat,
                    "longitude": base_lon,
                    "etage": etage_apt,
                    "porte": porte,
                    "id_batiment": batiment_pav,
                    "id_zone": zone_id,
                    "FAT_relative": fat_id,
                    "usage": usage,
                    "etage_fat": etage_fat,
                    "distance_real_m": round(distances_real[i], 2),
                    "distance_snap_m": distances_snap[i],
                    "cable_prefab_m": distances_snap[i],
                    "waste_m": round(wastes[i], 2),
                    "z_coord": round(z_coords[i], 2),
                    "nbr_etages": nb_et,
                    "nbr_logements_par_etage": nb_log_etage,
                    "nbr_logements_total": nbr_logements_total,
                    "type_batiment": type_batiment,
                    "presence_de_commerce": presence_de_commerce,
                    "Hauteur par étage (m)": hauteur_etage,
                })

                clients_out.append({
                    "code_client": cc,
                    "contact": contact,
                    "nom": f"{np.random.choice(PRENOMS)} {np.random.choice(NOMS_FAM)}",
                })

                adresses_out.append({
                    "code_client": cc,
                    "batiment_pav": batiment_pav,
                    "voie": voie_osm if voie_osm else f"FAT{ZONE_CODE}-{elot}",
                    "quartier": quartier_osm,
                    "commune": commune,
                    "wilaya": wilaya_nom,
                })

                numeros_out.append({
                    "num_de_groupe": int(f"4{1_800_000 + numero_seq:07d}"),
                    "code_client": cc,
                    "region_relative": zone_id,
                    "FAT_relative": fat_id,
                })

                client_seq += 1
                numero_seq += 1

            spl_seq += 1

    # RDC Commerce
    if presence_de_commerce == 1:
        nb_com = np.random.randint(2, 5)
        appts_rdc = [{"appt_in_floor": i, "porte": f"Commerce {i+1}", "etage": 0} for i in range(nb_com)]
        _creer_fat_et_abonnes(appts_rdc, "commerces")

    # Étages résidentiels
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

    _creer_fat_et_abonnes(appts_residentiels, "logements")

    return {
        "fats": fats_out,
        "spl2": spl2_out,
        "abonnes": abonnes_out,
        "clients": clients_out,
        "adresses": adresses_out,
        "numeros": numeros_out,
        "spl_seq": spl_seq,
        "client_seq": client_seq,
        "numero_seq": numero_seq,
    }

# =============================================================================
# Génération tables + merge (inchangés sauf appel v12)
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
        poly_area_m2  = float(row.get("poly_area_m2",  225.0))
        bat_largeur_m = float(row.get("bat_largeur_m",  15.0))
        bat_longueur_m= float(row.get("bat_longueur_m", 15.0))
        base_positions = generer_positions_batiment_v12(
            lat, lon, nb_log_etage, poly_area_m2,
            bat_largeur_m=bat_largeur_m, bat_longueur_m=bat_longueur_m
        )

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
            bat_largeur_m=bat_largeur_m,   # ← v14
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
# Merge (identique à v11)
# =============================================================================
def merge_all_tables():
    print("🔄 Lecture de TOUTES les tables depuis donnee_generee_v14/...")
    base = OUTPUT_DIR

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
        "lat_fat", "lon_fat",
        "etage_fat",                        # target ML Modèle A
        "nb_abonnes_sim",
        "distance_FAT_m",
        "distance_real_m",                  # physique correcte
        "cable_prefab_m",                   # snap correct
        "distance_snap_m",
        "waste_m",
        "z_coord",
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

if __name__ == "__main__":
    print("=" * 80)
    print("SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM ORAN v14")
    print("Pipeline réel : Greedy séquentiel par 8 + Médiane analytique + Snap déterministe")
    print("v14 : Géométrie rectangulaire + palier par abonné → spread câbles réaliste")
    print("=" * 80)

    bats = generer_batiments_oran(n_blocs_per_commune=5, n_nums_per_bloc=5, rng_seed=2026)

    tables = generer_tables(bats)

    for nom, df_table in tables.items():
        path = os.path.join(OUTPUT_DIR, f"{nom}.csv")
        df_table.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  ✓ {nom:15s} → {len(df_table):7,} lignes")

    df_final = merge_all_tables()

    print("\n" + "=" * 80)
    print("DATASET V14 GÉNÉRÉ AVEC SUCCÈS")
    print("• Groupement     : Greedy séquentiel par 8")
    print("• Placement FAT  : Médiane d'étage analytique")
    print("• Snap câble     : Règle déterministe (pas de ML)")
    print("• Palier         : Par abonné selon position dans la grille")
    print("• Géométrie      : Rectangulaire (aspect_ratio 1.0-3.0)")
    print("• ML             : Uniquement pour prédire K")
    print(f"  Abonnés total    : {len(df_final):,}")
    print(f"  FATs générés     : {df_final['FAT_relative'].nunique():,}")
    print(f"  Fichier final    : {OUTPUT_DIR}\\dataset_fusionnee_final.csv")
    print("=" * 80)