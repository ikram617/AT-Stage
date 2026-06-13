from __future__ import annotations

import argparse
import hashlib
import warnings
from math import ceil, cos, radians, sqrt, sin
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES — identiques à config.py et fat_planner_hybride.py
# ══════════════════════════════════════════════════════════════════════════════

FAT_CAPACITY    = 8          # abonnés max par FAT — utilisé UNIQUEMENT pour K_planifie
RANDOM_SEED     = 2026

# Encodage type bâtiment → entier (identique à fat_planner_hybride.py)
TYPE_BAT_MAP = {
    "AADL": 0, "HLM": 1, "LPP": 2,
    "LPA":  3, "LSL": 4, "CNEP": 5, "PRIVE": 6,
}

# ── Distributions géométriques réalistes par type de bâtiment ────────────────
BUILDING_PROFILES: dict[str, dict] = {
    "AADL": {
        "etages":        (5, 15),
        "log_etage":     (4, 6),
        "hauteur":       (2.9, 3.2),
        "prob_commerce": 0.45,
        "prob_weight":   0.38,
    },
    "HLM": {
        "etages":        (4, 8),
        "log_etage":     (4, 6),
        "hauteur":       (2.6, 2.9),
        "prob_commerce": 0.20,
        "prob_weight":   0.22,
    },
    "LPP": {
        "etages":        (4, 10),
        "log_etage":     (4, 8),
        "hauteur":       (3.0, 3.4),
        "prob_commerce": 0.35,
        "prob_weight":   0.15,
    },
    "LPA": {
        "etages":        (4, 9),
        "log_etage":     (4, 7),
        "hauteur":       (2.8, 3.1),
        "prob_commerce": 0.25,
        "prob_weight":   0.10,
    },
    "LSL": {
        "etages":        (3, 7),
        "log_etage":     (2, 4),
        "hauteur":       (2.6, 2.8),
        "prob_commerce": 0.15,
        "prob_weight":   0.08,
    },
    "CNEP": {
        "etages":        (5, 12),
        "log_etage":     (4, 6),
        "hauteur":       (3.0, 3.3),
        "prob_commerce": 0.50,
        "prob_weight":   0.04,
    },
    "PRIVE": {
        "etages":        (3, 6),
        "log_etage":     (2, 6),
        "hauteur":       (3.1, 3.6),
        "prob_commerce": 0.30,
        "prob_weight":   0.03,
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# MODE ESTIMÉ — Taux d'occupation phase initiale uniquement
#
# Le Mode Estimé cible exclusivement les nouveaux bâtiments identifiés
# via OSM, avant tout raccordement. Seule la phase "initial" (0-6 mois
# post-livraison) est pertinente.
#
# Sources :
# [1] ONS RGPH 2008 : vacance nationale 13.95% → occupation ~86% (borne haute)
# [2] LKeria 2016 : vacance estimée 27% → occupation ~73% (borne basse)
# [3] Raisonnement métier par programme algérien :
#     - HLM/LSL : attribution prioritaire → occupation rapide dès livraison
#     - AADL : liste d'attente longue → montée progressive
#     - PRIVE : investissement possible → vacance initiale plus haute
# ══════════════════════════════════════════════════════════════════════════════

OCCUPATION_RATES_ESTIME: dict[str, tuple[float, float]] = {
    # type → (taux_min, taux_max) — phase initiale uniquement
    "AADL":  (0.35, 0.45),  # montée lente — liste d'attente AADL pluriannuelle
    "LPP":   (0.40, 0.55),  # achat promotionnel — délai installation variable
    "HLM":   (0.55, 0.70),  # social locatif — attribution prioritaire rapide
    "LSL":   (0.50, 0.65),  # social locatif — similaire HLM
    "LPA":   (0.50, 0.65),  # promotionnel aidé — intermédiaire
    "CNEP":  (0.45, 0.60),  # financement bancaire — délai variable
    "PRIVE": (0.60, 0.80),  # résidentiel privé — propriétaires motivés
}

SEP1 = "═" * 72
SEP2 = "─" * 72


# ══════════════════════════════════════════════════════════════════════════════
# CIBLE ML : K_PLANIFIE
# ══════════════════════════════════════════════════════════════════════════════

def calculer_k_planifie(nb_etages: int, nb_log_etage: int,
                         presence_commerce: bool) -> int:
    """
    K_planifie = nombre de FATs que l'ingénieur AT dimensionnerait
    pour ce bâtiment à 100% d'occupation (planification réseau).

    C'est la CIBLE ML du K-Predictor.
    Deux bâtiments identiques → même K_planifie (déterministe).
    Le modèle apprend K = f(géométrie), pas K = f(taux simulé).
    """
    n_total_res = nb_etages * nb_log_etage
    k_res       = ceil(n_total_res / FAT_CAPACITY)
    k_total     = k_res + (1 if presence_commerce else 0)
    return k_total


# ══════════════════════════════════════════════════════════════════════════════
# TAUX D'OCCUPATION — MODE ESTIMÉ (phase initiale uniquement)
# ══════════════════════════════════════════════════════════════════════════════

def _tirer_taux_estime(type_bat: str,
                       rng: np.random.RandomState) -> float:
    """
    Tire un taux d'occupation initial pour un NOUVEAU bâtiment.

    Le mode estimé ne connaît pas la phase — il suppose toujours "initial"
    car il cible des bâtiments OSM récemment identifiés, avant raccordement.

    Retourne seulement le taux (phase toujours "initial").
    """
    lo, hi = OCCUPATION_RATES_ESTIME.get(type_bat, (0.40, 0.65))
    return float(rng.uniform(lo, hi))


# ══════════════════════════════════════════════════════════════════════════════
# PRÉDICTION INTELLIGENTE DES ABONNÉS — MODE ESTIMÉ
# ══════════════════════════════════════════════════════════════════════════════

def _predire_abonnes_estime(
    tous_appts: list,
    nb_etages: int,
    nb_log_etage: int,
    taux_occupation: float,
    rng: np.random.RandomState,
) -> list:
    """
    Prédit quels logements seront occupés — distribution uniforme par étage.

    Corrections v24 — quotas déséquilibrés supprimés :
    ───────────────────────────────────────────────────
    • Biais étages inférieurs réduit à 10 % max (était 30 % → vidait les étages hauts).
    • Remainder distribué par ordre décroissant de fraction (méthode Hamilton/largest-remainder)
      → garantit que l'écart max entre deux étages est ≤ 1 logement.
    • Pas de tirage RNG dans la boucle de quotas → déterministe, reproductible.
    • La randomisation locale par étage (qui choisit QUELS appts sont habités)
      est conservée avec seed XOR(base, étage×997) comme avant.

    Exemple (4 logements/étage, 10 étages, taux=0.60) :
      n_predit = 24
      quota_base = 2 par étage, reste = 4 → 4 étages à 3, 6 étages à 2
      → max écart entre étages = 1  ✓
    """
    n_total = len(tous_appts)
    if n_total == 0:
        return []

    n_predit = max(1, min(n_total, round(n_total * taux_occupation)))

    # Grouper par étage
    etage_to_appts: dict[int, list] = {}
    for apt in tous_appts:
        etage_to_appts.setdefault(apt["etage"], []).append(apt)

    etages_tries = sorted(etage_to_appts.keys())
    nb_floors = len(etages_tries)
    if nb_floors == 0:
        return []

    # ── Biais léger sur les étages inférieurs (10 % max) ─────────────────────
    # Valeur 0.10 au lieu de 0.30 : assez pour être réaliste, pas assez pour
    # vider les étages supérieurs.
    weights = np.array(
        [1.0 + 0.10 * (nb_etages - e) / max(nb_etages, 1) for e in etages_tries]
    )
    weights /= weights.sum()

    # ── Quotas par étage — méthode largest-remainder (Hamilton) ──────────────
    # Garantit : sum(quotas) == n_predit et max|quotas[i] - quotas[j]| ≤ 1
    frac_quotas = n_predit * weights
    quotas      = np.floor(frac_quotas).astype(int)

    # Plafonner à la capacité réelle de chaque étage
    max_per_floor = np.array([len(etage_to_appts[e]) for e in etages_tries])
    quotas        = np.minimum(quotas, max_per_floor)

    # Distribuer le reste par ordre décroissant de fraction résiduelle,
    # en évitant les étages déjà au maximum
    reste = n_predit - int(quotas.sum())
    if reste > 0:
        fractions = frac_quotas - quotas
        # Mettre -1 sur les étages saturés pour les exclure du tri
        fractions = np.where(quotas >= max_per_floor, -1.0, fractions)
        ordre = np.argsort(fractions)[::-1]  # décroissant
        for idx in ordre:
            if reste <= 0:
                break
            if quotas[idx] < max_per_floor[idx]:
                quotas[idx] += 1
                reste -= 1

    # ── Sélection aléatoire DANS chaque étage ─────────────────────────────────
    selected = []
    for ei, etage in enumerate(etages_tries):
        q = int(quotas[ei])
        if q <= 0:
            continue
        appts_floor = etage_to_appts[etage]

        # Seed XOR par étage — indépendant de l'ordre des étages (hérité v16+)
        base_seed = int(rng.randint(0, 2**31))
        floor_seed = base_seed ^ (etage * 997)
        rng_local = np.random.RandomState(floor_seed % (2**31))

        indices = list(range(len(appts_floor)))
        rng_local.shuffle(indices)
        for idx in indices[:q]:
            selected.append(appts_floor[idx])

    selected.sort(key=lambda a: (a["etage"], a.get("appt_in_floor", 0)))
    return selected


# ══════════════════════════════════════════════════════════════════════════════
# GÉNÉRATION DES POSITIONS DES APPARTEMENTS
# ══════════════════════════════════════════════════════════════════════════════

def _generer_positions_batiment(
    lat_centre: float,
    lon_centre: float,
    nb_log: int,
    rng: np.random.RandomState,
    nb_etages: int = 1,
    bat_largeur_m: float = 0.0,
    bat_longueur_m: float = 0.0,
) -> dict[int, list[tuple[float, float]]]:
    """
    Génère les positions en grille architecturale par paliers (aucun bruit/jitter).

    Règles :
    ─────────
    • n_cols  = nb_log (nombre de paliers = logements par étage).
    • n_rows  = nb_etages (un point par étage sur chaque palier).
    • distance_entre_paliers = longueur_facade / nb_log_etage.
    • Chaque point est placé au centre de son logement (centre de palier × centre d'étage).
    • Alignement vertical parfait : même X pour tous les étages d'un même palier.
    • Orientation du bâtiment : tirée une seule fois depuis le RNG bâtiment
      et appliquée à TOUS les étages (façade cohérente).
    """
    # ── Grille architecturale : paliers × étages ──────────────────────────────
    n_cols = max(1, nb_log)      # paliers = logements par étage
    n_rows = max(1, nb_etages)   # points par palier = nombre d'étages

    cos_lat       = cos(radians(lat_centre))
    m_per_deg_lat = 111_000
    m_per_deg_lon = 111_000 * cos_lat

    # ── Longueur de façade et profondeur (80 % de la surface bâtiment) ────────
    if bat_largeur_m > 0 and bat_longueur_m > 0:
        facade_m   = max(5.0, bat_largeur_m  * 0.80)   # axe des paliers (X)
        profond_m  = max(5.0, bat_longueur_m * 0.80)   # axe des étages  (Y)
    else:
        side_m    = max(10.0, sqrt(nb_log * 25.0))
        facade_m  = side_m * 0.70
        profond_m = side_m * 0.70

    # distance_entre_paliers = longueur_facade / nb_log_etage (exigence §3)
    dx_m = facade_m / n_cols   # espacement centre-à-centre des paliers

    # espacement vertical entre étages (centre de logement)
    dy_m = profond_m / n_rows if n_rows > 1 else 0.0

    # Orientation globale du bâtiment (constante pour tous les étages)
    angle_bat_rad = rng.uniform(0, 2 * np.pi)
    cos_a = cos(angle_bat_rad)
    sin_a = sin(angle_bat_rad)

    # ── Calcul des positions de base (une seule fois pour tous les étages) ────
    points_base = []
    for palier in range(nb_log):
        # Centre du palier le long de la façade — alignement X constant
        lx = (palier + 0.5 - n_cols / 2.0) * dx_m
        ly = 0.0  # fixe : même position GPS pour tous les étages d'un palier

        # Rotation globale du bâtiment
        rx = lx * cos_a - ly * sin_a
        ry = lx * sin_a + ly * cos_a

        # Conversion en degrés GPS
        p_lat = round(lat_centre + ry / m_per_deg_lat, 7)
        p_lon = round(lon_centre + rx / m_per_deg_lon, 7)

        points_base.append((p_lat, p_lon))

    # Puis chaque étage reçoit la même liste
    positions_par_etage = {}
    for etage in range(nb_etages + 1):  # 0 = RDC/commerce, 1..N = résidentiel
        positions_par_etage[etage] = list(points_base)

    return positions_par_etage



# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCTION D'UNE LIGNE DE DATASET
# ══════════════════════════════════════════════════════════════════════════════

def _make_row(
        bat_id: str,
        etage: int,
        appt_in_floor: int,
        usage: str,
        lat_abonne: float,
        lon_abonne: float,
        nb_etages: int,
        nb_log_etage: int,
        type_bat: str,
        presence_commerce: bool,
        hauteur_etage: float,
        surface_m2: float,
        nb_logements_total: int,
        k_planifie: int,
        taux_occupation: float,
        phase: str,
        commune: str,
        seq: int,
        occupe: int,
        nb_logements_habites: int,  # ← NOUVEAU
) -> dict:
    code_client = f"1{seq:013d}" if occupe == 1 else None

    return {
        "code_client": code_client,
        "id_batiment": bat_id,
        "etage": etage,
        "appt_in_floor": appt_in_floor,
        "porte": f"Etage{etage}-{appt_in_floor + 1}",
        "usage": usage,
        "occupe": occupe,
        "lat_abonne": lat_abonne,
        "lon_abonne": lon_abonne,
        "nb_etages_bat": nb_etages,
        "nb_log_etage": nb_log_etage,
        "nb_logements_total": nb_logements_total,
        "nb_logements_habites": nb_logements_habites,  # ← Ajouté
        "surface_m2": round(surface_m2, 1),
        "type_batiment": type_bat,
        "presence_de_commerce": int(presence_commerce),
        "Hauteur par étage (m)": hauteur_etage,
        "hauteur_bat_totale_m": round(nb_etages * hauteur_etage, 2),
        "taux_occupation": round(taux_occupation, 3),
        "phase_occupation": phase,
        "commune": commune,
        "K_planifie": k_planifie,
    }


def _generer_positions_paliers(
    lat_centre: float,
    lon_centre: float,
    nb_log_etage: int,
    bat_largeur_m: float = 0.0,
    angle_rad: float = 0.0,
) -> list[tuple[float, float]]:
    """
    Retourne nb_log_etage positions GPS, réparties uniformément
    le long de la façade (axe X), centrées sur le bâtiment.
    distance_entre_paliers = largeur_facade / nb_log_etage
    Alignement vertical garanti : même (lat, lon) pour tous les étages
    d'un même palier.
    """
    cos_lat       = cos(radians(lat_centre))
    m_per_deg_lat = 111_000
    m_per_deg_lon = 111_000 * cos_lat

    # Longueur de façade
    if bat_largeur_m > 0:
        facade_m = max(5.0, bat_largeur_m * 0.80)
    else:
        facade_m = max(10.0, sqrt(nb_log_etage * 25.0) * 0.70)

    # distance centre-à-centre entre paliers = façade / nb_log_etage
    dx_m = facade_m / nb_log_etage

    cos_a, sin_a = cos(angle_rad), sin(angle_rad)

    positions = []
    # np.linspace sur la façade : centres des nb_log_etage logements
    xs = np.linspace(
        -(facade_m - dx_m) / 2,   # centre du 1er logement
         (facade_m - dx_m) / 2,   # centre du dernier logement
        nb_log_etage
    )

    for lx in xs:
        ly = 0.0          # tous sur la façade principale (profondeur = 0)
        rx = lx * cos_a - ly * sin_a
        ry = lx * sin_a + ly * cos_a
        p_lat = round(lat_centre + ry / m_per_deg_lat, 7)
        p_lon = round(lon_centre + rx / m_per_deg_lon, 7)
        positions.append((p_lat, p_lon))

    return positions


def _simuler_batiment_estime(
    bat_idx:     int,
    bat_id:      str,
    rng:         np.random.RandomState,
    commune:     str,
    lat_base:    float,
    lon_base:    float,
    seq_start:   int,
) -> tuple[list[dict], int]:


    # ── 1. Géométrie ──────────────────────────────────────────────────────────
    types  = list(BUILDING_PROFILES.keys())
    probs  = np.array([BUILDING_PROFILES[t]["prob_weight"] for t in types])
    probs /= probs.sum()
    type_bat = rng.choice(types, p=probs)
    prof     = BUILDING_PROFILES[type_bat]

    nb_etages    = int(rng.randint(prof["etages"][0],    prof["etages"][1] + 1))
    nb_log_etage = int(rng.randint(prof["log_etage"][0], prof["log_etage"][1] + 1))
    hauteur_et   = round(float(rng.uniform(*prof["hauteur"])), 2)
    has_commerce = bool(rng.random() < prof["prob_commerce"])

    lat_c = round(lat_base + rng.uniform(-0.025, 0.025), 6)
    lon_c = round(lon_base + rng.uniform(-0.025, 0.025), 6)

    # Géométrie rectangulaire réaliste : ~25 m² par logement (norme F3/F4 algérienne)
    surface_m2   = float(np.clip(nb_log_etage * 25.0, 100.0, 1200.0))
    aspect_ratio = float(rng.uniform(1.0, 3.0))   # 1=carré, 2=barre courte, 3=barre longue
    bat_largeur_m  = round(float((surface_m2 * aspect_ratio) ** 0.5), 1)   # axe façade (couloir)
    bat_longueur_m = round(float((surface_m2 / aspect_ratio) ** 0.5), 1)   # axe profondeur
    nb_total      = nb_etages * nb_log_etage

    # ── 2. K_planifie ─────────────────────────────────────────────────────────
    k_planifie = calculer_k_planifie(nb_etages, nb_log_etage, has_commerce)

    # ── 3. Taux occupation INITIAL ────────────────────────────────────────────
    taux  = _tirer_taux_estime(type_bat, rng)
    phase = "initial"

    # ── 4. Positions grille logements ─────────────────────────────────────────
    angle_rad = rng.uniform(0, 2 * np.pi)  # tiré ici, une seule fois
    positions_paliers = _generer_positions_paliers(
        lat_c, lon_c, nb_log_etage,
        bat_largeur_m=bat_largeur_m,
        angle_rad=angle_rad,
    )

    # ── 5. Construire TOUS les logements résidentiels ─────────────────────────
    # Règle : nb_log_etage colonnes × nb_etages points chacune, toujours.
    # Étages résidentiels = 1..nb_etages (étage 0 réservé commerce, géré séparément).
    tous_res = []
    for appt_i in range(nb_log_etage):  # nb_log_etage colonnes
        lat_p, lon_p = positions_paliers[appt_i]
        for et in range(1, nb_etages + 1):  # toujours nb_etages points par colonne
            tous_res.append({"etage": et, "appt_in_floor": appt_i,
                             "lat": lat_p, "lon": lon_p})

    # ── 6. Prédire les abonnés occupés résidentiels ───────────────────────────
    habites_res = _predire_abonnes_estime(
        tous_res, nb_etages, nb_log_etage, taux, rng
    )
    habites_set = set(id(a) for a in habites_res)

    # ── 7. Commerces RDC (calcul avant nb_logements_habites) ─────────────────
    n_com_occupes = 0
    if has_commerce:
        lo_com, hi_com = OCCUPATION_RATES_ESTIME.get(type_bat, (0.45, 0.70))
        taux_com = min(1.0, float(rng.uniform(lo_com, hi_com)) * 1.15)

        appts_com = [{"etage": 0, "appt_in_floor": i, "lat": lat_p, "lon": lon_p}
                     for i, (lat_p, lon_p) in enumerate(positions_paliers[:nb_log_etage])]
        n_com_total = len(appts_com)
        n_com_occupes = max(1, round(n_com_total * taux_com)) if n_com_total > 0 else 0

    # ── 8. Nombre total d'abonnés occupés dans le bâtiment ───────────────────
    nb_logements_habites = len(habites_res) + n_com_occupes

    # ── 9. Créer les lignes résidentielles ────────────────────────────────────
    rows = []
    seq = seq_start

    for apt in tous_res:
        is_habite = int(id(apt) in habites_set)
        rows.append(_make_row(
            bat_id=bat_id,
            etage=apt["etage"],
            appt_in_floor=apt["appt_in_floor"],
            usage="logements",
            lat_abonne=apt["lat"],
            lon_abonne=apt["lon"],
            nb_etages=nb_etages,
            nb_log_etage=nb_log_etage,
            type_bat=type_bat,
            presence_commerce=has_commerce,
            hauteur_etage=hauteur_et,
            surface_m2=surface_m2,
            nb_logements_total=nb_total,
            nb_logements_habites=nb_logements_habites,   # ← Important
            k_planifie=k_planifie,
            taux_occupation=taux,
            phase=phase,
            commune=commune,
            seq=seq,
            occupe=is_habite,
        ))
        seq += 1

    # ── 10. Créer les lignes commerces ───────────────────────────────────────
    if has_commerce:
        idx_com = list(range(n_com_total))
        rng.shuffle(idx_com)
        habites_com_idx = set(idx_com[:n_com_occupes])

        for i, apt in enumerate(appts_com):
            is_hab_com = int(i in habites_com_idx)
            rows.append(_make_row(
                bat_id=bat_id,
                etage=0,
                appt_in_floor=apt["appt_in_floor"],
                usage="commerces",
                lat_abonne=apt["lat"],
                lon_abonne=apt["lon"],
                nb_etages=nb_etages,
                nb_log_etage=nb_log_etage,
                type_bat=type_bat,
                presence_commerce=has_commerce,
                hauteur_etage=hauteur_et,
                surface_m2=surface_m2,
                nb_logements_total=nb_total,
                nb_logements_habites=nb_logements_habites,   # ← Important
                k_planifie=k_planifie,
                taux_occupation=taux,
                phase=phase,
                commune=commune,
                seq=seq,
                occupe=is_hab_com,
            ))
            seq += 1

    return rows, seq


# ══════════════════════════════════════════════════════════════════════════════
# CITÉS RÉSIDENTIELLES D'ORAN (inchangé v18/v19)
# ══════════════════════════════════════════════════════════════════════════════

CITES_RESIDENTIELLES_ORAN: list[dict] = [
    {"nom": "HLM Gambetta",          "commune": "Es-Seddikia",  "type": "HLM"},
    {"nom": "Cité Les Castors",      "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Petit Lac",        "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Militaire",        "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Radieuse",         "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Protin",           "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Les Troènes",      "commune": "Oran",         "type": "HLM"},
    {"nom": "Cité Jardins Gambetta", "commune": "Es-Seddikia",  "type": "HLM"},
    {"nom": "Cité Point du Jour",    "commune": "Es-Seddikia",  "type": "Résidentiel"},
    {"nom": "Cité AADL Aïn El Beïda",    "commune": "Es-Senia",   "type": "AADL"},
    {"nom": "Cité 2700 Logements AADL",  "commune": "Es-Senia",   "type": "AADL"},
    {"nom": "Cité Ahmed Zabana",         "commune": "Misserghin", "type": "AADL"},
    {"nom": "Cité Belgaïd",              "commune": "Bir El Djir","type": "LPL / AADL"},
    {"nom": "Cité 1377 Logements AADL",  "commune": "Bir El Djir","type": "AADL"},
    {"nom": "Cité 400 Logements AADL",   "commune": "Oran",       "type": "AADL"},
    {"nom": "Canastel",               "commune": "El Menzeh", "type": "Résidentiel haut standing"},
    {"nom": "Akid Lotfi",             "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Les Palmiers",           "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Miramar",                "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Bel Air",                "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Maraval",                "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Saint-Hubert",           "commune": "Oran",      "type": "Résidentiel"},
    {"nom": "Cité Les Glycines",      "commune": "Oran",      "type": "HLM / Résidentiel"},
    {"nom": "Cité El Wiam",      "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité Ibn Rochd",    "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité El Menzah",    "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité El Emir",      "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité El Khalidia",  "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité Chouhada",     "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité Essaada",      "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité Dar Beida",    "commune": "Oran",         "type": "Résidentiel"},
    {"nom": "Cité Jamel",        "commune": "Bir El Djir",  "type": "Résidentiel"},
    {"nom": "Millenium",         "commune": "Bir El Djir",  "type": "Résidentiel moderne"},
    {"nom": "Résidence El Anaqa","commune": "Bir El Djir",  "type": "Résidence"},
]

BLOC_LETTERS = [chr(ord("A") + i) for i in range(10)]

COMMUNES_COORDS: dict[str, tuple[float, float]] = {
    "Oran":         (35.697, -0.633),
    "Es-Seddikia":  (35.712, -0.642),
    "Es-Senia":     (35.651, -0.603),
    "Bir El Djir":  (35.720, -0.568),
    "Misserghin":   (35.636, -0.794),
    "El Menzeh":    (35.738, -0.596),
}


def _nom_vers_id(nom: str) -> str:
    import unicodedata
    nfd      = unicodedata.normalize("NFD", nom)
    ascii_n  = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    cleaned  = "".join(c for c in ascii_n if c.isalnum() or c == " ")
    return "".join(w.capitalize() for w in cleaned.split())


_rng_init = np.random.RandomState(RANDOM_SEED + 1)
CITES_TABLE: list[dict] = []
for _cite in CITES_RESIDENTIELLES_ORAN:
    _slug     = _nom_vers_id(_cite["nom"])
    _nb_blocs = int(_rng_init.randint(3, 11))
    _commune  = _cite["commune"]
    _coords   = COMMUNES_COORDS.get(_commune, COMMUNES_COORDS["Oran"])
    for _bi, _letter in enumerate(BLOC_LETTERS[:_nb_blocs]):
        CITES_TABLE.append({
            "cite_slug":   _slug,
            "bloc":        _letter,
            "id_batiment": f"Cite_{_slug}Bloc_{_letter}",
            "commune":     _commune,
            "lat":         _coords[0],
            "lon":         _coords[1],
        })

_N_CITES_BLOCS = len(CITES_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# GÉNÉRATEUR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def generer_dataset(
    n_bats:    int = 1000,
    output:    str | None = None,
    seed:      int = RANDOM_SEED,
    verbose:   bool = True,
) -> pd.DataFrame:
    """
    Génère le dataset complet d'entraînement — v24.

    Responsabilité unique : abonnés + localisations.
    Aucune opération de place
    ment FAT ou câble.

    Paramètres
    ──────────
    n_bats  : nombre de bâtiments à simuler
    output  : chemin CSV de sortie
    seed    : graine aléatoire
    verbose : affichage des statistiques

    Retourne
    ────────
    pd.DataFrame : dataset complet (21 colonnes — sans FAT/câble/distance)
    """
    rng      = np.random.RandomState(seed)
    all_rows = []
    seq      = 1

    cites_indices = list(range(_N_CITES_BLOCS))
    rng.shuffle(cites_indices)

    if verbose:
        print(f"\n{SEP1}")
        print(f"  GÉNÉRATEUR v24 — FTTH Smart Planner · Algérie Télécom")
        print(SEP1)
        print(f"  Bâtiments à simuler : {n_bats}")
        print(f"  Seed               : {seed}")
        print(f"  Cités disponibles  : {len(CITES_RESIDENTIELLES_ORAN)} cités "
              f"→ {_N_CITES_BLOCS} combinaisons")


    # APRÈS — id_batiment toujours unique grâce au suffixe bat_idx
    assigned = [cites_indices[i % _N_CITES_BLOCS] for i in range(n_bats)]

    for bat_idx, cite_idx in enumerate(assigned, start=1):
        cite_entry = CITES_TABLE[cite_idx]
        bat_id = f"{cite_entry['id_batiment']}_{bat_idx:04d}"  # ← UNIQUE garanti
        commune = cite_entry["commune"]
        lat_c = cite_entry["lat"]
        lon_c = cite_entry["lon"]

        bat_seed = (int(seed)
                    ^ int(hashlib.md5(f"estime_{bat_idx}".encode())
                          .hexdigest()[:8], 16))
        bat_rng  = np.random.RandomState(bat_seed % (2**31))

        rows, seq = _simuler_batiment_estime(
            bat_idx=bat_idx,
            bat_id=bat_id,
            rng=bat_rng,
            commune=commune,
            lat_base=lat_c,
            lon_base=lon_c,
            seq_start=seq,
        )
        all_rows.extend(rows)

        if verbose and bat_idx % 50 == 0:
            print(f"  → {bat_idx:>4}/{n_bats} bâtiments "
                  f"· {len(all_rows):>7,} lignes")

    df = pd.DataFrame(all_rows)

    # ── Vérification colonnes obligatoires ────────────────────────────────────
    REQUIRED_COLS = [
        "code_client", "id_batiment",
        "etage", "appt_in_floor", "porte", "usage", "occupe",
        "lat_abonne", "lon_abonne",
        "nb_etages_bat", "nb_log_etage", "nb_logements_total", "surface_m2",
        "type_batiment", "presence_de_commerce",
        "Hauteur par étage (m)", "hauteur_bat_totale_m",
        "taux_occupation", "phase_occupation",
        "commune", "K_planifie",
    ]
    manquantes = [c for c in REQUIRED_COLS if c not in df.columns]
    if manquantes:
        raise RuntimeError(f"Colonnes manquantes dans le dataset : {manquantes}")

    # Vérifier qu'aucune colonne FAT/câble ne s'est glissée
    FORBIDDEN_COLS = [
        "FAT_relative", "lat_fat", "lon_fat", "etage_fat",
        "distance_real_m", "cable_prefab_m", "waste_m",
        "fat_load_balance_ok", "K_connectes",
    ]
    intrus = [c for c in FORBIDDEN_COLS if c in df.columns]
    if intrus:
        raise RuntimeError(
            f"Colonnes FAT/câble trouvées dans le dataset (interdit v24) : {intrus}"
        )

    if verbose:
        _afficher_stats(df)

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output, index=False, encoding="utf-8-sig")
        if verbose:
            taille_mb = Path(output).stat().st_size / 1024**2
            print(f"\n  💾 Dataset sauvegardé : {output}  ({taille_mb:.1f} MB)")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# AFFICHAGE DES STATISTIQUES
# ══════════════════════════════════════════════════════════════════════════════

def _afficher_stats(df: pd.DataFrame) -> None:
    """Affiche un audit complet du dataset v24."""
    print(f"\n{SEP1}")
    print(f"  AUDIT DATASET v24")
    print(SEP1)

    n_total = len(df)
    n_bats  = df["id_batiment"].nunique()

    n_habites = int((df["occupe"] == 1).sum())
    n_vides   = int((df["occupe"] == 0).sum())

    print(f"  Logements total        : {n_total:>10,}  (habités + vides)")
    print(f"    Habités (abonnés)    : {n_habites:>10,}  ({n_habites/n_total*100:5.1f}%)")
    print(f"    Vides                : {n_vides:>10,}  ({n_vides/n_total*100:5.1f}%)")
    print(f"  Bâtiments uniques      : {n_bats:>10,}")

    # ── Vérification : lat/lon renseignés pour TOUS ───────────────────────────
    n_lat_null = df["lat_abonne"].isna().sum()
    n_lon_null = df["lon_abonne"].isna().sum()
    print(f"\n  ── Localisations ──")
    print(f"  lat_abonne nulles      : {n_lat_null:>10,}  "
          f"({'✅ 0' if n_lat_null == 0 else '❌ ' + str(n_lat_null)})")
    print(f"  lon_abonne nulles      : {n_lon_null:>10,}  "
          f"({'✅ 0' if n_lon_null == 0 else '❌ ' + str(n_lon_null)})")

    # ── Distribution K_planifie ───────────────────────────────────────────────
    k_par_bat = df.groupby("id_batiment")["K_planifie"].first()
    print(f"\n  ── K_planifie (CIBLE ML) ──")
    print(f"  K min / moy / max      : {k_par_bat.min()} / "
          f"{k_par_bat.mean():.2f} / {k_par_bat.max()}")

    # ── Taux d'occupation ─────────────────────────────────────────────────────
    taux_bat = df.groupby("id_batiment")["taux_occupation"].first()
    print(f"\n  ── Taux d'occupation (initial) ──")
    print(f"  moy={taux_bat.mean():.3f}  [{taux_bat.min():.2f} – {taux_bat.max():.2f}]"
          f"  n={len(taux_bat)}")

    # ── Types de bâtiment ─────────────────────────────────────────────────────
    types_bat = df.groupby("id_batiment")["type_batiment"].first().value_counts()
    print(f"\n  ── Distribution types de bâtiment ──")
    for t, n in types_bat.items():
        print(f"    {t:<8} : {n:>4} bâtiments  ({n/n_bats*100:.1f}%)")

    # ── Vérification absence colonnes FAT/câble ───────────────────────────────
    FORBIDDEN = ["FAT_relative", "lat_fat", "lon_fat", "etage_fat",
                 "distance_real_m", "cable_prefab_m", "waste_m",
                 "fat_load_balance_ok", "K_connectes"]
    intrus = [c for c in FORBIDDEN if c in df.columns]
    print(f"\n  ── Colonnes FAT/câble (doivent être absentes) ──")
    if intrus:
        print(f"  ❌ Colonnes interdites présentes : {intrus}")
    else:
        print(f"  ✅ Aucune colonne FAT/câble — dataset propre")

    print(f"\n  Colonnes ({len(df.columns)}) : {list(df.columns)}")
    print(f"\n  ✅ Dataset v24 prêt — placement FAT délégué à fat_planner_hybride")
    print(SEP1)


# ══════════════════════════════════════════════════════════════════════════════
# TESTS UNITAIRES
# ══════════════════════════════════════════════════════════════════════════════

def _run_tests() -> None:
    """Suite de tests unitaires — v24 (abonnés + localisations uniquement)."""
    print(f"\n{SEP1}")
    print(f"  TESTS UNITAIRES v24")
    print(SEP1)

    ok = total = 0

    # ── TEST 1 : _predire_abonnes_estime — distribution uniforme ─────────────
    print(f"\n  TEST 1 — Prédiction abonnés : distribution uniforme par étage")
    rng_p = np.random.RandomState(99)
    nb_et, nb_log = 8, 6
    taux           = 0.50
    positions_dict = _generer_positions_batiment(35.7, -0.63, nb_log, rng_p, nb_et, 400.0)
    tous_appts     = [{"etage": et, "appt_in_floor": i, "lat": p[0], "lon": p[1]}
                      for et in range(1, nb_et + 1)
                      for i, p in enumerate(positions_dict[et])]

    predit = _predire_abonnes_estime(tous_appts, nb_et, nb_log, taux, rng_p)
    n_par_etage = {}
    for apt in predit:
        n_par_etage[apt["etage"]] = n_par_etage.get(apt["etage"], 0) + 1

    cible = round(len(predit) / nb_et)
    for et, n in n_par_etage.items():
        assert abs(n - cible) <= 1, f"Étage {et} : {n} prédits, cible={cible}"
    print(f"    48 appts, taux=0.50 → {len(predit)} prédits, "
          f"par étage: {dict(sorted(n_par_etage.items()))}  ✓")
    ok += 1; total += 1

    # ── TEST 2 : _generer_positions_batiment — positions valides ──────────────
    print(f"\n  TEST 2 — Positions grille réalistes")
    rng_g = np.random.RandomState(42)
    positions_dict = _generer_positions_batiment(35.697, -0.633, 8, rng_g, 1, 400.0)
    positions = positions_dict[1]
    assert len(positions) == 8, f"Attendu 8 positions, obtenu {len(positions)}"
    for lat, lon in positions:
        assert 35.0 < lat < 36.5,  f"lat hors plage Oran : {lat}"
        assert -2.0 < lon < 0.0,   f"lon hors plage Oran : {lon}"
        assert isinstance(lat, float) and isinstance(lon, float)
    print(f"    8 positions générées, toutes dans la plage Oran  ✓")
    ok += 1; total += 1

    # ── TEST 3 : K_planifie — formule ceil(n/8) ───────────────────────────────
    print(f"\n  TEST 3 — K_planifie : formule architecturale")
    cases_k = [
        (8,  4, False, 4),   # 32 log → 4 FATs résid
        (5,  8, False, 5),   # 40 log → 5 FATs résid
        (10, 6, True,  9),   # 60 log → 8 FATs résid + 1 COM = 9
        (3,  4, False, 2),   # 12 log → 2 FATs résid
        (8,  8, True,  9),   # 64 log → 8 FATs résid + 1 COM
    ]
    for nb_et, nb_log, has_com, expected in cases_k:
        k = calculer_k_planifie(nb_et, nb_log, has_com)
        assert k == expected, (
            f"calculer_k_planifie({nb_et}, {nb_log}, {has_com}) = {k}, "
            f"attendu {expected}"
        )
    print(f"    {len(cases_k)} cas validés  ✓")
    ok += 1; total += 1

    # ── TEST 4 : _make_row — colonnes présentes et absentes ───────────────────
    print(f"\n  TEST 4 — _make_row : schéma v24")
    row = _make_row(
        bat_id="BAT-TEST", etage=3, appt_in_floor=1,
        usage="logements", lat_abonne=35.697, lon_abonne=-0.633,
        nb_etages=7, nb_log_etage=6, type_bat="AADL",
        presence_commerce=True, hauteur_etage=3.0, surface_m2=288.0,
        nb_logements_total=42, k_planifie=6, taux_occupation=0.45,
        phase="initial", commune="Oran", seq=1, habite=1,
        nb_logements_habites=19,
    )
    REQUIRED = [
        "code_client", "id_batiment", "etage", "appt_in_floor", "porte",
        "usage", "habite", "lat_abonne", "lon_abonne",
        "nb_etages_bat", "nb_log_etage", "nb_logements_total", "surface_m2",
        "type_batiment", "presence_de_commerce", "Hauteur par étage (m)",
        "hauteur_bat_totale_m", "taux_occupation", "phase_occupation",
        "commune", "K_planifie",
    ]
    FORBIDDEN = [
        "FAT_relative", "lat_fat", "lon_fat", "etage_fat",
        "distance_real_m", "cable_prefab_m", "waste_m",
        "fat_load_balance_ok", "K_connectes",
    ]
    missing  = [c for c in REQUIRED  if c not in row]
    intrus   = [c for c in FORBIDDEN if c in row]
    assert not missing, f"Colonnes manquantes dans _make_row : {missing}"
    assert not intrus,  f"Colonnes interdites dans _make_row : {intrus}"
    assert row["code_client"] is not None,         "habite=1 → code_client requis"
    assert row["lat_abonne"]  == 35.697,           "lat_abonne incorrect"
    assert row["porte"]       == "Etage3-2",        "porte incorrecte"
    print(f"    21 colonnes requises présentes  ✓")
    print(f"    9 colonnes FAT/câble absentes   ✓")

    # habite=0 : lat_abonne RENSEIGNÉ (logement physique existant)
    row0 = _make_row(
        bat_id="BAT-TEST", etage=4, appt_in_floor=2,
        usage="logements", lat_abonne=35.698, lon_abonne=-0.634,
        nb_etages=7, nb_log_etage=6, type_bat="AADL",
        presence_commerce=True, hauteur_etage=3.0, surface_m2=288.0,
        nb_logements_total=42, k_planifie=6, taux_occupation=0.45,
        phase="initial", commune="Oran", seq=2, habite=0,
        nb_logements_habites=19,
    )
    assert row0["habite"]      == 0,    "habite=0 non conservé"
    assert row0["code_client"] is None, "habite=0 → code_client doit être None"
    assert row0["lat_abonne"]  == 35.698, \
        "habite=0 → lat_abonne doit être renseigné (logement physique)"
    assert row0["lon_abonne"]  == -0.634, \
        "habite=0 → lon_abonne doit être renseigné (logement physique)"
    print(f"    habite=0 : lat/lon renseignés (logement physique)  ✓")
    print(f"    habite=0 : code_client=None  ✓")
    ok += 1; total += 1

    # ── TEST 5 : dataset entier — schéma et cohérence ─────────────────────────
    print(f"\n  TEST 5 — Dataset complet : colonnes et cohérence")
    df = generer_dataset(n_bats=10, verbose=False)

    # Colonnes exactes attendues (ni plus, ni moins)
    expected_cols = {
        "code_client", "id_batiment", "etage", "appt_in_floor", "porte",
        "usage", "habite", "lat_abonne", "lon_abonne",
        "nb_etages_bat", "nb_log_etage", "nb_logements_total", "nb_logements_habites",
        "surface_m2",
        "type_batiment", "presence_de_commerce", "Hauteur par étage (m)",
        "hauteur_bat_totale_m", "taux_occupation", "phase_occupation",
        "commune", "K_planifie",
    }
    actual_cols = set(df.columns)
    extra   = actual_cols - expected_cols
    missing = expected_cols - actual_cols
    assert not missing, f"Colonnes manquantes dans le dataset : {missing}"
    assert not extra,   f"Colonnes inattendues dans le dataset : {extra}"
    print(f"    Schéma 21 colonnes exact  ✓")

    # Phase uniquement "initial"
    assert set(df["phase_occupation"].unique()) == {"initial"}, \
        f"Phase attendue 'initial', trouvé {df['phase_occupation'].unique()}"
    print(f"    Phase='initial' uniquement  ✓")

    # lat/lon renseignés pour TOUS (habités + vides)
    assert df["lat_abonne"].notna().all(), "lat_abonne NULL pour certains logements"
    assert df["lon_abonne"].notna().all(), "lon_abonne NULL pour certains logements"
    n_h = int((df["habite"] == 1).sum())
    n_v = int((df["habite"] == 0).sum())
    print(f"    lat/lon renseignés pour tous ({n_h} habités + {n_v} vides)  ✓")

    # code_client : renseigné si habite=1, None si habite=0
    hab = df[df["habite"] == 1]
    vid = df[df["habite"] == 0]
    assert hab["code_client"].notna().all(), "code_client NULL pour habité"
    assert vid["code_client"].isna().all(),  "code_client renseigné pour vide"
    print(f"    code_client : renseigné habite=1, None habite=0  ✓")

    # K_planifie cohérent avec géométrie
    for bat_id, grp in df.groupby("id_batiment"):
        nb_et  = grp["nb_etages_bat"].iloc[0]
        nb_log = grp["nb_log_etage"].iloc[0]
        has_com = bool(grp["presence_de_commerce"].iloc[0])
        k_att  = calculer_k_planifie(nb_et, nb_log, has_com)
        k_reel = grp["K_planifie"].iloc[0]
        assert k_reel == k_att, \
            f"{bat_id}: K_planifie={k_reel}, attendu {k_att}"
    print(f"    K_planifie cohérent avec géométrie pour tous les bâtiments  ✓")
    ok += 1; total += 1

    # ── TEST 6 : absence colonne 'mode' ──────────────────────────────────────
    print(f"\n  TEST 6 — Absence colonnes supprimées")
    df_m = generer_dataset(n_bats=5, verbose=False)
    SUPPRIMEES = [
        "mode", "FAT_relative", "lat_fat", "lon_fat", "etage_fat",
        "distance_real_m", "cable_prefab_m", "waste_m",
        "fat_load_balance_ok", "K_connectes",
    ]
    trouvees = [c for c in SUPPRIMEES if c in df_m.columns]
    assert not trouvees, f"Colonnes supprimées encore présentes : {trouvees}"
    print(f"    {len(SUPPRIMEES)} colonnes supprimées confirmées absentes  ✓")
    ok += 1; total += 1

    print(f"\n{SEP2}")
    print(f"  RÉSULTAT : {ok}/{total} tests passés")
    if ok == total:
        print(f"  ✅ Tous les tests v24 passés — générateur validé")
    else:
        print(f"  ❌ {total - ok} test(s) échoué(s)")
    print(SEP1)


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Générateur dataset d'entraînement K-Predictor v24"
    )
    p.add_argument("--n_bats",    type=int, default=1000,
                   help="Nombre de bâtiments à simuler (défaut : 1000)")
    p.add_argument("--output",    type=str, default=None,
                   help="Chemin CSV de sortie (défaut : dataset_v24.csv)")
    p.add_argument("--seed",      type=int, default=RANDOM_SEED)
    p.add_argument("--test",      action="store_true",
                   help="Exécuter les tests unitaires avant la génération")
    p.add_argument("--test_only", action="store_true",
                   help="Exécuter uniquement les tests")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.test or args.test_only:
        _run_tests()
        if args.test_only:
            raise SystemExit(0)

    output = args.output or "dataset_v24.csv"

    df = generer_dataset(
        n_bats=args.n_bats,
        output=output,
        seed=args.seed,
        verbose=True,
    )

    print(f"\n  COLONNES DU DATASET ({len(df.columns)}) :")
    print(f"  {SEP2}")
    for col in df.columns:
        dtype = str(df[col].dtype)
        ex    = repr(df[col].iloc[0]) if len(df) > 0 else "—"
        print(f"    {col:<32} {dtype:<10}  ex: {ex}")

    print(f"\n{SEP1}")
    print(f"  ✅ GÉNÉRATION TERMINÉE")
    print(f"  Dataset : {output}")
    print(f"  Lignes  : {len(df):,}")
    print(f"  Colonnes: {len(df.columns)}")
    print(f"\n ── Occupation réelle simulée ──")
    print(f" nb_logements_habites total : {df['nb_logements_habites'].sum():,}")
    print(f" Taux occupation moyen      : {df.groupby('id_batiment')['nb_logements_habites'].first().mean():.3f}")
    print(SEP1)