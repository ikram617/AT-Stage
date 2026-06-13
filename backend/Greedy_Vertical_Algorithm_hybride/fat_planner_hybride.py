from __future__ import annotations

import json
import random
import sys
import warnings
from math import radians, cos, sin, asin, sqrt, ceil as mceil
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

warnings.filterwarnings("ignore")

RANDOM_SEED    = 2026
PALIER_FIXE_M  = 4.0
from config import settings as _cfg

def get_fat_capacity() -> int:
    """Retourne la capacité FAT courante (dynamique, mis à jour via /api/config)."""
    return _cfg.FAT_CAPACITY

def get_prefab_cables() -> list:
    """Retourne les standards de câbles courants (dynamique, mis à jour via /api/config)."""
    return _cfg.AT_DROP_CABLE_STANDARDS_M

FAT_CAPACITY   = _cfg.FAT_CAPACITY
PREFAB_CABLES  = _cfg.AT_DROP_CABLE_STANDARDS_M

MODEL_DIR      = Path("models")
EXPORT_DIR     = Path("exports")
MODEL_DIR.mkdir(exist_ok=True)
EXPORT_DIR.mkdir(exist_ok=True)

TYPE_BAT_MAP = {
    "AADL": 0, "HLM": 1, "LPP": 2,
    "LPA":  3, "LSL": 4, "CNEP": 5, "PRIVE": 6,
}

MODE2_FEATURE_GEOM_OSM = [
    "nb_etages_bat",
    "nb_log_etage",
    "hauteur_bat_totale_m",
    "nb_logements_total",
    "surface_m2",
    "presence_de_commerce",
]
MODE2_FEATURE_TYPE = [f"type_{t}" for t in
                      ["AADL", "HLM", "LPP", "LPA", "LSL", "CNEP", "PRIVE"]]
MODE2_FEATURE_COLS_OSM = MODE2_FEATURE_GEOM_OSM + MODE2_FEATURE_TYPE
FULL_ONLY_FEATURE_COLS = [
    "nb_logements_habites",
    "taux_occupation",
    "densite",
]

# ── Taux d'occupation par type × phase (table de référence Mode 2) ────────────
# Source : audit dataset v17/v18 — FIX 3 générateur
MODE2_OCCUPATION_RATES = {
    ("AADL",  "initial"): 0.35, ("AADL",  "stable"): 0.45,
    ("HLM",   "initial"): 0.45, ("HLM",   "stable"): 0.70,
    ("LPP",   "initial"): 0.4, ("LPP",   "stable"): 0.55,
    ("LPA",   "initial"): 0.50, ("LPA",   "stable"): 0.65,
    ("LSL",   "initial"): 0.50, ("LSL",   "stable"): 0.60,
    ("CNEP",  "initial"): 0.45, ("CNEP",  "stable"): 0.60,
    ("PRIVE", "initial"): 0.60, ("PRIVE", "stable"): 0.80,
}
MODE2_DEFAULT_OCCUPATION  = 0.   # fallback si type/phase inconnu
MODE2_DEFAULT_RACCORDEMENT = 0.85  # taux raccordement parmi les habités

SEP1 = "═" * 68
SEP2 = "─" * 68
SEP3 = "·" * 68


def _mode2_default_occupation(type_batiment: str, phase: str = "stable") -> float:
    return MODE2_OCCUPATION_RATES.get(
        (str(type_batiment).upper(), phase),
        MODE2_DEFAULT_OCCUPATION,
    )


def _sanitize_training_frame(
    agg_bat: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    label: str,
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Keep only target + approved features before training.

    Mode Estime must stay OSM-only: subscriber/connection-derived columns
    must not leak into model B even when they exist in the training dataset.
    """
    keep_cols = [target_col] + list(feature_cols)
    missing = [c for c in keep_cols if c not in agg_bat.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes avant entrainement {label}: {missing}")

    if "OSM" in label:
        leaked = [c for c in FULL_ONLY_FEATURE_COLS if c in feature_cols]
        if leaked:
            raise ValueError(
                f"Fuite Mode Estime: colonnes interdites dans le modele OSM: {leaked}"
            )

    train_df = agg_bat.loc[:, keep_cols].copy()
    X_df = train_df.loc[:, feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    y = pd.to_numeric(train_df[target_col], errors="coerce").fillna(1).astype(int).values
    return X_df, y


# ══════════════════════════════════════════════════════════════════════════════
# PHYSIQUE — fonctions de base
# ══════════════════════════════════════════════════════════════════════════════

def haversine_vec(lat1: np.ndarray, lon1: np.ndarray,
                  lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Haversine vectorisé sur arrays numpy — distance en mètres."""
    R   = 6_371_000.0
    la1 = np.radians(lat1); la2 = np.radians(lat2)
    lo1 = np.radians(lon1); lo2 = np.radians(lon2)
    a   = (np.sin((la2-la1)/2)**2
           + np.cos(la1)*np.cos(la2)*np.sin((lo2-lo1)/2)**2)
    return R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))



class FATPlacementEngine:
    """
    Moteur standardisé de placement FAT — Algérie Télécom
    Contient les 4 étapes officielles :
    ① Regroupement glouton équilibré
    ② Médiane verticale (étage FAT)
    ③ Centroïde horizontal (position GPS)
    ④ Snap câble préfabriqué

    Cette classe est utilisée par LES DEUX modes (Connecté + Estimé).
    """

    def __init__(self, fat_capacity: int = None, prefab_cables: list = None):
        self.fat_capacity = fat_capacity or get_fat_capacity()
        self.prefab_cables = prefab_cables or get_prefab_cables()
        self.palier_fixe_m = PALIER_FIXE_M

    # ── ① Regroupement Glouton Équilibré ─────────────────────────────────────
    def greedy_balanced_grouping(self, subscribers: list[dict],
                               mode: str = "connected",
                               max_per_fat: int = None) -> list[list[dict]]:
        """
        Regroupe les abonnés/raccordés.
        - Mode Connecté : Remplissage glouton (chaque FAT remplie au max).
        - Mode Estimé : Distribution équilibrée (subscribers evenly distributed).
        """
        if not subscribers:
            return []

        if max_per_fat is None:
            max_per_fat = self.fat_capacity

        # Tri vertical puis horizontal pour cohérence
        seen_ids = set()
        unique_subs = []
        for s in subscribers:
            sid = s.get("id") or id(s)
            if sid not in seen_ids:
                seen_ids.add(sid)
                unique_subs.append(s)
        sorted_subs = sorted(
            unique_subs,
            key=lambda x: (x.get("etage", 0), x.get("appt_idx", 0), str(x.get("id", "")))
        )

        groups = []
        n_subs = len(sorted_subs)

        if mode == "estimated":
            # Si mode estimé, on calcule le nombre de FAT nécessaires avec ceil
            n_fats = mceil(n_subs / max_per_fat)
            if n_fats <= 0:
                return []
            
            # Répartition la plus équilibrée possible
            avg = n_subs // n_fats
            rem = n_subs % n_fats
            
            start = 0
            for i in range(n_fats):
                # On ajoute 1 abonné aux 'rem' premiers groupes pour équilibrer
                size = avg + (1 if i < rem else 0)
                if size > 0:
                    groups.append(sorted_subs[start : start + size])
                start += size
        else:
            # Mode Connecté : Remplissage glouton jusqu'à capacité max
            current = []
            for sub in sorted_subs:
                if len(current) >= max_per_fat:
                    groups.append(current)
                    current = []
                current.append(sub)
            if current:
                groups.append(current)

        return groups

    # ── ② Médiane Verticale ─────────────────────────────────────────────────
    def vertical_median_floor(self, group: list[dict]) -> int:
        """Étape 2 : Étage optimal de la FAT = médiane des étages du groupe"""
        if not group:
            return 1
        floors = [s.get("etage", 1) for s in group]
        sorted_floors = sorted(floors)
        return sorted_floors[len(sorted_floors) // 2]

    # ── ③ Centroïde Horizontal ──────────────────────────────────────────────
    def horizontal_centroid(self, group: list[dict]) -> tuple[float, float]:
        """Étape 3 : Position GPS de la FAT = centroïde (médiane)"""
        if not group:
            return 0.0, 0.0
        lats = [s["lat"] for s in group]
        lons = [s["lon"] for s in group]
        return float(np.median(lats)), float(np.median(lons))

    # ── ④ Snap Câble ───────────────────────────────────────────────────────
    def snap_cable(self, distance_m: float) -> int:
        """Étape 4 : Snap vers longueur de câble préfabriqué"""
        for length in self.prefab_cables:
            if length >= distance_m:
                return length
        return 9999

    def calculate_real_distance(self, sub: dict, fat_lat: float, fat_lon: float,
                              fat_etage: int, hauteur_etage: float) -> float:
        """Distance réelle abonné ↔ FAT (horizontale + verticale + palier)"""
        dh = haversine_vec(
            np.array([sub["lat"]]), np.array([sub["lon"]]),
            np.array([fat_lat]),   np.array([fat_lon])
        )[0]
        dv = abs(sub.get("etage", 1) - fat_etage) * hauteur_etage
        return round(dh + dv + self.palier_fixe_m, 2)


def load_k_predictor_model(model_dir: Path = MODEL_DIR) -> dict:

    candidates = [
        model_dir / "k_predictor_osm_optuna.joblib",
        model_dir / "k_predictor_osm.joblib",
        model_dir / "k_predictor.joblib",
    ]
    for path in candidates:
        if path.exists():
            bundle = joblib.load(path)
            # Normalisation : Optuna bundle utilise best_mae au lieu de metrics
            if "metrics" not in bundle:
                bundle["metrics"] = {
                    "R2_pct":        0.0,
                    "MAE_fats":      round(bundle.get("best_mae", 0.0), 3),
                    "RMSE_fats":     0.0,
                    "Accuracy_exact": 0.0,
                    "Accuracy_1fat": 0.0,
                    "CV_MAE_mean":   0.0,
                    "CV_MAE_std":    0.0,
                    "CV_R2_mean_pct": 0.0,
                }
            bundle.setdefault("model_type",  "optuna" if "optuna" in path.name else "B-OSM")
            bundle.setdefault("source_path", str(path))
            print(f"  [load_k_predictor_model] Chargé : {path.name}  "
                  f"(features={len(bundle['feature_cols'])}, "
                  f"MAE={bundle['metrics']['MAE_fats']:.3f} FATs)")
            return bundle
    raise FileNotFoundError(
        f"Aucun modèle K-Predictor trouvé dans {model_dir}. "
        "Lancer model.py pour entraîner le modèle Optuna."
    )


def load_k_predictor_optuna(model_dir: Path = MODEL_DIR) -> dict:
    """
    Charge spécifiquement le bundle Optuna (k_predictor_osm_optuna.joblib).

    Raccourci de load_k_predictor_model() avec vérification explicite que
    le fichier Optuna existe. Utile dans lifespan() de app.py pour prioriser
    le modèle Optuna et logger clairement son origine.

    Le bundle retourné est normalisé (clé "metrics" toujours présente) :
      "model"        : XGBRegressor (Optuna-tuned)
      "feature_cols" : liste dans l'ordre exact de l'entraînement
      "metrics"      : dict R2_pct, MAE_fats, Accuracy_1fat, ...
      "best_params"  : hyperparamètres Optuna
      "best_mae"     : MAE Optuna (validation)
      "model_type"   : "optuna"
    """
    optuna_path = model_dir / "k_predictor_osm_optuna.joblib"
    if not optuna_path.exists():
        raise FileNotFoundError(
            f"Bundle Optuna introuvable : {optuna_path}\n"
            "Lancer model.py pour entraîner le modèle Optuna."
        )
    bundle = joblib.load(optuna_path)
    # Normalisation : Optuna sauvegarde best_mae au lieu de metrics
    if "metrics" not in bundle:
        bundle["metrics"] = {
            "R2_pct":          0.0,
            "MAE_fats":        round(bundle.get("best_mae", 0.0), 3),
            "RMSE_fats":       0.0,
            "Accuracy_exact":  0.0,
            "Accuracy_1fat":   0.0,
            "CV_MAE_mean":     0.0,
            "CV_MAE_std":      0.0,
            "CV_R2_mean_pct":  0.0,
        }
    bundle.setdefault("model_type",  "optuna")
    bundle.setdefault("source_path", str(optuna_path))
    print(f"  [load_k_predictor_optuna] Chargé : {optuna_path.name}  "
          f"features={len(bundle['feature_cols'])}  "
          f"best_mae={bundle.get('best_mae', bundle['metrics']['MAE_fats']):.3f} FATs  "
          f"params={list(bundle.get('best_params', {}).keys())}")
    return bundle


def predire_k_nouveau_batiment(
        nb_etages: int,
        nb_log_etage: int,
        hauteur_etage: float,
        type_batiment: str,
        presence_commerce: int = 0,
        surface_m2: float | None = None,
        model_dir: Path = MODEL_DIR,
) -> dict:
    # Chargement du modèle (priorité Optuna)
    bundle = load_k_predictor_model(model_dir)

    nb_total = nb_etages * nb_log_etage
    hauteur_totale = nb_etages * hauteur_etage

    if surface_m2 is None:
        surface_m2 = float(nb_log_etage * 48.0)  # estimation réaliste

    # Construction des features dans le bon ordre
    type_dummies = {f"type_{t}": 0 for t in ["AADL", "HLM", "LPP", "LPA", "LSL", "CNEP", "PRIVE"]}
    col_type = f"type_{type_batiment.upper()}"
    if col_type in type_dummies:
        type_dummies[col_type] = 1

    feat_vals = {
        "nb_etages_bat": nb_etages,
        "nb_log_etage": nb_log_etage,
        "hauteur_bat_totale_m": hauteur_totale,
        "nb_logements_total": nb_total,
        "surface_m2": surface_m2,
        "presence_de_commerce": int(presence_commerce),
        "nb_logements_habites": 0,  # pas connu en Mode Estimé
        "taux_occupation": 0.0,
        "densite": 0.0,
        **type_dummies,
    }

    feature_cols = bundle["feature_cols"]
    X = np.array([[feat_vals.get(f, 0) for f in feature_cols]])

    K_pred_raw = bundle["model"].predict(X)[0]
    K_pred = max(1, int(round(K_pred_raw)))

    # Estimation du nombre d'abonnés
    nb_habites_estime = max(1, round(nb_total * MODE2_DEFAULT_OCCUPATION))

    return {
        "K_predit": K_pred,
        "K_predit_raw": round(float(K_pred_raw), 3),
        "nb_logements_total": nb_total,
        "nb_logements_habites_estime": nb_habites_estime,
        "taux_occupation_estime": MODE2_DEFAULT_OCCUPATION,
        "modele_type": bundle.get("model_type", "Optuna"),
        "modele_mae_fats": bundle["metrics"].get("MAE_fats", 0.0),
        "modele_r2_pct": bundle["metrics"].get("R2_pct", 0.0),
        "feature_cols_used": feature_cols,
    }


def predire_k_depuis_bundle(
    nb_etages:             int,
    nb_log_etage:          int,
    hauteur_etage:         float,
    type_batiment:         str,
    presence_commerce:     int,
    k_bundle:              dict,
    snap_bundle:           dict | None = None,
    taux_occupation_estime: float = 0.70,
    surface_m2:            float | None = None,
) -> dict:
    """
    Prédiction K pour un nouveau bâtiment (bundles déjà chargés).
    Version optimisée et cohérente avec FATPlacementEngine.
    """
    engine = FATPlacementEngine()  # Pour la cohérence snap + capacité

    feature_cols = k_bundle["feature_cols"]
    nb_total = nb_etages * nb_log_etage
    hauteur_tot = nb_etages * hauteur_etage

    if surface_m2 is None:
        surface_m2 = float(nb_log_etage * 48.0)

    # One-hot encoding
    type_dummies = {f"type_{t}": 0 for t in ["AADL", "HLM", "LPP", "LPA", "LSL", "CNEP", "PRIVE"]}
    col_type = f"type_{type_batiment}"
    if col_type in type_dummies:
        type_dummies[col_type] = 1

    # Features dans l'ordre exact du modèle
    feat_vals = {
        "nb_etages_bat":        nb_etages,
        "nb_log_etage":         nb_log_etage,
        "hauteur_bat_totale_m": hauteur_tot,
        "presence_de_commerce": presence_commerce,
        "nb_logements_total":   nb_total,
        "surface_m2":           surface_m2,
        "nb_logements_habites": max(1, round(nb_total * taux_occupation_estime)),
        "taux_occupation":      round(taux_occupation_estime, 3),
        "densite":              round(nb_total * taux_occupation_estime / max(nb_total, 1), 4),
        **type_dummies,
    }

    X = np.array([[feat_vals.get(f, 0) for f in feature_cols]])

    K_pred_raw = k_bundle["model"].predict(X)[0]
    K_pred = max(1, int(round(K_pred_raw)))

    # Capacité FDT (règle métier)
    multi_fdt = K_pred > 8
    K_final = min(K_pred, 8)

    # Estimation câble (utilise l'engine maintenant)
    dist_moy_est = (nb_etages / max(K_final, 1) / 4) * hauteur_etage + PALIER_FIXE_M
    cable_est = engine.snap_cable(dist_moy_est)

    return {
        "K_predit":               K_pred,
        "K_predit_final":         K_final,           # après cap FDT
        "multi_fdt_requis":       multi_fdt,
        "nb_logements_total":     nb_total,
        "nb_logements_habites_estime": feat_vals["nb_logements_habites"],
        "taux_occupation_estime": round(taux_occupation_estime, 3),
        "cable_type_m":           cable_est,
        "cable_total_estime_m":   K_final * engine.fat_capacity * cable_est,
        "modele_r2_pct":          k_bundle["metrics"].get("R2_pct", 0.0),
        "modele_mae_fats":        k_bundle["metrics"].get("MAE_fats", 0.0),
        "modele_type":            k_bundle.get("model_type", "inconnu"),
        "feature_cols_used":      feature_cols,
    }


def generer_fats_depuis_k(
    K:                 int,
    nb_etages:         int,
    nb_log_etage:      int,
    hauteur_etage:     float,
    presence_commerce: int,
    bat_id:            str,
    centroid_lat:      float,
    centroid_lon:      float,
    fdt_nom:           str,
    snap_bundle:       dict | None = None,
    fat_balance_thr:   float = 0.75,        # non utilisé maintenant (géré dans le moteur)
    type_batiment:     str = "AADL",
) -> dict:

    # Appel centralisé à la simulation complète
    sim = placer_fats_mode_estime(
        nb_etages=nb_etages,
        nb_log_etage=nb_log_etage,
        hauteur_etage=hauteur_etage,
        type_batiment=type_batiment,
        presence_commerce=presence_commerce,
        centroid_lat=centroid_lat,
        centroid_lon=centroid_lon,
        bat_id=bat_id,
        k_force=K,
        seed=RANDOM_SEED,
    )

    # Formatage propre pour l'API / Frontend
    fat_candidates = []
    for fat in sim.get("fats", []):
        fat_candidates.append({
            "id_batiment":          bat_id,
            "id_zone":              "Z310-001",
            "fat_id":               fat.get("fat_id"),
            "cluster_label":        int(fat.get("cluster_label", 0)),
            "centroid_lat":         round(float(fat.get("lat_fat", centroid_lat)), 6),
            "centroid_lon":         round(float(fat.get("lon_fat", centroid_lon)), 6),
            "etage_fat":            int(fat.get("etage_fat", 1)),
            "n_subscribers":        int(fat.get("n_raccordes", 0)),
            "usage":                fat.get("usage", "logements"),
            "fdt_assigned":         fdt_nom,
            "capacity_ok":          int(fat.get("n_raccordes", 0)) <= FAT_CAPACITY,
            "cable_m_to_fdt_real":  float(fat.get("dist_median_m", 0.0)),
            "cable_snap_m":         int(fat.get("cable_snap_m", 15)),
            "subscriber_ids":       fat.get("subscriber_ids", []),
            "mode":                 "prediction",
        })

    return {
        "fat_candidates": fat_candidates,
        "subscribers":    sim.get("portes", []),
        "K_predit":       sim.get("K_predit"),
        "multi_fdt_alerte": sim.get("multi_fdt_alerte", False),
        "stats":          sim.get("stats", {})
    }
# ══════════════════════════════════════════════════════════════════════════════
# MODE 1
# ══════════════════════════════════════════════════════════════════════════════
def placer_fats_mode_connecte(
    subscribers: list[dict],
    hauteur_etage: float,
    bat_id: str = None,
    fdt_nom: str = "FDT-001"
) -> dict:
    """
    Mode Connecté (abonnés réels) — Utilise exactement le même moteur que Mode Estimé.
    """
    engine = FATPlacementEngine()

    # ① Regroupement glouton (Remplissage maximal par FAT)
    groups = engine.greedy_balanced_grouping(subscribers, mode="connected")

    fats = []
    for idx, group in enumerate(groups):
        # ② Médiane verticale
        etage_fat = engine.vertical_median_floor(group)

        # ③ Centroïde horizontal
        lat_fat, lon_fat = engine.horizontal_centroid(group)

        # Calcul distance + ④ Snap câble
        distances = [
            engine.calculate_real_distance(sub, lat_fat, lon_fat, etage_fat, hauteur_etage)
            for sub in group
        ]
        dist_median = float(np.median(distances)) if distances else 0.0
        cable_snap = engine.snap_cable(dist_median)

        fats.append({
            "id_batiment":          bat_id or "BAT-UNKNOWN",
            "id_zone":              "Z310-001",
            "fat_id":               f"FAT-{bat_id or 'XX'}-{idx+1:02d}",
            "cluster_label":        idx,
            "centroid_lat":         round(lat_fat, 7),
            "centroid_lon":         round(lon_fat, 7),
            "etage_fat":            int(etage_fat),
            "n_subscribers":        len(group),
            "usage":                "logements",
            "fdt_assigned":         fdt_nom,
            "capacity_ok":          len(group) <= engine.fat_capacity,
            "cable_m_to_fdt_real":  round(dist_median, 2),
            "cable_snap_m":         cable_snap,
            "subscriber_ids":       [s.get("code_client") or s.get("id") for s in group],
            "mode":                 "connected",
        })

    return {
        "fat_candidates": fats,
        "n_fats": len(fats),
        "groups": groups
    }
# ══════════════════════════════════════════════════════════════════════════════
# MODE 2 — SIMULATION COMPLÈTE FTTH (sans abonnés réels)
# ══════════════════════════════════════════════════════════════════════════════
def _generer_portes_mode2(
        nb_etages: int,
        nb_log_etage: int,
        centroid_lat: float,
        centroid_lon: float,
        taux_occupation: float,
        taux_raccordement: float,
        rng: random.Random,
        presence_commerce: bool = False
) -> list[dict]:
    """
    Génère TOUS les logements (portes) d'un bâtiment en Mode Estimé.
    Retourne une liste de dicts avec lat/lon réalistes + statut habite/raccorde.
    """
    portes: list[dict] = []
    cos_lat = cos(radians(centroid_lat))

    # Paramètres de grille réaliste (bâtiments Algérie)
    dx_m = 7.0  # espacement horizontal (façade)
    dy_m = 11.0  # espacement profondeur

    n_cols = mceil(nb_log_etage ** 0.5)
    n_rows = mceil(nb_log_etage / n_cols)

    def _pos(etage: int, idx: int) -> tuple[float, float]:
        """Position réaliste avec bruit gaussien"""
        col = idx % n_cols
        row = idx // n_cols

        # Position de base en grille
        lat = centroid_lat + (row - (n_rows - 1) / 2) * dy_m / 111_000
        lon = centroid_lon + (col - (n_cols - 1) / 2) * dx_m / (111_000 * cos_lat)

        # Bruit réaliste (erreur GPS + positionnement intérieur)
        lat += rng.gauss(0, 0.000003)  # ~0.3m
        lon += rng.gauss(0, 0.000003)

        return round(lat, 7), round(lon, 7)

    # ── Commerces RDC ─────────────────────────────────────────────────────
    if presence_commerce and nb_log_etage > 0:  # s'il y a des commerces et des logements
        nb_com_max = min(3, nb_log_etage)  # max 3 commerces
        for j in range(nb_com_max):
            lat_p, lon_p = _pos(0, j)
            habite = 1 if rng.random() < taux_occupation * 1.15 else 0  # commerces plus occupés
            raccorde = 1 if habite and rng.random() < taux_raccordement else 0

            portes.append({
                "id": f"COM_{j}",
                "etage": 0,
                "appt_idx": j,
                "usage": "commerces",
                "lat": lat_p,
                "lon": lon_p,
                "habite": habite,
                "raccorde": raccorde,
            })

    # ── Logements Résidentiels ─────────────────────────────────────────────
    idx_global = 0
    for et in range(1, nb_etages + 1):
        for i in range(nb_log_etage):
            lat_p, lon_p = _pos(et, idx_global)
            habite = 1 if rng.random() < taux_occupation else 0
            raccorde = 1 if habite and rng.random() < taux_raccordement else 0

            portes.append({
                "id": (et - 1) * nb_log_etage + i + 1,
                "etage": et,
                "appt_idx": i,
                "usage": "logements",
                "lat": lat_p,
                "lon": lon_p,
                "habite": habite,
                "raccorde": raccorde,
            })
            idx_global += 1

    return portes
def placer_fats_mode_estime(
    nb_etages:          int,
    nb_log_etage:       int,
    hauteur_etage:      float,
    type_batiment:      str,
    presence_commerce:  int,
    centroid_lat:       float,
    centroid_lon:       float,
    bat_id:             str      = "BAT-MODE2",
    phase:              str      = "stable",
    taux_occupation:    float | None = None,
    taux_raccordement:  float    = MODE2_DEFAULT_RACCORDEMENT,
    surface_m2:         float | None = None,
    model_dir:          Path     = MODEL_DIR,
    k_force:            int | None = None,
    seed:               int | None = None,
) -> dict:
    """
    Mode Estimé — Utilise FATPlacementEngine (même logique que Mode Connecté).
    """
    rng = random.Random(seed) if seed is not None else random.Random(RANDOM_SEED)
    engine = FATPlacementEngine()

    nb_total = nb_etages * nb_log_etage
    if surface_m2 is None:
        surface_m2 = float(nb_log_etage * 48.0)

    # Prédiction K via Modèle Optuna
    if k_force is not None:
        K = max(1, int(k_force))
        if taux_occupation is None:
            taux_occupation = _mode2_default_occupation(type_batiment, phase)
    else:
        res_k = predire_k_nouveau_batiment(
            nb_etages=nb_etages, nb_log_etage=nb_log_etage,
            hauteur_etage=hauteur_etage, type_batiment=type_batiment,
            presence_commerce=presence_commerce, surface_m2=surface_m2,
            model_dir=model_dir
        )
        K = res_k["K_predit"]
        nb_habites_estime = res_k.get("nb_logements_habites_estime",
                                    max(1, round(nb_total * MODE2_DEFAULT_OCCUPATION)))
        if taux_occupation is None:
            taux_occupation = nb_habites_estime / nb_total if nb_total > 0 else MODE2_DEFAULT_OCCUPATION

    # Génération des portes
    portes = _generer_portes_mode2(
        nb_etages, nb_log_etage, centroid_lat, centroid_lon,
        taux_occupation, taux_raccordement, rng, presence_commerce=bool(presence_commerce)
    )

    # Raccordés résidentiels uniquement pour le placement principal
    raccordes = [p for p in portes if p.get("raccorde") == 1 and p.get("usage") == "logements"]

    # Application des 4 étapes via le moteur (Distribution équilibrée pour l'estimé)
    groups = engine.greedy_balanced_grouping(raccordes, mode="estimated")

    fats = []
    for idx, group in enumerate(groups):
        etage_fat = engine.vertical_median_floor(group)
        lat_fat, lon_fat = engine.horizontal_centroid(group)

        distances = [
            engine.calculate_real_distance(p, lat_fat, lon_fat, etage_fat, hauteur_etage)
            for p in group
        ]
        dist_median = float(np.median(distances)) if distances else 0.0
        cable_snap = engine.snap_cable(dist_median)

        fats.append({
            "fat_id":           f"FAT-{bat_id}-RES-{idx+1:02d}",
            "cluster_label":    idx,
            "etage_fat":        int(etage_fat),
            "lat_fat":          round(lat_fat, 7),
            "lon_fat":          round(lon_fat, 7),
            "n_raccordes":      len(group),
            "dist_median_m":    round(dist_median, 2),
            "cable_snap_m":     cable_snap,
            "subscriber_ids":   [p["id"] for p in group],
            "usage":            "logements",
            "mode":             "mode2_simulation",
            "fat_capacity": engine.fat_capacity,
        })

    # FAT Commerce
    if presence_commerce:
        com_raccordes = [p for p in portes if p.get("raccorde") == 1 and p.get("usage") == "commerces"]
        if com_raccordes:
            lat_com = float(np.median([p["lat"] for p in com_raccordes]))
            lon_com = float(np.median([p["lon"] for p in com_raccordes]))
            fats.insert(0, {
                "fat_id":        f"FAT-{bat_id}-COM-01",
                "cluster_label": -1,
                "etage_fat":     0,
                "lat_fat":       round(lat_com, 7),
                "lon_fat":       round(lon_com, 7),
                "n_raccordes":   len(com_raccordes),
                "dist_median_m": round(PALIER_FIXE_M + 1.5, 2),
                "cable_snap_m":  engine.snap_cable(PALIER_FIXE_M + 1.5),
                "subscriber_ids": [p["id"] for p in com_raccordes],
                "usage":         "commerces",
                "mode":          "mode2_simulation",
            })

    stats = {
        "K_predit": K,
        "nb_portes_total": nb_total,
        "nb_habites": sum(1 for p in portes if p.get("habite") == 1),
        "nb_raccordes": len(raccordes),
        "taux_occupation": round(taux_occupation, 3),
        "multi_fdt_alerte": K > 8,
    }

    return {
        "K_predit": K,
        "portes": portes,
        "fats": fats,
        "stats": stats,
        "multi_fdt_alerte": K > 8,
    }


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE (pour tests uniquement)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{SEP1}")
    print(f"  FAT PLANNER HYBRIDE v3.1 — Mode API + Inférence")
    print(f"  Algérie Télécom Oran")
    print(SEP1)
    print("  Usage recommandé :")
    print("    → Importer dans app.py pour l'API")
    print("    → Utiliser placer_fats_mode_connecte()  → Mode Connecté")
    print("    → Utiliser placer_fats_mode_estime()    → Mode Estimé")
    print(f"\n{SEP1}")

