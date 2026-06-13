"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FAT PLANNER HYBRIDE — FTTH Smart Planner                          ║
║          Algérie Télécom Oran  ·  v1.0                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Pipeline en 5 phases basé sur les données du Generateur.py (v13) :       ║
║                                                                            ║
║  P1 — CHARGEMENT & AUDIT                                                   ║
║       Lit dataset_fusionnee_final.csv, valide les colonnes,                ║
║       affiche un audit complet du dataset réel                             ║
║                                                                            ║
║  P2 — FEATURE ENGINEERING                                                  ║
║       Encode type_batiment, usage                                          ║
║       Calcule les features agrégées par groupe FAT                         ║
║       Recompute distance_real_m si absente (v11 compat)                   ║
║       Dérive le snap câble déterministe (if/elif — pas de ML)             ║
║                                                                            ║
║  P3 — MODÈLE ML : K-Predictor (XGBoost Regressor)                         ║
║       Seul rôle légitime du ML dans ce pipeline :                          ║
║       prédit K = nb_FATs pour bâtiments OSM sans abonnés connus            ║
║       Features : surface_m2, nb_etages, type_bat, presence_commerce,      ║
║                  hauteur_etage, nb_log_etage                               ║
║       Target   : K_fats_reel = nb FATs réels du bâtiment                  ║
║                                                                            ║
║  P4 — MÉTRIQUES RÉSEAU COMPLÈTES                                           ║
║       Conformité capacité FAT (≤ 8 abonnés)                               ║
║       Distribution câbles préfab (15/20/50/80/libre)                      ║
║       Gaspillage câble (snap - réel)                                       ║
║       Métriques ML K-predictor (MAE, RMSE, R², accuracy@1)                ║
║       Tout affiché en console avec ASCII art                               ║
║                                                                            ║
║  P5 — SAUVEGARDE DÉPLOIEMENT                                               ║
║       models/k_predictor.joblib       ← modèle + feature_cols + métriques ║
║       models/snap_rules.joblib        ← règles snap déterministes         ║
║       models/pipeline_config.joblib   ← config physique complète          ║
║       exports/fat_placement_results.csv ← dataset enrichi avec prédictions║
║       exports/metrics_report.json     ← toutes les métriques en JSON      ║
║       exports/deployment_manifest.json ← manifest de déploiement          ║
║                                                                            ║
║  USAGE :                                                                   ║
║       python fat_planner_hybride.py <chemin/dataset_fusionnee_final.csv>  ║
║       python fat_planner_hybride.py   (mode test synthétique)             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import sys
import warnings
from math import radians, cos, sin, asin, sqrt
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES PHYSIQUES & CONFIG
# ══════════════════════════════════════════════════════════════════════════════
RANDOM_SEED    = 2026
FAT_CAPACITY   = 8
PALIER_FIXE_M  = 4.0
PREFAB_CABLES  = [15, 20, 50, 80]   # mètres, câbles préfabriqués AT

MODEL_DIR      = Path("models")
EXPORT_DIR     = Path("exports")
MODEL_DIR.mkdir(exist_ok=True)
EXPORT_DIR.mkdir(exist_ok=True)

TYPE_BAT_MAP = {
    "AADL": 0, "HLM": 1, "LPP": 2,
    "LPA":  3, "LSL": 4, "CNEP": 5, "PRIVE": 6,
}

SEP1 = "═" * 68
SEP2 = "─" * 68
SEP3 = "·" * 68


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


def snap_cable_vec(distances: np.ndarray) -> np.ndarray:
    """
    Snap déterministe : câble préfab ≥ distance réelle.
    Règle AT officielle — aucun ML nécessaire.

    LEÇON — np.select vs apply :
        np.select évalue toutes les conditions en un seul passage vectorisé.
        Sur 100K lignes : apply() ≈ 2s, np.select ≈ 0.002s (1000x plus rapide).
    """
    conditions = [distances <= c for c in PREFAB_CABLES]
    return np.select(conditions, PREFAB_CABLES, default=9999).astype(int)


def snap_cable_scalar(distance: float) -> int:
    """Version scalaire pour usage en inférence unitaire."""
    for c in PREFAB_CABLES:
        if c >= distance:
            return c
    return 9999


# ══════════════════════════════════════════════════════════════════════════════
# P1 — CHARGEMENT & AUDIT
# ══════════════════════════════════════════════════════════════════════════════

def p1_charger_auditer(csv_path: str) -> pd.DataFrame:
    """
    Charge dataset_fusionnee_final.csv issu du Generateur.py v13.
    Valide les colonnes obligatoires et affiche un audit complet.

    COLONNES OBLIGATOIRES issues du merge du générateur :
        code_client, id_batiment, FAT_relative, etage,
        lat_abonne, lon_abonne, lat_fat, lon_fat,
        etage_fat, nb_etages_bat, nb_log_etage,
        type_batiment, presence_de_commerce,
        Hauteur par étage (m), usage,
        distance_real_m, cable_prefab_m, waste_m
    """
    print(f"\n{SEP1}")
    print(f"  P1 — CHARGEMENT & AUDIT DU DATASET")
    print(f"{SEP1}")
    print(f"  Fichier : {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)
    print(f"  Lignes brutes        : {len(df):>10,}")
    print(f"  Colonnes disponibles : {len(df.columns):>10}")
    print(f"  Colonnes             : {sorted(df.columns.tolist())}")

    # ── Colonnes obligatoires ─────────────────────────────────────────────────
    REQUIRED = [
        "code_client", "id_batiment", "FAT_relative",
        "etage", "lat_abonne", "lon_abonne",
        "lat_fat", "lon_fat", "etage_fat",
        "nb_etages_bat", "nb_log_etage",
        "type_batiment", "presence_de_commerce",
        "Hauteur par étage (m)", "usage",
    ]
    manquantes = [c for c in REQUIRED if c not in df.columns]
    if manquantes:
        print(f"\n  ❌ COLONNES MANQUANTES : {manquantes}")
        print(f"     → Vérifier que le CSV vient du Generateur.py v13")
        raise ValueError(f"Colonnes manquantes : {manquantes}")

    # ── Nettoyage types ───────────────────────────────────────────────────────
    df = df.dropna(subset=REQUIRED).copy()
    for col in ["etage", "nb_etages_bat", "nb_log_etage",
                "presence_de_commerce", "etage_fat"]:
        df[col] = df[col].astype(int)
    for col in ["Hauteur par étage (m)", "lat_abonne", "lon_abonne",
                "lat_fat", "lon_fat"]:
        df[col] = df[col].astype(float)

    # ── Détection version ─────────────────────────────────────────────────────
    has_real_dist = "distance_real_m" in df.columns
    has_cable     = "cable_prefab_m"  in df.columns
    version       = "v13" if (has_real_dist and has_cable) else "v11-compat"
    df.attrs["version"] = version

    # ── Audit dataset ─────────────────────────────────────────────────────────
    n_ab   = len(df)
    n_bats = df["id_batiment"].nunique()
    n_fats = df["FAT_relative"].nunique()

    print(f"\n  {SEP2}")
    print(f"  AUDIT DATASET — version détectée : {version}")
    print(f"  {SEP2}")
    print(f"  Abonnés total          : {n_ab:>10,}")
    print(f"  Bâtiments uniques      : {n_bats:>10,}")
    print(f"  FATs uniques           : {n_fats:>10,}")
    print(f"  Ratio abonnés/FAT      : {n_ab/n_fats:>10.2f}")

    print(f"\n  Répartition usage :")
    for u, n in df["usage"].value_counts().items():
        print(f"    {u:<20} : {n:>8,}  ({n/n_ab*100:5.1f}%)")

    print(f"\n  Répartition type_batiment :")
    for t, n in df["type_batiment"].value_counts().items():
        bar = "█" * int(n / n_ab * 40)
        print(f"    {t:<8} : {n:>7,}  ({n/n_ab*100:5.1f}%)  {bar}")

    print(f"\n  Étages (min/moy/max)  : {df['etage'].min()} / "
          f"{df['etage'].mean():.1f} / {df['etage'].max()}")
    print(f"  Log/étage (min/moy/max): {df['nb_log_etage'].min()} / "
          f"{df['nb_log_etage'].mean():.1f} / {df['nb_log_etage'].max()}")
    print(f"  Hauteur étage (moy)   : {df['Hauteur par étage (m)'].mean():.2f}m")
    print(f"  Commerce RDC          : {df['presence_de_commerce'].mean()*100:.1f}% des bâtiments")

    if has_real_dist:
        d = df["distance_real_m"].dropna()
        print(f"\n  Distance câble réelle (min/moy/max) :")
        print(f"    {d.min():.2f}m / {d.mean():.2f}m / {d.max():.2f}m")

    if has_cable:
        print(f"\n  Distribution câbles préfab dans le dataset :")
        for c, n in sorted(df["cable_prefab_m"].value_counts().items()):
            pct = n / n_ab * 100
            bar = "█" * int(pct / 2)
            label = f"{c}m" if c != 9999 else "libre"
            print(f"    {label:>6} : {n:>8,}  ({pct:5.1f}%)  {bar}")

    print(f"\n  ✅ P1 terminé — {n_ab:,} abonnés validés")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# P2 — FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

def p2_feature_engineering(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Construit toutes les features nécessaires au ML.

    FEATURES PAR NIVEAU :
    ─────────────────────
    Niveau abonné   : etage, dist_horiz_m, usage_enc
    Niveau groupe   : nb_abonnes_fat, etage_min/max/range_fat
    Niveau bâtiment : nb_etages_bat, nb_log_etage, hauteur_etage,
                      hauteur_bat_totale_m, type_bat_enc, presence_de_commerce
    Niveau ML K     : K_fats_reel (target du K-predictor, agrégé par bâtiment)

    LEÇON — groupby + transform vs merge :
        transform() propage une valeur agrégée à chaque ligne du groupe.
        Pour K_fats_reel on veut la valeur au niveau bâtiment → on merge
        après un groupby sur id_batiment.
    """
    print(f"\n{SEP1}")
    print(f"  P2 — FEATURE ENGINEERING")
    print(SEP1)

    version = df.attrs.get("version", "v11-compat")

    # ── Encodage catégorielles ────────────────────────────────────────────────
    df["type_bat_enc"] = df["type_batiment"].map(TYPE_BAT_MAP).fillna(99).astype(int)
    df["usage_enc"]    = (df["usage"] == "commerces").astype(int)

    # ── Distance horizontale FAT↔abonné ──────────────────────────────────────
    df["dist_horiz_m"] = np.round(
        haversine_vec(
            df["lat_abonne"].values, df["lon_abonne"].values,
            df["lat_fat"].values,   df["lon_fat"].values,
        ), 2
    )

    # ── Recalcul distance réelle si absente (compatibilité v11) ───────────────
    if "distance_real_m" not in df.columns or version == "v11-compat":
        hauteurs = df["Hauteur par étage (m)"].values
        dist_v   = np.abs(df["etage"].values - df["etage_fat"].values) * hauteurs
        df["distance_real_m"] = np.round(dist_v + df["dist_horiz_m"].values + PALIER_FIXE_M, 2)
        print(f"  ✓ distance_real_m recalculée (vertical + horizontal + palier)")
    else:
        print(f"  ✓ distance_real_m utilisée depuis le dataset v13")

    # ── Snap câble déterministe ───────────────────────────────────────────────
    # NOTE : pas de ML ici — règle physique pure
    df["cable_snap_calcule"] = snap_cable_vec(df["distance_real_m"].values)
    df["gaspillage_m"]       = np.where(
        df["cable_snap_calcule"] != 9999,
        df["cable_snap_calcule"] - df["distance_real_m"],
        0.0
    ).round(2)

    # ── Features agrégées par groupe FAT ──────────────────────────────────────
    fat_stats = (
        df.groupby("FAT_relative")["etage"]
          .agg(nb_abonnes_fat="count", etage_min_fat="min", etage_max_fat="max")
          .reset_index()
    )
    fat_stats["etage_range_fat"] = fat_stats["etage_max_fat"] - fat_stats["etage_min_fat"]
    df = df.merge(fat_stats, on="FAT_relative", how="left")

    # ── Hauteur totale bâtiment ───────────────────────────────────────────────
    df["hauteur_bat_totale_m"] = df["nb_etages_bat"] * df["Hauteur par étage (m)"]

    # ── K_fats_reel : nb FATs réels par bâtiment (TARGET du K-predictor) ──────
    # LEÇON — pourquoi compter les FATs logements seulement ?
    # Les FATs commerces RDC sont indépendants et toujours 1 par bâtiment.
    # Le K-predictor doit prédire les FATs résidentiels car c'est ce qui
    # varie selon la géométrie du bâtiment.
    k_bat = (
        df[df["usage"] == "logements"]
          .groupby("id_batiment")["FAT_relative"]
          .nunique()
          .reset_index()
          .rename(columns={"FAT_relative": "K_fats_reel"})
    )
    df = df.merge(k_bat, on="id_batiment", how="left")
    df["K_fats_reel"] = df["K_fats_reel"].fillna(1).astype(int)

    # ── Features finales ──────────────────────────────────────────────────────
    # Ces 12 features sont celles utilisées par le K-predictor.
    # Elles sont toutes disponibles depuis une géométrie OSM seule
    # (pas besoin de connaître les abonnés → utile en production).
    FEATURE_COLS = [
        "nb_etages_bat",          # nombre d'étages (OSM building:levels)
        "nb_log_etage",           # logements par étage (estimé ou connu)
        "Hauteur par étage (m)",  # hauteur étage (type bâtiment)
        "hauteur_bat_totale_m",   # nb_etages × hauteur_etage
        "type_bat_enc",           # AADL=0, HLM=1, ...
        "presence_de_commerce",   # 1 si RDC commercial
    ]

    manquantes = [f for f in FEATURE_COLS if f not in df.columns]
    if manquantes:
        raise ValueError(f"Features manquantes : {manquantes}")

    # ── Stats features ────────────────────────────────────────────────────────
    print(f"\n  Features construites ({len(FEATURE_COLS)}) :")
    for i, f in enumerate(FEATURE_COLS, 1):
        print(f"    {i:2d}. {f}")

    print(f"\n  TARGET K_fats_reel (par bâtiment) :")
    k_vals = df.groupby("id_batiment")["K_fats_reel"].first()
    print(f"    min={k_vals.min()} | moy={k_vals.mean():.2f} | max={k_vals.max()}")
    print(f"    Distribution :")
    for k, n in sorted(k_vals.value_counts().items()):
        bar = "█" * min(n, 40)
        print(f"      K={k:>3} : {n:>5} bâtiments  {bar}")

    print(f"\n  ✅ P2 terminé — {len(FEATURE_COLS)} features prêtes")
    return df, FEATURE_COLS


# ══════════════════════════════════════════════════════════════════════════════
# P3 — MODÈLE ML : K-PREDICTOR
# ══════════════════════════════════════════════════════════════════════════════

def p3_entrainer_k_predictor(df: pd.DataFrame, feature_cols: list[str]) -> dict:
    """
    Entraîne le K-Predictor : prédit K = nb_FATs pour bâtiments OSM.

    POURQUOI CE MODÈLE ET PAS UN AUTRE ?
    ────────────────────────────────────
    Le seul problème incertain dans ce pipeline est :
    "Combien de FATs faut-il pour un bâtiment OSM dont on ne connaît
    pas encore les abonnés ?"

    Règle analytique : K = ceil(nb_abonnes / 8)
    Mais nb_abonnes n'est pas connu pour les nouveaux bâtiments OSM.
    → On apprend à le prédire depuis la géométrie : surface, étages, type.

    NIVEAU D'AGRÉGATION :
    Le modèle travaille au niveau BÂTIMENT (une ligne = un bâtiment),
    pas au niveau abonné. On déduplique sur id_batiment.

    MÉTRIQUES :
        MAE      : erreur en nombre de FATs — directement interprétable
        RMSE     : pénalise les grosses erreurs
        R²       : 1.0 = parfait, 0.0 = modèle nul
        Acc@0    : % de prédictions exactes (K prédit = K réel)
        Acc@1    : % d'erreur ≤ 1 FAT (acceptable en production)
    """
    print(f"\n{SEP1}")
    print(f"  P3 — K-PREDICTOR (nb FATs par bâtiment OSM)")
    print(SEP1)

    # ── Niveau bâtiment — une ligne par bâtiment ──────────────────────────────
    df_bat = (
        df[df["usage"] == "logements"]
          .groupby("id_batiment")[feature_cols + ["K_fats_reel"]]
          .first()
          .reset_index()
    )
    print(f"  Dataset bâtiments : {len(df_bat):,} lignes")

    X = df_bat[feature_cols].values
    y = df_bat["K_fats_reel"].values

    # ── Split train/test ──────────────────────────────────────────────────────
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.20, random_state=RANDOM_SEED
    )
    print(f"  Train : {len(X_tr):,} bâtiments | Test : {len(X_te):,} bâtiments (80/20)")

    # ── Modèle XGBoost ────────────────────────────────────────────────────────
    # LEÇON — hyperparamètres :
    #   n_estimators=400  : assez d'arbres pour converger
    #   max_depth=5       : profondeur modérée → évite overfitting sur peu de données
    #   learning_rate=0.08: taux d'apprentissage standard
    #   subsample=0.8     : 80% des données par arbre → régularisation
    #   reg_alpha=0.1     : L1 régularisation → sparse features
    model = xgb.XGBRegressor(
        n_estimators     = 400,
        max_depth        = 5,
        learning_rate    = 0.08,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 2,
        reg_alpha        = 0.1,
        reg_lambda       = 1.0,
        random_state     = RANDOM_SEED,
        n_jobs           = -1,
        verbosity        = 0,
    )
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

    # ── Prédictions ───────────────────────────────────────────────────────────
    y_pred_f = model.predict(X_te)
    y_pred   = np.round(y_pred_f).astype(int)
    y_pred   = np.maximum(y_pred, 1)  # K ≥ 1 toujours

    # ── Métriques ─────────────────────────────────────────────────────────────
    mae  = mean_absolute_error(y_te, y_pred)
    rmse = np.sqrt(mean_squared_error(y_te, y_pred))
    r2   = r2_score(y_te, y_pred)
    acc0 = np.mean(y_te == y_pred) * 100
    acc1 = np.mean(np.abs(y_te - y_pred) <= 1) * 100

    # ── Cross-validation ──────────────────────────────────────────────────────
    kf     = KFold(n_splits=5, shuffle=True, random_state=RANDOM_SEED)
    cv_mae = -cross_val_score(model, X, y, cv=kf,
                               scoring="neg_mean_absolute_error", n_jobs=-1)
    cv_r2  = cross_val_score(model, X, y, cv=kf, scoring="r2", n_jobs=-1)

    # ── Feature importances ───────────────────────────────────────────────────
    importances = pd.Series(
        model.feature_importances_, index=feature_cols
    ).sort_values(ascending=False)

    # ── Affichage métriques ───────────────────────────────────────────────────
    print(f"\n  {'─'*50}")
    print(f"  MÉTRIQUES TEST (20% = {len(X_te)} bâtiments)")
    print(f"  {'─'*50}")
    print(f"  R²                      : {r2*100:>7.1f}%")
    print(f"  MAE (nb FATs)           : {mae:>7.3f}  FATs")
    print(f"  RMSE (nb FATs)          : {rmse:>7.3f}  FATs")
    print(f"  Accuracy K exact        : {acc0:>7.1f}%")
    print(f"  Accuracy K ± 1 FAT      : {acc1:>7.1f}%")

    print(f"\n  CROSS-VALIDATION 5-FOLD")
    print(f"  {'─'*50}")
    print(f"  MAE CV   : {cv_mae.mean():.3f} ± {cv_mae.std():.3f} FATs")
    print(f"  R²  CV   : {cv_r2.mean()*100:.1f}% ± {cv_r2.std()*100:.1f}%")

    delta = abs(cv_mae.mean() - mae)
    print(f"\n  DIAGNOSTIC")
    print(f"  {'─'*50}")
    if r2 >= 0.85:
        print(f"  ✅ R²={r2*100:.1f}% — Excellent. Modèle fiable en production.")
    elif r2 >= 0.65:
        print(f"  ⚠️  R²={r2*100:.1f}% — Acceptable. Plus de données amélioreraient.")
    else:
        print(f"  ❌ R²={r2*100:.1f}% — Insuffisant. Enrichir le dataset.")

    if delta > 1.0:
        print(f"  ⚠️  ΔMae={delta:.3f} > 1.0 → possible overfitting")
    else:
        print(f"  ✅ ΔMae={delta:.3f} → généralisation correcte")

    print(f"\n  FEATURE IMPORTANCES")
    print(f"  {'─'*50}")
    for feat, imp in importances.items():
        bar = "█" * int(imp * 50)
        print(f"  {feat:<30} {imp*100:5.1f}%  {bar}")

    # ── Exemples de prédictions ───────────────────────────────────────────────
    print(f"\n  EXEMPLES PRÉDICTIONS (10 premiers du test)")
    print(f"  {'─'*50}")
    print(f"  {'Réel K':>8}  {'Prédit K':>9}  {'Erreur':>8}  {'Statut':>8}")
    for real, pred in zip(y_te[:10], y_pred[:10]):
        err    = pred - real
        status = "✅" if abs(err) == 0 else ("⚠️" if abs(err) == 1 else "❌")
        print(f"  {real:>8}  {pred:>9}  {err:>+8}  {status}")

    print(f"\n  ✅ P3 terminé — K-Predictor entraîné")

    return {
        "model":       model,
        "label":       "K-Predictor",
        "metrics": {
            "R2_pct":          round(r2 * 100, 1),
            "MAE_fats":        round(mae, 3),
            "RMSE_fats":       round(rmse, 3),
            "Accuracy_exact":  round(acc0, 1),
            "Accuracy_1fat":   round(acc1, 1),
            "CV_MAE_mean":     round(cv_mae.mean(), 3),
            "CV_MAE_std":      round(cv_mae.std(), 3),
            "CV_R2_mean_pct":  round(cv_r2.mean() * 100, 1),
        },
        "feature_importances": importances.to_dict(),
        "n_train":  len(X_tr),
        "n_test":   len(X_te),
        "n_total":  len(df_bat),
    }


# ══════════════════════════════════════════════════════════════════════════════
# P4 — MÉTRIQUES RÉSEAU COMPLÈTES
# ══════════════════════════════════════════════════════════════════════════════

def p4_metriques_reseau(df: pd.DataFrame, res_k: dict) -> dict:
    """
    Calcule et affiche toutes les métriques réseau du dataset.

    MÉTRIQUES CALCULÉES :
    ─────────────────────
    Réseau     : conformité capacité FAT, distribution câbles, gaspillage
    Bâtiments  : distribution K (nb FATs/bat), taux de commerce RDC
    Câbles     : distribution 15/20/50/80m, câble libre (9999)
    Pipeline   : cohérence étage_fat vs médiane calculée
    """
    print(f"\n{SEP1}")
    print(f"  P4 — MÉTRIQUES RÉSEAU COMPLÈTES")
    print(SEP1)

    # ── Métriques FAT capacité ────────────────────────────────────────────────
    fat_occ = df.groupby("FAT_relative")["code_client"].count()
    n_fats  = len(fat_occ)

    conformes  = (fat_occ <= FAT_CAPACITY).sum()
    surcharges = (fat_occ > FAT_CAPACITY).sum()
    pleins     = (fat_occ == FAT_CAPACITY).sum()

    print(f"\n  ┌── CONFORMITÉ CAPACITÉ FAT (max {FAT_CAPACITY} abonnés) ──────")
    print(f"  │  FATs total             : {n_fats:>8,}")
    print(f"  │  FATs conformes (≤ 8)  : {conformes:>8,}  ({conformes/n_fats*100:.1f}%)")
    print(f"  │  FATs surchargés (> 8) : {surcharges:>8,}  ({surcharges/n_fats*100:.1f}%)")
    print(f"  │  FATs à capacité max   : {pleins:>8,}  ({pleins/n_fats*100:.1f}%)")
    print(f"  │  Occupancy moy/max     : {fat_occ.mean():.2f} / {fat_occ.max()}")
    print(f"  └{'─'*52}")

    # ── Distribution câbles (snap calculé) ────────────────────────────────────
    cables = df["cable_snap_calcule"].values
    print(f"\n  ┌── DISTRIBUTION CÂBLES PRÉFABRIQUÉS ──────────────────")
    total_snap = 0
    total_real = df["distance_real_m"].sum()
    cable_dist = {}
    for c in PREFAB_CABLES + [9999]:
        n   = int((cables == c).sum())
        pct = n / len(cables) * 100
        if c != 9999:
            total_snap += n * c
        label = f"{c}m" if c != 9999 else "libre (>80m)"
        bar   = "█" * int(pct / 2)
        print(f"  │  {label:<12} : {n:>8,}  ({pct:5.1f}%)  {bar}")
        cable_dist[str(c)] = {"count": n, "pct": round(pct, 1)}

    gaspillage_total  = df["gaspillage_m"].sum()
    gaspillage_moyen  = df["gaspillage_m"].mean()
    gaspillage_pct    = gaspillage_total / total_snap * 100 if total_snap > 0 else 0
    print(f"  │")
    print(f"  │  Câble total utilisé  : {total_snap/1000:>8.1f} km")
    print(f"  │  Distance réelle tot  : {total_real/1000:>8.1f} km")
    print(f"  │  Gaspillage total     : {gaspillage_total/1000:>8.1f} km  ({gaspillage_pct:.1f}%)")
    print(f"  │  Gaspillage moyen/lien: {gaspillage_moyen:>8.2f} m")
    print(f"  └{'─'*52}")

    # ── Métriques distances ───────────────────────────────────────────────────
    d = df["distance_real_m"]
    print(f"\n  ┌── DISTANCES CÂBLE RÉELLES ────────────────────────────")
    print(f"  │  Min    : {d.min():>8.2f} m")
    print(f"  │  P25    : {d.quantile(0.25):>8.2f} m")
    print(f"  │  Médiane: {d.median():>8.2f} m")
    print(f"  │  Moy    : {d.mean():>8.2f} m")
    print(f"  │  P75    : {d.quantile(0.75):>8.2f} m")
    print(f"  │  P95    : {d.quantile(0.95):>8.2f} m")
    print(f"  │  Max    : {d.max():>8.2f} m")
    print(f"  └{'─'*52}")

    # ── Métriques bâtiments ───────────────────────────────────────────────────
    k_par_bat = (
        df[df["usage"] == "logements"]
          .groupby("id_batiment")["FAT_relative"]
          .nunique()
    )
    print(f"\n  ┌── DISTRIBUTION K (nb FATs / bâtiment) ───────────────")
    for k, n in sorted(k_par_bat.value_counts().items()):
        bar = "█" * min(int(n / len(k_par_bat) * 40), 40)
        print(f"  │  K={k:>3} : {n:>5} bâtiments  ({n/len(k_par_bat)*100:5.1f}%)  {bar}")
    print(f"  │  K moy : {k_par_bat.mean():.2f}  |  K max : {k_par_bat.max()}")
    print(f"  └{'─'*52}")

    # ── Cohérence placement FAT ───────────────────────────────────────────────
    # Vérifie que etage_fat = médiane(étages du groupe) pour chaque FAT
    mediane_calcule = (
        df.groupby("FAT_relative")["etage"].median().round().astype(int)
    )
    df_check = df[["FAT_relative", "etage_fat"]].drop_duplicates("FAT_relative")
    df_check = df_check.merge(mediane_calcule.rename("mediane_calc"),
                               on="FAT_relative", how="left")
    coherent    = (df_check["etage_fat"] == df_check["mediane_calc"]).sum()
    n_check     = len(df_check)
    coherent_pct = coherent / n_check * 100

    print(f"\n  ┌── COHÉRENCE PLACEMENT FAT (médiane analytique) ───────")
    print(f"  │  FATs vérifiés          : {n_check:>8,}")
    print(f"  │  etage_fat = médiane    : {coherent:>8,}  ({coherent_pct:.1f}%)")
    print(f"  │  etage_fat ≠ médiane    : {n_check-coherent:>8,}  ({100-coherent_pct:.1f}%)")
    if coherent_pct >= 99:
        print(f"  │  ✅ Placement optimal confirmé")
    elif coherent_pct >= 90:
        print(f"  │  ⚠️  Quelques groupes multi-étages avec arrondi différent")
    else:
        print(f"  │  ❌ Incohérence — vérifier le générateur")
    print(f"  └{'─'*52}")

    # ── Résumé K-Predictor ────────────────────────────────────────────────────
    m = res_k["metrics"]
    print(f"\n  ┌── K-PREDICTOR (ML) ────────────────────────────────────")
    print(f"  │  R²                    : {m['R2_pct']:>7.1f}%")
    print(f"  │  MAE                   : {m['MAE_fats']:>7.3f} FATs")
    print(f"  │  RMSE                  : {m['RMSE_fats']:>7.3f} FATs")
    print(f"  │  Accuracy K exact      : {m['Accuracy_exact']:>7.1f}%")
    print(f"  │  Accuracy K ± 1 FAT   : {m['Accuracy_1fat']:>7.1f}%")
    print(f"  │  CV MAE                : {m['CV_MAE_mean']:.3f} ± {m['CV_MAE_std']:.3f} FATs")
    print(f"  │  Dataset               : {res_k['n_total']} bâtiments")
    print(f"  └{'─'*52}")

    print(f"\n  ✅ P4 terminé — métriques complètes calculées")

    return {
        "capacite_fat": {
            "n_fats":            n_fats,
            "conformes_pct":     round(conformes/n_fats*100, 1),
            "surcharges_pct":    round(surcharges/n_fats*100, 1),
            "pleins_pct":        round(pleins/n_fats*100, 1),
            "occupancy_moy":     round(fat_occ.mean(), 2),
            "occupancy_max":     int(fat_occ.max()),
        },
        "cables": {
            "distribution":           cable_dist,
            "total_snap_km":          round(total_snap/1000, 2),
            "total_reel_km":          round(total_real/1000, 2),
            "gaspillage_total_km":    round(gaspillage_total/1000, 2),
            "gaspillage_pct":         round(gaspillage_pct, 1),
            "gaspillage_moyen_m":     round(gaspillage_moyen, 2),
        },
        "distances": {
            "min_m":    round(d.min(), 2),
            "p25_m":    round(d.quantile(0.25), 2),
            "median_m": round(d.median(), 2),
            "mean_m":   round(d.mean(), 2),
            "p95_m":    round(d.quantile(0.95), 2),
            "max_m":    round(d.max(), 2),
        },
        "batiments": {
            "n_batiments":   int(df["id_batiment"].nunique()),
            "k_moy":         round(k_par_bat.mean(), 2),
            "k_max":         int(k_par_bat.max()),
        },
        "coherence_placement": {
            "coherent_pct": round(coherent_pct, 1),
        },
        "k_predictor": m,
    }


# ══════════════════════════════════════════════════════════════════════════════
# P5 — SAUVEGARDE DÉPLOIEMENT
# ══════════════════════════════════════════════════════════════════════════════

def p5_sauvegarder(
    res_k:        dict,
    feature_cols: list[str],
    metriques:    dict,
    df:           pd.DataFrame,
) -> dict[str, Path]:
    """
    Sauvegarde tout ce qui est nécessaire pour le déploiement frontend.

    STRUCTURE DE SORTIE :
    ─────────────────────
    models/
        k_predictor.joblib          ← modèle XGBoost + feature_cols + métriques
        snap_rules.joblib           ← règles snap déterministes (pour inférence)
        pipeline_config.joblib      ← config physique + TYPE_BAT_MAP complet

    exports/
        fat_placement_results.csv   ← dataset enrichi avec prédictions K
        metrics_report.json         ← toutes les métriques en JSON (pour dashboard)
        deployment_manifest.json    ← manifest complet pour le frontend

    LEÇON — Pourquoi sauvegarder snap_rules séparément ?
        Le snap est déterministe — pas besoin d'un modèle ML.
        Mais le frontend doit connaître les valeurs {15,20,50,80}
        et la règle "prendre le premier ≥ distance".
        On sauvegarde ces règles comme un objet Python simple (dict)
        pour éviter toute ambiguïté lors du déploiement.

    LEÇON — Pourquoi deployment_manifest.json ?
        Le frontend a besoin de savoir :
        - quels fichiers joblib charger et dans quel ordre
        - quelles features passer au modèle (et dans quel ordre !)
        - comment interpréter les prédictions
        - les métriques de qualité pour les afficher dans l'UI
        Sans ce manifest, le déploiement requiert de lire le code source.
    """
    print(f"\n{SEP1}")
    print(f"  P5 — SAUVEGARDE DÉPLOIEMENT")
    print(SEP1)

    paths: dict[str, Path] = {}

    # ── 1. K-Predictor (modèle principal) ─────────────────────────────────────
    path_k = MODEL_DIR / "k_predictor.joblib"
    joblib.dump({
        "model":         res_k["model"],
        "feature_cols":  feature_cols,
        "metrics":       res_k["metrics"],
        "type_bat_map":  TYPE_BAT_MAP,
        "n_train":       res_k["n_train"],
        "n_test":        res_k["n_test"],
        "version":       "1.0",
        "description":   "Prédit K=nb_FATs depuis géométrie OSM seule",
        "usage": (
            "bundle = joblib.load('k_predictor.joblib')\n"
            "X = df[bundle['feature_cols']].values\n"
            "K_pred = np.maximum(np.round(bundle['model'].predict(X)).astype(int), 1)"
        ),
    }, path_k, compress=3)
    paths["k_predictor"] = path_k

    # ── 2. Règles snap déterministes ──────────────────────────────────────────
    path_snap = MODEL_DIR / "snap_rules.joblib"
    joblib.dump({
        "prefab_cables":    PREFAB_CABLES,
        "palier_fixe_m":    PALIER_FIXE_M,
        "fat_capacity":     FAT_CAPACITY,
        "description":      "Règle : prendre le câble préfab >= distance réelle",
        "python_snippet": (
            "def snap(d): return next((c for c in [15,20,50,80] if c>=d), 9999)"
        ),
        "note": "PAS de modèle ML — règle physique déterministe AT officielle",
    }, path_snap, compress=1)
    paths["snap_rules"] = path_snap

    # ── 3. Config pipeline complète ───────────────────────────────────────────
    path_cfg = MODEL_DIR / "pipeline_config.joblib"
    joblib.dump({
        "feature_cols":  feature_cols,
        "type_bat_map":  TYPE_BAT_MAP,
        "prefab_cables": PREFAB_CABLES,
        "palier_fixe_m": PALIER_FIXE_M,
        "fat_capacity":  FAT_CAPACITY,
        "random_seed":   RANDOM_SEED,
        "pipeline": {
            "step1_groupement":     "Greedy séquentiel par 8 (déterministe)",
            "step2_placement_fat":  "Médiane analytique étages du groupe (optimal)",
            "step3_distance":       "Haversine + vertical + palier (physique exacte)",
            "step4_snap_cable":     "if/elif déterministe {15,20,50,80,9999}m",
            "step5_ml":             "K-Predictor XGBoost (bâtiments OSM sans abonnés)",
        },
        "metriques_globales": metriques,
    }, path_cfg, compress=3)
    paths["config"] = path_cfg

    # ── 4. Dataset enrichi (avec prédiction K) ────────────────────────────────
    # On ajoute la prédiction K du modèle au dataset
    df_bat = (
        df[df["usage"] == "logements"]
          .groupby("id_batiment")[feature_cols + ["K_fats_reel"]]
          .first()
          .reset_index()
    )
    X_all        = df_bat[feature_cols].values
    K_predit_bat = np.maximum(
        np.round(res_k["model"].predict(X_all)).astype(int), 1
    )
    df_bat["K_predit"] = K_predit_bat
    df_bat["K_erreur"] = df_bat["K_predit"] - df_bat["K_fats_reel"]

    # Merge K_predit dans df principal
    df = df.merge(
        df_bat[["id_batiment", "K_predit"]],
        on="id_batiment", how="left"
    )

    path_csv = EXPORT_DIR / "fat_placement_results.csv"
    df.to_csv(path_csv, index=False, encoding="utf-8-sig")
    paths["results_csv"] = path_csv

    # ── 5. Metrics report JSON ────────────────────────────────────────────────
    path_json = EXPORT_DIR / "metrics_report.json"
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(metriques, f, ensure_ascii=False, indent=2)
    paths["metrics_json"] = path_json

    # ── 6. Deployment manifest ────────────────────────────────────────────────
    manifest = {
        "version":          "1.0",
        "pipeline_name":    "FAT Planner Hybride",
        "description":      "FTTH Smart Planner — Algérie Télécom Oran",
        "files": {
            "k_predictor":      "models/k_predictor.joblib",
            "snap_rules":       "models/snap_rules.joblib",
            "pipeline_config":  "models/pipeline_config.joblib",
        },
        "inference_example": {
            "description": "Prédire K pour un nouveau bâtiment OSM",
            "code": (
                "import joblib, numpy as np\n"
                "bundle = joblib.load('models/k_predictor.joblib')\n"
                "# Préparer les features dans le bon ordre :\n"
                f"# {feature_cols}\n"
                "X = np.array([[nb_etages, nb_log_etage, hauteur_etage,\n"
                "               hauteur_bat_totale, type_bat_enc, presence_commerce]])\n"
                "K = max(1, round(bundle['model'].predict(X)[0]))\n"
                "# Puis snap : next(c for c in [15,20,50,80] if c >= dist_reelle)"
            ),
        },
        "feature_order":    feature_cols,
        "type_bat_map":     TYPE_BAT_MAP,
        "prefab_cables":    PREFAB_CABLES,
        "quality_metrics": {
            "k_predictor_r2_pct":       metriques["k_predictor"]["R2_pct"],
            "k_predictor_mae_fats":     metriques["k_predictor"]["MAE_fats"],
            "k_predictor_acc1_pct":     metriques["k_predictor"]["Accuracy_1fat"],
            "fat_conformite_pct":       metriques["capacite_fat"]["conformes_pct"],
            "coherence_placement_pct":  metriques["coherence_placement"]["coherent_pct"],
        },
    }
    path_manifest = EXPORT_DIR / "deployment_manifest.json"
    with open(path_manifest, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    paths["manifest"] = path_manifest

    # ── Affichage résumé sauvegarde ───────────────────────────────────────────
    print(f"\n  Fichiers sauvegardés :")
    print(f"  {'─'*52}")
    for nom, p in paths.items():
        size_kb = p.stat().st_size / 1024
        icon    = "🤖" if p.suffix == ".joblib" else ("📊" if p.suffix == ".csv" else "📋")
        print(f"  {icon} {nom:<22} → {p}  ({size_kb:.1f} KB)")

    print(f"\n  ✅ P5 terminé — tout sauvegardé pour le déploiement")
    return paths


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ FINAL
# ══════════════════════════════════════════════════════════════════════════════

def afficher_resume_final(df: pd.DataFrame, metriques: dict, paths: dict) -> None:
    """Affiche le résumé exécutif final du pipeline."""
    m_k  = metriques["k_predictor"]
    m_c  = metriques["capacite_fat"]
    m_ca = metriques["cables"]
    m_co = metriques["coherence_placement"]

    print(f"\n{SEP1}")
    print(f"  RÉSUMÉ EXÉCUTIF — FAT PLANNER HYBRIDE")
    print(SEP1)

    print(f"""
  DONNÉES
  {'─'*50}
  Abonnés traités     : {len(df):>10,}
  Bâtiments           : {df['id_batiment'].nunique():>10,}
  FATs générés        : {df['FAT_relative'].nunique():>10,}

  PIPELINE (4 étapes déterministes + 1 ML)
  {'─'*50}
  Étape 1 — Greedy séquentiel par 8     ✅  déterministe
  Étape 2 — Médiane analytique (étage)  ✅  optimal par théorème
  Étape 3 — Distance physique exacte    ✅  haversine+vertical+palier
  Étape 4 — Snap câble if/elif          ✅  règle AT officielle
  Étape 5 — K-Predictor (ML)            ✅  pour bâtiments OSM futurs

  QUALITÉ RÉSEAU
  {'─'*50}
  Conformité FAT ≤ 8 abonnés  : {m_c['conformes_pct']:>6.1f}%
  Placement FAT cohérent       : {m_co['coherent_pct']:>6.1f}%
  Gaspillage câble moyen       : {m_ca['gaspillage_moyen_m']:>6.2f} m/lien
  Gaspillage total             : {m_ca['gaspillage_total_km']:>6.2f} km  ({m_ca['gaspillage_pct']:.1f}%)

  K-PREDICTOR (ML — bâtiments OSM)
  {'─'*50}
  R²                  : {m_k['R2_pct']:>6.1f}%
  MAE                 : {m_k['MAE_fats']:>6.3f} FATs
  Accuracy K exact    : {m_k['Accuracy_exact']:>6.1f}%
  Accuracy K ± 1 FAT : {m_k['Accuracy_1fat']:>6.1f}%

  FICHIERS DÉPLOIEMENT
  {'─'*50}""")

    for nom, p in paths.items():
        print(f"  {p}")

    print(f"\n{SEP1}\n")


# ══════════════════════════════════════════════════════════════════════════════
# INFÉRENCE — fonction de production pour le frontend
# ══════════════════════════════════════════════════════════════════════════════

def predire_k_nouveau_batiment(
    nb_etages:          int,
    nb_log_etage:       int,
    hauteur_etage:      float,
    type_batiment:      str,
    presence_commerce:  int,
    model_dir:          Path = MODEL_DIR,
) -> dict:
    """
    Prédit K = nb_FATs pour un nouveau bâtiment OSM.
    Fonction de production — appelée par le frontend.

    Paramètres :
        nb_etages         : number of floors (from OSM building:levels)
        nb_log_etage      : estimated apartments per floor
        hauteur_etage     : floor height in meters
        type_batiment     : "AADL", "HLM", "LPP", "LPA", "LSL", "CNEP", "PRIVE"
        presence_commerce : 1 if ground floor is commercial, else 0

    Retourne un dict avec :
        K_predit     : nombre de FATs prédit
        cables_estim : estimation des câbles nécessaires
        confiance    : score de confiance basé sur les métriques du modèle
    """
    bundle      = joblib.load(model_dir / "k_predictor.joblib")
    snap_bundle = joblib.load(model_dir / "snap_rules.joblib")
    cfg         = joblib.load(model_dir / "pipeline_config.joblib")

    type_enc        = bundle["type_bat_map"].get(type_batiment, 99)
    hauteur_totale  = nb_etages * hauteur_etage
    nb_abonnes_est  = nb_etages * nb_log_etage

    X = np.array([[
        nb_etages, nb_log_etage, hauteur_etage,
        hauteur_totale, type_enc, presence_commerce
    ]])

    K_pred = max(1, int(round(bundle["model"].predict(X)[0])))

    # Estimation câbles : distance moy ≈ (nb_etages/K/4) * hauteur + palier
    dist_moy_est  = (nb_etages / K_pred / 4) * hauteur_etage + PALIER_FIXE_M
    cable_est     = snap_cable_scalar(dist_moy_est)
    total_cables  = K_pred * FAT_CAPACITY * cable_est

    return {
        "K_predit":           K_pred,
        "nb_abonnes_estimes": nb_abonnes_est,
        "cable_type_m":       cable_est,
        "cable_total_estime_m": total_cables,
        "modele_r2_pct":      bundle["metrics"]["R2_pct"],
        "modele_acc1_pct":    bundle["metrics"]["Accuracy_1fat"],
    }


# ══════════════════════════════════════════════════════════════════════════════
# DATASET SYNTHÉTIQUE (test sans CSV réel)
# ══════════════════════════════════════════════════════════════════════════════

def _generer_synthetique(n_bats: int = 120) -> str:
    """
    Génère un dataset synthétique fidèle au Generateur.py v13.
    Utilisé quand aucun CSV réel n'est fourni.
    """
    from math import radians, cos, ceil as mceil

    rng    = np.random.RandomState(RANDOM_SEED)
    rows   = []
    TYPES  = ["AADL","HLM","LPP","LPA","LSL","CNEP","PRIVE"]
    PROBS  = [0.40,  0.20, 0.15, 0.10, 0.08, 0.04,  0.03]
    LAT_C, LON_C = 35.697, -0.633

    for bat_i in range(n_bats):
        type_bat = rng.choice(TYPES, p=PROBS)
        H_RANGES = {"AADL":(2.9,3.2),"HLM":(2.6,2.9),"LPP":(3.0,3.4),
                    "LPA":(2.8,3.1),"LSL":(2.6,2.8),"CNEP":(3.0,3.3),"PRIVE":(3.1,3.6)}
        h_et     = round(rng.uniform(*H_RANGES[type_bat]), 2)
        nb_et    = int(rng.randint(3, 14))
        nb_log   = int(rng.randint(4, 14))
        has_com  = int(rng.random() < 0.40)
        lat_c    = LAT_C + rng.uniform(-0.03, 0.03)
        lon_c    = LON_C + rng.uniform(-0.03, 0.03)
        bat_id   = f"BAT-{bat_i:03d}"
        commune  = rng.choice(["Oran","Gdyel","Bir El Djir","Es Senia","Arzew"])
        bloc     = rng.choice(["A","B","C","D"])
        num_bat  = rng.randint(1, 6)

        # Positions grille (comme generer_positions_batiment_v12)
        n_cols  = mceil(nb_log ** 0.5)
        n_rows  = mceil(nb_log / n_cols)
        dx_m    = max(2.0, min(8.0 / max(n_cols-1, 1), 12.0))
        dy_m    = max(2.0, min(8.0 / max(n_rows-1, 1), 12.0))
        cos_lat = cos(radians(lat_c))

        def pos(i: int):
            col = i % n_cols; row = i // n_cols
            return (round(lat_c + (row-(n_rows-1)/2)*dy_m/111_000, 6),
                    round(lon_c + (col-(n_cols-1)/2)*dx_m/(111_000*cos_lat), 6))

        fdt_nom = f"F310-{bat_i+1:03d}-01"
        fat_ctr = 0

        # Commerce RDC
        if has_com:
            fat_ctr += 1
            fat_id  = f"F310-{bat_i+1:03d}-01-{fat_ctr:02d}-{commune.upper().replace(' ','-')}-{bloc}-{num_bat}-Commerce(1,2,3)-0F-1"
            for j in range(3):
                ab_lat, ab_lon = pos(j % nb_log)
                fat_lat, fat_lon = lat_c, lon_c
                rows.append(_make_row(
                    bat_id, fat_id, 0, f"Commerce {j+1}", "commerces",
                    ab_lat, ab_lon, fat_lat, fat_lon, 0,
                    nb_et, nb_log, type_bat, has_com, h_et,
                    commune, fdt_nom, lat_c, lon_c, len(rows)
                ))

        # Résidentiel — Greedy par 8 (identique au générateur v13)
        all_res = [(et, i) for et in range(1, nb_et+1) for i in range(nb_log)]
        for gi in range(0, len(all_res), FAT_CAPACITY):
            grp        = all_res[gi:gi+FAT_CAPACITY]
            fat_ctr   += 1
            etages_grp = [e for e,_ in grp]
            etage_fat  = int(np.median(etages_grp))
            et_label   = (str(etages_grp[0]) if len(set(etages_grp))==1
                          else f"{min(etages_grp)}-{max(etages_grp)}")
            portes_str = ",".join(str(i+1) for _,i in grp)
            fat_id     = f"F310-{bat_i+1:03d}-01-{fat_ctr:02d}-{commune.upper().replace(' ','-')}-{bloc}-{num_bat}-Porte({portes_str})-{et_label}F-1"

            g_lats = [pos(idx%nb_log)[0] for _,idx in grp]
            g_lons = [pos(idx%nb_log)[1] for _,idx in grp]
            fat_lat = round(float(np.mean(g_lats)), 6)
            fat_lon = round(float(np.mean(g_lons)), 6)

            for et, idx in grp:
                ab_lat, ab_lon = pos(idx % nb_log)
                dv     = abs(et - etage_fat) * h_et
                dh_m   = float(np.sqrt(((ab_lat-fat_lat)*111000)**2 + ((ab_lon-fat_lon)*111000*cos_lat)**2))
                d_real = round(dv + dh_m + PALIER_FIXE_M, 2)
                rows.append(_make_row(
                    bat_id, fat_id, et, f"Porte {idx+1}", "logements",
                    ab_lat, ab_lon, fat_lat, fat_lon, d_real,
                    nb_et, nb_log, type_bat, has_com, h_et,
                    commune, fdt_nom, lat_c, lon_c, len(rows),
                    etage_fat=etage_fat
                ))

    df = pd.DataFrame(rows)
    # Snap câble
    df["cable_prefab_m"]  = snap_cable_vec(df["distance_real_m"].values)
    df["waste_m"]         = (df["cable_prefab_m"] - df["distance_real_m"]).round(2)
    df["distance_snap_m"] = df["cable_prefab_m"]
    df["distance_FAT_m"]  = df["distance_real_m"]

    path = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_generee_v13_advanced\dataset_fusionnee_final.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")

    # Stats
    cables = df["cable_prefab_m"].values

    print(f"\n  Dataset synthétique : {len(df):,} abonnés · {df['id_batiment'].nunique()} bâtiments")
    print(f"  Distribution câbles :")
    for c in PREFAB_CABLES + [9999]:
        n = (cables == c).sum()
        if n > 0:
            print(f"    {c}m : {n:,} ({n/len(cables)*100:.1f}%)")
    return path


def _make_row(bat_id, fat_id, etage, porte, usage,
              ab_lat, ab_lon, fat_lat, fat_lon, d_real,
              nb_et, nb_log, type_bat, has_com, h_et,
              commune, fdt_nom, lat_fdt, lon_fdt, seq,
              etage_fat=None) -> dict:
    if etage_fat is None:
        etage_fat = etage
    bat_pav = f"{type_bat} - {commune} Bloc A-Num 1"
    return {
        "code_client":             f"CLI-{seq:07d}",
        "id_batiment":             bat_id,
        "id_zone":                 f"Z310-{int(bat_id.split('-')[1]):03d}",
        "FAT_relative":            fat_id,
        "etage":                   etage,
        "porte":                   porte,
        "usage":                   usage,
        "lat_abonne":              ab_lat,
        "lon_abonne":              ab_lon,
        "lat_fat":                 fat_lat,
        "lon_fat":                 fat_lon,
        "etage_fat":               etage_fat,
        "nb_etages_bat":           nb_et,
        "nb_log_etage":            nb_log,
        "nbr_etages":              nb_et,
        "nbr_logements_par_etage": nb_log,
        "nbr_logements_total":     nb_et * nb_log,
        "type_batiment":           type_bat,
        "presence_de_commerce":    has_com,
        "Hauteur par étage (m)":   h_et,
        "nb_abonnes_sim":          FAT_CAPACITY,
        "distance_real_m":         d_real,
        "distance_FAT_m":          d_real,
        "cable_prefab_m":          15,
        "distance_snap_m":         15,
        "waste_m":                 0.0,
        "z_coord":                 round(etage * h_et, 2),
        "nom_FDT":                 fdt_nom,
        "lat_fdt":                 lat_fdt,
        "lon_fdt":                 lon_fdt,
        "distance_olt_m":          200,
        "num_de_groupe":           1,
        "nom":                     "Test Abonné",
        "batiment_pav":            bat_pav,
        "quartier":                commune,
        "commune":                 commune,
    }


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def main(csv_path: str) -> None:
    print(f"\n{SEP1}")
    print(f"  FAT PLANNER HYBRIDE — FTTH Smart Planner")
    print(f"  Algérie Télécom Oran  ·  v1.0")
    print(f"{SEP1}")
    print(f"  Pipeline : Greedy → Médiane → Snap (if/elif) → K-Predictor (XGBoost)")
    print(f"  Chaque étape fait ce pour quoi elle est la meilleure.")
    print(f"{SEP1}")

    # P1 — Chargement
    df = p1_charger_auditer(csv_path)

    # P2 — Features
    df, feature_cols = p2_feature_engineering(df)

    # P3 — K-Predictor
    res_k = p3_entrainer_k_predictor(df, feature_cols)

    # P4 — Métriques
    metriques = p4_metriques_reseau(df, res_k)

    # P5 — Sauvegarde
    paths = p5_sauvegarder(res_k, feature_cols, metriques, df)

    # Résumé final
    afficher_resume_final(df, metriques, paths)

    # Démo inférence production (si modèles sauvegardés)
    print(f"  DÉMO INFÉRENCE — Nouveau bâtiment OSM AADL R+7")
    print(f"  {SEP2}")
    try:
        result = predire_k_nouveau_batiment(
            nb_etages=7, nb_log_etage=6, hauteur_etage=3.0,
            type_batiment="AADL", presence_commerce=1,
        )
        print(f"  K prédit           : {result['K_predit']} FATs")
        print(f"  Abonnés estimés    : {result['nb_abonnes_estimes']}")
        print(f"  Type câble estimé  : {result['cable_type_m']}m")
        print(f"  Câble total estimé : {result['cable_total_estime_m']}m")
        print(f"  Fiabilité modèle   : R²={result['modele_r2_pct']:.1f}%  Acc@1={result['modele_acc1_pct']:.1f}%")
    except Exception as e:
        print(f"  ⚠️  Démo inférence : {e}")

    print(f"\n{SEP1}")
    print(f"  ✅ PIPELINE TERMINÉ")
    print(f"{SEP1}\n")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        print("\n⚠️  Aucun CSV fourni — mode TEST avec dataset synthétique")
        print("   Usage : python fat_planner_hybride.py <dataset_fusionnee_final.csv>\n")
        csv_path = _generer_synthetique(n_bats=150)
        main(csv_path)