
import joblib
import pandas as pd
import numpy as np
from pathlib import Path
import optuna
from optuna.samplers import TPESampler
import xgboost as xgb
from optuna.study import study
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
import warnings

from config import settings

warnings.filterwarnings("ignore")

print(f"XGBoost version : {xgb.__version__}")

# ========================= CONFIG =========================
DATA_PATH = "dataset_v21.csv"
MODEL_DIR = Path("models_TEST")
MODEL_DIR.mkdir(exist_ok=True)

RANDOM_SEED = 2026
N_TRIALS = 80


# =========================================================

def load_and_prepare_data():
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    df = pd.get_dummies(df, columns=['type_batiment'], prefix='type', dtype=int)

    feature_cols = [
        "nb_etages_bat", "nb_log_etage"
        , "presence_de_commerce",
        "type_AADL", "type_HLM", "type_LPP", "type_LPA",
        "type_LSL", "type_CNEP", "type_PRIVE"
    ]

    # Features enrichies
    df["surface_par_logement"] = df["surface_m2"] / df["nb_logements_total"].clip(lower=1)
    df["hauteur_par_etage"] = df["hauteur_bat_totale_m"] / df["nb_etages_bat"].clip(lower=1)

    feature_cols.extend(["surface_par_logement", "hauteur_par_etage"])

    X = df[feature_cols].fillna(0)
    y = df["nb_logements_habites"].astype(int)

    return X, y, feature_cols



def objective(trial, X_train, y_train, X_val, y_val):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 300, 1000),
        "max_depth": trial.suggest_int("max_depth", 4, 9),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "subsample": trial.suggest_float("subsample", 0.75, 0.95),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.75, 0.95),
        "min_child_weight": trial.suggest_int("min_child_weight", 3, 12),
        "reg_alpha": trial.suggest_float("reg_alpha", 0.1, 5.0),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.5, 8.0),
        "random_state": RANDOM_SEED,
        "n_jobs": -1,
        "verbosity": 0,
    }

    model = xgb.XGBRegressor(**params)
    model.fit(X_train, y_train, verbose=False)  # Sans early stopping pour compatibilité

    preds = model.predict(X_val)
    mae = mean_absolute_error(y_val, preds)
    return -mae


from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    median_absolute_error,
    r2_score,
    max_error
)
import numpy as np


def evaluate_model(model, X, y_true, dataset_name=""):
    y_pred = model.predict(X)

    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    medae = median_absolute_error(y_true, y_pred)
    max_err = max_error(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / y_true.replace(0, 1))) * 100
    median_pe = np.median(np.abs((y_true - y_pred) / y_true.replace(0, 1))) * 100
    r2 = r2_score(y_true, y_pred)
    y_pred_rounded = np.round(y_pred).astype(int)

    exact_match = np.mean(y_pred_rounded == y_true.values) * 100
    within_1 = np.mean(np.abs(y_pred_rounded - y_true.values) <= 1) * 100
    within_2 = np.mean(np.abs(y_pred_rounded - y_true.values) <= 2) * 100


    metrics = {
        "MAE": mae,
        "RMSE": rmse,
        "MedAE": medae,
        "Max Error": max_err,
        "MAPE (%)": mape,
        "Median PE (%)": median_pe,
        "R²": r2,
        "Exact Match (%)": exact_match,   # ← ajouter
        "Within 1 (%)": within_1,
        "within_2 (%)":within_2,
    }

    print(f"\n📊 ÉVALUATION — {dataset_name}")
    print("=" * 60)
    print(f"   MAE                    : {mae:.4f}")
    print(f"   RMSE                   : {rmse:.4f}")
    print(f"   MedAE                  : {medae:.4f}")
    print(f"   Max Error              : {max_err:.4f}")
    print(f"   MAPE (%)               : {mape:.2f}%")
    print(f"   Median PE (%)          : {median_pe:.2f}%")
    print(f"   R²                     : {r2:.4f}")
    print(f"   Prédictions exactes    : {exact_match:.1f}%")
    print(f"   Erreur ≤ 1 logement    : {within_1:.1f}%")
    print(f"   Erreur ≤ 2 logements   : {within_2:.1f}%")
    return metrics

def train_model():
    X, y, feature_cols = load_and_prepare_data()

    # === SPLIT STRICT : hold-out réservé AVANT toute optimisation ===
    X_dev, X_holdout, y_dev, y_holdout = train_test_split(
        X, y, test_size=0.20, random_state=RANDOM_SEED
    )

    # Split dev → train / val pour Optuna
    X_train, X_val, y_train, y_val = train_test_split(
        X_dev, y_dev, test_size=0.20, random_state=RANDOM_SEED
    )

    print(f"\n📐 Tailles des splits :")
    print(f"   Train      : {len(X_train)} lignes")
    print(f"   Validation : {len(X_val)} lignes")
    print(f"   Hold-out   : {len(X_holdout)} lignes (jamais vu pendant l'optim)")

    print(f"\n🚀 Optimisation Optuna en cours ({N_TRIALS} trials)...")

    study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=RANDOM_SEED))
    study.optimize(
        lambda trial: objective(trial, X_train, y_train, X_val, y_val),
        n_trials=N_TRIALS,
        show_progress_bar=True
    )

    print(f"\n✅ Meilleurs paramètres : {study.best_params}")
    print(f"   Meilleur MAE (val)   : {-study.best_value:.4f}")

    # === Entraînement final sur train + val UNIQUEMENT (pas le hold-out) ===
    best_params = study.best_params
    final_model = xgb.XGBRegressor(**best_params, random_state=RANDOM_SEED, n_jobs=-1)
    final_model.fit(X_dev, y_dev, verbose=False)  # X_dev = train + val

    # === Évaluation sur les 3 sets ===
    print("\n" + "=" * 70)
    print("📊 ÉVALUATION FINALE DU MODÈLE")
    print("=" * 70)

    evaluate_model(final_model, X_train, y_train, "TRAIN")
    evaluate_model(final_model, X_val, y_val, "VALIDATION")
    evaluate_model(final_model, X_holdout, y_holdout, "HOLD-OUT (jamais vu)")  # ← métrique fiable

    # === Sauvegarde ===
    model_bundle = {
        "model": final_model,
        "feature_cols": feature_cols,
        "best_params": best_params,
        "best_mae": -study.best_value,
    }

    model_path = MODEL_DIR / "k_predictor_osm_optuna.joblib"
    joblib.dump(model_bundle, model_path)
    print(f"\n💾 Modèle sauvegardé : {model_path}")


if __name__ == "__main__":
    train_model()