# =============================================================================
# MODÈLE K-MEANS PLACEMENT FATs — FTTH Smart Planner
# =============================================================================
#
# CONTEXTE MÉTIER :
#   Ce modèle prend en entrée les abonnés d'UN bâtiment (groupés par id_batiment)
#   et propose les positions optimales des FATs via K-means contraint.
#
# POURQUOI K-MEANS ICI :
#   Les abonnés sont des points géographiques 3D (lat, lon, étage).
#   On veut les regrouper en clusters de ≤ 8 (capacité FAT AT).
#   Le centroïde de chaque cluster = position optimale de la FAT.
#
# ÉVALUATION :
#   On a la ground truth (FAT_relative) → on peut calculer une accuracy réelle.
#   5 métriques complémentaires couvrent tous les aspects du problème.
#
# COLONNES DATASET UTILISÉES :
#   Features  : lat_abonne, lon_abonne, etage
#   Grouping  : id_batiment (un KMeans PAR bâtiment)
#   Labels GT : FAT_relative (ground truth AT)
#   Infra     : nom_FDT, lat_fdt, lon_fdt (assignation FDT)
#   Validation: nb_abonnes_sim, distance_FAT_m, usage
# =============================================================================

import numpy as np
import pandas as pd
from math import ceil, radians, cos, sin, asin, sqrt
from dataclasses import dataclass, field
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, adjusted_rand_score
import warnings
from config import settings
warnings.filterwarnings("ignore")

# Config AT — importée depuis config.py
FAT_CAPACITY      = settings.FAT_CAPACITY       # Capacité max d'une FAT (standard AT Oran)
FLOOR_HEIGHT_M    = 3.0                         # Hauteur inter-étage standard (mètres)
METERS_PER_DEGREE = 111_000.0                   # 1 degré ≈ 111km en Algérie
VERTICAL_WEIGHT   = 2.0                         # Importance étage vs position horizontale


# =============================================================================
# STRUCTURES DE RÉSULTATS
# =============================================================================

@dataclass
class FATCandidate:
    """Une FAT proposée par l'algorithme pour un bâtiment."""
    fat_id: str                        # ID généré : FAT-001-RES, FAT-002-COM...
    cluster_label: int                 # Label K-means (0, 1, 2...)
    centroid_lat: float                # Latitude optimale (centroïde)
    centroid_lon: float                # Longitude optimale (centroïde)
    assigned_floor: int                # Étage physique d'installation
    subscriber_ids: list               # code_client des abonnés dans ce cluster
    n_subscribers: int                 # Nombre d'abonnés
    usage: str                         # 'logements' ou 'commerces'
    fdt_assigned: str                  # FDT la plus proche
    capacity_ok: bool = True           # Respecte contrainte ≤ 8


@dataclass
class BuildingKMeansResult:
    """Résultat complet pour un bâtiment."""
    id_batiment: str
    id_zone: str
    n_subscribers_total: int
    fat_candidates: list[FATCandidate]
    n_fats_proposed: int
    n_fats_ground_truth: int           # Nombre de FATs réelles AT
    # Métriques
    silhouette: float
    ari_score: float                   # Adjusted Rand Index vs ground truth
    capacity_compliance_pct: float     # % FATs qui respectent ≤ 8
    fat_count_match: bool              # Notre K == K réel AT
    fdt_match_pct: float               # % FATs assignées au bon FDT
    inertia: float


@dataclass
class GlobalMetrics:
    """Métriques agrégées sur tous les bâtiments traités."""
    n_buildings_processed: int = 0
    n_buildings_skipped: int = 0       # Trop peu d'abonnés, usage=commerces seul...
    # Accuracy globale
    mean_ari: float = 0.0              # ARI moyen (0=aléatoire, 1=parfait)
    mean_silhouette: float = 0.0
    mean_capacity_compliance: float = 0.0
    fat_count_match_rate: float = 0.0  # % bâtiments où K proposé == K réel
    mean_fdt_match: float = 0.0
    # Distribution K
    k_over_estimated: int = 0          # K proposé > K réel
    k_under_estimated: int = 0         # K proposé < K réel
    k_exact: int = 0                   # K proposé == K réel


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distance en mètres entre deux points GPS (formule Haversine)."""
    R = 6_371_000
    la1, lo1, la2, lo2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return R * 2 * asin(sqrt(max(0, a)))


def calculate_k_optimal(n_abonnes: int) -> int:
    """
    Calcule K (nombre de FATs) via la règle AT seuil 0.75.
    """
    if n_abonnes <= 0:
        return 0
    k = ceil(n_abonnes / FAT_CAPACITY)
    return max(1, k)


def find_nearest_fdt(lat: float, lon: float, fdts_df: pd.DataFrame) -> str:
    """
    Trouve le FDT le plus proche d'un point (lat, lon).
    fdts_df doit avoir les colonnes : nom_FDT, lat_fdt, lon_fdt
    """
    if fdts_df.empty:
        return "FDT_INCONNU"

    fdts_unique = fdts_df[["nom_FDT","lat_fdt","lon_fdt"]].drop_duplicates("nom_FDT")
    distances = fdts_unique.apply(
        lambda r: haversine_m(lat, lon, r["lat_fdt"], r["lon_fdt"]),
        axis=1
    )
    return fdts_unique.loc[distances.idxmin(), "nom_FDT"]


# =============================================================================
# CLASSE PRINCIPALE : MODÈLE K-MEANS PAR BÂTIMENT
# =============================================================================

class FATKMeansModel:
    """
    Modèle de placement FATs par K-means contraint.
    """

    def __init__(self, random_state: int = 2026, n_init: int = 15):
        self.random_state = random_state
        self.n_init = n_init

        # === PIPELINE FROZEN (sauvegardé avec joblib) ===
        self.FAT_CAPACITY      = FAT_CAPACITY
        self.FLOOR_HEIGHT_M    = FLOOR_HEIGHT_M
        self.METERS_PER_DEGREE = METERS_PER_DEGREE
        self.VERTICAL_WEIGHT   = VERTICAL_WEIGHT

        self.results_: list[BuildingKMeansResult] = []
        self.global_metrics_: GlobalMetrics = GlobalMetrics()

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 1 : Préparer les features
    # ──────────────────────────────────────────────────────────────────────────

    def _build_feature_matrix(self, df_bat: pd.DataFrame) -> tuple:
        floor_in_deg = self.FLOOR_HEIGHT_M / self.METERS_PER_DEGREE

        X = np.column_stack([
            df_bat["lat_abonne"].values,
            df_bat["lon_abonne"].values,
            df_bat["etage"].values * floor_in_deg * self.VERTICAL_WEIGHT
        ])

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        return X_scaled, scaler, X

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 2 : Lancer K-means pour un groupe d'abonnés
    # ──────────────────────────────────────────────────────────────────────────

    def _run_kmeans(
        self,
        X_scaled: np.ndarray,
        X_original: np.ndarray,
        k: int
    ) -> tuple[np.ndarray, np.ndarray, float]:
        km = KMeans(
            n_clusters=k,
            n_init=self.n_init,
            max_iter=300,
            init='k-means++',
            random_state=self.random_state
        )
        labels = km.fit_predict(X_scaled)

        centroids_original = np.array([
            X_original[labels == c].mean(axis=0)
            for c in range(k)
        ])

        return labels, centroids_original, km.inertia_

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 3 : Construire les FAT candidates depuis les clusters
    # ──────────────────────────────────────────────────────────────────────────

    def _build_fat_candidates(
        self,
        df_bat: pd.DataFrame,
        labels: np.ndarray,
        centroids: np.ndarray,
        usage_type: str,
        id_offset: int = 0
    ) -> list[FATCandidate]:
        candidates = []

        for cluster_id in range(len(centroids)):
            mask = labels == cluster_id
            subset = df_bat[mask]

            if subset.empty:
                continue

            n_sub = len(subset)
            centroid = centroids[cluster_id]
            assigned_floor = int(np.median(subset["etage"].values))

            fdt_nearest = find_nearest_fdt(centroid[0], centroid[1], df_bat)

            fat_num = cluster_id + 1 + id_offset
            usage_code = "LOG" if usage_type == "logements" else "COM"

            candidates.append(FATCandidate(
                fat_id=f"FAT-{fat_num:03d}-{usage_code}",
                cluster_label=cluster_id,
                centroid_lat=round(centroid[0], 6),
                centroid_lon=round(centroid[1], 6),
                assigned_floor=assigned_floor,
                subscriber_ids=subset["code_client"].tolist(),
                n_subscribers=n_sub,
                usage=usage_type,
                fdt_assigned=fdt_nearest,
                capacity_ok=(n_sub <= self.FAT_CAPACITY)
            ))

        return candidates

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 4 : Post-traitement — subdiviser les clusters trop grands
    # ──────────────────────────────────────────────────────────────────────────

    def _split_oversized_clusters(
        self,
        df_bat: pd.DataFrame,
        candidates: list[FATCandidate]
    ) -> list[FATCandidate]:
        fixed_candidates = []
        id_counter = 1

        for fat in candidates:
            if fat.capacity_ok:
                fat.fat_id = f"FAT-{id_counter:03d}-{'LOG' if fat.usage=='logements' else 'COM'}"
                fixed_candidates.append(fat)
                id_counter += 1
            else:
                subset = df_bat[df_bat["code_client"].isin(fat.subscriber_ids)].copy()
                k_sub = ceil(fat.n_subscribers / self.FAT_CAPACITY)

                if len(subset) < k_sub:
                    k_sub = len(subset)

                X_sub_scaled, _, X_sub_orig = self._build_feature_matrix(subset)
                labels_sub, centroids_sub, _ = self._run_kmeans(
                    X_sub_scaled, X_sub_orig, k_sub
                )

                sub_candidates = self._build_fat_candidates(
                    subset, labels_sub, centroids_sub, fat.usage, id_offset=0
                )

                for sc in sub_candidates:
                    sc.fat_id = f"FAT-{id_counter:03d}-{'LOG' if sc.usage=='logements' else 'COM'}"
                    fixed_candidates.append(sc)
                    id_counter += 1

        return fixed_candidates

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 5 : Calculer les métriques d'accuracy
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_metrics(
        self,
        df_bat: pd.DataFrame,
        labels_proposed: np.ndarray,
        k_proposed: int,
        fat_candidates: list[FATCandidate],
        X_scaled: np.ndarray
    ) -> dict:
        metrics = {}

        # Silhouette
        if k_proposed > 1 and len(X_scaled) > k_proposed:
            try:
                metrics["silhouette"] = round(silhouette_score(X_scaled, labels_proposed), 4)
            except Exception:
                metrics["silhouette"] = 0.0
        else:
            metrics["silhouette"] = 1.0

        # ARI
        fat_labels_gt = pd.Categorical(df_bat["FAT_relative"]).codes
        k_ground_truth = df_bat["FAT_relative"].nunique()

        if k_proposed > 1 and k_ground_truth > 1:
            metrics["ari"] = round(adjusted_rand_score(fat_labels_gt, labels_proposed), 4)
        else:
            metrics["ari"] = 1.0 if k_proposed == k_ground_truth else 0.0

        metrics["k_ground_truth"] = k_ground_truth

        # Capacity Compliance
        n_valid = sum(1 for f in fat_candidates if f.capacity_ok)
        metrics["capacity_compliance_pct"] = round(
            (n_valid / len(fat_candidates)) * 100, 1
        ) if fat_candidates else 100.0

        # FAT Count Match
        metrics["fat_count_match"] = (k_proposed == k_ground_truth)

        # FDT Match
        fdt_matches = 0
        for fat in fat_candidates:
            subset = df_bat[df_bat["code_client"].isin(fat.subscriber_ids)]
            fdt_real_majority = subset["nom_FDT"].mode()
            if not fdt_real_majority.empty and fat.fdt_assigned == fdt_real_majority.iloc[0]:
                fdt_matches += 1
        metrics["fdt_match_pct"] = round(
            (fdt_matches / len(fat_candidates)) * 100, 1
        ) if fat_candidates else 0.0

        return metrics

    # ──────────────────────────────────────────────────────────────────────────
    # MÉTHODE PRINCIPALE : Traiter un bâtiment complet
    # ──────────────────────────────────────────────────────────────────────────

    def process_building(self, df_bat: pd.DataFrame) -> BuildingKMeansResult | None:
        id_batiment = df_bat["id_batiment"].iloc[0]
        id_zone = df_bat["id_zone"].iloc[0]

        df_res = df_bat[df_bat["usage"] == "logements"].copy()
        df_com = df_bat[df_bat["usage"] == "commerces"].copy()

        all_candidates = []
        label_offset = 0

        # Résidentiels
        if len(df_res) >= 2:
            k_res = calculate_k_optimal(len(df_res))
            k_res = min(k_res, len(df_res))

            X_scaled, _, X_orig = self._build_feature_matrix(df_res)
            labels, centroids, inertia = self._run_kmeans(X_scaled, X_orig, k_res)

            candidates_res = self._build_fat_candidates(
                df_res, labels, centroids, "logements", id_offset=label_offset
            )
            all_candidates.extend(candidates_res)
            label_offset += k_res
            main_inertia = inertia
            main_X_scaled = X_scaled
            main_labels = labels
        else:
            main_inertia = 0.0
            main_X_scaled = np.zeros((1, 3))
            main_labels = np.array([0])

        # Commerces
        if len(df_com) >= 1:
            k_com = calculate_k_optimal(len(df_com))
            k_com = min(k_com, len(df_com))

            if len(df_com) >= 2:
                X_scaled_c, _, X_orig_c = self._build_feature_matrix(df_com)
                labels_c, centroids_c, _ = self._run_kmeans(X_scaled_c, X_orig_c, k_com)
            else:
                labels_c = np.array([0])
                centroids_c = df_com[["lat_abonne","lon_abonne","etage"]].values
                X_scaled_c = centroids_c

            candidates_com = self._build_fat_candidates(
                df_com, labels_c, centroids_c, "commerces", id_offset=label_offset
            )
            all_candidates.extend(candidates_com)

        if not all_candidates:
            return None

        # Post-processing capacité
        all_candidates = self._split_oversized_clusters(df_bat, all_candidates)

        # Métriques
        k_total_proposed = len(all_candidates)
        all_labels_final = np.full(len(df_bat), 0)
        for i, fat in enumerate(all_candidates):
            for cid in fat.subscriber_ids:
                pos = df_bat[df_bat["code_client"] == cid].index
                if not pos.empty:
                    all_labels_final[df_bat.index.get_loc(pos[0])] = i

        metrics = self._compute_metrics(
            df_bat, all_labels_final, k_total_proposed,
            all_candidates, main_X_scaled
        )

        return BuildingKMeansResult(
            id_batiment=id_batiment,
            id_zone=id_zone,
            n_subscribers_total=len(df_bat),
            fat_candidates=all_candidates,
            n_fats_proposed=k_total_proposed,
            n_fats_ground_truth=metrics["k_ground_truth"],
            silhouette=metrics["silhouette"],
            ari_score=metrics["ari"],
            capacity_compliance_pct=metrics["capacity_compliance_pct"],
            fat_count_match=metrics["fat_count_match"],
            fdt_match_pct=metrics["fdt_match_pct"],
            inertia=main_inertia
        )

    # ──────────────────────────────────────────────────────────────────────────
    # MÉTHODE D'ENTRAÎNEMENT
    # ──────────────────────────────────────────────────────────────────────────

    def fit(self, df: pd.DataFrame, max_buildings: int = None) -> "FATKMeansModel":
        self.results_ = []
        buildings = df["id_batiment"].unique()
        if max_buildings:
            buildings = buildings[:max_buildings]

        print(f"🚀 Lancement K-means sur {len(buildings)} bâtiments...")
        print(f"   KMeans    : n_init={self.n_init}, vertical_weight={self.VERTICAL_WEIGHT}")

        metrics_ari, metrics_sil, metrics_cap, metrics_fdt = [], [], [], []
        k_matches, k_over, k_under = 0, 0, 0
        skipped = 0

        for i, bat_id in enumerate(buildings):
            df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)

            if len(df_bat) < 2:
                skipped += 1
                continue

            result = self.process_building(df_bat)
            if result is None:
                skipped += 1
                continue

            self.results_.append(result)

            metrics_ari.append(result.ari_score)
            metrics_sil.append(result.silhouette)
            metrics_cap.append(result.capacity_compliance_pct)
            metrics_fdt.append(result.fdt_match_pct)

            if result.fat_count_match:
                k_matches += 1
            elif result.n_fats_proposed > result.n_fats_ground_truth:
                k_over += 1
            else:
                k_under += 1

            if (i + 1) % 100 == 0:
                print(f"   → {i+1}/{len(buildings)} bâtiments traités...")

        n = len(self.results_)
        self.global_metrics_ = GlobalMetrics(
            n_buildings_processed=n,
            n_buildings_skipped=skipped,
            mean_ari=round(np.mean(metrics_ari), 4) if metrics_ari else 0,
            mean_silhouette=round(np.mean(metrics_sil), 4) if metrics_sil else 0,
            mean_capacity_compliance=round(np.mean(metrics_cap), 2) if metrics_cap else 0,
            fat_count_match_rate=round(k_matches / n * 100, 2) if n > 0 else 0,
            mean_fdt_match=round(np.mean(metrics_fdt), 2) if metrics_fdt else 0,
            k_over_estimated=k_over,
            k_under_estimated=k_under,
            k_exact=k_matches
        )

        self.summary()
        return self

    # ──────────────────────────────────────────────────────────────────────────
    # AFFICHAGE + EXPORT
    # ──────────────────────────────────────────────────────────────────────────

    def summary(self):
        g = self.global_metrics_
        n = g.n_buildings_processed

        print()
        print("=" * 62)
        print("       RÉSULTATS MODÈLE K-MEANS — FTTH Smart Planner")
        print("=" * 62)
        print(f"  Bâtiments traités  : {n:,}")
        print(f"  Bâtiments ignorés  : {g.n_buildings_skipped}")
        print()
        print("  ── ACCURACY MÉTRIQUES ──────────────────────────────────")
        print(f"  1. Silhouette Score       : {g.mean_silhouette:.4f}")
        print(f"  2. Adjusted Rand Index    : {g.mean_ari:.4f}")
        print(f"  3. Capacity Compliance    : {g.mean_capacity_compliance:.1f}%")
        print(f"  4. FAT Count Match Rate   : {g.fat_count_match_rate:.1f}%")
        print(f"  5. FDT Assignment Match   : {g.mean_fdt_match:.1f}%")
        print("=" * 62)

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for r in self.results_:
            for fat in r.fat_candidates:
                rows.append({
                    "id_batiment": r.id_batiment,
                    "id_zone": r.id_zone,
                    "fat_id": fat.fat_id,
                    "usage": fat.usage,
                    "centroid_lat": fat.centroid_lat,
                    "centroid_lon": fat.centroid_lon,
                    "assigned_floor": fat.assigned_floor,
                    "n_subscribers": fat.n_subscribers,
                    "fdt_assigned": fat.fdt_assigned,
                    "capacity_ok": fat.capacity_ok,
                    "ari_score": r.ari_score,
                    "silhouette": r.silhouette,
                    "capacity_pct": r.capacity_compliance_pct,
                    "fat_count_match": r.fat_count_match,
                })
        return pd.DataFrame(rows)

    def _snap_cable(self, dist_m: float) -> int:
        lengths = [15, 20, 50, 80]
        for length in lengths:
            if dist_m <= length:
                return length
        return 80

    def get_optimal_fat_placements(self, df: pd.DataFrame) -> pd.DataFrame:
        output_rows = []
        for result in self.results_:
            for fat in result.fat_candidates:
                fdt_lat = df[df["id_batiment"] == result.id_batiment]["lat_fdt"].iloc[0]
                fdt_lon = df[df["id_batiment"] == result.id_batiment]["lon_fdt"].iloc[0]

                dist_m = haversine_m(fat.centroid_lat, fat.centroid_lon, fdt_lat, fdt_lon)
                cable_snap = self._snap_cable(dist_m)

                output_rows.append({
                    "zone_id": result.id_zone,
                    "id_batiment": result.id_batiment,
                    "fat_id": fat.fat_id,
                    "type": "RES" if fat.usage == "logements" else "COM",
                    "lat": fat.centroid_lat,
                    "lon": fat.centroid_lon,
                    "etage": fat.assigned_floor,
                    "nb_abonnes": fat.n_subscribers,
                    "taux_occupation": round(fat.n_subscribers / self.FAT_CAPACITY * 100, 2),
                    "fdt_id": fat.fdt_assigned,
                    "distance_m": round(dist_m, 2),
                    "cable_snappe": cable_snap,
                    "linéaire_fibre_m": cable_snap,
                })
        return pd.DataFrame(output_rows)

    def export_to_excel_by_building(self, df: pd.DataFrame, output_path: str):
        # (votre code original inchangé)
        pass   # ← gardez votre implémentation ici si vous l'utilisez

# =============================================================================
# POINT D'ENTRÉE STANDALONE
# =============================================================================

if __name__ == "__main__":
    import sys
    import os

    DATASET_PATH = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\dataset_fusionnee_final.csv"

    print(f"📂 Chargement {DATASET_PATH}...")
    df = pd.read_csv(DATASET_PATH, encoding="utf-8-sig")
    print(f"   → {len(df):,} lignes, {df['id_batiment'].nunique():,} bâtiments uniques")

    required = ["lat_abonne", "lon_abonne", "etage", "id_batiment",
                "FAT_relative", "usage", "nom_FDT", "lat_fdt", "lon_fdt", "code_client"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Colonnes manquantes : {missing}")
        sys.exit(1)

    # ====================== TRAINING + SAVE FULL PIPELINE ======================
    model = FATKMeansModel(random_state=2026, n_init=15)

    print("🚀 Entraînement du modèle (une seule fois)...")
    model.fit(df, max_buildings=None)

    SAVE_DIR = "model"
    os.makedirs(SAVE_DIR, exist_ok=True)
    SAVE_PATH = os.path.join(SAVE_DIR, "fat_kmeans_model.joblib")

    joblib.dump(model, SAVE_PATH)
    print(f"✅ FULL PIPELINE SAUVEGARDÉ (preprocessing + constantes + modèle) → {SAVE_PATH}")
    print(f"   Taille : {os.path.getsize(SAVE_PATH) / (1024*1024):.1f} MB")

    # Optionnel : export CSV
    df_optimal = model.get_optimal_fat_placements(df)
    output_path = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\resultat\fat_candidates.csv"
    model.to_dataframe().to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Résultats exportés : {output_path}")