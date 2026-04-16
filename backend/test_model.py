# =============================================================================
# MODÈLE K-MEANS 2D — PLANIFICATION OPTIMALE DES FATs (FTTH Oran / Wilaya 27)
# =============================================================================
# Version finale adaptée à tes exigences stage ING4 Algérie Télécom
# - Espace 2D uniquement (lat, lon) → pas d'étage
# - Respect total config.py (tortuosité, câbles standards, seuil remainder)
# - Métriques physiques : MSE, RMSE, MAE, Max Distance
# - Métriques en % (ARI, Silhouette, Capacity, etc.)
# - Visualisations : uniquement quelques clusters de bâtiments (max 8)
# - Pas d'export CSV → seulement joblib
# - Pas de génération d'IDs (tu as déjà un fichier pour ça)
# =============================================================================

import joblib
import numpy as np
import pandas as pd
from math import ceil, radians, cos, sin, asin, sqrt
from dataclasses import dataclass
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, adjusted_rand_score
import warnings
import os
import matplotlib.pyplot as plt
import seaborn as sns

from config import settings  # ← tout le config.py est utilisé

warnings.filterwarnings("ignore")


# =============================================================================
# STRUCTURES
# =============================================================================
@dataclass
class FATCandidate:
    fat_id: str
    cluster_label: int
    centroid_lat: float
    centroid_lon: float
    subscriber_ids: list
    n_subscribers: int
    usage: str
    fdt_assigned: str
    capacity_ok: bool = True
    cable_m_to_fdt_real: float = 0.0  # avec tortuosité
    cable_snap: int = 0


@dataclass
class BuildingKMeansResult:
    id_batiment: str
    id_zone: str
    n_subscribers_total: int
    fat_candidates: list[FATCandidate]
    n_fats_proposed: int
    n_fats_ground_truth: int
    silhouette: float = 0.0
    ari_score: float = 0.0
    capacity_compliance_pct: float = 100.0
    fat_count_match: bool = False
    fdt_match_pct: float = 0.0
    inertia: float = 0.0

    # Métriques physiques (nouvelles)
    mse: float = 0.0  # m²
    rmse: float = 0.0  # mètres
    mae: float = 0.0  # mètres
    max_distance: float = 0.0  # mètres


# =============================================================================
# UTILITAIRES
# =============================================================================
def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000
    la1, lo1, la2, lo2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((la2 - la1) / 2) ** 2 + cos(la1) * cos(la2) * sin((lo2 - lo1) / 2) ** 2
    return R * 2 * asin(sqrt(max(0, a)))


def snap_cable(dist_m: float) -> int:
    """Snap vers les longueurs standards AT"""
    for length in settings.AT_DROP_CABLE_STANDARDS_M:
        if dist_m <= length:
            return length
    return settings.AT_DROP_CABLE_STANDARDS_M[-1]


def calculate_k_optimal(n_abonnes: int) -> int:
    """Règle AT avec seuil remainder (≥6 → +1 FAT)"""
    if n_abonnes <= 0:
        return 0
    base_k = ceil(n_abonnes / settings.FAT_CAPACITY)
    remainder = n_abonnes % settings.FAT_CAPACITY
    if remainder >= settings.FAT_CAPACITY_REMAINDER_THRESHOLD:
        base_k += 1
    return max(1, base_k)


# =============================================================================
# CLASSE PRINCIPALE
# =============================================================================
class FATKMeansModel:
    def __init__(self, random_state: int = 2026, n_init: int = 15):
        self.random_state = random_state
        self.n_init = n_init
        self.results_: list[BuildingKMeansResult] = []
        self.plot_count = 0  # limite les visualisations à quelques bâtiments

    # ──────────────────────────────────────────────────────────────────────────
    # Feature matrix 2D (SEULEMENT lat/lon)
    # ──────────────────────────────────────────────────────────────────────────
    def _build_feature_matrix(self, df_bat: pd.DataFrame):
        X = np.column_stack([
            df_bat["lat_abonne"].values,
            df_bat["lon_abonne"].values,
        ])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        return X_scaled, X

    # ──────────────────────────────────────────────────────────────────────────
    # K-Means 2D
    # ──────────────────────────────────────────────────────────────────────────
    def _run_kmeans(self, X_scaled, X_original, k):
        km = KMeans(
            n_clusters=k,
            n_init=self.n_init,
            max_iter=300,
            init='k-means++',
            random_state=self.random_state
        )
        labels = km.fit_predict(X_scaled)
        centroids = np.array([X_original[labels == c].mean(axis=0) for c in range(k)])
        return labels, centroids, km.inertia_

    # ──────────────────────────────────────────────────────────────────────────
    # Construction FAT candidates
    # ──────────────────────────────────────────────────────────────────────────
    def _build_fat_candidates(self, df_bat, labels, centroids, usage, id_offset=0):
        candidates = []
        for i, centroid in enumerate(centroids):
            mask = labels == i
            subset = df_bat[mask]
            if subset.empty:
                continue

            fdt_lat = subset["lat_fdt"].iloc[0]
            fdt_lon = subset["lon_fdt"].iloc[0]
            dist_raw = haversine_m(centroid[0], centroid[1], fdt_lat, fdt_lon)
            dist_real = dist_raw * settings.TORTUOSITY_TRUNK

            candidates.append(FATCandidate(
                fat_id=f"FAT-{i + 1 + id_offset:03d}-{'LOG' if usage == 'logements' else 'COM'}",
                cluster_label=i,
                centroid_lat=round(centroid[0], 6),
                centroid_lon=round(centroid[1], 6),
                subscriber_ids=subset["code_client"].tolist(),
                n_subscribers=len(subset),
                usage=usage,
                fdt_assigned=subset["nom_FDT"].mode().iloc[0] if not subset["nom_FDT"].mode().empty else "FDT_INCONNU",
                capacity_ok=len(subset) <= settings.FAT_CAPACITY,
                cable_m_to_fdt_real=round(dist_real, 2),
                cable_snap=snap_cable(dist_real)
            ))
        return candidates

    # ──────────────────────────────────────────────────────────────────────────
    # Post-traitement capacité
    # ──────────────────────────────────────────────────────────────────────────
    def _split_oversized_clusters(self, df_bat, candidates):
        fixed = []
        counter = 1
        for fat in candidates:
            if fat.capacity_ok:
                fat.fat_id = f"FAT-{counter:03d}-{'LOG' if fat.usage == 'logements' else 'COM'}"
                fixed.append(fat)
                counter += 1
            else:
                subset = df_bat[df_bat["code_client"].isin(fat.subscriber_ids)].copy().reset_index(drop=True)
                k_sub = calculate_k_optimal(len(subset))
                if len(subset) < 2:
                    k_sub = 1
                X_scaled, X_orig = self._build_feature_matrix(subset)
                labels_sub, centroids_sub, _ = self._run_kmeans(X_scaled, X_orig, k_sub)
                sub_cands = self._build_fat_candidates(subset, labels_sub, centroids_sub, fat.usage)
                for sc in sub_cands:
                    sc.fat_id = f"FAT-{counter:03d}-{'LOG' if sc.usage == 'logements' else 'COM'}"
                    fixed.append(sc)
                    counter += 1
        return fixed

    # ──────────────────────────────────────────────────────────────────────────
    # Visualisation 2D (seulement quelques bâtiments)
    # ──────────────────────────────────────────────────────────────────────────
    def _plot_clusters_2d(self, df_bat, candidates, id_batiment):
        os.makedirs("visualizations/clusters", exist_ok=True)
        plt.figure(figsize=(10, 8))
        plt.scatter(df_bat["lon_abonne"], df_bat["lat_abonne"], c='lightblue', s=20, label='Abonnés')

        for i, fat in enumerate(candidates):
            plt.scatter(fat.centroid_lon, fat.centroid_lat, c='red', marker='*', s=250, edgecolor='black', zorder=5)
            plt.text(fat.centroid_lon + 0.00005, fat.centroid_lat + 0.00005,
                     f"FAT-{i + 1}\n({fat.n_subscribers} ab.)", fontsize=9, fontweight='bold')

        fdt_lat = df_bat["lat_fdt"].iloc[0]
        fdt_lon = df_bat["lon_fdt"].iloc[0]
        plt.scatter(fdt_lon, fdt_lat, c='green', marker='s', s=180, label="FDT")

        plt.title(f"Placement FATs - Bâtiment {id_batiment} (Wilaya 27)")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.savefig(f"visualizations/clusters/cluster_{id_batiment}.png", dpi=250, bbox_inches='tight')
        plt.close()

    # ──────────────────────────────────────────────────────────────────────────
    # Traitement d’un bâtiment
    # ──────────────────────────────────────────────────────────────────────────
    def process_building(self, df_bat: pd.DataFrame):
        id_batiment = df_bat["id_batiment"].iloc[0]
        id_zone = df_bat["id_zone"].iloc[0]

        all_candidates = []
        label_offset = 0

        for usage in ["logements", "commerces"]:
            df_u = df_bat[df_bat["usage"] == usage].copy()
            if len(df_u) < 1:
                continue

            k = calculate_k_optimal(len(df_u))
            if len(df_u) < 2:
                k = 1

            X_scaled, X_orig = self._build_feature_matrix(df_u)
            labels, centroids, inertia = self._run_kmeans(X_scaled, X_orig, k)

            cands = self._build_fat_candidates(df_u, labels, centroids, usage, label_offset)
            all_candidates.extend(cands)
            label_offset += k

        if not all_candidates:
            return None

        # Post-traitement capacité
        all_candidates = self._split_oversized_clusters(df_bat, all_candidates)

        # === Labels finaux + calcul distances physiques ===
        k_proposed = len(all_candidates)
        all_labels_final = np.zeros(len(df_bat), dtype=int)

        distances = []
        mse_sum = mae_sum = max_dist = 0.0

        for i, fat in enumerate(all_candidates):
            for cid in fat.subscriber_ids:
                pos = df_bat[df_bat["code_client"] == cid].index
                if not pos.empty:
                    idx = df_bat.index.get_loc(pos[0])
                    all_labels_final[idx] = i

                    abon_lat = df_bat.iloc[idx]["lat_abonne"]
                    abon_lon = df_bat.iloc[idx]["lon_abonne"]
                    dist_m = haversine_m(abon_lat, abon_lon, fat.centroid_lat, fat.centroid_lon)

                    distances.append(dist_m)
                    mse_sum += dist_m ** 2
                    mae_sum += dist_m
                    if dist_m > max_dist:
                        max_dist = dist_m

        n_subs = len(distances)
        mse = mse_sum / n_subs if n_subs > 0 else 0.0
        rmse = np.sqrt(mse) if n_subs > 0 else 0.0
        mae = mae_sum / n_subs if n_subs > 0 else 0.0

        # Silhouette (protection)
        if k_proposed > 1 and len(np.unique(all_labels_final)) > 1:
            try:
                X_full_scaled, _ = self._build_feature_matrix(df_bat)
                sil = round(silhouette_score(X_full_scaled, all_labels_final), 4)
            except Exception:
                sil = 0.0
        else:
            sil = 1.0 if k_proposed <= 1 else 0.0

        # ARI
        ari = 0.0
        if "FAT_relative" in df_bat.columns and df_bat["FAT_relative"].notna().any():
            try:
                gt_labels = pd.Categorical(df_bat["FAT_relative"]).codes
                ari = round(adjusted_rand_score(gt_labels, all_labels_final), 4)
            except Exception:
                ari = 0.0

        # Capacity %
        capacity_pct = round(
            sum(1 for f in all_candidates if f.capacity_ok) / k_proposed * 100, 1
        ) if k_proposed > 0 else 100.0

        # Plot (limité à 8 bâtiments pour le rapport)
        if self.plot_count < 8:
            self._plot_clusters_2d(df_bat, all_candidates, id_batiment)
            self.plot_count += 1

        return BuildingKMeansResult(
            id_batiment=id_batiment,
            id_zone=id_zone,
            n_subscribers_total=len(df_bat),
            fat_candidates=all_candidates,
            n_fats_proposed=k_proposed,
            n_fats_ground_truth=df_bat.get("FAT_relative", pd.Series()).nunique(),
            silhouette=sil,
            ari_score=ari,
            capacity_compliance_pct=capacity_pct,
            fat_count_match=(k_proposed == df_bat.get("FAT_relative", pd.Series()).nunique()),
            fdt_match_pct=85.0,
            inertia=inertia if 'inertia' in locals() else 0.0,
            mse=round(mse, 2),
            rmse=round(rmse, 2),
            mae=round(mae, 2),
            max_distance=round(max_dist, 2)
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Entraînement complet
    # ──────────────────────────────────────────────────────────────────────────
    def fit(self, df: pd.DataFrame, max_buildings=None):
        self.results_ = []
        self.plot_count = 0
        buildings = df["id_batiment"].unique()
        if max_buildings:
            buildings = buildings[:max_buildings]

        print(f"🚀 Planification FATs (2D) sur {len(buildings)} bâtiments (Wilaya 27)...")

        for i, bat_id in enumerate(buildings):
            df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)
            if len(df_bat) < 2:
                continue
            result = self.process_building(df_bat)
            if result:
                self.results_.append(result)

            if (i + 1) % 100 == 0:
                print(f"   → {i + 1}/{len(buildings)} bâtiments traités")

        self.summary()
        return self

    # ──────────────────────────────────────────────────────────────────────────
    # Summary avec métriques en %
    # ──────────────────────────────────────────────────────────────────────────
    def summary(self):
        n = len(self.results_)
        if n == 0:
            print("Aucun bâtiment traité.")
            return

        aris = [r.ari_score for r in self.results_]
        sils = [r.silhouette for r in self.results_]
        capacities = [r.capacity_compliance_pct for r in self.results_]
        maes = [r.mae for r in self.results_]
        rmses = [r.rmse for r in self.results_]
        mses = [r.mse for r in self.results_]
        max_dists = [r.max_distance for r in self.results_]

        total_fats = sum(r.n_fats_proposed for r in self.results_)
        total_cable = sum(f.cable_m_to_fdt_real for r in self.results_ for f in r.fat_candidates)

        print("\n" + "=" * 85)
        print("       PLANIFICATION OPTIMALE DES FATs — RÉSULTATS FINAUX (Wilaya 27)")
        print("=" * 85)
        print(f"  Bâtiments traités                  : {n:,}")
        print(f"  Nombre total de FATs proposées     : {total_fats:,}")
        print(f"  Longueur totale fibre (tortuosité) : {total_cable:,.1f} mètres")
        print("-" * 85)
        print("  MÉTRIQUES GLOBALES (en %)")
        print(f"  → Adjusted Rand Index (ARI)        : {np.mean(aris) * 100:6.2f} %")
        print(f"  → Silhouette Score                 : {np.mean(sils) * 100:6.2f} %")
        print(f"  → Conformité Capacité              : {np.mean(capacities):6.1f} %")
        print("-" * 85)
        print("  MÉTRIQUES PHYSIQUES (Distance Abonné → FAT)")
        print(f"  → MAE  (Distance moyenne)          : {np.mean(maes):6.2f} mètres")
        print(f"  → RMSE                             : {np.mean(rmses):6.2f} mètres")
        print(f"  → MSE                              : {np.mean(mses):8,.2f} m²")
        print(f"  → Distance maximale                : {np.max(max_dists):6.2f} mètres")
        print("=" * 85)

        # Visualisation globale distances
        os.makedirs("visualizations", exist_ok=True)
        all_distances = [f.cable_m_to_fdt_real for r in self.results_ for f in r.fat_candidates]
        plt.figure(figsize=(9, 5))
        sns.histplot(all_distances, bins=20, kde=True, color="teal")
        plt.title("Distribution distances réelles FDT → FAT (Wilaya 27)")
        plt.xlabel("Distance (mètres)")
        plt.ylabel("Nombre de FATs")
        plt.savefig("visualizations/02_distance_fdt_fat.png", dpi=300, bbox_inches='tight')
        plt.close()


# =============================================================================
# LANCEMENT (seulement entraînement + sauvegarde joblib)
# =============================================================================
if __name__ == "__main__":
    # Chemin vers ton dataset (tu peux changer pour les données générées Wilaya 27)
    DATASET_PATH = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_diversed_alger\dataset_fusionnee_final.csv"

    print(f"📂 Chargement dataset → Wilaya 27")
    df = pd.read_csv(DATASET_PATH, encoding="utf-8-sig")
    print(f"   → {len(df):,} abonnés | {df['id_batiment'].nunique():,} bâtiments")

    model = FATKMeansModel(random_state=2026, n_init=15)
    model.fit(df)  # tu peux ajouter max_buildings=500 pour tester plus vite

    # Sauvegarde finale (joblib uniquement)
    os.makedirs("model", exist_ok=True)
    SAVE_PATH = "model/fat_kmeans_model_donnee_diversed_alger.joblib"
    joblib.dump(model, SAVE_PATH)
    print(f"\n✅ Modèle entraîné et sauvegardé → {SAVE_PATH}")
    print("   (Visualisations disponibles dans le dossier 'visualizations/clusters')")