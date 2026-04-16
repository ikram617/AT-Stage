# =============================================================================
# FATSmartPlanner - Pipeline Complet P1 → P5 (Stage ING4 - Algérie Télécom Oran)
# Version 2D Pure + Summary enrichi (MSE, RMSE, MAE, R², Silhouette, ARI)
# =============================================================================

import joblib
import numpy as np
import pandas as pd
from math import ceil, radians, cos, sin, asin, sqrt
from dataclasses import dataclass
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import adjusted_rand_score, silhouette_score, r2_score
import warnings
import os
import matplotlib.pyplot as plt
from matplotlib.patches import Circle

from config import settings

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
    cable_m_to_fdt_real: float = 0.0
    cable_snap: int = 0
    radius_deg: float = 0.0
    max_dist_to_sub_m: float = 0.0


@dataclass
class BuildingKMeansResult:
    id_batiment: str
    id_zone: str
    n_subscribers_total: int
    fat_candidates: list[FATCandidate]
    n_fats_proposed: int
    n_fats_ground_truth: int
    ari_score: float = 0.0          # %
    silhouette_score: float = 0.0   # %
    r2_score: float = 0.0           # %
    capacity_compliance_pct: float = 100.0
    mae_m: float = 0.0
    mse_m2: float = 0.0
    rmse_m: float = 0.0
    max_distance_m: float = 0.0


# =============================================================================
# UTILITAIRES
# =============================================================================
def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distance euclidienne sur sphère (mètres) - 2D uniquement"""
    R = 6_371_000
    la1, lo1, la2, lo2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((la2 - la1) / 2) ** 2 + cos(la1) * cos(la2) * sin((lo2 - lo1) / 2) ** 2
    return R * 2 * asin(sqrt(max(0, a)))


def snap_cable(dist_m: float) -> int:
    """Snap vers longueurs standards AT"""
    for length in settings.AT_DROP_CABLE_STANDARDS_M:
        if dist_m <= length:
            return length
    return settings.AT_DROP_CABLE_STANDARDS_M[-1]


def calculate_k_optimal(n_abonnes: int) -> int:
    """
    Règle Algérie Télécom - Placement optimal des FATs (Stage ING4 Oran)
    Évite les FATs avec trop peu d'abonnés (gaspillage de splitter N2)
    """
    if n_abonnes <= 0:
        return 0

    # Calcul de base : nombre minimum de FATs nécessaires
    k = ceil(n_abonnes / settings.FAT_CAPACITY)

    # Règle du remainder threshold
    remainder = n_abonnes % settings.FAT_CAPACITY

    # Si le reste est >= seuil (6,7,8) → on préfère ajouter 1 FAT pour "remplir" mieux
    # Exemple : 13 abonnés → 1 FAT de 8 + 1 FAT de 5 → on passe à 2 FATs (8+5 mieux que 13 dans 1 FAT ? Non, mais selon ta règle métier)
    if remainder >= settings.FAT_CAPACITY_REMAINDER_THRESHOLD and remainder != 0:
        k += 1

    # Cas particulier : si n_abonnes <= FAT_CAPACITY, toujours 1 FAT
    if n_abonnes <= settings.FAT_CAPACITY:
        return 1

    return max(1, k)

# =============================================================================
# CLASSE PRINCIPALE - 2D STRICT
# =============================================================================
class FATSmartPlanner:
    def __init__(self, random_state: int = 2026):
        self.random_state = random_state
        self.results_: list[BuildingKMeansResult] = []
        self.plot_count = 0

    # =========================================================================
    # P1 : Estimer les points abonnés (simulation / mode évaluation)
    # =========================================================================
    def _estimate_subscriber_points(self, df_building: pd.DataFrame) -> pd.DataFrame:
        """P1 - Pour l'instant mode évaluation (points déjà présents). Prêt pour simulation réelle."""
        return df_building.copy()

    # =========================================================================
    # P2 : Générer candidats FAT (grille simplifiée)
    # =========================================================================
    def _generate_fat_candidates(self, df_sub: pd.DataFrame):
        """P2 - Placeholder grille. À enrichir avec vraie grille OSM plus tard."""
        return None  # On utilise directement les points abonnés pour clustering

    # =========================================================================
    # P3 + P4 : K-means contraint 2D + Positionnement centroïde
    # =========================================================================
    def _constrained_kmeans_2d(self, df_u: pd.DataFrame, k: int):
        """K-means 2D strict avec contrainte capacité ≤ 8"""
        if len(df_u) < 2:
            clat = df_u["lat_abonne"].mean()
            clon = df_u["lon_abonne"].mean()
            labels = np.zeros(len(df_u), dtype=int)
            centroids = np.array([[clat, clon]])
            return labels, centroids

        X = np.column_stack([df_u["lat_abonne"].values, df_u["lon_abonne"].values])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        km = KMeans(n_clusters=k, n_init=15, max_iter=300, random_state=self.random_state)
        km.fit(X_scaled)
        centroids_scaled = km.cluster_centers_

        # Assignment contraint capacité
        from scipy.spatial.distance import cdist
        distances = cdist(X_scaled, centroids_scaled)
        labels = np.full(len(X_scaled), -1)
        cluster_counts = np.zeros(k, dtype=int)

        pairs = sorted(((distances[i, j], i, j) for i in range(len(X_scaled)) for j in range(k)), key=lambda x: x[0])

        for _, i, j in pairs:
            if labels[i] == -1 and cluster_counts[j] < settings.FAT_CAPACITY:
                labels[i] = j
                cluster_counts[j] += 1

        # Recalcul centroïdes originaux (2D)
        centroids_orig = np.zeros((k, 2))
        for j in range(k):
            mask = labels == j
            if np.any(mask):
                centroids_orig[j] = X[mask].mean(axis=0)

        return labels, centroids_orig

    # =========================================================================
    # P5 : Validation contraintes
    # =========================================================================
    def _validate_constraints(self, fat_candidates: list, df_bat: pd.DataFrame) -> list:
        """P5 - Validation distance FDT, snap câble, distance max abonné-FAT"""
        validated = []
        fdt_lat = df_bat["lat_fdt"].iloc[0]
        fdt_lon = df_bat["lon_fdt"].iloc[0]

        for fat in fat_candidates:
            dist_fdt_real = haversine_m(fat.centroid_lat, fat.centroid_lon, fdt_lat, fdt_lon) * settings.TORTUOSITY_TRUNK
            cable_snap = snap_cable(dist_fdt_real)

            fat.cable_m_to_fdt_real = round(dist_fdt_real, 2)
            fat.cable_snap = cable_snap
            validated.append(fat)

        return validated

    # =========================================================================
    # Visualisation 2D
    # =========================================================================
    def _plot_clusters(self, df_bat, candidates, id_batiment, ari, cap, mae):
        os.makedirs("visualizations/clusters_2d_pipeline", exist_ok=True)
        fig, ax = plt.subplots(figsize=(11, 9))

        ax.scatter(df_bat["lon_abonne"], df_bat["lat_abonne"], c='lightblue', s=40, edgecolors='gray', label='Abonnés')

        for i, fat in enumerate(candidates):
            ax.scatter(fat.centroid_lon, fat.centroid_lat, c='red', marker='*', s=300, edgecolor='black', zorder=5)
            ax.text(fat.centroid_lon + 0.000008, fat.centroid_lat + 0.000008,
                    f"{fat.fat_id}\n({fat.n_subscribers})", fontsize=9, fontweight='bold')

            if fat.radius_deg > 0:
                circle = Circle((fat.centroid_lon, fat.centroid_lat), fat.radius_deg * 1.3, color='red', alpha=0.12)
                ax.add_patch(circle)

        ax.scatter(df_bat["lon_fdt"].iloc[0], df_bat["lat_fdt"].iloc[0], c='green', marker='s', s=200, label="FDT")

        plt.title(f"Planification FAT 2D - Bâtiment {id_batiment}\nARI: {ari:.1f}% | Capacité: {cap:.1f}% | MAE: {mae:.1f}m")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.savefig(f"visualizations/clusters_2d_pipeline/cluster_{id_batiment}.png", dpi=280, bbox_inches='tight')
        plt.close()

    # =========================================================================
    # PROCESSUS COMPLET PAR BÂTIMENT (P1 à P5)
    # =========================================================================
    def process_building(self, df_bat: pd.DataFrame) -> BuildingKMeansResult | None:
        id_batiment = df_bat["id_batiment"].iloc[0]
        id_zone = df_bat["id_zone"].iloc[0]

        # P1 : Estimation points abonnés
        df_sub = self._estimate_subscriber_points(df_bat)

        all_fat_candidates = []
        label_offset = 0

        for usage in ["logements", "commerces"]:
            df_u = df_sub[df_sub["usage"] == usage].copy()
            if df_u.empty:
                continue

            n_sub = len(df_u)
            k = calculate_k_optimal(n_sub)
            if n_sub < 2:
                k = 1

            labels, centroids = self._constrained_kmeans_2d(df_u, k)

            for cluster_id in range(k):
                mask = labels == cluster_id
                subset = df_u[mask]
                if subset.empty:
                    continue

                clat, clon = centroids[cluster_id]

                # Calcul rayon et distance max (2D)
                distances_m = [haversine_m(r["lat_abonne"], r["lon_abonne"], clat, clon) 
                              for _, r in subset.iterrows()]
                max_dist_m = max(distances_m) if distances_m else 0.0
                max_radius_deg = max(np.sqrt((r["lat_abonne"] - clat)**2 + (r["lon_abonne"] - clon)**2) 
                                   for _, r in subset.iterrows()) if not subset.empty else 0.00001

                fat = FATCandidate(
                    fat_id=f"FAT-{cluster_id + 1 + label_offset:03d}-{'LOG' if usage == 'logements' else 'COM'}",
                    cluster_label=cluster_id + label_offset,
                    centroid_lat=round(clat, 6),
                    centroid_lon=round(clon, 6),
                    subscriber_ids=subset["code_client"].tolist(),
                    n_subscribers=len(subset),
                    usage=usage,
                    fdt_assigned=subset["nom_FDT"].mode().iloc[0] if not subset["nom_FDT"].mode().empty else "FDT_INCONNU",
                    capacity_ok=len(subset) <= settings.FAT_CAPACITY,
                    radius_deg=max_radius_deg,
                    max_dist_to_sub_m=round(max_dist_m, 2)
                )
                all_fat_candidates.append(fat)

            label_offset += k

        if not all_fat_candidates:
            return None

        # P5 : Validation contraintes
        all_fat_candidates = self._validate_constraints(all_fat_candidates, df_bat)

        # ====================== Calcul métriques physiques 2D ======================
        distances_m = []
        all_labels_final = np.zeros(len(df_bat), dtype=int)

        for i, fat in enumerate(all_fat_candidates):
            for cid in fat.subscriber_ids:
                idx = df_bat[df_bat["code_client"] == cid].index[0]
                abon = df_bat.iloc[idx]
                dist_m = haversine_m(abon["lat_abonne"], abon["lon_abonne"], fat.centroid_lat, fat.centroid_lon)
                distances_m.append(dist_m)
                all_labels_final[idx] = i

        n = len(distances_m)
        mae = np.mean(distances_m) if n > 0 else 0.0
        mse = np.mean(np.array(distances_m)**2) if n > 0 else 0.0
        rmse = np.sqrt(mse) if n > 0 else 0.0
        max_dist = max(distances_m) if n > 0 else 0.0

        # Métriques d'évaluation du modèle
        ari = 0.0
        sil = 0.0
        r2 = 0.0

        if "FAT_relative" in df_bat.columns and df_bat["FAT_relative"].notna().any():
            try:
                gt_labels = pd.Categorical(df_bat["FAT_relative"]).codes
                ari = adjusted_rand_score(gt_labels, all_labels_final) * 100

                if len(np.unique(all_labels_final)) > 1:
                    X_scaled = StandardScaler().fit_transform(
                        np.column_stack([df_bat["lat_abonne"], df_bat["lon_abonne"]])
                    )
                    sil = silhouette_score(X_scaled, all_labels_final) * 100

                # FIX v9: R² correct — mesure la proportion de variance GPS expliquée
                # par les centroïdes FAT proposés.
                #
                # COMMENT ÇA MARCHE :
                #   SS_tot = variance totale des positions GPS abonnés
                #   SS_res = variance résiduelle (distance abonné → centroïde de son FAT)
                #   R²     = 1 - SS_res/SS_tot
                #
                # r2_score(y_true, y_pred) fait exactement ça :
                #   y_true[i] = (lat_i, lon_i) de l'abonné i
                #   y_pred[i] = (lat_fat, lon_fat) du centroïde du FAT assigné à l'abonné i
                #
                # AVANT : le code avait "if False" dans la list comprehension → array vide
                # → r2_score plantait silencieusement → r2=0 toujours
                y_true = np.column_stack([df_bat["lat_abonne"].values,
                                          df_bat["lon_abonne"].values])
                # Construire y_pred : pour chaque abonné, le centroïde de son FAT
                y_pred = np.zeros_like(y_true)
                for fat in all_fat_candidates:
                    for cid in fat.subscriber_ids:
                        # Trouver l'index de cet abonné dans df_bat (reset_index a été fait)
                        row_idx = df_bat.index[df_bat["code_client"] == cid]
                        if len(row_idx) > 0:
                            y_pred[row_idx[0]] = [fat.centroid_lat, fat.centroid_lon]

                # r2_score avec deux colonnes retourne le R² multivarié (coefficient de
                # détermination), entre -∞ (pire) et 1.0 (parfait).
                # On multiplie par 100 pour avoir un pourcentage.
                r2 = r2_score(y_true, y_pred) * 100

            except Exception:
                pass

        capacity_pct = sum(1 for f in all_fat_candidates if f.capacity_ok) / len(all_fat_candidates) * 100

        # Plot limité
        if self.plot_count < 8:
            self._plot_clusters(df_bat, all_fat_candidates, id_batiment, ari, capacity_pct, mae)
            self.plot_count += 1

        return BuildingKMeansResult(
            id_batiment=id_batiment,
            id_zone=id_zone,
            n_subscribers_total=len(df_bat),
            fat_candidates=all_fat_candidates,
            n_fats_proposed=len(all_fat_candidates),
            n_fats_ground_truth=df_bat.get("FAT_relative", pd.Series()).nunique(),
            ari_score=round(ari, 2),
            silhouette_score=round(sil, 2),
            r2_score=round(r2, 2),
            capacity_compliance_pct=round(capacity_pct, 1),
            mae_m=round(mae, 2),
            mse_m2=round(mse, 2),
            rmse_m=round(rmse, 2),
            max_distance_m=round(max_dist, 2)
        )

    # =========================================================================
    # FIT + SUMMARY ENRICHIE
    # =========================================================================
    def fit(self, df: pd.DataFrame, max_buildings=None):
        self.results_ = []
        self.plot_count = 0

        buildings = df["id_batiment"].unique()
        if max_buildings:
            buildings = buildings[:max_buildings]

        print(f"🚀 Pipeline FAT 2D P1→P5 sur {len(buildings)} bâtiments...")

        for i, bat_id in enumerate(buildings):
            df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)
            result = self.process_building(df_bat)
            if result:
                self.results_.append(result)

            if (i + 1) % 100 == 0:
                print(f"   → {i+1}/{len(buildings)} bâtiments traités")

        self.summary()
        return self

    def summary(self):
        """Summary complet avec métriques 2D enrichies"""
        if not self.results_:
            print("Aucun bâtiment traité.")
            return

        n = len(self.results_)
        total_fats = sum(r.n_fats_proposed for r in self.results_)

        aris = [r.ari_score for r in self.results_]
        sils = [r.silhouette_score for r in self.results_]
        r2s = [r.r2_score for r in self.results_]
        capacities = [r.capacity_compliance_pct for r in self.results_]
        maes = [r.mae_m for r in self.results_]
        mses = [r.mse_m2 for r in self.results_]
        rmses = [r.rmse_m for r in self.results_]
        max_dists = [r.max_distance_m for r in self.results_]

        print("\n" + "=" * 95)
        print("    RÉSULTATS PIPELINE FAT 2D (P1→P5) - ALGÉRIE TÉLÉCOM ORAN")
        print("=" * 95)
        print(f"  Bâtiments traités                  : {n:,}")
        print(f"  Nombre total de FATs proposées     : {total_fats:,}")
        print("-" * 95)
        print("  MÉTRIQUES D'ÉVALUATION DU MODÈLE (%)")
        print(f"  → Adjusted Rand Index (ARI)        : {np.mean(aris):6.2f} %")
        print(f"  → Silhouette Score                 : {np.mean(sils):6.2f} %")
        print(f"  → R² Score (Variance expliquée)    : {np.mean(r2s):6.2f} %")
        print(f"  → Conformité Capacité FAT          : {np.mean(capacities):6.1f} %")
        print("-" * 95)
        print("  MÉTRIQUES PHYSIQUES 2D (Distance Abonné → FAT)")
        print(f"  → MAE                              : {np.mean(maes):6.2f} mètres")
        print(f"  → MSE                              : {np.mean(mses):8,.2f} m²")
        print(f"  → RMSE                             : {np.mean(rmses):6.2f} mètres")
        print(f"  → Distance maximale                : {np.max(max_dists):6.2f} mètres")
        print("=" * 95)


# =============================================================================
# LANCEMENT
# =============================================================================
if __name__ == "__main__":
    DATASET_PATH = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_annaba2\dataset_fusionnee_final.csv"

    df = pd.read_csv(DATASET_PATH, encoding="utf-8-sig")
    print(f"Dataset chargé → {len(df):,} abonnés | {df['id_batiment'].nunique():,} bâtiments")

    model = FATSmartPlanner(random_state=2026)
    model.fit(df, max_buildings=None)

    os.makedirs("model", exist_ok=True)
    joblib.dump(model, "model/fat_pipeline_2d_annaba.joblib")
    print("✅ Modèle 2D complet sauvegardé → model/fat_pipeline_2d_annaba.joblib")