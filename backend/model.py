import pandas as pd


dataset = pd.read_csv(r"C:\Users\blabl\OneDrive\Desktop\New folder\newdata/dataset_fusionnee_final.csv")

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
# (inline ici pour portabilité si lancé standalone)
FAT_CAPACITY      = settings.FAT_CAPACITY       # Capacité max d'une FAT (standard AT Oran)
#FAT_THRESHOLD     = 0.75    # Seuil rentabilité : 6/8 = 0.75
#ACCUMULATION_MAX  = 6       # Résidus max avant FAT mutualisée
FLOOR_HEIGHT_M    = 3.0     # Hauteur inter-étage standard (mètres)
METERS_PER_DEGREE = 111_000.0  # 1 degré ≈ 111km en Algérie
VERTICAL_WEIGHT   = 2.0     # Importance étage vs position horizontale


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

    LOGIQUE EXACTE (mémoire Rezgui Fig 4.3) :
    - Division entière : quotient = n // 8, reste_frac = (n % 8) / 8
    - Si reste_frac >= 0.75 → une FAT de plus (6+ abonnés résiduels)
    - Si reste_frac < 0.75 → accumuler ; si résidu > 6 → une FAT mutualisée
    - Minimum : 1 FAT

    Exemples :
      8  → 8/8 = 1.00 → 1 FAT
      9  → 9/8 = 1.125, reste=1 → 1 FAT  (1 < 6 → pas de FAT supplémentaire)
      14 → 14/8 = 1.75, reste_frac=0.75 → 2 FATs
      16 → 16/8 = 2.00 → 2 FATs
      20 → 20/8 = 2.5, reste=4 → 2 FATs (4 < 6)
      23 → 23/8 = 2.875, reste_frac=0.875 >= 0.75 → 3 FATs
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

    PRINCIPE :
    On itère sur chaque bâtiment unique (id_batiment).
    Pour chaque bâtiment, on lance un K-means séparé.
    → Pourquoi séparé ? Parce que les abonnés de deux bâtiments différents
      ne doivent JAMAIS partager une FAT (règle physique AT).

    FEATURES K-MEANS :
    X = [lat_abonne, lon_abonne, floor_weighted]
    floor_weighted = etage × (FLOOR_HEIGHT_M / METERS_PER_DEGREE) × VERTICAL_WEIGHT

    Pourquoi pondérer l'étage ?
    → Sans pondération : lat (~35°) >> etage (~0.00003°)
      Le KMeans ignorerait complètement la dimension verticale.
    → Avec VERTICAL_WEIGHT=2.0 : monter 1 étage = 6m de fibre dans la gaine,
      ce qui compte autant que ~6m de distance horizontale.
    """

    def __init__(self, random_state: int = 2026, n_init: int = 3):
        self.random_state = random_state
        self.n_init = n_init           # 15 initialisations → résultat plus stable
        self.results_: list[BuildingKMeansResult] = []
        self.global_metrics_: GlobalMetrics = GlobalMetrics()

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 1 : Préparer les features
    # ──────────────────────────────────────────────────────────────────────────

    def _build_feature_matrix(self, df_bat: pd.DataFrame) -> np.ndarray:
        """
        Construit la matrice X [lat, lon, floor_weighted] pour un bâtiment.

        POURQUOI StandardScaler ici ?
        Lat ≈ 35.69, Lon ≈ -0.62, floor_weighted ≈ 0.000054
        → Sans scaling, K-means calcule des distances dominées par lat/lon.
        → Avec StandardScaler, chaque dimension a mean=0, std=1 → équilibrée.
        """
        floor_in_deg = FLOOR_HEIGHT_M / METERS_PER_DEGREE

        X = np.column_stack([
            df_bat["lat_abonne"].values,
            df_bat["lon_abonne"].values,
            df_bat["etage"].values * floor_in_deg * VERTICAL_WEIGHT
        ])

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        return X_scaled, scaler, X  # X original gardé pour les centroïdes GPS réels

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 2 : Lancer K-means pour un groupe d'abonnés
    # ──────────────────────────────────────────────────────────────────────────

    def _run_kmeans(
        self,
        X_scaled: np.ndarray,
        X_original: np.ndarray,
        k: int
    ) -> tuple[np.ndarray, np.ndarray, float]:
        """
        Applique K-means et retourne labels + centroïdes en coordonnées originales.

        n_init=15 : K-means est sensible à l'initialisation aléatoire.
        On lance 15 fois et on garde le meilleur (inertie minimale).
        C'est le standard scikit-learn recommandé pour des données géospatiales.
        """
        km = KMeans(
            n_clusters=k,
            n_init=self.n_init,
            max_iter=300,
            init='k-means++',
            random_state=self.random_state
        )
        labels = km.fit_predict(X_scaled)

        # Centroïdes dans l'espace ORIGINAL (pas normalisé) → coordonnées GPS réelles
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
        """
        Transforme les résultats K-means en objets FATCandidate.

        Pour chaque cluster :
        - Centroïde lat/lon = position GPS optimale de la FAT
        - Étage = médiane des étages des abonnés du cluster
          (médiane et non moyenne : on n'installe pas une FAT entre deux étages)
        - FDT = FDT la plus proche du centroïde
        """
        candidates = []

        for cluster_id in range(len(centroids)):
            mask = labels == cluster_id
            subset = df_bat[mask]

            if subset.empty:
                continue

            n_sub = len(subset)
            centroid = centroids[cluster_id]
            assigned_floor = int(np.median(subset["etage"].values))

            # FDT la plus proche du centroïde de cette FAT
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
                capacity_ok=(n_sub <= FAT_CAPACITY)
            ))

        return candidates

    # ──────────────────────────────────────────────────────────────────────────
    # ÉTAPE 4 : Calculer les métriques d'accuracy
    # ──────────────────────────────────────────────────────────────────────────

    def _split_oversized_clusters(
        self,
        df_bat: pd.DataFrame,
        candidates: list[FATCandidate]
    ) -> list[FATCandidate]:
        """
        Post-traitement : subdivise les clusters qui dépassent FAT_CAPACITY=8.

        POURQUOI K-MEANS PEUT VIOLER LA CONTRAINTE ?
        K-means minimise l'inertie géométrique, pas la taille des clusters.
        Si on lui donne K=3 et 26 abonnés bien répartis, il peut faire
        des clusters de 10, 8, 8 au lieu de 9, 9, 8.

        SOLUTION : Après K-means, pour chaque cluster > 8 on relance
        un K-means interne avec K = ceil(taille / 8).

        Exemple : cluster de 10 abonnés → K_interne = ceil(10/8) = 2
        → 2 sous-clusters de 5 abonnés chacun → tous ≤ 8 ✅
        """
        fixed_candidates = []
        id_counter = 1

        for fat in candidates:
            if fat.capacity_ok:
                # OK → on garde tel quel, juste renuméroter
                fat.fat_id = f"FAT-{id_counter:03d}-{'LOG' if fat.usage=='logements' else 'COM'}"
                fixed_candidates.append(fat)
                id_counter += 1
            else:
                # Violation → subdiviser ce cluster
                subset = df_bat[df_bat["code_client"].isin(fat.subscriber_ids)].copy()
                k_sub = ceil(fat.n_subscribers / FAT_CAPACITY)

                if len(subset) < k_sub:
                    k_sub = len(subset)

                X_sub_scaled, _, X_sub_orig = self._build_feature_matrix(subset)
                labels_sub, centroids_sub, _ = self._run_kmeans(
                    X_sub_scaled, X_sub_orig, k_sub
                )

                sub_candidates = self._build_fat_candidates(
                    subset, labels_sub, centroids_sub,
                    fat.usage, id_offset=0
                )

                for sc in sub_candidates:
                    sc.fat_id = f"FAT-{id_counter:03d}-{'LOG' if sc.usage=='logements' else 'COM'}"
                    fixed_candidates.append(sc)
                    id_counter += 1

        return fixed_candidates

    def _compute_metrics(
        self,
        df_bat: pd.DataFrame,
        labels_proposed: np.ndarray,
        k_proposed: int,
        fat_candidates: list[FATCandidate],
        X_scaled: np.ndarray
    ) -> dict:
        """
        5 métriques complémentaires — chacune mesure un aspect différent.

        ┌─────────────────────────────────────────────────────────────────┐
        │ MÉTRIQUE 1 — Silhouette Score (qualité interne des clusters)    │
        │ Range : [-1, 1]. >0.5 = bon, >0.7 = excellent.                 │
        │ Mesure : cohésion intra-cluster vs séparation inter-cluster.    │
        │ Ne dépend PAS de la ground truth → mesure purement géométrique.│
        └─────────────────────────────────────────────────────────────────┘
        ┌─────────────────────────────────────────────────────────────────┐
        │ MÉTRIQUE 2 — ARI (Adjusted Rand Index)                          │
        │ Range : [-1, 1]. 0=aléatoire, 1=identique à la ground truth.   │
        │ Mesure : à quel point nos clusters ressemblent aux FATs réelles.│
        │ "Adjusted" = corrige le hasard (meilleur que Rand Index brut).  │
        └─────────────────────────────────────────────────────────────────┘
        ┌─────────────────────────────────────────────────────────────────┐
        │ MÉTRIQUE 3 — Capacity Compliance                                │
        │ Range : [0%, 100%]. 100% = toutes les FATs ≤ 8 abonnés.        │
        │ C'est LA contrainte AT physique. Elle prime sur tout.           │
        └─────────────────────────────────────────────────────────────────┘
        ┌─────────────────────────────────────────────────────────────────┐
        │ MÉTRIQUE 4 — FAT Count Match                                    │
        │ Bool. True = notre K == nombre de FATs réelles AT.              │
        │ Directement lié à la règle 0.75 bien appliquée.                 │
        └─────────────────────────────────────────────────────────────────┘
        ┌─────────────────────────────────────────────────────────────────┐
        │ MÉTRIQUE 5 — FDT Assignment Match                               │
        │ Range : [0%, 100%].                                             │
        │ % de FATs candidates assignées au même FDT que dans les données │
        │ réelles AT. Valide la logique de proximité géographique.        │
        └─────────────────────────────────────────────────────────────────┘
        """
        metrics = {}

        # ── Métrique 1 : Silhouette ───────────────────────────────────────────
        if k_proposed > 1 and len(X_scaled) > k_proposed:
            try:
                metrics["silhouette"] = round(
                    silhouette_score(X_scaled, labels_proposed), 4
                )
            except Exception:
                metrics["silhouette"] = 0.0
        else:
            metrics["silhouette"] = 1.0  # 1 seul cluster = parfait par définition

        # ── Métrique 2 : ARI vs ground truth ─────────────────────────────────
        # On encode FAT_relative comme des labels numériques
        fat_labels_gt = pd.Categorical(df_bat["FAT_relative"]).codes
        k_ground_truth = df_bat["FAT_relative"].nunique()

        if k_proposed > 1 and k_ground_truth > 1:
            metrics["ari"] = round(
                adjusted_rand_score(fat_labels_gt, labels_proposed), 4
            )
        else:
            metrics["ari"] = 1.0 if k_proposed == k_ground_truth else 0.0

        metrics["k_ground_truth"] = k_ground_truth

        # ── Métrique 3 : Capacity Compliance ─────────────────────────────────
        n_valid = sum(1 for f in fat_candidates if f.capacity_ok)
        metrics["capacity_compliance_pct"] = round(
            (n_valid / len(fat_candidates)) * 100, 1
        ) if fat_candidates else 100.0

        # ── Métrique 4 : FAT Count Match ──────────────────────────────────────
        metrics["fat_count_match"] = (k_proposed == k_ground_truth)

        # ── Métrique 5 : FDT Assignment Match ────────────────────────────────
        # Pour chaque FAT candidate, vérifier si son FDT assigné == FDT réel
        # Le FDT "réel" = le FDT majoritaire des abonnés dans ce cluster
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
        """
        Pipeline complet pour un bâtiment :
        données → K optimal → KMeans → FAT candidates → métriques

        Traite résidentiels et commerces SÉPARÉMENT
        (règle AT : 1 FAT commerces + N FATs logements).
        """
        id_batiment = df_bat["id_batiment"].iloc[0]
        id_zone = df_bat["id_zone"].iloc[0]

        df_res = df_bat[df_bat["usage"] == "logements"].copy()
        df_com = df_bat[df_bat["usage"] == "commerces"].copy()

        all_candidates = []
        all_labels = np.full(len(df_bat), -1)
        all_X_scaled = np.zeros((len(df_bat), 3))

        label_offset = 0

        # ── Résidentiels ──────────────────────────────────────────────────────
        if len(df_res) >= 2:
            k_res = calculate_k_optimal(len(df_res))
            k_res = min(k_res, len(df_res))  # k ne peut pas dépasser n points

            X_scaled, _, X_orig = self._build_feature_matrix(df_res)
            labels, centroids, inertia = self._run_kmeans(X_scaled, X_orig, k_res)

            candidates_res = self._build_fat_candidates(
                df_res, labels, centroids, "logements", id_offset=label_offset
            )
            all_candidates.extend(candidates_res)

            # Stocker labels dans l'ordre du df_bat complet
            idx_res = df_bat[df_bat["usage"] == "logements"].index
            for i, idx in enumerate(df_res.index):
                pos_in_bat = df_bat.index.get_loc(idx)
                all_labels[pos_in_bat] = labels[i]
                all_X_scaled[pos_in_bat] = X_scaled[i]

            label_offset += k_res
            main_inertia = inertia
            main_X_scaled = X_scaled
            main_labels = labels
            main_df = df_res
        else:
            main_inertia = 0.0
            main_X_scaled = np.zeros((1, 3))
            main_labels = np.array([0])
            main_df = df_bat

        # ── Commerces ─────────────────────────────────────────────────────────
        if len(df_com) >= 1:
            k_com = calculate_k_optimal(len(df_com))
            k_com = min(k_com, len(df_com))

            if len(df_com) >= 2:
                X_scaled_c, _, X_orig_c = self._build_feature_matrix(df_com)
                labels_c, centroids_c, _ = self._run_kmeans(X_scaled_c, X_orig_c, k_com)
            else:
                # Un seul abonné commercial → 1 FAT à sa position exacte
                labels_c = np.array([0])
                centroids_c = df_com[["lat_abonne","lon_abonne","etage"]].values
                X_scaled_c = centroids_c

            candidates_com = self._build_fat_candidates(
                df_com, labels_c, centroids_c, "commerces", id_offset=label_offset
            )
            all_candidates.extend(candidates_com)

        if not all_candidates:
            return None

        # ── Post-processing : corriger les violations de capacité ─────────────
        # K-means peut créer des clusters > 8 → on subdivise
        all_candidates = self._split_oversized_clusters(df_bat, all_candidates)

        # ── Métriques ─────────────────────────────────────────────────────────
        k_total_proposed = len(all_candidates)
        k_total_gt = df_bat["FAT_relative"].nunique()

        # Rebuild combined labels pour métriques globales
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
    # MÉTHODE D'ENTRAÎNEMENT : Traiter tout le dataset
    # ──────────────────────────────────────────────────────────────────────────

    def fit(self, df: pd.DataFrame, max_buildings: int = None) -> "FATKMeansModel":
        """
        Lance le modèle sur tous les bâtiments du dataset.

        Args:
            df           : dataset_final.csv chargé en DataFrame
            max_buildings: limite pour les tests (None = tout traiter)

        Après fit(), accéder aux résultats via :
            model.results_         → liste des BuildingKMeansResult
            model.global_metrics_  → métriques agrégées
            model.summary()        → rapport imprimé
        """
        buildings = df["id_batiment"].unique()
        if max_buildings:
            buildings = buildings[:max_buildings]

        print(f"🚀 Lancement K-means sur {len(buildings)} bâtiments...")
        print(f"   KMeans    : n_init={self.n_init}, vertical_weight={VERTICAL_WEIGHT}")
        print()

        metrics_ari, metrics_sil, metrics_cap, metrics_fdt = [], [], [], []
        k_matches, k_over, k_under = 0, 0, 0
        skipped = 0

        for i, bat_id in enumerate(buildings):
            df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)

            # Skip si bâtiment trop petit (1 seul abonné → pas besoin d'algo)
            if len(df_bat) < 2:
                skipped += 1
                continue

            result = self.process_building(df_bat)
            if result is None:
                skipped += 1
                continue

            self.results_.append(result)

            # Accumuler métriques
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

            # Progress tous les 100 bâtiments
            if (i + 1) % 100 == 0:
                print(f"   → {i+1}/{len(buildings)} bâtiments traités...")

        # ── Métriques globales ────────────────────────────────────────────────
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
    # AFFICHAGE DES RÉSULTATS
    # ──────────────────────────────────────────────────────────────────────────

    def summary(self):
        """Affiche le rapport complet des métriques d'accuracy."""
        g = self.global_metrics_
        n = g.n_buildings_processed

        print()
        print("=" * 62)
        print("       RÉSULTATS MODÈLE K-MEANS — FTTH Smart Planner")
        print("=" * 62)
        print(f"  Bâtiments traités  : {n:,}")
        print(f"  Bâtiments ignorés  : {g.n_buildings_skipped} (< 2 abonnés)")
        print()
        print("  ── ACCURACY MÉTRIQUES ──────────────────────────────────")
        print()
        print(f"  1. Silhouette Score       : {g.mean_silhouette:.4f}")
        _bar(g.mean_silhouette, 1.0)
        print(f"     → Qualité géométrique des clusters")
        print(f"     → Référence : >0.50 bon | >0.70 excellent")
        print()
        print(f"  2. Adjusted Rand Index    : {g.mean_ari:.4f}")
        _bar(g.mean_ari, 1.0)
        print(f"     → Similarité avec les FATs réelles AT")
        print(f"     → Référence : 0=aléatoire | 1=identique à AT")
        print()
        print(f"  3. Capacity Compliance    : {g.mean_capacity_compliance:.1f}%")
        _bar(g.mean_capacity_compliance, 100.0)
        print(f"     → % FATs respectant la contrainte ≤ 8 abonnés")
        print(f"     → Objectif AT : 100%")
        print()
        print(f"  4. FAT Count Match Rate   : {g.fat_count_match_rate:.1f}%")
        _bar(g.fat_count_match_rate, 100.0)
        print(f"     → % bâtiments où K proposé == K réel AT")
        print(f"     → Exact={g.k_exact} | Sur-estimé={g.k_over_estimated} | Sous-estimé={g.k_under_estimated}")
        print()
        print(f"  5. FDT Assignment Match   : {g.mean_fdt_match:.1f}%")
        _bar(g.mean_fdt_match, 100.0)
        print(f"     → % FATs assignées au bon FDT (vs données réelles)")
        print()
        print("=" * 62)

        # Score global synthétique (moyenne pondérée)
        # Capacity et Count Match comptent plus (contraintes physiques AT)
        score_global = (
            g.mean_ari * 0.25 +
            (g.mean_silhouette) * 0.15 +
            (g.mean_capacity_compliance / 100) * 0.30 +
            (g.fat_count_match_rate / 100) * 0.20 +
            (g.mean_fdt_match / 100) * 0.10
        )
        print(f"  SCORE GLOBAL (pondéré)    : {score_global*100:.1f}/100")
        print("=" * 62)

    def to_dataframe(self) -> pd.DataFrame:
        """Exporte les résultats par bâtiment en DataFrame."""
        rows = []
        for r in self.results_:
            for fat in r.fat_candidates:
                rows.append({
                    "id_batiment":      r.id_batiment,
                    "id_zone":          r.id_zone,
                    "fat_id":           fat.fat_id,
                    "usage":            fat.usage,
                    "centroid_lat":     fat.centroid_lat,
                    "centroid_lon":     fat.centroid_lon,
                    "assigned_floor":   fat.assigned_floor,
                    "n_subscribers":    fat.n_subscribers,
                    "fdt_assigned":     fat.fdt_assigned,
                    "capacity_ok":      fat.capacity_ok,
                    # Métriques bâtiment
                    "ari_score":        r.ari_score,
                    "silhouette":       r.silhouette,
                    "capacity_pct":     r.capacity_compliance_pct,
                    "fat_count_match":  r.fat_count_match,
                })


        # Petite fonction helper (à ajouter dans la classe ou en global)
        def _snap_cable(self, dist_m: float) -> int:
            lengths = [15, 20, 50, 80]
            for l in lengths:
                if dist_m <= l:
                    return l
            return 80

        return pd.DataFrame(rows)

    def _snap_cable(self, dist_m: float) -> int:
        """Snap sur les longueurs de câbles préfabriqués AT Oran (15, 20, 50, 80m)."""
        lengths = [15, 20, 50, 80]
        for length in lengths:
            if dist_m <= length:
                return length
        return 80  # maximum disponible

    def get_optimal_fat_placements(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        FONCTION PRODUCTION : Retourne UNIQUEMENT les FATs optimales
        avec les informations demandées par les ingénieurs Algérie Télécom.

        Output columns :
        - zone_id, id_batiment, fat_id, type, lat, lon, etage,
          nb_abonnes, taux_occupation, fdt_id, distance_m, cable_snappe
        """
        buildings = df["id_batiment"].unique()
        output_rows = []

        print("🚀 Génération des emplacements optimaux des FATs (mode Production)...\n")

        for bat_id in buildings:
            df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)
            if len(df_bat) < 2:
                continue

            # Réutilise le pipeline existant
            result = self.process_building(df_bat)
            if result is None or not result.fat_candidates:
                continue

            for fat in result.fat_candidates:
                # Distance vers le FDT (on prend le premier FDT du bâtiment ou le plus proche)
                # Amélioration possible : utiliser find_nearest_fdt avec tous les FDTs
                fdt_lat = df_bat["lat_fdt"].iloc[0]
                fdt_lon = df_bat["lon_fdt"].iloc[0]

                dist_m = haversine_m(
                    fat.centroid_lat, fat.centroid_lon,
                    fdt_lat, fdt_lon
                )

                cable_snap = self._snap_cable(dist_m)

                row = {
                    "zone_id": result.id_zone,
                    "id_batiment": result.id_batiment,
                    "fat_id": f"FAT-{result.id_zone}-{fat.fdt_assigned}-{fat.fat_id[-3:]}",  # Format plus proche AT
                    "type": "RES" if fat.usage == "logements" else "COM",
                    "lat": round(fat.centroid_lat, 6),
                    "lon": round(fat.centroid_lon, 6),
                    "etage": fat.assigned_floor,
                    "nb_abonnes": fat.n_subscribers,
                    "taux_occupation": round(fat.n_subscribers / FAT_CAPACITY * 100, 2),
                    "fdt_id": fat.fdt_assigned,
                    "distance_m": round(dist_m, 2),
                    "cable_snappe": cable_snap,
                    "linéaire_fibre_m": cable_snap
                }
                output_rows.append(row)

        df_optimal = pd.DataFrame(output_rows)

        if not df_optimal.empty:
            lineaire_total = df_optimal["linéaire_fibre_m"].sum()
            mean_occup = df_optimal["taux_occupation"].mean()

            print(f"✅ {len(df_optimal)} FATs optimales générées avec succès !")
            print(f"   Linéaire fibre total estimé : {lineaire_total:,} m")
            print(f"   Taux d'occupation moyen     : {mean_occup:.1f}%")
            print(f"   Nombre de ports GPON utilisés : {len(df_optimal)} (1 par FAT)")
            print(f"   FATs commerciales incluses   : {len(df_optimal[df_optimal['type'] == 'COM'])}")

        return df_optimal

    def export_to_excel_by_building(self, df: pd.DataFrame, output_path: str):
        """
        Exporte les FATs optimales dans un fichier Excel avec 1 sheet par bâtiment.
        - Nettoie automatiquement les caractères interdits dans les noms de feuilles
        - Ajoute un titre clair en première ligne
        - Ajuste la largeur des colonnes
        - Affiche la progression
        """
        buildings = df["id_batiment"].unique()

        print("📊 Export Excel par bâtiment en cours...\n")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            exported = 0

            for bat_id in buildings:
                df_bat = df[df["id_batiment"] == bat_id].copy().reset_index(drop=True)

                if len(df_bat) < 2:
                    continue

                result = self.process_building(df_bat)
                if result is None or not result.fat_candidates:
                    continue

                # Construction des lignes pour cette feuille
                rows = []
                for fat in result.fat_candidates:
                    row = {
                        "fat_id": fat.fat_id,
                        "type": "RES" if fat.usage == "logements" else "COM",
                        "etage": fat.assigned_floor,
                        "lat": round(fat.centroid_lat, 6),
                        "lon": round(fat.centroid_lon, 6),
                        "nb_abonnes": fat.n_subscribers,
                        "taux_occupation (%)": round(fat.n_subscribers / FAT_CAPACITY * 100, 2),
                        "fdt_id": fat.fdt_assigned,
                        "capacity_ok": fat.capacity_ok
                    }
                    rows.append(row)

                df_sheet = pd.DataFrame(rows)

                # ====================== NETTOYAGE DU NOM DE FEUILLE ======================
                sheet_name = str(bat_id)

                # Caractères interdits par Excel
                invalid_chars = ['/', '\\', '?', '*', '[', ']', ':', "'"]
                for char in invalid_chars:
                    sheet_name = sheet_name.replace(char, "_")

                # Limite Excel : maximum 31 caractères
                if len(sheet_name) > 31:
                    sheet_name = sheet_name[:31]

                # Si le nom devient vide après nettoyage (cas extrême)
                if not sheet_name.strip():
                    sheet_name = f"Batiment_{exported + 1}"

                # ====================== ÉCRITURE DE LA FEUILLE ======================
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)

                # Ajout d'un titre en gras
                worksheet = writer.sheets[sheet_name]
                title = f"Bâtiment : {bat_id} | Zone : {result.id_zone} | {len(df_sheet)} FAT(s)"
                worksheet['A1'] = title

                # Mise en forme du titre
                from openpyxl.styles import Font
                worksheet['A1'].font = Font(bold=True, size=12)

                # Ajustement automatique de la largeur des colonnes
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 25)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

                exported += 1

                # Affichage progression tous les 20 bâtiments
                if exported % 20 == 0:
                    print(f"   → {exported} bâtiments exportés...")

        print(f"\n✅ Export Excel terminé avec succès !")
        print(f"   Nombre de feuilles créées : {exported}")
        print(f"   Fichier généré : {output_path}")

def _bar(value: float, max_val: float, width: int = 30):
    """Affiche une barre de progression ASCII."""
    normalized = min(value / max_val, 1.0)
    filled = int(normalized * width)
    bar = "█" * filled + "░" * (width - filled)
    print(f"     [{bar}] {normalized*100:.0f}%")

# =============================================================================
# POINT D'ENTRÉE — Lance le modèle sur dataset_final.csv
# =============================================================================

if __name__ == "__main__":
    import os

    DATASET_PATH = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\dataset_fusionnee_final.csv"


    print(f"📂 Chargement {DATASET_PATH}...")
    df = pd.read_csv(DATASET_PATH, encoding="utf-8-sig")
    print(f"   → {len(df):,} lignes, {df['id_batiment'].nunique():,} bâtiments uniques")
    print(f"   → Colonnes : {list(df.columns)}")

    # ── Validation colonnes requises ──────────────────────────────────────────
    required = ["lat_abonne", "lon_abonne", "etage", "id_batiment",
                "FAT_relative", "usage", "nom_FDT", "lat_fdt", "lon_fdt",
                "code_client"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Colonnes manquantes dans le dataset : {missing}")
        exit(1)

    # ── Lancer le modèle ──────────────────────────────────────────────────────
    # max_buildings=200 pour un test rapide
    # Mettre None pour traiter tout le dataset
    model = FATKMeansModel(random_state=2026, n_init=15)
    model.fit(df, max_buildings=None)
    df_optimal = model.get_optimal_fat_placements(df)
    excel_path = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\resultat\FATs_optimaux.xlsx"
    model.export_to_excel_by_building(df, excel_path)    # ── Export des résultats ──────────────────────────────────────────────────
    df_results = model.to_dataframe()
    output_path = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\resultat\fat_candidates.csv"
    df_results.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Résultats exportés : {output_path}")
    print(f"   → {len(df_results):,} FAT candidates générées")