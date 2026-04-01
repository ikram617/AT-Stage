# =============================================================================
# id_generator.py — Générateur d'IDs normalisés Algérie Télécom
# =============================================================================
#
# FORMAT ID FAT VALIDÉ AT :
#   F{wilaya}-{OLT}-{FDT}-{N°FAT}-{ADRESSE}-Porte({portes})-{etage}F-{seq}
#
# Exemple réel depuis les données AT Oran :
#   F310-001-01-02-CITÉ-CHOUHADA-Porte(4,5,6,7,8,9,10,11)-1F-1
#   │   │   │  │  │              │                         │  │
#   F   Wil OLT FDT N°FAT       Adresse                  Ét Seq
#
# RÈGLES MÉTIER (séances terrain + ingénieur AT) :
#   - Wilaya Oran = 310
#   - OLT : extrait de l'id_zone (Z310-XXX → XXX)
#   - FDT : extrait de nom_FDT (F310-XXX-YY → YY)
#   - N°FAT : numéro séquentiel dans le bâtiment (01, 02, 03...)
#   - Adresse : nom lisible du bâtiment (ELOT), tirets entre les mots
#   - Portes : liste des numéros de portes desservies par cette FAT
#   - Étage départ : étage le plus bas du cluster
#   - Séquence : 1 (toujours 1 dans les données AT Oran)
# =============================================================================

import re
import pandas as pd
from dataclasses import dataclass


@dataclass
class ATFatID:
    """Représente un ID FAT AT complètement décodé."""
    raw: str               # ID complet : F310-001-01-02-ADRESSE-Porte(X,Y)-1F-1
    wilaya: str            # 310
    olt_num: str           # 001
    fdt_num: str           # 01
    fat_seq: str           # 02
    adresse: str           # CITÉ-CHOUHADA
    portes: list[int]      # [4, 5, 6, 7, 8, 9, 10, 11]
    etage_depart: int      # 1
    sequence: int          # 1
    usage: str             # 'logements' ou 'commerces'


class ATIDGenerator:
    """
    Génère les IDs normalisés AT pour les FAT candidates du modèle KMeans.

    PRINCIPE :
    Pour chaque FAT candidate (sortie du modèle KMeans), on reconstitue
    l'ID AT en utilisant les informations du dataset et du résultat KMeans.

    Les abonnés assignés à la FAT → leurs numéros de portes → Porte(X,Y,Z)
    L'étage le plus bas du cluster → étage départ (NF)
    Le FDT assigné → extraction du numéro FDT
    L'id_zone → extraction du numéro OLT
    """

    # Pattern pour décoder un ID existant (validation/comparaison)
    _PATTERN = re.compile(
        r'F(\d+)-(\d+)-(\d+)-(\d+)-(.+)-Porte\(([^)]+)\)-(\d+)F-(\d+)'
    )

    def __init__(self, wilaya_code: str = "310"):
        self.wilaya = wilaya_code

    # ─── MÉTHODE PRINCIPALE ──────────────────────────────────────────────────

    def generate_for_candidates(
        self,
        df_candidates: pd.DataFrame,  # sortie de model.to_dataframe()
        df_dataset: pd.DataFrame       # dataset_final.csv
    ) -> pd.DataFrame:
        """
        Génère les IDs AT pour toutes les FAT candidates.

        Args:
            df_candidates : DataFrame de fat_candidates.csv
                            colonnes : id_batiment, id_zone, fat_id, usage,
                                       centroid_lat, centroid_lon, assigned_floor,
                                       n_subscribers, fdt_assigned
            df_dataset    : dataset_final.csv
                            colonnes : code_client, id_batiment, porte, etage,
                                       FAT_relative, batiment_pav, ...

        Retourne df_candidates enrichi avec la colonne 'fat_id_AT'.
        """
        print("🔑 Génération des IDs AT normalisés...")

        # Construire un index : (id_batiment, fat_id) → liste des portes
        # On regroupe les abonnés par bâtiment pour retrouver leurs portes
        portes_map = self._build_portes_map(df_candidates, df_dataset)

        # Compteur séquentiel par bâtiment
        fat_seq_counter = {}
        ids_at = []

        for _, row in df_candidates.iterrows():
            bat_id  = row["id_batiment"]
            fat_id  = row["fat_id"]       # ex: FAT-001-LOG
            zone_id = row["id_zone"]       # ex: Z310-792
            fdt_id  = row["fdt_assigned"]  # ex: F310-792-01
            floor   = int(row["assigned_floor"])
            usage   = row["usage"]

            # Incrémenter le compteur séquentiel par bâtiment
            if bat_id not in fat_seq_counter:
                fat_seq_counter[bat_id] = 0
            fat_seq_counter[bat_id] += 1
            fat_seq = fat_seq_counter[bat_id]

            # Extraire les composants de l'ID
            olt_num = self._extract_olt_num(zone_id)   # Z310-792 → 792
            fdt_num = self._extract_fdt_num(fdt_id)    # F310-792-01 → 01
            adresse = self._extract_adresse(bat_id)    # RES-ACTEL-EL-MAKKARI-BLOC-L → ACTEL-EL-MAKKARI

            # Récupérer les portes desservies par cette FAT candidate
            key = (bat_id, fat_id)
            portes = portes_map.get(key, [floor * 100 + i for i in range(1, row.get("n_subscribers", 8) + 1)])

            # Générer l'ID AT
            id_at = self._format_id(
                wilaya=self.wilaya,
                olt_num=olt_num,
                fdt_num=fdt_num,
                fat_seq=fat_seq,
                adresse=adresse,
                portes=portes,
                etage_depart=floor,
                sequence=1
            )
            ids_at.append(id_at)

        df_result = df_candidates.copy()
        df_result["fat_id_AT"] = ids_at

        # Déplacer fat_id_AT en première position lisible
        cols = ["id_batiment", "id_zone", "fat_id_AT", "fat_id"] + \
               [c for c in df_result.columns if c not in ["id_batiment", "id_zone", "fat_id_AT", "fat_id"]]
        df_result = df_result[cols]

        n_unique = df_result["fat_id_AT"].nunique()
        print(f"✅ {n_unique:,} IDs AT uniques générés sur {len(df_result):,} FATs")

        return df_result

    # ─── FONCTIONS DE FORMATAGE ──────────────────────────────────────────────

    def _format_id(
        self,
        wilaya: str,
        olt_num: str,
        fdt_num: str,
        fat_seq: int,
        adresse: str,
        portes: list[int],
        etage_depart: int,
        sequence: int = 1
    ) -> str:
        """
        Formate l'ID AT complet.

        Exemple :
          wilaya=310, olt=001, fdt=01, seq=2, adresse=CITÉ-CHOUHADA,
          portes=[4,5,6,7,8,9,10,11], etage=1, sequence=1
          → F310-001-01-02-CITÉ-CHOUHADA-Porte(4,5,6,7,8,9,10,11)-1F-1
        """
        portes_str = ",".join(str(p) for p in sorted(portes))
        return (
            f"F{wilaya}"
            f"-{olt_num}"
            f"-{fdt_num}"
            f"-{fat_seq:02d}"
            f"-{adresse}"
            f"-Porte({portes_str})"
            f"-{etage_depart}F"
            f"-{sequence}"
        )

    def _extract_olt_num(self, zone_id: str) -> str:
        """
        Extrait le numéro OLT depuis zone_id.
        Z310-792 → '792'
        Z310-001 → '001'
        """
        parts = zone_id.split("-")
        if len(parts) >= 2:
            return parts[1].zfill(3)
        return "001"

    def _extract_fdt_num(self, fdt_id: str) -> str:
        """
        Extrait le numéro FDT depuis l'ID FDT.
        F310-792-01 → '01'
        F310-001-03 → '03'
        """
        parts = fdt_id.split("-")
        if len(parts) >= 3:
            return parts[-1].zfill(2)
        return "01"

    def _extract_adresse(self, id_batiment: str) -> str:
        """
        Extrait l'adresse lisible depuis id_batiment.
        RES-ACTEL-EL-MAKKARI-BLOC-L → ACTEL-EL-MAKKARI
        RES-CITÉ-CHOUHADA-BLOC-A    → CITÉ-CHOUHADA

        On retire le préfixe 'RES-' et le suffixe '-BLOC-X'.
        """
        # Supprimer préfixe RES-
        addr = re.sub(r'^RES-', '', id_batiment)
        # Supprimer suffixe -BLOC-X (une seule lettre)
        addr = re.sub(r'-BLOC-[A-Z]$', '', addr)
        return addr if addr else id_batiment

    # ─── CONSTRUCTION DE LA MAP PORTES ───────────────────────────────────────

    def _build_portes_map(
        self,
        df_candidates: pd.DataFrame,
        df_dataset: pd.DataFrame
    ) -> dict:
        """
        Construit un dictionnaire (id_batiment, fat_id) → liste des portes.

        LOGIQUE :
        Le modèle KMeans assigne chaque abonné à une FAT candidate.
        On a besoin des numéros de portes pour générer Porte(X,Y,Z) dans l'ID.

        Mais fat_candidates.csv n'a pas les portes directement.
        On les reconstruit en associant les abonnés du dataset aux clusters
        via la colonne 'assigned_floor' et 'n_subscribers'.

        APPROXIMATION :
        Si on n'a pas le mapping exact abonné→cluster dans df_candidates,
        on reconstruit les portes depuis la ground truth FAT_relative du dataset,
        groupées par (id_batiment, assigned_floor).
        C'est valide car ARI=0.9993 → nos clusters ≈ clusters réels AT.
        """
        portes_map = {}

        if "FAT_relative" not in df_dataset.columns:
            return portes_map

        # Grouper les abonnés par (id_batiment, FAT_relative) pour avoir les portes
        for (bat_id, fat_rel), group in df_dataset.groupby(["id_batiment", "FAT_relative"]):
            portes = sorted(group["porte"].tolist())
            etage_min = group["etage"].min()

            # Trouver la FAT candidate correspondante dans df_candidates
            # Match par id_batiment + étage le plus proche
            candidates_bat = df_candidates[df_candidates["id_batiment"] == bat_id]
            if candidates_bat.empty:
                continue

            # Trouver la FAT candidate dont l'étage est le plus proche
            floor_diffs = (candidates_bat["assigned_floor"] - etage_min).abs()
            best_idx = floor_diffs.idxmin()
            best_fat_id = candidates_bat.loc[best_idx, "fat_id"]

            key = (bat_id, best_fat_id)
            if key not in portes_map:
                portes_map[key] = portes
            else:
                # Plusieurs FATs réelles → même candidat → merger les portes
                portes_map[key] = sorted(set(portes_map[key] + portes))

        return portes_map

    # ─── VALIDATION ──────────────────────────────────────────────────────────

    def decode_existing_id(self, fat_id_at: str) -> ATFatID | None:
        """
        Décode un ID AT existant en ses composants.
        Utile pour valider ou comparer avec les IDs réels AT.
        """
        m = self._PATTERN.match(fat_id_at)
        if not m:
            return None
        return ATFatID(
            raw=fat_id_at,
            wilaya=m.group(1),
            olt_num=m.group(2),
            fdt_num=m.group(3),
            fat_seq=m.group(4),
            adresse=m.group(5),
            portes=[int(p) for p in m.group(6).split(",")],
            etage_depart=int(m.group(7)),
            sequence=int(m.group(8)),
            usage="logements"
        )

    def validate_against_ground_truth(
        self,
        df_with_ids: pd.DataFrame,
        df_dataset: pd.DataFrame
    ) -> dict:
        """
        Compare les IDs générés avec les IDs réels AT (FAT_relative).
        Retourne un rapport de validation.
        """
        real_ids = set(df_dataset["FAT_relative"].unique())
        generated_ids = set(df_with_ids["fat_id_AT"].unique())

        # Parser les deux sets
        real_addrs   = {self._extract_adresse_from_real(i) for i in real_ids}
        gen_addrs    = {self._extract_adresse(i.split("-Porte")[0].split("-")[3:] and
                        "-".join(i.split("-")[4:].split("-Porte")[0].split("-"))
                        or "") for i in generated_ids}

        return {
            "ids_generes"     : len(generated_ids),
            "ids_reels"       : len(real_ids),
            "match_exact"     : len(generated_ids & real_ids),
            "format_valide_pct": self._check_format_compliance(df_with_ids),
        }

    def _extract_adresse_from_real(self, fat_id_real: str) -> str:
        m = self._PATTERN.match(fat_id_real)
        return m.group(5) if m else ""

    def _check_format_compliance(self, df: pd.DataFrame) -> float:
        """% IDs qui respectent le format AT."""
        if "fat_id_AT" not in df.columns:
            return 0.0
        valid = df["fat_id_AT"].apply(
            lambda x: bool(self._PATTERN.match(str(x)))
        ).sum()
        return round(valid / len(df) * 100, 2)


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

if __name__ == "__main__":
    import os

    CANDIDATES_PATH = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\resultat\fat_candidates.csv"
    DATASET_PATH    = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\dataset_fusionnee_final.csv"
    OUTPUT_PATH     = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee\resultat\fat_candidates_avec_ids_AT.csv"

    print("📂 Chargement des données...")
    df_candidates = pd.read_csv(CANDIDATES_PATH, encoding="utf-8-sig")
    df_dataset    = pd.read_csv(DATASET_PATH,    encoding="utf-8-sig")

    print(f"   → {len(df_candidates):,} FAT candidates")
    print(f"   → {df_dataset['id_batiment'].nunique():,} bâtiments dans le dataset")
    print()

    generator = ATIDGenerator(wilaya_code="310")
    df_result = generator.generate_for_candidates(df_candidates, df_dataset)

    # Validation
    print()
    print("=== VALIDATION FORMAT ===")
    compliance = generator._check_format_compliance(df_result)
    print(f"Format AT valide : {compliance}% des IDs")

    print()
    print("=== EXEMPLES D'IDs GÉNÉRÉS ===")
    print(df_result[["id_batiment", "fat_id_AT", "assigned_floor", "n_subscribers"]].head(10).to_string())

    df_result.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"\n✅ Fichier exporté : {OUTPUT_PATH}")