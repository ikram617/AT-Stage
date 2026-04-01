from pydantic_settings import BaseSettings  # lit aussi les .env


class ATConfig(BaseSettings):
    # Capacités équipements
    FAT_CAPACITY: int = 8
   # FAT_CAPACITY_LARGE: int = 16   # FATs 16 ports (vues comme 2×8 en software)
    SPLITTER_N1_RATIO: int = 8     # Splitter niveau 1 : 1:8
    SPLITTER_N2_RATIO: int = 8     # Splitter niveau 2 : 1:8
    FAT_THRESHOLD: float = 0.75

    # ─── Seuil d'accumulation (compteur résidus) ────────────────────────────
    # Si on accumule > 6 logements sans FAT dédiée → créer une FAT mutualisée
    FAT_ACCUMULATION_MAX: int = 6

    # ─── Infrastructure OLT ─────────────────────────────────────────────────
    OLT_MAX_DISTANCE_KM: float = 12.0   # Distance max OLT → abonné (Huawei)
    GPON_PORTS_PER_CARD: int = 16      # Ports GPON par carte
    CARDS_PER_OLT: int = 2             # Cartes par OLT

    # ─── Longueurs câbles préfabriqués (en mètres) ──────────────────────────
    CABLE_LENGTHS: list[int] = [15, 20, 50, 80]

    # ─── Identifiants par défaut (Alger = 16) ───────────────────────────────
    DEFAULT_WILAYA_CODE: str = "016"

    class Config:
        env_file = ".env"   # permet de surcharger via un fichier .env


# Instance globale — importée partout dans le projet
# Utilisation: from app.config import settings
settings = ATConfig()