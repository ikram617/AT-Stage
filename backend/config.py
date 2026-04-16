from typing import ClassVar, Dict

from pydantic_settings import BaseSettings  # lit aussi les .env


class ATConfig(BaseSettings):
    OLT_CARDS_PER_OLT: int = 2
    GPON_PORTS_PER_CARD: int = 16
    OLT_MAX_SUBSCRIBERS: int = OLT_CARDS_PER_OLT * GPON_PORTS_PER_CARD * 8 * 8
    SPLITTER_N1_RATIO: int = 8
    SPLITTER_N2_RATIO: int = 8
    FAT_CAPACITY: int = SPLITTER_N2_RATIO  # 8 abonnés/FAT
    FDT_MAX_FATS: int = SPLITTER_N1_RATIO  # 8 FATs/FDT
    CARD_MAX_FDTS: int = GPON_PORTS_PER_CARD  # 16 FDTs/carte
    MAX_OLT_TO_SUBSCRIBER_M: float = 12_000.0  # 12km
    TORTUOSITY_TRUNK: float = 1.3
    """
    Facteur de tortuosité pour câble trunk (FDT→FAT).
    La fibre ne va jamais en ligne droite : elle suit les murs,
    monte les cages d'escalier, contourne les obstacles.
    distance_réelle ≈ distance_haversine × 1.3
    """

    TORTUOSITY_FEEDER: float = 1.2
    AT_DROP_CABLE_STANDARDS_M: list[int] = [15, 20, 50, 80]
    FAT_CAPACITY_REMAINDER_THRESHOLD: int = 6
    BUILDING_TYPES: ClassVar[Dict[str, str]] = {
        "AADL": "Agence Nationale de l'Amélioration et du Développement du Logement",
        "LPP": "Logement Promotionnel Public",
        "LPA": "Logement Promotionnel Aidé",
        "LSL": "Logement Social Locatif",
        "HLM": "Habitation à Loyer Modéré",
        "CNEP": "Résidence CNEP/Banque",
        "PRIVE": "Résidence privée",
    }

    DEFAULT_WILAYA_CODE: str = "016"

    class Config:
        env_file = ".env"
settings = ATConfig()