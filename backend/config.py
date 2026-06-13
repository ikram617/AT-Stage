from typing import ClassVar, Dict, Union
from pathlib import Path
from pydantic import field_validator
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

    @field_validator("AT_DROP_CABLE_STANDARDS_M", mode="before")
    @classmethod
    def parse_cable_standards(cls, v: Union[str, list]) -> list[int]:
        if isinstance(v, str):
            # Supprime les crochets si présents (format JSON) et split par virgule
            v = v.strip("[]").replace(" ", "")
            if not v: return []
            return [int(x) for x in v.split(",") if x.strip()]
        return v
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

    # ── K-Predictor Mode defaults ──────────────────────────────────────────────
    # Default floor heights per building type (metres).
    # Used by the K-Predictor placement mode when subscribers are unknown.
    BUILDING_TYPE_HEIGHT_M: ClassVar[Dict[str, float]] = {
        "AADL": 3.05,
        "LPP":  3.20,
        "HLM":  2.75,
        "LPA":  2.95,
        "LSL":  2.70,
        "CNEP": 3.10,
        "PRIVE": 3.30,
    }
    DEFAULT_BUILDING_HEIGHT_M: float = 3.0

    # Threshold-based balancing: fill a FAT up to this fraction of capacity
    # before opening a new one. Avoids highly unbalanced FATs.
    FAT_BALANCE_THRESHOLD: float = 0.75   # 75% of FAT_CAPACITY = 6/8

    class Config:
        env_file = str(Path(__file__).parent / ".env")
        extra = "ignore"
settings = ATConfig()