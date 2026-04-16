"""
SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM (v8)
Corrections v8 vs v7:
  A. Volume rétabli : suppression du filtre MIN_POLYGON_AREA qui éliminait 95% des bâtiments.
     Le plafond MAX_LOG_BATIMENT (80) seul suffit pour éviter les méga-bâtiments.
     MultiPolygon explosion conservée MAIS sans filtre surface agressif.
  B. Notation RDC correcte : un bâtiment "5 étages" = RDC + R+1 + ... + R+5 → nb_et=5
     Le RDC (etage=0) a son propre FAT commerce si comm_rdc=True, sinon appartements.
     Les étages résidentiels = range(1, nb_et+1) toujours.
  C. FAT groupement par étage PRIORITAIRE : les appartements du MÊME étage sont
     regroupés en premier. Si un étage a > 8 appts, il est découpé en FATs de 8.
     Si un étage a ≤ 8 appts, ils tiennent dans 1 FAT. Les FATs ne MÉLANGENT JAMAIS
     des étages différents (physiquement impossible — le câble vertical est trop long).

Conservé de v6/v7:
  1. batiment_pav : numérotation réinitialisée par résidence
  2. GPS abonnés : colonnes verticales avec jitter par étage (buffer interne)
  3. Câble drop : np.random.choice([15, 20, 50, 80]) pur
  4. Jitter dans polygone érodé (buffer négatif)
"""

import os, sys, warnings
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt, ceil
from shapely.geometry import Point
from backend.config import settings

PREFAB_LENGTHS = settings.AT_DROP_CABLE_STANDARDS_M

warnings.filterwarnings("ignore")
np.random.seed(2026)
os.makedirs(r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_annaba4", exist_ok=True)

try:
    import osmnx as ox
    ox.settings.log_console = False
except ImportError:
    sys.exit("Installe osmnx : pip install osmnx")


# ====================== CONSTANTES WILAYAS (inchangées) ======================
WILAYAS = {
    1:  {"PLACE": "Adrar, Algeria",           "BBOX": {"north": 27.910, "south": 27.840, "east": -0.255, "west": -0.330}, "WILAYA": 1,  "ZONE_CODE": "010", "wilaya_nom": "Adrar"},
    2:  {"PLACE": "Chlef, Algeria",            "BBOX": {"north": 36.195, "south": 36.135, "east": 1.360,  "west": 1.280},  "WILAYA": 2,  "ZONE_CODE": "020", "wilaya_nom": "Chlef"},
    3:  {"PLACE": "Laghouat, Algeria",         "BBOX": {"north": 33.840, "south": 33.775, "east": 2.915,  "west": 2.840},  "WILAYA": 3,  "ZONE_CODE": "030", "wilaya_nom": "Laghouat"},
    4:  {"PLACE": "Oum El Bouaghi, Algeria",   "BBOX": {"north": 35.895, "south": 35.845, "east": 7.130,  "west": 7.060},  "WILAYA": 4,  "ZONE_CODE": "040", "wilaya_nom": "Oum El Bouaghi"},
    5:  {"PLACE": "Batna, Algeria",            "BBOX": {"north": 35.590, "south": 35.520, "east": 6.210,  "west": 6.140},  "WILAYA": 5,  "ZONE_CODE": "050", "wilaya_nom": "Batna"},
    6:  {"PLACE": "Béjaïa, Algeria",           "BBOX": {"north": 36.770, "south": 36.720, "east": 5.110,  "west": 5.040},  "WILAYA": 6,  "ZONE_CODE": "060", "wilaya_nom": "Béjaïa"},
    7:  {"PLACE": "Biskra, Algeria",           "BBOX": {"north": 34.870, "south": 34.820, "east": 5.760,  "west": 5.700},  "WILAYA": 7,  "ZONE_CODE": "070", "wilaya_nom": "Biskra"},
    8:  {"PLACE": "Béchar, Algeria",           "BBOX": {"north": 31.650, "south": 31.590, "east": -2.170, "west": -2.240}, "WILAYA": 8,  "ZONE_CODE": "080", "wilaya_nom": "Béchar"},
    9:  {"PLACE": "Blida, Algeria",            "BBOX": {"north": 36.500, "south": 36.440, "east": 2.870,  "west": 2.800},  "WILAYA": 9,  "ZONE_CODE": "090", "wilaya_nom": "Blida"},
    10: {"PLACE": "Bouira, Algeria",           "BBOX": {"north": 36.395, "south": 36.340, "east": 3.930,  "west": 3.860},  "WILAYA": 10, "ZONE_CODE": "100", "wilaya_nom": "Bouira"},
    11: {"PLACE": "Tamanrasset, Algeria",      "BBOX": {"north": 22.800, "south": 22.740, "east": 5.555,  "west": 5.480},  "WILAYA": 11, "ZONE_CODE": "110", "wilaya_nom": "Tamanrasset"},
    12: {"PLACE": "Tébessa, Algeria",          "BBOX": {"north": 35.430, "south": 35.375, "east": 8.155,  "west": 8.085},  "WILAYA": 12, "ZONE_CODE": "120", "wilaya_nom": "Tébessa"},
    13: {"PLACE": "Tlemcen, Algeria",          "BBOX": {"north": 34.905, "south": 34.850, "east": -1.290, "west": -1.360}, "WILAYA": 13, "ZONE_CODE": "130", "wilaya_nom": "Tlemcen"},
    14: {"PLACE": "Tiaret, Algeria",           "BBOX": {"north": 35.400, "south": 35.340, "east": 1.350,  "west": 1.280},  "WILAYA": 14, "ZONE_CODE": "140", "wilaya_nom": "Tiaret"},
    15: {"PLACE": "Tizi Ouzou, Algeria",       "BBOX": {"north": 36.740, "south": 36.690, "east": 4.090,  "west": 4.000},  "WILAYA": 15, "ZONE_CODE": "150", "wilaya_nom": "Tizi Ouzou"},
    16: {"PLACE": "Alger, Algeria",            "BBOX": {"north": 36.810, "south": 36.720, "east": 3.130,  "west": 2.990},  "WILAYA": 16, "ZONE_CODE": "160", "wilaya_nom": "Alger"},
    17: {"PLACE": "Djelfa, Algeria",           "BBOX": {"north": 34.700, "south": 34.640, "east": 3.280,  "west": 3.210},  "WILAYA": 17, "ZONE_CODE": "170", "wilaya_nom": "Djelfa"},
    18: {"PLACE": "Jijel, Algeria",            "BBOX": {"north": 36.835, "south": 36.790, "east": 5.775,  "west": 5.710},  "WILAYA": 18, "ZONE_CODE": "180", "wilaya_nom": "Jijel"},
    19: {"PLACE": "Sétif, Algeria",            "BBOX": {"north": 36.215, "south": 36.155, "east": 5.440,  "west": 5.370},  "WILAYA": 19, "ZONE_CODE": "190", "wilaya_nom": "Sétif"},
    20: {"PLACE": "Saïda, Algeria",            "BBOX": {"north": 34.855, "south": 34.800, "east": 0.165,  "west": 0.095},  "WILAYA": 20, "ZONE_CODE": "200", "wilaya_nom": "Saïda"},
    21: {"PLACE": "Skikda, Algeria",           "BBOX": {"north": 36.890, "south": 36.840, "east": 6.920,  "west": 6.855},  "WILAYA": 21, "ZONE_CODE": "210", "wilaya_nom": "Skikda"},
    22: {"PLACE": "Sidi Bel Abbès, Algeria",   "BBOX": {"north": 35.220, "south": 35.155, "east": -0.595, "west": -0.670}, "WILAYA": 22, "ZONE_CODE": "220", "wilaya_nom": "Sidi Bel Abbès"},
    23: {"PLACE": "Annaba, Algeria",           "BBOX": {"north": 36.930, "south": 36.880, "east": 7.780,  "west": 7.710},  "WILAYA": 23, "ZONE_CODE": "230", "wilaya_nom": "Annaba"},
    24: {"PLACE": "Guelma, Algeria",           "BBOX": {"north": 36.480, "south": 36.430, "east": 7.465,  "west": 7.400},  "WILAYA": 24, "ZONE_CODE": "240", "wilaya_nom": "Guelma"},
    25: {"PLACE": "Constantine, Algeria",      "BBOX": {"north": 36.400, "south": 36.330, "east": 6.650,  "west": 6.570},  "WILAYA": 25, "ZONE_CODE": "250", "wilaya_nom": "Constantine"},
    26: {"PLACE": "Médéa, Algeria",            "BBOX": {"north": 36.285, "south": 36.230, "east": 2.780,  "west": 2.715},  "WILAYA": 26, "ZONE_CODE": "260", "wilaya_nom": "Médéa"},
    27: {"PLACE": "Mostaganem, Algeria",       "BBOX": {"north": 35.960, "south": 35.910, "east": 0.110,  "west": 0.040},  "WILAYA": 27, "ZONE_CODE": "270", "wilaya_nom": "Mostaganem"},
    28: {"PLACE": "M'Sila, Algeria",           "BBOX": {"north": 35.720, "south": 35.665, "east": 4.580,  "west": 4.510},  "WILAYA": 28, "ZONE_CODE": "280", "wilaya_nom": "M'Sila"},
    29: {"PLACE": "Mascara, Algeria",          "BBOX": {"north": 35.410, "south": 35.350, "east": 0.170,  "west": 0.100},  "WILAYA": 29, "ZONE_CODE": "290", "wilaya_nom": "Mascara"},
    30: {"PLACE": "Ouargla, Algeria",          "BBOX": {"north": 31.990, "south": 31.940, "east": 5.360,  "west": 5.295},  "WILAYA": 30, "ZONE_CODE": "300", "wilaya_nom": "Ouargla"},
    31: {"PLACE": "Oran, Algeria",             "BBOX": {"north": 35.760, "south": 35.620, "east": -0.520, "west": -0.730}, "WILAYA": 31, "ZONE_CODE": "310", "wilaya_nom": "Oran"},
    32: {"PLACE": "El Bayadh, Algeria",        "BBOX": {"north": 33.710, "south": 33.655, "east": 1.030,  "west": 0.960},  "WILAYA": 32, "ZONE_CODE": "320", "wilaya_nom": "El Bayadh"},
    33: {"PLACE": "Illizi, Algeria",           "BBOX": {"north": 26.500, "south": 26.450, "east": 8.490,  "west": 8.425},  "WILAYA": 33, "ZONE_CODE": "330", "wilaya_nom": "Illizi"},
    34: {"PLACE": "Bordj Bou Arréridj, Algeria","BBOX": {"north": 36.090, "south": 36.035, "east": 4.775,  "west": 4.705}, "WILAYA": 34, "ZONE_CODE": "340", "wilaya_nom": "Bordj Bou Arréridj"},
    35: {"PLACE": "Boumerdès, Algeria",        "BBOX": {"north": 36.780, "south": 36.730, "east": 3.500,  "west": 3.430},  "WILAYA": 35, "ZONE_CODE": "350", "wilaya_nom": "Boumerdès"},
    36: {"PLACE": "El Tarf, Algeria",          "BBOX": {"north": 36.780, "south": 36.730, "east": 8.330,  "west": 8.265},  "WILAYA": 36, "ZONE_CODE": "360", "wilaya_nom": "El Tarf"},
    37: {"PLACE": "Tindouf, Algeria",          "BBOX": {"north": 27.710, "south": 27.660, "east": -8.095, "west": -8.165}, "WILAYA": 37, "ZONE_CODE": "370", "wilaya_nom": "Tindouf"},
    38: {"PLACE": "Tissemsilt, Algeria",       "BBOX": {"north": 35.620, "south": 35.565, "east": 1.840,  "west": 1.775},  "WILAYA": 38, "ZONE_CODE": "380", "wilaya_nom": "Tissemsilt"},
    39: {"PLACE": "El Oued, Algeria",          "BBOX": {"north": 33.380, "south": 33.325, "east": 6.900,  "west": 6.835},  "WILAYA": 39, "ZONE_CODE": "390", "wilaya_nom": "El Oued"},
    40: {"PLACE": "Khenchela, Algeria",        "BBOX": {"north": 35.440, "south": 35.390, "east": 7.155,  "west": 7.085},  "WILAYA": 40, "ZONE_CODE": "400", "wilaya_nom": "Khenchela"},
    41: {"PLACE": "Souk Ahras, Algeria",       "BBOX": {"north": 36.295, "south": 36.245, "east": 7.990,  "west": 7.920},  "WILAYA": 41, "ZONE_CODE": "410", "wilaya_nom": "Souk Ahras"},
    42: {"PLACE": "Tipaza, Algeria",           "BBOX": {"north": 36.600, "south": 36.550, "east": 2.460,  "west": 2.390},  "WILAYA": 42, "ZONE_CODE": "420", "wilaya_nom": "Tipaza"},
    43: {"PLACE": "Mila, Algeria",             "BBOX": {"north": 36.465, "south": 36.415, "east": 6.280,  "west": 6.215},  "WILAYA": 43, "ZONE_CODE": "430", "wilaya_nom": "Mila"},
    44: {"PLACE": "Aïn Defla, Algeria",        "BBOX": {"north": 36.275, "south": 36.225, "east": 1.985,  "west": 1.915},  "WILAYA": 44, "ZONE_CODE": "440", "wilaya_nom": "Aïn Defla"},
    45: {"PLACE": "Naâma, Algeria",            "BBOX": {"north": 33.280, "south": 33.225, "east": -0.285, "west": -0.355}, "WILAYA": 45, "ZONE_CODE": "450", "wilaya_nom": "Naâma"},
    46: {"PLACE": "Aïn Témouchent, Algeria",   "BBOX": {"north": 35.310, "south": 35.255, "east": -1.120, "west": -1.195}, "WILAYA": 46, "ZONE_CODE": "460", "wilaya_nom": "Aïn Témouchent"},
    47: {"PLACE": "Ghardaïa, Algeria",         "BBOX": {"north": 32.510, "south": 32.455, "east": 3.700,  "west": 3.630},  "WILAYA": 47, "ZONE_CODE": "470", "wilaya_nom": "Ghardaïa"},
    48: {"PLACE": "Relizane, Algeria",         "BBOX": {"north": 35.750, "south": 35.695, "east": 0.580,  "west": 0.510},  "WILAYA": 48, "ZONE_CODE": "480", "wilaya_nom": "Relizane"},
    49: {"PLACE": "Timimoun, Algeria",         "BBOX": {"north": 29.270, "south": 29.220, "east": 0.260,  "west": 0.190},  "WILAYA": 49, "ZONE_CODE": "490", "wilaya_nom": "Timimoun"},
    50: {"PLACE": "Bordj Badji Mokhtar, Algeria","BBOX": {"north": 21.340, "south": 21.290, "east": 0.945, "west": 0.875}, "WILAYA": 50, "ZONE_CODE": "500", "wilaya_nom": "Bordj Badji Mokhtar"},
    51: {"PLACE": "Ouled Djellal, Algeria",    "BBOX": {"north": 34.440, "south": 34.385, "east": 5.090,  "west": 5.020},  "WILAYA": 51, "ZONE_CODE": "510", "wilaya_nom": "Ouled Djellal"},
    52: {"PLACE": "Béni Abbès, Algeria",       "BBOX": {"north": 30.140, "south": 30.085, "east": -2.140, "west": -2.215}, "WILAYA": 52, "ZONE_CODE": "520", "wilaya_nom": "Béni Abbès"},
    53: {"PLACE": "In Salah, Algeria",         "BBOX": {"north": 27.215, "south": 27.165, "east": 2.490,  "west": 2.420},  "WILAYA": 53, "ZONE_CODE": "530", "wilaya_nom": "In Salah"},
    54: {"PLACE": "In Guezzam, Algeria",       "BBOX": {"north": 19.580, "south": 19.530, "east": 5.780,  "west": 5.710},  "WILAYA": 54, "ZONE_CODE": "540", "wilaya_nom": "In Guezzam"},
    55: {"PLACE": "Touggourt, Algeria",        "BBOX": {"north": 33.125, "south": 33.075, "east": 6.090,  "west": 6.020},  "WILAYA": 55, "ZONE_CODE": "550", "wilaya_nom": "Touggourt"},
    56: {"PLACE": "Djanet, Algeria",           "BBOX": {"north": 24.575, "south": 24.525, "east": 9.505,  "west": 9.435},  "WILAYA": 56, "ZONE_CODE": "560", "wilaya_nom": "Djanet"},
    57: {"PLACE": "El M'Ghair, Algeria",       "BBOX": {"north": 33.960, "south": 33.910, "east": 5.960,  "west": 5.890},  "WILAYA": 57, "ZONE_CODE": "570", "wilaya_nom": "El M'Ghair"},
    58: {"PLACE": "El Meniaa, Algeria",        "BBOX": {"north": 30.600, "south": 30.550, "east": 2.905,  "west": 2.835},  "WILAYA": 58, "ZONE_CODE": "580", "wilaya_nom": "El Meniaa"},
}

PRENOMS = [
    "Mohamed", "Ahmed", "Abdelkader", "Youcef", "Karim", "Rachid", "Hichem", "Sofiane",
    "Amir", "Bilal", "Yassine", "Omar", "Ali", "Hamza", "Ibrahim", "Zakaria", "Nadir",
    "Walid", "Fayçal", "Salim", "Redouane", "Mehdi", "Ismaïl", "Anis", "Lotfi", "Tarek",
    "Nabil", "Sami", "Khaled", "Mourad", "Aymen", "Badr", "Djamel", "Farid", "Hassan",
    "Imad", "Jamel", "Kamel", "Lamine", "Mokhtar", "Nasser", "Oussama", "Rafik", "Sadek",
    "Taha", "Younes", "Zinedine", "Abdelhamid", "Abderrahmane", "Abdellah", "Abdou",
    "Fatima", "Amina", "Nadia", "Samira", "Meriem", "Lynda", "Dalila", "Sofia", "Leila",
    "Houda", "Yasmine", "Sara", "Nour", "Imane", "Kheira", "Zineb", "Rania", "Warda",
    "Amel", "Dounia", "Fatiha", "Ghania", "Hassiba", "Karima", "Lamia", "Mouna", "Nabila",
    "Ouarda", "Rachida", "Sabrine", "Salima", "Souad", "Wahiba", "Yamina", "Zahra",
    "Aicha", "Baya", "Cherifa", "Djamila", "Fella", "Ghalia", "Hayet", "Ines", "Jazia",
    "Keltoum", "Lila", "Malika", "Naima", "Ouahiba", "Rym", "Siham", "Touria", "Ikram"
]
NOMS_FAM = [
    "KEBIR", "BENALI", "KHELIFI", "BOUDIAF", "MANSOURI", "ZERROUK", "HAMIDI", "BENSALEM",
    "RAHMANI", "BELARBI", "CHABANE", "MERAD", "GUERFI", "BOUCHENAK", "FERHAT", "HADJ",
    "SAIDI", "BOUALI", "BENDJEDDOU", "CHERIF", "DJABRI", "ELKHALFI", "GHERBI", "HOCINE",
    "IDIR", "KACI", "LAKHDARI", "MEZIANE", "NEMOUCHI", "OUALI", "REZGUI", "SAHRAOUI",
    "TAHRI", "YAHIA", "ZEMMOURI", "ABDELLI", "BENYAHIA", "BOUKHEZAR", "DAOUD", "ELAMRI",
    "FERRADJ", "GHOUALI", "HADDAD", "IBRAHIMI", "JABRI", "KHEMISSI", "LARBES", "MAHFOUD",
    "NEDJARI", "OUAMAR", "RAHAL", "SADOUN", "TAMER", "YAHYAOUI", "ZIANI", "AMROUCHE",
    "BENAISSA", "BOUAZIZ", "CHOUKRI", "DJERADI", "ELKADI", "FARAH", "GHOUL", "HAMAIDI",
    "ISSAOUI", "KABIR", "LAKHAL", "MOKRANI", "NASRI", "OUICHAOUI", "RAHIM", "SAHLI",
    "TADJER", "YOUNSI", "ZITOUNI", "BENMOUSSA", "BOUKHARI", "CHELALI", "DJILALI",
    "ELHADJ", "FELLAH", "GUECHTOULI", "HADDAR", "KHALDI", "LALLAOUI", "MEZGHICHE",
    "NAIT", "OUEDRAOUI", "REBAI", "SIDI", "TAHIRI", "YAHIAOUI", "ZOUAOUI", "BENDALI",
    "BOUSSAID", "CHAABANE", "DJEBBAR", "ELKHEIR", "FERRARI", "GHAZI", "HASSANI", "KARA"
]

WILAYA_INPUT = 23
_w = WILAYAS[WILAYA_INPUT]
PLACE = _w["PLACE"]
WILAYA = _w["WILAYA"]
ZONE_CODE = _w["ZONE_CODE"]
wilaya_nom = _w["wilaya_nom"]

OPERATEURS = {
    "Djezzy":  ["077", "078"],
    "Ooredoo": ["066", "069", "079"],
    "Mobilis": ["055", "056", "057"]
}

# ====================== CHARGEMENT OSM ======================
def charger_communes_oran():
    print("🔄 Chargement dynamique des COMMUNES depuis OSM...")
    try:
        df = ox.features_from_place(PLACE, tags={"admin_level": "8", "boundary": "administrative"})
        communes = []
        for _, row in df.iterrows():
            name = str(row.get("name", "")).strip()
            if name:
                lat = row.geometry.centroid.y
                lon = row.geometry.centroid.x
                cecli = f"CECLI {name.upper().replace(' ', '-')}"
                communes.append((name.upper().replace(" ", "-"), round(lat, 3), round(lon, 3), cecli))
        if communes:
            print(f"  → {len(communes)} communes chargées depuis OSM")
            return communes
    except Exception as e:
        print(f"  ⚠️ OSM communes échoué : {e}")
    print("  → Fallback vers liste statique")
    return [("ANNABA", 36.905, 7.745, "CECLI ANNABA")]

def charger_quartiers_oran():
    print("🔄 Chargement dynamique des QUARTIERS depuis OSM...")
    try:
        df = ox.features_from_place(PLACE, tags={"place": ["suburb", "neighbourhood", "quarter"]})
        quartiers = sorted(df["name"].dropna().str.upper().str.replace(" ", "-").unique().tolist())
        if quartiers:
            print(f"  → {len(quartiers)} quartiers chargés depuis OSM")
            return quartiers
    except Exception as e:
        print(f"  ⚠️ OSM quartiers échoué : {e}")
    print("  → Fallback vers liste statique")
    return ["CENTRE-VILLE", "EL-BOUNI", "SIDI-BRAHIM", "EL-HADJAR"]

COMMUNES_ORAN = charger_communes_oran()
QUARTIERS_ORAN = charger_quartiers_oran()

# ====================== CONSTANTES ======================
ETAGES_DEFAUT = {
    "apartments": 6, "residential": 5, "yes": 5, "commercial": 2, "retail": 1, "dormitory": 5
}
EXCLURE = {
    "garage","garages","shed","hut","industrial","warehouse",
    "school","church","mosque","hospital","kindergarten",
    "stadium","parking","service","roof","fence","wall",
    "government","civic","public","house","detached"
}

# ====================== UTILITAIRES GPS ======================
def haversine(la1, lo1, la2, lo2):
    R = 6371000
    la1, lo1, la2, lo2 = map(radians, [la1, lo1, la2, lo2])
    a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
    return R * 2 * asin(sqrt(a))

def offset_gps(lat, lon, dist_m, angle_deg):
    a = radians(angle_deg)
    dlat = (dist_m * cos(a)) / 111000
    dlon = (dist_m * sin(a)) / (111000 * cos(radians(lat)))
    return round(lat + dlat, 6), round(lon + dlon, 6)

def rand_offset(lat, lon, dmin, dmax):
    return offset_gps(lat, lon, np.random.uniform(dmin, dmax), np.random.uniform(0, 360))

def commune_proche(lat, lon):
    dists = [(haversine(lat, lon, c[1], c[2]), c[0], c[3]) for c in COMMUNES_ORAN]
    dists.sort(key=lambda x: x[0])
    return dists[0][1], dists[0][2]

def fmt_zone_id(seq): return f"Z{ZONE_CODE}-{seq:03d}"   # 3 chiffres — cohérent avec zone_id dans generer_tables
def fmt_olt(seq, elot): return f"T{ZONE_CODE}-{seq:03d}-{elot}-AN6000-IN"
def fmt_fdt(olt_seq, fdt_seq): return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}"
def fmt_fat(olt_seq, fdt_seq, spl_seq, elot, portes, etage, num_fat):
    p = ",".join(str(x) for x in portes)
    return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}-{spl_seq:02d}-{elot}-Porte({p})-{etage}F-{num_fat}"

def fmt_code_client(seq): return f"1000000271{1200 + seq:04d}"

def resoudre_elot(row, bat_idx):
    nom = str(row.get("nom_bat", "")).strip()
    if nom and nom.lower() not in ("nan", "", "none"):
        return nom.upper().replace(" ", "-")
    for col in ["addr:street", "addr_street"]:
        rue = str(row.get(col, "")).strip()
        if rue and rue.lower() not in ("nan", "", "none"):
            return rue.upper().replace(" ", "-")
    for col in ["addr:suburb", "addr:quarter"]:
        q = str(row.get(col, "")).strip()
        if q and q.lower() not in ("nan", "", "none"):
            return q.upper().replace(" ", "-")
    return QUARTIERS_ORAN[bat_idx % len(QUARTIERS_ORAN)]

def resoudre_voie(row):
    for col in ["addr:street", "addr_street"]:
        rue = str(row.get(col, "")).strip()
        if rue and rue.lower() not in ("nan", "", "none"):
            return rue.title()
    return ""

def resoudre_quartier_osm(row, bat_idx):
    for col in ["addr:suburb", "addr:quarter"]:
        q = str(row.get(col, "")).strip()
        if q and q.lower() not in ("nan", "", "none"):
            return q.upper()
    return QUARTIERS_ORAN[bat_idx % len(QUARTIERS_ORAN)].replace("-", " ")


# ====================== FIX 1: GÉNÉRATEUR DE NOM RÉSIDENCE AVEC RESET ======================
#
# POURQUOI ce design ?
# -------------------
# Le problème original : chaque bâtiment utilisait `olt_seq` (index global 1, 2, 3, ..., N)
# comme numéro dans batiment_pav. Résultat : une résidence de 3 blocs donnait
# "AADL-BLOC-A-numero-47", "AADL-BLOC-B-numero-48", "AADL-BLOC-C-numero-49"
# au lieu de "AADL-BLOC-A-numero-1", "AADL-BLOC-B-numero-2", "AADL-BLOC-C-numero-3"
#
# LA SOLUTION : une classe stateful qui maintient un compteur par (promoteur, quartier).
# Quand un nouveau groupe commence, le compteur repart à 1.
#
# PATTERN UTILISÉ : "compteur par clé" avec un dict Python.
# C'est exactement ce qu'on fait en AT pour gérer les séquences locales.
class ResidenceNamer:
    """
    Génère des noms de bâtiment réalistes avec numérotation qui repart à 1
    pour chaque résidence (groupe de bâtiments d'un même promoteur/quartier).

    Fonctionnement :
      - Chaque résidence = groupe de RESIDENCE_SIZE bâtiments consécutifs
      - Les bâtiments d'une même résidence partagent le même promoteur et quartier
      - Le numéro (numero_in_res) repart à 1 pour chaque nouvelle résidence

    Exemple pour RESIDENCE_SIZE=3 :
      bat_idx=0 → résidence 0, numero=1, AADL BLOC-A-numero-1
      bat_idx=1 → résidence 0, numero=2, AADL BLOC-B-numero-2
      bat_idx=2 → résidence 0, numero=3, AADL BLOC-C-numero-3
      bat_idx=3 → résidence 1, numero=1, LPA BLOC-A-numero-1  ← RESET !
    """
    PROMOTEURS = ["AADL", "HASNAOUI", "LPP", "LPA", "HLM", "CNEP", "LSL", "PRIVE"]
    RESIDENCE_SIZE = 3  # nombre de bâtiments par résidence

    def __init__(self, seed=2026):
        # np.random pour reproducibilité, indépendant du seed global
        self._rng = np.random.RandomState(seed)
        self._residence_promoteurs = {}  # résidence_id → promoteur choisi

    def get(self, bat_idx: int, elot: str) -> tuple[str, str, str]:
        """
        Retourne (batiment_pav, nom_bat, type_batiment) pour le bâtiment bat_idx.

        bat_idx    : index global du bâtiment (0-based)
        elot       : identifiant de l'îlot/adresse (pour la lisibilité)

        Returns:
          batiment_pav : nom complet de l'adresse postale
          nom_bat      : même chose (utilisé dans d'autres colonnes)
          promoteur    : type_batiment (AADL, etc.)
        """
        # Quelle résidence ? La division entière donne le groupe.
        residence_id = bat_idx // self.RESIDENCE_SIZE

        # Numéro DANS la résidence (1-based) — le modulo repart à 0 puis +1
        numero_in_res = (bat_idx % self.RESIDENCE_SIZE) + 1

        # Chaque résidence a UN promoteur fixe, choisi aléatoirement à la première
        # occurrence. On mémorise pour que tous les blocs d'une même résidence
        # aient le même promoteur.
        if residence_id not in self._residence_promoteurs:
            self._residence_promoteurs[residence_id] = self._rng.choice(self.PROMOTEURS)
        promoteur = self._residence_promoteurs[residence_id]

        # La lettre de bloc (A, B, C, ...) dépend de la position dans la résidence
        bloc_letter = chr(64 + numero_in_res)  # 65=A → numero=1 donne 'A'

        nom = f"{promoteur} – {elot.replace('-', ' ').title()} BLOC-{bloc_letter}-numero-{numero_in_res}"
        return nom, nom, promoteur


# Instanciation unique, partagée par toute la génération
_namer = ResidenceNamer(seed=2026)


# ====================== LIMITES RÉALISTES ======================
# CORRECTION v9: valeurs réduites pour correspondre aux vraies normes AT
#
# POURQUOI ces valeurs ?
# - AADL standard = R+7 (7 étages + RDC) → MAX_ETAGES=8 (cap pour building:levels aberrants)
# - Standard AT Oran : 2-4 logements par étage par cage d'escalier (FAT = 1 cage)
#   → MAX_LOG_PAR_ETAGE=4 (était 6 → générait trop d'abonnés)
# - MAX_LOG_BATIMENT=40 : bâtiment typique = 8 étages × 4 apps = 32, cap à 40 pour sécurité
#
# IMPACT SUR LES MÉTRIQUES :
# Avant : ~78 abonnés/bâtiment théorique × 9 bâtiments collapsés = 700 abonnés/id_batiment
# Après : ~20-32 abonnés/bâtiment × 1 bâtiment = 20-32 abonnés/id_batiment
MAX_ETAGES = 8           # R+8 maximum (OSM building:levels ≥9 = complexe mal tagué)
MAX_LOG_PAR_ETAGE = 8    # jusqu'à 8 logements par étage — aligné sur FAT_CAPACITY=8 (1 FAT plein par étage)
MAX_LOG_BATIMENT = 80    # cap absolu — 8 étages × 8 appts = 64, cap à 80 pour sécurité

# Seuil surface MINIMAL — seulement pour éliminer les erreurs OSM grossières (< 1m²)
# CORRECTION v8 : on abaisse ce seuil drastiquement (était 1e-7 = ~100m² en Algérie,
# beaucoup trop élevé — éliminait les petits immeubles légitimes)
# 1e-9 deg² ≈ 1m² → seulement les points/lignes OSM mal tagués comme polygones
MIN_POLYGON_AREA_DEG2 = 1e-9

# ====================== CHARGER BÂTIMENTS ======================
def charger_batiments():
    """
    Charge les bâtiments depuis OSM.

    POURQUOI L'EXPLOSION MULTIPOLYGON MAIS SANS FILTRE SURFACE AGRESSIF ?
    -----------------------------------------------------------------------
    Un objet OSM MultiPolygon représente physiquement plusieurs bâtiments séparés
    qui partagent les mêmes tags (même propriétaire, même résidence).
    On les explose pour avoir 1 ligne = 1 bâtiment réel.

    MAIS le filtre MIN_POLYGON_AREA en v7 était 1e-7 deg² ≈ 100m² — il éliminait
    les bâtiments de 10×8m (80m²) qui sont des immeubles R+5 tout à fait normaux
    en Algérie. Résultat : 95% des bâtiments perdus → 18K abonnés au lieu de 400K.

    En v8 : seuil abaissé à 1e-9 ≈ 1m² — uniquement pour les erreurs OSM vraiment
    grossières (nœuds mal fermés qui créent des triangles de 0.5m²).
    """
    print(f"\n[1/3] Chargement bâtiments OSM — {PLACE}")
    bats = ox.features_from_place(PLACE, tags={"building": True})
    bats = bats[bats.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if "building" in bats.columns:
        bats = bats[~bats["building"].fillna("").isin(EXCLURE)].copy()

    # ---- EXPLOSION DES MULTIPOLYGONS ----
    expanded_rows = []
    for _, row in bats.iterrows():
        geom = row.geometry
        if geom.geom_type == "MultiPolygon":
            for sub_poly in geom.geoms:
                if sub_poly.area >= MIN_POLYGON_AREA_DEG2:
                    new_row = row.copy()
                    new_row.geometry = sub_poly
                    expanded_rows.append(new_row)
        else:
            if geom.area >= MIN_POLYGON_AREA_DEG2:
                expanded_rows.append(row)

    import geopandas as gpd
    bats = gpd.GeoDataFrame(expanded_rows, geometry="geometry", crs="EPSG:4326")

    centroids = bats.geometry.centroid
    bats["lat"] = centroids.y.round(6)
    bats["lon"] = centroids.x.round(6)
    bats["btype"] = bats.get("building", "unknown").fillna("unknown")
    bats["nom_bat"] = bats.get("name", "").fillna("")
    bats["poly_area_m2"] = bats.geometry.area * (111000 ** 2)  # estimation m² (debug)

    if "building:levels" in bats.columns:
        bats["etages_osm"] = pd.to_numeric(bats["building:levels"], errors="coerce")
    else:
        bats["etages_osm"] = np.nan

    TYPES_COM = {"commercial", "retail", "office", "shop", "mixed"}
    bats["commerce_rdc"] = bats["btype"].isin(TYPES_COM)
    bats = bats.reset_index(drop=True)
    print(f"  → {len(bats):,} bâtiments (après explosion MultiPolygon)")
    return bats


# ====================== ESTIMATION LOGEMENTS ======================
# ====================== ESTIMATION LOGEMENTS RÉALISTE (correction finale) ======================
def estimer_logements(bats):
    """
    Estimation réaliste du nombre de logements par bâtiment.
    - Un seul bâtiment OSM = 1 bloc physique (pas une résidence entière).
    - Valeurs typiques AADL / HLM à Oran : 3-5 étages, 3-4 logements par étage.
    - Maximum absolu : 5 étages × 5 log/étage = 25 logements + RDC commerce éventuel.
    - Résultat attendu : 12 à 28 abonnés par bâtiment (136 → beaucoup trop élevé).
    """
    print(f"\n[2/3] Estimation logements réalistes (1 bâtiment = 1 bloc physique)")

    nb_et_list, nb_log_list, log_par_etage_list = [], [], []

    for _, row in bats.iterrows():
        # Lecture OSM + plafond réaliste
        if pd.notna(row.get("etages_osm")) and row["etages_osm"] >= 1:
            nb_et = min(int(row["etages_osm"]), 8)          # max 8 étages (R+8)
        else:
            nb_et = np.random.randint(3, 6)                 # 3 à 5 étages (typique)

        # Logements par étage (3 à 4 → très courant en AADL)
        max_lpe = min(5, MAX_LOG_BATIMENT // max(nb_et, 1))
        log_par_etage = np.random.randint(3, max_lpe + 1)

        # Nombre total de logements résidentiels (sans compter le RDC commerce)
        nb_log = nb_et * log_par_etage

        # Plafond absolu (sécurité)
        if nb_log > MAX_LOG_BATIMENT:
            nb_log = MAX_LOG_BATIMENT
            log_par_etage = MAX_LOG_BATIMENT // nb_et

        nb_et_list.append(nb_et)
        nb_log_list.append(nb_log)
        log_par_etage_list.append(log_par_etage)

    bats["nb_etages"] = nb_et_list
    bats["nb_logements"] = nb_log_list
    bats["log_par_etage"] = log_par_etage_list

    print(f" → {len(bats)} bâtiments traités")
    print(f" → Étages moyen     : {np.mean(nb_et_list):.1f}  (max={max(nb_et_list)})")
    print(f" → Log/étage moyen  : {np.mean(log_par_etage_list):.1f}")
    print(f" → Abonnés/bâtiment moyen : {np.mean(nb_log_list):.1f}  (max={max(nb_log_list)})")
    print(f" → Total logements estimés : {sum(nb_log_list):,}")
    return bats

# ====================== FIX 2 : POSITIONS 2D POUR UN BÂTIMENT ======================
#
# POURQUOI ce changement ?
# -----------------------
# Avant : door_coords était samplé UNE FOIS avec nb_log_etage points.
#         Chaque étage réutilisait door_coords[log_idx], donc appartement 2
#         au 3ème étage avait exactement le même (lat, lon) qu'au 1er étage.
#         Résultat : vue de dessus = cluster serré, et la 3D montrait des points
#         parfaitement empilés au lieu d'être légèrement décalés.
#
# Maintenant : on crée UNE position 2D de base par appartement (index dans l'étage).
#              Ces positions sont FIXES pour tous les étages (même appartement,
#              même cage d'escalier = même colonne).
#              Chaque (étage, appartement) reçoit un JITTER indépendant de ~1-3m
#              pour simuler la légère imprécision de placement du câble.
#
# PHYSIQUE RÉELLE : les appartements d'une même colonne verticale sont EXACTEMENT
# au-dessus les uns des autres. Donc base_lat/base_lon IDENTIQUES, seul l'étage change.
# Le jitter simule l'imprécision GPS/placement, pas un déplacement horizontal réel.
# ====================== GÉNÉRATION COLONNES VERTICALES RÉALISTES ======================
def generer_positions_batiment(polygon, nb_colonnes):
    """
    P1 amélioré : Génère nb_colonnes positions fixes (une par colonne verticale d'appartements)
    Chaque colonne = même (lat, lon) pour tous les étages → vue 3D réaliste.
    Points toujours à l'intérieur du polygone du bâtiment (érosion 5m).
    """
    if polygon.is_empty or not polygon.is_valid:
        center = polygon.centroid
        return [(center.y, center.x)] * nb_colonnes

    # Érosion légère pour rester dans les murs (immeuble réaliste)
    eroded = polygon.buffer(-5)
    if eroded.is_empty or eroded.area < 1:
        eroded = polygon

    positions = []
    minx, miny, maxx, maxy = eroded.bounds

    attempts = 0
    while len(positions) < nb_colonnes and attempts < 2000:
        x = np.random.uniform(minx, maxx)
        y = np.random.uniform(miny, maxy)
        pt = Point(x, y)
        if eroded.contains(pt):
            positions.append((round(pt.y, 7), round(pt.x, 7)))  # lat, lon
        attempts += 1

    # Complétion si pas assez de points (très rare)
    while len(positions) < nb_colonnes:
        positions.append(positions[0])

    # Petite perturbation pour éviter superposition parfaite (réalisme)
    for i in range(len(positions)):
        positions[i] = rand_offset(positions[i][0], positions[i][1], 2, 5)

    return positions

def ajouter_jitter_etage(base_lat: float, base_lon: float) -> tuple[float, float]:
    """
    Ajoute un jitter réaliste par étage.

    VALEUR : normal(0, 0.000008) ≈ ±0.8m (1σ), max ~2.4m (3σ)
    La marge de sécurité de generer_positions_batiment (3.2m) est > 2.4m,
    donc les points restent dans le bâtiment avec probabilité > 99.7%.

    Ce jitter simule :
    - L'imprécision du relevé GPS terrain
    - Les légères variations de position réelle des terminaux optiques
    """
    return (
        round(base_lat + np.random.normal(0, 0.000050), 6),
        round(base_lon + np.random.normal(0, 0.000050), 6)
    )


# ====================== FAT ASSIGNMENT : PRIORITÉ ÉTAGE (v8) ======================
#
# RÈGLE PHYSIQUE AT RÉELLE :
# Un FAT est un boîtier physique installé dans la gaine technique d'UN ÉTAGE.
# Il est impossible de câbler des abonnés de l'étage 3 ET de l'étage 7 sur le
# même FAT sans traverser 4 étages de câble vertical — ce serait absurde et
# refusé lors de l'audit terrain.
#
# NOUVELLE RÈGLE DE GROUPEMENT (v8) :
#   1. On traite chaque étage INDÉPENDAMMENT
#   2. Pour chaque étage : on groupe les appts par porte_rank // 8
#      → Si nb_log_etage = 4 : 1 FAT de 4 abonnés
#      → Si nb_log_etage = 10 : 1 FAT de 8 + 1 FAT de 2
#   3. Le RDC est toujours traité séparément (commerce ou résidentiel)
#
# DIFFÉRENCE AVEC v6/v7 :
#   v6/v7 : groupement GLOBAL par porte_rank // 8 → mélangeait les étages
#   v8    : groupement PAR ÉTAGE → chaque FAT = 1 seul étage
#
# NUMÉROTATION DES PORTES (convention AT) :
#   - RDC = porte 01, 02, ... nb_rdc_appts
#   - Étage 1 = porte 01, 02, ... nb_log_etage (REPART À 1 CHAQUE ÉTAGE)
#   Oui, AT utilise des numéros de porte relatifs à l'étage, pas globaux.
#   C'est pourquoi la vraie règle est (porte_rank_dans_etage) // 8.

def assigner_fats_batiment(
    nb_et: int,
    nb_log_etage: int,
    base_positions: list[tuple[float, float]],
    comm_rdc: bool,
    olt_seq: int,
    fdt_seq_num: int,
    spl_seq_start: int,
    fat_num_start: int,
    elot: str,
    zone_id: str,
    bat_unique_id: str,      # FIX v9: identifiant UNIQUE par bâtiment OSM (= zone_id)
    olt_lat: float,
    olt_lon: float,
    fdt_nom: str,
    client_seq_start: int,
    numero_seq_start: int,
    batiment_pav: str,
    voie_osm: str,
    quartier_osm: str,
    commune: str,
    nbr_logements_total: int,
    type_batiment: str,
    presence_de_commerce: int,
):
    """
    Génère tous les abonnés + FATs pour un bâtiment entier.

    Structure physique simulée :
      Étage 0 (RDC) :
        - si comm_rdc=True  → 1 FAT commerce (2-4 locaux)
        - si comm_rdc=False → RDC absent, les logements démarrent à l'étage 1
      Étages 1..nb_et :
        - 1 ou plusieurs FATs résidentiels selon nb_log_etage
        - Chaque FAT = max 8 abonnés du MÊME étage
    """
    fats_out, spl2_out, abonnes_out, clients_out, adresses_out, numeros_out = [], [], [], [], [], []

    spl_seq = spl_seq_start
    client_seq = client_seq_start
    numero_seq = numero_seq_start

    # Position GPS du FAT = centroïde du polygone du bâtiment (cage technique)
    fat_lat, fat_lon = olt_lat, olt_lon

    def _creer_fat_et_abonnes(etage: int, appts_etage: list[dict], usage: str):
        """
        Crée les FATs pour un étage donné, en groupant par tranches de 8.

        appts_etage : liste de dicts {appt_in_floor, porte}
        usage       : "logements" ou "commerces"

        Cette fonction interne (closure) modifie les listes de sortie
        et les compteurs via nonlocal.

        NUMÉROTATION FAT (fix v10) :
        fat_num_etage repart à 1 à chaque étage — il représente le N° du FAT
        DANS l'étage (pas dans le bâtiment). Ex: étage 3 avec 2 FATs → 3F-1 et 3F-2.
        spl_seq reste global au bâtiment (N° unique du splitter N2 dans la gaine).
        """
        nonlocal spl_seq, client_seq, numero_seq

        nb_appts = len(appts_etage)
        if nb_appts == 0:
            return

        # fat_num_etage repart à 1 pour chaque étage
        fat_num_etage = 1

        # Groupement par porte_rank // 8 DANS l'étage (portes 0-based)
        nb_groups_raw = ceil(nb_appts / 8)
        remainder = nb_appts % 8
        # Règle AT : remainder < 2 → absorber dans le groupe précédent
        if nb_groups_raw > 1 and 0 < remainder < 2:
            nb_groups = nb_groups_raw - 1
        else:
            nb_groups = nb_groups_raw

        # Répartir les appartements dans les groupes
        groups: list[list[dict]] = [[] for _ in range(nb_groups)]
        for idx, apt in enumerate(appts_etage):
            g = min(idx // 8, nb_groups - 1)
            groups[g].append(apt)

        for group in groups:
            if not group:
                continue

            portes_group = [apt["porte"] for apt in group]
            fat_id = fmt_fat(olt_seq, fdt_seq_num, spl_seq, elot,
                             portes_group, etage, fat_num_etage)
            dist_fat = int(np.random.choice([15, 20, 50, 80]))

            fats_out.append({
                "id": fat_id,
                "nom_FDT": fdt_nom,
                "num_de_groupe": fat_num_etage,
                "latitude": fat_lat,
                "longitude": fat_lon,
                "usage": usage,
                "nb_ports": settings.FAT_CAPACITY,
                "nb_abonnes_sim": len(group),
                "nb_etages_bat": nb_et,
                "nb_log_etage": nb_log_etage,
                "zone_id": zone_id,
                "distance_FAT_m": dist_fat
            })

            for dn in range(1, settings.FAT_CAPACITY + 1):
                spl2_out.append({
                    "id": f"{fat_id}-DOWN-{dn:02d}",
                    "nom_FAT": fat_id,
                    "id_splitter1": f"{fdt_nom}-S{spl_seq:02d}",
                    "rapport_de_division": f"1:{settings.SPLITTER_N2_RATIO}",
                    "port_splitter": f"{fdt_nom}-S{spl_seq:02d}-DOWN-{dn}",
                    "etat": "utilisé" if dn <= len(group) else "libre",
                    "zone_id": zone_id
                })

            # Créer les abonnés de ce groupe
            for apt in group:
                appt_in_floor = apt["appt_in_floor"]
                porte = apt["porte"]

                # Position de base de cet appartement (colonne verticale)
                # On utilise le modulo pour éviter un IndexError si nb_com au RDC > nb_log_etage
                base_lat, base_lon = base_positions[appt_in_floor % len(base_positions)]
                lat_abonne, lon_abonne = ajouter_jitter_etage(base_lat, base_lon)

                cc = fmt_code_client(client_seq)
                op = np.random.choice(list(OPERATEURS.keys()))
                prefix = np.random.choice(OPERATEURS[op])
                contact = int(prefix + f"{np.random.randint(1000000, 9999999):07d}")

                abonnes_out.append({
                    "code_client": cc,
                    "latitude": lat_abonne,
                    "longitude": lon_abonne,
                    "etage": etage,
                    "porte": porte,
                    # FIX v10: id_batiment = batiment_pav (ex: "HASNAOUI – Aadl Sidi Achour BLOC-A-numero-1")
                    # Lisible, unique par bâtiment OSM (promoteur + elot + bloc + numero)
                    # zone_id reste disponible via id_zone pour les jointures techniques
                    "id_batiment": batiment_pav,
                    "id_zone": zone_id,
                    "FAT_relative": fat_id,
                    "distance_FAT_m": dist_fat,
                    "nbr_etages": nb_et,
                    "nbr_logements_par_etage": nb_log_etage,
                    "nbr_logements_total": nbr_logements_total,
                    "type_batiment": type_batiment,
                    "presence_de_commerce": presence_de_commerce
                })
                clients_out.append({
                    "code_client": cc,
                    "contact": contact,
                    "nom": f"{np.random.choice(PRENOMS)} {np.random.choice(NOMS_FAM)}"
                })
                adresses_out.append({
                    "code_client": cc,
                    "batiment_pav": batiment_pav,
                    "voie": voie_osm if voie_osm else f"FAT{ZONE_CODE}-{elot}",
                    "quartier": quartier_osm,
                    "commune": commune.replace("-", " "),
                    "wilaya": wilaya_nom
                })
                numeros_out.append({
                    "num_de_groupe": int(f"4{1800000 + numero_seq:07d}"),
                    "code_client": cc,
                    "region_relative": zone_id,
                    "FAT_relative": fat_id
                })

                client_seq += 1
                numero_seq += 1

            spl_seq += 1
            fat_num_etage += 1

    # ====================== RDC (ÉTAGE 0) ======================
    # RÈGLE : RDC créé UNIQUEMENT si le bâtiment a un commerce (comm_rdc=True).
    # Si comm_rdc=False → pas de FAT étage 0, les logements commencent à l'étage 1.
    # Justification : presence_de_commerce=0 signifie RDC vide ou inexistant.
    # Un RDC résidentiel serait incohérent avec presence_de_commerce=0.
    if comm_rdc:
        # RDC commercial : 2-4 locaux dans leur propre FAT
        nb_com = np.random.randint(2, 5)
        appts_rdc = [{"appt_in_floor": i, "porte": i + 1} for i in range(nb_com)]
        _creer_fat_et_abonnes(etage=0, appts_etage=appts_rdc, usage="commerces")
    # else : RDC absent → rien à générer pour l'étage 0

    # ====================== ÉTAGES RÉSIDENTIELS (1..nb_et) ======================
    # Fix 1: numéro de porte GLOBAL au bâtiment (pas relatif à l'étage).
    # Étage 1 → portes 1..nb_log_etage
    # Étage 2 → portes nb_log_etage+1..2*nb_log_etage
    # etc. → chaque porte est unique dans le bâtiment.
    for et in range(1, nb_et + 1):
        porte_offset = (et - 1) * nb_log_etage  # décalage global
        appts_etage = [{"appt_in_floor": i, "porte": porte_offset + i + 1} for i in range(nb_log_etage)]
        _creer_fat_et_abonnes(etage=et, appts_etage=appts_etage, usage="logements")

    return {
        "fats": fats_out, "spl2": spl2_out,
        "abonnes": abonnes_out, "clients": clients_out,
        "adresses": adresses_out, "numeros": numeros_out,
        "spl_seq": spl_seq,
        "client_seq": client_seq, "numero_seq": numero_seq
    }


# ====================== GÉNÉRATION TABLES ======================
def generer_tables(bats):
    print(f"\n[3/3] Génération tables AT — {wilaya_nom}")
    from backend.config import settings

    zones, equipements, cartes, ports = [], [], [], []
    fdts, spl1_rows, fats, spl2 = [], [], [], []
    clients, adresses, numeros, abonnes = [], [], [], []

    client_seq = 0
    numero_seq = 0
    olt_abonnes_count = {}

    nb_bat_total = len(bats)
    # Fallback aléatoire 40% uniquement pour les bâtiments sans tag OSM commerce
    nb_commerce_rdc = int(nb_bat_total * 0.40)
    commerce_indices = np.random.choice(nb_bat_total, nb_commerce_rdc, replace=False)
    commerce_set = set(commerce_indices)

    # compteur de zone par commune — zone_id suit la commune et repart à 001
    commune_seq_counter: dict[str, int] = {}

    for bat_idx, row in bats.iterrows():
        olt_seq = bat_idx + 1
        olt_lat = float(row["lat"])
        olt_lon = float(row["lon"])
        nb_et = int(row["nb_etages"])
        nb_log_etage = int(row.get("log_par_etage", 4))
        polygon = row.geometry
        # Fix 2: RDC commercial = tag OSM en priorité, sinon fallback aléatoire 40%
        osm_is_commerce = bool(row.get("commerce_rdc", False))
        comm_rdc = osm_is_commerce or (bat_idx in commerce_set)

        elot = resoudre_elot(row, bat_idx)
        commune, cecli = commune_proche(olt_lat, olt_lon)
        voie_osm = resoudre_voie(row)
        quartier_osm = resoudre_quartier_osm(row, bat_idx)

        # Fix 5: zone_id suit la commune — compteur par commune repart à 001
        commune_seq_counter[commune] = commune_seq_counter.get(commune, 0) + 1
        zone_seq = commune_seq_counter[commune]
        zone_id = f"Z{ZONE_CODE}-{zone_seq:03d}"

        # FIX 1 EN ACTION : numérotation réinitialisée par résidence
        batiment_pav, nom_bat, type_batiment = _namer.get(bat_idx, elot)

        olt_nom = f"T{ZONE_CODE}-{olt_seq:03d}-{elot}-AN6000-IN"

        if zone_id not in olt_abonnes_count:
            olt_abonnes_count[zone_id] = 0

        # ZONE + ÉQUIPEMENT + CARTE + PORT (inchangés)
        zones.append({
            "id": zone_id,
            "wilaya": WILAYA,
            "wilaya_nom": wilaya_nom,
            "commune": commune,
            "zone_geographique": cecli
        })
        equipements.append({
            "id": zone_id,
            "nom": olt_nom,
            "ip": f"100.{WILAYA}.{(olt_seq % 254)+1}.{np.random.randint(1,254)}",
            "type": np.random.choice(["FIBERHOME","HUAWEI","ZTE"], p=[0.5,0.35,0.15]),
            "latitude": olt_lat,
            "longitude": olt_lon
        })
        for slot in [1, 2]:
            cartes.append({
                "id": int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}"),
                "nom": f"{olt_nom}_Frame:0/Slot:{slot}",
                "id_equipement": zone_id,
                "position": f"0/{slot}",
                "type": "gpon",
                "rack": slot
            })
            for p in range(1, 17):
                etat = "utilisé" if (slot == 1 and p <= 4) else "libre"
                ports.append({
                    "id": int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}{p:02d}"),
                    "nom": f"{olt_nom}-Frame:0/Slot:{slot}/Port:{p}",
                    "nomCarte": f"{olt_nom}_Frame:0/Slot:{slot}",
                    "position": p,
                    "etat": etat,
                    "zone_id": zone_id
                })

        # FDT + SPLITTER N1 (inchangés)
        nb_fdts = np.random.randint(2, 5)
        fdts_bat = []
        for i in range(nb_fdts):
            angle = i * (360 / nb_fdts) + np.random.uniform(-15, 15)
            dist = np.random.uniform(80, 400)
            fl, flo = offset_gps(olt_lat, olt_lon, dist, angle)
            fdt_nom = f"F{ZONE_CODE}-{olt_seq:03d}-{i+1:02d}"
            fdts_bat.append({
                "id": fdt_nom,
                "nom_equipement": olt_nom,
                "zone": zone_id,
                "latitude": fl,
                "longitude": flo,
                "distance_olt_m": round(dist)
            })
            fdts.append(fdts_bat[-1])
            spl1_rows.append({
                "id": f"{fdt_nom}-S01",
                "nom_FDT": fdt_nom,
                "rapport_de_division": "1:8",
                "etat": "utilisé",
                "zone_id": zone_id
            })

        fdt_ref = fdts_bat[0]
        fdt_nom = fdt_ref["id"]
        fdt_seq_num = int(fdt_nom.split("-")[-1])

        spl_seq_local = 1
        fat_num_local = 1

        # FIX v9: SUPPRESSION du doublon FAT commerce.
        # AVANT: generer_tables créait un FAT commerce ICI (lignes ~900-931) sans abonnés,
        #        PUIS assigner_fats_batiment en créait un autre AVEC abonnés.
        #        Résultat: fat.csv avait 2 FATs pour le RDC commerce, dont 1 orphelin.
        # APRÈS: la création du FAT commerce est uniquement dans assigner_fats_batiment
        #        qui gère aussi la création des abonnés → source unique de vérité.
        #
        # Le spl_seq_local et fat_num_local commencent à 1 pour TOUS les bâtiments.
        # Le RDC (commerce ou résidentiel) reçoit le numéro 1, puis les étages suivants.

        # FIX 2 + 3 EN ACTION : générer les positions 2D AVANT la boucle d'étages,
        # puis déléguer l'assignation FAT à la fonction globale
        base_positions = generer_positions_batiment(polygon, nb_log_etage)

        result = assigner_fats_batiment(
            nb_et=nb_et,
            nb_log_etage=nb_log_etage,
            base_positions=base_positions,
            comm_rdc=comm_rdc,
            olt_seq=olt_seq,
            fdt_seq_num=fdt_seq_num,
            spl_seq_start=spl_seq_local,
            fat_num_start=fat_num_local,
            elot=elot,
            zone_id=zone_id,
            # FIX v9: passe zone_id comme identifiant unique du bâtiment OSM.
            # zone_id = f"Z{ZONE_CODE}-{olt_seq:05d}" est garanti unique car
            # olt_seq = bat_idx + 1 (index d'itération du GeoDataFrame).
            bat_unique_id=zone_id,
            olt_lat=olt_lat,
            olt_lon=olt_lon,
            fdt_nom=fdt_nom,
            client_seq_start=client_seq,
            numero_seq_start=numero_seq,
            batiment_pav=batiment_pav,
            voie_osm=voie_osm,
            quartier_osm=quartier_osm,
            commune=commune,
            nbr_logements_total=int(row["nb_logements"]),
            type_batiment=type_batiment,
            presence_de_commerce=1 if comm_rdc else 0,
        )

        fats.extend(result["fats"])
        spl2.extend(result["spl2"])
        abonnes.extend(result["abonnes"])
        clients.extend(result["clients"])
        adresses.extend(result["adresses"])
        numeros.extend(result["numeros"])

        client_seq = result["client_seq"]
        numero_seq = result["numero_seq"]
        olt_abonnes_count[zone_id] = olt_abonnes_count.get(zone_id, 0) + len(result["abonnes"])

    return {
        "zone": pd.DataFrame(zones),
        "equipement": pd.DataFrame(equipements),
        "carte": pd.DataFrame(cartes),
        "port": pd.DataFrame(ports),
        "fdt": pd.DataFrame(fdts),
        "splitter_n1": pd.DataFrame(spl1_rows),
        "fat": pd.DataFrame(fats),
        "splitter_n2": pd.DataFrame(spl2),
        "client": pd.DataFrame(clients),
        "adresse": pd.DataFrame(adresses),
        "numero": pd.DataFrame(numeros),
        "abonnes": pd.DataFrame(abonnes)
    }


# ====================== MERGE TABLES (inchangé) ======================
def merge_all_tables():
    print("🔄 Lecture de TOUTES les tables depuis donnee_annaba4/...")
    base = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_annaba4"

    abonnes    = pd.read_csv(f"{base}/abonnes.csv")
    client     = pd.read_csv(f"{base}/client.csv")
    fat        = pd.read_csv(f"{base}/fat.csv")
    numero     = pd.read_csv(f"{base}/numero.csv")
    adresse    = pd.read_csv(f"{base}/adresse.csv")
    zone       = pd.read_csv(f"{base}/zone.csv")
    equipement = pd.read_csv(f"{base}/equipement.csv")
    fdt        = pd.read_csv(f"{base}/fdt.csv")
    carte      = pd.read_csv(f"{base}/carte.csv")
    port       = pd.read_csv(f"{base}/port.csv")
    splitter_n2 = pd.read_csv(f"{base}/splitter_n2.csv")

    print(f"   → {len(abonnes):,} abonnés chargés")

    df = abonnes.merge(client, on="code_client", how="left")
    df = df.rename(columns={"latitude": "lat_abonne", "longitude": "lon_abonne"})
    df = df.merge(
        fat[["id", "latitude", "longitude", "usage", "nb_ports",
             "nb_abonnes_sim", "distance_FAT_m", "nom_FDT"]],
        left_on="FAT_relative", right_on="id", how="left"
    ).drop(columns=["id"], errors="ignore")
    df = df.rename(columns={"latitude": "lat_fat", "longitude": "lon_fat"})
    df = df.merge(numero[["code_client", "num_de_groupe"]], on="code_client", how="left")
    df = df.merge(adresse, on="code_client", how="left")
    df = df.merge(zone[["id", "commune", "zone_geographique"]],
                  left_on="id_zone", right_on="id", how="left").drop(columns=["id"], errors="ignore")
    df = df.merge(equipement[["id", "nom", "type", "ip"]],
                  left_on="id_zone", right_on="id", how="left").drop(columns=["id"], errors="ignore")
    df = df.rename(columns={"nom": "nom_OLT", "type": "type_OLT"})
    df = df.merge(fdt[["id", "latitude", "longitude", "distance_olt_m"]],
                  left_on="nom_FDT", right_on="id", how="left").drop(columns=["id"], errors="ignore")
    df = df.rename(columns={"latitude": "lat_fdt", "longitude": "lon_fdt"})

    if not carte.empty:
        carte_agg = carte.groupby("id_equipement").agg(
            nb_cartes=('id', 'count'),
            cartes_positions=('position', lambda x: ', '.join(sorted(set(x.astype(str)))))
        ).reset_index()
        df = df.merge(carte_agg, left_on="id_zone", right_on="id_equipement",
                      how="left").drop(columns=["id_equipement"], errors="ignore")

    if not port.empty:
        port_agg = port.groupby("zone_id").agg(
            nb_ports_total=('id', 'count'),
            nb_ports_utilises=('etat', lambda x: (x == 'utilisé').sum()),
            nb_ports_libres=('etat', lambda x: (x == 'libre').sum())
        ).reset_index()
        df = df.merge(port_agg, left_on="id_zone", right_on="zone_id",
                      how="left").drop(columns=["zone_id"], errors="ignore")

    colonnes = [
        "code_client", "id_batiment", "id_zone",
        "lat_abonne", "lon_abonne", "etage", "porte",
        "FAT_relative", "usage",
        "lat_fat", "lon_fat", "nb_abonnes_sim", "distance_FAT_m",
        "nom_FDT", "lat_fdt", "lon_fdt", "distance_olt_m",
        "nb_etages_bat", "nb_log_etage",
        "nbr_etages", "nbr_logements_par_etage", "nbr_logements_total",
        "type_batiment", "presence_de_commerce",
        "num_de_groupe",
        "nom", "batiment_pav", "quartier", "commune"
    ]
    colonnes = [c for c in colonnes if c in df.columns]
    df_final = df[colonnes].sort_values(by=["id_batiment", "etage", "code_client"]).reset_index(drop=True)
    df_final.to_csv(f"{base}/dataset_fusionnee_final.csv", index=False, encoding="utf-8-sig")

    print(f"\n✅ dataset_final.csv créé avec succès !")
    print(f"   → {len(df_final):,} lignes")
    print(f"   → Colonnes : {list(df_final.columns)}")
    return df_final


if __name__ == "__main__":
    print("=" * 70)
    print(f"SIMULATION DONNÉES FTTH MASSIVE — ALGÉRIE TÉLÉCOM v6")
    print(f"→ Wilaya : {WILAYA} — {wilaya_nom}")
    print("=" * 70)

    bats = charger_batiments()
    bats = estimer_logements(bats)
    tables = generer_tables(bats)
    base = r"C:\Users\blabl\OneDrive\Desktop\New folder\donnee_annaba4"
    for nom, df in tables.items():
        path = os.path.join(base, f"{nom}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  ✓ {nom:15s} {len(df):7d} lignes")
    merge_all_tables()