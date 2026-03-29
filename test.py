"""
SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM (v3 OSM DYNAMIQUE + MASSIVE)
=========================================================
→ Tous les bâtiments résidentiels (données massives)
→ Communes + Quartiers chargés AUTOMATIQUEMENT depuis OSM
→ Séquences FDT et splitter LOCALES par OLT
"""
#suprimer @ ET @ normalisee
import os, sys, time, warnings
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt

warnings.filterwarnings("ignore")
np.random.seed(2026)
os.makedirs("data", exist_ok=True)

try:
    import osmnx as ox
    ox.settings.log_console = False
except ImportError:
    sys.exit("Installe osmnx : pip install osmnx")

# ══════════════════════════════════════════════════════════════════════════════
# DICTIONNAIRE DES 58 WILAYAS ALGÉRIENNES
# Clé : numéro de wilaya (int)
# Valeur : dict avec PLACE, BBOX, WILAYA, ZONE_CODE, wilaya_nom
# ══════════════════════════════════════════════════════════════════════════════

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

# ══════════════════════════════════════════════════════════════════════════════
# SÉLECTION DE LA WILAYA  ← changer juste ce chiffre
# ══════════════════════════════════════════════════════════════════════════════

WILAYA_INPUT = 31   # ← numéro de wilaya (1–58)

_w         = WILAYAS[WILAYA_INPUT]
PLACE      = _w["PLACE"]
BBOX       = _w["BBOX"]
WILAYA     = _w["WILAYA"]
ZONE_CODE  = _w["ZONE_CODE"]
wilaya_nom = _w["wilaya_nom"]

OPERATEURS = {
    "Djezzy":  ["077", "078"],
    "Ooredoo": ["066", "069", "079"],
    "Mobilis": ["055", "056", "057"]
}


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


COMMUNES_ORAN = charger_communes_oran()
QUARTIERS_ORAN = charger_quartiers_oran()

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ══════════════════════════════════════════════════════════════════════════════

ETAGES_DEFAUT = {
    "apartments": 6, "residential": 5, "yes": 5, "commercial": 2, "retail": 1, "dormitory": 5
}
ETAGES_GENERIQUE = np.random.randint(3, 11)
LOG_PAR_ETAGE    = np.random.randint(2, 8)

EXCLURE = {
    "garage","garages","shed","hut","industrial","warehouse",
    "school","church","mosque","hospital","kindergarten",
    "stadium","parking","service","roof","fence","wall",
    "government","civic","public",
}

# ====================== 100 PRÉNOMS ALGÉRIENS (mix hommes/femmes) ======================
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
    "Keltoum", "Lila", "Malika", "Naima", "Ouahiba", "Rym", "Siham", "Touria","Ikram"
]

# ====================== 100 NOMS DE FAMILLE ALGÉRIENS ======================
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

# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES GPS + FORMAT (inchangés)
# ══════════════════════════════════════════════════════════════════════════════

def haversine(la1, lo1, la2, lo2):
    R = 6371000
    la1,lo1,la2,lo2 = map(radians,[la1,lo1,la2,lo2])
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

def fmt_zone_id(seq): return f"Z{ZONE_CODE}-{seq:03d}"
def fmt_olt(seq, elot): return f"T{ZONE_CODE}-{seq:03d}-{elot}-AN6000-IN"
def fmt_fdt(olt_seq, fdt_seq): return f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}"
def fmt_spl1(fdt_nom, s): return f"{fdt_nom}-S{s:02d}"
def fmt_carte_nom(olt_nom, slot): return f"{olt_nom}_Frame:0/Slot:{slot}"
def fmt_carte_id(olt_seq, slot): return int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}")
def fmt_port_id(olt_seq, slot, p): return int(f"{ZONE_CODE}{olt_seq:03d}7{slot:02d}{p:02d}")
def fmt_port_nom(olt_nom, slot, p): return f"{olt_nom}-Frame:0/Slot:{slot}/Port:{p}"

def fmt_fat(olt_seq, fdt_seq, spl_seq, elot, portes, etage, num_fat):
    p = ",".join(str(x) for x in portes)
    return (f"F{ZONE_CODE}-{olt_seq:03d}-{fdt_seq:02d}-{spl_seq:02d}-"
            f"{elot}-Porte({p})-{etage}F-{num_fat}")

def fmt_code_client(seq): return f"1000000271{1200 + seq:04d}"

def resoudre_elot(row, bat_idx):
    nom = str(row.get("nom_bat", "")).strip()
    if nom and nom.lower() not in ("nan", "", "none"): return _nettoyer(nom)
    for col in ["addr:street", "addr_street"]:
        rue = str(row.get(col, "")).strip()
        if rue and rue.lower() not in ("nan", "", "none"): return _nettoyer(rue)
    for col in ["addr:suburb", "addr:quarter", "addr_suburb", "addr_quarter"]:
        quartier = str(row.get(col, "")).strip()
        if quartier and quartier.lower() not in ("nan", "", "none"): return _nettoyer(quartier)
    idx_quartier = bat_idx % len(QUARTIERS_ORAN)
    return QUARTIERS_ORAN[idx_quartier]

def _nettoyer(s):
    return (s.upper().strip().replace(" ", "-").replace(",", "").replace("'", "")
            .replace("/", "-").replace("(", "").replace(")", ""))[:35]

# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRE ADRESSE RÉELLE
# Structure réelle (table Excel) :
#   batiment_pav | voie | quartier | commune | wilaya | code_client
# ══════════════════════════════════════════════════════════════════════════════

def resoudre_voie(row):
    """Retourne la voie (rue/boulevard) depuis les tags OSM."""
    for col in ["addr:street", "addr_street"]:
        rue = str(row.get(col, "")).strip()
        if rue and rue.lower() not in ("nan", "", "none"):
            return rue.title()
    return ""

def resoudre_quartier_osm(row, bat_idx):
    """Retourne le quartier OSM du bâtiment."""
    for col in ["addr:suburb", "addr:quarter", "addr_suburb", "addr_quarter"]:
        q = str(row.get(col, "")).strip()
        if q and q.lower() not in ("nan", "", "none"):
            return q.upper()
    return QUARTIERS_ORAN[bat_idx % len(QUARTIERS_ORAN)].replace("-", " ")

# ══════════════════════════════════════════════════════════════════════════════
# LES 3 ÉTAPES
# ══════════════════════════════════════════════════════════════════════════════

def charger_batiments():
    print(f"\n[1/3] Chargement bâtiments OSM — {PLACE}")
    t0 = time.time()
    bats = ox.features_from_place(PLACE, tags={"building": True})
    print(f"  ✓ Chargé par nom ({time.time()-t0:.0f}s)")

    bats = bats[bats.geometry.geom_type.isin(["Polygon","MultiPolygon"])].copy()
    if "building" in bats.columns:
        bats = bats[~bats["building"].fillna("").isin(EXCLURE)].copy()

    centroids = bats.geometry.centroid
    bats["lat"] = centroids.y.round(6)
    bats["lon"] = centroids.x.round(6)
    bats["btype"] = bats.get("building", "unknown").fillna("unknown")
    bats["nom_bat"] = bats.get("name", "").fillna("")
    for col in ["addr:street", "addr:suburb", "addr:quarter"]:
        if col in bats.columns: bats[col] = bats[col].fillna("")
    if "building:levels" in bats.columns:
        bats["etages_osm"] = pd.to_numeric(bats["building:levels"], errors="coerce")
    else:
        bats["etages_osm"] = np.nan

    TYPES_COM = {"commercial","retail","office","shop","mixed","supermarket","hotel","bank","restaurant"}
    bats["commerce_rdc"] = bats["btype"].isin(TYPES_COM)
    masque_residentiel = (~bats["commerce_rdc"] & bats["btype"].isin(["apartments","residential","yes"]) & (bats["etages_osm"].fillna(5) >= 3))
    bats.loc[masque_residentiel, "commerce_rdc"] = np.random.random(masque_residentiel.sum()) < 0.50

    bats = bats.reset_index(drop=True)
    print(f"  → {len(bats):,} bâtiments résidentiels/mixtes")
    return bats

def estimer_logements(bats):
    print(f"\n[2/3] Estimation logements")
    nb_et_list, nb_log_list = [], []
    for _, row in bats.iterrows():
        btype = str(row.get("btype","unknown")).lower()
        nb_et = int(row["etages_osm"]) if pd.notna(row.get("etages_osm")) and row["etages_osm"] >= 1 else ETAGES_DEFAUT.get(btype, ETAGES_GENERIQUE)
        etages_log = (nb_et - 1) if row.get("commerce_rdc", False) else nb_et
        nb_log = max(1, etages_log) * LOG_PAR_ETAGE
        nb_et_list.append(nb_et)
        nb_log_list.append(nb_log)
    bats["nb_etages"] = nb_et_list
    bats["nb_logements"] = nb_log_list
    print(f"  → Total logements estimés : {sum(nb_log_list):,}")
    return bats

def estimer_logements(bats):
    print(f"\n[2/3] Estimation logements réalistes ")

    nb_et_list = []
    nb_log_list = []
    log_par_etage_list = []   # ← on garde aussi cette info utile

    for _, row in bats.iterrows():
        btype = str(row.get("btype", "unknown")).lower()

        # 1. Priorité : vraie valeur OSM si elle existe et est valide
        if pd.notna(row.get("etages_osm")) and row["etages_osm"] >= 1:
            nb_et = int(row["etages_osm"])
        else:
            # 2. Valeurs aléatoires réalistes selon le type de bâtiment
            nb_et = np.random.randint(4, 11)      # 4 à 10 étages (nouvelles résidences)

        # Commerce au RDC → on enlève 1 étage pour les logements
        etages_log = (nb_et - 1) if row.get("commerce_rdc", False) else nb_et
        etages_log = max(1, etages_log)

        # Nombre de logements par étage : aléatoire réaliste (3 à 6)
        log_par_etage = np.random.randint(3, 7)

        # Nombre total de logements
        nb_log = etages_log * log_par_etage

        nb_et_list.append(nb_et)
        nb_log_list.append(nb_log)
        log_par_etage_list.append(log_par_etage)

    # Ajout des colonnes dans le DataFrame
    bats["nb_etages"]      = nb_et_list
    bats["nb_logements"]   = nb_log_list
    bats["log_par_etage"]  = log_par_etage_list   # utile pour le K-Means plus tard

    # Statistiques pour vérifier que c’est bien aléatoire
    print(f" → {len(bats)} bâtiments traités")
    print(f" → Étages moyen     : {np.mean(nb_et_list):.1f} (min={min(nb_et_list)} | max={max(nb_et_list)})")
    print(f" → Log/étage moyen  : {np.mean(log_par_etage_list):.1f} (min=3 | max=6)")
    print(f" → Total logements estimés : {sum(nb_log_list):,}")

    return bats
def generer_tables(bats):
    print(f"\n[3/3] Génération tables AT")

    zones, equipements, cartes, ports = [], [], [], []
    fdts, spl1_rows, fats, spl2 = [], [], [], []
    clients, adresses, numeros, abonnes = [], [], [], []

    client_seq = 0
    numero_seq = 0

    for bat_idx, row in bats.iterrows():

        olt_seq = bat_idx + 1
        zone_id = fmt_zone_id(olt_seq)
        olt_lat = row["lat"]
        olt_lon = row["lon"]
        nb_et = int(row["nb_etages"])
        nb_log = int(row["nb_logements"])
        comm_rdc = bool(row.get("commerce_rdc", False))
        polygon = row.geometry

        elot = resoudre_elot(row, bat_idx)
        commune, cecli = commune_proche(olt_lat, olt_lon)

        # Résolution adresse réelle depuis OSM
        voie_osm     = resoudre_voie(row)
        quartier_osm = resoudre_quartier_osm(row, bat_idx)
        # batiment_pav = nom du bâtiment ou fallback sur l'îlot
        nom_bat = str(row.get("nom_bat", "")).strip()
        batiment_pav = nom_bat if nom_bat and nom_bat.lower() not in ("nan","","none") else elot.replace("-", " ").title()

        olt_nom = fmt_olt(olt_seq, elot)

        # ── ZONE ─────────────────────────────────────────────────────────────
        zones.append({
            "id": zone_id, "wilaya": WILAYA, "wilaya_nom": wilaya_nom,
            "commune": commune, "zone_geographique": cecli,
        })

        # ── ÉQUIPEMENT (OLT indoor au centroïde bâtiment = RDC) ──────────────
        equipements.append({
            "id": zone_id, "nom": olt_nom,
            "ip": f"100.{WILAYA}.{(olt_seq % 254) + 1}.{np.random.randint(1, 254)}",
            "type": np.random.choice(["FIBERHOME", "HUAWEI", "ZTE"], p=[0.5, 0.35, 0.15]),
            "latitude": olt_lat, "longitude": olt_lon,
        })

        # ── CARTE + PORT ──────────────────────────────────────────────────────
        for slot in [1, 2]:
            cartes.append({
                "id": fmt_carte_id(olt_seq, slot),
                "nom": fmt_carte_nom(olt_nom, slot),
                "id_equipement": zone_id,
                "position": f"0/{slot}", "type": "gpon", "rack": slot,
            })
            for p in range(1, 17):
                etat = "utilisé" if (slot == 1 and p <= 4) else "libre"
                ports.append({
                    "id": fmt_port_id(olt_seq, slot, p),
                    "nom": fmt_port_nom(olt_nom, slot, p),
                    "nomCarte": fmt_carte_nom(olt_nom, slot),
                    "position": p, "etat": etat, "zone_id": zone_id,
                })

        # ── FDT (séquences locales à cet OLT : 01, 02, 03...) ────────────────
        nb_fdts = np.random.randint(2, 5)
        fdts_bat = []
        for i in range(nb_fdts):
            angle = i * (360 / nb_fdts) + np.random.uniform(-15, 15)
            dist = np.random.uniform(80, 400)
            fl, flo = offset_gps(olt_lat, olt_lon, dist, angle)
            fdt_nom = fmt_fdt(olt_seq, i + 1)
            fdts_bat.append({
                "id": fdt_nom, "nom_equipement": olt_nom,
                "zone": zone_id, "latitude": fl, "longitude": flo,
                "distance_olt_m": round(dist),
            })
            fdts.append(fdts_bat[-1])

        fdt_ref = fdts_bat[0]
        fdt_nom = fdt_ref["id"]
        fdt_seq_num = int(fdt_nom.split("-")[-1])
        fdt_lat = fdt_ref["latitude"]
        fdt_lon = fdt_ref["longitude"]

        # ── SPLITTER N1 ────────────────────────────────────────────────────────
        nb_spl = np.random.randint(4, 14)
        port_ctr = 1
        for s in range(1, nb_spl + 1):
            spl_nom = fmt_spl1(fdt_nom, s)
            sl, slo = rand_offset(fdt_lat, fdt_lon, 0, 10)
            spl1_rows.append({
                "id": spl_nom,
                "nom_FDT": fdt_nom,
                "id_port": fmt_port_nom(olt_nom, 1, port_ctr),
                "latitude": sl, "longitude": slo,
                "zone_id": zone_id,
            })
            port_ctr += 1

        spl_seq_local = 1
        fat_num = 1
        porte_globale = 1

        # ── FAT Commerce RDC ──────────────────────────────────────────────────
        if comm_rdc:
            nb_com = np.random.randint(2, 5)
            portes_c = list(range(porte_globale, porte_globale + nb_com))
            porte_globale += nb_com
            fat_id = fmt_fat(olt_seq, fdt_seq_num, spl_seq_local,
                             elot, portes_c, 0, 1)
            fl, flo = rand_offset(olt_lat, olt_lon, 0, 15)
            nb_occ = np.random.randint(1, nb_com + 1)

            fats.append({
                "id": fat_id, "nom_FDT": fdt_nom, "num_de_groupe": 0,
                "latitude": fl, "longitude": flo,
                "usage": "commerces", "nb_ports": 8, "nb_abonnes_sim": nb_occ,
                "nb_etages_bat": nb_et, "nb_log_etage": LOG_PAR_ETAGE,
                "zone_id": zone_id,
                "distance_fdt_m": round(haversine(fl, flo, fdt_lat, fdt_lon)),
            })
            for dn in range(1, 9):
                spl2.append({
                    "id": f"{fat_id}-DOWN-{dn:02d}",
                    "nom_FAT": fat_id,
                    "id_splitter1": fmt_spl1(fdt_nom, spl_seq_local),
                    "rapport_de_division": "1:08",
                    "port_splitter": f"{fmt_spl1(fdt_nom, spl_seq_local)}-DOWN-{dn}",
                    "etat": "utilisé" if dn <= nb_occ else "libre",
                    "zone_id": zone_id,
                })
            spl_seq_local += 1
            fat_num += 1

        # ── FATs Logements ────────────────────────────────────────────────────
        etage = 1
        while etage <= nb_et:
            portes_groupe = []
            etage_debut = etage

            while etage <= nb_et and len(portes_groupe) < 8:
                places_restantes = 8 - len(portes_groupe)
                portes_etage_courant = min(LOG_PAR_ETAGE, places_restantes)
                for _ in range(portes_etage_courant):
                    portes_groupe.append(porte_globale)
                    porte_globale += 1
                etage += 1

            if not portes_groupe:
                break

            groupes = [portes_groupe[i:i + 8] for i in range(0, len(portes_groupe), 8)]

            for g_idx, groupe in enumerate(groupes):
                fat_id = fmt_fat(olt_seq, fdt_seq_num, spl_seq_local,
                                 elot, groupe, etage_debut, g_idx + 1)
                fl, flo = rand_offset(olt_lat, olt_lon, 0, 30)
                nb_occ = len(groupe)

                fats.append({
                    "id": fat_id, "nom_FDT": fdt_nom,
                    "num_de_groupe": fat_num,
                    "latitude": fl, "longitude": flo,
                    "usage": "logements", "nb_ports": 8, "nb_abonnes_sim": nb_occ,
                    "nb_etages_bat": nb_et, "nb_log_etage": LOG_PAR_ETAGE,
                    "zone_id": zone_id,
                    "distance_fdt_m": round(haversine(fl, flo, fdt_lat, fdt_lon)),
                })
                for dn in range(1, 9):
                    spl2.append({
                        "id": f"{fat_id}-DOWN-{dn:02d}",
                        "nom_FAT": fat_id,
                        "id_splitter1": fmt_spl1(fdt_nom, spl_seq_local),
                        "rapport_de_division": "1:08",
                        "port_splitter":
                            f"{fmt_spl1(fdt_nom, spl_seq_local)}-DOWN-{dn}",
                        "etat": "utilisé" if dn <= nb_occ else "libre",
                        "zone_id": zone_id,
                    })

                # Abonnés pour ce groupe de portes
                for porte in groupe:
                    cc = fmt_code_client(client_seq)
                    op = np.random.choice(list(OPERATEURS.keys()))
                    prefix = np.random.choice(OPERATEURS[op])
                    contact = int(prefix + f"{np.random.randint(1000000, 9999999):07d}")

                    pt = polygon.representative_point()

                    abonnes.append({
                        "code_client": cc,
                        "latitude": round(pt.y, 6),
                        "longitude": round(pt.x, 6),
                        "etage": etage_debut,
                        "porte": porte,
                        "id_batiment": f"BAT{bat_idx + 1:05d}",
                        "id_zone": zone_id,
                        "FAT_relative": fat_id,
                    })
                    clients.append({
                        "code_client": cc, "contact": contact,
                        "nom": f"{np.random.choice(PRENOMS)} {np.random.choice(NOMS_FAM)}",
                    })

                    # ── ADRESSE : structure réelle (comme table Excel) ────────
                    # batiment_pav | voie | quartier | commune | wilaya | code_client
                    adresses.append({
                        "code_client":  cc,
                        "batiment_pav": batiment_pav,
                        "voie":         voie_osm if voie_osm else f"FAT{ZONE_CODE}-{elot}",
                        "quartier":     quartier_osm,
                        "commune":      commune.replace("-", " "),
                        "wilaya":       wilaya_nom,
                    })

                    numeros.append({
                        "num_de_groupe": int(f"4{1800000 + numero_seq:07d}"),
                        "code_client": cc,
                        "region_relative": zone_id,
                        "FAT_relative": fat_id,
                    })
                    client_seq += 1
                    numero_seq += 1

                fat_num += 1

            spl_seq_local += 1

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
        "abonnes": pd.DataFrame(abonnes),
    }


if __name__ == "__main__":
    print("=" * 70)
    print(f"SIMULATION DONNÉES FTTH MASSIVE — ALGÉRIE TÉLÉCOM")
    print(f"→ Wilaya : {WILAYA} — {wilaya_nom}")
    print(f"→ Communes + Quartiers chargés AUTOMATIQUEMENT depuis OSM")
    print("=" * 70)

    bats = charger_batiments()
    bats = estimer_logements(bats)
    tables = generer_tables(bats)

    for nom, df in tables.items():
        path = os.path.join("datas", f"{nom}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  ✓ {nom:15s} {len(df):7d} lignes")

    fat = tables["fat"]
    print(f"\n✅ VÉRIFICATION FAT IDs :")
    print(fat["id"].head(8).tolist())
    print(f"   → Nombre total de FATs : {len(fat):,}")

    # Aperçu table adresse
    print(f"\n📋 Aperçu table ADRESSE (5 premières lignes) :")
    print(tables["adresse"].head(5).to_string(index=False))
    print(f"\nTerminé !")