"""
SIMULATION DONNÉES FTTH — ALGÉRIE TÉLÉCOM (v3 OSM DYNAMIQUE + MASSIVE)
=========================================================
→ Tous les bâtiments résidentiels d’Oran (données massives)
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
# PARAMÈTRES
# ══════════════════════════════════════════════════════════════════════════════

"""PLACE = "Oran, Algeria"
BBOX  = {"north": 35.760, "south": 35.620, "east": -0.520, "west": -0.730}
WILAYA    = 31
ZONE_CODE = "310"  
wilaya_nom= "Oran" """
PLACE = "Tizi Ouzou, Algeria"
wilaya_nom="Tizi Ouzou"
BBOX = {
    "north": 36.740,
    "south": 36.690,
    "east": 4.090,
    "west": 4.000
}

WILAYA = 15
ZONE_CODE = "150"
OPERATEURS = {
        "Djezzy":  ["077", "078"],
        "Ooredoo": ["066", "069", "079"],
        "Mobilis": ["055", "056", "057"]
    }
# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT DYNAMIQUE COMMUNES + QUARTIERS DEPUIS OSM
# ══════════════════════════════════════════════════════════════════════════════

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
# CONSTANTES (ajoutées pour que le code soit complet)
# ══════════════════════════════════════════════════════════════════════════════

ETAGES_DEFAUT = {
    "apartments": 6, "residential": 5, "yes": 5, "house": 1,
    "detached": 1, "commercial": 2, "retail": 1, "dormitory": 5
}
ETAGES_GENERIQUE = 5
LOG_PAR_ETAGE    = 4

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
# LES 3 ÉTAPES (charger_batiments, estimer_logements, generer_tables)
# ══════════════════════════════════════════════════════════════════════════════

# (copie exacte de tes fonctions précédentes avec la seule correction : séquences locales)

def charger_batiments():
    # ... (ton code original complet – je l’ai gardé tel quel)
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
    # ... (ton code original)
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
            fdt_nom = fmt_fdt(olt_seq, i + 1)  # séquence locale : 01, 02, 03...
            fdts_bat.append({
                "id": fdt_nom, "nom_equipement": olt_nom,
                "zone": zone_id, "latitude": fl, "longitude": flo,
                "distance_olt_m": round(dist),
            })
            fdts.append(fdts_bat[-1])

        fdt_ref = fdts_bat[0]
        fdt_nom = fdt_ref["id"]
        fdt_seq_num = int(fdt_nom.split("-")[-1])  # toujours 01 pour fdt_ref
        fdt_lat = fdt_ref["latitude"]
        fdt_lon = fdt_ref["longitude"]

        # ── SPLITTER N1 (séquences physiques dans le FDT, indépendant de K-Means)
        # spl_seq = séquence de l'équipement splitter N1 dans le FDT
        # Complètement indépendant du nombre de FATs ou des clusters K-Means
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

        # spl_seq_local pour les FATs — séquence indépendante
        # Un splitter N1 par groupe de FATs, pas par cluster K-Means
        spl_seq_local = 1
        fat_num = 1
        porte_globale = 1  # portes numérotées globalement dans tout le bâtiment

        # ── FAT Commerce RDC → étage 0, numéro 1, format 0F-1 ────────────────
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

        # ── FATs Logements — CORRECTION BUG BOUCLE ───────────────────────────
        # Règle : 1 FAT = max 8 abonnés
        # On parcourt les étages un par un, on accumule les portes
        # Quand on atteint 8 portes OU le dernier étage → on crée la FAT
        # IMPORTANT : etage est incrémenté APRÈS chaque groupe, pas dedans

        etage = 1
        while etage <= nb_et:

            # Construire un groupe de max 8 portes en cumulant les étages
            portes_groupe = []
            etage_debut = etage  # étage de référence pour le nom FAT

            while etage <= nb_et and len(portes_groupe) < 8:
                # Ajouter les portes de cet étage (max 8 au total)
                places_restantes = 8 - len(portes_groupe)
                portes_etage_courant = min(LOG_PAR_ETAGE, places_restantes)

                for _ in range(portes_etage_courant):
                    portes_groupe.append(porte_globale)
                    porte_globale += 1

                etage += 1  # ← toujours incrémenté, même si on a rempli le groupe

            if not portes_groupe:
                break

            # Diviser en groupes de 8 (normalement déjà <= 8, mais sécurité)
            groupes = [portes_groupe[i:i + 8] for i in range(0, len(portes_groupe), 8)]

            for g_idx, groupe in enumerate(groupes):
                fat_id = fmt_fat(olt_seq, fdt_seq_num, spl_seq_local,
                                 elot, groupe, etage_debut, g_idx + 1)
                fl, flo = rand_offset(olt_lat, olt_lon, 0, 30)
                nb_occ = len(groupe)  # tous les abonnés sont occupés

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

                    # Position GPS abonné dans le polygone du bâtiment
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
                    adresses.append({
                        "code_client": cc,
                        "adresse": f"FAT08-{elot}-V{np.random.randint(10, 99)}",
                        "adresse_normalisee":
                            f"{elot.replace('-', ' ')}, "
                            f"{commune.replace('-', ' ')}, ORAN",
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
    print("SIMULATION DONNÉES FTTH MASSIVE — ALGÉRIE TÉLÉCOM")
    print("→ Communes + Quartiers chargés AUTOMATIQUEMENT depuis OSM")
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
    print(f"   → Format correct : {fat['id'].str.match(r'^F310-\\d{{3}}-\\d{{2}}-\\d{{2}}-').all()}")
    print(f"   → Nombre total de FATs : {len(fat):,}")
    print(f"\nTerminé !")