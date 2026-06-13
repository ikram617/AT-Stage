"""
save_project.py — FastAPI router for saving & loading FAT Smart Planner projects.

Mount in app.py:
    from save_project import router as project_router
    app.include_router(project_router)

Requires:
    pip install supabase

Environment variables (or hard-code for dev):
    SUPABASE_URL  — your project URL
    SUPABASE_KEY  — service-role key (NOT the anon key — backend needs write access)
"""

import os
import asyncio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client

# ── Supabase client ──────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kzdxrojsvfgyuojxzgnu.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt6ZHhyb2pzdmZneXVvanh6Z251Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzY2NTQ4NCwiZXhwIjoyMDkzMjQxNDg0fQ.dn6aZDMyLxLokHJXon4iHsa7Ok_1uOMvrNhxC-f7ToQ")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
#  PYDANTIC INPUT MODELS
# ══════════════════════════════════════════════════════════════════════════════

class ImmeubleIn(BaseModel):
    osm_id: Optional[int] = None
    lat: float
    lon: float
    etage: int
    logement_par_etage: int
    presence_commerce: bool = False
    wilaya: Optional[str] = None
    commune: Optional[str] = None
    hauteur_etage: Optional[float] = None
    nbr_abonne: Optional[int] = None
    source_type: Optional[str] = "osm"

class FATIn(BaseModel):
    fat_id_AT: Optional[str] = None
    lat: float
    lon: float
    etage_fat: Optional[int] = None
    usage: Optional[str] = None
    cable_snap_m: Optional[float] = None
    capacite: Optional[int] = 8
    subscriber_ids: Optional[List[str]] = []

class AbonneIn(BaseModel):
    code_client: str
    lat: float
    lon: float
    etage: int
    porte: int
    nom: Optional[str] = None
    usage: Optional[str] = "résidentiel"

class FatAbonneIn(BaseModel):
    fat_id_AT: str
    code_client: str
    distance_real_m: Optional[float] = None
    cable_snap_m: Optional[float] = None

class SaveProjectRequest(BaseModel):
    user_id: int
    nom_projet: str
    planning_mode: str
    wilaya: Optional[str] = None
    commune: Optional[str] = None
    immeubles: List[ImmeubleIn] = []
    fats: List[FATIn] = []
    abonnes: List[AbonneIn] = []
    fat_abonnes: List[FatAbonneIn] = []

class SaveProjectResponse(BaseModel):
    success: bool
    id_projet: int
    message: str


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_ALLOWED_SNAPS = [15, 20, 50, 80]

def _normalize_snap(val: Optional[float]) -> Optional[float]:
    if val is None:
        return None
    return min(_ALLOWED_SNAPS, key=lambda x: abs(x - val))

def _normalize_fat_usage(usage: Optional[str]) -> str:
    if usage in ("commerces", "commercial"):
        return "commercial"
    if usage == "mixte":
        return "mixte"
    return "résidentiel"


# ══════════════════════════════════════════════════════════════════════════════
#  POST /api/save-project
#  All Supabase calls run in asyncio.to_thread() to avoid blocking the event loop
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/api/save-project", response_model=SaveProjectResponse)
async def save_project(body: SaveProjectRequest):
    print(f"POST /api/save-project — '{body.nom_projet}' "
          f"({len(body.fats)} FATs, {len(body.abonnes)} abonnés)")

    # ── 1. Create projet ─────────────────────────────────────────────────────
    def _insert_projet():
        return supabase.table("projets").insert({
            "id_utilisateur": body.user_id,
            "nom_projet":     body.nom_projet,
            "planning_mode":  body.planning_mode,
            "wilaya":         body.wilaya,
            "commune":        body.commune,
        }).execute()

    projet_res = await asyncio.to_thread(_insert_projet)

    if not projet_res.data:
        raise HTTPException(status_code=500, detail="Échec création projet dans la base de données")

    id_projet: int = projet_res.data[0]["id_projet"]
    print(f"  ✓ Projet créé — id_projet={id_projet}")

    # ── 2. Insert immeubles ───────────────────────────────────────────────────
    immeuble_id_map: dict[int, int] = {}
    first_immeuble_id: Optional[int] = None

    for idx, imm in enumerate(body.immeubles):
        wkt = f"SRID=4326;POINT({imm.lon} {imm.lat})"

        def _insert_imm(i=imm, w=wkt):
            return supabase.rpc("insert_immeuble", {
                "p_id_projet":          id_projet,
                "p_geom_wkt":           w,
                "p_etage":              i.etage,
                "p_logement_par_etage": i.logement_par_etage,
                "p_presence_commerce":  i.presence_commerce,
                "p_wilaya":             i.wilaya,
                "p_commune":            i.commune,
                "p_hauteur_etage":      i.hauteur_etage,
                "p_nbr_abonne":         i.nbr_abonne,
                "p_osm_id":             i.osm_id,
                "p_source_type":        i.source_type or "osm",
            }).execute()

        res = await asyncio.to_thread(_insert_imm)

        if not res.data:
            raise HTTPException(status_code=500, detail=f"Échec insertion immeuble #{idx}")

        db_id: int = res.data[0]["id_immeuble"]
        immeuble_id_map[idx] = db_id
        if first_immeuble_id is None:
            first_immeuble_id = db_id

    if first_immeuble_id is None and body.fats:
        raise HTTPException(status_code=400, detail="FATs fournis mais aucun immeuble dans la requête")

    print(f"  ✓ {len(immeuble_id_map)} immeuble(s) insérés")

    # ── 3. Insert fats ────────────────────────────────────────────────────────
    fat_id_map: dict[str, int] = {}

    for fat in body.fats:
        wkt = f"SRID=4326;POINT({fat.lon} {fat.lat})"

        def _insert_fat(f=fat, w=wkt):
            return supabase.rpc("insert_fat", {
                "p_id_immeuble":  first_immeuble_id,
                "p_at_id":        f.fat_id_AT,
                "p_geom_wkt":     w,
                "p_etage_fat":    f.etage_fat,
                "p_usage":        _normalize_fat_usage(f.usage),
                "p_cable_snap_m": _normalize_snap(f.cable_snap_m),
                "p_capacite":     f.capacite or 8,
            }).execute()

        res = await asyncio.to_thread(_insert_fat)

        if not res.data:
            raise HTTPException(status_code=500, detail=f"Échec insertion FAT {fat.fat_id_AT}")

        db_fat_id: int = res.data[0]["id_fat"]
        if fat.fat_id_AT:
            fat_id_map[fat.fat_id_AT] = db_fat_id

        for code_client in (fat.subscriber_ids or []):
            if code_client and code_client not in [fa.code_client for fa in body.fat_abonnes]:
                body.fat_abonnes.append(FatAbonneIn(
                    fat_id_AT=fat.fat_id_AT or "",
                    code_client=code_client,
                    cable_snap_m=fat.cable_snap_m,
                ))

    print(f"  ✓ {len(fat_id_map)} FAT(s) insérés")

    # ── 4. Insert abonnes ─────────────────────────────────────────────────────
    abonne_fat_lookup: dict[str, Optional[int]] = {}
    for fa in body.fat_abonnes:
        abonne_fat_lookup[fa.code_client] = fat_id_map.get(fa.fat_id_AT)

    ab_ok = 0
    for ab in body.abonnes:
        wkt = f"SRID=4326;POINT({ab.lon} {ab.lat})" if (ab.lat and ab.lon) else None
        id_fat_db = abonne_fat_lookup.get(ab.code_client)

        def _insert_ab(a=ab, w=wkt, fid=id_fat_db):
            return supabase.rpc("insert_abonne", {
                "p_code_client": a.code_client,
                "p_id_fat":      fid,
                "p_geom_wkt":    w,
                "p_etage":       a.etage,
                "p_porte":       a.porte,
                "p_nom":         a.nom,
                "p_usage":       a.usage or "résidentiel",
            }).execute()

        res = await asyncio.to_thread(_insert_ab)
        if res.data:
            ab_ok += 1

    print(f"  ✓ {ab_ok}/{len(body.abonnes)} abonné(s) insérés/mis à jour")

    # ── 5. Insert fat_abonnes junction ────────────────────────────────────────
    fa_rows = []
    seen = set()

    for fa in body.fat_abonnes:
        id_fat_db = fat_id_map.get(fa.fat_id_AT)
        if not id_fat_db:
            continue
        key = (id_fat_db, fa.code_client)
        if key in seen:
            continue
        seen.add(key)
        fa_rows.append({
            "id_fat":          id_fat_db,
            "code_client":     fa.code_client,
            "distance_real_m": fa.distance_real_m,
            "cable_snap_m":    _normalize_snap(fa.cable_snap_m),
        })

    if fa_rows:
        BATCH = 500
        for i in range(0, len(fa_rows), BATCH):
            batch = fa_rows[i:i + BATCH]
            await asyncio.to_thread(
                lambda b=batch: supabase.table("fat_abonnes").upsert(
                    b, on_conflict="id_fat,code_client"
                ).execute()
            )

    print(f"  ✓ {len(fa_rows)} liaison(s) FAT↔Abonné insérées")
    print(f"  ✅ Projet '{body.nom_projet}' sauvegardé (id={id_projet})")

    return SaveProjectResponse(
        success=True,
        id_projet=id_projet,
        message=f"Projet '{body.nom_projet}' sauvegardé avec {len(body.fats)} FATs et {len(body.abonnes)} abonnés"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  GET /api/projects?user_id=1
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/projects")
async def list_projects(user_id: int):
    def _fetch():
        return supabase.table("projets").select(
            "id_projet, nom_projet, planning_mode, wilaya, commune, date_creation, date_modification"
        ).eq("id_utilisateur", user_id).order("date_modification", desc=True).execute()

    res = await asyncio.to_thread(_fetch)

    if res.data is None:
        raise HTTPException(status_code=500, detail="Impossible de charger les projets")

    projects = []
    for p in res.data:
        fat_count = 0
        ab_count = 0
        try:
            def _counts(pid=p["id_projet"]):
                imm_res = supabase.table("immeubles").select("id_immeuble").eq("id_projet", pid).execute()
                imm_ids = [r["id_immeuble"] for r in (imm_res.data or [])]
                fc, ac = 0, 0
                if imm_ids:
                    fat_res = supabase.table("fats").select("id_fat", count="exact").in_("id_immeuble", imm_ids).execute()
                    fc = fat_res.count or 0
                    fat_ids = [r["id_fat"] for r in (fat_res.data or [])]
                    if fat_ids:
                        ab_res = supabase.table("fat_abonnes").select("id", count="exact").in_("id_fat", fat_ids).execute()
                        ac = ab_res.count or 0
                return fc, ac

            fat_count, ab_count = await asyncio.to_thread(_counts)
        except Exception:
            pass

        projects.append({**p, "nb_fats": fat_count, "nb_abonnes": ab_count})

    return {"projects": projects}


# ══════════════════════════════════════════════════════════════════════════════
#  GET /api/projects/{id_projet}
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/projects/{id_projet}")
async def load_project(id_projet: int):
    def _fetch_projet():
        return supabase.table("projets").select("*").eq("id_projet", id_projet).single().execute()

    proj_res = await asyncio.to_thread(_fetch_projet)
    if not proj_res.data:
        raise HTTPException(status_code=404, detail=f"Projet {id_projet} introuvable")

    projet = proj_res.data

    def _fetch_immeubles():
        return supabase.table("immeubles").select("*").eq("id_projet", id_projet).execute()

    imm_res = await asyncio.to_thread(_fetch_immeubles)
    immeubles = imm_res.data or []

    if not immeubles:
        raise HTTPException(status_code=404, detail=f"Aucun immeuble pour le projet {id_projet}")

    first_imm = immeubles[0]
    imm_ids = [i["id_immeuble"] for i in immeubles]

    etages            = first_imm.get("etage", 5)
    logements         = first_imm.get("logement_par_etage", 4)
    presence_commerce = first_imm.get("presence_commerce", False)
    hauteur_etage     = first_imm.get("hauteur_etage", 3.0)

    def _fetch_fats():
        res = supabase.rpc("get_fats_with_coords", {"p_immeuble_ids": imm_ids}).execute()
        if not res.data:
            res = supabase.table("fats").select(
                "id_fat, at_id, etage_fat, usage, cable_snap_m, capacite, id_immeuble"
            ).in_("id_immeuble", imm_ids).execute()
        return res

    fat_res = await asyncio.to_thread(_fetch_fats)
    fats_db = fat_res.data or []
    fat_id_to_at: dict[int, str] = {f["id_fat"]: f.get("at_id", "") for f in fats_db}
    fat_ids = [f["id_fat"] for f in fats_db]

    fa_rows = []
    if fat_ids:
        def _fetch_fa():
            return supabase.table("fat_abonnes").select(
                "id_fat, code_client, distance_real_m, cable_snap_m"
            ).in_("id_fat", fat_ids).execute()

        fa_res = await asyncio.to_thread(_fetch_fa)
        fa_rows = fa_res.data or []

    fat_subscribers: dict[int, list[str]] = {f["id_fat"]: [] for f in fats_db}
    for fa in fa_rows:
        if fa["id_fat"] in fat_subscribers:
            fat_subscribers[fa["id_fat"]].append(fa["code_client"])

    all_codes = list({fa["code_client"] for fa in fa_rows})
    abonnes_db = []
    if all_codes:
        BATCH = 400
        for i in range(0, len(all_codes), BATCH):
            batch_codes = all_codes[i:i + BATCH]

            def _fetch_ab(codes=batch_codes):
                res = supabase.rpc("get_abonnes_with_coords", {"p_codes": codes}).execute()
                if res.data:
                    return res.data
                fb = supabase.table("abonnes").select(
                    "code_client, id_fat, etage, porte, nom, usage"
                ).in_("code_client", codes).execute()
                return fb.data or []

            batch_data = await asyncio.to_thread(_fetch_ab)
            abonnes_db.extend(batch_data)

    fat_results = []
    for f in fats_db:
        fid = f["id_fat"]
        subs = fat_subscribers.get(fid, [])
        usage_fe = "commerces" if f.get("usage") == "commercial" else "logements"
        fat_results.append({
            "fat_id":         f.get("at_id", f"FAT-{fid}"),
            "fat_id_AT":      f.get("at_id", ""),
            "id_fat_db":      fid,
            "centroid_lat":   f.get("lat", first_imm.get("centroid_lat", 0)),
            "centroid_lon":   f.get("lon", first_imm.get("centroid_lon", 0)),
            "etage_fat":      f.get("etage_fat", 1),
            "usage":          usage_fe,
            "cable_snap_m":   f.get("cable_snap_m"),
            "capacite":       f.get("capacite", 8),
            "subscriber_ids": subs,
            "n_subscribers":  len(subs),
            "capacity_ok":    len(subs) <= f.get("capacite", 8),
        })

    subscribers_data = []
    for ab in abonnes_db:
        subscribers_data.append({
            "code_client": ab["code_client"],
            "lat_abonne":  ab.get("lat", 0),
            "lon_abonne":  ab.get("lon", 0),
            "etage":       ab.get("etage", 1),
            "porte":       ab.get("porte", 1),
            "nom":         ab.get("nom", ""),
            "usage":       "commerces" if ab.get("usage") == "commercial" else "logements",
        })

    total_abonnes = len(subscribers_data)
    total_fats    = len(fat_results)
    total_ports   = sum(f.get("capacite", 8) for f in fat_results) or 1
    lineaire      = sum(fa.get("cable_snap_m") or 0 for fa in fa_rows)
    kpis = {
        "totalAbonnes":  total_abonnes,
        "fatsNeeded":    total_fats,
        "fatsPortsUsed": round(total_abonnes / total_ports * 100) if total_ports else 0,
        "lineaire":      round(lineaire),
    }

    planning_mode_fe = (
        "prediction" if projet.get("planning_mode") == "Réseau estimé"
        else "subscriber"
    )

    return {
        "ville":   projet.get("wilaya", ""),
        "commune": projet.get("commune", ""),
        "residenceObj": {
            "name":   projet.get("nom_projet", ""),
            "osm_id": first_imm.get("osm_id"),
            "lat":    first_imm.get("lat"),
            "lon":    first_imm.get("lon"),
        },
        "etages":             etages,
        "logements":          logements,
        "hauteurEtage":       hauteur_etage,
        "presenceCommercial": presence_commerce,
        "planningMode":       planning_mode_fe,
        "kpis":               kpis,
        "sectorisationSnapshot": {
            "fatResults":         fat_results,
            "subscribersData":    subscribers_data,
            "etages":             etages,
            "logements":          logements,
            "residenceName":      projet.get("nom_projet", ""),
            "presenceCommercial": presence_commerce,
            "planningMode":       planning_mode_fe,
            "timestamp":          0,
        },
        "id_projet":     id_projet,
        "nom_projet":    projet.get("nom_projet"),
        "date_creation": projet.get("date_creation"),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  DELETE /api/projects/{id_projet}
# ══════════════════════════════════════════════════════════════════════════════

@router.delete("/api/projects/{id_projet}")
async def delete_project(id_projet: int):
    def _delete():
        return supabase.table("projets").delete().eq("id_projet", id_projet).execute()

    res = await asyncio.to_thread(_delete)
    if not res.data:
        raise HTTPException(status_code=404, detail=f"Projet {id_projet} introuvable")
    return {"success": True, "message": f"Projet {id_projet} supprimé"}