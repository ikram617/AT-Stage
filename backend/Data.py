from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
import numpy as np
import os
import types
import sys
import asyncio
import json
import time
import re
import sqlite3
import threading
import unicodedata
from pathlib import Path
import httpx

# ====================== IMPORT MODELE ======================
config_module = types.ModuleType("config")
class _Settings:
    FAT_CAPACITY = 8
    TORTUOSITY_TRUNK = 1.3
    AT_DROP_CABLE_STANDARDS_M = [10, 15, 20, 30, 50, 100]
config_module.settings = _Settings()
sys.modules["config"] = config_module

from model2D import FATSmartPlanner
from id_generator import ATIDGenerator

# ====================== CACHE & DB ======================
_CACHE_DIR = Path("osm_cache")
_CACHE_DIR.mkdir(exist_ok=True)
_CACHE_DB = _CACHE_DIR / "osm_cache.db"
_DZ_ADMIN_DB = Path("dz_admin.db")

ox.settings.use_cache = True
ox.settings.cache_folder = str(_CACHE_DIR / "osmnx_http")
_db_lock = threading.Lock()

def _init_databases():
    with _db_lock:
        with sqlite3.connect(str(_CACHE_DB)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, ts REAL NOT NULL, category TEXT, parent_key TEXT)")
        with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS wilayas (code TEXT PRIMARY KEY, name TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS communes (code TEXT PRIMARY KEY, wilaya_code TEXT, name TEXT NOT NULL, FOREIGN KEY(wilaya_code) REFERENCES wilayas(code))")
            conn.execute("CREATE TABLE IF NOT EXISTS quartiers (id INTEGER PRIMARY KEY AUTOINCREMENT, commune_code TEXT, name TEXT NOT NULL, UNIQUE(commune_code, name))")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_com_w ON communes(wilaya_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_q_c ON quartiers(commune_code)")

_init_databases()

async def sync_dz_admin_db():
    try:
        with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
            if conn.execute("SELECT COUNT(*) FROM communes").fetchone()[0] >= 1541: return
    except: pass
    url = "https://raw.githubusercontent.com/Kenandarabeh/algeria-wilayas-communes-2026/main/wilayas.json"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(url)
            if r.status_code == 200:
                data = r.json()
                with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
                    conn.execute("DELETE FROM wilayas"); conn.execute("DELETE FROM communes")
                    for w in data:
                        wc, wn = str(w.get("code", "")).zfill(2), w.get("nom") or w.get("name", "")
                        conn.execute("INSERT INTO wilayas (code, name) VALUES (?, ?)", (wc, wn))
                        for c in w.get("communes", []):
                            cc, cn = c.get("code") or f"{wc}{str(c.get('id',''))[-3:].zfill(3)}", c.get("nom") or c.get("name", "")
                            conn.execute("INSERT OR IGNORE INTO communes (code, wilaya_code, name) VALUES (?, ?, ?)", (cc, wc, cn))
                print("✅ [DZ_ADMIN] Synchronisation terminée.")
    except Exception as e: print(f"WARNING [SYNC] {e}")

async def _sync_quartiers_osm(c_code: str, c_name: str, v_name: str):
    try:
        cp = c_name.split(' ')[0]
        q = f'[out:json][timeout:25];area["ISO3166-1"="DZ"]->.a;area[name~"^{cp}",i]["admin_level"="8"](.a)->.c;(node["place"~"suburb|quarter|neighbourhood"](area.c);way["landuse"="residential"](area.c););out tags;'
        async with httpx.AsyncClient(timeout=30.0) as cl:
            for srv in ["https://overpass-api.de/api/interpreter", "https://lz4.overpass-api.de/api/interpreter"]:
                try:
                    r = await cl.post(srv, data={"data": q})
                    if r.status_code == 200:
                        found = {f"{e['tags'].get('name:fr', e['tags'].get('name',''))} {e['tags'].get('name:ar','')}".strip() for e in r.json().get('elements',[]) if e['tags'].get('name')}
                        with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
                            for n in found: conn.execute("INSERT OR IGNORE INTO quartiers (commune_code, name) VALUES (?, ?)", (c_code, n))
                        break
                except: continue
    except: pass

# ====================== APP & MODELS ======================
app = FastAPI(); app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class ImportOSMRequest(BaseModel): ville: str; quartier: str; residence: str; nombre_etages: int = 8; logements_par_etage: int = 12
class FATPlacementRequest(BaseModel): subscribers: List[Dict[str, Any]]
class NamingFATRequest(BaseModel): fat_candidates: List[Dict[str, Any]]; subscribers: List[Dict[str, Any]]

# ====================== ENDPOINTS LOCALISATION ======================
@app.get("/api/ville")
async def get_villes():
    with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
        rows = conn.execute("SELECT code, name FROM wilayas ORDER BY code ASC").fetchall()
        return {"villes": [f"{r[0]} - {r[1]}" for r in rows]}

@app.get("/api/commune/{ville}")
async def get_communes(ville: str):
    wc = ville.split(' ')[0].zfill(2)
    with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
        rows = conn.execute("SELECT name FROM communes WHERE wilaya_code = ? ORDER BY name ASC", (wc,)).fetchall()
        return {"communes": [r[0] for r in rows]}

@app.get("/api/quartier")
async def get_quartiers(ville: str, commune: str, background_tasks: BackgroundTasks):
    wc = ville.split(' ')[0].zfill(2); cp = commune.split(' ')[0]
    with sqlite3.connect(str(_DZ_ADMIN_DB)) as conn:
        c_row = conn.execute("SELECT code FROM communes WHERE wilaya_code = ? AND name LIKE ?", (wc, f"%{cp}%")).fetchone()
        if not c_row: return {"quartiers": []}
        qs = [r[0] for r in conn.execute("SELECT name FROM quartiers WHERE commune_code = ? ORDER BY name ASC", (c_row[0],)).fetchall()]
        if not qs: background_tasks.add_task(_sync_quartiers_osm, c_row[0], commune, ville)
        return {"quartiers": qs}

@app.get("/api/residence")
async def get_residences(ville: str, commune: str, quartier: str):
    vp, qp = ville.split(' ')[0], quartier.split(' ')[0]
    async with httpx.AsyncClient(timeout=10.0) as cl:
        r = await cl.get("https://nominatim.openstreetmap.org/search", params={"q":f"{qp}, {vp}, Algeria", "format":"jsonv2", "limit":1})
        if not r.json(): return {"residences": []}
        bbox = r.json()[0].get('boundingbox')
        if not bbox: return {"residences": []}
        s, n, w, e = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
        vb = f"{w-0.005},{s-0.005},{e+0.005},{n+0.005}"
        names = set()
        for t in ["residence", "cite", "lotissement"]:
            r2 = await cl.get("https://nominatim.openstreetmap.org/search", params={"q":f"{t} {vp}", "format":"jsonv2", "limit":20, "viewbox":vb, "bounded":1})
            for it in r2.json():
                nm = it.get('display_name','').split(',')[0].strip()
                if nm: names.add(nm)
        return {"residences": sorted(list(names))}

# ====================== BUSINESS ENDPOINTS ======================
@app.post("/api/importOSM")
async def import_osm(req: ImportOSMRequest):
    vp, qp = req.ville.split(' ')[0], req.quartier.split(' ')[0]
    place = f"{req.residence}, {qp}, {vp}, Algeria"
    try:
        gdf = ox.features_from_place(place, tags={"building": True})
        if gdf.empty: gdf = ox.features_from_place(f"{qp}, {vp}, Algeria", tags={"building": True})
    except: raise HTTPException(status_code=400, detail="Zone introuvable")
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy().reset_index(drop=True)
    subs = []
    for i, b in gdf.iterrows():
        b_name = f"{vp}-{qp}-{req.residence}-B{i+1}"
        for j in range(req.nombre_etages * req.logements_par_etage):
            etg = (j // req.logements_par_etage) + 1
            subs.append({"code_client": f"AB{i:03d}{j:03d}", "id_batiment": b_name, "lat_abonne": b.geometry.centroid.y, "lon_abonne": b.geometry.centroid.x, "etage": etg, "porte": etg*100+(j%req.logements_par_etage)+1, "usage": "logements"})
    return {"buildings_geojson": gdf.to_json(), "subscribers": subs, "count": len(gdf)}

@app.post("/api/emplacementFATs")
async def sectorize(req: FATPlacementRequest):
    planner = FATSmartPlanner(); planner.fit(pd.DataFrame(req.subscribers))
    res = []
    for r in planner.results_:
        for f in r.fat_candidates:
            res.append({"id_batiment": r.id_batiment, "fat_id": f.fat_id, "centroid_lat": f.centroid_lat, "centroid_lon": f.centroid_lon, "n_subscribers": f.n_subscribers, "capacity_ok": f.capacity_ok, "cable_m_to_fdt_real": f.cable_m_to_fdt_real, "subscriber_ids": f.subscriber_ids})
    return {"fat_candidates": res}

@app.post("/api/nomFAT")
async def naming(req: NamingFATRequest):
    gen = ATIDGenerator(wilaya_code="310"); dat = req.fat_candidates
    for i, d in enumerate(dat): d["fat_id_AT"] = f"FAT-AT-{d['id_batiment'][-4:]}-{i+1}"
    return {"fat_candidates_with_ids": dat}

@app.on_event("startup")
async def startup(): asyncio.create_task(sync_dz_admin_db())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)