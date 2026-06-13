/**
 * projectService.js — Save & Load FAT Smart Planner projects via Supabase.
 */

import { supabase } from "./supabase";

const ALLOWED_SNAPS = [15, 20, 50, 80];

function normalizeSnap(val) {
  if (val == null) return null;
  const n = Number(val);
  return ALLOWED_SNAPS.reduce((prev, cur) =>
    Math.abs(cur - n) < Math.abs(prev - n) ? cur : prev
  );
}

function toFeUsage(dbUsage) {
  if (dbUsage === "commercial" || dbUsage === "commerces") return "commerces";
  return "logements";
}

function toDbFatUsage(feUsage) {
  if (feUsage === "commerces" || feUsage === "commercial") return "commercial";
  if (feUsage === "mixte") return "mixte";
  return "résidentiel";
}

function toDbAbUsage(feUsage) {
  if (feUsage === "commerces" || feUsage === "commercial") return "commercial";
  return "résidentiel";
}

/**
 * Convertit un objet GeoJSON Geometry en chaîne WKT (Well-Known Text)
 */
function toWKT(geom) {
  if (!geom) return null;
  if (typeof geom === "string") return geom; // Déjà WKT
  if (geom.type === "Point") {
    return `POINT(${geom.coordinates[0]} ${geom.coordinates[1]})`;
  }
  if (geom.type === "Polygon") {
    const rings = geom.coordinates.map(ring => 
      "(" + ring.map(p => `${p[0]} ${p[1]}`).join(", ") + ")"
    ).join(", ");
    return `POLYGON(${rings})`;
  }
  if (geom.type === "MultiPolygon") {
    const polys = geom.coordinates.map(poly => 
      "(" + poly.map(ring => "(" + ring.map(p => `${p[0]} ${p[1]}`).join(", ") + ")").join(", ") + ")"
    ).join(", ");
    return `MULTIPOLYGON(${polys})`;
  }
  return null;
}

function parseWKT(wkt) {
  if (!wkt || typeof wkt !== "string") return null;
  const cleaned = wkt.trim().toUpperCase();
  
  if (cleaned.startsWith("POINT")) {
    const coordsMatch = cleaned.match(/POINT\s*\(\s*([^\)]+)\s*\)/);
    if (coordsMatch) {
      const parts = coordsMatch[1].trim().split(/\s+/);
      return {
        type: "Point",
        coordinates: [parseFloat(parts[0]), parseFloat(parts[1])]
      };
    }
  }
  
  if (cleaned.startsWith("POLYGON")) {
    const ringsStr = cleaned.slice(cleaned.indexOf("(") + 1, cleaned.lastIndexOf(")"));
    const rings = [];
    let currentRing = "";
    let parenDepth = 0;
    for (let i = 0; i < ringsStr.length; i++) {
      const char = ringsStr[i];
      if (char === '(') {
        parenDepth++;
        if (parenDepth === 1) continue;
      }
      if (char === ')') {
        parenDepth--;
        if (parenDepth === 0) {
          rings.push(currentRing.trim());
          currentRing = "";
          continue;
        }
      }
      if (parenDepth > 0) {
        currentRing += char;
      }
    }
    const coordinates = rings.map(ring => {
      return ring.split(",").map(p => {
        const parts = p.trim().split(/\s+/);
        return [parseFloat(parts[0]), parseFloat(parts[1])];
      });
    });
    return {
      type: "Polygon",
      coordinates
    };
  }
  
  if (cleaned.startsWith("MULTIPOLYGON")) {
    const polysStr = cleaned.slice(cleaned.indexOf("(") + 1, cleaned.lastIndexOf(")"));
    const polygons = [];
    let currentPoly = "";
    let parenDepth = 0;
    for (let i = 0; i < polysStr.length; i++) {
      const char = polysStr[i];
      if (char === '(') {
        parenDepth++;
        if (parenDepth === 1) continue;
      }
      if (char === ')') {
        parenDepth--;
        if (parenDepth === 0) {
          polygons.push(currentPoly.trim());
          currentPoly = "";
          continue;
        }
      }
      if (parenDepth > 0) {
        currentPoly += char;
      }
    }
    const coordinates = polygons.map(poly => {
      const rings = [];
      let currentRing = "";
      let ringDepth = 0;
      for (let i = 0; i < poly.length; i++) {
        const char = poly[i];
        if (char === '(') {
          ringDepth++;
          if (ringDepth === 1) continue;
        }
        if (char === ')') {
          ringDepth--;
          if (ringDepth === 0) {
            rings.push(currentRing.trim());
            currentRing = "";
            continue;
          }
        }
        if (ringDepth > 0) {
          currentRing += char;
        }
      }
      return rings.map(ring => {
        return ring.split(",").map(p => {
          const parts = p.trim().split(/\s+/);
          return [parseFloat(parts[0]), parseFloat(parts[1])];
        });
      });
    });
    return {
      type: "MultiPolygon",
      coordinates
    };
  }
  
  return null;
}

/**
 * Parsing PostGIS Geometry (Point ou Polygon)
 * Gère le GeoJSON (objet) ou le WKT (string "POINT(lon lat)")
 */
function parseGeom(geom) {
  if (!geom) return { lat: 0, lon: 0, type: null, coordinates: null };
  if (typeof geom === "object") {
    if (geom.type === "Point" && Array.isArray(geom.coordinates)) {
      return { lat: geom.coordinates[1], lon: geom.coordinates[0], type: "Point", coordinates: geom.coordinates };
    }
    if (geom.type === "Polygon" || geom.type === "MultiPolygon") {
      return { type: geom.type, coordinates: geom.coordinates, lat: 0, lon: 0 };
    }
  }
  if (typeof geom === "string") {
    if (geom.startsWith("{")) {
      try {
        const parsed = JSON.parse(geom);
        return parseGeom(parsed);
      } catch (e) { /* ignore */ }
    }
    const parsed = parseWKT(geom);
    if (parsed) {
      if (parsed.type === "Point") {
        return { lat: parsed.coordinates[1], lon: parsed.coordinates[0], type: "Point", coordinates: parsed.coordinates };
      }
      return { type: parsed.type, coordinates: parsed.coordinates, lat: 0, lon: 0 };
    }
  }
  return { lat: 0, lon: 0, type: null };
}

// ══════════════════════════════════════════════════════════════════════════════
//  SAVE PROJECT
// ══════════════════════════════════════════════════════════════════════════════

export async function saveProject({
  userId, nomProjet, planningMode, ville, commune,
  immeubles, fats, abonnes, fatAbonnes
}) {
  console.log(`💾 Sauvegarde "${nomProjet}" — ${fats.length} FATs / ${abonnes.length} abonnés`);

  // ── 0. Overwrite if project already exists ────────────────────────────────
  const { data: existingProject } = await supabase
    .from("projets")
    .select("id_projet")
    .eq("nom_projet", nomProjet)
    .eq("id_utilisateur", userId)
    .maybeSingle();

  if (existingProject) {
    console.log(`⚠️ Projet "${nomProjet}" existant (ID: ${existingProject.id_projet}). Écrasement...`);
    const { error: delErr } = await supabase
      .from("projets")
      .delete()
      .eq("id_projet", existingProject.id_projet);
    if (delErr) throw new Error(`Erreur lors de la suppression de l'ancien projet: ${delErr.message}`);
  }

  // ── 1. Projet ─────────────────────────────────────────────────────────────
  const { data: projetData, error: projetErr } = await supabase
    .from("projets")
    .insert({
      id_utilisateur: userId,
      nom_projet: nomProjet,
      planning_mode: planningMode === "prediction" ? "Réseau estimé" : "Réseau connecté",
      wilaya: ville || null,
      commune: commune || null,
    })
    .select("id_projet")
    .single();

  if (projetErr) throw new Error(`Projet insert: ${projetErr.message}`);
  const idProjet = projetData.id_projet;
  console.log(`  ✓ Projet #${idProjet}`);

  // ── 2. Immeuble (sans geom) ───────────────────────────────────────────────
  const imm = immeubles[0];
  const { data: immData, error: immErr } = await supabase
    .from("immeubles")
    .insert({
      id_projet: idProjet,
      etage: imm.etage,
      logement_par_etage: imm.logement_par_etage,
      presence_commerce: imm.presence_commerce ?? false,
      wilaya: imm.wilaya || null,
      commune: imm.commune || null,
      geom: imm.lon && imm.lat ? `POINT(${imm.lon} ${imm.lat})` : null,
      geom_poly: toWKT(imm.geom_poly),
      hauteur_etage: imm.hauteur_etage || null,
      nbr_abonne: imm.nbr_abonne || null,
      osm_id: imm.osm_id || null,
      source_type: imm.source_type || "osm",
    })
    .select("id_immeuble")
    .single();

  if (immErr) throw new Error(`Immeuble insert: ${immErr.message}`);
  const idImmeuble = immData.id_immeuble;
  console.log(`  ✓ Immeuble #${idImmeuble}`);

  // ── 3. FATs ───────────────────────────────────────────────────────────────
  const seenAtIds = new Set();
  const fatRows = fats.map((f, idx) => {
    let atId = f.fat_id_AT || null;
    // Sécurité : éviter les doublons de clés at_id dans le même lot d'insertion (Conflict 409)
    if (atId && seenAtIds.has(atId)) {
      atId = `${atId}-D${idx}`;
    }
    if (atId) seenAtIds.add(atId);

    return {
      id_immeuble: idImmeuble,
      at_id: atId,
      etage_fat: f.etage_fat ?? null,
      usage: toDbFatUsage(f.usage),
      geom: f.lon && f.lat ? `POINT(${f.lon} ${f.lat})` : null,
      cable_snap_m: normalizeSnap(f.cable_snap_m),
      capacite: f.capacite || 8,
    };
  });

  const { data: fatsInserted, error: fatsErr } = await supabase
    .from("fats")
    .insert(fatRows)
    .select("id_fat, at_id");

  if (fatsErr) throw new Error(`FATs insert: ${fatsErr.message}`);
  console.log(`  ✓ ${fatsInserted.length} FATs insérés`);

  // at_id → id_fat DB
  const fatIdMap = {};
  for (const f of fatsInserted) {
    if (f.at_id) fatIdMap[f.at_id] = f.id_fat;
  }

  // ── 4. Abonnés ────────────────────────────────────────────────────────────
  const clientToFatAt = {};
  for (const f of fats) {
    for (const cc of (f.subscriber_ids || [])) {
      clientToFatAt[cc] = f.fat_id_AT;
    }
  }

  const abRows = abonnes.map(ab => ({
    code_client: ab.code_client,
    id_fat: fatIdMap[clientToFatAt[ab.code_client]] ?? null,
    geom: ab.lon && ab.lat ? `POINT(${ab.lon} ${ab.lat})` : null,
    etage: ab.etage ?? 1,
    porte: ab.porte ?? 1,
    nom: ab.nom || null,
    usage: toDbAbUsage(ab.usage),
  }));

  let abOk = 0;
  for (let i = 0; i < abRows.length; i += 500) {
    const { error: abErr } = await supabase
      .from("abonnes")
      .upsert(abRows.slice(i, i + 500), { onConflict: "code_client" });
    if (abErr) console.warn(`  ⚠️  Abonnés batch ${i}: ${abErr.message}`);
    else abOk += Math.min(500, abRows.length - i);
  }
  console.log(`  ✓ ${abOk}/${abonnes.length} abonnés insérés`);

  // ── 5. fat_abonnes ────────────────────────────────────────────────────────
  const faRows = [];
  const seen = new Set();

  for (const f of fats) {
    const idFatDb = fatIdMap[f.fat_id_AT];
    if (!idFatDb) {
      console.warn(`  ⚠️  FAT "${f.fat_id_AT}" absent de fatIdMap — subscriber_ids ignorés`);
      continue;
    }
    for (const cc of (f.subscriber_ids || [])) {
      const key = `${idFatDb}::${cc}`;
      if (seen.has(key)) continue;
      seen.add(key);
      faRows.push({
        id_fat: idFatDb,
        code_client: cc,
        cable_snap_m: normalizeSnap(f.cable_snap_m),
        distance_real_m: null,
      });
    }
  }

  console.log(`  → ${faRows.length} liaisons fat_abonnes`);

  for (let i = 0; i < faRows.length; i += 500) {
    const { error: faErr } = await supabase
      .from("fat_abonnes")
      .upsert(faRows.slice(i, i + 500), { onConflict: "id_fat,code_client" });
    if (faErr) console.error(`  ❌ fat_abonnes batch ${i}: ${faErr.message}`);
  }

  console.log(`  ✅ Projet "${nomProjet}" sauvegardé — id=${idProjet}`);
  return { success: true, id_projet: idProjet };
}


// ══════════════════════════════════════════════════════════════════════════════
//  LIST PROJECTS
// ══════════════════════════════════════════════════════════════════════════════

export async function listProjects(userId) {
  const { data, error } = await supabase
    .from("projets")
    .select("id_projet, nom_projet, planning_mode, wilaya, commune, date_creation, date_modification")
    .eq("id_utilisateur", userId)
    .order("date_modification", { ascending: false });

  if (error) throw new Error(`listProjects: ${error.message}`);

  const projects = await Promise.all((data || []).map(async (p) => {
    let nb_fats = 0, nb_abonnes = 0;
    try {
      const { data: imms } = await supabase
        .from("immeubles").select("id_immeuble").eq("id_projet", p.id_projet);
      const immIds = (imms || []).map(i => i.id_immeuble);

      if (immIds.length) {
        const { count: fc } = await supabase
          .from("fats").select("id_fat", { count: "exact", head: true }).in("id_immeuble", immIds);
        nb_fats = fc || 0;

        const { data: fatRows } = await supabase
          .from("fats").select("id_fat").in("id_immeuble", immIds);
        const fatIds = (fatRows || []).map(f => f.id_fat);

        if (fatIds.length) {
          const { count: ac } = await supabase
            .from("fat_abonnes").select("id", { count: "exact", head: true }).in("id_fat", fatIds);
          nb_abonnes = ac || 0;
        }
      }
    } catch { /* ignore */ }

    return { ...p, nb_fats, nb_abonnes };
  }));

  return projects;
}


// ══════════════════════════════════════════════════════════════════════════════
//  LOAD PROJECT
// ══════════════════════════════════════════════════════════════════════════════

export async function loadProject(idProjet) {

  const { data: projet, error: pErr } = await supabase
    .from("projets").select("*").eq("id_projet", idProjet).single();
  if (pErr) throw new Error(`Projet ${idProjet}: ${pErr.message}`);

  const { data: immeubles } = await supabase
    .from("immeubles").select("*").eq("id_projet", idProjet);
  if (!immeubles?.length) throw new Error(`Aucun immeuble pour projet ${idProjet}`);

  const firstImm = immeubles[0];
  const logements = firstImm.logement_par_etage || 6;
  const etages = firstImm.etage || 5;
  const immIds = immeubles.map(i => i.id_immeuble);

  const { data: fatsDb, error: fatErr } = await supabase
    .from("fats")
    .select("id_fat, at_id, etage_fat, usage, cable_snap_m, capacite")
    .in("id_immeuble", immIds);
  if (fatErr) throw new Error(`FATs: ${fatErr.message}`);

  const fatIds = (fatsDb || []).map(f => f.id_fat);
  console.log(`  📦 ${fatsDb.length} FATs`);

  let faRows = [];
  if (fatIds.length) {
    const { data } = await supabase
      .from("fat_abonnes")
      .select("id_fat, code_client, distance_real_m, cable_snap_m")
      .in("id_fat", fatIds);
    faRows = data || [];
  }
  console.log(`  📦 ${faRows.length} liaisons fat_abonnes`);

  // subscriber_ids par FAT
  const fatSubscribers = {};
  for (const f of fatsDb) fatSubscribers[f.id_fat] = [];
  for (const fa of faRows) {
    if (fatSubscribers[fa.id_fat]) fatSubscribers[fa.id_fat].push(fa.code_client);
  }

  // Abonnés : On récupère TOUS les abonnés liés aux FATs de ce projet
  const allCodes = Array.from(new Set(faRows.map(fa => fa.code_client)));
  let abonnesDb = [];
  let abErr = null;
  if (allCodes.length) {
    const { data, error } = await supabase
      .from("abonnes")
      .select("code_client, etage, porte, nom, usage, geom")
      .in("code_client", allCodes);
    abonnesDb = data || [];
    abErr = error;
  }
    
  if (abErr) console.error(`  ❌ Erreur chargement abonnés: ${abErr.message}`);
  console.log(`  📦 ${abonnesDb?.length || 0} abonnés récupérés`);

  // ── Reconstruction snapshot ────────────────────────────────────────────────
  const toFeUsage = (dbUsage) => {
    if (dbUsage === "commercial" || dbUsage === "commerces") return "commerces";
    return "logements";
  };

  // lon_abonne = (porte-1) % logements → index de colonne 0-based dans l'étage
  const subscribersData = abonnesDb.map(ab => {
    const feUsage = toFeUsage(ab.usage);
    const { lat, lon } = parseGeom(ab.geom);
    return {
      code_client: ab.code_client,
      lat_abonne: lat,
      lon_abonne: lon,
      etage: feUsage === "commerces" ? 0 : (ab.etage ?? 1),
      porte: ab.porte ?? 1,
      nom: ab.nom ?? "",
      usage: feUsage,
    };
  });

  // Prediction mode: pad empty doors so the full grid is restored on load
  if (projet.planning_mode === "Réseau estimé") {
    const occupied = {};
    subscribersData.forEach(s => { occupied[`${s.etage}_${s.porte}`] = true; });
    const presComm = firstImm.presence_commerce ?? false;
    if (presComm) {
      for (let p = 1; p <= logements; p++) {
        if (!occupied[`0_${p}`]) {
          subscribersData.push({
            code_client: `EMPTY_0_${p}`, lat_abonne: 0, lon_abonne: 0,
            etage: 0, porte: p, nom: "", usage: "commerces",
          });
        }
      }
    }
    for (let e = 1; e <= etages; e++) {
      for (let p = 1; p <= logements; p++) {
        if (!occupied[`${e}_${p}`]) {
          subscribersData.push({
            code_client: `EMPTY_${e}_${p}`, lat_abonne: 0, lon_abonne: 0,
            etage: e, porte: p, nom: "", usage: "logements",
          });
        }
      }
    }
  }

  const subByCode = {};
  subscribersData.forEach(s => { subByCode[s.code_client] = s; });

  const fatResults = (fatsDb || []).map(f => {
    const subscriberIds = fatSubscribers[f.id_fat] || [];
    const fatEtage = f.etage_fat ?? 1;
    const fatUsage = toFeUsage(f.usage);

    // centroid_lon = médiane des lon_abonne des abonnés sur cet étage
    const subsOnFloor = subscriberIds
      .map(id => subByCode[id])
      .filter(s => s && s.etage === fatEtage);

    const { lat: fLat, lon: fLon } = parseGeom(f.geom);

    let centroidLon = fLon || (logements - 1) / 2;
    if (subsOnFloor.length > 0 && !fLon) {
      const sorted = [...subsOnFloor].sort((a, b) => a.lon_abonne - b.lon_abonne);
      const mid = Math.floor(sorted.length / 2);
      centroidLon = sorted.length % 2 !== 0
        ? sorted[mid].lon_abonne
        : (sorted[mid - 1].lon_abonne + sorted[mid].lon_abonne) / 2;
    }

    return {
      fat_id: f.at_id || `FAT-${f.id_fat}`,
      fat_id_AT: f.at_id || "",
      id_fat_db: f.id_fat,
      centroid_lat: fLat,
      centroid_lon: centroidLon,
      etage_fat: fatEtage,
      usage: fatUsage,
      cable_snap_m: f.cable_snap_m,
      capacite: f.capacite ?? 8,
      subscriber_ids: subscriberIds,
      n_subscribers: subscriberIds.length,
      capacity_ok: subscriberIds.length <= (f.capacite ?? 8),
    };
  });

  const totalPorts = fatResults.reduce((s, f) => s + (f.capacite ?? 8), 0) || 1;
  const kpis = {
    totalAbonnes: subscribersData.length,
    fatsNeeded: fatResults.length,
    fatsPortsUsed: Math.round(subscribersData.length / totalPorts * 100),
    lineaire: Math.round(faRows.reduce((s, fa) => s + (fa.cable_snap_m ?? 0), 0)),
  };

  return {
    ville: projet.wilaya || "",
    commune: projet.commune || "",
    residenceObj: { 
      name: projet.nom_projet, 
      osm_id: firstImm.osm_id,
      lat: parseGeom(firstImm.geom).lat,
      lon: parseGeom(firstImm.geom).lon,
      geom_poly: firstImm.geom_poly ? parseGeom(firstImm.geom_poly) : null
    },
    etages, logements,
    hauteurEtage: firstImm.hauteur_etage ?? 3.0,
    presenceCommercial: firstImm.presence_commerce ?? false,
    planningMode: projet.planning_mode === "Réseau estimé" ? "prediction" : "subscriber",
    kpis,
    sectorisationSnapshot: {
      fatResults, subscribersData, etages, logements,
      residenceName: projet.nom_projet,
      presenceCommercial: firstImm.presence_commerce ?? false,
      planningMode: projet.planning_mode === "Réseau estimé" ? "prediction" : "subscriber",
      timestamp: Date.now(),
      connections: faRows, // Ajout des liaisons réelles
    },
    id_projet: idProjet,
    nom_projet: projet.nom_projet,
    date_creation: projet.date_creation,
  };
}


// ══════════════════════════════════════════════════════════════════════════════
//  DELETE PROJECT
// ══════════════════════════════════════════════════════════════════════════════


export async function deleteProject(idProjet) {
  const { error } = await supabase.from("projets").delete().eq("id_projet", idProjet);
  if (error) throw new Error(`deleteProject: ${error.message}`);
  return { success: true };
}

/**
 * Récupère tous les OSM IDs des immeubles déjà traités (enregistrés) 
 * pour une wilaya et commune donnée.
 */
// PAR :
export async function getTreatedBuildings(wilaya, commune) {
  if (!wilaya || !commune) return [];
  const { data, error } = await supabase
    .from("immeubles")
    .select("osm_id, geom_poly, geom, id_projet, projets(nom_projet)")
    .eq("wilaya", wilaya)
    .eq("commune", commune);

  if (error) {
    console.error("❌ Erreur getTreatedBuildings:", error);
    return [];
  }
  return (data || []).filter(i => i.osm_id != null);
}