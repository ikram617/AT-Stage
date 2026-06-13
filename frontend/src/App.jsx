import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import ATLogoImg from "./assets/algerie-telecom-logo-png_seeklogo-210074.png";
import Profile from "./profile";
import Login from "./login";
import { saveProject as spSave, listProjects, loadProject as spLoad, deleteProject as spDelete, getTreatedBuildings } from "./projectService";
import Building3D from "./Building3D";

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

const AT_BLUE = "#005BAA";
const AT_BLUE_DARK = "#004080";
const AT_BLUE_LIGHT = "#E8F1FA";
const AT_ORANGE = "#F7941D";
const AT_ORANGE_LIGHT = "#FEF3E6";
const GRAY_50 = "#F9FAFB";
const GRAY_100 = "#F3F4F6";
const GRAY_200 = "#E5E7EB";
const GRAY_300 = "#D1D5DB";
const GRAY_400 = "#9CA3AF";
const GRAY_500 = "#6B7280";
const GRAY_600 = "#4B5563";
const GRAY_700 = "#374151";
const GRAY_800 = "#1F2937";
const GREEN = "#10B981";
const GREEN_LIGHT = "#D1FAE5";
const RED = "#EF4444";
const PURPLE = "#7C3AED";
const PURPLE_LIGHT = "#EDE9FE";
const AMBER = "#F59E0B";
const AMBER_LIGHT = "#FEF3C7";

const API = "http://127.0.0.1:8000";

// ── AT Logo ────────────────────────────────────────────────
const ATLogo = ({ size = 40 }) => (
  <img src={ATLogoImg} alt="Algerie Telecom" style={{ width: size * 1.5, height: "auto" }} />
);

// ── Notification ───────────────────────────────────────────
const Notification = ({ notif, onClose }) => {
  useEffect(() => {
    const t = setTimeout(onClose, 4000);
    return () => clearTimeout(t);
  }, [onClose]);

  const colors = {
    success: { bg: GREEN_LIGHT, border: GREEN, icon: "fa-check-circle", iconColor: GREEN },
    info: { bg: AT_BLUE_LIGHT, border: AT_BLUE, icon: "fa-info-circle", iconColor: AT_BLUE },
    error: { bg: "#FEE2E2", border: RED, icon: "fa-times-circle", iconColor: RED },
    warning: { bg: AT_ORANGE_LIGHT, border: AT_ORANGE, icon: "fa-exclamation-triangle", iconColor: AT_ORANGE },
  };
  const c = colors[notif.type] || colors.info;

  return (
    <div style={{
      position: "fixed", bottom: 24, right: 24, zIndex: 9999,
      background: "white", border: `1.5px solid ${c.border}`,
      borderRadius: 12, padding: "14px 18px",
      display: "flex", alignItems: "flex-start", gap: 12,
      boxShadow: "0 8px 32px rgba(0,0,0,0.12)", maxWidth: 360,
    }}>
      <div style={{ width: 30, height: 30, borderRadius: 8, background: c.bg, color: c.iconColor, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 700, fontSize: 14 }}>
        <i className={`fas ${c.icon}`}></i>
      </div>
      <div style={{ flex: 1 }}>
        <div style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>{notif.title}</div>
        <div style={{ fontSize: 11, color: GRAY_600, marginTop: 2 }}>{notif.sub}</div>
      </div>
      <button onClick={onClose} style={{ background: "none", border: "none", cursor: "pointer", color: GRAY_400, fontSize: 16 }}>✕</button>
    </div>
  );
};

// ── KPICard ────────────────────────────────────────────────
const KPICard = ({ label, value, suffix = "", sub, color, icon }) => {
  const colors = {
    blue: { accent: AT_BLUE, bg: AT_BLUE_LIGHT, text: AT_BLUE },
    orange: { accent: AT_ORANGE, bg: AT_ORANGE_LIGHT, text: AT_ORANGE },
    green: { accent: GREEN, bg: GREEN_LIGHT, text: GREEN },
    purple: { accent: PURPLE, bg: PURPLE_LIGHT, text: PURPLE },
  };
  const c = colors[color] || colors.blue;
  return (
    <div style={{ background: "white", borderRadius: 12, padding: "18px 20px", border: `1px solid ${GRAY_200}`, position: "relative", overflow: "hidden", boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: c.accent, borderRadius: "12px 12px 0 0" }} />
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
        <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.8px", textTransform: "uppercase", color: GRAY_400 }}>{label}</span>
        <span style={{ fontSize: 18, width: 32, height: 32, background: c.bg, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" }}>{icon}</span>
      </div>
      <div style={{ fontSize: 30, fontWeight: 800, color: c.text, lineHeight: 1, letterSpacing: "-1px" }}>
        {value !== null ? value : <div style={{ width: 60, height: 28, background: GRAY_200, borderRadius: 6 }} />}
        {value !== null && <span style={{ fontSize: 16 }}>{suffix}</span>}
      </div>
      <div style={{ fontSize: 11, color: GRAY_400, marginTop: 4 }}>{sub}</div>
    </div>
  );
};

// ── FATNode ────────────────────────────────────────────────
// ── FATNode ────────────────────────────────────────────────
const FATNode = ({ id, connected, totalPorts, onHover, onLeave, onClick, realName, emplacement, isHovered, isSelected, usage }) => {
  const isActive = isHovered || isSelected;
  const isCom = usage === "commerces";
  const primaryColor = isCom ? AT_ORANGE : AT_BLUE;
  const lightColor = isCom ? AT_ORANGE_LIGHT : AT_BLUE_LIGHT;

  return (
    <div
      className="fat-node-container"
      onMouseEnter={onHover}
      onMouseLeave={onLeave}
      onClick={(e) => {
        e.stopPropagation();
        if (onClick) onClick();
      }}
      style={{
        position: "relative",
        zIndex: isActive ? 10000 : 10,
        transition: "all 0.2s"
      }}
    >
      <div style={{
        background: "white",
        border: `2px solid ${isSelected ? primaryColor : GRAY_400}`,
        borderRadius: 8,
        padding: "5px 10px",
        cursor: "pointer",
        boxShadow: isSelected
          ? `0 0 0 3px ${lightColor}, 0 4px 12px rgba(0,0,0,0.15)`
          : `0 2px 8px rgba(0,0,0,0.05)`,
        minWidth: 90,
        textAlign: "center",
        transform: isHovered ? "scale(1.05)" : "scale(1)",
        transition: "all 0.2s"
      }}>
        <div style={{ fontSize: 11, fontWeight: 800, color: isSelected ? primaryColor : GRAY_800, marginTop: 2 }}>FAT {id}</div>
        <div style={{ display: "flex", gap: 3, justifyContent: "center", marginTop: 4, flexWrap: "wrap" }}>
          {Array.from({ length: totalPorts || 8 }).map((_, i) => (
            <div key={i} style={{ width: 6, height: 6, borderRadius: "50%", background: i < connected ? AT_BLUE : GREEN, border: "1px solid rgba(0,0,0,0.05)" }} />
          ))}
        </div>
      </div>
      <div className="fat-tooltip" style={{ opacity: isActive ? 1 : 0, transform: isActive ? "translateX(-50%) translateY(0)" : "translateX(-50%) translateY(8px)" }}>
        {realName}
      </div>
    </div>
  );
};

// ── Haversine : distance en mètres entre deux points GPS ───────────────────
const haversineM = (lat1, lon1, lat2, lon2) => {
  if (!lat1 || !lon1 || !lat2 || !lon2) return null;
  const R = 6371000, toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1), dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
};

// ── Snap Câble : sélectionne la longueur de câble standard supérieure ou égale ──
const snapDropCable = (distance, cables) => {
  if (!cables || cables.length === 0) return Math.ceil(distance);
  const sorted = [...cables].sort((a, b) => a - b);
  for (let c of sorted) {
    if (c >= distance) return c;
  }
  return sorted[sorted.length - 1];
};

// ── BuildingPlan ────────────────────────────────────────────
const BuildingPlan = ({ etages, logements, residenceName, presenceCommercial, fatResults, subscribersData, planningMode }) => {
  const [hoveredFatId, setHoveredFatId] = useState(null);
  const [selectedFatId, setSelectedFatId] = useState(null);
  const isPrediction = planningMode === "prediction";

  if (!fatResults || fatResults.length === 0) {
    return (
      <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>
        <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Sélectionnez et chargez un bâtiment</div>
        <div style={{ fontSize: 12 }}>Puis lancez la sectorisation pour générer l'architecture.</div>
      </div>
    );
  }

  // ── Index abonnés : code_client → subscriber ──
  const subByCode = useMemo(() => {
    const map = {};
    (subscribersData || []).forEach(s => { map[s.code_client || s.id] = s; });
    return map;
  }, [subscribersData]);

  // ── Séparation et Tri des FATs pour numérotation globale ──
  const allSortedFats = useMemo(() => {
    const commercialFats = [...(fatResults || [])]
      .filter(f => f.usage === "commerces")
      .sort((a, b) => (a.centroid_lon ?? 0) - (b.centroid_lon ?? 0));
    const residentialFats = [...(fatResults || [])]
      .filter(f => f.usage === "logements")
      .sort((a, b) => (a.etage_fat ?? 1) - (b.etage_fat ?? 1) || (a.centroid_lon ?? 0) - (b.centroid_lon ?? 0));
    return [...commercialFats, ...residentialFats];
  }, [fatResults]);

  // ── Grouper FATs par étage ──
  const fatsByFloor = useMemo(() => {
    return (fatResults || []).reduce((acc, fat) => {
      const fl = Math.floor(fat.etage_fat ?? 0);
      if (!acc[fl]) acc[fl] = [];
      acc[fl].push(fat);
      return acc;
    }, {});
  }, [fatResults]);

  // ── Abonnés triés par étage et longitude ──
  const subsByFloorSorted = useMemo(() => {
    const map = {};
    for (let e = 0; e <= etages; e++) {
      const usage = (e === 0) ? "commerces" : "logements";
      map[e] = (subscribersData || [])
        .filter(s => s.etage === e && s.usage === usage)
        .sort((a, b) => {
          const pa = a.porte ?? 0, pb = b.porte ?? 0;
          if (pa !== pb) return pa - pb;
          return (a.lon_abonne ?? 0) - (b.lon_abonne ?? 0);
        });
    }
    return map;
  }, [subscribersData, etages]);

  // ── Mapping Abonné -> FAT (O(1) lookup pour les rendus) ──
  const fatBySubCode = useMemo(() => {
    const map = {};
    (fatResults || []).forEach(f => {
      (f.subscriber_ids || []).forEach(id => {
        map[`${f.usage}_${id}`] = f;
      });
    });
    return map;
  }, [fatResults]);

  // ── Calcul des bornes géographiques globales pour le projet ──
  const geoBounds = useMemo(() => {
    const lons = [
      ...(subscribersData || []).map(s => s.lon_abonne),
      ...(fatResults || []).map(f => f.centroid_lon)
    ].filter(v => v != null && v !== 0);
    if (lons.length === 0) return null;
    return { min: Math.min(...lons), max: Math.max(...lons) };
  }, [subscribersData, fatResults]);

  // ── Calcul position horizontale d'un FAT ──
  const getFatColumnPosition = (fat, floorNum) => {
    const floorSubs = subsByFloorSorted[floorNum] || [];
    const nLog = Math.max(logements, 1);
    if (floorSubs.length === 0) return (nLog - 1) / 2;

    // Récupérer les abonnés réels connectés à ce FAT sur cet étage
    const fatSubsOnFloor = (fat.subscriber_ids || [])
      .map(id => subByCode[id])
      .filter(s => s && s.etage === floorNum);

    if (fatSubsOnFloor.length > 0) {
      // Trouver l'indice de colonne visuel de chaque abonné dans le tableau 2D
      const colIndices = fatSubsOnFloor.map(s => {
        return floorSubs.findIndex(fs => (fs.code_client || fs.id) === (s.code_client || s.id));
      }).filter(idx => idx !== -1);

      if (colIndices.length > 0) {
        // Positionner le FAT exactement au centre visuel de ses abonnés
        const avgCol = colIndices.reduce((sum, val) => sum + val, 0) / colIndices.length;
        return avgCol;
      }
    }

    // Fallback: s'il n'y a pas d'abonnés, on distribue équitablement les FATs de ce type sur la ligne
    const fatsOnFloor = (fatsByFloor[floorNum] || []).filter(f => f.usage === fat.usage);
    const idx = fatsOnFloor.indexOf(fat);
    return fatsOnFloor.length > 1
      ? (idx / (fatsOnFloor.length - 1)) * (nLog - 1)
      : (nLog - 1) / 2;
  };


  // ── Floors à afficher : de etages → 1 (puis 0 si commercial) ──
  const floorsToRender = [];
  for (let e = etages; e >= 1; e--) floorsToRender.push(e);
  if (presenceCommercial) floorsToRender.push(0);

  const FLOOR_HEIGHT = 90;

  return (
    <div style={{ padding: 20, overflowX: "auto" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
        <div>
          <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800 }}>{residenceName || "Résidence"}</div>
          <div style={{ fontSize: 11, color: GRAY_400 }}>
            {etages} étages · {presenceCommercial ? "RDC Commercial" : "Résidentiel pur"} · {logements} unités/étage
          </div>
        </div>
      </div>

      {floorsToRender.map((e) => {
        const isCommercialFloor = (e === 0);
        // Filtrer les FATs par étage ET usage pour une séparation stricte
        const fatsOnFloor = (fatsByFloor[e] || []).filter(f =>
          isCommercialFloor ? f.usage === "commerces" : f.usage === "logements"
        );
        const hasFat = fatsOnFloor.length > 0;
        const floorSubsSorted = subsByFloorSorted[e] || [];

        return (
          <div key={e} style={{ marginBottom: 2 }}>
            <div style={{ display: "flex", alignItems: "stretch", gap: 0 }}>
              <div style={{ width: 64, flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "flex-end", paddingRight: 10, fontSize: 9, fontWeight: 700, color: isCommercialFloor ? AT_ORANGE : GRAY_400, letterSpacing: "0.5px", textTransform: "uppercase" }}>
                {isCommercialFloor ? "RDC (C)" : `ÉT. ${e}`}
              </div>

              <div style={{ width: 6, background: isCommercialFloor ? AT_ORANGE : AT_BLUE, borderRadius: "4px 0 0 4px", opacity: 0.7 }} />

              <div style={{
                flex: 1,
                position: "relative",
                background: isCommercialFloor ? AT_ORANGE_LIGHT : GRAY_50,
                border: `1px solid ${isCommercialFloor ? AT_ORANGE : GRAY_200}`,
                borderLeft: "none",
                borderRight: "none",
                minHeight: hasFat ? FLOOR_HEIGHT : 64,
                display: "flex",
                alignItems: "stretch",
              }}>
                <div style={{ display: "flex", flex: 1, alignItems: "stretch", padding: hasFat ? "36px 4px 4px 4px" : "4px" }}>
                  {Array.from({ length: logements }, (_, colIdx) => {
                    // Source unique de vérité : les abonnés de cet étage triés par coordonnée
                    const floorSubs = subsByFloorSorted[e] || [];
                    // On prend l'abonné à la colonne colIdx selon l'ordre de tri
                    const sub = floorSubs[colIdx];
                    const codeClient = sub?.code_client ?? sub?.id ?? null;

                    // Le FAT auquel est assigné cet abonné (O(1) via map)
                    let assignedFat = null;
                    if (codeClient !== null && sub?.usage) {
                      assignedFat = fatBySubCode[`${sub.usage}_${codeClient}`] ||
                        (sub.id ? fatBySubCode[`${sub.usage}_${sub.id}`] : null);
                    }
                    const assignedFatKey = assignedFat ? (assignedFat.fat_id_AT || assignedFat.fat_id) : null;

                    // Est-ce que ce slot est assigné à UN FAT quelconque ?
                    const isAssignedToAny = !!assignedFat;

                    // Est-ce que ce slot appartient au FAT actif (survolé ou sélectionné) ?
                    const isHighlighted = isAssignedToAny && (
                      (hoveredFatId && assignedFatKey === hoveredFatId) ||
                      (selectedFatId && assignedFatKey === selectedFatId)
                    );

                    // Label : utilise la porte réelle si disponible, sinon on affiche le numéro de colonne pour les "faux" carrés vides
                    const label = sub
                      ? (isCommercialFloor ? `C.${sub.porte}` : `P.${sub.porte}`)
                      : (isCommercialFloor ? `C.${colIdx + 1}` : `P.${colIdx + 1}`);
                    return (
                      <div key={colIdx} style={{ flex: 1, display: "flex", padding: "0 2px" }}>
                        <div style={{
                          flex: 1,
                          display: "flex",
                          flexDirection: "column",
                          alignItems: "center",
                          justifyContent: "center",
                          background: isHighlighted ? (isCommercialFloor ? AT_ORANGE : AT_BLUE) : (isAssignedToAny ? "white" : GRAY_300),
                          color: isHighlighted ? "white" : (isAssignedToAny ? "black" : GRAY_600),
                          border: `1px solid ${isHighlighted ? (isCommercialFloor ? AT_ORANGE : AT_BLUE) : (isAssignedToAny ? GRAY_300 : GRAY_400)}`,
                          borderRadius: 4,
                          padding: "6px 4px",
                          minWidth: 30,
                          transition: "all 0.15s",
                          opacity: 1
                        }}>
                          <div style={{ fontSize: 9, fontWeight: 800, fontFamily: "monospace" }}>
                            {label}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {fatsOnFloor.map((fat, fIdx) => {
                  const fatKey = fat.fat_id_AT || fat.fat_id;
                  const fatIndex = allSortedFats.findIndex(f => (f.fat_id_AT || f.fat_id) === fatKey) + 1;
                  const colPos = getFatColumnPosition(fat, e);
                  const leftPct = (colPos / (logements - 1)) * 100;

                  return (
                    <div key={`${fatKey}-${e}-${fIdx}`} style={{
                      position: "absolute",
                      top: 3,
                      left: `${Math.max(5, Math.min(95, leftPct))}%`,
                      transform: "translateX(-50%)",
                      zIndex: hoveredFatId === fatKey ? 200 : 10,
                    }}>
                      <FATNode
                        id={fatIndex}
                        connected={fat.subscriber_ids?.length ?? 0}
                        totalPorts={fat.fat_capacity || 8}
                        realName={fat.fat_id_AT || fat.fat_id}
                        emplacement={isCommercialFloor ? `RDC Commercial` : `Étage ${e}`}
                        usage={fat.usage}
                        onHover={() => setHoveredFatId(fatKey)}
                        onLeave={() => setHoveredFatId(null)}
                        onClick={() => setSelectedFatId(selectedFatId === fatKey ? null : fatKey)}
                        isHovered={hoveredFatId === fatKey}
                        isSelected={selectedFatId === fatKey}
                      />
                    </div>
                  );
                })}
              </div>
              <div style={{ width: 6, background: isCommercialFloor ? AT_ORANGE : AT_BLUE, borderRadius: "0 4px 4px 0", opacity: 0.7 }} />
            </div>
          </div>
        );
      })}

      <div style={{ display: "flex", alignItems: "center", paddingLeft: 70, marginTop: 4 }}>
        <div style={{ flex: 1, height: 8, background: `linear-gradient(90deg, ${GRAY_400}, ${GRAY_300})`, borderRadius: 4 }} />
      </div>
      <div style={{ paddingLeft: 70, marginTop: 20, fontSize: 11, color: GRAY_400 }}>
        <div style={{ marginBottom: 8 }}>
          FAT positionnés par usage : <span style={{ color: AT_ORANGE, fontWeight: 700 }}>Commerces (RDC)</span> et <span style={{ color: AT_BLUE, fontWeight: 700 }}>Logements (Étages)</span>.
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 20, background: "rgba(0,0,0,0.02)", padding: "8px 12px", borderRadius: 6, border: `1px solid ${GRAY_100}`, width: "fit-content", flexWrap: "wrap" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: "50%", background: GREEN, border: "1px solid rgba(0,0,0,0.1)" }} />
            <span style={{ fontWeight: 600, color: GRAY_600 }}>Port libre</span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: "50%", background: AT_BLUE, border: "1px solid rgba(0,0,0,0.1)" }} />
            <span style={{ fontWeight: 600, color: GRAY_600 }}>Port occupé</span>
          </div>
          {planningMode === "prediction" && (
            <>
              <div style={{ width: 1, height: 14, background: GRAY_200 }}></div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 16, height: 12, borderRadius: 2, background: GRAY_300, border: `1px solid ${GRAY_400}`, opacity: 0.6 }} />
                <span style={{ fontWeight: 600, color: GRAY_600 }}>Porte vide</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 16, height: 12, borderRadius: 2, background: "white", border: `1px solid ${GRAY_300}` }} />
                <span style={{ fontWeight: 600, color: GRAY_600 }}>Porte occupé</span>
              </div>
            </>
          )}
        </div>
      </div>

    </div>
  );
};

// ── FontAwesome & Leaflet loaders ──────────────────────────────────────────
function loadResources() {
  const FA_CSS = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css";
  const LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
  const LEAFLET_JS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";

  return new Promise((resolve, reject) => {
    // FontAwesome
    if (!document.querySelector(`link[href="${FA_CSS}"]`)) {
      const link = document.createElement("link"); link.rel = "stylesheet"; link.href = FA_CSS; document.head.appendChild(link);
    }

    // Leaflet
    if (window.L) { resolve(window.L); return; }
    if (!document.querySelector(`link[href="${LEAFLET_CSS}"]`)) {
      const link = document.createElement("link"); link.rel = "stylesheet"; link.href = LEAFLET_CSS; document.head.appendChild(link);
    }
    if (!document.querySelector(`script[src="${LEAFLET_JS}"]`)) {
      const script = document.createElement("script"); script.src = LEAFLET_JS;
      script.onload = () => resolve(window.L); script.onerror = () => reject(new Error("Impossible de charger Leaflet"));
      document.head.appendChild(script);
    } else {
      const wait = setInterval(() => { if (window.L) { clearInterval(wait); resolve(window.L); } }, 50);
    }
  });
}

// ── LeafletMap ────────────────────────────────────────────────────────────────
// Affiche les bâtiments avec leur label bloc ou nom sur la carte.
// Les bâtiments sans nom → label = {commune}-citéX-BlocX
// Les bâtiments nommés  → label = nom officiel
const LeafletMap = ({ buildingsGeoJson, fatResults, onBuildingClick, selectedOsmId, selectedLat, selectedLon, primaryTargetId, isBloc, treatedOsmIds = new Set() }) => {
  const mapRef = useRef(null), mapInstanceRef = useRef(null), buildingsLayerRef = useRef(null), fatsLayerRef = useRef(null), radiusLayerRef = useRef(null);
  const [leafletReady, setLeafletReady] = useState(false);

  useEffect(() => { loadResources().then(() => setLeafletReady(true)).catch(console.error); }, []);

  useEffect(() => {
    if (!leafletReady || !mapRef.current || mapInstanceRef.current) return;
    const L = window.L, map = L.map(mapRef.current, { zoomControl: true });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { attribution: "© OpenStreetMap", maxZoom: 19 }).addTo(map);
    map.setView([35.7, -0.65], 13); mapInstanceRef.current = map;
  }, [leafletReady]);

  // Centrer et zoomer automatiquement sur le bâtiment sélectionné/cliqué
  useEffect(() => {
    if (!leafletReady || !mapInstanceRef.current || !selectedLat || !selectedLon) return;
    mapInstanceRef.current.setView([selectedLat, selectedLon], 18, { animate: true });
  }, [leafletReady, selectedOsmId, selectedLat, selectedLon]);

  useEffect(() => {
    if (!leafletReady || !mapInstanceRef.current) return;
    const L = window.L, map = mapInstanceRef.current;
    if (buildingsLayerRef.current) { map.removeLayer(buildingsLayerRef.current); buildingsLayerRef.current = null; }
    if (fatsLayerRef.current) { map.removeLayer(fatsLayerRef.current); fatsLayerRef.current = null; }
    if (radiusLayerRef.current) { map.removeLayer(radiusLayerRef.current); radiusLayerRef.current = null; }

    if (buildingsGeoJson) {
      try {
        const geoJsonData = typeof buildingsGeoJson === "string" ? JSON.parse(buildingsGeoJson) : buildingsGeoJson;
        buildingsLayerRef.current = L.geoJSON(geoJsonData, {
          style: (feature) => {
            const bid = feature.properties?.id_batiment;
            const realId = feature.properties?.osm_id_real || bid;
            const cleanRealId = realId ? String(realId).replace(/^(way|node|relation)\//i, '') : "";
            const cleanBid = bid ? String(bid).replace(/^(way|node|relation)\//i, '') : "";

            const cleanSelectedOsmId = selectedOsmId ? String(selectedOsmId).replace(/^(way|node|relation)\//i, '') : "";
            const cleanPrimaryTargetId = primaryTargetId ? String(primaryTargetId).replace(/^(way|node|relation)\//i, '') : "";

            const isInitial = bid === primaryTargetId || 
                              realId === primaryTargetId || 
                              (cleanRealId && cleanRealId === cleanPrimaryTargetId) || 
                              (cleanBid && cleanBid === cleanPrimaryTargetId) || 
                              feature.properties?.is_target;

            const isSelected = bid === selectedOsmId || 
                               realId === selectedOsmId || 
                               (cleanRealId && cleanRealId === cleanSelectedOsmId) || 
                               (cleanBid && cleanBid === cleanSelectedOsmId) ||
                               (feature.properties?.is_target && (
                                 selectedOsmId === primaryTargetId || 
                                 (cleanSelectedOsmId && cleanSelectedOsmId === cleanPrimaryTargetId) || 
                                 !selectedOsmId
                               ));

            // Si c'est la cible principale et que son ID d'origine est dans la BD, on force "Traité"
            const forceTargetTreated = feature.properties?.is_target && primaryTargetId && (
              treatedOsmIds.has(String(primaryTargetId)) ||
              treatedOsmIds.has(String(primaryTargetId).replace(/^(way|node|relation)\//i, ''))
            );

            const isTreated = forceTargetTreated ||
              treatedOsmIds.has(String(realId)) ||
              treatedOsmIds.has(String(bid)) ||
              (cleanRealId && treatedOsmIds.has(cleanRealId)) ||
              (cleanBid && treatedOsmIds.has(cleanBid));

            // S'il a déjà été traité (sauvegardé), il est vert (avec style distinct s'il est actif/sélectionné)
            if (isTreated) {
              if (isSelected || (isInitial && !selectedOsmId)) {
                return { color: "#10B981", weight: 6, opacity: 1, fillColor: "#10B981", fillOpacity: 0.35, dashArray: "6, 6" };
              }
              return { color: "#10B981", weight: 5, opacity: 1, fillColor: "#10B981", fillOpacity: 0.35 };
            }

            // Priorité absolue : le bâtiment actif (sélectionné ou cible initiale) pas encore traité DOIT être bleu
            if (isSelected || (isInitial && !selectedOsmId)) {
              return { color: "#005BAA", weight: 4, opacity: 1, fillColor: "#005BAA", fillOpacity: 0.25 };
            }

            // Reste: gris (voisinage)
            return { color: "#9CA3AF", weight: 1.5, opacity: 0.9, fillColor: "#F3F4F6", fillOpacity: 0.4 };
          },
          onEachFeature: (feature, layer) => {
            const bid = feature.properties?.id_batiment;
            const realId = feature.properties?.osm_id_real || bid;
            const cleanRealId = realId ? String(realId).replace(/^(way|node|relation)\//i, '') : "";
            const cleanBid = bid ? String(bid).replace(/^(way|node|relation)\//i, '') : "";

            const cleanSelectedOsmId = selectedOsmId ? String(selectedOsmId).replace(/^(way|node|relation)\//i, '') : "";
            const cleanPrimaryTargetId = primaryTargetId ? String(primaryTargetId).replace(/^(way|node|relation)\//i, '') : "";

            const isInitial = bid === primaryTargetId || 
                              realId === primaryTargetId || 
                              (cleanRealId && cleanRealId === cleanPrimaryTargetId) || 
                              (cleanBid && cleanBid === cleanPrimaryTargetId) || 
                              feature.properties?.is_target;

            const isSelected = bid === selectedOsmId || 
                               realId === selectedOsmId || 
                               (cleanRealId && cleanRealId === cleanSelectedOsmId) || 
                               (cleanBid && cleanBid === cleanSelectedOsmId) ||
                               (feature.properties?.is_target && (
                                 selectedOsmId === primaryTargetId || 
                                 (cleanSelectedOsmId && cleanSelectedOsmId === cleanPrimaryTargetId) || 
                                 !selectedOsmId
                               ));

            const forceTargetTreated = feature.properties?.is_target && primaryTargetId && (
              treatedOsmIds.has(String(primaryTargetId)) ||
              treatedOsmIds.has(String(primaryTargetId).replace(/^(way|node|relation)\//i, ''))
            );

            const isTreated = forceTargetTreated ||
              treatedOsmIds.has(String(realId)) ||
              treatedOsmIds.has(String(bid)) ||
              (cleanRealId && treatedOsmIds.has(cleanRealId)) ||
              (cleanBid && treatedOsmIds.has(cleanBid));

            const baseLabel = feature.properties?.nom_batiment || bid || "Bât.";
            const mapLabel = isTreated ? `✅ ${baseLabel} - Traité` : baseLabel;

            let labelClass = "neighbor-building-label";
            if (isTreated) {
              labelClass = (isSelected || (isInitial && !selectedOsmId)) ? "treated-building-label treated-selected" : "treated-building-label";
            } else if (isSelected || (isInitial && !selectedOsmId)) {
              labelClass = "target-building-label";
            }

            layer.bindTooltip(mapLabel, {
              permanent: true, direction: "center", interactive: true,
              className: labelClass
            });
            layer.on("click", (e) => {
              if (onBuildingClick) onBuildingClick(feature.properties);
              const center = feature.properties?.centroid_lat && feature.properties?.centroid_lon
                ? [feature.properties.centroid_lat, feature.properties.centroid_lon]
                : (layer.getBounds ? layer.getBounds().getCenter() : e.latlng);
              map.setView(center, 18, { animate: true });
            });
            // S'assurer que le clic sur le label (tooltip) déclenche aussi l'action
            layer.getTooltip().on("click", (e) => {
              L.DomEvent.stopPropagation(e);
              if (onBuildingClick) onBuildingClick(feature.properties);
              const center = feature.properties?.centroid_lat && feature.properties?.centroid_lon
                ? [feature.properties.centroid_lat, feature.properties.centroid_lon]
                : (layer.getBounds ? layer.getBounds().getCenter() : e.latlng);
              map.setView(center, 18, { animate: true });
            });
          },
        }).addTo(map);

        // Ajout d'un cercle de rayon (100m) seulement pour les blocs non nommés
        const targetFeature = geoJsonData.features.find(f => f.properties?.id_batiment === primaryTargetId || f.properties?.is_target);
        if (isBloc && targetFeature && targetFeature.properties?.centroid_lat) {
          radiusLayerRef.current = L.circle([targetFeature.properties.centroid_lat, targetFeature.properties.centroid_lon], {
            radius: 100,
            color: "#005BAA",
            weight: 1.5,
            opacity: 0.4,
            fillColor: "#005BAA",
            fillOpacity: 0.05,
            dashArray: "5, 10"
          }).addTo(map);

          radiusLayerRef.current.bindTooltip("Rayon d'importation : 100m", {
            permanent: true,
            direction: "top",
            className: "radius-label"
          });
        }

        // Zoom intelligent : Priorité sur le bâtiment cible, sinon fitBounds global
        if (targetFeature && targetFeature.properties?.centroid_lat) {
          map.setView([targetFeature.properties.centroid_lat, targetFeature.properties.centroid_lon], 18, { animate: true });
        } else {
          const bounds = buildingsLayerRef.current.getBounds();
          if (bounds.isValid()) map.fitBounds(bounds, { padding: [50, 50] });
        }
      } catch (e) { console.error("Erreur parsing GeoJSON:", e); }
    }


  }, [leafletReady, buildingsGeoJson, fatResults, selectedOsmId, primaryTargetId, treatedOsmIds]);

  useEffect(() => {
    if (!leafletReady || !mapInstanceRef.current || !mapRef.current) return;
    const observer = new ResizeObserver(() => {
      mapInstanceRef.current.invalidateSize();
    });
    observer.observe(mapRef.current);
    return () => observer.disconnect();
  }, [leafletReady]);

  return (
    <div style={{ height: "100%", minHeight: 400, borderRadius: 10, overflow: "hidden", position: "relative" }}>
      {!leafletReady && (<div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "#F3F4F6", zIndex: 10, fontSize: 13, color: "#9CA3AF" }}>Chargement de la carte…</div>)}
      <div ref={mapRef} style={{ width: "100%", height: "100%", minHeight: 400 }} />
    </div>
  );
};


const ResidenceSearchSelect = ({ commune, ville, onSelect, selectedObj, disabled }) => {
  const [query, setQuery] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [options, setOptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const containerRef = useRef(null);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQ(query), 200);
    return () => clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    if (!commune) { setOptions([]); setQuery(""); return; }
    setLoading(true); setOptions([]); setQuery("");
    fetch(`${API}/api/residence?ville=${encodeURIComponent(ville)}&commune=${encodeURIComponent(commune)}`)
      .then(r => r.json())
      .then(d => setOptions(d.residences || []))
      .catch(() => setOptions([]))
      .finally(() => setLoading(false));
  }, [commune, ville]);

  // Filtrage client sur la liste déjà structurée par le backend
  const filtered = useMemo(() => {
    if (!debouncedQ.trim()) return options;
    const q = debouncedQ.toLowerCase();
    return options.filter(r =>
      r.name.toLowerCase().includes(q) ||
      (r.type || "").toLowerCase().includes(q) ||
      (r.operator || "").toLowerCase().includes(q)
    );
  }, [debouncedQ, options]);

  // Deux sections : nommées + blocs (déjà calculés par le backend)
  const filteredNamed = filtered.filter(r => r.has_official_name);
  const filteredBlocs = filtered.filter(r => r.is_bloc);

  const displayedNamed = filteredNamed.slice(0, 80);
  const displayedBlocs = filteredBlocs.slice(0, 40);

  const nNamed = options.filter(r => r.has_official_name).length;
  const nBlocs = options.filter(r => r.is_bloc).length;

  useEffect(() => {
    const handler = (e) => {
      if (containerRef.current && !containerRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const selectedLabel = selectedObj?.name || null;

  return (
    <div ref={containerRef} style={{ position: "relative" }}>
      {/* ── Champ de saisie ── */}
      <div
        style={{ display: "flex", alignItems: "center", border: `1.5px solid ${open ? AT_BLUE : GRAY_200}`, borderRadius: 8, background: disabled ? GRAY_100 : "white", padding: "0 12px", gap: 8, opacity: disabled ? 0.5 : 1, cursor: disabled ? "not-allowed" : "text", transition: "border-color 0.2s" }}
        onClick={() => !disabled && setOpen(true)}
      >
        <span style={{ fontSize: 14, color: GRAY_400 }}><i className="fas fa-search"></i></span>
        <input
          type="text"
          value={open ? query : (selectedLabel || "")}
          placeholder={
            loading ? "Chargement des bâtiments..." :
              !commune ? "Sélectionnez une commune d'abord" :
                `Rechercher parmi ${nNamed} résidences + ${nBlocs} cité...`
          }
          disabled={disabled || !commune}
          readOnly={!open}
          onChange={e => setQuery(e.target.value)}
          onFocus={() => !disabled && setOpen(true)}
          style={{ flex: 1, border: "none", outline: "none", fontSize: 13, color: GRAY_800, background: "transparent", padding: "10px 0", cursor: disabled ? "not-allowed" : "text" }}
        />
        {loading && (
          <div style={{ width: 16, height: 16, border: `2px solid ${AT_BLUE}`, borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.8s linear infinite", flexShrink: 0 }} />
        )}
        {!loading && selectedObj && !open && (
          <button onClick={e => { e.stopPropagation(); onSelect(null); setQuery(""); }} style={{ background: "none", border: "none", color: GRAY_400, cursor: "pointer", fontSize: 14, padding: 0 }}>✕</button>
        )}
      </div>

      {/* ── Dropdown ── */}
      {open && !disabled && (
        <div style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 1000, background: "white", border: `1.5px solid ${AT_BLUE}`, borderTop: "none", borderRadius: "0 0 8px 8px", maxHeight: 340, overflowY: "auto", boxShadow: "0 8px 24px rgba(0,91,170,0.12)" }}>

          {/* ── En-tête ── */}
          <div style={{ padding: "8px 12px", borderBottom: `1px solid ${GRAY_100}`, background: GRAY_50, position: "sticky", top: 0, zIndex: 2 }}>
            <div style={{ fontSize: 11, color: GRAY_600, fontWeight: 700 }}>
              {debouncedQ.trim()
                ? `${filteredNamed.length + filteredBlocs.length} résultat(s) pour "${debouncedQ}"`
                : `${nNamed} résidence${nNamed > 1 ? "s" : ""} nommée${nNamed > 1 ? "s" : ""} · ${nBlocs} cité${nBlocs > 1 ? "s" : ""}`
              }
            </div>
          </div>

          {/* ── Section : Résidences nommées ── */}
          {displayedNamed.length > 0 && (
            <>
              <div style={{ padding: "5px 12px 3px", fontSize: 9, fontWeight: 800, letterSpacing: "1px", textTransform: "uppercase", color: GRAY_400, background: GRAY_50, borderBottom: `1px solid ${GRAY_100}` }}>
                <i className="fas fa-city"></i> Résidences nommées
              </div>
              {displayedNamed.map((res, i) => {
                const isSelected = res.osm_id === selectedObj?.osm_id;
                const subParts = [];
                if (res.levels) subParts.push(`${res.levels} ét.`);
                if (res.units) subParts.push(`${res.units} log.`);
                const subLabel = subParts.join(" · ");
                return (
                  <div
                    key={res.osm_id || i}
                    onClick={() => { onSelect(res); setOpen(false); setQuery(""); }}
                    style={{ padding: "8px 12px", cursor: "pointer", fontSize: 12, borderBottom: `1px solid ${GRAY_100}`, background: isSelected ? AT_BLUE_LIGHT : "white", color: isSelected ? AT_BLUE : GRAY_800, display: "flex", alignItems: "center", gap: 8 }}
                    onMouseEnter={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : GRAY_50}
                    onMouseLeave={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : "white"}
                  >
                    <span style={{ fontSize: 16, flexShrink: 0 }}>🏢</span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: 600 }}>
                        {res.name}
                      </div>
                      <div style={{ display: "flex", gap: 4, marginTop: 2, flexWrap: "wrap", alignItems: "center" }}>
                        {res.operator ? (
                          <span style={{ fontSize: 9, padding: "1px 6px", background: AT_BLUE_LIGHT, color: AT_BLUE, borderRadius: 4, fontWeight: 700, border: `1px solid ${AT_BLUE}33` }}>
                            {res.operator}
                          </span>
                        ) : (
                          <span style={{ fontSize: 9, padding: "1px 6px", background: GRAY_100, color: GRAY_600, borderRadius: 4, fontWeight: 600 }}>
                            immeuble
                          </span>
                        )}
                        {subLabel && <span style={{ fontSize: 9, color: GRAY_400 }}>{subLabel}</span>}
                      </div>
                    </div>
                    {isSelected && <span style={{ color: AT_BLUE, fontSize: 14, flexShrink: 0 }}>✓</span>}
                  </div>
                );
              })}
            </>
          )}

          {/* ── Section : cite résidentiels ── */}
          {displayedBlocs.length > 0 && (
            <>
              <div style={{ padding: "5px 12px 3px", fontSize: 9, fontWeight: 800, letterSpacing: "1px", textTransform: "uppercase", color: GRAY_400, background: GRAY_50, borderBottom: `1px solid ${GRAY_100}` }}>
                <i className="fas fa-building"></i> cité résidentiels ({nBlocs})
              </div>
              {displayedBlocs.map((bloc) => {
                const isSelected = selectedObj?.osm_id === bloc.osm_id || selectedObj?.name === bloc.name;
                return (
                  <div
                    key={bloc.osm_id}
                    onClick={() => { onSelect(bloc); setOpen(false); setQuery(""); }}
                    style={{ padding: "8px 12px", cursor: "pointer", fontSize: 12, borderBottom: `1px solid ${GRAY_100}`, background: isSelected ? AT_BLUE_LIGHT : "white", color: isSelected ? AT_BLUE : GRAY_800, display: "flex", alignItems: "center", gap: 8 }}
                    onMouseEnter={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : GRAY_50}
                    onMouseLeave={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : "white"}
                  >
                    <span style={{ fontSize: 16, flexShrink: 0 }}>🏗️</span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: 600 }}>
                        {bloc.name}
                      </div>
                      <div style={{ display: "flex", gap: 4, marginTop: 2, alignItems: "center" }}>
                        <span style={{ fontSize: 9, padding: "1px 6px", background: PURPLE_LIGHT, color: PURPLE, borderRadius: 4, fontWeight: 700 }}>bloc</span>
                        <span style={{ fontSize: 9, color: GRAY_400 }}>{bloc.count} immeuble{bloc.count > 1 ? "s" : ""}</span>
                      </div>
                    </div>
                    {isSelected && <span style={{ color: AT_BLUE, fontSize: 14, flexShrink: 0 }}>✓</span>}
                  </div>
                );
              })}
            </>
          )}

          {/* ── Aucun résultat ── */}
          {displayedNamed.length === 0 && displayedBlocs.length === 0 && !loading && (
            <div style={{ padding: "20px 12px", textAlign: "center", color: GRAY_400, fontSize: 12 }}>Aucun résultat</div>
          )}

          {/* ── Hint ── */}
          {(filteredNamed.length + filteredBlocs.length) > 120 && (
            <div style={{ padding: "8px 12px", textAlign: "center", fontSize: 11, color: GRAY_400, background: GRAY_50, borderTop: `1px solid ${GRAY_100}` }}>
              Affinez votre recherche pour voir plus de résultats
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const SuccessModal = ({ isOpen, onClose, title, subtitle, message, icon = "fa-circle-check" }) => {
  if (!isOpen) return null;
  return (
    <div style={{
      position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh",
      background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center",
      justifyContent: "center", zIndex: 30000, backdropFilter: "blur(5px)",
      animation: "modalFadeIn 0.2s ease"
    }}>
      <div style={{
        background: "white", borderRadius: 16, width: "90%", maxWidth: 380,
        padding: "32px 24px", textAlign: "center", position: "relative",
        border: `1px solid ${GRAY_200}`,
        boxShadow: "0 20px 40px rgba(0,0,0,0.1)",
        animation: "modalSlideUp 0.3s ease-out"
      }}>
        <div style={{
          width: 64, height: 64, borderRadius: "50%",
          background: GREEN_LIGHT,
          display: "flex", alignItems: "center", justifyContent: "center",
          margin: "0 auto 20px", color: GREEN, fontSize: 32
        }}>
          <i className={`fas ${icon}`}></i>
        </div>

        {subtitle && (
          <div style={{ fontSize: 10, fontWeight: 800, color: GRAY_500, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 4 }}>
            {subtitle}
          </div>
        )}
        <h3 style={{ margin: "0 0 8px", color: GRAY_800, fontSize: 18, fontWeight: 700 }}>{title}</h3>
        <p style={{ margin: "0 0 24px", color: GRAY_600, fontSize: 14 }}>{message}</p>
        <button
          onClick={onClose}
          style={{
            background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`,
            color: "white", border: "none", borderRadius: 10,
            padding: "11px 40px", fontSize: 13, fontWeight: 700,
            cursor: "pointer", transition: "all 0.2s",
            boxShadow: "0 4px 12px rgba(0, 91, 170, 0.2)"
          }}
          onMouseEnter={e => {
            e.currentTarget.style.transform = "translateY(-1px)";
            e.currentTarget.style.boxShadow = "0 6px 15px rgba(0, 91, 170, 0.3)";
          }}
          onMouseLeave={e => {
            e.currentTarget.style.transform = "translateY(0)";
            e.currentTarget.style.boxShadow = "0 4px 12px rgba(0, 91, 170, 0.2)";
          }}
        >
          Continuer
        </button>
      </div>
      <style>{`
        @keyframes modalFadeIn { from { opacity: 0; } to { opacity: 1; } }
        @keyframes modalSlideUp {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: translateY(0); }
        }
      `}</style>
    </div>
  );
};


// ══════════════════════════════════════════════════════════════
//  MAIN APP
// ══════════════════════════════════════════════════════════════
export default function FTTHSmartPlanner() {
  const [screen, setScreen] = useState("login");
  const [notif, setNotif] = useState(null);
  const [modeModal, setModeModal] = useState({ open: false, title: "", message: "" });

  const [ville, setVille] = useState("");
  const [commune, setCommune] = useState("");
  const [residenceObj, setResidenceObj] = useState(null);

  const [villesOpts, setVillesOpts] = useState([]);
  const [communesOpts, setCommunesOpts] = useState([]);

  const [etages, setEtages] = useState(5);
  const [logements, setLogements] = useState(4);
  const [hauteurEtage, setHauteurEtage] = useState(3.0);
  const [presenceCommercial, setPresenceCommercial] = useState(false);
  const [programmeResidentiel, setProgrammeResidentiel] = useState("AADL");
  const [fatCap, setFatCap] = useState(8);
  const [planningMode, setPlanningMode] = useState("subscriber");

  // User Info
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const userName = user.prenom && user.nom ? `${user.prenom} ${user.nom}` : (user.full_name || user.name || "Utilisateur");
  const userInitials = (user.prenom && user.nom)
    ? (user.prenom[0] + user.nom[0]).toUpperCase()
    : userName.split(" ").map(n => n[0]).join("").toUpperCase().slice(0, 2);

  const [osmLoaded, setOsmLoaded] = useState(false);
  const [osmLoading, setOsmLoading] = useState(false);
  const [planGenerated, setPlanGenerated] = useState(false);
  const [kpis, setKpis] = useState(null);
  const [rawBuildings, setRawBuildings] = useState(null);
  const [subscribersData, setSubscribersData] = useState([]);
  const [fatResults, setFatResults] = useState([]);
  const [primaryTargetId, setPrimaryTargetId] = useState(null);
  const [lastImportedId, setLastImportedId] = useState(null);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [sectorisationSnapshot, setSectorisationSnapshot] = useState(null);
  const [saveLoading, setSaveLoading] = useState(false);
  const [archiveOpen, setArchiveOpen] = useState(false);
  const [projects, setProjects] = useState([]);
  const [treatedOsmIds, setTreatedOsmIds] = useState(new Set());
  const [communeGeoJson, setCommuneGeoJson] = useState(null);
  const [osmIdToProjectId, setOsmIdToProjectId] = useState({});
  const [isSectorising, setIsSectorising] = useState(false);

  const [pendingRelaunch, setPendingRelaunch] = useState(false);

  const [cablesStandards, setCablesStandards] = useState([15, 20, 50, 80]);

  useEffect(() => {
    fetch(`${API}/api/config`)
      .then(r => r.json())
      .then(data => {
        if (data.cable_standards) {
          setCablesStandards(data.cable_standards);
          console.log("🔌 Standard de câbles AT chargé :", data.cable_standards);
        }
      })
      .catch(err => console.error("Erreur de chargement des câbles standards :", err));
  }, []);

  const buildingPlanRef = useRef(null);

  // Récupération permanente des bâtiments déjà enregistrés dans la BD pour la commune active
  // PAR :
  useEffect(() => {
    if (screen !== "dashboard" || !ville || !commune) {
      setTreatedOsmIds(new Set());
      setCommuneGeoJson(null);
      return;
    }
    getTreatedBuildings(ville, commune).then(rows => {
      console.log(`🏠 [DB] ${rows.length} bâtiments déjà traités chargés pour ${commune}`);
      const allIds = new Set();
      rows.forEach(row => {
        const id = row.osm_id;
        if (!id) return;
        const s = String(id);
        allIds.add(s);
        allIds.add(s.replace(/^(way|node|relation)\//i, ''));
      });
      setTreatedOsmIds(allIds);

      const idMap = {};
      rows.forEach(row => {
        if (row.osm_id && row.id_projet) {
          idMap[String(row.osm_id)] = row.id_projet;
          idMap[String(row.osm_id).replace(/^(way|node|relation)\//i, '')] = row.id_projet;
        }
      });
      setOsmIdToProjectId(idMap);

      // Construire un GeoJSON pour la vue commune à partir des geom_poly stockées
      const features = rows
        .filter(row => row.geom_poly || row.geom)
        .map(row => {
          const geomRaw = row.geom_poly || row.geom;
          let geometry = null;
          // geom_poly peut être un objet GeoJSON ou une string WKT
          if (geomRaw && typeof geomRaw === "object" && geomRaw.type) {
            geometry = geomRaw;
          } else if (geomRaw && typeof geomRaw === "string" && geomRaw.startsWith("{")) {
            try { geometry = JSON.parse(geomRaw); } catch { geometry = null; }
          } else if (geomRaw && typeof geomRaw === "string") {
            geometry = parseWKT(geomRaw);
          }
          if (!geometry) return null;
          const nomProjet = row.projets?.nom_projet || row.osm_id || "Bât.";
          return {
            type: "Feature",
            properties: {
              id_batiment: row.osm_id,
              osm_id_real: row.osm_id,
              nom_batiment: nomProjet,
              is_target: false,
            },
            geometry,
          };
        })
        .filter(Boolean);

      if (features.length > 0) {
        setCommuneGeoJson({ type: "FeatureCollection", features });
      } else {
        setCommuneGeoJson(null);
      }
    });
  }, [screen, ville, commune]);
  const [projectsLoading, setProjectsLoading] = useState(false);
  const [loadingProjectId, setLoadingProjectId] = useState(null);

  const notify = useCallback((type, title, sub) => setNotif({ type, title, sub }), []);

  // Regénérer les abonnés virtuels si l'utilisateur change la structure manuellement
  useEffect(() => {
    if (osmLoaded && residenceObj && !planGenerated) {
      const newSubs = [];
      let cc = 1;
      let gp_com = 1;
      let gp_log = 1;
      const centroid_lat = residenceObj.lat;
      const centroid_lon = residenceObj.lon;
      const esp_deg = 0.00005; // Approximation simple
      const start_lon = centroid_lon - (logements - 1) * esp_deg / 2;

      if (presenceCommercial) {
        for (let i = 0; i < logements; i++) {
          newSubs.push({
            code_client: `AB${String(cc).padStart(6, '0')}`,
            id_batiment: primaryTargetId || residenceObj.osm_id,
            lat_abonne: centroid_lat,
            lon_abonne: start_lon + i * esp_deg,
            etage: 0,
            porte: gp_com,
            usage: "commerces"
          });
          cc++; gp_com++;
        }
      }
      for (let etg = 1; etg <= etages; etg++) {
        for (let i = 0; i < logements; i++) {
          newSubs.push({
            code_client: `AB${String(cc).padStart(6, '0')}`,
            id_batiment: primaryTargetId || residenceObj.osm_id,
            lat_abonne: centroid_lat,
            lon_abonne: start_lon + i * esp_deg,
            etage: etg,
            porte: gp_log,
            usage: "logements"
          });
          cc++; gp_log++;
        }
      }
      setSubscribersData(newSubs);
    }
  }, [etages, logements, presenceCommercial, osmLoaded, residenceObj, primaryTargetId, planGenerated]);

  // Fermer le menu si on clique ailleurs
  useEffect(() => {
    const handleOutsideClick = (e) => {
      if (showExportMenu && !e.target.closest('.export-dropdown-container')) {
        setShowExportMenu(false);
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    return () => document.removeEventListener('mousedown', handleOutsideClick);
  }, [showExportMenu]);

  const handleExport = async (format) => {
    if (!planGenerated || !sectorisationSnapshot) {
      notify("warning", "Export impossible", "Générez d'abord un plan de sectorisation.");
      return;
    }

    const { fatResults } = sectorisationSnapshot;
    const headers = ["Type d'équipement", "Nom de l'équipement", "Ville", "Commune", "Nom du projet", "Étage Placement", "Ports Occupés"];

    const rows = (fatResults || []).map(fat => [
      "FAT",
      fat.fat_id_AT || fat.fat_id || "—",
      ville,
      commune,
      residenceObj?.name || "Projet",
      fat.etage_fat ?? "—",
      fat.subscriber_ids?.length || 0
    ]);

    // 2ème tableau: Synthèse des Branchements
    const brHeaders = ["FAT", "Adresse", "Étage", "Distance (m)", "Câble (m)"];
    const brRows = (sectorisationSnapshot.subscribersData || [])
      .filter(sub => {
        const isOccupied = sectorisationSnapshot.planningMode !== "prediction" || sub.habite === 1;
        const hasFat = fatResults.some(f => f.subscriber_ids?.includes(sub.code_client));
        return isOccupied && hasFat;
      })
      .map(sub => {
        const connectedFat = fatResults.find(f => f.subscriber_ids?.includes(sub.code_client));
        const realConnection = sectorisationSnapshot?.connections?.find(c => c.code_client === sub.code_client);
        const diffEtage = Math.abs(sub.etage - (connectedFat?.etage_fat || 0));
        const hauteurEstimee = (diffEtage * (sectorisationSnapshot.hauteurEtage || 3)) + 8;
        const distanceVal = realConnection?.distance_real_m ?? (connectedFat?.cable_snap_m ? (connectedFat.cable_snap_m * 0.85).toFixed(2) : (hauteurEstimee * 0.85).toFixed(2));
        const cableVal = realConnection?.cable_snap_m ?? (connectedFat?.cable_snap_m || Math.ceil(hauteurEstimee));
        return [
          connectedFat?.fat_id_AT || connectedFat?.fat_id || "—",
          sub.usage === "commerces" ? `Commerce ${sub.porte}` : `Porte ${sub.porte}`,
          sub.etage,
          distanceVal,
          cableVal
        ];
      });

    const fileName = `Export_FTTH_${residenceObj?.name || "Projet"}_${new Date().toISOString().slice(0, 10)}`;

    if (format === "pdf") {
      if (!buildingPlanRef.current) {
        notify("warning", "Plan non disponible", "Générez d'abord un plan de sectorisation.");
        return;
      }
      // Charger html2canvas dynamiquement depuis CDN
      const loadHtml2canvas = () => new Promise((resolve, reject) => {
        if (window.html2canvas) { resolve(window.html2canvas); return; }
        const s = document.createElement("script");
        s.src = "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js";
        s.onload = () => resolve(window.html2canvas);
        s.onerror = reject;
        document.head.appendChild(s);
      });

      notify("info", "Génération PDF…", "Capture du plan en cours…");
      try {
        const html2canvas = await loadHtml2canvas();
        const canvas = await html2canvas(buildingPlanRef.current, {
          scale: 2,
          backgroundColor: "#ffffff",
          useCORS: true,
          logging: false
        });
        const imgData = canvas.toDataURL("image/png");
        const modeLabel = sectorisationSnapshot.planningMode === "prediction" ? "Réseau Estimé" : "Réseau Connecté";
        const modeBg = sectorisationSnapshot.planningMode === "prediction" ? "#F3E8FF" : "#E8F1FA";
        const modeColor = sectorisationSnapshot.planningMode === "prediction" ? "#7C3AED" : "#005BAA";
        const htmlContent = `<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">
          <title>Plan 2D — ${residenceObj?.name || "Projet"}</title>
          <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700;800&display=swap" rel="stylesheet">
          <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { font-family: 'Outfit', sans-serif; color: #1F2937; background: #fff; padding: 20px; }
            img { max-width: 100%; max-height: calc(297mm - 70mm); object-fit: contain; display: block; margin: 0 auto; border-radius: 8px; border: 1px solid #E5E7EB; }
            @media print {
              body { padding: 10px; }
              @page { size: A4 portrait; margin: 8mm; }
              img { max-height: calc(297mm - 55mm); }
            }
          </style>
        </head><body>
          <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:3px solid #005BAA;padding-bottom:14px;margin-bottom:20px;">
            <div>
              <div style="font-size:20px;font-weight:800;color:#005BAA;">Plan de Sectorisation FTTH</div>
              <div style="font-size:12px;color:#6B7280;margin-top:3px;">${residenceObj?.name || "Projet"} · ${commune || ""} · ${ville || ""}</div>
            </div>
            <div style="text-align:right;">
              <div style="font-size:11px;padding:3px 10px;border-radius:20px;background:${modeBg};color:${modeColor};font-weight:700;display:inline-block;">${modeLabel}</div>
              <div style="font-size:11px;color:#9CA3AF;margin-top:5px;">Généré le ${new Date().toLocaleDateString("fr-DZ")}</div>
            </div>
          </div>
          <img src="${imgData}" />
          <script>window.onload = function(){ window.print(); }<\/script>
        </body></html>`;
        const win = window.open("", "_blank");
        win.document.write(htmlContent);
        win.document.close();
        notify("success", "PDF prêt", "La fenêtre d'impression s'est ouverte.");
      } catch (err) {
        notify("error", "Erreur PDF", err.message);
      }
      return;
    }

    if (format === "excel") {
      const xmlStyles = `
        <Styles>
          <Style ss:ID="sHeader">
            <Font ss:Bold="1" ss:Color="#FFFFFF"/>
            <Interior ss:Color="#005BAA" ss:Pattern="Solid"/>
            <Borders>
              <Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Left" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Right" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Top" ss:LineStyle="Continuous" ss:Weight="1"/>
            </Borders>
          </Style>
          <Style ss:ID="sRow">
            <Borders>
              <Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Left" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Right" ss:LineStyle="Continuous" ss:Weight="1"/>
              <Border ss:Position="Top" ss:LineStyle="Continuous" ss:Weight="1"/>
            </Borders>
          </Style>
        </Styles>`;

      const renderRows = (data) => data.map(r => `
        <Row>
          ${r.map(c => `<Cell ss:StyleID="sRow"><Data ss:Type="String">${c}</Data></Cell>`).join("")}
        </Row>`).join("");

      const renderHeaders = (hList) => `
        <Row>
          ${hList.map(h => `<Cell ss:StyleID="sHeader"><Data ss:Type="String">${h}</Data></Cell>`).join("")}
        </Row>`;

      const xmlTemplate = `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:html="http://www.w3.org/TR/REC-html40">
 ${xmlStyles}
 <Worksheet ss:Name="Inventaire FAT">
  <Table>
   ${renderHeaders(headers)}
   ${renderRows(rows)}
  </Table>
 </Worksheet>
 <Worksheet ss:Name="Synthèse Branchements">
  <Table>
   ${renderHeaders(brHeaders)}
   ${renderRows(brRows)}
  </Table>
 </Worksheet>
</Workbook>`;

      const blob = new Blob([xmlTemplate], { type: "application/vnd.ms-excel" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${fileName}.xls`;
      link.click();
    }

    notify("success", "Export réussi", `Fichier ${format.toUpperCase()} généré.`);
  };


  const handleRestart = () => {
    setVille(""); setCommune(""); setResidenceObj(null);
    setOsmLoaded(false); setPlanGenerated(false); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
    setRawBuildings(null); setSubscribersData([]); setPrimaryTargetId(null); setLastImportedId(null);
    setSavedProjectId(null);
    setRawBuildings(null); setCommuneGeoJson(null);
    notify("info", "Réinitialisation", "Vous pouvez commencer un nouveau projet.");
  };


  useEffect(() => {
    // Ne charger les wilayas que lorsque l'utilisateur est connecté (écran principal)
    if (screen !== "dashboard") return;
    console.log("🚀 Chargement initial des wilayas...");
    fetch(`${API}/api/ville`)
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(data => {
        console.log("✅ Wilayas reçues:", data.villes?.length, "entrées — source:", data.source);
        if (data.villes && data.villes.length > 0) {
          setVillesOpts(data.villes);
        } else {
          console.error("❌ Le backend a retourné une liste de wilayas vide");
        }
      })
      .catch(err => console.error("❌ Erreur wilayas:", err));
  }, [screen]);



  const handleResidenceSelect = (res) => {
    console.log("🏢 Résidence/Cité sélectionné :", res);
    setResidenceObj(res);
    setPrimaryTargetId(null); setLastImportedId(null);
    setOsmLoaded(false); setRawBuildings(null);
    setPlanGenerated(false); setSubscribersData([]); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
  };


  useEffect(() => {
    if (!ville) {
      setCommunesOpts([]);
      return;
    }
    console.log(`🔍 Chargement des communes pour : ${ville}...`);
    fetch(`${API}/api/commune?ville=${encodeURIComponent(ville)}`)
      .then(r => r.json())
      .then(d => {
        console.log(`🏙️ Communes reçues (${ville}) :`, d.communes);
        setCommunesOpts(d.communes || []);
      })
      .catch(() => notify("error", "Erreur", "Impossible de charger les communes"));
  }, [ville, notify]);


  const importOSM = async () => {
    if (!ville || !commune || !residenceObj) { notify("error", "Données manquantes", "Veuillez sélectionner votre cible"); return; }
    setOsmLoading(true);
    console.log("Lancement de l'import OSM pour :", residenceObj.name);
    try {
      const resp = await fetch(`${API}/api/importOSM`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ville, commune, residence: residenceObj.name, lat: residenceObj.lat, lon: residenceObj.lon, nombre_etages: etages, logements_par_etage: logements, commerce: presenceCommercial }) });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "Erreur Import");

      console.log("📦 Données OSM reçues :", data);
      setRawBuildings(data.buildings_geojson);
      setSubscribersData(data.subscribers);
      if (data.etages_detectes) setEtages(data.etages_detectes);
      if (data.logements_detectes) setLogements(data.logements_detectes);

      // Extraire le vrai OSM ID de la cible depuis le GeoJSON retourné
      let targetRealOsmId = residenceObj.osm_id;
      try {
        const gj = typeof data.buildings_geojson === "string" ? JSON.parse(data.buildings_geojson) : data.buildings_geojson;
        const targetFeat = gj?.features?.find(f => f.properties?.is_target);
        if (targetFeat?.properties?.osm_id_real) targetRealOsmId = targetFeat.properties.osm_id_real;
      } catch (e) { console.warn("[importOSM] Impossible d'extraire osm_id_real :", e); }

      setOsmLoaded(true);
      setLastImportedId(targetRealOsmId);
      setPrimaryTargetId(targetRealOsmId);
      setResidenceObj(prev => prev ? { ...prev, osm_id: targetRealOsmId } : prev);

      notify("success", "Carte & Données synchronisées", "Bâtiment ciblé et voisinage importés");
    } catch (err) { notify("error", "Erreur réseau", err.message); }
    finally { setOsmLoading(false); }
  };

  const lancerSectorisation = async () => {
    setIsSectorising(true);
    // Réinitialisation immédiate pour forcer la mise à jour visuelle
    setPlanGenerated(false);
    setFatResults([]);
    setKpis(null);
    setSectorisationSnapshot(null);

    try {
      if (!osmLoaded && planningMode !== "prediction") {
        notify("info", "Import requis", "Importez d'abord les données de la résidence");
        return;
      }

      // ── Mode 2 — K-Predictor (sans abonnés) ──────────────────────────────────
      if (planningMode === "prediction") {
        if (!residenceObj) { notify("error", "Données manquantes", "Aucune résidence sélectionnée."); return; }

        const body = {
          nb_etages: etages,
          nb_log_etage: logements,
          hauteur_etage: hauteurEtage,
          type_batiment: programmeResidentiel,
          presence_commerce: presenceCommercial ? 1 : 0,
          id_batiment: residenceObj.osm_id,
          centroid_lat: residenceObj.lat,
          centroid_lon: residenceObj.lon,
          fat_balance_thr: 0.75,
          ville: ville,
        };
        const resp = await fetch(`${API}/api/emplacementFATs/predict`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.detail || `Erreur K-Predictor (${resp.status})`);

        const finalFats = (data.fat_candidates || []).map(fat => ({
          ...fat,
          subscriber_ids: fat.subscriber_ids || []
        }));
        const kPred = data.k_prediction || {};

        const simulatedSubs = (data.subscribers || []).map((sub, idx) => {
          // Ajustement de l'étage : Le backend K-Predictor est souvent en 0-indexé.
          // En frontend : 0 = Commercial, 1+ = Résidentiel.
          // Si pas de commerce, le backend envoie 0 pour le 1er étage -> on doit le passer à 1.
          const adjustedEtage = sub.etage;

          let porteVal;
          if (sub.appt_idx !== undefined) {
            porteVal = (sub.usage === "commerces")
              ? (sub.appt_idx + 1)
              : ((adjustedEtage - 1) * logements + (sub.appt_idx + 1));
          } else {
            porteVal = idx + 1;
          }
          return {
            id: sub.id || porteVal,
            code_client: sub.id || porteVal,
            etage: adjustedEtage,
            porte: porteVal,
            usage: sub.usage,
            lat_abonne: sub.lat,
            lon_abonne: sub.lon,
            habite: sub.habite,
            raccorde: sub.raccorde,
          };
        });

        const reqNom = await fetch(`${API}/api/nomFAT`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            fat_candidates: finalFats,
            subscribers: simulatedSubs,
            ville: ville,
            residence: residenceObj?.name
          })
        });
        const dataNom = await reqNom.json();
        const namedFats = dataNom.fat_candidates_with_ids || finalFats;

        const actualConnectedSubs = namedFats.reduce((sum, f) => sum + (f.subscriber_ids?.length || f.n_subscribers || 0), 0);

        const kpiObj = {
          totalAbonnes: actualConnectedSubs,
          fatsNeeded: namedFats.length,
          fatsPortsUsed: Math.round((actualConnectedSubs / (namedFats.length * fatCap || 1)) * 100),
          lineaire: Math.round(simulatedSubs.reduce((sum, sub) => {
            const connectedFat = namedFats.find(f => f.subscriber_ids?.includes(sub.code_client));
            if (!connectedFat) return sum;
            const diffEtage = Math.abs(sub.etage - (connectedFat.etage_fat || 0));
            const verticalM = diffEtage * (hauteurEtage || 3);
            const geoM = haversineM(sub.lat_abonne, sub.lon_abonne, connectedFat.centroid_lat, connectedFat.centroid_lon);
            const cableVal = geoM !== null
              ? snapDropCable(geoM + verticalM + 4.0, cablesStandards)
              : (connectedFat.cable_snap_m || snapDropCable(diffEtage * (hauteurEtage || 3) + 4.0, cablesStandards));
            return sum + cableVal;
          }, 0)),
        };

        setFatResults(namedFats);
        setKpis(kpiObj);
        setSectorisationSnapshot({
          fatResults: namedFats,
          subscribersData: simulatedSubs,
          connections: data.connections || [],
          etages: etages,
          logements: logements,
          residenceName: residenceObj?.name,
          presenceCommercial: presenceCommercial,
          planningMode: "prediction",
          kPrediction: kPred,
          timestamp: Date.now(),
        });
        setPlanGenerated(true);
        return;
      }

      // ── Mode 1 — Greedy Vertical (abonnés connus) ─────────────────────────────
      if (!subscribersData || subscribersData.length === 0) { notify("error", "Données manquantes", "Aucun abonné détecté."); return; }
      notify("info", "Traitement Algorithmique", "Positionnement dynamique...");

      const req1 = await fetch(`${API}/api/emplacementFATs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          subscribers: subscribersData,
          hauteur_etage: hauteurEtage
        })
      });
      const data1 = await req1.json();
      if (!req1.ok) throw new Error(data1.detail || `Erreur FAT (${req1.status})`);

      const req2 = await fetch(`${API}/api/nomFAT`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fat_candidates: data1.fat_candidates,
          subscribers: subscribersData,
          ville: ville,
          residence: residenceObj?.name
        })
      });
      const data2 = await req2.json();
      if (!req2.ok) throw new Error(data2.detail || `Erreur nommage FAT (${req2.status})`);

      const finalFats = data2.fat_candidates_with_ids || data1.fat_candidates;
      const connectedSubsCount = finalFats.reduce((sum, f) => sum + (f.subscriber_ids?.length || f.n_subscribers || 0), 0);

      const kpiObj = {
        totalAbonnes: connectedSubsCount,
        fatsNeeded: finalFats.length,
        fatsPortsUsed: Math.round((connectedSubsCount / (finalFats.length * fatCap || 1)) * 100),
        lineaire: Math.round((data1.connections || []).reduce((sum, c) => sum + (c.cable_snap_m || 0), 0))
      };

      setFatResults(finalFats);
      setKpis(kpiObj);
      setSectorisationSnapshot({
        fatResults: finalFats,
        subscribersData: [...subscribersData],
        connections: data1.connections || [],
        etages: etages,
        logements: logements,
        residenceName: residenceObj?.name,
        presenceCommercial: presenceCommercial,
        planningMode: "subscriber",
        timestamp: Date.now()
      });
      setPlanGenerated(true);
      notify("success", "Sectorisation terminée", `Topologie générée pour ${finalFats.length} boîtiers`);
    } catch (err) {
      notify("error", "Échec process", err.message);
    } finally {
      setIsSectorising(false);
    }
  };

  // Auto-relaunch sectorisation when mode is switched while a plan is active
  const lancerSectorisationRef = useRef(null);
  useEffect(() => { lancerSectorisationRef.current = lancerSectorisation; });
  useEffect(() => {
    if (!pendingRelaunch) return;
    setPendingRelaunch(false);
    // Small delay to let the loader appear before heavy computation
    setTimeout(() => { lancerSectorisationRef.current?.(); }, 100);
  }, [pendingRelaunch]);

  const [showSaveSuccess, setShowSaveSuccess] = useState(false);
  const [showOverwriteConfirm, setShowOverwriteConfirm] = useState(false);
  const [savedProjectId, setSavedProjectId] = useState(null);

  const saveProject = async () => {
    if (!planGenerated || !sectorisationSnapshot) {
      notify("warning", "Rien à sauvegarder", "Lancez d'abord une sectorisation");
      return;
    }

    const user = JSON.parse(localStorage.getItem("user") || "{}");
    if (!user.id) { notify("error", "Non connecté", "Utilisateur introuvable"); return; }

    const nomProjet = sectorisationSnapshot.residenceName || residenceObj?.name || "Projet sans nom";

    setSaveLoading(true);
    try {
      const projects = await listProjects(user.id);
      const exists = projects.some(p => p.nom_projet === nomProjet);

      setSaveLoading(false);
      if (exists) {
        setShowOverwriteConfirm(true);
      } else {
        executeSave();
      }
    } catch (err) {
      setSaveLoading(false);
      notify("error", "Erreur de vérification", err.message);
    }
  };

  const executeSave = async () => {
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    if (!user.id) { notify("error", "Non connecté", "Utilisateur introuvable"); return; }

    setShowOverwriteConfirm(false);
    setSaveLoading(true);
    try {
      // Extraction du polygone et du vrai ID OSM si disponible dans rawBuildings
      let geomPoly = null;
      let actualOsmId = residenceObj?.osm_id || null;

      if (rawBuildings) {
        try {
          const geojson = typeof rawBuildings === "string" ? JSON.parse(rawBuildings) : rawBuildings;
          // Chercher le bâtiment cible par is_target ou par osm_id_real (vrai ID OSM)
          const targetFeature = geojson.features.find(f =>
            f.properties.is_target ||
            f.properties.osm_id_real === residenceObj?.osm_id ||
            f.properties.id_batiment === residenceObj?.osm_id
          );
          if (targetFeature) {
            if (targetFeature.geometry) geomPoly = targetFeature.geometry;
            if (targetFeature.properties?.osm_id_real) {
              actualOsmId = targetFeature.properties.osm_id_real;
            } else if (targetFeature.properties?.id_batiment) {
              actualOsmId = targetFeature.properties.id_batiment;
            }
          }
        } catch (e) { console.error("Erreur extraction polygone:", e); }
      }

      // S'assurer que l'osm_id est bien un entier pour Supabase (bigint)
      let cleanOsmId = null;
      if (actualOsmId) {
        // Extraire uniquement les chiffres (pour gérer way/12345 ou CIBLE-12345)
        const digits = String(actualOsmId).replace(/\D/g, '');
        if (digits) cleanOsmId = parseInt(digits, 10);
      }

      const immeubles = [{
        osm_id: cleanOsmId,
        lat: residenceObj?.lat || 0,
        lon: residenceObj?.lon || 0,
        geom_poly: geomPoly,
        etage: etages,
        logement_par_etage: logements,
        presence_commerce: presenceCommercial,
        wilaya: ville || null,
        commune: commune || null,
        hauteur_etage: hauteurEtage,
        nbr_abonne: (subscribersData || []).length || null,
        source_type: "osm",
      }];

      const fats = (sectorisationSnapshot.fatResults || []).map(f => ({
        fat_id_AT: f.fat_id_AT || f.fat_id || null,
        lat: f.centroid_lat,
        lon: f.centroid_lon,
        etage_fat: f.etage_fat ?? null,
        usage: f.usage || "logements",
        cable_snap_m: f.cable_snap_m ?? null,
        capacite: f.capacite || 8,
        subscriber_ids: f.subscriber_ids || [],
      }));

      // APRÈS — porte locale à l'étage (index de colonne + 1)
const subToPorte = {};

// Grouper par étage + usage, trier par longitude, assigner porte locale 1..N
const allSubs = sectorisationSnapshot.subscribersData || [];
const etageUsageGroups = {};
for (const ab of allSubs) {
  const key = `${ab.etage}_${ab.usage}`;
  if (!etageUsageGroups[key]) etageUsageGroups[key] = [];
  etageUsageGroups[key].push(ab);
}
for (const group of Object.values(etageUsageGroups)) {
  group.sort((a, b) => (a.lon_abonne || 0) - (b.lon_abonne || 0));
  group.forEach((ab, idx) => {
    subToPorte[ab.code_client] = idx + 1; // porte locale 1..logements
  });
}

      const abonnes = allSubs.map(ab => ({
        code_client: ab.code_client,
        lon: ab.lon_abonne || ab.lon || 0,
        lat: ab.lat_abonne || ab.lat || 0,
        etage: ab.etage ?? 1,
        porte: subToPorte[ab.code_client] ?? ab.porte ?? 1,
        nom: ab.nom || null,
        usage: ab.usage || "logements",
      }));

      const fat_abonnes = [];
      for (const f of fats) {
        for (const code_client of (f.subscriber_ids || [])) {
          fat_abonnes.push({ fat_id_AT: f.fat_id_AT, code_client, cable_snap_m: f.cable_snap_m ?? null, distance_real_m: null });
        }
      }

      const result = await spSave({
        userId: user.id,
        nomProjet: sectorisationSnapshot.residenceName || residenceObj?.name || "Projet sans nom",
        planningMode: sectorisationSnapshot.planningMode,
        ville: ville || null,
        commune: commune || null,
        immeubles, fats, abonnes, fatAbonnes: fat_abonnes,
      });

      setSavedProjectId(result.id_projet);
      setShowSaveSuccess(true);

      const rows = await getTreatedBuildings(ville, commune);
      const allIds = new Set();
      const idMap = {};
      rows.forEach(row => {
        const id = row.osm_id;
        if (!id) return;
        const s = String(id);
        allIds.add(s);
        allIds.add(s.replace(/^(way|node|relation)\//i, ''));
        if (row.id_projet) {
          idMap[s] = row.id_projet;
          idMap[s.replace(/^(way|node|relation)\//i, '')] = row.id_projet;
        }
      });
      setTreatedOsmIds(allIds);
      setOsmIdToProjectId(idMap);
    } catch (err) {
      notify("error", "Échec sauvegarde", err.message);
    } finally {
      setSaveLoading(false);
    }
  };


  const loadProjectsList = async () => {
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    if (!user.id) { notify("error", "Non connecté", "Reconnectez-vous"); return; }
    setArchiveOpen(true);
    setProjectsLoading(true);
    try {
      const projects = await listProjects(user.id);
      setProjects(projects);
    } catch (err) {
      notify("error", "Impossible de charger les projets", err.message);
    } finally {
      setProjectsLoading(false);
    }
  };

  const loadProject = async (id_projet) => {
    setLoadingProjectId(id_projet);
    // Redémarrer the dashboard to clear previous data while loading
    setResidenceObj(null);
    setOsmLoaded(false);
    setPlanGenerated(false);
    setFatResults([]);
    setSubscribersData([]);
    setKpis(null);
    setSectorisationSnapshot(null);
    setLastImportedId(null);
    setRawBuildings(null);
    setCommuneGeoJson(null);
    try {
      const data = await spLoad(id_projet);
      if (data.ville) setVille(data.ville);
      if (data.commune) setCommune(data.commune);
      if (data.residenceObj) {
        setResidenceObj(data.residenceObj);

        // Reconstruire la visualisation cartographique depuis l'archive (geom_poly)
        if (data.residenceObj.geom_poly) {
          const buildingFeature = {
            type: "Feature",
            geometry: data.residenceObj.geom_poly,
            properties: {
              id_batiment: data.residenceObj.osm_id,
              nom_batiment: data.residenceObj.name,
              is_target: true,
              bat_levels: data.etages || 5,
              bat_units: (data.etages || 5) * (data.logements || 4),
              centroid_lat: data.residenceObj.lat,
              centroid_lon: data.residenceObj.lon
            }
          };
          setRawBuildings(JSON.stringify({
            type: "FeatureCollection",
            features: [buildingFeature]
          }));
          setPrimaryTargetId(data.residenceObj.osm_id);
        }
      }
      setEtages(data.etages || 5);
      setLogements(data.logements || 4);
      setHauteurEtage(data.hauteurEtage || 3.0);
      setPresenceCommercial(data.presenceCommercial || false);
      setPlanningMode(data.planningMode || "subscriber");
      setFatResults(data.sectorisationSnapshot?.fatResults || []);
      setSubscribersData(data.sectorisationSnapshot?.subscribersData || []);
      setKpis(data.kpis);
      setSectorisationSnapshot({ ...data.sectorisationSnapshot, timestamp: Date.now() });
      setPlanGenerated(true);
      setOsmLoaded(true);
      if (data.residenceObj?.osm_id) setLastImportedId(data.residenceObj.osm_id);
      setArchiveOpen(false);
      notify("success", "Projet rechargé ✓", data.nom_projet);
    } catch (err) {
      notify("error", "Échec chargement projet", err.message);
    } finally {
      setLoadingProjectId(null);
    }
  };
  const loadProjectRef = useRef(loadProject);
  useEffect(() => { loadProjectRef.current = loadProject; }, [loadProject]);
  const handleBuildingClick = useCallback((properties) => {
    if (!properties) return;

    // Si bâtiment traité sur la carte commune ou voisinage -> ouvrir son projet archivé
    const bid = properties.id_batiment;
    const realId = properties.osm_id_real || bid;
    const cleanId = realId ? String(realId).replace(/^(way|node|relation)\//i, '') : "";

    const projectId = osmIdToProjectId[String(realId)]
      || osmIdToProjectId[String(bid)]
      || osmIdToProjectId[cleanId];

    if (projectId) {
      // Le clic ouvre le projet archivé directement
      loadProjectRef.current(projectId);
      return;
    }

    console.log("📍 Bâtiment cliqué (Détails OSM) :", properties);
    // Utiliser le vrai OSM ID (osm_id_real) pour la correspondance DB, sinon id_batiment formaté
    const newObj = { name: properties.nom_batiment || "Bâtiment", osm_id: realId, lat: properties.centroid_lat, lon: properties.centroid_lon };

    // Si on change de cible, on invalide l'import précédent
    if (residenceObj?.osm_id !== realId) {
      setOsmLoaded(false);
      setRawBuildings(null);
    }

    setResidenceObj(newObj);
    if (properties.bat_levels) setEtages(properties.bat_levels);
    if (properties.bat_units) setLogements(Math.max(1, Math.floor(properties.bat_units / Math.max(1, properties.bat_levels || 1))));

    setPlanGenerated(false);
    setFatResults([]);
    setKpis(null);
    setSectorisationSnapshot(null);
    notify("info", "Bâtiment sélectionné", `Prêt pour l'import : ${properties.nom_batiment}`);
  }, [osmIdToProjectId, residenceObj, notify]);



  const inputStyle = { width: "100%", padding: "10px 14px", background: "white", border: `1.5px solid ${GRAY_200}`, borderRadius: 8, color: GRAY_800, fontSize: 13, boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 700, color: GRAY_600, marginBottom: 5, display: "block", letterSpacing: "0.5px", textTransform: "uppercase" };
  const btnPrimary = { background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`, color: "white", border: "none", borderRadius: 8, padding: "11px 20px", fontSize: 13, fontWeight: 700, cursor: "pointer", width: "100%" };
  const cardStyle = { background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.05)", marginBottom: 14 };
  const globalStyle = `
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    @keyframes spin { to { transform: rotate(360deg) } }
    html, body, #root {
      width: 100% !important;
      height: 100% !important;
      margin: 0 !important;
      padding: 0 !important;
      overflow: hidden !important;
    }
    body { background: ${GRAY_50}; }
    select:focus, input:focus { outline:none; border-color:${AT_BLUE} !important; }
    .target-building-label { pointer-events:none !important; background:transparent; border:none; box-shadow:none; color:#005BAA; font-weight:800; font-size:10px; text-shadow:1px 1px 2px white,-1px -1px 2px white,1px -1px 2px white,-1px 1px 2px white; text-align:center; white-space:nowrap; }
    .treated-building-label {
      pointer-events: auto !important;
      cursor: pointer;
      background: #10B981 !important;
      border: none !important;
      color: white !important;
      font-weight: 800 !important;
      font-size: 13px !important;
      padding: 6px 14px !important;
      border-radius: 12px !important;
      text-align: center !important;
      white-space: nowrap !important;
      box-shadow: 0 4px 12px rgba(16, 185, 129, 0.25) !important;
    }
    .treated-building-label.treated-selected {
      box-shadow: 0 0 0 3px rgba(0, 91, 170, 0.4), 0 4px 12px rgba(16, 185, 129, 0.3) !important;
      border: 1px solid white !important;
    }
    .neighbor-building-label { pointer-events:none !important; background:transparent; border:none; box-shadow:none; color:#4B5563; font-weight:700; font-size:9px; text-shadow:1px 1px 2px white,-1px -1px 2px white,1px -1px 2px white,-1px 1px 2px white; text-align:center; opacity:0.85; white-space:nowrap; }
    .radius-label { background: rgba(0, 91, 170, 0.85) !important; border: none !important; color: white !important; font-weight: 700 !important; font-size: 9px !important; border-radius: 4px !important; padding: 2px 6px !important; box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important; }
    .radius-label:before { border-top-color: rgba(0, 91, 170, 0.85) !important; }
    .leaflet-tooltip-top:before,.leaflet-tooltip-bottom:before,.leaflet-tooltip-left:before,.leaflet-tooltip-right:before { border:none !important; display:none; }

    .fat-node-container:hover { transform: translateY(-3px); transition: transform 0.2s; }
    .fat-tooltip {
      position: absolute;
      bottom: 115%;
      left: 50%;
      transform: translateX(-50%) translateY(8px);
      background: #1F2937;
      color: white;
      padding: 6px 10px;
      border-radius: 6px;
      font-size: 10px;
      font-weight: 700;
      white-space: nowrap;
      z-index: 9999;
      opacity: 0;
      pointer-events: none;
      transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
      box-shadow: 0 4px 12px rgba(0,0,0,0.4);
      display: flex;
      flex-direction: column;
      border: 1px solid rgba(255,255,255,0.15);
    }
    .fat-node-container:hover .fat-tooltip {
      opacity: 1;
      transform: translateX(-50%) translateY(0);
    }
    .fat-tooltip:after {
      content: "";
      position: absolute;
      top: 100%;
      left: 50%;
      margin-left: -6px;
      border-width: 6px;
      border-style: solid;
      border-color: #1F2937 transparent transparent transparent;
    }

    /* Export Dropdown */
    .export-dropdown-container { position: relative; display: inline-block; }
    .export-dropdown-menu {
      position: absolute;
      top: 100%;
      right: 0;
      width: max-content;
      background: white;
      border: 1px solid ${GRAY_200};
      border-radius: 10px;
      box-shadow: 0 10px 25px rgba(0,0,0,0.1);
      padding: 6px;
      display: none;
      z-index: 9999;
      margin-top: 8px;
    }
    .export-dropdown-menu.show { display: block; animation: slideIn 0.2s ease; }
    .export-item {
      padding: 8px 12px;
      font-size: 11px;
      font-weight: 600;
      color: ${GRAY_700};
      border-radius: 6px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 8px;
      transition: all 0.2s;
    }
    .export-item:hover { background: ${GRAY_50}; color: ${AT_BLUE}; }
    @keyframes slideIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
  `;

  const steps = [
    { label: "Wilaya", done: !!ville, active: !ville },
    { label: "Commune", done: !!commune, active: !!ville && !commune },
    { label: "Résidence", done: !!residenceObj, active: !!commune && !residenceObj },
  ];

  if (screen === "login") {
    return (
      <Login
        onLoginSuccess={() => setScreen("dashboard")}
        notify={notify}
        notif={notif}
        setNotif={setNotif}
        Notification={Notification}
        globalStyle={globalStyle}
        labelStyle={labelStyle}
        inputStyle={inputStyle}
        btnPrimary={btnPrimary}
      />
    );
  }

  if (screen === "profile") {
    return (
      <Profile
        onBack={() => setScreen("dashboard")}
        onLoadProject={(id) => {
          loadProject(id);
          setScreen("dashboard");
        }}
        onLogout={() => {
          localStorage.removeItem("user");
          window.location.reload();
        }}
      />
    );
  }

  return (
    <div style={{ width: "100vw", height: "100vh", background: GRAY_50, fontFamily: "'Outfit','Segoe UI',sans-serif", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <style>{globalStyle}</style>

      <nav style={{ height: 60, background: "white", borderBottom: `1px solid ${GRAY_200}`, display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0 20px", zIndex: 10000 }}>

        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <ATLogo size={34} />
          <div style={{ fontSize: 14, fontWeight: 800, color: GRAY_800 }}>FAT <span style={{ color: AT_BLUE }}>SMART</span> PLANNER</div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>

          { }
          <button
            onClick={handleRestart}
            style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 14px", background: GRAY_100, border: `1px solid ${GRAY_200}`, borderRadius: 8, cursor: "pointer", fontSize: 12, fontWeight: 700, color: GRAY_700 }}
            onMouseEnter={e => e.currentTarget.style.background = GRAY_200}
            onMouseLeave={e => e.currentTarget.style.background = GRAY_100}
          >
            <i className="fas fa-rotate-right"></i> Redémarrer
          </button>

          { }
          <div className="export-dropdown-container">
            <button
              onClick={() => planGenerated && setShowExportMenu(!showExportMenu)}
              disabled={!planGenerated}
              style={{
                display: "flex", alignItems: "center", gap: 6, padding: "7px 14px",
                background: planGenerated ? AT_BLUE_LIGHT : GRAY_100,
                border: `1px solid ${planGenerated ? AT_BLUE : GRAY_200}`,
                borderRadius: 8, cursor: planGenerated ? "pointer" : "not-allowed",
                fontSize: 12, fontWeight: 700, color: planGenerated ? AT_BLUE : GRAY_400,
                opacity: planGenerated ? 1 : 0.6
              }}
            >
              <i className="fas fa-file-export"></i> Exporter
            </button>
            <div className={`export-dropdown-menu ${showExportMenu ? 'show' : ''}`}>
              <div className="export-item" onClick={() => { handleExport("excel"); setShowExportMenu(false); }}><i className="fas fa-file-excel" style={{ color: '#1D6F42' }}></i> Excel (.xls)</div>
              <div className="export-item" onClick={() => { handleExport("pdf"); setShowExportMenu(false); }}><i className="fas fa-file-pdf" style={{ color: '#DC2626' }}></i> Plan de Sectorisation 2D (.pdf)</div>
            </div>
          </div>

          {/* ── Save button — only active when plan is generated ── */}
          <button
            onClick={saveProject}
            disabled={saveLoading || !planGenerated}
            style={{
              display: "flex", alignItems: "center", gap: 6,
              padding: "7px 14px",
              background: planGenerated ? `linear-gradient(135deg, ${GREEN}, #059669)` : GRAY_200,
              border: "none", borderRadius: 8, cursor: planGenerated ? "pointer" : "default",
              fontSize: 12, fontWeight: 700,
              color: planGenerated ? "white" : GRAY_400,
              opacity: saveLoading ? 0.7 : 1,
              transition: "all 0.2s",
            }}
          >
            {saveLoading ? <><i className="fas fa-spinner fa-spin"></i> Sauvegarde…</> : <><i className="fas fa-save"></i> Sauvegarder</>}
          </button>

          {/* ── User avatar ── */}
          <button
            onClick={() => setScreen("profile")}
            style={{ background: "none", border: "none", cursor: "pointer", display: "flex", alignItems: "center", gap: 8, padding: "4px 8px", borderRadius: 8, transition: "background 0.2s" }}
            onMouseEnter={e => e.currentTarget.style.background = GRAY_100}
            onMouseLeave={e => e.currentTarget.style.background = "none"}
          >
            <div style={{ fontSize: 11, fontWeight: 700, color: GRAY_700, textAlign: "right" }}>
              <div>{userName}</div>

            </div>
            <div style={{ width: 32, height: 32, borderRadius: "50%", background: AT_BLUE_LIGHT, color: AT_BLUE, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 700, fontSize: 12 }}>{userInitials}</div>
          </button>

        </div>
      </nav>


      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* ── Overlays ── */}
        {saveLoading && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(10, 22, 40, 0.75)", backdropFilter: "blur(4px)", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", zIndex: 10000, color: "white" }}>
            <div style={{ width: 64, height: 64, border: "4px solid rgba(255,255,255,0.1)", borderTop: `4px solid ${AT_BLUE}`, borderRadius: "50%", animation: "spin 1s linear infinite", marginBottom: 24 }} />
            <div style={{ fontSize: 22, fontWeight: 800, letterSpacing: "-0.5px" }}>Sauvegarde en cours...</div>
            <div style={{ fontSize: 13, opacity: 0.7, marginTop: 10 }}>Traitement en cours, merci de patienter</div>
          </div>
        )}

        {!!loadingProjectId && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(10, 22, 40, 0.75)", backdropFilter: "blur(4px)", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", zIndex: 10000, color: "white" }}>
            <div style={{ width: 64, height: 64, border: "4px solid rgba(255,255,255,0.1)", borderTop: `4px solid ${AT_BLUE}`, borderRadius: "50%", animation: "spin 1s linear infinite", marginBottom: 24 }} />
            <div style={{ fontSize: 22, fontWeight: 800, letterSpacing: "-0.5px" }}>Chargement du projet...</div>
            <div style={{ fontSize: 13, opacity: 0.7, marginTop: 10 }}>Récupération des données en cours</div>
          </div>
        )}

        {isSectorising && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(10, 22, 40, 0.75)", backdropFilter: "blur(4px)", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", zIndex: 10000, color: "white" }}>
            <div style={{ width: 64, height: 64, border: "4px solid rgba(255,255,255,0.1)", borderTop: `4px solid ${planningMode === "prediction" ? PURPLE : AT_ORANGE}`, borderRadius: "50%", animation: "spin 1s linear infinite", marginBottom: 24 }} />
            <div style={{ fontSize: 22, fontWeight: 800, letterSpacing: "-0.5px" }}>{planningMode === "prediction" ? "Calcul de l'Estimation..." : "Calcul de la Sectorisation..."}</div>
            <div style={{ fontSize: 13, opacity: 0.7, marginTop: 10 }}>Optimisation des branchements en cours</div>
          </div>
        )}

        {showSaveSuccess && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(10, 22, 40, 0.6)", backdropFilter: "blur(6px)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 10001 }}>
            <div style={{ background: "white", borderRadius: 24, padding: 36, width: 340, textAlign: "center", boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)", animation: "popIn 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275)" }}>
              <style>{`@keyframes popIn { from { transform: scale(0.85); opacity: 0; } to { transform: scale(1); opacity: 1; } }`}</style>
              <div style={{ width: 72, height: 72, background: GREEN_LIGHT, color: GREEN, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 36, margin: "0 auto 24px" }}>
                <i className="fas fa-check"></i>
              </div>
              <h3 style={{ fontSize: 20, fontWeight: 800, color: GRAY_800, margin: "0 0 10px 0" }}>Projet Sauvegardé !</h3>
              <p style={{ fontSize: 13, color: GRAY_500, lineHeight: 1.6, marginBottom: 28 }}>
                Le projet a été enregistré avec succès.<br />Il est désormais accessible dans vos archives.
              </p>

              <button
                onClick={() => setShowSaveSuccess(false)}
                style={{ width: "100%", padding: "14px", background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`, color: "white", border: "none", borderRadius: 12, fontWeight: 700, fontSize: 14, cursor: "pointer", transition: "all 0.2s", boxShadow: "0 8px 16px rgba(0, 91, 170, 0.25)" }}
                onMouseEnter={e => e.currentTarget.style.transform = "translateY(-2px)"}
                onMouseLeave={e => e.currentTarget.style.transform = "translateY(0)"}
              >
                Terminer
              </button>
            </div>
          </div>
        )}

        {showOverwriteConfirm && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(10, 22, 40, 0.6)", backdropFilter: "blur(6px)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 10001 }}>
            <div style={{ background: "white", borderRadius: 24, padding: 36, width: 400, textAlign: "center", boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)", animation: "popIn 0.4s" }}>
              <div style={{ width: 72, height: 72, background: "#FEF3C7", color: "#D97706", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 36, margin: "0 auto 24px" }}>
                <i className="fas fa-exclamation-triangle"></i>
              </div>
              <h3 style={{ fontSize: 20, fontWeight: 800, color: GRAY_800, marginBottom: 12 }}>Projet déjà existant</h3>
              <p style={{ color: GRAY_600, fontSize: 13, lineHeight: "1.6", marginBottom: 32 }}>
                Ce projet a déjà été sauvegardé. Voulez-vous <strong>écraser</strong> la version précédente avec vos nouvelles modifications ?
              </p>
              <div style={{ display: "flex", gap: 12 }}>
                <button
                  onClick={() => setShowOverwriteConfirm(false)}
                  style={{ flex: 1, padding: "12px", borderRadius: 12, border: `1px solid ${GRAY_200}`, background: "white", fontWeight: 700, cursor: "pointer", fontSize: 14 }}
                >
                  Annuler
                </button>
                <button
                  onClick={executeSave}
                  style={{ flex: 1, padding: "12px", borderRadius: 12, border: "none", background: AT_BLUE, color: "white", fontWeight: 700, cursor: "pointer", fontSize: 14 }}
                >
                  Oui, écraser
                </button>
              </div>
            </div>
          </div>
        )}

        <aside style={{ width: 310, background: "white", borderRight: `1px solid ${GRAY_200}`, padding: 20, overflowY: "auto" }}>

          <div style={cardStyle}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
              <span style={{ width: 28, height: 28, background: AT_BLUE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, color: AT_BLUE }}>
                <i className="fas fa-map-marker-alt"></i>
              </span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Localisation</span>
            </div>

            {/* Steps indicator */}
            <div style={{ display: "flex", alignItems: "center", marginBottom: 16, gap: 4 }}>
              {steps.map((s, i) => (
                <div key={s.label} style={{ display: "flex", alignItems: "center", flex: 1 }}>
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: 1 }}>
                    <div style={{ width: 22, height: 22, borderRadius: "50%", background: s.done ? GREEN : s.active ? AT_BLUE : GRAY_200, color: (s.done || s.active) ? "white" : GRAY_400, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 10, fontWeight: 700 }}>
                      {s.done ? <i className="fas fa-check"></i> : i + 1}
                    </div>
                    <div style={{ fontSize: 9, color: s.done ? GREEN : s.active ? AT_BLUE : GRAY_400, fontWeight: 600, marginTop: 3, textAlign: "center" }}>{s.label}</div>
                  </div>
                  {i < steps.length - 1 && <div style={{ width: 20, height: 2, background: s.done ? GREEN : GRAY_200, flexShrink: 0, marginBottom: 18 }} />}
                </div>
              ))}
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={labelStyle}>1. Wilaya</label>
              <select style={inputStyle} value={ville} onChange={e => {
                setVille(e.target.value);
                setCommune(""); setResidenceObj(null); setCommunesOpts([]);
                setRawBuildings(null); setCommuneGeoJson(null);
                setPrimaryTargetId(null); setLastImportedId(null);
                setPlanGenerated(false); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
              }}>
                <option value="">{villesOpts.length === 0 ? "Chargement..." : "— Sélectionner une wilaya —"}</option>
                {villesOpts.map(v => <option key={v} value={v}>{v}</option>)}
              </select>
            </div>

            <div style={{ marginBottom: 12, opacity: ville ? 1 : 0.4, pointerEvents: ville ? "auto" : "none" }}>
              <label style={labelStyle}>2. Commune</label>
              <select style={inputStyle} value={commune} onChange={e => {
                setCommune(e.target.value);
                setResidenceObj(null); setRawBuildings(null); setCommuneGeoJson(null);
                setPrimaryTargetId(null); setLastImportedId(null); setSectorisationSnapshot(null);
                setPlanGenerated(false); setFatResults([]); setKpis(null);
              }} disabled={!ville}>
                <option value="">{!ville ? "Sélectionnez une wilaya" : communesOpts.length === 0 ? "Chargement..." : "— Sélectionner une commune —"}</option>
                {communesOpts.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>

            <div style={{ marginBottom: 16, opacity: commune ? 1 : 0.4, pointerEvents: commune ? "auto" : "none" }}>
              <label style={labelStyle}>3. Résidence / Cité</label>
              <ResidenceSearchSelect commune={commune} ville={ville} onSelect={handleResidenceSelect} selectedObj={residenceObj} disabled={!commune} />
              {residenceObj && (
                <div style={{ marginTop: 6, padding: "6px 10px", background: GREEN_LIGHT, borderRadius: 6, fontSize: 11, color: GREEN, fontWeight: 600 }}>
                  <i className="fas fa-check-circle"></i> {commune} – {residenceObj.name}
                </div>
              )}
            </div>

            {(() => {
              const isImported = (osmLoaded && !!residenceObj) || planGenerated;
              return (
                <button style={{ ...btnPrimary, background: isImported ? GREEN : AT_BLUE, marginTop: 4 }} onClick={importOSM} disabled={osmLoading || !residenceObj}>
                  {osmLoading ? "Chargement Résidence..." : isImported ? <><i className="fas fa-check"></i> Résidence Synchronisée</> : "Importez depuis OpenStreetMap"}
                </button>
              );
            })()}
          </div>

          {(() => {
            const isImported = (osmLoaded && !!residenceObj) || planGenerated;
            return (
              <div style={{ ...cardStyle, opacity: isImported ? 1 : 0.4, pointerEvents: isImported ? "auto" : "none" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                  <span style={{ width: 28, height: 28, background: GREEN_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>🏗</span>
                  <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Vérification Structure</span>

                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Nombre d'Étages</label>
                  <input type="number" style={inputStyle} value={etages} onChange={e => { setEtages(parseInt(e.target.value) || 1); setPlanGenerated(false); }} min={1} />
                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Logements / Étage</label>
                  <input type="number" style={inputStyle} value={logements} onChange={e => { setLogements(parseInt(e.target.value) || 1); setPlanGenerated(false); }} min={1} />
                </div>
                {planningMode === "prediction" && (
                  <div style={{ marginBottom: 10 }}>
                    <label style={labelStyle}>Programme Résidentiel</label>
                    <select
                      style={{
                        ...inputStyle,
                        cursor: "pointer",
                        appearance: "none",
                        backgroundImage: `url("data:image/svg+xml;charset=UTF-8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%234B5563' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E")`,
                        backgroundRepeat: "no-repeat",
                        backgroundPosition: "right 12px center",
                        backgroundSize: "16px",
                        paddingRight: "40px"
                      }}
                      value={programmeResidentiel}
                      onChange={e => { setProgrammeResidentiel(e.target.value); setPlanGenerated(false); }}
                    >
                      {[
                        { key: "AADL", value: "AADL" },
                        { key: "HLM", value: "HLM (Social)" },
                        { key: "LPP", value: "LPP" },
                        { key: "LPA", value: "LPA" },
                        { key: "LSL", value: "LSL" },
                        { key: "CNEP", value: "CNEP" },
                        { key: "PRIVE", value: "PRIVÉ" }
                      ].map(opt => (
                        <option key={opt.key} value={opt.key}>{opt.value}</option>
                      ))}
                    </select>
                  </div>
                )}
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Hauteur d'étage (m)</label>
                  <input type="number" step="0.1" style={inputStyle} value={hauteurEtage} onChange={e => { setHauteurEtage(parseFloat(e.target.value) || 3.0); setPlanGenerated(false); }} min={1} />
                </div>

                <div style={{ marginBottom: 15 }}>
                  <label style={labelStyle}>Présence Commerciale</label>
                  <div style={{ display: "flex", gap: 10, marginTop: 4 }}>
                    {["Oui", "Non"].map((opt) => {
                      const val = opt === "Oui";
                      const active = presenceCommercial === val;
                      return (
                        <div
                          key={opt}
                          onClick={() => { setPresenceCommercial(val); setPlanGenerated(false); }}
                          style={{
                            flex: 1,
                            display: "flex",
                            alignItems: "center",
                            gap: 8,
                            padding: "8px 12px",
                            background: "white",
                            border: `1.5px solid ${active ? AT_ORANGE : GRAY_200}`,
                            borderRadius: 8,
                            cursor: "pointer",
                            transition: "all 0.2s",
                            color: active ? AT_ORANGE : GRAY_600,
                            fontWeight: 700,
                            fontSize: "12px"
                          }}
                        >
                          <div style={{
                            width: 16,
                            height: 16,
                            borderRadius: 4,
                            border: `1.5px solid ${active ? AT_ORANGE : GRAY_300}`,
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            background: active ? AT_ORANGE : "transparent"
                          }}>
                            {active && <span style={{ color: "white", fontSize: "10px" }}>✓</span>}
                          </div>
                          {opt}
                        </div>
                      );
                    })}
                  </div>
                </div>
                <button
                  disabled={isSectorising}
                  style={{ ...btnPrimary, background: planningMode === "prediction" ? `linear-gradient(135deg, ${PURPLE}, #6d28d9)` : `linear-gradient(135deg, ${AT_ORANGE}, #d97706)`, opacity: isSectorising ? 0.7 : 1, cursor: isSectorising ? "not-allowed" : "pointer" }}
                  onClick={lancerSectorisation}
                >
                  {isSectorising ? <><i className="fas fa-spinner fa-spin"></i> Traitement...</> : planningMode === "prediction" ? <><i className="fas fa-play"></i> Lancer Estimation</> : <><i className="fas fa-play"></i> Lancer Sectorisation</>}
                </button>
              </div>
            );
          })()}
        </aside>

        <div style={{ flex: 1, padding: 20, overflow: "auto", background: GRAY_50 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, alignItems: "stretch" }}>
              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 0, overflow: "hidden", minHeight: 450, display: "flex", flexDirection: "column" }}>
                {/* ── Header with planning mode toggle ── */}
                <div style={{ padding: "12px 20px", borderBottom: `1px solid ${GRAY_100}`, display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800 }}>Plan de Sectorisation</div>
                  {/* Toggle: Subscriber-Based / Prediction-Based */}
                  <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                    <span style={{ fontSize: 10, fontWeight: 700, color: planningMode === "subscriber" ? AT_BLUE : GRAY_400, letterSpacing: "0.3px", transition: "color 0.2s", whiteSpace: "nowrap" }}>
                      Réseau connecté
                    </span>
                    {/* Toggle pill */}
                    <div
                      onClick={() => {
                        const next = planningMode === "subscriber" ? "prediction" : "subscriber";
                        const wasGenerated = planGenerated;
                        setPlanningMode(next);
                        setPlanGenerated(false);
                        setFatResults([]);
                        setKpis(null);
                        setSectorisationSnapshot(null);
                        if (wasGenerated) {
                          setPendingRelaunch(true);
                        } else {
                          setModeModal({
                            open: true,
                            title: next === "prediction" ? "Mode estimé activé" : "Mode connecté activé",
                            message: next === "prediction" ? "Vision réaliste et optimisée" : "Vision complète"
                          });
                        }
                      }}
                      style={{
                        width: 44, height: 24, borderRadius: 12, cursor: "pointer",
                        background: planningMode === "prediction"
                          ? `linear-gradient(135deg, ${PURPLE}, #6d28d9)`
                          : `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`,
                        position: "relative", transition: "background 0.3s",
                        boxShadow: "0 2px 6px rgba(0,0,0,0.15)", flexShrink: 0,
                      }}
                    >
                      <div style={{
                        position: "absolute", top: 3,
                        left: planningMode === "prediction" ? 23 : 3,
                        width: 18, height: 18, borderRadius: "50%",
                        background: "white",
                        boxShadow: "0 1px 4px rgba(0,0,0,0.2)",
                        transition: "left 0.25s cubic-bezier(0.4,0,0.2,1)",
                      }} />
                    </div>
                    <span style={{ fontSize: 10, fontWeight: 700, color: planningMode === "prediction" ? PURPLE : GRAY_400, letterSpacing: "0.3px", transition: "color 0.2s", whiteSpace: "nowrap" }}>
                      Réseau estimé
                    </span>

                  </div>
                </div>
                <div ref={buildingPlanRef}>
                  {isSectorising ? (
                    <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>
                      <div style={{ width: 40, height: 40, border: `3px solid ${GRAY_200}`, borderTopColor: planningMode === "prediction" ? PURPLE : AT_ORANGE, borderRadius: "50%", animation: "spin 0.8s linear infinite", margin: "0 auto 16px" }}></div>
                      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6, color: GRAY_800 }}>
                        {planningMode === "prediction" ? "Estimation de l'architecture en cours..." : "Sectorisation en cours..."}
                      </div>
                      <div style={{ fontSize: 12 }}>Veuillez patienter pendant le traitement des données.</div>
                    </div>
                  ) : planGenerated && sectorisationSnapshot ? (
                    <BuildingPlan
                      key={sectorisationSnapshot.timestamp}
                      etages={sectorisationSnapshot.etages}
                      logements={sectorisationSnapshot.logements}
                      residenceName={sectorisationSnapshot.residenceName}
                      presenceCommercial={sectorisationSnapshot.presenceCommercial}
                      fatResults={sectorisationSnapshot.fatResults}
                      subscribersData={sectorisationSnapshot.subscribersData}
                      planningMode={sectorisationSnapshot.planningMode}
                    />
                  ) : (
                    <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>
                      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Sélectionnez et chargez un bâtiment</div>
                      <div style={{ fontSize: 12 }}>Puis lancez l'ingénierie pour générer l'architecture.</div>
                    </div>
                  )}
                </div>
              </div>

              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, overflow: "hidden", display: "flex", flexDirection: "column" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, padding: "16px 20px", borderBottom: `1px solid ${GRAY_100}` }}>Carte géographique OpenStreetMap</div>
                <div style={{ flex: 1, minHeight: 450 }}>
                  <LeafletMap
                    buildingsGeoJson={rawBuildings || communeGeoJson}
                    fatResults={fatResults}
                    onBuildingClick={handleBuildingClick}
                    selectedOsmId={residenceObj?.osm_id}
                    selectedLat={residenceObj?.lat}
                    selectedLon={residenceObj?.lon}
                    primaryTargetId={primaryTargetId}
                    isBloc={residenceObj?.is_bloc}
                    treatedOsmIds={treatedOsmIds}
                  />
                </div>
              </div>
            </div>

            {kpis && (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 16 }}>
                {/* 3D View Column */}
                <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: "12px", boxShadow: "0 1px 4px rgba(0,0,0,0.05)", height: 400 }}>
                  <Building3D
                    etages={sectorisationSnapshot?.etages || etages}
                    logements={sectorisationSnapshot?.logements || logements}
                    hauteurEtage={sectorisationSnapshot?.hauteurEtage || hauteurEtage}
                    fatResults={sectorisationSnapshot?.fatResults || []}
                    subscribersData={sectorisationSnapshot?.subscribersData || []}
                    planningMode={sectorisationSnapshot?.planningMode || planningMode}
                  />
                </div>

                {/* KPI Table Column */}
                <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: "20px", boxShadow: "0 1px 4px rgba(0,0,0,0.05)", display: "flex", flexDirection: "column" }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, marginBottom: 16, display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ width: 4, height: 16, background: sectorisationSnapshot?.planningMode === "prediction" ? PURPLE : AT_ORANGE, borderRadius: 2 }} />
                    Indicateurs (KPIs)

                  </div>
                  <div style={{ overflow: "hidden", borderRadius: 8, border: `1px solid ${GRAY_200}`, flex: 1 }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", background: "white", height: "100%" }}>
                      <thead>
                        <tr style={{ background: sectorisationSnapshot?.planningMode === "prediction" ? PURPLE : AT_BLUE }}>
                          <th style={{ padding: "10px 15px", textAlign: "left", fontSize: 10, fontWeight: 700, color: "white", textTransform: "uppercase", letterSpacing: "0.5px" }}>Indicateur</th>
                          <th style={{ padding: "10px 15px", textAlign: "left", fontSize: 10, fontWeight: 700, color: "white", textTransform: "uppercase", letterSpacing: "0.5px" }}>Valeur</th>
                        </tr>
                      </thead>
                      <tbody>
                        {[
                          { label: sectorisationSnapshot?.planningMode === "prediction" ? "Abonnés estimés" : "Total Abonnés", value: kpis.totalAbonnes, suffix: "" },
                          ...(sectorisationSnapshot?.planningMode === "prediction" ? [{ label: "Taux occupation estimé", value: sectorisationSnapshot?.kPrediction?.taux_occupation_pct || Math.round((kpis.totalAbonnes / Math.max(1, (sectorisationSnapshot.etages * sectorisationSnapshot.logements))) * 100), suffix: " %" }] : []),
                          { label: "Boîtiers FAT", value: kpis.fatsNeeded, suffix: "" },
                          { label: "Occupation ports", value: kpis.fatsPortsUsed, suffix: " %" },
                          { label: "Linéaire fibre", value: kpis.lineaire, suffix: " m" },
                        ].map((row, i, arr) => (
                          <tr key={row.label} style={{ borderBottom: i === arr.length - 1 ? "none" : `1px solid ${GRAY_100}`, background: i % 2 === 0 ? "white" : GRAY_50 }}>
                            <td style={{ padding: "10px 15px", fontSize: 12, color: GRAY_600, fontWeight: 500 }}>{row.label}</td>
                            <td style={{ padding: "10px 15px", fontSize: 13, color: GRAY_800, fontWeight: 800 }}>
                              {row.value}<span style={{ fontSize: 11, color: GRAY_400, marginLeft: 2, fontWeight: 600 }}>{row.suffix}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}


            {/* Synthèse des Branchements Abonnés ── */}
            {planGenerated && sectorisationSnapshot && (
              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: "20px", boxShadow: "0 1px 4px rgba(0,0,0,0.05)", marginTop: "20px" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, marginBottom: 16, display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ width: 4, height: 16, background: AT_BLUE, borderRadius: 2 }} />
                  Synthèse des Branchements Abonnés
                </div>
                <div style={{ overflowX: "auto", borderRadius: 8, border: `1px solid ${GRAY_200}`, maxHeight: "400px", overflowY: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", background: "white", fontSize: 12 }}>
                    <thead style={{ position: "sticky", top: 0, zIndex: 10, background: GRAY_50 }}>
                      <tr style={{ borderBottom: `2px solid ${GRAY_200}` }}>
                        <th style={{ padding: "12px 15px", textAlign: "left", color: GRAY_500, fontWeight: 700, textTransform: "uppercase", fontSize: 10 }}>FAT</th>
                        <th style={{ padding: "12px 15px", textAlign: "left", color: GRAY_500, fontWeight: 700, textTransform: "uppercase", fontSize: 10 }}>Adresse</th>
                        <th style={{ padding: "12px 15px", textAlign: "left", color: GRAY_500, fontWeight: 700, textTransform: "uppercase", fontSize: 10 }}>Étage</th>
                        <th style={{ padding: "12px 15px", textAlign: "left", color: GRAY_500, fontWeight: 700, textTransform: "uppercase", fontSize: 10 }}>Distance (m)</th>
                        <th style={{ padding: "12px 15px", textAlign: "left", color: GRAY_500, fontWeight: 700, textTransform: "uppercase", fontSize: 10 }}>Câble</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(sectorisationSnapshot.subscribersData || [])
                        .filter(sub => {
                          const isOccupied = sectorisationSnapshot.planningMode !== "prediction" || sub.habite === 1;
                          const hasFat = fatResults.some(f => f.subscriber_ids?.includes(sub.code_client));
                          return isOccupied && hasFat;
                        })
                        .map((sub, i) => {
                          const connectedFat = fatResults.find(f => f.subscriber_ids?.includes(sub.code_client));

                          const realConnection = sectorisationSnapshot?.connections?.find(c => c.code_client === sub.code_client);

                          // Distance individuelle abonné → FAT via Haversine + correction verticale
                          const diffEtage = Math.abs(sub.etage - (connectedFat?.etage_fat || 0));
                          const verticalM = diffEtage * (sectorisationSnapshot.hauteurEtage || 3);
                          const geoM = haversineM(sub.lat_abonne, sub.lon_abonne, connectedFat?.centroid_lat, connectedFat?.centroid_lon);

                          const distanceVal = realConnection?.distance_real_m ??
                            (geoM !== null
                              ? (geoM + verticalM + 4.0).toFixed(2)
                              : (connectedFat?.cable_snap_m
                                ? (connectedFat.cable_snap_m * 0.85).toFixed(2)
                                : ((diffEtage * (sectorisationSnapshot.hauteurEtage || 3) + 4.0) * 0.85).toFixed(2)));

                          const cableVal = realConnection?.cable_snap_m ?? (geoM !== null ? snapDropCable(geoM + verticalM + 4.0, cablesStandards) : (connectedFat?.cable_snap_m || snapDropCable(diffEtage * (sectorisationSnapshot.hauteurEtage || 3) + 4.0, cablesStandards)));

                          return (
                            <tr key={sub.code_client} style={{ borderBottom: `1px solid ${GRAY_100}`, background: i % 2 === 0 ? "white" : "#FAFAFA" }}>
                              <td style={{ padding: "10px 15px", fontWeight: 600, color: AT_BLUE }}>{connectedFat?.fat_id_AT || connectedFat?.fat_id || "—"}</td>
                              <td style={{ padding: "10px 15px", color: GRAY_800, fontWeight: 500, display: "flex", alignItems: "center", gap: 8 }}>
                                <span>{sub.usage === "commerces" ? `Commerce ${sub.porte}` : `Porte ${sub.porte}`}</span>
                                {sub.raccorde === 0 && sub.habite === 1 && sectorisationSnapshot.planningMode !== "prediction" && (
                                  <span style={{ fontSize: 9, color: RED, padding: "2px 6px", background: "#FEE2E2", borderRadius: 4, fontWeight: 700 }}>Non raccordé</span>
                                )}
                                {sub.habite === 0 && <span style={{ fontSize: 9, color: GRAY_500, padding: "2px 6px", background: GRAY_100, borderRadius: 4, fontWeight: 700 }}>Inoccupé</span>}
                              </td>
                              <td style={{ padding: "10px 15px", color: GRAY_600 }}>{sub.etage}</td>
                              <td style={{ padding: "10px 15px", color: GRAY_600 }}>{distanceVal}</td>
                              <td style={{ padding: "10px 15px", textAlign: "center" }}>
                                <span style={{ padding: "2px 8px", background: GRAY_100, borderRadius: 4, fontWeight: 700, color: GRAY_700 }}>
                                  {cableVal}
                                </span>
                              </td>
                            </tr>
                          );
                        })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
      {archiveOpen && (
        <div style={{ position: "fixed", inset: 0, zIndex: 9000, background: "rgba(0,0,0,0.45)", display: "flex", alignItems: "center", justifyContent: "center" }}>
          <div style={{ background: "white", borderRadius: 16, width: 640, maxHeight: "80vh", display: "flex", flexDirection: "column", boxShadow: "0 24px 64px rgba(0,0,0,0.2)" }}>

            {/* Header */}
            <div style={{ padding: "20px 24px", borderBottom: `1px solid ${GRAY_200}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div>
                <div style={{ fontWeight: 800, fontSize: 16, color: GRAY_800 }}><i className="fas fa-folder-open"></i> Mes Projets Archivés</div>
                <div style={{ fontSize: 11, color: GRAY_400, marginTop: 2 }}>{projects.length} projet(s) sauvegardé(s)</div>
              </div>
              <button onClick={() => setArchiveOpen(false)} style={{ background: GRAY_100, border: "none", borderRadius: 8, width: 32, height: 32, cursor: "pointer", fontSize: 16, color: GRAY_600 }}>✕</button>
            </div>

            {/* Body */}
            <div style={{ overflowY: "auto", flex: 1, padding: 16 }}>
              {projectsLoading ? (
                <div style={{ textAlign: "center", padding: 60, color: GRAY_400 }}>⏳ Chargement…</div>
              ) : projects.length === 0 ? (
                <div style={{ textAlign: "center", padding: 60, color: GRAY_400 }}>
                  <div style={{ fontSize: 32, marginBottom: 10 }}>📭</div>
                  <div style={{ fontWeight: 600 }}>Aucun projet sauvegardé</div>
                  <div style={{ fontSize: 12, marginTop: 4 }}>Lancez une sectorisation puis cliquez sur <i className="fas fa-save"></i> Sauvegarder</div>
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {projects.map(p => (
                    <div key={p.id_projet} style={{ background: GRAY_50, border: `1px solid ${GRAY_200}`, borderRadius: 10, padding: "14px 16px", display: "flex", alignItems: "center", gap: 14 }}>
                      {/* Mode badge */}
                      <div style={{ width: 40, height: 40, borderRadius: 10, background: p.planning_mode === "Réseau estimé" ? PURPLE_LIGHT : AT_BLUE_LIGHT, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18, flexShrink: 0 }}>
                        {p.planning_mode === "Réseau estimé" ? <i className="fas fa-robot" style={{ color: PURPLE }}></i> : <i className="fas fa-satellite-dish" style={{ color: AT_BLUE }}></i>}
                      </div>
                      {/* Info */}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontWeight: 700, fontSize: 13, color: GRAY_800, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.nom_projet}</div>
                        <div style={{ fontSize: 11, color: GRAY_400, marginTop: 3 }}>
                          {p.wilaya} · {p.commune} &nbsp;·&nbsp;
                          <span style={{ color: AT_BLUE, fontWeight: 600 }}>{p.nb_fats} FATs</span> &nbsp;·&nbsp;
                          {p.nb_abonnes} abonnés &nbsp;·&nbsp;
                          {new Date(p.date_modification).toLocaleDateString("fr-DZ", { day: "2-digit", month: "short", year: "numeric" })}
                        </div>
                      </div>
                      {/* Actions */}
                      <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
                        <button
                          onClick={() => loadProject(p.id_projet)}
                          disabled={loadingProjectId === p.id_projet}
                          style={{ padding: "6px 14px", background: AT_BLUE, color: "white", border: "none", borderRadius: 7, cursor: "pointer", fontSize: 12, fontWeight: 700 }}
                        >
                          {loadingProjectId === p.id_projet ? <i className="fas fa-spinner fa-spin"></i> : <><i className="fas fa-play"></i> Charger</>}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

          </div>
        </div>
      )}
      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
      <SuccessModal
        isOpen={modeModal.open}
        onClose={() => setModeModal({ ...modeModal, open: false })}
        subtitle={modeModal.subtitle}
        title={modeModal.title}
        message={modeModal.message}
      />
    </div>
  );
}