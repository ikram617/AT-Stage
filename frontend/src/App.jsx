import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import ATLogoImg from "./assets/algerie-telecom-logo-png_seeklogo-210074.png";


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

const API = "http://localhost:8000";

// ── AT Logo ────────────────────────────────────────────────
const ATLogo = ({ size = 40 }) => (
  <svg width={size} height={size} viewBox="0 0 60 60" fill="none">
    <rect width="60" height="60" rx="10" fill={AT_BLUE} />
    <text x="30" y="38" textAnchor="middle" fill="white" fontSize="22" fontWeight="900" fontFamily="Arial Black, sans-serif">AT</text>
    <rect x="10" y="44" width="40" height="3" rx="1.5" fill={AT_ORANGE} />
  </svg>
);

// ── Notification ───────────────────────────────────────────
const Notification = ({ notif, onClose }) => {
  useEffect(() => {
    const t = setTimeout(onClose, 4000);
    return () => clearTimeout(t);
  }, [onClose]);

  const colors = {
    success: { bg: GREEN_LIGHT, border: GREEN, icon: "✓", iconColor: GREEN },
    info: { bg: AT_BLUE_LIGHT, border: AT_BLUE, icon: "ℹ", iconColor: AT_BLUE },
    error: { bg: "#FEE2E2", border: RED, icon: "✕", iconColor: RED },
    warning: { bg: AT_ORANGE_LIGHT, border: AT_ORANGE, icon: "⚠", iconColor: AT_ORANGE },
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
      <div style={{ width: 30, height: 30, borderRadius: 8, background: c.bg, color: c.iconColor, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 700, fontSize: 14 }}>{c.icon}</div>
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
const FATNode = ({ id, connected, totalPorts, onHover, onLeave, realName, emplacement, isHovered }) => (
  <div
    className="fat-node-container"
    onMouseEnter={onHover}
    onMouseLeave={onLeave}
    style={{
      position: "relative",
      zIndex: isHovered ? 10000 : 10,
      transition: "z-index 0s"
    }}
  >
    <div style={{ background: "white", border: `2px solid ${AT_ORANGE}`, borderRadius: 8, padding: "5px 10px", cursor: "pointer", boxShadow: `0 2px 8px rgba(247,148,29,0.25)`, minWidth: 90, textAlign: "center", transition: "transform 0.2s" }}>
      <div style={{ fontSize: 11, fontWeight: 800, color: GRAY_800, marginTop: 2 }}>FAT {id}</div>
      <div style={{ display: "flex", gap: 3, justifyContent: "center", marginTop: 4, flexWrap: "wrap" }}>
        {Array.from({ length: totalPorts || 8 }).map((_, i) => (
          <div key={i} style={{ width: 6, height: 6, borderRadius: "50%", background: i < connected ? GREEN : AT_BLUE, border: "1px solid rgba(0,0,0,0.05)" }} />
        ))}
      </div>
    </div>
    <div className="fat-tooltip">
      {realName}
    </div>
  </div>
);

// ── BuildingPlan ────────────────────────────────────────────
const BuildingPlan = ({ etages, logements, residenceName, presenceCommercial, fatResults, subscribersData }) => {
  const [hoveredFatId, setHoveredFatId] = useState(null);

  if (!fatResults || fatResults.length === 0) {
    return (
      <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>
        <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Sélectionnez et chargez un bâtiment</div>
        <div style={{ fontSize: 12 }}>Puis lancez l'ingénierie pour générer l'architecture.</div>
      </div>
    );
  }

  // ── Index abonnés : code_client → subscriber ──
  const subByCode = {};
  (subscribersData || []).forEach(s => { subByCode[s.code_client] = s; });

  // ── Séparation et Tri des FATs pour numérotation globale ──
  // On trie d'abord les FATs commerciaux (RDC), puis les résidentiels par étage.
  const commercialFats = [...fatResults]
    .filter(f => f.usage === "commerces")
    .sort((a, b) => (a.centroid_lon ?? 0) - (b.centroid_lon ?? 0));

  const residentialFats = [...fatResults]
    .filter(f => f.usage === "logements")
    .sort((a, b) => (a.etage_fat ?? 1) - (b.etage_fat ?? 1) || (a.centroid_lon ?? 0) - (b.centroid_lon ?? 0));

  const allSortedFats = [...commercialFats, ...residentialFats];

  // ── Grouper FATs par étage ──
  const fatsByFloor = (fatResults || []).reduce((acc, fat) => {
    const fl = Math.floor(fat.etage_fat ?? 0);
    if (!acc[fl]) acc[fl] = [];
    acc[fl].push(fat);
    return acc;
  }, {});

  // ── Abonnés triés par étage et longitude ──
  const subsByFloorSorted = {};
  const startFloor = presenceCommercial ? 0 : 1;
  for (let e = startFloor; e <= etages; e++) {
    const usage = e === 0 ? "commerces" : "logements";
    const floorSubs = (subscribersData || [])
      .filter(s => s.etage === e && s.usage === usage)
      .sort((a, b) => a.lon_abonne - b.lon_abonne);
    subsByFloorSorted[e] = floorSubs;
  }

  // ── Calcul position horizontale d'un FAT ──
  const getFatColumnPosition = (fat, floorNum) => {
    const floorSubs = subsByFloorSorted[floorNum] || [];
    if (floorSubs.length === 0) return (logements - 1) / 2;

    const minLon = floorSubs[0].lon_abonne;
    const maxLon = floorSubs[floorSubs.length - 1].lon_abonne;
    const range = maxLon - minLon;

    const fatSubsOnFloor = (fat.subscriber_ids || [])
      .map(id => subByCode[id])
      .filter(s => s && s.etage === floorNum);

    let fatLon = fat.centroid_lon;
    if (fatSubsOnFloor.length > 0) {
      fatLon = fatSubsOnFloor.reduce((sum, s) => sum + s.lon_abonne, 0) / fatSubsOnFloor.length;
    }

    if (range < 1e-8) {
      const fatsOnFloor = fatsByFloor[floorNum] || [];
      const idx = fatsOnFloor.indexOf(fat);
      return (idx / Math.max(fatsOnFloor.length - 1, 1)) * (logements - 1);
    }

    const pct = Math.max(0, Math.min(1, (fatLon - minLon) / range));
    return pct * (logements - 1);
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
                    const sub = floorSubsSorted[colIdx];
                    const codeClient = sub ? sub.code_client : null;

                    // Label C.x pour commercial, P.x pour résidentiel
                    const label = isCommercialFloor ? `C.${colIdx + 1}` : `P.${(e - 1) * logements + colIdx + 1}`;

                    // On vérifie si ce logement appartient au FAT survolé
                    const assignedToHovered = hoveredFatId && codeClient &&
                      fatResults.find(f => (f.fat_id_AT === hoveredFatId || f.fat_id === hoveredFatId) &&
                        Array.isArray(f.subscriber_ids) && f.subscriber_ids.includes(codeClient));

                    return (
                      <div key={colIdx} style={{ flex: 1, display: "flex", padding: "0 2px" }}>
                        <div style={{
                          flex: 1,
                          display: "flex",
                          flexDirection: "column",
                          alignItems: "center",
                          justifyContent: "center",
                          background: assignedToHovered ? (isCommercialFloor ? AT_ORANGE : AT_BLUE) : "white",
                          color: assignedToHovered ? "white" : (isCommercialFloor ? AT_ORANGE : GRAY_600),
                          border: `1px solid ${isCommercialFloor ? AT_ORANGE : GRAY_200}`,
                          borderRadius: 4,
                          padding: "6px 4px",
                          minWidth: 30,
                          transition: "all 0.15s",
                          opacity: sub ? 1 : 0.3
                        }}>
                          <div style={{ fontSize: 9, fontWeight: 800, fontFamily: "monospace" }}>
                            {label}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {fatsOnFloor.map((fat) => {
                  const fatKey = fat.fat_id_AT || fat.fat_id;
                  const fatIndex = allSortedFats.findIndex(f => (f.fat_id_AT || f.fat_id) === fatKey) + 1;
                  const colPos = getFatColumnPosition(fat, e);
                  const leftPct = (colPos / (logements - 1)) * 100;

                  return (
                    <div key={fatKey} style={{
                      position: "absolute",
                      top: 3,
                      left: `${Math.max(5, Math.min(95, leftPct))}%`,
                      transform: "translateX(-50%)",
                      zIndex: hoveredFatId === fatKey ? 200 : 10,
                    }}>
                      <FATNode
                        id={fatIndex}
                        connected={fat.n_subscribers || 0}
                        totalPorts={8}
                        realName={fat.fat_id_AT || fat.fat_id}
                        emplacement={isCommercialFloor ? `RDC Commercial` : `Étage ${e}`}
                        onHover={() => setHoveredFatId(fatKey)}
                        onLeave={() => setHoveredFatId(null)}
                        isHovered={hoveredFatId === fatKey}
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
        FAT positionnés par usage : <span style={{ color: AT_ORANGE, fontWeight: 700 }}>Commerces (RDC)</span> et <span style={{ color: AT_BLUE, fontWeight: 700 }}>Logements (Étages)</span>.
      </div>
    </div>
  );
};

// ── Leaflet loader ────────────────────────────────────────────────────────────
const LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
const LEAFLET_JS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";

function loadLeaflet() {
  return new Promise((resolve, reject) => {
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
// Les bâtiments sans nom → label = {commune}-blocX-numeroN
// Les bâtiments nommés  → label = nom officiel
const LeafletMap = ({ buildingsGeoJson, fatResults, onBuildingClick, selectedOsmId, primaryTargetId, isBloc }) => {
  const mapRef = useRef(null), mapInstanceRef = useRef(null), buildingsLayerRef = useRef(null), fatsLayerRef = useRef(null), radiusLayerRef = useRef(null);
  const [leafletReady, setLeafletReady] = useState(false);

  useEffect(() => { loadLeaflet().then(() => setLeafletReady(true)).catch(console.error); }, []);

  useEffect(() => {
    if (!leafletReady || !mapRef.current || mapInstanceRef.current) return;
    const L = window.L, map = L.map(mapRef.current, { zoomControl: true });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { attribution: "© OpenStreetMap", maxZoom: 19 }).addTo(map);
    map.setView([35.7, -0.65], 13); mapInstanceRef.current = map;
  }, [leafletReady]);

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
            const isInitial = bid === primaryTargetId || feature.properties?.is_target;
            const isSelected = bid === selectedOsmId;
            if (isSelected) return { color: "#F7941D", weight: 4, opacity: 1, fillColor: "#F7941D", fillOpacity: 0.2 };
            if (isInitial) return { color: "#005BAA", weight: 3, opacity: 0.9, fillColor: "#005BAA", fillOpacity: 0.3 };
            return { color: "#9CA3AF", weight: 1.5, opacity: 0.9, fillColor: "#F3F4F6", fillOpacity: 0.4 };
          },
          onEachFeature: (feature, layer) => {
            const bid = feature.properties?.id_batiment;
            const isInitial = bid === primaryTargetId || feature.properties?.is_target;
            const mapLabel = feature.properties?.nom_batiment || bid || "Bât.";
            layer.bindTooltip(mapLabel, {
              permanent: true, direction: "center", interactive: false,
              className: isInitial ? "target-building-label" : "neighbor-building-label"
            });
            layer.on("click", () => { if (onBuildingClick) onBuildingClick(feature.properties); });
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

        const bounds = buildingsLayerRef.current.getBounds();
        if (bounds.isValid()) map.fitBounds(bounds, { padding: [40, 40] });
      } catch (e) { console.error("Erreur parsing GeoJSON:", e); }
    }


  }, [leafletReady, buildingsGeoJson, fatResults, selectedOsmId, primaryTargetId]);

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

// ══════════════════════════════════════════════════════════════
//  RÉSIDENCE SEARCH SELECT — v6.1
//  Le backend retourne déjà la liste structurée :
//    - has_official_name=true  → résidence nommée (1 ligne)
//    - is_bloc=true            → bloc groupé, name="commune-blocA" (1 ligne)
//  Le frontend affiche et filtre seulement.
// ══════════════════════════════════════════════════════════════

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
        <span style={{ fontSize: 14, color: GRAY_400 }}>🔍</span>
        <input
          type="text"
          value={open ? query : (selectedLabel || "")}
          placeholder={
            loading ? "Chargement des bâtiments..." :
              !commune ? "Sélectionnez une commune d'abord" :
                `Rechercher parmi ${nNamed} résidences + ${nBlocs} blocs...`
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
                : `${nNamed} résidence${nNamed > 1 ? "s" : ""} nommée${nNamed > 1 ? "s" : ""} · ${nBlocs} bloc${nBlocs > 1 ? "s" : ""}`
              }
            </div>
          </div>

          {/* ── Section : Résidences nommées ── */}
          {displayedNamed.length > 0 && (
            <>
              <div style={{ padding: "5px 12px 3px", fontSize: 9, fontWeight: 800, letterSpacing: "1px", textTransform: "uppercase", color: GRAY_400, background: GRAY_50, borderBottom: `1px solid ${GRAY_100}` }}>
                🏢 Résidences nommées
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

          {/* ── Section : Blocs résidentiels ── */}
          {displayedBlocs.length > 0 && (
            <>
              <div style={{ padding: "5px 12px 3px", fontSize: 9, fontWeight: 800, letterSpacing: "1px", textTransform: "uppercase", color: GRAY_400, background: GRAY_50, borderBottom: `1px solid ${GRAY_100}` }}>
                🏗️ Blocs résidentiels ({nBlocs})
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

// ══════════════════════════════════════════════════════════════
//  MAIN APP
// ══════════════════════════════════════════════════════════════
export default function FTTHSmartPlanner() {
  const [screen, setScreen] = useState("login");
  const [notif, setNotif] = useState(null);
  const [showPassword, setShowPassword] = useState(false);
  const [loginData, setLoginData] = useState({ id: "", password: "atdz2026" });
  const [loginLoading, setLoginLoading] = useState(false);

  const [ville, setVille] = useState("");
  const [commune, setCommune] = useState("");
  const [residenceObj, setResidenceObj] = useState(null);

  const [villesOpts, setVillesOpts] = useState([]);
  const [communesOpts, setCommunesOpts] = useState([]);

  const [etages, setEtages] = useState(5);
  const [logements, setLogements] = useState(4);
  const [hauteurEtage, setHauteurEtage] = useState(3.0);
  const [presenceCommercial, setPresenceCommercial] = useState(false);
  const [fatCap, setFatCap] = useState(8);

  const [osmLoaded, setOsmLoaded] = useState(false);
  const [osmLoading, setOsmLoading] = useState(false);
  const [planGenerated, setPlanGenerated] = useState(false);
  const [kpis, setKpis] = useState(null);
  const [rawBuildings, setRawBuildings] = useState(null);
  const [subscribersData, setSubscribersData] = useState([]);
  const [fatResults, setFatResults] = useState([]);
  const [primaryTargetId, setPrimaryTargetId] = useState(null);
  const [lastImportedId, setLastImportedId] = useState(null);
  const [sectorisationSnapshot, setSectorisationSnapshot] = useState(null);

  const notify = useCallback((type, title, sub) => setNotif({ type, title, sub }), []);

  const handleBuildingClick = (properties) => {
    if (!properties) return;
    console.log("📍 Bâtiment cliqué (Détails OSM) :", properties);
    const newObj = { name: properties.nom_batiment || "Bâtiment", osm_id: properties.id_batiment, lat: properties.centroid_lat, lon: properties.centroid_lon };
    setResidenceObj(newObj);
    if (properties.bat_levels) setEtages(properties.bat_levels);
    if (properties.bat_units) setLogements(Math.max(1, Math.floor(properties.bat_units / Math.max(1, properties.bat_levels || 1))));
    setPlanGenerated(false); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
    notify("info", "Bâtiment sélectionné", `Infos mises à jour pour : ${properties.nom_batiment}`);
  };

  const handleResidenceSelect = (res) => {
    console.log("🏢 Résidence/Bloc sélectionné :", res);
    setResidenceObj(res);
    setPrimaryTargetId(null); setLastImportedId(null);
    setOsmLoaded(false); setRawBuildings(null);
    setPlanGenerated(false); setSubscribersData([]); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
  };

  useEffect(() => {
    fetch(`${API}/api/ville`)
      .then(r => r.json())
      .then(d => {
        console.log("🌍 Wilayas chargées :", d.villes);
        setVillesOpts(d.villes || []);
      })
      .catch(() => notify("error", "Connexion API", "Impossible de contacter le backend"));
  }, [notify]);

  useEffect(() => {
    setCommune(""); setResidenceObj(null); setCommunesOpts([]);
    setPrimaryTargetId(null); setLastImportedId(null);
    if (!ville) return;
    console.log(`🔍 Chargement des communes pour : ${ville}...`);
    fetch(`${API}/api/commune?ville=${encodeURIComponent(ville)}`)
      .then(r => r.json())
      .then(d => {
        console.log(`🏙️ Communes reçues (${ville}) :`, d.communes);
        setCommunesOpts(d.communes || []);
      })
      .catch(() => notify("error", "Erreur", "Impossible de charger les communes"));
  }, [ville, notify]);

  useEffect(() => {
    setResidenceObj(null); setPrimaryTargetId(null); setLastImportedId(null); setSectorisationSnapshot(null);
  }, [commune]);

  useEffect(() => {
    setPlanGenerated(false); setFatResults([]); setKpis(null); setSectorisationSnapshot(null);
  }, [ville, commune]);

  const login = () => {
    setLoginLoading(true);
    console.log("🔐 Tentative de connexion en cours...");
    setTimeout(() => {
      setLoginLoading(false);
      setScreen("dashboard");
      console.log("✅ Authentification réussie. Bienvenue.");
      notify("success", "Connexion réussie", "Bienvenue sur l'espace d'ingénierie");
    }, 1200);
  };

  const importOSM = async () => {
    if (!ville || !commune || !residenceObj) { notify("error", "Données manquantes", "Veuillez sélectionner votre cible"); return; }
    setOsmLoading(true);
    console.log("🚀 Lancement de l'import OSM pour :", residenceObj.name);
    try {
      const resp = await fetch(`${API}/api/importOSM`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ville, commune, residence: residenceObj.name, lat: residenceObj.lat, lon: residenceObj.lon, nombre_etages: etages, logements_par_etage: logements, commerce: presenceCommercial }) });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "Erreur Import");

      console.log("📦 Données OSM reçues :", data);
      setRawBuildings(data.buildings_geojson);
      setSubscribersData(data.subscribers);
      if (data.etages_detectes) setEtages(data.etages_detectes);
      if (data.logements_detectes) setLogements(data.logements_detectes);

      setOsmLoaded(true); setLastImportedId(residenceObj.osm_id);
      if (!primaryTargetId) setPrimaryTargetId(residenceObj.osm_id);

      notify("success", "Carte & Données synchronisées", "Bâtiment ciblé et voisinage importés");
    } catch (err) { notify("error", "Erreur réseau", err.message); }
    finally { setOsmLoading(false); }
  };

  const lancerSectorisation = async () => {
    if (!osmLoaded) { notify("info", "Import requis", "Importez d'abord les données de la résidence"); return; }
    if (!subscribersData || subscribersData.length === 0) { notify("error", "Données manquantes", "Aucun abonné détecté."); return; }
    notify("info", "Traitement Algorithmique", "Positionnement dynamique...");
    console.log("⚙️ Lancement de la sectorisation pour", subscribersData.length, "abonnés...");
    try {
      const req1 = await fetch(`${API}/api/emplacementFATs`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ subscribers: subscribersData }) });
      const data1 = await req1.json();
      if (!req1.ok) throw new Error(data1.detail || `Erreur FAT (${req1.status})`);
      console.log("📍 Emplacements FAT calculés :", data1.fat_candidates);

      const req2 = await fetch(`${API}/api/nomFAT`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fat_candidates: data1.fat_candidates, subscribers: subscribersData }) });
      const data2 = await req2.json();
      if (!req2.ok) throw new Error(data2.detail || `Erreur nommage FAT (${req2.status})`);
      console.log("🏷️ Nommage FAT terminé :", data2.fat_candidates_with_ids);

      const finalFats = data2.fat_candidates_with_ids || data1.fat_candidates;
      const kpiObj = {
        totalAbonnes: subscribersData.length,
        fatsNeeded: finalFats.length,
        fatsPortsUsed: Math.round((subscribersData.length / (finalFats.length * fatCap || 1)) * 100),
        lineaire: Math.round(finalFats.reduce((acc, f) => acc + (f.cable_m_to_fdt_real || 0), 0))
      };
      console.log("📊 KPIs de planification :", kpiObj);


      setFatResults(finalFats);
      setKpis(kpiObj);
      setSectorisationSnapshot({
        fatResults: finalFats,
        subscribersData: [...subscribersData],
        etages: etages,
        logements: logements,
        residenceName: residenceObj?.name,
        presenceCommercial: presenceCommercial,
        timestamp: Date.now()
      });
      setPlanGenerated(true);
      notify("success", "Sectorisation terminée", `Topologie générée pour ${finalFats.length} boîtiers`);
    } catch (err) { notify("error", "Échec process", err.message); }
  };

  const inputStyle = { width: "100%", padding: "10px 14px", background: "white", border: `1.5px solid ${GRAY_200}`, borderRadius: 8, color: GRAY_800, fontSize: 13, boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 700, color: GRAY_600, marginBottom: 5, display: "block", letterSpacing: "0.5px", textTransform: "uppercase" };
  const btnPrimary = { background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`, color: "white", border: "none", borderRadius: 8, padding: "11px 20px", fontSize: 13, fontWeight: 700, cursor: "pointer", width: "100%" };
  const cardStyle = { background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.05)", marginBottom: 14 };
  const globalStyle = `
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    @keyframes spin { to { transform: rotate(360deg) } }
    html, body, #root { margin:0 !important; padding:0 !important; width:100vw !important; height:100vh !important; overflow:hidden !important; }
    select:focus, input:focus { outline:none; border-color:${AT_BLUE} !important; }
    .target-building-label { pointer-events:none !important; background:transparent; border:none; box-shadow:none; color:#005BAA; font-weight:800; font-size:10px; text-shadow:1px 1px 2px white,-1px -1px 2px white,1px -1px 2px white,-1px 1px 2px white; text-align:center; white-space:nowrap; }
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
  `;

  const steps = [
    { label: "Wilaya", done: !!ville, active: !ville },
    { label: "Commune", done: !!commune, active: !!ville && !commune },
    { label: "Résidence", done: !!residenceObj, active: !!commune && !residenceObj },
  ];

  if (screen === "login") {
    return (
      <div style={{
        width: "100vw",
        height: "100vh",
        display: "flex",
        overflow: "hidden",
        fontFamily: "'Outfit', sans-serif"
      }}>
        <style>{globalStyle}</style>

        {/* --- SIDEBAR (LEFT) --- */}
        <div style={{
          width: "25%",
          background: AT_BLUE,
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-evenly",
          padding: "80px 40px",
          position: "relative"
        }}>
          {/* Logo Container (Centered horizontally in sidebar) */}
          <div style={{
            background: "white",
            width: "240px",
            height: "140px",
            padding: "20px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            borderRadius: "4px",
            boxShadow: "0 10px 30px rgba(0,0,0,0.1)",
            margin: "0 auto"
          }}>
            <img src={ATLogoImg} alt="Algerie Telecom" style={{ width: "100%", height: "auto", maxHeight: "100%" }} />
          </div>

          {/* Title Section (Left Aligned at the bottom) */}
          <div style={{ textAlign: "left" }}>
            <h1 style={{
              color: "white",
              fontSize: "42px",
              fontWeight: 800,
              margin: 0,
              lineHeight: 1.1,
              letterSpacing: "-1px"
            }}>
              FAT SMART<br />
              <span style={{ color: AT_ORANGE }}>PLANNER</span>
            </h1>
            <p style={{
              color: "rgba(255,255,255,0.7)",
              fontSize: "14px",
              marginTop: "20px",
              fontWeight: 500
            }}>
              Algérie Télécom · Système de planification FTTH
            </p>
          </div>
        </div>

        {/* --- MAIN AREA (RIGHT) --- */}
        <div style={{
          flex: 1,
          background: "#E8F1FA",
          display: "flex",
          alignItems: "center",
          justifyContent: "center"
        }}>
          <div style={{
            background: "white",
            borderRadius: "24px",
            padding: "45px 50px",
            width: "420px",
            boxShadow: "0 20px 50px rgba(0,0,0,0.05)",
            border: "1px solid rgba(255,255,255,0.8)"
          }}>
            <div style={{ textAlign: "center", marginBottom: "35px" }}>
              <h2 style={{ fontSize: "28px", fontWeight: 800, color: GRAY_800, margin: "0 0 8px 0" }}>Connexion</h2>
              <p style={{ fontSize: "13px", color: GRAY_400, margin: 0 }}>Accès réservé aux ingénieurs autorisés</p>
              <div style={{ width: "100%", height: "1px", background: GRAY_100, marginTop: "20px" }} />
            </div>

            <div style={{ marginBottom: "20px" }}>
              <label style={{ ...labelStyle, color: GRAY_700, fontSize: "11px", letterSpacing: "0.5px" }}>Nom d'utilisateur</label>
              <input
                style={{ ...inputStyle, height: "46px", background: "white", border: `1px solid ${GRAY_200}` }}
                type="text"
                value={loginData.id}
                onChange={e => setLoginData(p => ({ ...p, id: e.target.value }))}
                placeholder="ex: k.benali@at.dz"
              />
            </div>

            <div style={{ marginBottom: "30px" }}>
              <label style={{ ...labelStyle, color: GRAY_700, fontSize: "11px", letterSpacing: "0.5px" }}>MOT DE PASSE</label>
              <div style={{ position: "relative" }}>
                <input
                  style={{ ...inputStyle, height: "46px", paddingRight: "44px", background: "white", border: `1px solid ${GRAY_200}` }}
                  type={showPassword ? "text" : "password"}
                  value={loginData.password}
                  onChange={e => setLoginData(p => ({ ...p, password: e.target.value }))}
                  placeholder="••••••••"
                />
                <button
                  onClick={() => setShowPassword(p => !p)}
                  style={{
                    position: "absolute",
                    right: 14,
                    top: "50%",
                    transform: "translateY(-50%)",
                    background: "none",
                    border: "none",
                    cursor: "pointer",
                    color: GRAY_800,
                    fontSize: "16px",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center"
                  }}
                >
                  <i className={showPassword ? "fas fa-eye-slash" : "fas fa-eye"}></i>
                </button>
              </div>
            </div>

            <button
              style={{
                ...btnPrimary,
                height: "50px",
                fontSize: "14px",
                fontWeight: 600,
                background: AT_BLUE,
                borderRadius: "8px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "8px"
              }}
              onClick={login}
              disabled={loginLoading}
            >
              {loginLoading ? "Connexion..." : "Accéder au Planner →"}
            </button>
          </div>
        </div>

        {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
      </div>
    );
  }

  return (
    <div style={{ width: "100vw", height: "100vh", background: GRAY_50, fontFamily: "'Outfit','Segoe UI',sans-serif", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <style>{globalStyle}</style>

      <nav style={{ height: 60, background: "white", borderBottom: `1px solid ${GRAY_200}`, display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0 20px", zIndex: 100 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <ATLogo size={34} />
          <div style={{ fontSize: 14, fontWeight: 800, color: GRAY_800 }}>FTTH <span style={{ color: AT_BLUE }}>SMART</span> PLANNER</div>
        </div>
        <div style={{ fontSize: 11, color: GRAY_400 }}>Planification Architecturale</div>
      </nav>


      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <aside style={{ width: 310, background: "white", borderRight: `1px solid ${GRAY_200}`, padding: 20, overflowY: "auto" }}>
          <div style={cardStyle}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
              <span style={{ width: 28, height: 28, background: AT_BLUE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>📍</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Localisation</span>
            </div>

            {/* Steps indicator */}
            <div style={{ display: "flex", alignItems: "center", marginBottom: 16, gap: 4 }}>
              {steps.map((s, i) => (
                <div key={s.label} style={{ display: "flex", alignItems: "center", flex: 1 }}>
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: 1 }}>
                    <div style={{ width: 22, height: 22, borderRadius: "50%", background: s.done ? GREEN : s.active ? AT_BLUE : GRAY_200, color: (s.done || s.active) ? "white" : GRAY_400, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 10, fontWeight: 700 }}>
                      {s.done ? "✓" : i + 1}
                    </div>
                    <div style={{ fontSize: 9, color: s.done ? GREEN : s.active ? AT_BLUE : GRAY_400, fontWeight: 600, marginTop: 3, textAlign: "center" }}>{s.label}</div>
                  </div>
                  {i < steps.length - 1 && <div style={{ width: 20, height: 2, background: s.done ? GREEN : GRAY_200, flexShrink: 0, marginBottom: 18 }} />}
                </div>
              ))}
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={labelStyle}>1. Wilaya</label>
              <select style={inputStyle} value={ville} onChange={e => setVille(e.target.value)}>
                <option value="">{villesOpts.length === 0 ? "Chargement..." : "— Sélectionner une wilaya —"}</option>
                {villesOpts.map(v => <option key={v} value={v}>{v}</option>)}
              </select>
            </div>

            <div style={{ marginBottom: 12, opacity: ville ? 1 : 0.4, pointerEvents: ville ? "auto" : "none" }}>
              <label style={labelStyle}>2. Commune</label>
              <select style={inputStyle} value={commune} onChange={e => setCommune(e.target.value)} disabled={!ville}>
                <option value="">{!ville ? "Sélectionnez une wilaya" : communesOpts.length === 0 ? "Chargement..." : "— Sélectionner une commune —"}</option>
                {communesOpts.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>

            <div style={{ marginBottom: 16, opacity: commune ? 1 : 0.4, pointerEvents: commune ? "auto" : "none" }}>
              <label style={labelStyle}>3. Résidence / Cité</label>
              <ResidenceSearchSelect commune={commune} ville={ville} onSelect={handleResidenceSelect} selectedObj={residenceObj} disabled={!commune} />
              {residenceObj && (
                <div style={{ marginTop: 6, padding: "6px 10px", background: GREEN_LIGHT, borderRadius: 6, fontSize: 11, color: GREEN, fontWeight: 600 }}>
                  ✓ {commune} – {residenceObj.name}
                </div>
              )}
            </div>

            {(() => {
              const isImported = osmLoaded && lastImportedId === residenceObj?.osm_id;
              return (
                <button style={{ ...btnPrimary, background: isImported ? GREEN : AT_BLUE, marginTop: 4 }} onClick={importOSM} disabled={osmLoading || !residenceObj}>
                  {osmLoading ? "Chargement Résidence..." : isImported ? "✓ Résidence Synchronisée" : "Importez depuis OpenStreetMap"}
                </button>
              );
            })()}
          </div>

          {(() => {
            const isImported = osmLoaded && lastImportedId === residenceObj?.osm_id;
            return (
              <div style={{ ...cardStyle, opacity: isImported ? 1 : 0.4, pointerEvents: isImported ? "auto" : "none" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                  <span style={{ width: 28, height: 28, background: GREEN_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>🏗</span>
                  <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Vérification Structure</span>
                </div>
                <div style={{ fontSize: 11, color: GRAY_400, marginBottom: 10 }}>Modifiez si l'estimation OSM diffère de la réalité terrain.</div>
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Nombre d'Étages</label>
                  <input type="number" style={inputStyle} value={etages} onChange={e => setEtages(parseInt(e.target.value) || 1)} min={1} />
                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Logements / Étage</label>
                  <input type="number" style={inputStyle} value={logements} onChange={e => setLogements(parseInt(e.target.value) || 1)} min={1} />
                </div>
                <div style={{ marginBottom: 10 }}>
                  <label style={labelStyle}>Hauteur d'étage (m)</label>
                  <input type="number" step="0.1" style={inputStyle} value={hauteurEtage} onChange={e => setHauteurEtage(parseFloat(e.target.value) || 3.0)} min={1} />
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
                          onClick={() => setPresenceCommercial(val)}
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
                <button style={{ ...btnPrimary, background: `linear-gradient(135deg, ${AT_ORANGE}, #d97706)` }} onClick={lancerSectorisation}>
                  ▶ Lancer Sectorisation
                </button>
              </div>
            );
          })()}
        </aside>

        <div style={{ flex: 1, padding: 20, overflow: "auto", background: GRAY_50 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, alignItems: "stretch" }}>
              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 0, overflow: "hidden", minHeight: 450, display: "flex", flexDirection: "column" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, padding: "16px 20px", borderBottom: `1px solid ${GRAY_100}` }}>Plan de Sectorisation</div>
                {planGenerated && sectorisationSnapshot ? (
                  <BuildingPlan
                    key={sectorisationSnapshot.timestamp}
                    etages={sectorisationSnapshot.etages}
                    logements={sectorisationSnapshot.logements}
                    residenceName={sectorisationSnapshot.residenceName}
                    presenceCommercial={sectorisationSnapshot.presenceCommercial}
                    fatResults={sectorisationSnapshot.fatResults}
                    subscribersData={sectorisationSnapshot.subscribersData}
                  />
                ) : (
                  <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>
                    <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Sélectionnez et chargez un bâtiment</div>
                    <div style={{ fontSize: 12 }}>Puis lancez l'ingénierie pour générer l'architecture.</div>
                  </div>
                )}
              </div>

              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, overflow: "hidden", display: "flex", flexDirection: "column" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, padding: "16px 20px", borderBottom: `1px solid ${GRAY_100}` }}>Carte géographique OpenStreetMap</div>
                <div style={{ flex: 1, minHeight: 450 }}>
                  <LeafletMap
                    buildingsGeoJson={rawBuildings}
                    fatResults={fatResults}
                    onBuildingClick={handleBuildingClick}
                    selectedOsmId={residenceObj?.osm_id}
                    primaryTargetId={primaryTargetId}
                    isBloc={residenceObj?.is_bloc}
                  />
                </div>
              </div>
            </div>

            {kpis && (
              <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: "20px", boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, marginBottom: 16, display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ width: 4, height: 16, background: AT_ORANGE, borderRadius: 2 }} />
                  Indicateurs de Performance (KPIs)
                </div>
                <div style={{ overflow: "hidden", borderRadius: 8, border: `1px solid ${GRAY_200}` }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", background: "white" }}>
                    <thead>
                      <tr style={{ background: AT_BLUE }}>
                        <th style={{ padding: "14px 20px", textAlign: "left", fontSize: 11, fontWeight: 700, color: "white", textTransform: "uppercase", letterSpacing: "0.5px" }}>Indicateur</th>
                        <th style={{ padding: "14px 20px", textAlign: "left", fontSize: 11, fontWeight: 700, color: "white", textTransform: "uppercase", letterSpacing: "0.5px" }}>Valeur</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[
                        { label: "Total Abonnés estimés", value: kpis.totalAbonnes, suffix: "" },
                        { label: "Nombre de FATs nécessaires", value: kpis.fatsNeeded, suffix: "" },
                        { label: "Taux d'occupation des ports", value: kpis.fatsPortsUsed, suffix: " %" },
                        { label: "Linéaire fibre estimé", value: kpis.lineaire, suffix: " m" },
                      ].map((row, i) => (
                        <tr key={row.label} style={{ borderBottom: i === 3 ? "none" : `1px solid ${GRAY_100}`, background: i % 2 === 0 ? "white" : GRAY_50 }}>
                          <td style={{ padding: "14px 20px", fontSize: 13, color: GRAY_600, fontWeight: 500 }}>{row.label}</td>
                          <td style={{ padding: "14px 20px", fontSize: 14, color: GRAY_800, fontWeight: 800 }}>
                            {row.value}<span style={{ fontSize: 12, color: GRAY_400, marginLeft: 2, fontWeight: 600 }}>{row.suffix}</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );
}