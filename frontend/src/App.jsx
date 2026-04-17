import { useState, useEffect, useRef, useCallback } from "react";

/* ============================================================
   FTTH SMART PLANNER — React · Light Theme · Algérie Télécom
   Version v5.0 : Wilaya → Commune → Résidence
   ============================================================ */

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
const GRAY_600 = "#4B5563";
const GRAY_700 = "#374151";
const GRAY_800 = "#1F2937";
const GREEN = "#10B981";
const GREEN_LIGHT = "#D1FAE5";
const RED = "#EF4444";
const PURPLE = "#7C3AED";
const PURPLE_LIGHT = "#EDE9FE";

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
      <div style={{
        width: 30, height: 30, borderRadius: 8,
        background: c.bg, color: c.iconColor,
        display: "flex", alignItems: "center", justifyContent: "center",
        fontWeight: 700, fontSize: 14,
      }}>{c.icon}</div>
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
    <div style={{
      background: "white", borderRadius: 12, padding: "18px 20px",
      border: `1px solid ${GRAY_200}`, position: "relative", overflow: "hidden",
      boxShadow: "0 1px 4px rgba(0,0,0,0.05)",
    }}>
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
const FATNode = ({ id, connected, totalPorts, onHover, onLeave }) => (
  <div
    onMouseEnter={onHover}
    onMouseLeave={onLeave}
    style={{
      background: "white", border: `2px solid ${AT_ORANGE}`,
      borderRadius: 8, padding: "5px 8px", cursor: "pointer",
      boxShadow: `0 2px 8px rgba(247,148,29,0.25)`,
      minWidth: 90, textAlign: "center",
      transition: "transform 0.2s", zIndex: 10
    }}
  >
    <div style={{ fontSize: 8, fontWeight: 800, color: AT_ORANGE, letterSpacing: "0.5px", textTransform: "uppercase" }}>POINT FAT</div>
    <div style={{ fontSize: 9, fontWeight: 700, color: GRAY_700, fontFamily: "monospace", marginTop: 2 }}>FAT-ORA-{String(100 + id).padStart(3, "0")}</div>
    <div style={{ display: "flex", gap: 2, justifyContent: "center", marginTop: 3, flexWrap: "wrap" }}>
      {Array.from({ length: totalPorts || 8 }).map((_, i) => (
        <div key={i} style={{ width: 5, height: 5, borderRadius: "50%", background: i < connected ? GREEN : AT_BLUE }} />
      ))}
    </div>
  </div>
);

// ── BuildingPlan ────────────────────────────────────────────
const BuildingPlan = ({ etages, logements, residenceName }) => {
  const [hoveredFatId, setHoveredFatId] = useState(null);
  const totalAbonnes = etages * logements;
  const fatsNeeded = Math.ceil(totalAbonnes / 10);
  const limitPerFat = Math.ceil(totalAbonnes / fatsNeeded);

  const fatFloors = [];
  if (fatsNeeded === 1) fatFloors.push(Math.floor(etages / 2));
  else if (fatsNeeded === 2) { fatFloors.push(Math.floor(etages * 0.7)); fatFloors.push(Math.floor(etages * 0.25)); }
  else { for (let i = 0; i < fatsNeeded; i++) fatFloors.push(Math.round(i * (etages - 1) / (fatsNeeded - 1))); }

  return (
    <div style={{ padding: 20, overflowX: "auto" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
        <div>
          <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800 }}>{residenceName || "Résidence"}</div>
          <div style={{ fontSize: 11, color: GRAY_400 }}>{etages} étages · {logements} logements/étage · {fatsNeeded} FAT(s)</div>
        </div>
      </div>
      {(() => {
        let fi = 0;
        return Array.from({ length: etages + 1 }, (_, i) => etages - i).map((e) => {
          const isFatFloor = fatFloors.includes(e);
          const rawFatId = isFatFloor ? fi++ : null;
          const logicalFatId = isFatFloor ? fatsNeeded - 1 - rawFatId : null;
          const fatCol = Math.floor(logements / 2) - 1;

          return (
            <div key={e} style={{ marginBottom: 2 }}>
              <div style={{ display: "flex", alignItems: "stretch", gap: 0 }}>
                <div style={{
                  width: 64, flexShrink: 0, display: "flex", alignItems: "center",
                  justifyContent: "flex-end", paddingRight: 10, fontSize: 9, fontWeight: 700,
                  color: GRAY_400, letterSpacing: "0.5px", textTransform: "uppercase",
                }}>
                  {e === 0 ? "RDC" : `ÉT. ${e}`}
                </div>
                <div style={{ width: 6, background: AT_BLUE, borderRadius: "4px 0 0 4px", opacity: 0.7 }} />
                <div style={{
                  flex: 1, display: "flex",
                  background: GRAY_50, border: `1px solid ${GRAY_200}`,
                  borderLeft: "none", borderRight: "none",
                  minHeight: isFatFloor ? 90 : 64, position: "relative",
                }}>
                  {Array.from({ length: logements }).map((_, l) => {
                    const isLastUnit = l === logements - 1;
                    const isFatBetween = isFatFloor && l === fatCol;
                    const doorNumber = e * logements + l + 1;
                    const fatAssignId = Math.floor((doorNumber - 1) / limitPerFat);
                    const isHovered = hoveredFatId !== null && hoveredFatId === fatAssignId;

                    return (
                      <div key={l} style={{ display: "flex", flex: 1, alignItems: "stretch" }}>
                        <div style={{
                          flex: 1, display: "flex", flexDirection: "column",
                          alignItems: "center", justifyContent: "center",
                          padding: "8px 4px", gap: 4, zIndex: 5,
                          background: isHovered ? AT_ORANGE_LIGHT : "white",
                          border: isHovered ? `2px solid ${AT_ORANGE}` : `1px solid ${GRAY_200}`,
                          borderRadius: 4, margin: "4px 2px", minWidth: 56,
                        }}>
                          <div style={{ fontSize: 9, fontWeight: 700, color: GRAY_600, fontFamily: "monospace" }}>P.{doorNumber}</div>
                          <div style={{ width: 6, height: 6, borderRadius: "50%", background: isHovered ? AT_ORANGE : AT_BLUE }} />
                        </div>
                        {!isLastUnit && (
                          <div style={{
                            width: isFatBetween ? 100 : 20,
                            display: "flex", flexDirection: "column",
                            alignItems: "center", justifyContent: "center",
                            background: isFatBetween ? AT_ORANGE_LIGHT : "transparent",
                            borderLeft: isFatBetween ? `1px dashed ${isHovered ? AT_ORANGE : GRAY_200}` : "none",
                            borderRight: isFatBetween ? `1px dashed ${isHovered ? AT_ORANGE : GRAY_200}` : "none",
                            position: "relative", flexShrink: 0,
                          }}>
                            {/* Flèche SVG inclinée connectant le bloc vers la colonne centrale */}
                            {!isFatBetween && (
                              <svg style={{ position: "absolute", width: "100%", height: "100%", top: 0, left: 0, pointerEvents: "none" }}>
                                <defs>
                                  <marker id={`arrow-${isHovered ? 'hover' : 'normal'}`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto">
                                    <path d="M 0 0 L 10 5 L 0 10 z" fill={isHovered ? AT_ORANGE : GRAY_300} />
                                  </marker>
                                </defs>
                                <line x1="0" y1="50%" x2="100%" y2={e > fatFloors[0] ? "20%" : "80%"} stroke={isHovered ? AT_ORANGE : GRAY_300} strokeWidth="1.5" strokeDasharray="3 3" markerEnd={`url(#arrow-${isHovered ? 'hover' : 'normal'})`} />
                              </svg>
                            )}

                            {isFatBetween && logicalFatId !== null && (
                              <FATNode
                                id={logicalFatId}
                                connected={Math.min(limitPerFat, totalAbonnes - logicalFatId * limitPerFat)}
                                totalPorts={limitPerFat}
                                onHover={() => setHoveredFatId(logicalFatId)}
                                onLeave={() => setHoveredFatId(null)}
                              />
                            )}
                            {!isFatBetween && <div style={{ width: 1, height: "100%", background: GRAY_300, opacity: 0.3 }} />}
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {e > 0 && (
                    <div style={{ position: "absolute", left: "50%", top: -12, transform: "translateX(-50%)", width: 0, height: 12, borderLeft: `2px dashed ${AT_ORANGE}88` }} />
                  )}
                </div>
                <div style={{ width: 6, background: AT_BLUE, borderRadius: "0 4px 4px 0", opacity: 0.7 }} />
              </div>
            </div>
          );
        });
      })()}
      <div style={{ display: "flex", alignItems: "center", paddingLeft: 70, marginTop: 4 }}>
        <div style={{ flex: 1, height: 8, background: `linear-gradient(90deg, ${GRAY_400}, ${GRAY_300})`, borderRadius: 4 }} />
      </div>
      <div style={{ textAlign: "center", fontSize: 10, color: GRAY_400, marginTop: 4 }}>VOIRIE / RUE</div>
    </div>
  );
};

// ── Leaflet loader ────────────────────────────────────────────────────────────
const LEAFLET_CSS = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
const LEAFLET_JS  = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";

function loadLeaflet() {
  return new Promise((resolve, reject) => {
    if (window.L) { resolve(window.L); return; }
    if (!document.querySelector(`link[href="${LEAFLET_CSS}"]`)) {
      const link = document.createElement("link");
      link.rel = "stylesheet"; link.href = LEAFLET_CSS;
      document.head.appendChild(link);
    }
    if (!document.querySelector(`script[src="${LEAFLET_JS}"]`)) {
      const script = document.createElement("script");
      script.src = LEAFLET_JS;
      script.onload = () => resolve(window.L);
      script.onerror = () => reject(new Error("Impossible de charger Leaflet"));
      document.head.appendChild(script);
    } else {
      const wait = setInterval(() => {
        if (window.L) { clearInterval(wait); resolve(window.L); }
      }, 50);
    }
  });
}

// ── LeafletMap ────────────────────────────────────────────────────────────────
const LeafletMap = ({ buildingsGeoJson, fatResults }) => {
  const mapRef            = useRef(null);
  const mapInstanceRef    = useRef(null);
  const buildingsLayerRef = useRef(null);
  const fatsLayerRef      = useRef(null);
  const [leafletReady, setLeafletReady] = useState(false);

  useEffect(() => {
    loadLeaflet()
      .then(() => setLeafletReady(true))
      .catch(err => console.error("Leaflet load error:", err));
  }, []);

  useEffect(() => {
    if (!leafletReady || !mapRef.current) return;
    if (mapInstanceRef.current) return;

    const L   = window.L;
    const map = L.map(mapRef.current, { zoomControl: true });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "© OpenStreetMap", maxZoom: 19,
    }).addTo(map);
    map.setView([35.7, -0.65], 13);
    mapInstanceRef.current = map;
  }, [leafletReady]);

  useEffect(() => {
    if (!leafletReady || !mapInstanceRef.current) return;

    const L   = window.L;
    const map = mapInstanceRef.current;

    if (buildingsLayerRef.current) { map.removeLayer(buildingsLayerRef.current); buildingsLayerRef.current = null; }
    if (fatsLayerRef.current)      { map.removeLayer(fatsLayerRef.current);      fatsLayerRef.current      = null; }

    if (buildingsGeoJson) {
      try {
        const geoJsonData = typeof buildingsGeoJson === "string" ? JSON.parse(buildingsGeoJson) : buildingsGeoJson;
        buildingsLayerRef.current = L.geoJSON(geoJsonData, {
          style: (feature) => {
            const isTarget = feature.properties?.is_target;
            return {
              color: isTarget ? "#F7941D" : "#9CA3AF",
              weight: isTarget ? 3 : 1.5,
              opacity: 0.9,
              fillColor: isTarget ? "#FEF3E6" : "#F3F4F6",
              fillOpacity: isTarget ? 0.8 : 0.4,
            };
          },
          onEachFeature: (feature, layer) => {
            const isTarget = feature.properties?.is_target;
            const nom = feature.properties?.id_batiment || "Bâtiment";
            layer.bindPopup(`<b>${nom}</b><br>${isTarget ? "Résidence sélectionnée" : "Bâtiment voisin"}`);
          },
        }).addTo(map);

        const bounds = buildingsLayerRef.current.getBounds();
        if (bounds.isValid()) map.fitBounds(bounds, { padding: [40, 40] });
      } catch (e) { console.error("Erreur parsing GeoJSON:", e); }
    }

    if (fatResults && fatResults.length > 0) {
      fatsLayerRef.current = L.layerGroup().addTo(map);
      fatResults.forEach((fat, index) => {
        if (fat.centroid_lat && fat.centroid_lon) {
          L.circleMarker([fat.centroid_lat, fat.centroid_lon], {
            radius: 8, fillColor: "#10B981", color: "#fff", weight: 2, opacity: 1, fillOpacity: 0.9,
          })
            .bindPopup(`<b>FAT #${index + 1}</b><br>${fat.n_subscribers || 0} abonnés`)
            .addTo(fatsLayerRef.current);
        }
      });
    }
  }, [leafletReady, buildingsGeoJson, fatResults]);

  return (
    <div style={{ height: "100%", minHeight: 400, borderRadius: 10, overflow: "hidden", position: "relative" }}>
      {!leafletReady && (
        <div style={{
          position: "absolute", inset: 0, display: "flex",
          alignItems: "center", justifyContent: "center",
          background: "#F3F4F6", zIndex: 10, fontSize: 13, color: "#9CA3AF",
        }}> Chargement de la carte… </div>
      )}
      <div ref={mapRef} style={{ width: "100%", height: "100%", minHeight: 400 }} />
    </div>
  );
};

// ── ResidenceSearchSelect ──────────────────────────────────
const ResidenceSearchSelect = ({ commune, ville, onSelect, selectedObj, disabled }) => {
  const [query, setQuery] = useState("");
  const [options, setOptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const containerRef = useRef(null);

  useEffect(() => {
    if (!commune) { setOptions([]); setQuery(""); return; }
    setLoading(true); setOptions([]); setQuery("");
    const url = `${API}/api/residence?ville=${encodeURIComponent(ville)}&commune=${encodeURIComponent(commune)}`;
    console.log(`[Fetch] Résidences: ${url}`);
    fetch(url)
      .then(r => r.json())
      .then(d => {
        console.log("[Response] Résidences:", d);
        setOptions(d.residences || []);
        setTotalCount(d.count || 0);
      })
      .catch((err) => {
        console.error("[Error] Résidences:", err);
        setOptions([]);
      })
      .finally(() => setLoading(false));
  }, [commune, ville]);

  const filtered = query.trim() ? options.filter(r => r.name.toLowerCase().includes(query.toLowerCase())) : options;
  const displayed = filtered.slice(0, 80);

  useEffect(() => {
    const handler = (e) => { if (containerRef.current && !containerRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const selectedLabel = selectedObj?.name || null;

  return (
    <div ref={containerRef} style={{ position: "relative" }}>
      <div
        style={{
          display: "flex", alignItems: "center",
          border: `1.5px solid ${open ? AT_BLUE : GRAY_200}`,
          borderRadius: 8, background: disabled ? GRAY_100 : "white",
          padding: "0 12px", gap: 8, opacity: disabled ? 0.5 : 1,
          cursor: disabled ? "not-allowed" : "text", transition: "border-color 0.2s",
        }}
        onClick={() => !disabled && setOpen(true)}
      >
        <span style={{ fontSize: 14, color: GRAY_400 }}>🔍</span>
        <input
          type="text"
          value={open ? query : (selectedLabel || "")}
          placeholder={
            loading ? "Chargement des bâtiments..." :
            !commune ? "Sélectionnez une commune d'abord" :
            `Rechercher parmi ${totalCount} bâtiment${totalCount > 1 ? "s" : ""}...`
          }
          disabled={disabled || !commune}
          readOnly={!open}
          onChange={e => setQuery(e.target.value)}
          onFocus={() => !disabled && setOpen(true)}
          style={{
            flex: 1, border: "none", outline: "none", fontSize: 13, color: GRAY_800,
            background: "transparent", padding: "10px 0", cursor: disabled ? "not-allowed" : "text",
          }}
        />
        {loading && (
          <div style={{ width: 16, height: 16, border: `2px solid ${AT_BLUE}`, borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.8s linear infinite", flexShrink: 0 }} />
        )}
        {!loading && selectedObj && !open && (
          <button onClick={e => { e.stopPropagation(); onSelect(null); setQuery(""); }} style={{ background: "none", border: "none", color: GRAY_400, cursor: "pointer", fontSize: 14, padding: 0 }}>✕</button>
        )}
      </div>

      {open && !disabled && (
        <div style={{
          position: "absolute", top: "100%", left: 0, right: 0, zIndex: 1000,
          background: "white", border: `1.5px solid ${AT_BLUE}`, borderTop: "none",
          borderRadius: "0 0 8px 8px", maxHeight: 260, overflowY: "auto",
          boxShadow: "0 8px 24px rgba(0,91,170,0.12)",
        }}>
          <div style={{ padding: "8px 12px", fontSize: 11, color: GRAY_400, borderBottom: `1px solid ${GRAY_100}`, background: GRAY_50, fontWeight: 600 }}>
            {query ? `${filtered.length} résultat${filtered.length > 1 ? "s" : ""} pour "${query}"` : `${totalCount} bâtiment résidentiel${totalCount > 1 ? "s" : ""} disponible${totalCount > 1 ? "s" : ""}`}
            {displayed.length < filtered.length && ` · affiché ${displayed.length}`}
          </div>

          {displayed.length === 0 && !loading && (
            <div style={{ padding: "20px 12px", textAlign: "center", color: GRAY_400, fontSize: 12 }}>Aucun résultat</div>
          )}

          {displayed.map((res, i) => {
            const isSelected = res.osm_id === selectedObj?.osm_id;
            const icon =
              res.building_type === "apartments" ? "🏢" :
              res.building_type === "house" ? "🏠" : "🏡";

            const subParts = [];
            if (res.levels) subParts.push(`${res.levels} étage${res.levels > 1 ? "s" : ""}`);
            if (res.units) subParts.push(`${res.units} logement${res.units > 1 ? "s" : ""}`);
            const subLabel = subParts.join(" · ");

            return (
              <div
                key={res.osm_id || i}
                onClick={() => { onSelect(res); setOpen(false); setQuery(""); }}
                style={{
                  padding: "8px 12px", cursor: "pointer", fontSize: 12, borderBottom: `1px solid ${GRAY_100}`,
                  background: isSelected ? AT_BLUE_LIGHT : "white", color: isSelected ? AT_BLUE : GRAY_800,
                  fontWeight: isSelected ? 700 : 400, display: "flex", alignItems: "center", gap: 8,
                }}
                onMouseEnter={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : GRAY_50}
                onMouseLeave={e => e.currentTarget.style.background = isSelected ? AT_BLUE_LIGHT : "white"}
              >
                <span style={{ fontSize: 14, flexShrink: 0 }}>{icon}</span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{res.name}</div>
                  {subLabel && <div style={{ fontSize: 10, color: isSelected ? AT_BLUE : GRAY_400, marginTop: 1 }}>{subLabel}</div>}
                </div>
                {isSelected && <span style={{ color: AT_BLUE, fontSize: 14, flexShrink: 0 }}>✓</span>}
              </div>
            );
          })}
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
  const [activeTab, setActiveTab] = useState("planner");
  const [notif, setNotif] = useState(null);
  const [showPassword, setShowPassword] = useState(false);
  const [loginData, setLoginData] = useState({ id: "k.benali@at.dz", password: "atdz2026" });
  const [loginLoading, setLoginLoading] = useState(false);

  const [ville, setVille]         = useState("");
  const [commune, setCommune]     = useState("");
  const [residenceObj, setResidenceObj] = useState(null);

  const [villesOpts, setVillesOpts]     = useState([]);
  const [communesOpts, setCommunesOpts] = useState([]);

  // Structure bâtie (mise à jour via OSM ou manuel)
  const [etages, setEtages]       = useState(5);
  const [logements, setLogements] = useState(4);
  const [fatCap, setFatCap]       = useState(8);

  const [osmLoaded, setOsmLoaded]     = useState(false);
  const [osmLoading, setOsmLoading]   = useState(false);
  const [planGenerated, setPlanGenerated] = useState(false);
  const [kpis, setKpis]               = useState(null);
  const [rawBuildings, setRawBuildings] = useState(null);
  const [subscribersData, setSubscribersData] = useState([]);
  const [fatResults, setFatResults]   = useState([]);

  const notify = useCallback((type, title, sub) => setNotif({ type, title, sub }), []);

  useEffect(() => {
    console.log(`[Fetch] Villes: ${API}/api/ville`);
    fetch(`${API}/api/ville`)
      .then(r => r.json())
      .then(d => {
        console.log("[Response] Villes:", d);
        setVillesOpts(d.villes || []);
      })
      .catch((err) => {
        console.error("[Error] Villes:", err);
        notify("error", "Connexion API", "Impossible de contacter le backend");
      });
  }, [notify]);

  useEffect(() => {
    setCommune(""); setResidenceObj(null); setCommunesOpts([]);
    if (!ville) return;
    const url = `${API}/api/commune?ville=${encodeURIComponent(ville)}`;
    console.log(`[Fetch] Communes: ${url}`);
    fetch(url)
      .then(r => r.json())
      .then(d => {
        console.log("[Response] Communes:", d);
        setCommunesOpts(d.communes || []);
      })
      .catch((err) => {
        console.error("[Error] Communes:", err);
        notify("error", "Erreur", "Impossible de charger les communes");
      });
  }, [ville, notify]);

  useEffect(() => { setResidenceObj(null); }, [commune]);

  const login = () => {
    setLoginLoading(true);
    setTimeout(() => {
      setLoginLoading(false);
      setScreen("dashboard");
      notify("success", "Connexion réussie", "Bienvenue sur l'espace d'ingénierie");
    }, 1200);
  };

  const importOSM = async () => {
    if (!ville || !commune || !residenceObj) {
      notify("error", "Données manquantes", "Veuillez sélectionner votre cible");
      return;
    }
    setOsmLoading(true);
    const payload = {
      ville, commune,
      residence: residenceObj.name,
      lat: residenceObj.lat,
      lon: residenceObj.lon,
      nombre_etages: etages,
      logements_par_etage: logements
    };
    console.log("[Action] Import OSM - Payload:", payload);
    try {
      const resp = await fetch(`${API}/api/importOSM`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await resp.json();
      console.log("[Response] Import OSM:", data);
      if (!resp.ok) throw new Error(data.detail || "Erreur Import");

      setRawBuildings(data.buildings_geojson);
      setSubscribersData(data.subscribers);

      if (data.etages_detectes) setEtages(data.etages_detectes);
      if (data.logements_detectes) setLogements(data.logements_detectes);

      setOsmLoaded(true);
      notify("success", "Carte & Données synchronisées", `Bâtiment ciblé et voisinage importés`);
    } catch (err) {
      console.error("[Error] Import OSM:", err);
      notify("error", "Erreur réseau", err.message);
    } finally {
      setOsmLoading(false);
    }
  };

  const lancerSectorisation = async () => {
    if (!osmLoaded) { notify("info", "Import requis", "Importez d'abord les données de la résidence"); return; }
    if (!subscribersData || subscribersData.length === 0) {
      notify("error", "Données manquantes", "Aucun abonné détecté pour la sectorisation.");
      return;
    }
    notify("info", "Traitement Algorithmique", "Positionnement dynamique...");
    try {
      console.log("[Action] Sectorisation - Emplacement FATs");
      const req1 = await fetch(`${API}/api/emplacementFATs`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ subscribers: subscribersData })
      });
      const data1 = await req1.json();
      console.log("[Response] Emplacement FATs:", data1);
      if (!req1.ok) throw new Error(data1.detail || `Erreur FAT (${req1.status})`);

      console.log("[Action] Sectorisation - Nommage FATs");
      const req2 = await fetch(`${API}/api/nomFAT`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fat_candidates: data1.fat_candidates, subscribers: subscribersData })
      });
      const data2 = await req2.json();
      console.log("[Response] Nommage FATs:", data2);
      if (!req2.ok) throw new Error(data2.detail || `Erreur nommage FAT (${req2.status})`);

      const finalFats = data2.fat_candidates_with_ids || data1.fat_candidates;
      setFatResults(finalFats);

      const totalAbonnes = subscribersData.length;
      const fatsNeeded = finalFats.length;
      const kpisResult = {
        totalAbonnes, fatsNeeded,
        fatsPortsUsed: Math.round((totalAbonnes / (fatsNeeded * fatCap || 1)) * 100),
        lineaire: Math.round(finalFats.reduce((acc, f) => acc + (f.cable_m_to_fdt_real || 0), 0))
      };

      setKpis(kpisResult);
      setPlanGenerated(true);
      notify("success", "Sectorisation terminée", `Topologie générée pour ${fatsNeeded} boîtiers`);
    } catch (err) {
      console.error("[Error] Sectorisation:", err);
      notify("error", "Échec process", err.message);
    }
  };

  const inputStyle = { width: "100%", padding: "10px 14px", background: "white", border: `1.5px solid ${GRAY_200}`, borderRadius: 8, color: GRAY_800, fontSize: 13, boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 700, color: GRAY_600, marginBottom: 5, display: "block", letterSpacing: "0.5px", textTransform: "uppercase" };
  const btnPrimary = { background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`, color: "white", border: "none", borderRadius: 8, padding: "11px 20px", fontSize: 13, fontWeight: 700, cursor: "pointer", width: "100%" };
  const cardStyle = { background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.05)", marginBottom: 14 };
  const globalStyle = `
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    @keyframes spin { to { transform: rotate(360deg) } }
    html, body, #root { margin:0 !important; padding:0 !important; width:100vw !important; height:100vh !important; overflow:hidden !important; }
    select:focus, input:focus { outline: none; border-color: ${AT_BLUE} !important; }
  `;

  const steps = [
    { label: "Wilaya",    done: !!ville,    active: !ville },
    { label: "Commune",   done: !!commune,  active: !!ville && !commune },
    { label: "Résidence", done: !!residenceObj, active: !!commune && !residenceObj },
  ];

  if (screen === "login") {
    return (
      <div style={{ width: "100vw", height: "100vh", background: `linear-gradient(135deg, ${AT_BLUE_LIGHT} 0%, white 50%, ${AT_ORANGE_LIGHT} 100%)`, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <style>{globalStyle}</style>
        <div style={{ background: "white", borderRadius: 20, padding: 48, width: 440, boxShadow: "0 24px 80px rgba(0,91,170,0.15)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 36 }}>
            <ATLogo size={52} />
            <div>
              <div style={{ fontSize: 18, fontWeight: 800, color: GRAY_800 }}>FTTH <span style={{ color: AT_BLUE }}>SMART</span> PLANNER</div>
              <div style={{ fontSize: 10, color: GRAY_400 }}>Algérie Télécom · Interface Pro</div>
            </div>
          </div>
          <div style={{ fontSize: 24, fontWeight: 800, color: GRAY_800, marginBottom: 6 }}>Connexion Sécurisée</div>
          <div style={{ fontSize: 13, color: GRAY_400, marginBottom: 32 }}>Accès SaaS ingénierie réseau</div>

          <div style={{ marginBottom: 18 }}>
            <label style={labelStyle}>Identifiant Agent</label>
            <input style={inputStyle} type="text" value={loginData.id} onChange={e => setLoginData(p => ({ ...p, id: e.target.value }))} placeholder="ex: agent@at.dz" />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label style={labelStyle}>Mot de passe</label>
            <div style={{ position: "relative" }}>
              <input style={{ ...inputStyle, paddingRight: 44 }} type={showPassword ? "text" : "password"} value={loginData.password} onChange={e => setLoginData(p => ({ ...p, password: e.target.value }))} placeholder="••••••••" />
              <button onClick={() => setShowPassword(p => !p)} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", color: GRAY_400 }}>{showPassword ? "🙈" : "👁"}</button>
            </div>
          </div>
          <button style={btnPrimary} onClick={login} disabled={loginLoading}>{loginLoading ? "Authentification..." : "Accéder au Planner →"}</button>
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
        <div style={{ fontSize: 11, color: GRAY_400 }}>Planification Architecturale v5.0</div>
      </nav>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, padding: "16px 20px", background: GRAY_50, borderBottom: `1px solid ${GRAY_200}` }}>
        <KPICard label="Abonnés Estimés" value={kpis?.totalAbonnes ?? null} sub={`${etages} étages × ${logements} log/ét.`} color="blue" icon="👥" />
        <KPICard label="FATs Proposées" value={kpis?.fatsNeeded ?? null} sub="16 ports/FAT" color="orange" icon="📡" />
        <KPICard label="Linéaire Fibre" value={kpis?.lineaire ?? null} suffix="m" sub="Câble fibre estimé" color="purple" icon="🔌" />
        <KPICard label="Ports Utilisés" value={kpis?.fatsPortsUsed ?? null} suffix="%" sub="Taux d'utilisation" color="green" icon="📶" />
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <aside style={{ width: 310, background: "white", borderRight: `1px solid ${GRAY_200}`, padding: 20, overflowY: "auto" }}>
          <div style={cardStyle}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
              <span style={{ width: 28, height: 28, background: AT_BLUE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>📍</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Localisation</span>
            </div>

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
              <ResidenceSearchSelect commune={commune} ville={ville} onSelect={setResidenceObj} selectedObj={residenceObj} disabled={!commune} />
              {residenceObj && <div style={{ marginTop: 6, padding: "6px 10px", background: GREEN_LIGHT, borderRadius: 6, fontSize: 11, color: GREEN, fontWeight: 600 }}>✓ {residenceObj.name}</div>}
            </div>

            <button style={{ ...btnPrimary, background: osmLoaded ? GREEN : AT_BLUE, marginTop: 4 }} onClick={importOSM} disabled={osmLoading || !residenceObj}>
              {osmLoading ? "Chargement Résidence..." : osmLoaded ? "✓ Résidence Synchronisée" : "Import depuis OpenStreetMap"}
            </button>
          </div>

          <div style={{ ...cardStyle, opacity: osmLoaded ? 1 : 0.4, pointerEvents: osmLoaded ? "auto" : "none" }}>
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
              <label style={labelStyle}>Logements par Étage</label>
              <input type="number" style={inputStyle} value={logements} onChange={e => setLogements(parseInt(e.target.value) || 1)} min={1} />
            </div>
            <button style={{ ...btnPrimary, background: `linear-gradient(135deg, ${AT_ORANGE}, #d97706)` }} onClick={lancerSectorisation}>
              ▶ Lancer Sectorisation
            </button>
          </div>
        </aside>

        <div style={{ flex: 1, padding: 20, overflow: "auto", background: GRAY_50 }}>
          {activeTab === "planner" && (
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, padding: 0, overflow: "hidden", minHeight: 450 }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, padding: "16px 20px", borderBottom: `1px solid ${GRAY_100}` }}>Plan de Sectorisation</div>
                  {planGenerated ? (
                    <BuildingPlan residenceName={residenceObj?.name} />
                  ) : (
                    <div style={{ textAlign: "center", padding: 80, color: GRAY_400 }}>

                      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Sélectionnez et chargez un bâtiment</div>
                      <div style={{ fontSize: 12 }}>Puis lancez l'ingénierie pour générer l'architecture.</div>
                    </div>
                  )}
                </div>

                <div style={{ background: "white", borderRadius: 12, border: `1px solid ${GRAY_200}`, overflow: "hidden" }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, padding: "16px 20px", borderBottom: `1px solid ${GRAY_100}` }}>Carte Geographique OpenStreetMap</div>
                  <div style={{ height: "100%", minHeight: 450 }}>
                    <LeafletMap buildingsGeoJson={rawBuildings} fatResults={fatResults} />
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );
}