import { useState, useEffect, useRef, useCallback } from "react";

/* ============================================================
   FTTH SMART PLANNER — React · Light Theme · Algérie Télécom
   Palette: AT Blue #005BAA + Orange #F7941D + White
   FULL SCREEN VERSION — 100vw × 100vh (laptop ready)
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

// ── Algérie Télécom SVG logo ────────────────────────────────
const ATLogo = ({ size = 40 }) => (
  <svg width={size} height={size} viewBox="0 0 60 60" fill="none">
    <rect width="60" height="60" rx="10" fill={AT_BLUE} />
    <text x="30" y="38" textAnchor="middle" fill="white" fontSize="22" fontWeight="900" fontFamily="Arial Black, sans-serif">AT</text>
    <rect x="10" y="44" width="40" height="3" rx="1.5" fill={AT_ORANGE} />
  </svg>
);

// ── Sample data ─────────────────────────────────────────────
const PROJECTS_ARCHIVE = [
  { id: 1, name: "Rés. Les Falaises", ville: "Oran", quartier: "Seddikia", date: "27/03/2026", etages: 5, logements: 4 },
  { id: 2, name: "El Bahia Tower",    ville: "Oran", quartier: "Carteaux",  date: "25/03/2026", etages: 8, logements: 6 },
  { id: 3, name: "Rés. Atlas B3",     ville: "Oran", quartier: "Victor Hugo", date: "22/03/2026", etages: 6, logements: 4 },
  { id: 4, name: "Cité AADL Nedjma",  ville: "Oran", quartier: "Haï Nedjma", date: "18/03/2026", etages: 10, logements: 8 },
  { id: 5, name: "Rés. Yasmine",      ville: "Oran", quartier: "Gambetta",   date: "15/03/2026", etages: 4, logements: 4 },
  { id: 6, name: "Tour Gambetta",     ville: "Oran", quartier: "Gambetta",   date: "10/03/2026", etages: 12, logements: 6 },
  { id: 7, name: "Cité AADL Alger-Est", ville: "Alger", quartier: "Bab Ezzouar", date: "05/03/2026", etages: 8, logements: 4 },
  { id: 8, name: "Rés. Zouaghi",      ville: "Constantine", quartier: "Centre", date: "28/02/2026", etages: 7, logements: 4 },
];

// ── Notification component ──────────────────────────────────
const Notification = ({ notif, onClose }) => {
  useEffect(() => {
    const t = setTimeout(onClose, 4000);
    return () => clearTimeout(t);
  }, [onClose]);

  const colors = {
    success: { bg: GREEN_LIGHT, border: GREEN, icon: "✓", iconColor: GREEN },
    info: { bg: AT_BLUE_LIGHT, border: AT_BLUE, icon: "ℹ", iconColor: AT_BLUE },
    error: { bg: "#FEE2E2", border: RED, icon: "✕", iconColor: RED },
  };
  const c = colors[notif.type] || colors.info;

  return (
    <div style={{
      position: "fixed", bottom: 24, right: 24, zIndex: 9999,
      background: "white", border: `1.5px solid ${c.border}`,
      borderRadius: 12, padding: "14px 18px",
      display: "flex", alignItems: "flex-start", gap: 12,
      boxShadow: "0 8px 32px rgba(0,0,0,0.12)", maxWidth: 360,
      animation: "slideUp 0.3s ease",
    }}>
      <div style={{
        width: 30, height: 30, borderRadius: 8,
        background: c.bg, color: c.iconColor,
        display: "flex", alignItems: "center", justifyContent: "center",
        fontWeight: 700, fontSize: 14, flexShrink: 0,
      }}>{c.icon}</div>
      <div style={{ flex: 1 }}>
        <div style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>{notif.title}</div>
        <div style={{ fontSize: 11, color: GRAY_600, marginTop: 2 }}>{notif.sub}</div>
      </div>
      <button onClick={onClose} style={{ background: "none", border: "none", cursor: "pointer", color: GRAY_400, fontSize: 16, lineHeight: 1 }}>✕</button>
    </div>
  );
};

// ── KPI Card ────────────────────────────────────────────────
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
        {value !== null ? value : <div style={{ width: 60, height: 28, background: GRAY_200, borderRadius: 6, animation: "shimmer 1.5s infinite" }} />}
        {value !== null && <span style={{ fontSize: 16 }}>{suffix}</span>}
      </div>
      <div style={{ fontSize: 11, color: GRAY_400, marginTop: 4 }}>{sub}</div>
    </div>
  );
};

// ── FAT node ────────────────────────────────────────────────
const FATNode = ({ id }) => (
  <div style={{
    background: "white", border: `2px solid ${AT_ORANGE}`,
    borderRadius: 8, padding: "5px 8px",
    boxShadow: `0 2px 8px rgba(247,148,29,0.25)`,
    minWidth: 90, textAlign: "center",
  }}>
    <div style={{ fontSize: 8, fontWeight: 800, color: AT_ORANGE, letterSpacing: "0.5px", textTransform: "uppercase" }}>POINT FAT</div>
    <div style={{ fontSize: 9, fontWeight: 700, color: GRAY_700, fontFamily: "monospace", marginTop: 2 }}>FAT-ORA-{String(100 + id).padStart(3, "0")}</div>
    <div style={{ display: "flex", gap: 2, justifyContent: "center", marginTop: 3, flexWrap: "wrap" }}>
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} style={{ width: 5, height: 5, borderRadius: "50%", background: i < 5 ? AT_BLUE : GRAY_200 }} />
      ))}
    </div>
  </div>
);

// ── Building floor plan ─────────────────────────────────────
const BuildingPlan = ({ etages, logements, residence }) => {
  const totalAbonnes = etages * logements;
  const fatsNeeded = Math.ceil(totalAbonnes / 10);

  const fatFloors = [];
  if (fatsNeeded === 1) fatFloors.push(Math.floor(etages / 2));
  else if (fatsNeeded === 2) { fatFloors.push(Math.floor(etages * 0.7)); fatFloors.push(Math.floor(etages * 0.25)); }
  else { for (let i = 0; i < fatsNeeded; i++) fatFloors.push(Math.round(i * (etages - 1) / (fatsNeeded - 1))); }

  return (
    <div style={{ padding: 20, overflowX: "auto" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
        <span style={{ fontSize: 20 }}>🏢</span>
        <div>
          <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800 }}>{residence || "Résidence AADL"}</div>
          <div style={{ fontSize: 11, color: GRAY_400 }}>{etages} étages · {logements} logements/étage · {fatsNeeded} FAT(s)</div>
        </div>
      </div>

      {(() => {
        let fi = 0;
        return Array.from({ length: etages + 1 }, (_, i) => etages - i).map((e) => {
          const isFatFloor = fatFloors.includes(e);
          const fatId = isFatFloor ? fi++ : null;
          const fatCol = Math.floor(logements / 2) - 1;

          return (
            <div key={e} style={{ marginBottom: 2 }}>
              <div style={{ display: "flex", alignItems: "stretch", gap: 0 }}>
                <div style={{
                  width: 64, flexShrink: 0,
                  display: "flex", alignItems: "center", justifyContent: "flex-end",
                  paddingRight: 10, fontSize: 9, fontWeight: 700,
                  color: GRAY_400, letterSpacing: "0.5px", textTransform: "uppercase",
                }}>
                  {e === 0 ? "RDC" : `ÉT. ${e}`}
                </div>
                <div style={{ width: 6, background: AT_BLUE, borderRadius: "4px 0 0 4px", opacity: 0.7 }} />
                <div style={{
                  flex: 1, display: "flex",
                  background: GRAY_50, border: `1px solid ${GRAY_200}`,
                  borderLeft: "none", borderRight: "none",
                  minHeight: isFatFloor ? 90 : 64,
                  position: "relative",
                }}>
                  {Array.from({ length: logements }).map((_, l) => {
                    const isLastUnit = l === logements - 1;
                    const isFatBetween = isFatFloor && l === fatCol;
                    return (
                      <div key={l} style={{ display: "flex", flex: 1, alignItems: "stretch" }}>
                        <div style={{
                          flex: 1, display: "flex", flexDirection: "column",
                          alignItems: "center", justifyContent: "center",
                          padding: "8px 4px", gap: 4,
                          background: "white",
                          border: `1px solid ${GRAY_200}`,
                          borderRadius: 4, margin: "4px 2px",
                          minWidth: 56,
                        }}>
                          <div style={{ fontSize: 14 }}>🚪</div>
                          <div style={{ fontSize: 9, fontWeight: 700, color: GRAY_600, fontFamily: "monospace" }}>L.{l + 1}</div>
                          <div style={{ width: 6, height: 6, borderRadius: "50%", background: AT_BLUE, boxShadow: `0 0 4px ${AT_BLUE}55` }} />
                        </div>
                        {!isLastUnit && (
                          <div style={{
                            width: isFatBetween ? 100 : 12,
                            display: "flex", flexDirection: "column",
                            alignItems: "center", justifyContent: "center",
                            background: isFatBetween ? AT_ORANGE_LIGHT : GRAY_100,
                            borderLeft: `1px dashed ${GRAY_200}`,
                            borderRight: `1px dashed ${GRAY_200}`,
                            position: "relative",
                            flexShrink: 0,
                          }}>
                            {isFatBetween && fatId !== null && <FATNode id={fatId} />}
                            {!isFatBetween && <div style={{ width: 1, height: "100%", background: GRAY_300, opacity: 0.5 }} />}
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {e > 0 && (
                    <div style={{
                      position: "absolute", left: "50%", top: -12,
                      transform: "translateX(-50%)",
                      width: 0, height: 12,
                      borderLeft: `2px dashed ${AT_ORANGE}88`,
                    }} />
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

      <div style={{ display: "flex", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
        {[
          { color: AT_BLUE, label: "Mur porteur" },
          { color: AT_ORANGE, label: "Point FAT (couloir)" },
          { color: AT_BLUE, dot: true, label: "Prise fibre logement" },
          { color: `${AT_ORANGE}88`, dashed: true, label: "Liaison fibre verticale" },
        ].map((l, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: GRAY_600 }}>
            {l.dot ? <div style={{ width: 8, height: 8, borderRadius: "50%", background: l.color }} /> :
             l.dashed ? <div style={{ width: 16, height: 0, borderTop: `2px dashed ${l.color}` }} /> :
             <div style={{ width: 12, height: 8, background: l.color, borderRadius: 2 }} />}
            {l.label}
          </div>
        ))}
      </div>
    </div>
  );
};

// ── Leaflet Map component ────────────────────────────────────
const LeafletMap = ({ residence }) => {
  const mapRef = useRef(null);
  const mapInstanceRef = useRef(null);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;

    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css";
    document.head.appendChild(link);

    const script = document.createElement("script");
    script.src = "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js";
    script.onload = () => {
      const L = window.L;
      const map = L.map(mapRef.current, { zoomControl: true, scrollWheelZoom: false });
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '© OpenStreetMap',
        maxZoom: 19,
      }).addTo(map);
      map.setView([35.697, -0.634], 15);

      const buildings = [
        { name: "Rés. Les Falaises", lat: 35.6980, lng: -0.6330, type: "selected" },
        { name: "El Bahia Tower", lat: 35.6955, lng: -0.6350, type: "neighbor" },
        { name: "Rés. Atlas", lat: 35.6998, lng: -0.6310, type: "neighbor" },
        { name: "Cité AADL B4", lat: 35.6965, lng: -0.6375, type: "neighbor" },
        { name: "Cité AADL B5", lat: 35.6942, lng: -0.6295, type: "neighbor" },
      ];

      buildings.forEach(b => {
        const color = b.type === "selected" ? AT_BLUE : "#666";
        const icon = L.divIcon({
          html: `<div style="background:${color};color:white;border:2px solid white;border-radius:6px;padding:3px 6px;font-size:9px;font-weight:700;white-space:nowrap;box-shadow:0 2px 6px rgba(0,0,0,0.25);font-family:sans-serif;">🏢 ${b.name}</div>`,
          className: "",
          iconAnchor: [0, 0],
        });
        L.marker([b.lat, b.lng], { icon }).addTo(map)
          .bindPopup(`<b>${b.name}</b><br>Résidence AADL<br><small>Oran · Seddikia</small>`);
      });

      mapInstanceRef.current = map;
    };
    document.head.appendChild(script);
    return () => {};
  }, []);

  return (
    <div style={{ height: "100%", minHeight: 280, borderRadius: 10, overflow: "hidden" }}>
      <div ref={mapRef} style={{ width: "100%", height: "100%", minHeight: 280 }} />
    </div>
  );
};

// ══════════════════════════════════════════════════════════════
//  MAIN APP — FULL SCREEN VERSION
// ══════════════════════════════════════════════════════════════
export default function FTTHSmartPlanner() {
  const [screen, setScreen] = useState("login");
  const [activeTab, setActiveTab] = useState("planner");
  const [notif, setNotif] = useState(null);
  const [showPassword, setShowPassword] = useState(false);
  const [loginData, setLoginData] = useState({ id: "k.benali@at.dz", password: "atdz2026" });
  const [loginLoading, setLoginLoading] = useState(false);

  const [ville, setVille] = useState("oran");
  const [quartier, setQuartier] = useState("seddikia");
  const [residence, setResidence] = useState("falaises");
  const [etages, setEtages] = useState(5);
  const [logements, setLogements] = useState(4);
  const [fatCap, setFatCap] = useState(16);
  const [osmLoaded, setOsmLoaded] = useState(false);
  const [osmLoading, setOsmLoading] = useState(false);
  const [planGenerated, setPlanGenerated] = useState(false);
  const [kpis, setKpis] = useState(null);
  const [archiveSearch, setArchiveSearch] = useState("");

  const notify = useCallback((type, title, sub) => setNotif({ type, title, sub }), []);

  const login = () => {
    setLoginLoading(true);
    setTimeout(() => {
      setLoginLoading(false);
      setScreen("dashboard");
      notify("success", "Connexion réussie", "Bienvenue, Khaled B. — Ingénieur Réseau");
    }, 1200);
  };

  const importOSM = () => {
    if (!ville || !quartier || !residence) { notify("error", "Données manquantes", "Sélectionnez ville, quartier et résidence"); return; }
    setOsmLoading(true);
    setTimeout(() => {
      setOsmLoading(false);
      setOsmLoaded(true);
      notify("success", "OSM Synchronisé", "Données importées pour la résidence sélectionnée");
    }, 1600);
  };

  const lancerSectorisation = () => {
    if (!osmLoaded) { notify("info", "Import requis", "Importez d'abord les données OSM"); return; }
    const totalAbonnes = etages * logements;
    const fatsNeeded = Math.ceil(totalAbonnes / 10);
    const fatsPortsUsed = Math.round((totalAbonnes / (fatsNeeded * fatCap)) * 100);
    const lineaire = Math.round(etages * 3.2 * logements * 1.8);
    setKpis({ totalAbonnes, fatsNeeded, fatsPortsUsed, lineaire });
    setPlanGenerated(true);
    notify("success", "Sectorisation terminée !", `${fatsNeeded} FAT(s) proposée(s) pour ${totalAbonnes} abonnés`);
  };

  const residenceLabel = {
    falaises: "Rés. Les Falaises", bahia: "El Bahia Tower",
    yasmine: "Rés. Yasmine", atlas: "Rés. Atlas",
  }[residence] || "";

  const filteredProjects = PROJECTS_ARCHIVE.filter(p =>
    p.name.toLowerCase().includes(archiveSearch.toLowerCase()) ||
    p.ville.toLowerCase().includes(archiveSearch.toLowerCase())
  );

  const inputStyle = {
    width: "100%", padding: "10px 14px",
    background: "white", border: `1.5px solid ${GRAY_200}`,
    borderRadius: 8, color: GRAY_800,
    fontSize: 13, fontFamily: "inherit", outline: "none",
    transition: "border-color 0.2s, box-shadow 0.2s",
  };
  const labelStyle = { fontSize: 11, fontWeight: 700, color: GRAY_600, marginBottom: 5, display: "block", letterSpacing: "0.5px", textTransform: "uppercase" };
  const cardStyle = {
    background: "white", borderRadius: 12,
    border: `1px solid ${GRAY_200}`, padding: 20,
    boxShadow: "0 1px 4px rgba(0,0,0,0.05)",
  };
  const btnPrimary = {
    background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`,
    color: "white", border: "none", borderRadius: 8,
    padding: "11px 20px", fontSize: 13, fontWeight: 700,
    cursor: "pointer", display: "flex", alignItems: "center",
    justifyContent: "center", gap: 8, width: "100%",
    boxShadow: `0 4px 12px ${AT_BLUE}44`,
    transition: "all 0.2s",
  };

  // Global full-screen reset
  const globalStyle = `
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    @keyframes slideUp { from { opacity:0;transform:translateY(20px) } to { opacity:1;transform:translateY(0) } }
    @keyframes shimmer { 0%,100%{opacity:0.6} 50%{opacity:1} }
    @keyframes spin { to { transform: rotate(360deg); } }
    html, body, #root {
      margin: 0 !important;
      padding: 0 !important;
      width: 100vw !important;
      height: 100vh !important;
      overflow: hidden !important;
    }
    * { box-sizing: border-box; }
  `;

  // ─────────────────────────────────────────────────────────────
  // LOGIN SCREEN — FULL SCREEN
  // ─────────────────────────────────────────────────────────────
  if (screen === "login") return (
    <div style={{
      width: "100vw",
      height: "100vh",
      background: `linear-gradient(135deg, ${AT_BLUE_LIGHT} 0%, white 50%, ${AT_ORANGE_LIGHT} 100%)`,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontFamily: "'Outfit', 'Segoe UI', sans-serif",
      position: "fixed",
      top: 0,
      left: 0,
      overflow: "hidden",
    }}>
      <style>{globalStyle}</style>

      <div style={{ position: "fixed", top: -120, left: -120, width: 400, height: 400, borderRadius: "50%", background: `${AT_BLUE}08` }} />
      <div style={{ position: "fixed", bottom: -80, right: -80, width: 320, height: 320, borderRadius: "50%", background: `${AT_ORANGE}08` }} />

      <div style={{
        background: "white", borderRadius: 20, padding: 48, width: 440,
        boxShadow: "0 24px 80px rgba(0,91,170,0.15)",
        border: `1px solid ${GRAY_100}`,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 36 }}>
          <ATLogo size={52} />
          <div>
            <div style={{ fontSize: 18, fontWeight: 800, color: GRAY_800, letterSpacing: "-0.3px" }}>
              FTTH <span style={{ color: AT_BLUE }}>SMART</span> PLANNER
            </div>
            <div style={{ fontSize: 10, color: GRAY_400, letterSpacing: "1px", textTransform: "uppercase", marginTop: 2 }}>
              Algérie Télécom · v3.2
            </div>
          </div>
        </div>

        <div style={{ fontSize: 24, fontWeight: 800, color: GRAY_800, marginBottom: 6, letterSpacing: "-0.5px" }}>Connexion Sécurisée</div>
        <div style={{ fontSize: 13, color: GRAY_400, marginBottom: 32 }}>Accès réservé aux ingénieurs réseau autorisés</div>

        <div style={{ marginBottom: 18 }}>
          <label style={labelStyle}>Identifiant Agent</label>
          <input style={inputStyle} type="text" value={loginData.id}
            onChange={e => setLoginData(p => ({ ...p, id: e.target.value }))}
            placeholder="ex: k.benali@at.dz" />
        </div>

        <div style={{ marginBottom: 24 }}>
          <label style={labelStyle}>Mot de passe</label>
          <div style={{ position: "relative" }}>
            <input style={{ ...inputStyle, paddingRight: 44 }}
              type={showPassword ? "text" : "password"}
              value={loginData.password}
              onChange={e => setLoginData(p => ({ ...p, password: e.target.value }))}
              placeholder="••••••••••" />
            <button onClick={() => setShowPassword(p => !p)} style={{
              position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)",
              background: "none", border: "none", cursor: "pointer", color: GRAY_400,
              fontSize: 16, padding: 4,
            }}>
              {showPassword ? "🙈" : "👁"}
            </button>
          </div>
        </div>

        <button style={btnPrimary} onClick={login} disabled={loginLoading}>
          {loginLoading ? (
            <div style={{ width: 18, height: 18, border: "2px solid rgba(255,255,255,0.3)", borderTopColor: "white", borderRadius: "50%", animation: "spin 0.7s linear infinite" }} />
          ) : "Accéder au Planner →"}
        </button>

        <div style={{ textAlign: "center", marginTop: 20 }}>
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 6,
            background: GREEN_LIGHT, border: `1px solid ${GREEN}44`,
            borderRadius: 20, padding: "4px 12px",
            fontSize: 10, fontWeight: 700, color: GREEN,
          }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: GREEN }} />
            SERVEUR EN LIGNE · TLS 1.3
          </span>
        </div>
      </div>

      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );

  // ─────────────────────────────────────────────────────────────
  // ARCHIVE SCREEN — FULL SCREEN
  // ─────────────────────────────────────────────────────────────
  if (screen === "archive") return (
    <div style={{
      width: "100vw",
      height: "100vh",
      background: GRAY_50,
      fontFamily: "'Outfit','Segoe UI',sans-serif",
      overflow: "hidden",
      position: "fixed",
      top: 0,
      left: 0,
    }}>
      <style>{globalStyle}</style>

      <nav style={{
        height: 60, background: "white", borderBottom: `1px solid ${GRAY_200}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 24px", position: "sticky", top: 0, zIndex: 100,
        boxShadow: "0 1px 4px rgba(0,0,0,0.06)",
      }}>
        <button onClick={() => setScreen("dashboard")} style={{
          display: "flex", alignItems: "center", gap: 10,
          background: "none", border: "none", cursor: "pointer",
          fontSize: 14, fontWeight: 600, color: AT_BLUE,
        }}>
          <ATLogo size={32} /> ← Retour au Planner
        </button>
        <div style={{ fontSize: 16, fontWeight: 700, color: GRAY_800 }}>📁 Archivage des Projets</div>
        <div style={{ fontSize: 12, color: GRAY_400 }}>Khaled B. — Ingénieur Réseau</div>
      </nav>

      <div style={{ height: "calc(100vh - 60px)", padding: "32px 24px", overflow: "auto", maxWidth: 900, margin: "0 auto" }}>
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontSize: 26, fontWeight: 800, color: GRAY_800, letterSpacing: "-0.5px" }}>Tous les projets</div>
          <div style={{ fontSize: 13, color: GRAY_400, marginTop: 4 }}>{PROJECTS_ARCHIVE.length} projets · triés par date décroissante</div>
        </div>

        <div style={{ position: "relative", marginBottom: 24 }}>
          <span style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", color: GRAY_400, fontSize: 16 }}>🔍</span>
          <input style={{ ...inputStyle, paddingLeft: 40, borderRadius: 10 }}
            placeholder="Rechercher un projet, une ville..."
            value={archiveSearch}
            onChange={e => setArchiveSearch(e.target.value)} />
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {filteredProjects.map((p, i) => (
            <div key={p.id} style={{
              ...cardStyle,
              display: "flex", alignItems: "center", gap: 16,
              cursor: "pointer", transition: "box-shadow 0.2s, transform 0.15s",
            }}
              onMouseEnter={e => { e.currentTarget.style.boxShadow = `0 4px 20px ${AT_BLUE}22`; e.currentTarget.style.transform = "translateY(-1px)"; }}
              onMouseLeave={e => { e.currentTarget.style.boxShadow = "0 1px 4px rgba(0,0,0,0.05)"; e.currentTarget.style.transform = "none"; }}>
              <div style={{ width: 48, height: 48, borderRadius: 10, background: AT_BLUE_LIGHT, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 22, flexShrink: 0 }}>🏢</div>
              <div style={{ flex: 1 }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800 }}>{p.name}</div>
                <div style={{ fontSize: 11, color: GRAY_400, marginTop: 3 }}>{p.ville} · {p.quartier} · {p.etages} étages · {p.logements} log/ét.</div>
              </div>
              <div style={{ background: AT_BLUE_LIGHT, color: AT_BLUE, borderRadius: 8, padding: "6px 14px", fontSize: 12, fontWeight: 700, fontFamily: "monospace", flexShrink: 0 }}>📅 {p.date}</div>
              <button onClick={() => {
                setResidence("falaises");
                setOsmLoaded(true);
                setPlanGenerated(false);
                setScreen("dashboard");
                notify("info", "Projet chargé", `${p.name} · ${p.ville}`);
              }} style={{ background: AT_BLUE, color: "white", border: "none", borderRadius: 8, padding: "8px 16px", fontSize: 12, fontWeight: 700, cursor: "pointer", flexShrink: 0 }}>Ouvrir →</button>
            </div>
          ))}
        </div>
      </div>

      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );

  // ─────────────────────────────────────────────────────────────
  // DASHBOARD SCREEN — FULL SCREEN (100vw × 100vh)
  // ─────────────────────────────────────────────────────────────
  return (
    <div style={{
      width: "100vw",
      height: "100vh",
      background: GRAY_50,
      fontFamily: "'Outfit','Segoe UI',sans-serif",
      display: "flex",
      flexDirection: "column",
      overflow: "hidden",
      position: "fixed",
      top: 0,
      left: 0,
    }}>
      <style>{globalStyle}</style>

      {/* TOP NAV */}
      <nav style={{
        height: 60, background: "white", borderBottom: `1px solid ${GRAY_200}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 20px", zIndex: 100, boxShadow: "0 1px 6px rgba(0,0,0,0.06)",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <ATLogo size={34} />
          <div style={{ fontSize: 14, fontWeight: 800, color: GRAY_800, letterSpacing: "-0.3px" }}>
            FTTH <span style={{ color: AT_BLUE }}>SMART</span> PLANNER
          </div>
        </div>

        <div style={{ display: "flex", gap: 4, background: GRAY_100, borderRadius: 8, padding: 4 }}>
          {[["planner", "🗺 Planificateur"], ["results", "📊 Résultats FAT"], ["settings", "⚙ Capacités"]].map(([id, label]) => (
            <button key={id} onClick={() => setActiveTab(id)} style={{
              padding: "6px 16px", borderRadius: 6, border: "none",
              fontSize: 12, fontWeight: 600, cursor: "pointer",
              background: activeTab === id ? "white" : "transparent",
              color: activeTab === id ? AT_BLUE : GRAY_600,
              boxShadow: activeTab === id ? "0 1px 4px rgba(0,0,0,0.08)" : "none",
            }}>{label}</button>
          ))}
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            display: "inline-flex", alignItems: "center", gap: 6,
            background: PURPLE_LIGHT, border: `1px solid ${PURPLE}44`,
            borderRadius: 20, padding: "4px 12px",
            fontSize: 10, fontWeight: 700, color: PURPLE,
          }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: PURPLE, animation: "shimmer 1.5s infinite" }} />
            IA Active · K-Means
          </div>

          <button onClick={() => setScreen("archive")} style={{
            display: "flex", alignItems: "center", gap: 8,
            background: "none", border: `1.5px solid ${GRAY_200}`,
            borderRadius: 8, padding: "6px 12px", cursor: "pointer",
          }}
            onMouseEnter={e => { e.currentTarget.style.background = AT_BLUE_LIGHT; e.currentTarget.style.borderColor = AT_BLUE; }}
            onMouseLeave={e => { e.currentTarget.style.background = "none"; e.currentTarget.style.borderColor = GRAY_200; }}>
            <div style={{
              width: 28, height: 28, borderRadius: "50%",
              background: `linear-gradient(135deg, ${AT_BLUE}, ${AT_ORANGE})`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 11, fontWeight: 800, color: "white",
            }}>KB</div>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: GRAY_800 }}>Khaled B.</div>
              <div style={{ fontSize: 10, color: GRAY_400 }}>Mes Projets</div>
            </div>
          </button>
        </div>
      </nav>

      {/* STATUS BAR */}
      <div style={{
        background: "white", borderBottom: `1px solid ${GRAY_100}`,
        display: "flex", alignItems: "center", gap: 16,
        padding: "5px 20px", fontSize: 11, color: GRAY_400,
      }}>
        <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span style={{ width: 6, height: 6, borderRadius: "50%", background: GREEN, boxShadow: `0 0 5px ${GREEN}` }} />
          API OSM connectée
        </span>
        <span>·</span>
        <span>Wilayas chargées : 48</span>
        <span>·</span>
        <span style={{ fontFamily: "monospace" }}>Sync: 27/03/2026 · 11:42</span>
        <span style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 6, color: PURPLE, fontWeight: 700 }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: PURPLE, animation: "shimmer 1.5s infinite" }} />
          Algorithme: K-Means clustering
        </span>
      </div>

      {/* KPI ROW */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, padding: "16px 20px", background: GRAY_50, borderBottom: `1px solid ${GRAY_200}` }}>
        <KPICard label="Abonnés Estimés" value={kpis?.totalAbonnes ?? null} sub={kpis ? `${etages} étages × ${logements} log/ét.` : "—"} color="blue" icon="👥" />
        <KPICard label="FATs Proposées" value={kpis?.fatsNeeded ?? null} sub={kpis ? "16 ports/FAT · AT standard" : "—"} color="orange" icon="📡" />
        <KPICard label="Linéaire Fibre" value={kpis?.lineaire ?? null} suffix="m" sub="Câble fibre estimé" color="purple" icon="🔌" />
        <KPICard label="Ports Utilisés" value={kpis?.fatsPortsUsed ?? null} suffix="%" sub="Taux d'utilisation FAT" color="green" icon="📶" />
      </div>

      {/* MAIN FLEX LAYOUT — FULL REMAINING HEIGHT */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>

        {/* SIDEBAR */}
        <aside style={{
          width: 300,
          background: "white",
          borderRight: `1px solid ${GRAY_200}`,
          padding: 20,
          display: "flex",
          flexDirection: "column",
          gap: 16,
          overflowY: "auto",
        }}>

          {/* Localisation */}
          <div style={cardStyle}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
              <span style={{ width: 28, height: 28, background: AT_BLUE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>📍</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Localisation</span>
            </div>
            {[
              { label: "Ville / Wilaya", val: ville, set: setVille, opts: [["oran","Oran"],["alger","Alger"],["constantine","Constantine"],["annaba","Annaba"]] },
              { label: "Quartier", val: quartier, set: setQuartier, opts: [["seddikia","Seddikia"],["haî-nedjma","Haï Nedjma"],["carteaux","Carteaux"],["victor-hugo","Victor Hugo"]] },
              { label: "Résidence", val: residence, set: setResidence, opts: [["falaises","Rés. Les Falaises"],["bahia","El Bahia Tower"],["yasmine","Rés. Yasmine"],["atlas","Rés. Atlas"]] },
            ].map(f => (
              <div key={f.label} style={{ marginBottom: 10 }}>
                <label style={labelStyle}>{f.label}</label>
                <select style={{ ...inputStyle, appearance: "none", cursor: "pointer" }} value={f.val} onChange={e => f.set(e.target.value)}>
                  <option value="">Sélectionner...</option>
                  {f.opts.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
                </select>
              </div>
            ))}
            <button style={{ ...btnPrimary, background: osmLoaded ? `linear-gradient(135deg,${GREEN},#059669)` : btnPrimary.background }} onClick={importOSM}>
              {osmLoading ? (
                <div style={{ width: 16, height: 16, border: "2px solid rgba(255,255,255,0.3)", borderTopColor: "white", borderRadius: "50%", animation: "spin 0.7s linear infinite" }} />
              ) : osmLoaded ? "✓ OSM Synchronisé" : "🔍 Import Data OSM"}
            </button>
            {osmLoaded && (
              <div style={{ marginTop: 10, background: GREEN_LIGHT, border: `1px solid ${GREEN}44`, borderRadius: 7, padding: "8px 12px" }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: GREEN, letterSpacing: "1px", textTransform: "uppercase" }}>✓ Données OSM chargées</div>
                <div style={{ fontSize: 12, fontWeight: 600, color: GRAY_800, marginTop: 2 }}>{residenceLabel}</div>
              </div>
            )}
          </div>

          {/* Structure Bâtiment */}
          <div style={{ ...cardStyle, opacity: osmLoaded ? 1 : 0.4, pointerEvents: osmLoaded ? "auto" : "none", transition: "opacity 0.3s" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
              <span style={{ width: 28, height: 28, background: GREEN_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>🏗</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Structure Bâtiment</span>
            </div>
            {[
              { label: "Nombre d'Étages", val: etages, set: setEtages, min: 1, max: 30 },
              { label: "Logements / Étage", val: logements, set: setLogements, min: 2, max: 20 },
            ].map(f => (
              <div key={f.label} style={{ marginBottom: 10 }}>
                <label style={labelStyle}>{f.label}</label>
                <input type="number" style={inputStyle} value={f.val} min={f.min} max={f.max} onChange={e => f.set(parseInt(e.target.value) || f.min)} />
              </div>
            ))}
            <div style={{ marginBottom: 14 }}>
              <label style={labelStyle}>Capacité FAT (ports)</label>
              <select style={{ ...inputStyle, appearance: "none", cursor: "pointer" }} value={fatCap} onChange={e => setFatCap(parseInt(e.target.value))}>
                <option value={8}>8 ports</option>
                <option value={16}>16 ports</option>
                <option value={32}>32 ports</option>
              </select>
            </div>
            <button style={{ ...btnPrimary, background: `linear-gradient(135deg,${AT_ORANGE},#d97706)` }} onClick={lancerSectorisation}>
              ▶ Lancer Sectorisation
            </button>
          </div>

          {/* Export */}
          <div style={{ ...cardStyle, opacity: planGenerated ? 1 : 0.4, pointerEvents: planGenerated ? "auto" : "none", transition: "opacity 0.3s" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
              <span style={{ width: 28, height: 28, background: AT_BLUE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>📤</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Exporter le Projet</span>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 8 }}>
              {[["PDF","📄"],["Excel","📊"],["JSON","{ }"],["KMZ","🗺"]].map(([fmt, ico]) => (
                <button key={fmt} onClick={() => notify("success", `Export ${fmt} lancé`, "Génération en cours...")}
                  style={{ padding: "8px 4px", background: GRAY_50, border: `1px solid ${GRAY_200}`, borderRadius: 8, cursor: "pointer", fontSize: 11, fontWeight: 700, color: GRAY_700, display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}>
                  {ico} {fmt}
                </button>
              ))}
            </div>
            <button style={btnPrimary} onClick={() => notify("success", "Export PDF complet", "Téléchargement démarré")}>
              Exporter le projet →
            </button>
          </div>

          {/* Projets Récents */}
          <div style={cardStyle}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
              <span style={{ width: 28, height: 28, background: PURPLE_LIGHT, borderRadius: 7, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>🗂</span>
              <span style={{ fontWeight: 700, fontSize: 13, color: GRAY_800 }}>Projets Récents</span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
              {PROJECTS_ARCHIVE.slice(0, 3).map(p => (
                <div key={p.id} onClick={() => { setOsmLoaded(true); setPlanGenerated(false); notify("info", "Projet chargé", `${p.name} · ${p.ville}`); }}
                  style={{ padding: "9px 12px", background: GRAY_50, borderRadius: 8, cursor: "pointer", border: `1px solid ${GRAY_100}`, transition: "background 0.15s" }}
                  onMouseEnter={e => e.currentTarget.style.background = AT_BLUE_LIGHT}
                  onMouseLeave={e => e.currentTarget.style.background = GRAY_50}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: GRAY_800 }}>{p.name}</div>
                  <div style={{ fontSize: 10, color: GRAY_400, marginTop: 2, fontFamily: "monospace" }}>{p.ville} · {p.quartier} · {p.date}</div>
                </div>
              ))}
            </div>
          </div>
        </aside>

        {/* MAIN CONTENT AREA */}
        <div style={{ flex: 1, padding: 20, overflow: "auto", background: GRAY_50 }}>

          {/* PLANNER TAB */}
          {activeTab === "planner" && (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, flex: 1 }}>
              <div style={{ ...cardStyle, gridColumn: "1 / 2" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, display: "flex", alignItems: "center", gap: 8 }}>
                    Plan de Sectorisation
                    {planGenerated && <span style={{ background: AT_BLUE_LIGHT, color: AT_BLUE, fontSize: 10, fontWeight: 700, padding: "2px 8px", borderRadius: 20 }}>{residenceLabel}</span>}
                  </div>
                  {planGenerated && (
                    <button onClick={() => { setPlanGenerated(false); setKpis(null); }} style={{ background: GRAY_100, border: "none", borderRadius: 6, padding: "5px 10px", fontSize: 11, cursor: "pointer", color: GRAY_600 }}>↺ Reset</button>
                  )}
                </div>
                {!planGenerated ? (
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", minHeight: 340, gap: 12, color: GRAY_400 }}>
                    <div style={{ fontSize: 56 }}>🏢</div>
                    <div style={{ fontWeight: 600, fontSize: 14, color: GRAY_600 }}>Veuillez importer les données de la résidence</div>
                    <div style={{ fontSize: 12 }}>Sélectionnez une ville, un quartier, puis lancez la sectorisation</div>
                  </div>
                ) : (
                  <BuildingPlan etages={etages} logements={logements} residence={residenceLabel} />
                )}
              </div>

              <div style={{ ...cardStyle, display: "flex", flexDirection: "column" }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: GRAY_800, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
                  🗺 Carte OpenStreetMap — Bâtiments de la zone
                  <span style={{ background: AT_ORANGE_LIGHT, color: AT_ORANGE, fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 20, textTransform: "uppercase" }}>AADL Cluster</span>
                </div>
                <div style={{ flex: 1, minHeight: 400, borderRadius: 10, overflow: "hidden", border: `1px solid ${GRAY_200}` }}>
                  <LeafletMap residence={residenceLabel} />
                </div>
                <div style={{ fontSize: 10, color: GRAY_400, marginTop: 8 }}>
                  © OpenStreetMap contributors · Zoom avec la molette désactivé
                </div>
              </div>
            </div>
          )}

          {/* RESULTS TAB */}
          {activeTab === "results" && (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gridColumn: "1 / -1", marginBottom: 4 }}>
                <div>
                  <div style={{ fontSize: 20, fontWeight: 800, color: GRAY_800 }}>Résultats IA — Sectorisation FAT</div>
                  <div style={{ fontSize: 12, color: GRAY_400, marginTop: 2 }}>Algorithme K-Means · {residenceLabel} · Oran/Seddikia</div>
                </div>
                <span style={{ display: "inline-flex", alignItems: "center", gap: 6, background: PURPLE_LIGHT, border: `1px solid ${PURPLE}33`, borderRadius: 20, padding: "6px 14px", fontSize: 11, fontWeight: 700, color: PURPLE }}>
                  <span style={{ width: 5, height: 5, borderRadius: "50%", background: PURPLE, animation: "shimmer 1.5s infinite" }} />
                  Analyse complète · Confiance 94%
                </span>
              </div>

              <div style={cardStyle}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                  <span style={{ fontSize: 18 }}>📊</span>
                  <span style={{ fontWeight: 700, fontSize: 13 }}>Récapitulatif IA</span>
                </div>
                {[
                  ["Algorithme", "K-Means Clustering", AT_BLUE],
                  ["Total Abonnés", kpis?.totalAbonnes ?? "—", AT_BLUE],
                  ["FATs Nécessaires", kpis?.fatsNeeded ?? "—", AT_ORANGE],
                  ["Capacité FAT", `${fatCap} ports`, GRAY_700],
                  ["Taux d'utilisation", kpis ? `${kpis.fatsPortsUsed}%` : "—", GREEN],
                  ["Linéaire estimé", kpis ? `${kpis.lineaire}m` : "—", GRAY_700],
                ].map(([k, v, c]) => (
                  <div key={k} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "9px 0", borderBottom: `1px solid ${GRAY_100}` }}>
                    <span style={{ fontSize: 13, color: GRAY_600 }}>{k}</span>
                    <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: c }}>{v}</span>
                  </div>
                ))}
              </div>

              <div style={cardStyle}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                  <span style={{ fontSize: 18 }}>📡</span>
                  <span style={{ fontWeight: 700, fontSize: 13 }}>Points FAT Proposés</span>
                </div>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr>
                      {["ID FAT","Étage","Position","Abonnés","Statut"].map(h => (
                        <th key={h} style={{ textAlign: "left", padding: "8px 10px", fontSize: 10, fontWeight: 700, letterSpacing: "0.8px", textTransform: "uppercase", color: GRAY_400, borderBottom: `1px solid ${GRAY_200}` }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {kpis && Array.from({ length: kpis.fatsNeeded }).map((_, i) => (
                      <tr key={i} style={{ background: i % 2 === 0 ? "white" : GRAY_50 }}>
                        <td style={{ padding: "9px 10px", fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: AT_ORANGE }}>FAT-ORA-{100 + i}</td>
                        <td style={{ padding: "9px 10px", fontSize: 12 }}>Ét.{Math.round((i + 1) * etages / (kpis.fatsNeeded + 1))}</td>
                        <td style={{ padding: "9px 10px", fontSize: 12 }}>Couloir L.{Math.floor(logements / 2)}-{Math.floor(logements / 2) + 1}</td>
                        <td style={{ padding: "9px 10px", fontSize: 12 }}>{Math.round(kpis.totalAbonnes / kpis.fatsNeeded)}</td>
                        <td style={{ padding: "9px 10px" }}><span style={{ background: GREEN_LIGHT, color: GREEN, fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 20 }}>✓ OPTIMAL</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div style={{ ...cardStyle, gridColumn: "1 / -1" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
                  <span style={{ fontSize: 18 }}>📈</span>
                  <span style={{ fontWeight: 700, fontSize: 13 }}>Utilisation des Ports par FAT</span>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                  {kpis && Array.from({ length: kpis.fatsNeeded }).map((_, i) => {
                    const pct = Math.round((kpis.totalAbonnes / kpis.fatsNeeded) / fatCap * 100);
                    return (
                      <div key={i}>
                        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 7 }}>
                          <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "monospace", color: AT_ORANGE }}>FAT-ORA-{100 + i}</span>
                          <span style={{ fontSize: 11, color: GRAY_400 }}>{Math.round(kpis.totalAbonnes / kpis.fatsNeeded)}/{fatCap} ports · Ét.{Math.round((i + 1) * etages / (kpis.fatsNeeded + 1))}</span>
                        </div>
                        <div style={{ height: 10, background: GRAY_200, borderRadius: 5, overflow: "hidden" }}>
                          <div style={{ height: "100%", width: `${pct}%`, background: `linear-gradient(90deg, ${AT_BLUE}, ${GREEN})`, borderRadius: 5 }} />
                        </div>
                        <div style={{ fontSize: 10, color: GRAY_400, marginTop: 3 }}>{pct}% d'utilisation</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {/* SETTINGS TAB */}
          {activeTab === "settings" && (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div style={{ fontSize: 20, fontWeight: 800, color: GRAY_800, gridColumn: "1/-1" }}>⚙ Paramètres & Capacités</div>
              {[
                { title: "Paramètres FAT", icon: AT_ORANGE, rows: [["Capacité standard", "16 ports"],["Taux max utilisation", "80%"],["Distance max", "150m"],["Redondance", "Activée ✓"]] },
                { title: "Algorithme IA", icon: PURPLE, rows: [["Méthode clustering", "K-Means"],["Itérations max", "300"],["Tolérance convergence", "0.0001"],["Seed aléatoire", "42"]] },
                { title: "🗺 OSM & Cartographie", icon: AT_BLUE, rows: [["API OSM", "Nominatim v1"],["Format export carte", "GeoJSON"],["Système coord.", "WGS84"],["Mise à jour auto", "Activée ✓"]] },
                { title: "Export", icon: GREEN, rows: [["Format PDF", "A3 paysage"],["Format Excel", "XLSX 2007+"],["Format cartographique", "KMZ/KML"],["Compression JSON", "Gzip"]] },
              ].map(s => (
                <div key={s.title} style={cardStyle}>
                  <div style={{ fontWeight: 700, fontSize: 13, color: GRAY_800, marginBottom: 14, borderLeft: `3px solid ${s.icon}`, paddingLeft: 10 }}>{s.title}</div>
                  {s.rows.map(([k, v]) => (
                    <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "9px 0", borderBottom: `1px solid ${GRAY_100}` }}>
                      <span style={{ fontSize: 13, color: GRAY_600 }}>{k}</span>
                      <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: GRAY_800 }}>{v}</span>
                    </div>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );
}