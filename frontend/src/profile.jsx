import React, { useState, useEffect } from "react";
import ATLogoImg from "./assets/algerie-telecom-logo-png_seeklogo-210074.png";
import { listProjects } from "./projectService";

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

const Profile = ({ onBack, onLoadProject, onLogout }) => {
    const [search, setSearch] = useState("");
    const [hoveredRowId, setHoveredRowId] = useState(null);
    const [projects, setProjects] = useState([]);
    const [loading, setLoading] = useState(true);
    const [currentPage, setCurrentPage] = useState(1);
    const [showSettings, setShowSettings] = useState(false);
    const [showSettingsModal, setShowSettingsModal] = useState(false);
    const [config, setConfig] = useState({ fat_capacity: 8, cable_standards: [] });
    const [saveLoading, setSaveLoading] = useState(false);
    const [toast, setToast] = useState({ show: false, message: "", type: "success" });
    const PROJECTS_PER_PAGE = 10;

    const fetchConfig = async () => {
        console.log("📡 Récupération de la configuration...");
        try {
            const res = await fetch("http://127.0.0.1:8000/api/config");
            console.log("📥 Status réponse:", res.status);
            const data = await res.json();
            console.log("📦 Données reçues:", data);
            setConfig(data);
        } catch (err) {
            console.error("❌ Erreur lors de la récupération de la config:", err);
        }
    };

    const handleSaveConfig = async () => {
        setSaveLoading(true);
        try {
            const res = await fetch("http://127.0.0.1:8000/api/config", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(config)
            });
            if (res.ok) {
                setToast({ show: true, message: "Paramètres mis à jour avec succès !", type: "success" });
                setTimeout(() => {
                    setShowSettingsModal(false);
                    setShowSettings(false);
                    setToast({ show: false, message: "", type: "success" });
                }, 3000);
            }
        } catch (err) {
            console.error("Error saving config:", err);
            setToast({ show: true, message: "Erreur lors de la sauvegarde", type: "error" });
            setTimeout(() => setToast({ ...toast, show: false }), 3000);
        } finally {
            setSaveLoading(false);
        }
    };

    useEffect(() => {
        if (showSettingsModal) fetchConfig();
    }, [showSettingsModal]);

    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const userName = user.prenom && user.nom ? `${user.prenom} ${user.nom}` : (user.full_name || user.name || "Utilisateur");

    useEffect(() => {
        setCurrentPage(1);
    }, [search]);

    useEffect(() => {
        const fetchProjects = async () => {
            const user = JSON.parse(localStorage.getItem("user") || "{}");
            if (!user.id) return;
            try {
                const data = await listProjects(user.id);
                setProjects(data);
            } catch (err) {
                console.error("Erreur chargement projets:", err);
            } finally {
                setLoading(false);
            }
        };
        fetchProjects();
    }, []);

    const filteredProjects = projects.filter(p =>
        p.nom_projet.toLowerCase().includes(search.toLowerCase()) ||
        p.wilaya.toLowerCase().includes(search.toLowerCase()) ||
        p.commune.toLowerCase().includes(search.toLowerCase())
    );

    const totalPages = Math.ceil(filteredProjects.length / PROJECTS_PER_PAGE);
    const paginatedProjects = filteredProjects.slice(
        (currentPage - 1) * PROJECTS_PER_PAGE,
        currentPage * PROJECTS_PER_PAGE
    );

    const stats = {
        totalProjects: projects.length,
        coveredCities: new Set(projects.map(p => p.wilaya)).size,
        totalFats: projects.reduce((acc, p) => acc + (p.nb_fats || 0), 0)
    };

    return (
        <div style={{
            position: "fixed",
            top: 0,
            left: 0,
            width: "100vw",
            height: "100vh",
            backgroundColor: "#F8FAFC",
            fontFamily: "'Outfit', sans-serif",
            color: GRAY_800,
            overflowX: "hidden",
            overflowY: "auto",
            zIndex: 9999
        }}>
            <style>{`
                html, body, #root {
                    width: 100% !important;
                    height: 100% !important;
                    margin: 0 !important;
                    padding: 0 !important;
                    overflow: hidden !important;
                }
            `}</style>
            {/* Header */}
            <header style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "16px 40px",
                backgroundColor: "white",
                boxShadow: "0 1px 3px rgba(0,0,0,0.05)",
                position: "sticky",
                top: 0,
                zIndex: 100
            }}>
                <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
                    <img src={ATLogoImg} alt="Algerie Telecom" style={{ height: 40, width: "auto" }} />
                    <button
                        onClick={onBack}
                        style={{
                            display: "flex", alignItems: "center", gap: 8,
                            padding: "8px 16px", borderRadius: 8, border: `1px solid ${AT_BLUE_LIGHT}`,
                            backgroundColor: AT_BLUE_LIGHT, color: AT_BLUE, fontSize: 13, fontWeight: 600,
                            cursor: "pointer", transition: "all 0.2s"
                        }}
                    >
                        ← Retour au Planner
                    </button>
                </div>



                <div style={{ position: "relative" }}>
                    <button
                        onClick={() => setShowSettings(!showSettings)}
                        style={{
                            width: 38, height: 38, borderRadius: "50%", border: `1px solid ${GRAY_200}`,
                            backgroundColor: "white", color: GRAY_600, display: "flex", alignItems: "center",
                            justifyContent: "center", cursor: "pointer", transition: "all 0.2s"
                        }}
                    >
                        <i className="fas fa-cog" style={{ fontSize: 18 }}></i>
                    </button>

                    {showSettings && (
                        <div style={{
                            position: "absolute", top: "100%", right: 0, marginTop: 10, width: 220,
                            backgroundColor: "white", borderRadius: 12, boxShadow: "0 10px 25px rgba(0,0,0,0.1)",
                            border: `1px solid ${GRAY_200}`, padding: "8px", zIndex: 1000,
                        }}>
                            <div style={{ padding: "10px 12px", borderBottom: `1px solid ${GRAY_100}`, marginBottom: 4 }}>
                                <div style={{ fontSize: 13, fontWeight: 700, color: GRAY_800 }}>{userName}</div>
                                <div style={{ fontSize: 11, color: GRAY_400 }}>Session active</div>
                            </div>
                            <button
                                onClick={() => setShowSettingsModal(true)}
                                style={{
                                    width: "100%", padding: "10px 12px", display: "flex", alignItems: "center", gap: 10,
                                    border: "none", backgroundColor: "transparent", borderRadius: 8, cursor: "pointer",
                                    color: GRAY_700, fontSize: 13, fontWeight: 500, transition: "background 0.2s"
                                }}
                                onMouseEnter={e => e.currentTarget.style.backgroundColor = GRAY_50}
                                onMouseLeave={e => e.currentTarget.style.backgroundColor = "transparent"}
                            >
                                <i className="fas fa-user-edit" style={{ color: AT_BLUE }}></i> Paramètres de planification
                            </button>
                            <button
                                onClick={onLogout}
                                style={{
                                    width: "100%", padding: "10px 12px", display: "flex", alignItems: "center", gap: 10,
                                    border: "none", backgroundColor: "transparent", borderRadius: 8, cursor: "pointer",
                                    color: "#EF4444", fontSize: 13, fontWeight: 500, transition: "background 0.2s"
                                }}
                                onMouseEnter={e => e.currentTarget.style.backgroundColor = "#FEF2F2"}
                                onMouseLeave={e => e.currentTarget.style.backgroundColor = "transparent"}
                            >
                                <i className="fas fa-sign-out-alt"></i> Déconnecter
                            </button>
                        </div>
                    )}
                </div>
            </header>
            {/* Modal de Paramètres - Design Harmonisé AT */}
            {showSettingsModal && (
                <div style={{
                    position: "fixed", top: 0, left: 0, width: "100%", height: "100%",
                    backgroundColor: "rgba(31, 41, 55, 0.5)", display: "flex", alignItems: "center",
                    justifyContent: "center", zIndex: 2000, backdropFilter: "blur(5px)"
                }}>
                    <div style={{
                        width: 480, backgroundColor: "white", borderRadius: 24, padding: "35px",
                        color: GRAY_800, boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.15)", position: "relative",
                        border: `1px solid ${GRAY_200}`
                    }}>
                        <button
                            onClick={() => setShowSettingsModal(false)}
                            style={{
                                position: "absolute", top: 25, right: 25, background: GRAY_100,
                                border: "none", color: GRAY_500, width: 34, height: 34, borderRadius: "50%",
                                cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
                                transition: "all 0.2s"
                            }}
                            onMouseEnter={e => e.currentTarget.style.backgroundColor = GRAY_200}
                            onMouseLeave={e => e.currentTarget.style.backgroundColor = GRAY_100}
                        >
                            <i className="fas fa-times"></i>
                        </button>

                        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
                            <div style={{ width: 40, height: 40, borderRadius: 10, background: AT_BLUE_LIGHT, display: "flex", alignItems: "center", justifyContent: "center", color: AT_BLUE }}>
                                <i className="fas fa-sliders-h" style={{ fontSize: 20 }}></i>
                            </div>
                            <h2 style={{ fontSize: 22, margin: 0, fontWeight: 800, color: AT_BLUE }}>Paramètres Experts</h2>
                        </div>
                        <p style={{ fontSize: 13, color: GRAY_500, marginBottom: 30, marginLeft: 52 }}>Configuration de l'algorithme de planification</p>

                        <div style={{ marginBottom: 25 }}>
                            <label style={{ display: "block", fontSize: 11, color: GRAY_500, marginBottom: 10, textTransform: "uppercase", fontWeight: 700, letterSpacing: "0.5px" }}>
                                <i className="fas fa-users" style={{ marginRight: 6 }}></i> Capacité maximale FAT
                            </label>
                            <div style={{ position: "relative" }}>
                                <input
                                    type="number"
                                    value={config?.fat_capacity || ""}
                                    onChange={(e) => {
                                        const val = e.target.value;
                                        setConfig({ ...config, fat_capacity: val === "" ? "" : val });
                                    }}
                                    placeholder="Ex: 8"
                                    style={{
                                        width: "100%", padding: "14px 16px", backgroundColor: GRAY_50,
                                        border: `1.5px solid ${(!config?.fat_capacity || isNaN(config?.fat_capacity) || parseInt(config?.fat_capacity) <= 0) ? "#EF4444" : GRAY_200}`,
                                        borderRadius: 12, color: GRAY_800,
                                        fontSize: 15, fontWeight: 600, outline: "none", boxSizing: "border-box",
                                        transition: "all 0.2s"
                                    }}
                                    onFocus={e => { if (e.target.style.borderColor !== "#EF4444") e.target.style.borderColor = AT_BLUE }}
                                    onBlur={e => { if (e.target.style.borderColor !== "#EF4444") e.target.style.borderColor = GRAY_200 }}
                                />
                                <span style={{ position: "absolute", right: 16, top: "50%", transform: "translateY(-50%)", fontSize: 12, color: GRAY_400, fontWeight: 600 }}>Abonnés / Boîtier</span>
                            </div>
                            {(!config?.fat_capacity || isNaN(config?.fat_capacity) || parseInt(config?.fat_capacity) <= 0) && (
                                <div style={{ color: "#EF4444", fontSize: 11, marginTop: 6, fontWeight: 600, display: "flex", alignItems: "center", gap: 5 }}>
                                    <i className="fas fa-exclamation-circle"></i> Veuillez saisir un nombre entier positif (ex: 8, 16)
                                </div>
                            )}
                        </div>

                        <div style={{ marginBottom: 35 }}>
                            <label style={{ display: "block", fontSize: 11, color: GRAY_500, marginBottom: 12, textTransform: "uppercase", fontWeight: 700, letterSpacing: "0.5px" }}>
                                <i className="fas fa-project-diagram" style={{ marginRight: 6 }}></i> Standards de câbles préfabriqués
                            </label>

                            {/* Liste actuelle */}
                            <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginBottom: 20 }}>
                                {config?.cable_standards?.map((m, idx) => (
                                    <div key={idx} style={{
                                        padding: "10px 15px", backgroundColor: AT_BLUE_LIGHT, borderRadius: 10,
                                        fontSize: 14, display: "flex", alignItems: "center", gap: 12,
                                        color: AT_BLUE, fontWeight: 700, border: `1px solid rgba(0, 91, 170, 0.1)`
                                    }}>
                                        <i className="fas fa-ruler-horizontal" style={{ opacity: 0.5 }}></i>
                                        {m} mètres
                                        <button
                                            onClick={() => {
                                                const newCables = config?.cable_standards?.filter((_, i) => i !== idx);
                                                setConfig({ ...config, cable_standards: newCables });
                                            }}
                                            style={{
                                                background: "none", border: "none", color: "#EF4444", cursor: "pointer",
                                                padding: "2px", display: "flex", alignItems: "center"
                                            }}
                                            title="Supprimer"
                                        >
                                            <i className="fas fa-trash-alt"></i>
                                        </button>
                                    </div>
                                ))}
                            </div>

                            {/* Zone d'ajout dessous */}
                            <div style={{
                                display: "flex", gap: 10, padding: "16px", background: GRAY_50,
                                borderRadius: 12, border: `1.5px dashed ${GRAY_300}`
                            }}>
                                <input
                                    id="new-cable-input"
                                    type="number"
                                    placeholder="Ajouter une longueur (m)..."
                                    onKeyDown={(e) => {
                                        if (e.key === 'Enter' && e.target.value) {
                                            const val = parseInt(e.target.value);
                                            if (!config?.cable_standards?.includes(val)) {
                                                setConfig({...config, cable_standards: [...(config?.cable_standards || []), val].sort((a,b)=>a-b)});
                                            }
                                            e.target.value = '';
                                        }
                                    }}
                                    style={{
                                        flex: 1, padding: "12px 15px", backgroundColor: "white",
                                        border: `1px solid ${GRAY_200}`, borderRadius: 10, color: GRAY_800,
                                        fontSize: 14, outline: "none"
                                    }}
                                />
                                <button
                                    onClick={() => {
                                        const input = document.getElementById('new-cable-input');
                                        if (input.value) {
                                            const val = parseInt(input.value);
                                            if (!config?.cable_standards?.includes(val)) {
                                                setConfig({...config, cable_standards: [...(config?.cable_standards || []), val].sort((a,b)=>a-b)});
                                            }
                                            input.value = '';
                                        }
                                    }}
                                    style={{
                                        width: 44, height: 44, background: AT_BLUE, color: "white",
                                        border: "none", borderRadius: 10, fontSize: 20, cursor: "pointer",
                                        display: "flex", alignItems: "center", justifyContent: "center",
                                        boxShadow: "0 4px 12px rgba(0, 91, 170, 0.2)"
                                    }}
                                >
                                    <i className="fas fa-plus"></i>
                                </button>
                            </div>
                        </div>

                        <div style={{ display: "flex", gap: 15 }}>
                            <button
                                onClick={() => setShowSettingsModal(false)}
                                style={{
                                    flex: 1, padding: "16px", backgroundColor: "white",
                                    color: GRAY_600, border: `1px solid ${GRAY_200}`, borderRadius: 14,
                                    cursor: "pointer", fontSize: 14, fontWeight: 700, transition: "all 0.2s"
                                }}
                                onMouseEnter={e => e.currentTarget.style.backgroundColor = GRAY_50}
                                onMouseLeave={e => e.currentTarget.style.backgroundColor = "white"}
                            >
                                Annuler
                            </button>
                            <button
                                onClick={handleSaveConfig}
                                disabled={saveLoading || !config?.fat_capacity || isNaN(config?.fat_capacity) || parseInt(config?.fat_capacity) <= 0}
                                style={{
                                    flex: 2, padding: "16px",
                                    background: (saveLoading || !config?.fat_capacity || isNaN(config?.fat_capacity) || parseInt(config?.fat_capacity) <= 0)
                                        ? GRAY_300
                                        : `linear-gradient(135deg, ${AT_BLUE}, ${AT_BLUE_DARK})`,
                                    color: "white", border: "none", borderRadius: 14, cursor: (saveLoading || !config?.fat_capacity || isNaN(config?.fat_capacity) || parseInt(config?.fat_capacity) <= 0) ? "not-allowed" : "pointer",
                                    fontSize: 15, fontWeight: 700, display: "flex", alignItems: "center",
                                    justifyContent: "center", gap: 12, boxShadow: (saveLoading || !config.fat_capacity || isNaN(config.fat_capacity) || parseInt(config.fat_capacity) <= 0) ? "none" : "0 10px 20px -5px rgba(0, 91, 170, 0.3)",
                                    transition: "all 0.2s"
                                }}
                                onMouseEnter={e => { if (!e.currentTarget.disabled) e.currentTarget.style.transform = "translateY(-2px)" }}
                                onMouseLeave={e => { if (!e.currentTarget.disabled) e.currentTarget.style.transform = "translateY(0)" }}
                            >
                                {saveLoading ? <i className="fas fa-spinner fa-spin"></i> : <i className="fas fa-save"></i>}
                                Appliquer les réglages
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Main Content */}
            <main style={{ padding: "40px", maxWidth: "100%", margin: "0" }}>
                <div style={{ marginBottom: "32px" }}>
                    <h2 style={{ fontSize: 32, fontWeight: 800, color: GRAY_800, margin: "0 0 4px 0" }}>Archivage des Projets</h2>
                    <div style={{ fontSize: 14, color: GRAY_400 }}>{stats.totalProjects} projets · triés par date décroissante</div>
                </div>

                {/* Top Controls & KPIs */}
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: "24px", gap: "24px" }}>
                    {/* Search Bar */}
                    <div style={{ flex: 1, maxWidth: "500px", position: "relative" }}>
                        <span style={{ position: "absolute", left: "16px", top: "50%", transform: "translateY(-50%)", color: GRAY_400 }}>
                            <i className="fas fa-search"></i>
                        </span>

                        <input
                            type="text"
                            placeholder="Rechercher un projet, une ville, une commune..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            style={{
                                width: "100%", padding: "14px 16px 14px 44px", borderRadius: 12, border: `1.5px solid ${GRAY_200}`,
                                fontSize: 14, outline: "none", transition: "border-color 0.2s",
                                backgroundColor: "white"
                            }}
                        />
                    </div>

                    {/* KPI Cards */}
                    <div style={{ display: "flex", gap: "16px" }}>
                        <KPICard label="Total projets" value={stats.totalProjects} color={AT_BLUE} />
                        <KPICard label="Villes couvertes" value={stats.coveredCities} color={GREEN} />
                        <KPICard label="Total FATs déployés" value={stats.totalFats} color={AT_ORANGE} />
                    </div>
                </div>

                {/* Projects Table */}
                <div style={{
                    backgroundColor: "white", borderRadius: 16, border: `1px solid ${GRAY_200}`,
                    overflow: "hidden", boxShadow: "0 4px 6px -1px rgba(0,0,0,0.05)"
                }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", textAlign: "left" }}>
                        <thead>
                            <tr style={{ borderBottom: `1px solid ${GRAY_100}` }}>
                                <th style={tableHeaderStyle}> </th>
                                <th style={tableHeaderStyle}>Projet</th>
                                <th style={tableHeaderStyle}>Ville</th>
                                <th style={tableHeaderStyle}>Commune</th>
                                <th style={tableHeaderStyle}>Date</th>
                                <th style={tableHeaderStyle}>Mode</th>
                                <th style={tableHeaderStyle}>FATs</th>
                                <th style={{ ...tableHeaderStyle, textAlign: "right" }}>Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {loading ? (
                                <tr><td colSpan="8" style={{ textAlign: "center", padding: "40px", color: GRAY_400 }}>Chargement des projets...</td></tr>
                            ) : paginatedProjects.length === 0 ? (
                                <tr><td colSpan="8" style={{ textAlign: "center", padding: "40px", color: GRAY_400 }}>Aucun projet trouvé</td></tr>
                            ) : paginatedProjects.map((p, idx, arr) => {
                                const globalIdx = (currentPage - 1) * PROJECTS_PER_PAGE + idx + 1;
                                return (
                                    <tr 
                                        key={p.id_projet} 
                                        onMouseEnter={() => setHoveredRowId(p.id_projet)}
                                        onMouseLeave={() => setHoveredRowId(null)}
                                        style={{
                                            borderBottom: idx === arr.length - 1 ? "none" : `1px solid ${GRAY_100}`,
                                            backgroundColor: hoveredRowId === p.id_projet ? "rgba(0, 91, 170, 0.02)" : "transparent",
                                            borderLeft: hoveredRowId === p.id_projet ? `4px solid ${AT_BLUE}` : "4px solid transparent",
                                            transition: "all 0.15s ease",
                                            cursor: "default"
                                        }}
                                    >
                                        <td style={tableCellStyle}>{globalIdx}</td>
                                        <td style={{ ...tableCellStyle, fontWeight: 700, color: AT_BLUE }}>{p.nom_projet}</td>
                                        <td style={tableCellStyle}>{p.wilaya}</td>
                                        <td style={tableCellStyle}>{p.commune}</td>
                                        <td style={{ ...tableCellStyle, color: GRAY_400 }}>
                                            {new Date(p.date_modification).toLocaleDateString("fr-DZ")}
                                        </td>
                                        <td style={tableCellStyle}>
                                            <span style={{
                                                fontSize: 10, fontWeight: 700, padding: "2px 8px", borderRadius: 4,
                                                backgroundColor: p.planning_mode === "Réseau estimé" ? "#F3E8FF" : AT_BLUE_LIGHT,
                                                color: p.planning_mode === "Réseau estimé" ? "#7C3AED" : AT_BLUE,
                                                textTransform: "uppercase"
                                            }}>
                                                {p.planning_mode}
                                            </span>
                                        </td>
                                        <td style={tableCellStyle}>
                                            <span style={{
                                                padding: "4px 10px", borderRadius: 20, backgroundColor: AT_ORANGE_LIGHT,
                                                color: AT_ORANGE, fontSize: 11, fontWeight: 700
                                            }}>
                                                {p.nb_fats} FATs
                                            </span>
                                        </td>

                                        <td style={{ ...tableCellStyle, textAlign: "right" }}>
                                            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>

                                                <button
                                                    onClick={() => onLoadProject(p.id_projet)}
                                                    style={{
                                                        padding: "8px 16px", borderRadius: 8, border: "none",
                                                        backgroundColor: hoveredRowId === p.id_projet ? AT_BLUE : GRAY_100,
                                                        color: hoveredRowId === p.id_projet ? "white" : GRAY_400,
                                                        fontSize: 12, fontWeight: 600, cursor: "pointer",
                                                        display: "flex", alignItems: "center", gap: 4,
                                                        transition: "all 0.2s"
                                                    }}
                                                >
                                                    {hoveredRowId === p.id_projet ? "Ouvrir ↗" : "Voir"}
                                                </button>
                                            </div>
                                        </td>
                                    </tr>

                                );
                            })}
                        </tbody>
                    </table>

                    {/* Pagination Footer */}
                    {totalPages > 1 && (
                        <div style={{
                            padding: "16px 24px", display: "flex", justifyContent: "center", alignItems: "center",
                            borderTop: `1px solid ${GRAY_100}`, gap: 8
                        }}>
                            <button
                                onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                                disabled={currentPage === 1}
                                style={pageButtonStyle(false, currentPage === 1)}
                            >
                                ←
                            </button>

                            {Array.from({ length: totalPages }, (_, i) => i + 1).map(page => (
                                <button
                                    key={page}
                                    onClick={() => setCurrentPage(page)}
                                    style={pageButtonStyle(currentPage === page)}
                                >
                                    {page}
                                </button>
                            ))}

                            <button
                                onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                                disabled={currentPage === totalPages}
                                style={pageButtonStyle(false, currentPage === totalPages)}
                            >
                                →
                            </button>
                        </div>
                    )}
                </div>
            </main>

            {/* Notification Toast */}
            {toast.show && (
                <div style={{
                    position: "fixed",
                    top: 30,
                    left: "50%",
                    transform: "translateX(-50%)",
                    backgroundColor: toast.type === "success" ? "#10B981" : "#EF4444",
                    color: "white",
                    padding: "12px 24px",
                    borderRadius: 12,
                    boxShadow: "0 10px 25px -5px rgba(0,0,0,0.15)",
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    zIndex: 100000,
                    fontSize: 14,
                    fontWeight: 600,
                    animation: "slideDown 0.3s ease-out"
                }}>
                    <i className={toast.type === "success" ? "fas fa-check-circle" : "fas fa-exclamation-circle"}></i>
                    {toast.message}
                    <style>{`
                        @keyframes slideDown {
                            from { transform: translate(-50%, -20px); opacity: 0; }
                            to { transform: translate(-50%, 0); opacity: 1; }
                        }
                    `}</style>
                </div>
            )}
        </div>
    );
};

const KPICard = ({ label, value, color }) => (
    <div style={{
        backgroundColor: "white", padding: "12px 20px", borderRadius: 12, border: `1px solid ${GRAY_200}`,
        borderTop: `4px solid ${color}`,
        display: "flex", alignItems: "center", gap: 16, minWidth: "180px", boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
    }}>
        <div style={{ fontSize: 24, fontWeight: 800, color: color }}>{value}</div>
        <div style={{ fontSize: 11, fontWeight: 600, color: GRAY_400, textTransform: "uppercase", letterSpacing: "0.5px" }}>{label}</div>
    </div>
);

const tableHeaderStyle = {
    padding: "16px 24px",
    fontSize: 11,
    fontWeight: 700,
    color: GRAY_400,
    textTransform: "uppercase",
    letterSpacing: "0.5px"
};

const tableCellStyle = {
    padding: "20px 24px",
    fontSize: 13,
    color: GRAY_700
};

const pageButtonStyle = (active, disabled) => ({
    width: 32, height: 32, borderRadius: 6, border: "none",
    backgroundColor: active ? AT_BLUE : GRAY_100,
    color: active ? "white" : GRAY_500,
    fontSize: 12, fontWeight: 600, cursor: disabled ? "default" : "pointer",
    display: "flex", alignItems: "center", justifyContent: "center",
    opacity: disabled ? 0.5 : 1
});

export default Profile;
