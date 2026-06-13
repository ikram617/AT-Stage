import React, { useState } from "react";
import ATLogoImg from "./assets/algerie-telecom-logo-png_seeklogo-210074.png";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  "https://kzdxrojsvfgyuojxzgnu.supabase.co",
  "sb_publishable_eQi-v7FpGVfBSWESddINnA_t0Hg4lYO"
);
const AT_BLUE = "#005BAA";
const AT_ORANGE = "#F7941D";
const GRAY_100 = "#F3F4F6";
const GRAY_200 = "#E5E7EB";
const GRAY_400 = "#9CA3AF";
const GRAY_700 = "#374151";
const GRAY_800 = "#1F2937";

const API = "http://127.0.0.1:8000";


const Login = ({ onLoginSuccess, notify, notif, setNotif, Notification, globalStyle, labelStyle, inputStyle, btnPrimary }) => {
  const [loginData, setLoginData] = useState({ id: "", password: "" });
  const [loginLoading, setLoginLoading] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  const handleLogin = async () => {
    if (!loginData.id || !loginData.password) {
      notify("error", "Données manquantes", "Veuillez saisir vos identifiants");
      return;
    }
    setLoginLoading(true);
    console.log("🔐 Tentative de connexion via Supabase...");
    try {
      // Appel de la fonction RPC 'verify_user' créée dans Supabase
      const { data, error } = await supabase.rpc('verify_user', {
        p_username: loginData.id,
        p_password: loginData.password
      });

      if (error) throw error;

      if (data && data.length > 0) {
        const user = data[0];
        console.log("✅ Authentification réussie. Bienvenue,", user.nom);
        
        // Stockage optionnel des infos utilisateur
        localStorage.setItem("user", JSON.stringify(user));
        
        notify("success", "Connexion réussie", `Bienvenue ${user.prenom} !`);
        onLoginSuccess();
      } else {
        throw new Error("Nom d'utilisateur ou mot de passe incorrect");
      }
    } catch (err) {
      console.log("❌ Échec de connexion :", err.message);
      notify("error", "Échec de connexion", err.message);
    } finally {
      setLoginLoading(false);
    }
  };

  return (
    <div style={{
      width: "100vw",
      height: "100vh",
      display: "flex",
      overflow: "hidden",
      fontFamily: "'Outfit', sans-serif"
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
            Algérie Télécom · Système de planification FAT
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
                placeholder=""
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
            onClick={handleLogin}
            disabled={loginLoading}
          >
            {loginLoading ? "Connexion..." : "Accéder au Planner →"}
          </button>
        </div>
      </div>
      {notif && <Notification notif={notif} onClose={() => setNotif(null)} />}
    </div>
  );
};

export default Login;
