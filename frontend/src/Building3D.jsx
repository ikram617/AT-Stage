import React, { Suspense } from "react";
import * as THREE from "three";
import { Canvas } from "@react-three/fiber";
import { OrbitControls, PerspectiveCamera, Environment, ContactShadows, Text, Billboard, Float as FloatDrei } from "@react-three/drei";

// ── BuildingStructure ──
const BuildingStructure = ({ etages, hauteurEtage, width, depth }) => {
  const safeEtages = Math.max(etages +1 || 1, 1);
  const safeHauteur = hauteurEtage || 3;
  const totalHeight = safeEtages * safeHauteur;

  return (
    <group position={[0, 0, 0]}>
      {/* Glass outer envelope */}
      <mesh position={[0, totalHeight / 2, 0]}>
        <boxGeometry args={[width, totalHeight, depth]} />
        <meshStandardMaterial 
          color="#f8fafc" 
          transparent 
          opacity={0.03} 
          roughness={0.1} 
          metalness={0.1} 
        />
      </mesh>
      
      {/* Outer Envelope Wireframe Outline */}
      <mesh position={[0, totalHeight / 2, 0]}>
        <boxGeometry args={[width + 0.05, totalHeight + 0.05, depth + 0.05]} />
        <meshBasicMaterial 
          color="#38bdf8" 
          wireframe 
          transparent 
          opacity={0.1} 
        />
      </mesh>
      
      {/* Floor Slabs */}
      {Array.from({ length: safeEtages + 1 }).map((_, i) => (
        <group key={i} position={[0, i * safeHauteur, 0]}>
          <mesh rotation={[-Math.PI / 2, 0, 0]}>
            <planeGeometry args={[width, depth]} />
            <meshStandardMaterial color="#475569" transparent opacity={0.2} />
          </mesh>
          
          {/* Slab Wireframe Border */}
          <mesh rotation={[-Math.PI / 2, 0, 0]}>
            <planeGeometry args={[width + 0.02, depth + 0.02]} />
            <meshBasicMaterial color="#334155" wireframe transparent opacity={0.25} />
          </mesh>

          {/* Floor Number Label */}
          <Billboard position={[-width/2 - 1.5, 0.4, 0]}>
            <Text fontSize={0.35} color="#38bdf8" fontWeight={900} outlineWidth={0.04} outlineColor="#020617">
              {i === 0 ? "RDC" : `ET.${i}`}
            </Text>
          </Billboard>
        </group>
      ))}

      {/* Central Core / Elevator Shaft */}
      <mesh position={[0, totalHeight / 2, 0]}>
        <boxGeometry args={[2, totalHeight, 2]} />
        <meshStandardMaterial color="#0f172a" roughness={0.6} metalness={0.4} transparent opacity={0.7} />
      </mesh>
      
      {/* Ground Plane */}
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -0.05, 0]} receiveShadow>
        <circleGeometry args={[Math.max(width, depth) * 1.5, 64]} />
        <meshStandardMaterial color="#0b0f19" roughness={1} />
      </mesh>
    </group>
  );
};

// ── ApartmentRoom ──
const ApartmentRoom = ({ position, label, subColor, isOccupied, isCommerce }) => {
  const roomColor = isCommerce ? "#fffbeb" : "#f8fafc";
  const borderColor = isCommerce ? "#f97316" : "#38bdf8";
  const glowColor = subColor || (isOccupied ? "#38bdf8" : "#475569");

  return (
    <group position={position}>
      {/* Semi-transparent Glass Room Cube */}
      <mesh castShadow receiveShadow>
        <boxGeometry args={[1.5, 1.1, 1.5]} />
        <meshStandardMaterial 
          color={roomColor} 
          roughness={0.7} 
          metalness={0.1} 
          transparent 
          opacity={0.06} 
        />
      </mesh>
      
      {/* Wireframe Outline */}
      <mesh>
        <boxGeometry args={[1.51, 1.11, 1.51]} />
        <meshBasicMaterial 
          color={borderColor} 
          wireframe 
          transparent 
          opacity={0.25} 
        />
      </mesh>

      {/* Glowing Port Sphere on front facade */}
      <mesh position={[0, 0, 0.76]} castShadow>
        <sphereGeometry args={[0.2, 16, 16]} />
        <meshStandardMaterial 
          color={glowColor} 
          emissive={glowColor} 
          emissiveIntensity={isOccupied ? 1.6 : 0.2} 
          roughness={0.1}
          metalness={0.7}
        />
      </mesh>

      {/* Subscriber Label */}
      <Billboard position={[0, 0.75, 0]}>
        <Text fontSize={0.22} color={borderColor} fontWeight={800} outlineWidth={0.03} outlineColor="#020617">
          {label}
        </Text>
      </Billboard>
    </group>
  );
};

// ── FATMarker ──
const FATMarker = ({ position, label, color = "#f97316" }) => {
  const [hovered, setHovered] = React.useState(false);
  return (
    <group 
      position={position}
      onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = "pointer"; }}
      onPointerOut={(e) => { setHovered(false); document.body.style.cursor = "auto"; }}
    >
      <FloatDrei speed={3} rotationIntensity={0.6} floatIntensity={0.6}>
        <mesh rotation={[Math.PI / 4, 0, Math.PI / 4]}>
          <octahedronGeometry args={[0.38, 0]} />
          <meshStandardMaterial 
            color={color} 
            emissive={color} 
            emissiveIntensity={hovered ? 2.5 : 1.0} 
            metalness={0.9}
            roughness={0.05}
          />
        </mesh>
      </FloatDrei>
      
      {/* Floating text label for the FAT Name */}
      <Billboard position={[0, 0.8, 0]}>
        <Text fontSize={0.28} color="white" fontWeight={900} outlineWidth={0.05} outlineColor="#020617" depthTest={false}>
          {label}
        </Text>
      </Billboard>
    </group>
  );
};

const FAT_COLORS = [
  "#38bdf8", "#f97316", "#22c55e", "#ef4444",
  "#a855f7", "#eab308", "#ec4899", "#14b8a6",
  "#6366f1", "#f43f5e", "#84cc16", "#06b6d4"
];

// ── FiberLine (Native fallback for Line2) ──
const FiberLine = ({ start, end, color }) => {
  const points = React.useMemo(() => [
    new THREE.Vector3(...start),
    new THREE.Vector3(...end)
  ], [start, end]);

  const geometry = React.useMemo(() => {
    const geo = new THREE.BufferGeometry();
    geo.setFromPoints(points);
    return geo;
  }, [points]);

  return (
    <line geometry={geometry}>
      <lineBasicMaterial color={color} transparent opacity={0.65} />
    </line>
  );
};

// ── BuildingScene ──
const BuildingScene = ({ etages, logements, hauteurEtage, fatResults, subscribersData, planningMode, targetY }) => {
  const safeEtages = Math.max(etages || 1, 1);
  const safeLogements = Math.max(logements || 1, 1);
  const safeHauteur = hauteurEtage || 3;

  const cols = Math.ceil(Math.sqrt(safeLogements));
  const rows = Math.ceil(safeLogements / cols);
  
  const spacing = 3.6;
  const width = cols * spacing + 2.0;
  const depth = rows * spacing + 2.0;
  
  const getSubPosition = React.useCallback((etage, subIdx) => {
    const col = subIdx % cols;
    const row = Math.floor(subIdx / cols);
    const x = -((cols - 1) * spacing) / 2 + col * spacing;
    const z = -((rows - 1) * spacing) / 2 + row * spacing;
    const y = etage * safeHauteur + safeHauteur * 0.45;
    return [x, y, z];
  }, [cols, rows, spacing, safeHauteur]);

  const subsByFloor = React.useMemo(() => {
    const map = {};
    (subscribersData || []).forEach(s => {
      // En mode estimé (prediction), on n'affiche que les portes occupées (habite === 1)
      if (planningMode === "prediction" && s.habite === 0) {
        return;
      }
      if (!map[s.etage]) map[s.etage] = [];
      map[s.etage].push(s);
    });
    Object.keys(map).forEach(et => {
      map[et].sort((a, b) => (a.porte ?? 0) - (b.porte ?? 0));
    });
    return map;
  }, [subscribersData, planningMode]);

  const subPositions = React.useMemo(() => {
    const posMap = {};
    Object.entries(subsByFloor).forEach(([etageStr, subs]) => {
      const etage = parseInt(etageStr);
      subs.forEach((sub, idx) => {
        const key = sub.code_client || sub.id;
        if (key) {
          posMap[key] = getSubPosition(etage, idx);
        }
      });
    });
    return posMap;
  }, [subsByFloor, getSubPosition]);

  const subToColor = React.useMemo(() => {
    const map = {};
    if (!fatResults) return map;
    fatResults.forEach((fat, idx) => {
      const color = FAT_COLORS[idx % FAT_COLORS.length];
      if (fat.subscriber_ids) {
        fat.subscriber_ids.forEach(id => { map[id] = color; });
      }
    });
    return map;
  }, [fatResults]);

  const fatPositions = React.useMemo(() => {
    if (!fatResults) return [];
    return fatResults.map((fat, i) => {
      const connectedIds = fat.subscriber_ids || [];
      const floorNum = fat.etage_fat || 0;
      
      let sumX = 0, sumZ = 0, count = 0;
      connectedIds.forEach(id => {
        const pos = subPositions[id];
        if (pos) {
          sumX += pos[0];
          sumZ += pos[2];
          count++;
        }
      });

      let x = 0;
      let z = 0;
      if (count > 0) {
        x = sumX / count;
        z = sumZ / count;
      } else {
        x = 0;
        z = 0;
      }
      
      const y = floorNum * safeHauteur + safeHauteur * 0.8;
      return {
        fat,
        position: [x, y, z],
        color: FAT_COLORS[i % FAT_COLORS.length],
        label: fat.fat_id_AT || fat.fat_id
      };
    });
  }, [fatResults, subPositions, safeHauteur]);

  const fiberLines = React.useMemo(() => {
    const lines = [];
    fatPositions.forEach(({ fat, position: fatPos, color }) => {
      const connectedIds = fat.subscriber_ids || [];
      connectedIds.forEach(subId => {
        const subPos = subPositions[subId];
        if (subPos) {
          lines.push({
            id: `${fat.fat_id_AT || fat.fat_id}-${subId}`,
            points: [fatPos, subPos],
            color
          });
        }
      });
    });
    return lines;
  }, [fatPositions, subPositions]);

  return (
    <>
      <ambientLight intensity={0.5} />
      <directionalLight position={[20, 30, 10]} intensity={1.3} castShadow />
      <pointLight position={[-15, 10, -10]} intensity={0.8} color="#38bdf8" />
      
      <BuildingStructure etages={safeEtages} hauteurEtage={safeHauteur} width={width} depth={depth} />
      
      {Object.entries(subsByFloor).map(([etageStr, subs]) => {
        const etage = parseInt(etageStr);
        return subs.map((sub, idx) => {
          const key = sub.code_client || sub.id;
          const pos = subPositions[key];
          if (!pos) return null;
          const isCommerce = sub.usage === "commerces";
          const subKey = key || `sub-${etage}-${idx}`;
          const isOccupied = !!subToColor[key];
          
          return (
            <ApartmentRoom 
              key={subKey} 
              position={pos} 
              label={isCommerce ? `C.${sub.porte}` : `P.${sub.porte}`} 
              subColor={subToColor[key]} 
              isOccupied={isOccupied}
              isCommerce={isCommerce}
            />
          );
        });
      })}
      
      {fatPositions.map(({ position, color, label }) => {
        return (
          <FATMarker 
            key={label} 
            position={position} 
            label={label} 
            color={color} 
          />
        );
      })}

      {fiberLines.map(line => (
        <FiberLine 
          key={line.id} 
          start={line.points[0]} 
          end={line.points[1]} 
          color={line.color} 
        />
      ))}

      <ContactShadows position={[0, 0, 0]} opacity={0.6} scale={40} blur={2} far={15} />
      <OrbitControls makeDefault enablePan={true} panSpeed={1} target={[0, targetY, 0]} />
    </>
  );
};

export default function Building3D({ etages, logements, hauteurEtage = 3, fatResults, subscribersData, planningMode }) {
  const safeEtages = Math.max(etages || 1, 1);
  const safeLogements = Math.max(logements || 1, 1);
  const safeHauteur = hauteurEtage || 3;

  const [scrollVal, setScrollVal] = React.useState(0);
  const maxHeight = safeEtages * safeHauteur;
  const centerY = maxHeight / 2;
  const targetY = centerY + (scrollVal - 0.5) * maxHeight;

  return (
    <div style={{ width: "100%", height: "100%", background: "#020617", borderRadius: 16, overflow: "hidden", position: "relative", display: "flex" }}>
      <div style={{ flex: 1, position: "relative" }}>
        <div style={{ position: "absolute", top: 20, left: 20, zIndex: 10, pointerEvents: "none" }}>
          <div style={{ fontSize: 10, fontWeight: 800, color: "#38bdf8", textTransform: "uppercase", letterSpacing: 3, marginBottom: 6 }}>Architecture 3D</div>
          <div style={{ fontSize: 20, fontWeight: 800, color: "white" }}>{safeEtages} Étages · {fatResults?.length || 0} FATs</div>
        </div>
        
        <Canvas shadows gl={{ antialias: true }}>
          <PerspectiveCamera makeDefault position={[18, centerY + 8, 18]} fov={30} />
          <Suspense fallback={null}>
            <BuildingScene etages={safeEtages} logements={safeLogements} hauteurEtage={safeHauteur} fatResults={fatResults} subscribersData={subscribersData} planningMode={planningMode} targetY={targetY} />
            <Environment preset="night" />
          </Suspense>
        </Canvas>

        <div style={{ position: "absolute", bottom: 20, right: 20, zIndex: 10, pointerEvents: "none", background: "rgba(15, 23, 42, 0.9)", backdropFilter: "blur(12px)", padding: "12px 16px", borderRadius: 12, fontSize: 10, color: "white", border: "1px solid rgba(56, 189, 248, 0.3)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
            <span style={{ color: "#38bdf8" }}>↕</span>
            <span>Navigation Verticale (Slider)</span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ color: "#38bdf8" }}>⟳</span>
            <span>Rotation Libre (Clic Gauche)</span>
          </div>
        </div>
      </div>

      {/* ── Vertical Scrollbar Slider ── */}
      <div style={{ width: 44, background: "rgba(15, 23, 42, 0.8)", borderLeft: "1px solid rgba(56, 189, 248, 0.15)", display: "flex", flexDirection: "column", alignItems: "center", padding: "40px 0" }}>
        <div style={{ fontSize: 9, color: "#38bdf8", fontWeight: 900, marginBottom: 20 }}>MAX</div>
        <div style={{ flex: 1, position: "relative", width: "100%", display: "flex", justifyContent: "center" }}>
          <input type="range" min="0" max="1" step="0.01" value={scrollVal} onChange={(e) => setScrollVal(parseFloat(e.target.value))} style={{ writingMode: "bt-lr", appearance: "slider-vertical", width: 8, height: "100%", cursor: "pointer", accentColor: "#38bdf8" }} />
        </div>
        <div style={{ fontSize: 9, color: "#38bdf8", fontWeight: 900, marginTop: 20 }}>RDC</div>
      </div>
    </div>
  );
}
