const { app, BrowserWindow } = require("electron");
const path = require("path");
const { spawn } = require("child_process");
const fs = require("fs");

let backendProcess;
let mainWindow;

function startBackend() {
  const backendPath = path.join(
    process.resourcesPath,
    "FAT_Planner_Backend.exe"
  );

  console.log("Backend path:", backendPath);
  console.log("Exists:", fs.existsSync(backendPath));

  if (!fs.existsSync(backendPath)) {
    console.error("FAT_Planner_Backend.exe introuvable !");
    return;
  }

  backendProcess = spawn(backendPath, [], {
    detached: false,
    stdio: "pipe",
    env: { ...process.env, PORT: "8000" }
  });

  backendProcess.stdout.on("data", (data) => {
    console.log("Backend:", data.toString());
  });

  backendProcess.stderr.on("data", (data) => {
    console.error("Backend stderr:", data.toString());
  });

  backendProcess.on("error", (err) => {
    console.error("Failed to start backend:", err);
  });

  backendProcess.on("close", (code) => {
    console.log("Backend exited with code:", code);
  });
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    icon: path.join(__dirname, "src/assets/icon.ico"),
    title: "FAT Planner",
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
    },
  });

  mainWindow.setMenuBarVisibility(false);

  // Attendre 5 secondes que le backend démarre
  setTimeout(() => {
    mainWindow.loadFile(path.join(__dirname, "dist/index.html"));
  }, 5000);

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

app.whenReady().then(() => {
  startBackend();
  createWindow();
});

app.on("window-all-closed", () => {
  if (backendProcess) {
    backendProcess.kill();
  }
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("activate", () => {
  if (mainWindow === null) {
    createWindow();
  }
});