# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_data_files
import os

datas = []
datas += copy_metadata('osmnx')
datas += copy_metadata('geopandas')
datas += copy_metadata('shapely')
datas += copy_metadata('folium')
datas += copy_metadata('xgboost')
datas += copy_metadata('optuna')
datas += copy_metadata('scikit-learn')
datas += copy_metadata('pandas')
datas += copy_metadata('numpy')
datas += copy_metadata('fastapi')
datas += copy_metadata('uvicorn')
datas += collect_data_files('osmnx')
datas += collect_data_files('geopandas')
datas += collect_data_files('folium')

# xgboost VERSION file
datas += [(r'C:\Users\blabl\anaconda3\Lib\site-packages\xgboost\VERSION', 'xgboost')]

# Modeles joblib
datas += [
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\Greedy_Vertical_Algorithm_hybride\models\k_predictor.joblib', 'Greedy_Vertical_Algorithm_hybride\\models'),
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\Greedy_Vertical_Algorithm_hybride\models\k_predictor_osm.joblib', 'Greedy_Vertical_Algorithm_hybride\\models'),
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\Greedy_Vertical_Algorithm_hybride\models\k_predictor_osm_optuna.joblib', 'Greedy_Vertical_Algorithm_hybride\\models'),
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\Greedy_Vertical_Algorithm_hybride\models\pipeline_config.joblib', 'Greedy_Vertical_Algorithm_hybride\\models'),
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\Greedy_Vertical_Algorithm_hybride\models\snap_rules.joblib', 'Greedy_Vertical_Algorithm_hybride\\models'),
]

# Donnees statiques
datas += [
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\data\communes.json', 'data'),
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\data\wilayas.json', 'data'),
]

# OSM json cache (58 wilayas + villes)
datas += [
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\osm_json_cache', 'osm_json_cache'),
]

# Residence cache
datas += [
    (r'C:\Users\blabl\OneDrive\Desktop\4eme annee stage\CodeSource\AT-Stage\backend\residence_cache', 'residence_cache'),
]

binaries = [
    (r'C:\Users\blabl\anaconda3\Library\bin\xgboost.dll', 'xgboost'),
    (r'C:\Users\blabl\anaconda3\Library\bin\xgboost.dll', 'Library\\bin'),
    (r'C:\Users\blabl\anaconda3\Library\bin\xgboost.dll', '.'),
]

block_cipher = None

a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=[
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        'uvicorn.main',
        'uvicorn._types',
        'fastapi',
        'pydantic',
        'xgboost',
        'xgboost.sklearn',
        'xgboost.training',
        'xgboost.core',
        'optuna',
        'geopandas',
        'shapely',
        'shapely.geometry',
        'folium',
        'osmnx',
        'sklearn',
        'sklearn.utils._cython_blas',
        'sklearn.neighbors._partition_nodes',
        'joblib',
        'pandas',
        'numpy',
    ],
    excludes=[
        'PyQt5',
        'PySide6',
        'tensorflow',
        'xgboost.testing',
    ],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='FAT_Planner_Backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)