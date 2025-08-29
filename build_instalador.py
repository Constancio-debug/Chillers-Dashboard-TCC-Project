"""
Builder do instalador (usa PyInstaller).
Execute com o Python preferido (3.13 recomendado):
    py -3.13 build_instalador.py
ou
    py -3.10 build_instalador.py
"""

import os
import sys
import subprocess
from pathlib import Path
import shlex

BASE_DIR = Path(r"C:\Cummins Chillers Dashboard")
SCRIPT = BASE_DIR / "Dashboard_install_dependencies.py"
DIST = BASE_DIR / "_instaladores"
BUILD = BASE_DIR / "_build"
SPEC = BASE_DIR
NAME = "CumminsDashboardInstaller"

def run(cmd, check=True):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd, posix=False)
    print("$", " ".join(cmd), flush=True)
    r = subprocess.run(cmd, text=True)
    if check and r.returncode != 0:
        raise SystemExit(r.returncode)

def main():
    DIST.mkdir(parents=True, exist_ok=True)
    BUILD.mkdir(parents=True, exist_ok=True)

    # garantir pyinstaller instalado no Python que está rodando este script
    run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=True)
    run([sys.executable, "-m", "pip", "install", "pyinstaller>=6.0"], check=True)

    # build
    run([
        sys.executable, "-m", "PyInstaller",
        "--noconfirm", "--clean", "--onefile", "--console",
        f"--name={NAME}",
        f"--distpath={str(DIST)}",
        f"--workpath={str(BUILD)}",
        f"--specpath={str(SPEC)}",
        "--paths=.",
        str(SCRIPT)
    ], check=True)

    print(f"\n[OK] Gerado: {DIST / (NAME + '.exe')}", flush=True)

if __name__ == "__main__":
    main()
