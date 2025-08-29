# rebuild.py 
# Rebuild do Atualizar_Dados usando SEMPRE Python 3.13 em venv própria (Windows).
# Não depende do Python 3.10 instalado.

import os
import sys
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
# Nome final do executável
PROJ = "AtualizarCumminsDashboard"
SCRIPT = BASE_DIR / "Atualizar_Dados.py"

DIST = BASE_DIR / "_instaladores"
BUILD = BASE_DIR / "_build"
SPEC  = BASE_DIR

VENV_DIR = BASE_DIR / ".venv-build-313"
VENV_PY  = VENV_DIR / "Scripts" / "python.exe"   # Windows

PIP_ENV = dict(os.environ, PIP_DISABLE_PIP_VERSION_CHECK="1")

def run(cmd, cwd=None, env=None):
    print(f"\n$ {' '.join(str(c) for c in cmd)}")
    try:
        subprocess.check_call(cmd, cwd=cwd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"\n[ERRO] Comando falhou com código {e.returncode}.")
        sys.exit(e.returncode)

def fail(msg):
    print(f"[ERRO] {msg}")
    sys.exit(1)

def check_py313():
    try:
        out = subprocess.check_output(["py", "-3.13", "-c", "import sys;print(sys.version)"], text=True)
        ver = out.strip().split()[0]
        print(f"Python 3.13 encontrado pelo py launcher: {ver}")
        return True
    except Exception:
        return False

def ensure_script():
    if not SCRIPT.exists():
        fail(f"Arquivo não encontrado: {SCRIPT}")

def ensure_dirs():
    DIST.mkdir(exist_ok=True)
    BUILD.mkdir(exist_ok=True)

def ensure_venv():
    if VENV_PY.exists():
        print(f"Venv já existe: {VENV_DIR}")
        return
    if not check_py313():
        fail("Python 3.13 não encontrado pelo launcher 'py'. "
             "Instale o Python 3.13 (Add to PATH + py launcher) ou rode manualmente: "
             "py -3.13 -m venv .venv-build-313")
    print(f"Criando venv com Python 3.13 em: {VENV_DIR}")
    run(["py", "-3.13", "-m", "venv", str(VENV_DIR)])

def pip_install(pkgs):
    run([str(VENV_PY), "-m", "pip", "install", "--upgrade"] + pkgs, env=PIP_ENV)

def main():
    print("=== Rebuild dos executáveis (Python 3.13) ===")
    print(f"Invocado por: {sys.executable}")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"Saída final: {DIST}")

    ensure_script()
    ensure_dirs()
    ensure_venv()

    # Ferramentas básicas
    pip_install(["pip", "setuptools", "wheel"])

    # Dependências do projeto para congelamento (inclui beautifulsoup4)
    pip_install([
        "pandas",
        "openpyxl",
        "httpx",
        "certifi",
        "matplotlib",   # fonts/backends serão coletados
        "beautifulsoup4",  # <— ADICIONADO
        "pyinstaller",
    ])

    # Limpa spec antigo do mesmo nome (evita conflito)
    spec_file = SPEC / f"{PROJ}.spec"
    if spec_file.exists():
        try:
            spec_file.unlink()
            print(f"Removido spec antigo: {spec_file}")
        except Exception:
            pass

    # PyInstaller (coleta dados necessários e inclui submódulos do bs4)
    cmd = [
        str(VENV_PY), "-m", "PyInstaller",
        "--noconfirm", "--clean", "--onefile", "--console",
        f"--name={PROJ}",
        f"--distpath={DIST}",
        f"--workpath={BUILD}",
        f"--specpath={SPEC}",
        "--paths=.",                                  # raiz do projeto
        f"--paths={BASE_DIR / '_executores'}",        # módulos internos
        "--collect-data=certifi",
        "--collect-data=openpyxl",
        "--collect-data=matplotlib",
        "--collect-submodules=httpx",
        "--collect-submodules=bs4",                   # <— ADICIONADO
        "--exclude-module=scipy",
        "--exclude-module=sklearn",
        str(SCRIPT),
    ]

    print(f"\n=== Build: {PROJ} ===")
    run(cmd, env=PIP_ENV)

    exe_path = DIST / f"{PROJ}.exe"
    if exe_path.exists():
        print(f"\n[OK] Build concluído: {exe_path}")
    else:
        print("\n[AVISO] Build terminou, mas o executável não foi encontrado. Verifique os logs acima.")

if __name__ == "__main__":
    main()
