# Dashboard_installer.py
# Instalador resiliente: só exige Atualizar_Dados.exe; demais artefatos são opcionais.
# Cria pastas-alvo, copia (ou move) o que existir e apaga a origem após copiar.

import os
import shutil
import sys
from pathlib import Path
from datetime import datetime

# --- Configurações de destino ---
BASE_DIR = Path(r"C:\Cummins Chillers Dashboard")
DEST_EXECUTORES = BASE_DIR / "_executores"
DEST_INSTALADORES = BASE_DIR / "_instaladores"
DEST_BUILD = BASE_DIR / "_build"
DEST_LOGS = BASE_DIR / "_logs"
DEST_CUMMINS = BASE_DIR / "dados_cummins"
DEST_TRANSICOES = DEST_CUMMINS / "_transicoes"
DEST_INMET = BASE_DIR / "dados_inmet"
DEST_SIS_ELETRICO = BASE_DIR / "dados_sistema_eletrico_brasil"

# --- Artefatos esperados ao lado do instalador ---
SELF_DIR = Path(sys.argv[0]).resolve().parent

# Essencial: se faltar, aborta
ESSENCIAIS_ARQUIVOS = [
    SELF_DIR / "Atualizar_Dados.exe",
]

# Opcionais: se existir, copia; se não existir, segue
OPCIONAIS_ARQUIVOS = [
    # exemplos comuns de ficar junto do instalador:
    SELF_DIR / "CumminsDashboardInstaller.exe",     # se você também distribuir essa cópia
    SELF_DIR / "python-3.13.5-amd64.exe",           # caso mande o instalador do Python
]

# Pastas opcionais que, se presentes, vão para _executores
OPCIONAIS_PASTAS_EXECUTORES = [
    SELF_DIR / "baixar_dados.py",
    SELF_DIR / "estimativas_dados.py",
    SELF_DIR / "tratamento_dados.py",
    SELF_DIR / "utils.py",
    SELF_DIR / "validacao_acuracia.py",
]

# Pastas/arquivos opcionais que, se presentes, vão para _instaladores
OPCIONAIS_PARA_INSTALADORES = [
    SELF_DIR / "Dashboard_install_dependencies.py",
    SELF_DIR / "Cummins Chillers Dashboard.rar",  # se você mandar algum pacote/zip com esse nome
]

LOG_FILE = DEST_LOGS / f"instalador_{datetime.now():%Y%m%d_%H%M%S}.log"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} - {msg}"
    print(line)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def safe_copy_file(src: Path, dst_dir: Path, remove_src=True):
    if not src.exists():
        log(f"[ignorado] Arquivo não encontrado: {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        log(f"[ok] Copiado: {src} -> {dst}")
        if remove_src:
            try:
                src.unlink()
                log(f"[ok] Removido da origem: {src}")
            except Exception as e:
                log(f"[warn] Não removi origem: {src} ({e})")
        return True
    except Exception as e:
        log(f"[erro] Falha ao copiar arquivo {src}: {e}")
        return False

def safe_copy_tree(src: Path, dst_dir: Path, remove_src=True):
    if not src.exists():
        log(f"[ignorado] Pasta não encontrada: {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        if dst.exists():
            # mescla conteúdo: copia por arquivo
            for root, _, files in os.walk(src):
                rel = Path(root).relative_to(src)
                target_root = dst / rel
                target_root.mkdir(parents=True, exist_ok=True)
                for fn in files:
                    s = Path(root) / fn
                    d = target_root / fn
                    shutil.copy2(s, d)
            log(f"[ok] Mesclado conteúdo: {src} -> {dst}")
        else:
            shutil.copytree(src, dst)
            log(f"[ok] Copiado diretório: {src} -> {dst}")
        if remove_src:
            try:
                shutil.rmtree(src)
                log(f"[ok] Removido diretório de origem: {src}")
            except Exception as e:
                log(f"[warn] Não removi origem: {src} ({e})")
        return True
    except Exception as e:
        log(f"[erro] Falha ao copiar pasta {src}: {e}")
        return False

def main():
    log("=== Instalador do Cummins Chillers Dashboard ===")
    # 1) Pastas base
    log("Criando pastas do projeto…")
    for p in [
        DEST_EXECUTORES, DEST_INSTALADORES, DEST_BUILD, DEST_LOGS,
        DEST_CUMMINS, DEST_TRANSICOES, DEST_INMET, DEST_SIS_ELETRICO
    ]:
        p.mkdir(parents=True, exist_ok=True)
        log(f"[ok] {p}")

    # 2) Verifica essenciais
    faltando = [str(p) for p in ESSENCIAIS_ARQUIVOS if not p.exists()]
    if faltando:
        log("[erro] Artefatos essenciais ausentes:")
        for f in faltando:
            log(f"       - {f}")
        log("Instalação abortada. Certifique-se de colocar o Atualizar_Dados.exe ao lado deste instalador.")
        sys.exit(1)

    # 3) Copia essenciais
    for f in ESSENCIAIS_ARQUIVOS:
        safe_copy_file(f, DEST_EXECUTORES, remove_src=True)

    # 4) Copia opcionais (arquivos)
    for f in OPCIONAIS_ARQUIVOS:
        safe_copy_file(f, DEST_INSTALADORES, remove_src=True)

    # 5) Copia pastas opcionais para _executores
    for d in OPCIONAIS_PASTAS_EXECUTORES:
        safe_copy_tree(d, DEST_EXECUTORES, remove_src=True)

    # 6) Copia opcionais para _instaladores (podem ser pastas ou arquivos)
    for item in OPCIONAIS_PARA_INSTALADORES:
        if item.is_dir():
            safe_copy_tree(item, DEST_INSTALADORES, remove_src=True)
        else:
            safe_copy_file(item, DEST_INSTALADORES, remove_src=True)

    log("[ok] Instalação finalizada com sucesso.")
    log(f"Executável principal em: {BASE_DIR / 'AtualizarCumminsDashboard.exe'}")
    log("Você pode executar manualmente agora, se quiser.")

    # Opcional: iniciar automaticamente o Atualizar_Dados
    try:
        os.startfile(BASE_DIR / "AtualizarCumminsDashboard.exe")
        log("[ok] AtualizarCumminsDashboard.exe iniciado.")
    except Exception as e:
        log(f"[warn] Não consegui iniciar automaticamente: {e}")

if __name__ == "__main__":
    main()
