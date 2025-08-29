# Dashboard_install_dependencies.py
# Instalador resiliente (padrão de logs igual ao utils):
# - Exige apenas AtualizarCumminsDashboard.exe ao lado do instalador.
# - Cria a estrutura mínima de pastas.
# - Copia/move itens opcionais para os destinos corretos e remove a origem após copiar.
# - Mantém o executável principal no diretório raiz do projeto.
#
# Formato de log: "%(asctime)s - %(levelname)s - %(message)s"
# Exemplos:
#   2025-08-27 21:10:00 - INFO - Pasta pronta: C:\Cummins Chillers Dashboard\_logs
#   2025-08-27 21:10:01 - ERRO - Artefatos essenciais ausentes:
#   2025-08-27 21:10:02 - INFO - Cópia concluída: X -> Y

import os
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime

# === [Seção inst-010: Configuração base e destinos] =========================
# Objetivo:
#     Definir diretório-base e todas as pastas-alvo da instalação.
# ============================================================================
BASE_DIR = Path(r"C:\Cummins Chillers Dashboard")
DEST_EXECUTORES   = BASE_DIR / "_executores"
DEST_INSTALADORES = BASE_DIR / "_instaladores"
DEST_LOGS         = BASE_DIR / "_logs"
DEST_CUMMINS      = BASE_DIR / "dados_cummins"
DEST_TRANSICOES   = DEST_CUMMINS / "_transicoes"
DEST_INMET        = BASE_DIR / "dados_inmet"
DEST_SIS_ELETRICO = BASE_DIR / "dados_sistema_eletrico_brasil"

# Diretório onde o instalador está rodando (arquivos de origem)
SELF_DIR = Path(sys.argv[0]).resolve().parent

# === [Seção inst-020: Artefatos essenciais/opcionais] ======================
# Objetivo:
#     Declarar o que é obrigatório e o que é opcional copiar.
# ============================================================================
# Essencial: se faltar, aborta.
ESSENCIAIS_ARQUIVOS = [
    SELF_DIR / "AtualizarCumminsDashboard.exe",
]

# Opcionais (se existir, copia para _instaladores)
OPCIONAIS_ARQUIVOS_PARA_INSTALADORES = [
    SELF_DIR / "CumminsDashboardInstaller.exe",   # se você também distribuir esta cópia
    SELF_DIR / "python-3.13.5-amd64.exe",         # instalador do Python (se enviado)
    SELF_DIR / "Dashboard_install_dependencies.py"
]

# Opcionais (se existir, copia para _executores)
OPCIONAIS_ARQUIVOS_PARA_EXECUTORES = [
    SELF_DIR / "baixar_dados.py",
    SELF_DIR / "estimativas_dados.py",
    SELF_DIR / "tratamento_dados.py",
    SELF_DIR / "utils.py",
    SELF_DIR / "validacao_acuracia.py",
]

# Opcionais (arquivo compactado, etc.) → _instaladores
OPCIONAIS_MISTOS_PARA_INSTALADORES = [
    SELF_DIR / "Cummins Chillers Dashboard.rar",
]

# === [Seção inst-030: Logger] ==============================================
# Objetivo:
#     Configurar logging no mesmo padrão do utils (arquivo + console).
# ============================================================================
def _init_logger() -> Path:
    DEST_LOGS.mkdir(parents=True, exist_ok=True)
    log_path = DEST_LOGS / f"instalador_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"),
                  logging.StreamHandler()]
    )
    logging.info("Logger inicializado.")
    return log_path

# === [Seção inst-040: Utilidades de cópia] =================================
# Objetivo:
#     Copiar arquivo ou pasta, registrando sucesso/falha e removendo a origem.
# ============================================================================
def _copy_file(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Arquivo não encontrado (ignorado): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        logging.info(f"Cópia concluída: {src} -> {dst}")
        if remove_src:
            try:
                src.unlink()
                logging.info(f"Origem removida: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar arquivo {src}: {e}")
        return False

def _copy_tree(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Pasta não encontrada (ignorada): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        if dst.exists():
            # mescla conteúdo quando destino já existe
            for root, _, files in os.walk(src):
                rel = Path(root).relative_to(src)
                target_root = dst / rel
                target_root.mkdir(parents=True, exist_ok=True)
                for fn in files:
                    s = Path(root) / fn
                    d = target_root / fn
                    shutil.copy2(s, d)
            logging.info(f"Conteúdo mesclado: {src} -> {dst}")
        else:
            shutil.copytree(src, dst)
            logging.info(f"Diretório copiado: {src} -> {dst}")
        if remove_src:
            try:
                shutil.rmtree(src)
                logging.info(f"Diretório de origem removido: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover diretório de origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar pasta {src}: {e}")
        return False

# === [Seção inst-050: Estrutura de pastas] =================================
# Objetivo:
#     Criar a estrutura mínima do projeto (SEM criar _build).
# ============================================================================
def _criar_estrutura_minima():
    logging.info("Criando pastas do projeto…")
    for p in [
        DEST_EXECUTORES, DEST_INSTALADORES, DEST_LOGS,
        DEST_CUMMINS, DEST_TRANSICOES, DEST_INMET, DEST_SIS_ELETRICO
    ]:
        p.mkdir(parents=True, exist_ok=True)
        logging.info(f"Pasta pronto: {p}")

# === [Seção inst-060: Execução principal] ==================================
# Objetivo:
#     Validar essenciais, copiar opcionais e concluir instalação.
# ============================================================================
def main():
    _init_logger()
    logging.info("=== Instalador do Cummins Chillers Dashboard ===")

    # 1) Estrutura
    _criar_estrutura_minima()

    # 2) Verifica essenciais
    faltando = [str(p) for p in ESSENCIAIS_ARQUIVOS if not p.exists()]
    if faltando:
        logging.error("Artefatos essenciais ausentes:")
        for f in faltando:
            logging.error(f" - {f}")
        logging.error("Instalação abortada. Coloque o 'AtualizarCumminsDashboard.exe' ao lado deste instalador e execute novamente.")
        sys.exit(1)

    # 3) Copia o executável principal para a RAIZ do projeto (não vai para _executores)
    for f in ESSENCIAIS_ARQUIVOS:
        _copy_file(f, BASE_DIR, remove_src=True)

    # 4) Opcionais → _instaladores
    for f in OPCIONAIS_ARQUIVOS_PARA_INSTALADORES:
        _copy_file(f, DEST_INSTALADORES, remove_src=True)
    for item in OPCIONAIS_MISTOS_PARA_INSTALADORES:
        if item.is_dir():
            _copy_tree(item, DEST_INSTALADORES, remove_src=True)
        else:
            _copy_file(item, DEST_INSTALADORES, remove_src=True)

    # 5) Opcionais → _executores
    for f in OPCIONAIS_ARQUIVOS_PARA_EXECUTORES:
        if f.is_dir():
            _copy_tree(f, DEST_EXECUTORES, remove_src=True)
        else:
            _copy_file(f, DEST_EXECUTORES, remove_src=True)

    # 6) Finalização
    logging.info("Instalação finalizada com sucesso.")
    logging.info(f"Executável principal disponível em: {BASE_DIR / 'AtualizarCumminsDashboard.exe'}")
    logging.info("Você pode executar manualmente agora, se quiser.")

    # Opcional: tentar iniciar automaticamente
    try:
        os.startfile(BASE_DIR / "AtualizarCumminsDashboard.exe")
        logging.info("Inicialização automática disparada: AtualizarCumminsDashboard.exe")
    except Exception as e:
        logging.warning(f"Não foi possível iniciar automaticamente: {e}")

if __name__ == "__main__":
    main()
# Dashboard_install_dependencies.py
# Instalador resiliente (padrão de logs igual ao utils):
# - Exige apenas AtualizarCumminsDashboard.exe ao lado do instalador.
# - Cria a estrutura mínima de pastas (sem _build).
# - Copia/move itens opcionais para os destinos corretos e remove a origem após copiar.
# - Mantém o executável principal no diretório raiz do projeto (não vai para _executores).
#
# Formato de log: "%(asctime)s - %(levelname)s - %(message)s"
# Exemplos:
#   2025-08-27 21:10:00 - INFO - Pasta pronta: C:\Cummins Chillers Dashboard\_logs
#   2025-08-27 21:10:01 - ERRO - Artefatos essenciais ausentes:
#   2025-08-27 21:10:02 - INFO - Cópia concluída: X -> Y

import os
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime

# === [Seção inst-010: Configuração base e destinos] =========================
# Objetivo:
#     Definir diretório-base e todas as pastas-alvo da instalação.
# ============================================================================
BASE_DIR = Path(r"C:\Cummins Chillers Dashboard")
DEST_EXECUTORES   = BASE_DIR / "_executores"
DEST_INSTALADORES = BASE_DIR / "_instaladores"
DEST_LOGS         = BASE_DIR / "_logs"
DEST_CUMMINS      = BASE_DIR / "dados_cummins"
DEST_TRANSICOES   = DEST_CUMMINS / "_transicoes"
DEST_INMET        = BASE_DIR / "dados_inmet"
DEST_SIS_ELETRICO = BASE_DIR / "dados_sistema_eletrico_brasil"

# Diretório onde o instalador está rodando (arquivos de origem)
SELF_DIR = Path(sys.argv[0]).resolve().parent

# === [Seção inst-020: Artefatos essenciais/opcionais] ======================
# Objetivo:
#     Declarar o que é obrigatório e o que é opcional copiar.
# ============================================================================
# Essencial: se faltar, aborta.
ESSENCIAIS_ARQUIVOS = [
    SELF_DIR / "AtualizarCumminsDashboard.exe",
]

# Opcionais (se existir, copia para _instaladores)
OPCIONAIS_ARQUIVOS_PARA_INSTALADORES = [
    SELF_DIR / "CumminsDashboardInstaller.exe",   # se você também distribuir esta cópia
    SELF_DIR / "python-3.13.5-amd64.exe",         # instalador do Python (se enviado)
    SELF_DIR / "Dashboard_install_dependencies.py"
]

# Opcionais (se existir, copia para _executores)
OPCIONAIS_ARQUIVOS_PARA_EXECUTORES = [
    SELF_DIR / "baixar_dados.py",
    SELF_DIR / "estimativas_dados.py",
    SELF_DIR / "tratamento_dados.py",
    SELF_DIR / "utils.py",
    SELF_DIR / "validacao_acuracia.py",
]

# Opcionais (arquivo compactado, etc.) → _instaladores
OPCIONAIS_MISTOS_PARA_INSTALADORES = [
    SELF_DIR / "Cummins Chillers Dashboard.rar",
]

# === [Seção inst-030: Logger] ==============================================
# Objetivo:
#     Configurar logging no mesmo padrão do utils (arquivo + console).
# ============================================================================
def _init_logger() -> Path:
    DEST_LOGS.mkdir(parents=True, exist_ok=True)
    log_path = DEST_LOGS / f"instalador_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"),
                  logging.StreamHandler()]
    )
    logging.info("Logger inicializado.")
    return log_path

# === [Seção inst-040: Utilidades de cópia] =================================
# Objetivo:
#     Copiar arquivo ou pasta, registrando sucesso/falha e removendo a origem.
# ============================================================================
def _copy_file(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Arquivo não encontrado (ignorado): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        logging.info(f"Cópia concluída: {src} -> {dst}")
        if remove_src:
            try:
                src.unlink()
                logging.info(f"Origem removida: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar arquivo {src}: {e}")
        return False

def _copy_tree(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Pasta não encontrada (ignorada): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        if dst.exists():
            # mescla conteúdo quando destino já existe
            for root, _, files in os.walk(src):
                rel = Path(root).relative_to(src)
                target_root = dst / rel
                target_root.mkdir(parents=True, exist_ok=True)
                for fn in files:
                    s = Path(root) / fn
                    d = target_root / fn
                    shutil.copy2(s, d)
            logging.info(f"Conteúdo mesclado: {src} -> {dst}")
        else:
            shutil.copytree(src, dst)
            logging.info(f"Diretório copiado: {src} -> {dst}")
        if remove_src:
            try:
                shutil.rmtree(src)
                logging.info(f"Diretório de origem removido: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover diretório de origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar pasta {src}: {e}")
        return False

# === [Seção inst-050: Estrutura de pastas] =================================
# Objetivo:
#     Criar a estrutura mínima do projeto (SEM criar _build).
# ============================================================================
def _criar_estrutura_minima():
    logging.info("Criando pastas do projeto…")
    for p in [
        DEST_EXECUTORES, DEST_INSTALADORES, DEST_LOGS,
        DEST_CUMMINS, DEST_TRANSICOES, DEST_INMET, DEST_SIS_ELETRICO
    ]:
        p.mkdir(parents=True, exist_ok=True)
        logging.info(f"Pasta pronto: {p}")

# === [Seção inst-060: Execução principal] ==================================
# Objetivo:
#     Validar essenciais, copiar opcionais e concluir instalação.
# ============================================================================
def main():
    _init_logger()
    logging.info("=== Instalador do Cummins Chillers Dashboard ===")

    # 1) Estrutura
    _criar_estrutura_minima()

    # 2) Verifica essenciais
    faltando = [str(p) for p in ESSENCIAIS_ARQUIVOS if not p.exists()]
    if faltando:
        logging.error("Artefatos essenciais ausentes:")
        for f in faltando:
            logging.error(f" - {f}")
        logging.error("Instalação abortada. Coloque o 'AtualizarCumminsDashboard.exe' ao lado deste instalador e execute novamente.")
        sys.exit(1)

    # 3) Copia o executável principal para a RAIZ do projeto (não vai para _executores)
    for f in ESSENCIAIS_ARQUIVOS:
        _copy_file(f, BASE_DIR, remove_src=True)

    # 4) Opcionais → _instaladores
    for f in OPCIONAIS_ARQUIVOS_PARA_INSTALADORES:
        _copy_file(f, DEST_INSTALADORES, remove_src=True)
    for item in OPCIONAIS_MISTOS_PARA_INSTALADORES:
        if item.is_dir():
            _copy_tree(item, DEST_INSTALADORES, remove_src=True)
        else:
            _copy_file(item, DEST_INSTALADORES, remove_src=True)

    # 5) Opcionais → _executores
    for f in OPCIONAIS_ARQUIVOS_PARA_EXECUTORES:
        if f.is_dir():
            _copy_tree(f, DEST_EXECUTORES, remove_src=True)
        else:
            _copy_file(f, DEST_EXECUTORES, remove_src=True)

    # 6) Finalização
    logging.info("Instalação finalizada com sucesso.")
    logging.info(f"Executável principal disponível em: {BASE_DIR / 'AtualizarCumminsDashboard.exe'}")
    logging.info("Você pode executar manualmente agora, se quiser.")

    # Opcional: tentar iniciar automaticamente
    try:
        os.startfile(BASE_DIR / "AtualizarCumminsDashboard.exe")
        logging.info("Inicialização automática disparada: AtualizarCumminsDashboard.exe")
    except Exception as e:
        logging.warning(f"Não foi possível iniciar automaticamente: {e}")

if __name__ == "__main__":
    main()
# Dashboard_install_dependencies.py
# Instalador resiliente (padrão de logs igual ao utils):
# - Exige apenas AtualizarCumminsDashboard.exe ao lado do instalador.
# - Cria a estrutura mínima de pastas (sem _build).
# - Copia/move itens opcionais para os destinos corretos e remove a origem após copiar.
# - Mantém o executável principal no diretório raiz do projeto (não vai para _executores).
#
# Formato de log: "%(asctime)s - %(levelname)s - %(message)s"
# Exemplos:
#   2025-08-27 21:10:00 - INFO - Pasta pronta: C:\Cummins Chillers Dashboard\_logs
#   2025-08-27 21:10:01 - ERRO - Artefatos essenciais ausentes:
#   2025-08-27 21:10:02 - INFO - Cópia concluída: X -> Y

import os
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime

# === [Seção inst-010: Configuração base e destinos] =========================
# Objetivo:
#     Definir diretório-base e todas as pastas-alvo da instalação.
# ============================================================================
BASE_DIR = Path(r"C:\Cummins Chillers Dashboard")
DEST_EXECUTORES   = BASE_DIR / "_executores"
DEST_INSTALADORES = BASE_DIR / "_instaladores"
DEST_LOGS         = BASE_DIR / "_logs"
DEST_CUMMINS      = BASE_DIR / "dados_cummins"
DEST_TRANSICOES   = DEST_CUMMINS / "_transicoes"
DEST_INMET        = BASE_DIR / "dados_inmet"
DEST_SIS_ELETRICO = BASE_DIR / "dados_sistema_eletrico_brasil"

# Diretório onde o instalador está rodando (arquivos de origem)
SELF_DIR = Path(sys.argv[0]).resolve().parent

# === [Seção inst-020: Artefatos essenciais/opcionais] ======================
# Objetivo:
#     Declarar o que é obrigatório e o que é opcional copiar.
# ============================================================================
# Essencial: se faltar, aborta.
ESSENCIAIS_ARQUIVOS = [
    SELF_DIR / "AtualizarCumminsDashboard.exe",
]

# Opcionais (se existir, copia para _instaladores)
OPCIONAIS_ARQUIVOS_PARA_INSTALADORES = [
    SELF_DIR / "CumminsDashboardInstaller.exe",   # se você também distribuir esta cópia
    SELF_DIR / "python-3.13.5-amd64.exe",         # instalador do Python (se enviado)
    SELF_DIR / "Dashboard_install_dependencies.py"
]

# Opcionais (se existir, copia para _executores)
OPCIONAIS_ARQUIVOS_PARA_EXECUTORES = [
    SELF_DIR / "baixar_dados.py",
    SELF_DIR / "estimativas_dados.py",
    SELF_DIR / "tratamento_dados.py",
    SELF_DIR / "utils.py",
    SELF_DIR / "validacao_acuracia.py",
]

# Opcionais (arquivo compactado, etc.) → _instaladores
OPCIONAIS_MISTOS_PARA_INSTALADORES = [
    SELF_DIR / "Cummins Chillers Dashboard.rar",
]

# === [Seção inst-030: Logger] ==============================================
# Objetivo:
#     Configurar logging no mesmo padrão do utils (arquivo + console).
# ============================================================================
def _init_logger() -> Path:
    DEST_LOGS.mkdir(parents=True, exist_ok=True)
    log_path = DEST_LOGS / f"instalador_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"),
                  logging.StreamHandler()]
    )
    logging.info("Logger inicializado.")
    return log_path

# === [Seção inst-040: Utilidades de cópia] =================================
# Objetivo:
#     Copiar arquivo ou pasta, registrando sucesso/falha e removendo a origem.
# ============================================================================
def _copy_file(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Arquivo não encontrado (ignorado): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        logging.info(f"Cópia concluída: {src} -> {dst}")
        if remove_src:
            try:
                src.unlink()
                logging.info(f"Origem removida: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar arquivo {src}: {e}")
        return False

def _copy_tree(src: Path, dst_dir: Path, remove_src: bool = True) -> bool:
    if not src.exists():
        logging.info(f"Pasta não encontrada (ignorada): {src}")
        return False
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        if dst.exists():
            # mescla conteúdo quando destino já existe
            for root, _, files in os.walk(src):
                rel = Path(root).relative_to(src)
                target_root = dst / rel
                target_root.mkdir(parents=True, exist_ok=True)
                for fn in files:
                    s = Path(root) / fn
                    d = target_root / fn
                    shutil.copy2(s, d)
            logging.info(f"Conteúdo mesclado: {src} -> {dst}")
        else:
            shutil.copytree(src, dst)
            logging.info(f"Diretório copiado: {src} -> {dst}")
        if remove_src:
            try:
                shutil.rmtree(src)
                logging.info(f"Diretório de origem removido: {src}")
            except Exception as e:
                logging.warning(f"Não foi possível remover diretório de origem: {src} ({e})")
        return True
    except Exception as e:
        logging.error(f"Falha ao copiar pasta {src}: {e}")
        return False

# === [Seção inst-050: Estrutura de pastas] =================================
# Objetivo:
#     Criar a estrutura mínima do projeto (SEM criar _build).
# ============================================================================
def _criar_estrutura_minima():
    logging.info("Criando pastas do projeto…")
    for p in [
        DEST_EXECUTORES, DEST_INSTALADORES, DEST_LOGS,
        DEST_CUMMINS, DEST_TRANSICOES, DEST_INMET, DEST_SIS_ELETRICO
    ]:
        p.mkdir(parents=True, exist_ok=True)
        logging.info(f"Pasta pronto: {p}")

# === [Seção inst-060: Execução principal] ==================================
# Objetivo:
#     Validar essenciais, copiar opcionais e concluir instalação.
# ============================================================================
def main():
    _init_logger()
    logging.info("=== Instalador do Cummins Chillers Dashboard ===")

    # 1) Estrutura
    _criar_estrutura_minima()

    # 2) Verifica essenciais
    faltando = [str(p) for p in ESSENCIAIS_ARQUIVOS if not p.exists()]
    if faltando:
        logging.error("Artefatos essenciais ausentes:")
        for f in faltando:
            logging.error(f" - {f}")
        logging.error("Instalação abortada. Coloque o 'AtualizarCumminsDashboard.exe' ao lado deste instalador e execute novamente.")
        sys.exit(1)

    # 3) Copia o executável principal para a RAIZ do projeto (não vai para _executores)
    for f in ESSENCIAIS_ARQUIVOS:
        _copy_file(f, BASE_DIR, remove_src=True)

    # 4) Opcionais → _instaladores
    for f in OPCIONAIS_ARQUIVOS_PARA_INSTALADORES:
        _copy_file(f, DEST_INSTALADORES, remove_src=True)
    for item in OPCIONAIS_MISTOS_PARA_INSTALADORES:
        if item.is_dir():
            _copy_tree(item, DEST_INSTALADORES, remove_src=True)
        else:
            _copy_file(item, DEST_INSTALADORES, remove_src=True)

    # 5) Opcionais → _executores
    for f in OPCIONAIS_ARQUIVOS_PARA_EXECUTORES:
        if f.is_dir():
            _copy_tree(f, DEST_EXECUTORES, remove_src=True)
        else:
            _copy_file(f, DEST_EXECUTORES, remove_src=True)

    # 6) Finalização
    logging.info("Instalação finalizada com sucesso.")
    logging.info(f"Executável principal disponível em: {BASE_DIR / 'AtualizarCumminsDashboard.exe'}")
    logging.info("Você pode executar manualmente agora, se quiser.")

    # Opcional: tentar iniciar automaticamente
    try:
        os.startfile(BASE_DIR / "AtualizarCumminsDashboard.exe")
        logging.info("Inicialização automática disparada: AtualizarCumminsDashboard.exe")
    except Exception as e:
        logging.warning(f"Não foi possível iniciar automaticamente: {e}")

if __name__ == "__main__":
    main()
