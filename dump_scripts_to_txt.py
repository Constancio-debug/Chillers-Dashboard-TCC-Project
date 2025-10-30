#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# === META =========================================================
# Script: dump_scripts_to_txt.py
# Versão: v1.0.1
# Objetivo: Consolidar Atualizar_Dados.py + _executores/**/*.py em um único TXT.
# Observação: ignora apenas _executores/__pycache__
# ==================================================================

from pathlib import Path
from datetime import datetime
import os

# ===== Config principal ======================================================
BASE_DIR = Path(r"C:/Cummins Chillers Dashboard")
ORQ_POSSIVEIS = ["Atualizar_Dados.py", "Atualizar_dados.py"]  # aceita ambas
EXECUTORES_DIR = BASE_DIR / "_executores"
PYCACHE_DIR = EXECUTORES_DIR / "__pycache__"
LOGS_DIR = BASE_DIR / "_logs"

# ============================================================================

def _ler_arquivo(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        return f"# [ERRO] Falha ao ler {p}: {e}\n"

def _coletar_orquestrador() -> list[Path]:
    for nome in ORQ_POSSIVEIS:
        p = BASE_DIR / nome
        if p.exists() and p.is_file():
            return [p]
    return []

def _coletar_executores() -> list[Path]:
    if not EXECUTORES_DIR.exists():
        return []
    files = []
    pycache_resolved = PYCACHE_DIR.resolve()
    for root, dirs, fnames in os.walk(EXECUTORES_DIR, topdown=True):
        # Poda apenas o __pycache__ específico
        dirs[:] = [d for d in dirs if (Path(root) / d).resolve() != pycache_resolved]
        for f in fnames:
            if f.lower().endswith(".py"):
                files.append(Path(root) / f)
    files.sort(key=lambda p: str(p.relative_to(EXECUTORES_DIR)).lower())
    return files

def main():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    saida = LOGS_DIR / f"Scripts_Utilizados_{ts}.txt"

    orq = _coletar_orquestrador()
    mods = _coletar_executores()
    total = len(orq) + len(mods)

    with saida.open("w", encoding="utf-8", errors="ignore") as out:
        out.write(f"Scripts utilizados - Gerado em {datetime.now():%d/%m/%Y %H:%M:%S}\n")
        out.write("=" * 78 + "\n")
        out.write(f"BASE_DIR: {BASE_DIR}\n")
        out.write(f"Total de scripts: {total}\n")
        out.write("=" * 78 + "\n\n")

        # 1) Orquestrador (se existir)
        for p in orq:
            rel = p.relative_to(BASE_DIR)
            out.write("\n" + "=" * 78 + "\n")
            out.write(f">>> INÍCIO DO SCRIPT: {rel.as_posix()}\n")
            out.write("=" * 78 + "\n\n")
            out.write(_ler_arquivo(p))
            out.write("\n\n" + "=" * 78 + "\n")
            out.write(f">>> FIM DO SCRIPT: {rel.as_posix()}\n")
            out.write("=" * 78 + "\n")

        # 2) _executores/**/*.py (ignorando apenas __pycache__)
        for p in mods:
            rel = p.relative_to(BASE_DIR)
            out.write("\n" + "=" * 78 + "\n")
            out.write(f">>> INÍCIO DO SCRIPT: {rel.as_posix()}\n")
            out.write("=" * 78 + "\n\n")
            out.write(_ler_arquivo(p))
            out.write("\n\n" + "=" * 78 + "\n")
            out.write(f">>> FIM DO SCRIPT: {rel.as_posix()}\n")
            out.write("=" * 78 + "\n")

    print(f"[ok] TXT gerado: {saida}")

if __name__ == "__main__":
    main()
