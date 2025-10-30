# === META =========================================================
# Módulo: baixar_inmet (INMET - Mirante SP)
# Versão: v1.0.0
# Fonte oficial: portal.inmet.gov.br (ZIPs anuais)
# Saída chave: Dados_Tratados_INMET_Mirante_De_São_Paulo.csv
# ==================================================================

import logging
import time
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

from _executores.utils import DADOS_INMET_DIR, extrair_data_final, backup_saida

# === [Função compartilhada: Download robusto com retentativas] =========================
def baixar_arquivo_com_retentativas(url, destino, tentativas=3, delay=5, verificar_ssl=True):
    """Baixa um arquivo com retentativas e verificação básica."""
    UA = {"User-Agent": "CumminsChillers/1.0 (+diagnostics)"}
    for tentativa in range(1, tentativas + 1):
        try:
            with httpx.stream("GET", url, timeout=60.0, verify=verificar_ssl, headers=UA) as resposta:
                if resposta.status_code == 200:
                    with open(destino, "wb") as f:
                        for bloco in resposta.iter_bytes():
                            f.write(bloco)
                    if destino.stat().st_size <= 0:
                        raise RuntimeError("Arquivo baixado com tamanho 0")
                    logging.info(f"Sucesso ao baixar {destino.name}")
                    return True
                else:
                    logging.warning(f"HTTP {resposta.status_code} ao baixar {url}")
        except Exception as e:
            logging.warning(f"Tentativa {tentativa} falhou: {e}")
            time.sleep(delay)
    return False

# === [Script baixar_inmet: INMET — Mirante de São Paulo] ================================
# Objetivo:
#     Baixar, extrair, deduplicar e consolidar séries históricas do INMET para
#     a estação "São Paulo - Mirante" a partir de pacotes anuais ZIP.
# Fluxo:
#     baixar_inmet ->
#         baixar_e_processar_anos_dados_clima_mirante_sao_paulo ->
#         tratar_duplicatas_ano_atual_dados_clima_mirante_sao_paulo ->
#         consolidar_dados_clima_mirante_sao_paulo
# ============================================================================


def baixar_e_processar_anos_dados_clima_mirante_sao_paulo(ano: int, destino: Path, padrao_mirante: str):
    """Baixa o ZIP do ano, extrai e remove arquivos que não são da estação-alvo."""
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
    zip_path = destino / f"{ano}.zip"
    ano_atual = datetime.now().year
    zip_existe = zip_path.exists()
    precisa_extrair = True

    if zip_existe and ano != ano_atual:
        logging.info(f"ZIP de {ano} já existe. Pulando download.")
        arquivos_csv = list(destino.glob(f"*{ano}*.csv"))
        if any(padrao_mirante in arquivo.name for arquivo in arquivos_csv):
            logging.info(f"Arquivos de {ano} já extraídos. Pulando extração.\n")
            precisa_extrair = False
    else:
        if zip_existe:
            try:
                zip_path.unlink()
            except Exception as e:
                logging.warning(f"Falha ao remover ZIP antigo {zip_path.name}: {e}")
        logging.info(f"Baixando ZIP de {ano}...")
        sucesso = baixar_arquivo_com_retentativas(url, zip_path, verificar_ssl=True)
        if not sucesso:
            logging.warning(f"Falha no download de {ano}.")
        else:
            logging.info(f"Download concluído para {ano}.")

    if precisa_extrair and zip_path.exists():
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(path=destino)
            logging.info(f"Arquivos de {ano} extraídos com sucesso.\n")
        except Exception as e:
            logging.warning(f"Erro ao extrair ZIP de {ano}: {e}")

        arquivos_csv = list(destino.glob("**/*.csv"))
        for arquivo in arquivos_csv:
            if padrao_mirante not in arquivo.name:
                try:
                    arquivo.unlink()
                except Exception as e:
                    logging.warning(f"Falha ao remover {arquivo}: {e}")


def tratar_duplicatas_ano_atual_dados_clima_mirante_sao_paulo(destino: Path, padrao_mirante: str, ano_atual: int):
    """Remove CSVs duplicados do ano corrente, mantendo o mais recente."""
    arquivos_csv = list(destino.glob("*.csv"))
    mirantes = []
    for arq in arquivos_csv:
        if padrao_mirante in arq.name:
            data_final = extrair_data_final(arq.name)
            if data_final.year == ano_atual:
                mirantes.append((data_final, arq))
    if len(mirantes) > 1:
        mirantes.sort(reverse=True)
        for _, arq in mirantes[1:]:
            try:
                arq.unlink()
            except Exception as e:
                logging.warning(f"Falha ao remover duplicata {arq}: {e}")


def consolidar_dados_clima_mirante_sao_paulo(destino: Path, padrao_mirante: str):
    """Consolida CSVs da estação em um único arquivo tratado."""
    logging.info("Tratando dados do INMET...")
    arquivos = list(destino.glob("*.csv"))
    dfs = []
    for arquivo in arquivos:
        if padrao_mirante in arquivo.name:
            try:
                df = pd.read_csv(arquivo, encoding='latin1', sep=';', skiprows=8)
                dfs.append(df)
            except Exception as e:
                logging.warning(f"Erro ao ler {arquivo.name}: {e}")

    if dfs:
        consolidado = pd.concat(dfs, ignore_index=True)

        def norm(s):
            return (s.lower()
                      .replace("ã", "a").replace("á", "a").replace("â", "a")
                      .replace("é", "e").replace("ê", "e").replace("í", "i")
                      .replace("ó", "o").replace("ô", "o").replace("õ", "o")
                      .replace("ç", "c").strip())
        cols_map = {c: norm(c) for c in consolidado.columns}
        consolidado.columns = [cols_map[c] for c in consolidado.columns]

        alvo_data = [c for c in consolidado.columns if "data" == c]
        alvo_hora = [c for c in consolidado.columns if "hora utc" in c]
        alvo_prec = [c for c in consolidado.columns if "precipitacao total" in c]
        alvo_temp = [c for c in consolidado.columns if "temperatura do ar - bulbo seco" in c]
        colunas_desejadas = [
            alvo_data[0] if alvo_data else None,
            alvo_hora[0] if alvo_hora else None,
            alvo_prec[0] if alvo_prec else None,
            alvo_temp[0] if alvo_temp else None,
        ]
        colunas_desejadas = [c for c in colunas_desejadas if c is not None]
        if not colunas_desejadas:
            logging.warning("Colunas esperadas não encontradas no INMET. Verifique o layout.\n")
            return

        filtrado = consolidado[colunas_desejadas]
        caminho_final = destino / "Dados_Tratados_INMET_Mirante_De_São_Paulo.csv"
        if caminho_final.exists():
            backup_saida(caminho_final)
        try:
            filtrado.to_csv(caminho_final, sep=';', index=False, encoding='latin1')
            logging.info(f"Dados consolidados e salvos com sucesso em: {caminho_final}\n")
        except Exception as e:
            logging.warning(f"Falha ao salvar consolidação INMET: {e}")
    else:
        logging.warning("Nenhum dado foi consolidado. Verifique os arquivos CSV disponíveis.\n")


def baixar_inmet():
    """Orquestra download, extração e consolidação dos dados climáticos (INMET - Mirante SP)."""
    logging.info("Iniciando atualização dos dados do clima (INMET - Mirante São Paulo)...\n")
    destino = DADOS_INMET_DIR
    destino.mkdir(parents=True, exist_ok=True)
    ano_atual = datetime.now().year
    padrao_mirante = "INMET_SE_SP_A701_SAO PAULO - MIRANTE_"

    for ano in range(2020, ano_atual + 1):
        baixar_e_processar_anos_dados_clima_mirante_sao_paulo(ano, destino, padrao_mirante)

    tratar_duplicatas_ano_atual_dados_clima_mirante_sao_paulo(destino, padrao_mirante, ano_atual)
    consolidar_dados_clima_mirante_sao_paulo(destino, padrao_mirante)
