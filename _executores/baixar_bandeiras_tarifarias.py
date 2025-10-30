# === META =========================================================
# Módulo: baixar_bandeiras_tarifarias (ANEEL)
# Versão: v1.0.0
# Arquivos: bandeira_tarifaria_acionamento.csv, bandeira_tarifaria_adicional.csv
# ==================================================================

import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import httpx
import asyncio

from _executores.utils import DADOS_SISTEMA_ELETRICO_DIR

# === [Seção baixar_bandeiras-000: Download oficial de Bandeiras Tarifárias] ===
# Módulo: _executores/baixar_bandeiras_tarifarias.py (consolidado, sem execução direta)
# Objetivo:
#     Fornecer funções para baixar os CSVs oficiais de Bandeiras Tarifárias (ANEEL):
#       1) bandeira-tarifaria-acionamento.csv
#       2) bandeira-tarifaria-adicional.csv
# Saída:
#     Arquivos salvos em DADOS_SISTEMA_ELETRICO_DIR
# Contratos:
#     Timeout de 60s; HTTP 200 obrigatório; arquivo final com st_size > 0
# Observações:
#     Função de download copiada (mesmos parâmetros e tentativas).
# ==============================================================================

# Constante global: identificação do cliente HTTP para logs do servidor.
UA = {"User-Agent": "CumminsChillers/1.0 (+diagnostics)"}

# URLs oficiais (ANEEL / Dados Abertos)
URLS_BANDEIRAS = {
	"bandeira_tarifaria_acionamento.csv": (
		"https://dadosabertos.aneel.gov.br/dataset/7f43a020-6dc5-44b8-80b4-d97eaa94436c/"
		"resource/0591b8f6-fe54-437b-b72b-1aa2efd46e42/download/bandeira-tarifaria-acionamento.csv"
	),
	"bandeira_tarifaria_adicional.csv": (
		"https://dadosabertos.aneel.gov.br/dataset/7f43a020-6dc5-44b8-80b4-d97eaa94436c/"
		"resource/5879ca80-b3bd-45b1-a135-d9b77c1d5b36/download/bandeira-tarifaria-adicional.csv"
	),
}


# === [Seção bandeiras-010: Checagem de necessidade de download] ==============
# Objetivo:
#		Verificar se já existe um arquivo local "bandeira_tarifaria_adicional.csv"
#		e se o último mês disponível corresponde ao mês atual. Caso positivo,
#		não é necessário realizar novos downloads.
# Fluxo:
#		precisa_atualizar_bandeiras
# Entradas:
#		- Arquivo local (caso exista)
# Saídas:
#		- bool: True se precisa atualizar, False caso contrário
# =============================================================================
def precisa_atualizar_bandeiras(destino_base: Path) -> bool:
	arquivo_adicional = destino_base / "bandeira_tarifaria_adicional.csv"
	if not arquivo_adicional.exists():
		return True

	try:
		df = pd.read_csv(arquivo_adicional, sep=";", encoding="latin-1")
		ultima_data = pd.to_datetime(df["DatCompetencia"].iloc[-1])
		agora = datetime.now()
		if ultima_data.year == agora.year and ultima_data.month == agora.month:
			logging.info("Dados de bandeiras já estão atualizados para o mês corrente.\n")
			return False
	except Exception as e:
		logging.warning(f"Falha ao validar necessidade de atualização: {e}")
		return True

	return True


# === [Seção bandeiras-020: Download robusto assíncrono] ======================
# Objetivo:
#		Fazer download de arquivos em paralelo, reaproveitando sessão HTTP,
#		com checagem básica de integridade (tamanho > 0).
# Fluxo:
#		baixar_arquivo, baixar_todos
# Entradas:
#		- url (str), destino (Path)
# Saídas:
#		- Arquivo gravado em disco (True) ou falha (False)
# =============================================================================
async def baixar_arquivo(client: httpx.AsyncClient, url: str, destino: Path) -> bool:
	try:
		resp = await client.get(url)
		if resp.status_code == 200:
			destino.write_bytes(resp.content)
			if destino.stat().st_size <= 0:
				raise RuntimeError("Arquivo baixado com tamanho 0")
			logging.info(f"Sucesso ao baixar {destino.name}")
			return True
		else:
			logging.warning(f"HTTP {resp.status_code} ao baixar {url}")
	except Exception as e:
		logging.warning(f"Falha ao baixar {destino.name}: {e}")
	return False


async def baixar_todos(destino_base: Path):
	async with httpx.AsyncClient(timeout=60.0, headers=UA, verify=True) as client:
		tarefas = []
		for nome_arquivo, url in URLS_BANDEIRAS.items():
			destino = destino_base / nome_arquivo
			logging.info(f"Baixando {nome_arquivo}...")
			tarefas.append(baixar_arquivo(client, url, destino))
		return await asyncio.gather(*tarefas)


# === [Seção bandeiras-030: Orquestração] ====================================
# Objetivo:
#		Coordenar a checagem de necessidade e, se aplicável, os downloads.
# Fluxo:
#		baixar_csvs_bandeiras_tarifarias
# Entradas:
#		- DADOS_SISTEMA_ELETRICO_DIR
# Saídas:
#		- 2 CSVs salvos com nomes fixos (ver URLS_BANDEIRAS.keys())
# =============================================================================
def baixar_csvs_bandeiras_tarifarias():
	destino_base = DADOS_SISTEMA_ELETRICO_DIR
	destino_base.mkdir(parents=True, exist_ok=True)
	logging.info("Iniciando atualização dos dados de Bandeiras Tarifárias (ANEEL)...\n")

	if not precisa_atualizar_bandeiras(destino_base):
		return True

	resultados = asyncio.run(baixar_todos(destino_base))
	ok_total = all(resultados)

	if ok_total:
		logging.info("Atualização concluída com sucesso (bandeiras tarifárias).\n")
	else:
		logging.warning("Atualização finalizada com falhas em um ou mais arquivos.\n")

	return ok_total