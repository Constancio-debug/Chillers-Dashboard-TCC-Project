# === META =========================================================
# Módulo: tratamento_consumo
# Versão: v1.0.0
# Regras fixas: ano 2022 → valor KWh = 0,5
# Saídas: Valor_KWh_Ano.xlsx, Consumo_Total_Cummins.xlsx
# ==================================================================

import logging
import shutil
import pandas as pd
from pathlib import Path

from _executores.utils import (
	BASE_DIR,
	backup_saida,
	DADOS_CUMMINS_DIR,
	MES_NUM_PARA_PT,
)

# === [Seção tratamento_consumo-010: KWh] ===================================
# Objetivo:
#     Processar arquivo KWH.xlsx e atualizar/gerar Valor_KWh_Ano.xlsx.
# Fluxo:
#     tratar_arquivo_kwh_para_dados_cummins
# ==========================================================================
def tratar_arquivo_kwh_para_dados_cummins():
	"""Processa 'KWH.xlsx' e atualiza 'Valor_KWh_Ano.xlsx' (dedup/ordenado)."""
	logging.info("Tratando arquivo KWH...")

	pasta_destino = DADOS_CUMMINS_DIR
	pasta_destino.mkdir(parents=True, exist_ok=True)

	arquivo_xlsx = BASE_DIR / "KWH.xlsx"
	destino = pasta_destino / "Valor_KWh_Ano.xlsx"

	if not arquivo_xlsx.exists():
		df_vazio = pd.DataFrame(columns=["Ano", "Valor"])
		df_vazio.to_excel(arquivo_xlsx, index=False, engine="openpyxl")
		logging.info("Arquivo 'KWH.xlsx' criado com os cabeçalhos 'Ano' e 'Valor'.\n")

	try:
		df_entrada = pd.read_excel(arquivo_xlsx, engine="openpyxl")

		if df_entrada.empty or df_entrada.shape[1] < 2:
			logging.warning(f"O arquivo '{arquivo_xlsx.name}' não possui dados válidos.\n")
			return

		df_entrada.columns = ["Ano", "Valor"]
		df_entrada = df_entrada.dropna()

		# regra fixa do seu fluxo
		df_entrada = df_entrada[df_entrada["Ano"] != 2022]
		df_fixo = pd.DataFrame([{"Ano": 2022, "Valor": "0,5"}])
		df_entrada = pd.concat([df_entrada, df_fixo], ignore_index=True)

		if destino.exists():
			df_existente = pd.read_excel(destino, engine="openpyxl")
			df_total = pd.concat([df_existente, df_entrada], ignore_index=True)
		else:
			df_total = df_entrada

		df_total.drop_duplicates(subset=["Ano"], keep="last", inplace=True)
		df_total.sort_values("Ano", inplace=True)

		if destino.exists():
			backup_saida(destino)
		with pd.ExcelWriter(destino, engine='openpyxl') as writer:
			df_total.to_excel(writer, sheet_name="KWH", index=False)

		logging.info(f"Arquivo '{arquivo_xlsx.name}' processado e anexado com sucesso em '{destino.name}'.")
		# limpa entrada
		df_limpo = pd.DataFrame(columns=["Ano", "Valor"])
		df_limpo.to_excel(arquivo_xlsx, index=False, engine="openpyxl")
		logging.info(f"Arquivo '{arquivo_xlsx.name}' foi limpo após o processamento.")

	except Exception as e:
		logging.warning(f"Erro ao tratar o arquivo '{arquivo_xlsx.name}': {e}")


# === [Seção tratamento_consumo-020: Consumo Total] =========================
# Objetivo:
#     Processar arquivo CONSUMO_TOTAL.xlsx e atualizar/gerar Consumo_Total_Cummins.xlsx.
# Fluxo:
#     inserir_consumo_total_cummins
# ==========================================================================
def inserir_consumo_total_cummins():
	"""Insere/atualiza planilha de Consumo Total mensal (dedup e ordenação)."""
	logging.info("Inserindo Consumo Total Cummins...")

	pasta_destino = DADOS_CUMMINS_DIR
	pasta_destino.mkdir(parents=True, exist_ok=True)

	arquivo_origem = BASE_DIR / "CONSUMO_TOTAL.xlsx"
	arquivo_destino = pasta_destino / "Consumo_Total_Cummins.xlsx"

	if not arquivo_origem.exists():
		df_vazio = pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)"])
		df_vazio.to_excel(arquivo_origem, index=False, engine="openpyxl")
		logging.info("Arquivo 'CONSUMO_TOTAL.xlsx' criado com cabeçalhos.\n")
		return

	try:
		df_entrada = pd.read_excel(arquivo_origem, engine="openpyxl")

		if df_entrada.shape[1] < 3:
			logging.warning(f"Arquivo '{arquivo_origem.name}' não possui colunas suficientes.\n")
			df_entrada = pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)"])
		else:
			df_entrada.columns = ["Ano", "Mês", "Consumo (KWh)"]
			df_entrada = df_entrada.dropna()
			df_entrada["Mês"] = pd.to_numeric(df_entrada["Mês"], errors="coerce").astype("Int64")
			df_entrada = df_entrada[df_entrada["Mês"].between(1, 12)]
			df_entrada["Consumo (KWh)"] = pd.to_numeric(
				df_entrada["Consumo (KWh)"].astype(str).str.replace(",", ".", regex=False),
				errors="coerce"
			)
			df_entrada = df_entrada[df_entrada["Consumo (KWh)"] > 0]
			df_entrada["Nome do Mês"] = df_entrada["Mês"].map(MES_NUM_PARA_PT)

		colunas_finais = ["Ano", "Mês", "Nome do Mês", "Consumo (KWh)"]
		df_entrada = df_entrada.reindex(columns=colunas_finais)

		if arquivo_destino.exists():
			df_existente = pd.read_excel(arquivo_destino, engine="openpyxl")
			df_existente = df_existente.reindex(columns=colunas_finais)
			df_total = pd.concat([df_existente, df_entrada], ignore_index=True)
		else:
			df_total = df_entrada.copy()

		if not df_total.empty:
			df_total.drop_duplicates(subset=["Ano", "Mês"], keep="last", inplace=True)
			df_total.sort_values(by=["Ano", "Mês"], inplace=True)

		if arquivo_destino.exists():
			backup_saida(arquivo_destino)
		with pd.ExcelWriter(arquivo_destino, engine='openpyxl') as writer:
			df_total.to_excel(writer, sheet_name="Consumo Total", index=False)

		logging.info(f"Arquivo '{arquivo_destino.name}' atualizado com sucesso.")

		# limpa entrada
		df_limpo = pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)"])
		df_limpo.to_excel(arquivo_origem, index=False, engine="openpyxl")
		logging.info(f"Arquivo '{arquivo_origem.name}' foi limpo após o processamento.\n")

	except Exception as e:
		logging.warning(f"Erro ao tratar o arquivo '{arquivo_origem.name}': {e}\n")
