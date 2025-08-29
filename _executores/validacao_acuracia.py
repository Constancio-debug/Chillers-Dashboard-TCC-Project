import logging
import pandas as pd
import numpy as np
from pathlib import Path
from _executores.utils import DADOS_CUMMINS_DIR, MESES_ORDEM, backup_saida


# === [Seção validacao_acuraria-010: Validação de acurácia das estimativas] ===
# Objetivo:
#     Comparar valores reais (histórico tratado) com estimativas e gerar um
#     histórico de precisão (erros percentuais e métricas auxiliares).
# Fluxo:
#     atualizar_historico_acuracia
# Entradas:
#     - Excel: BASE_DIR/dados_cummins/Tabela_Historico_Tratada.xlsx
#     - CSV  : BASE_DIR/dados_cummins/Estimativa_Consumo_Consolidado.csv
# Saídas:
#     - CSV  : BASE_DIR/dados_cummins/Historico_Precisao_Estimativas.csv (utf-8-sig; ';')
# Contratos:
#     - Merge por ['Ano','Mês']; meses capitalizados; evita divisão por zero (0 → NaN)
# Observações:
#     Backups são feitos antes de sobrescrever. Colunas de saída são renomeadas
#     para rótulos claros (Real/Estimado/Erro).
# ============================================================================


# === [Seção validacao_acuraria-020: Definições de métricas] =================
# Convenções:
#     - Todas as porcentagens são em %, não frações.
#     - Sinal do erro:
#         * Erro > 0  → estimativa acima do real (superestimação)
#         * Erro < 0  → estimativa abaixo do real (subestimação)
#     - Para evitar divisão por zero, denominadores iguais a 0 são tratados como NaN.
#
# Métricas:
#     1) Erro Consumo (%)
#         Erro_Consumo = ((Consumo_Estimado - Consumo_Real) / Consumo_Real) * 100
#
#     2) Erro Horas (%)
#         Erro_Horas = ((Horas_Estimadas - Horas_Reais) / Horas_Reais) * 100
#
#     3) Erro Temperatura (%)
#         Erro_Temp = ((Temp_Estimada - Temp_Real) / Temp_Real) * 100
#
#     4) Fator de Utilização (adimensional)
#         FU = Consumo_Real / (Potência_Média_kW * Horas_Reais)
#         Interpretação aproximada:
#             ~1.0 → coerente com Consumo ≈ Potência_Média * Horas
#             <1.0 → Consumo menor que o estimado por Pm * h (ex.: ociosidade, COP alto)
#             >1.0 → Consumo maior (ex.: picos, perdas não capturadas)
# ============================================================================


def atualizar_historico_acuracia() -> None:
	"""Gera/atualiza o CSV de precisão das estimativas comparando Real vs. Estimado.

	Ver fórmulas e convenções na Seção validacao_acuraria-020.
	"""
	logging.info("Iniciando atualização do histórico de acurácia das estimativas...")

	pasta_dados = DADOS_CUMMINS_DIR
	caminho_real = pasta_dados / "Tabela_Historico_Tratada.xlsx"
	caminho_estimado = pasta_dados / "Estimativa_Consumo_Consolidado.csv"
	caminho_saida = pasta_dados / "Historico_Precisao_Estimativas.csv"

	if not caminho_real.exists():
		logging.warning(f"Arquivo não encontrado: {caminho_real.name}")
		return
	if not caminho_estimado.exists():
		logging.warning(f"Arquivo não encontrado: {caminho_estimado.name}")
		return

	try:
		df_real = pd.read_excel(caminho_real, sheet_name=0, engine="openpyxl")
		df_real["Mês"] = df_real["Mês"].str.capitalize().str.strip()

		df_estimado = pd.read_csv(caminho_estimado, sep=";", decimal=",")
		df_estimado["Mês"] = df_estimado["Mês"].str.capitalize().str.strip()

		df_comparado = pd.merge(
			df_real,
			df_estimado,
			on=["Ano", "Mês"],
			how="inner",
			suffixes=("_Real", "_Estimado"),
		)

		# Erros percentuais (protege contra divisão por zero)
		den_c = df_comparado["Consumo (KWh)"].replace(0, np.nan)
		df_comparado["Erro Consumo (%)"] = (
			(df_comparado["Consumo Esperado (KWh)"] - df_comparado["Consumo (KWh)"]) / den_c
		) * 100

		den_h = df_comparado["Horas Trabalhadas (h)"].replace(0, np.nan)
		df_comparado["Erro Horas (%)"] = (
			(df_comparado["Horas Estimadas (h)"] - df_comparado["Horas Trabalhadas (h)"]) / den_h
		) * 100

		den_t = df_comparado["Temp. Média (ºC)"].replace(0, np.nan)
		df_comparado["Erro Temp (%)"] = (
			(df_comparado["Temp Estimada (ºC)"] - df_comparado["Temp. Média (ºC)"]) / den_t
		) * 100

		# Fator de utilização (FU) = Consumo / (Potência Média * Horas)
		den_fu = (df_comparado["Potência Média (KW)"] * df_comparado["Horas Trabalhadas (h)"]).replace(0, np.nan)
		df_comparado["Fator Utilizacao"] = df_comparado["Consumo (KWh)"] / den_fu

		# Seleção e renomeação de colunas para saída
		colunas_saida = {
			"Ano": "Ano",
			"Mês": "Mês",
			"Tipo": "Tipo",
			"Consumo (KWh)": "Consumo Real (KWh)",
			"Consumo Esperado (KWh)": "Consumo Estimado (KWh)",
			"Erro Consumo (%)": "Erro Consumo (%)",
			"Horas Trabalhadas (h)": "Horas Reais (h)",
			"Horas Estimadas (h)": "Horas Estimadas (h)",
			"Erro Horas (%)": "Erro Horas (%)",
			"Temp. Média (ºC)": "Temp Real (ºC)",
			"Temp Estimada (ºC)": "Temp Estimada (ºC)",
			"Erro Temp (%)": "Erro Temp (%)",
			"Potência Média (KW)": "Potência Média (KW)",
			"Fator Utilizacao": "Fator Utilizacao",
			"COP Médio": "COP Médio",
			"CO2 Emitido (Kg)": "CO2 Emitido (Kg)",
		}
		df_final = df_comparado[list(colunas_saida.keys())].rename(columns=colunas_saida)
		df_final["Mês"] = pd.Categorical(df_final["Mês"], categories=MESES_ORDEM, ordered=True)
		df_final.sort_values(by=["Ano", "Mês"], inplace=True)

		if caminho_saida.exists():
			backup_saida(caminho_saida)
		df_final.to_csv(caminho_saida, sep=";", decimal=",", index=False, encoding="utf-8-sig")
		logging.info(f"Histórico de acurácia salvo com sucesso em: {caminho_saida}\n")

	except Exception as e:
		logging.warning(f"Erro ao atualizar histórico de acurácia: {e}\n")