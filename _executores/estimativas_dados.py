import logging
from datetime import datetime
from calendar import monthrange
from pathlib import Path
import pandas as pd
import numpy as np

from _executores.utils import DADOS_CUMMINS_DIR, MESES_ORDEM, backup_saida


# === [Seção estimativas_dados-010: Utilitários de estatística e estimativa mensal] ===
# Objetivo:
#     Fornecer helpers para média histórica corrigida (IQR), diferença percentual
#     e cálculo de faixa (min/esperado/máx) mensal com base no histórico e, se houver,
#     consumo parcial do mês corrente.
# Fluxo:
#     media_historica_corrigida -> calcular_diferenca -> estimar_consumo_mensal
# Entradas:
#     - DataFrame histórico consolidado (Tratamento) com colunas padrão
# Saídas:
#     - Valores numéricos e dicionários com estimativas mensais
# Contratos:
#     - Colunas esperadas: ['Ano','Mês','Consumo (KWh)'] quando aplicável
# ============================================================================
def media_historica_corrigida(df: pd.DataFrame, coluna: str, mes: str, ano_atual: int):
	"""Calcula média histórica corrigida por IQR e pesos lineares no tempo."""
	df_mes = df[(df["Mês"] == mes) & (df["Ano"] < ano_atual)]
	if df_mes.empty or coluna not in df_mes.columns:
		return None
	Q1 = df_mes[coluna].quantile(0.25)
	Q3 = df_mes[coluna].quantile(0.75)
	IQR = Q3 - Q1
	limite_inf = Q1 - 1.5 * IQR
	limite_sup = Q3 + 1.5 * IQR
	df_filtrado = df_mes[(df_mes[coluna] >= limite_inf) & (df_mes[coluna] <= limite_sup)].copy()
	if df_filtrado.empty:
		return None
	df_filtrado["Peso"] = df_filtrado["Ano"].apply(lambda x: max(1, (x - df_filtrado["Ano"].min()) + 1))
	media_ponderada = np.average(df_filtrado[coluna], weights=df_filtrado["Peso"])
	return media_ponderada

def calcular_diferenca(estimado, historico):
	"""Retorna (delta, tendência%) entre estimado e histórico; None se inválido."""
	if pd.notnull(estimado) and pd.notnull(historico) and historico != 0:
		delta = estimado - historico
		tendencia_pct = (delta / historico) * 100
		return round(delta, 2), round(tendencia_pct, 2)
	return None, None

def estimar_consumo_mensal(df: pd.DataFrame, mes: str, ano: int = None,
							consumo_parcial=None, dias_medidos=None, dias_do_mes=None) -> dict:
	"""Estima consumo mensal (min/esperado/máx) para um mês específico."""
	colunas_esperadas = ["Ano", "Mês", "Consumo (KWh)"]
	for col in colunas_esperadas:
		if col not in df.columns:
			return {"min": None, "esperado": None, "max": None}

	df_mes = df[df["Mês"] == mes]
	if ano is not None:
		df_mes = df_mes[df_mes["Ano"] < ano]
	if df_mes.empty:
		return {"min": None, "esperado": None, "max": None}

	consumo_medio = df_mes["Consumo (KWh)"].mean()
	consumo_std = df_mes["Consumo (KWh)"].std(ddof=0)

	if consumo_parcial is not None and dias_medidos and dias_do_mes:
		esperado = (consumo_parcial / dias_medidos) * dias_do_mes
	else:
		esperado = consumo_medio

	minimo = max(0, esperado - consumo_std)
	maximo = esperado + consumo_std

	return {
		"min": round(minimo, 2) if pd.notnull(minimo) else None,
		"esperado": round(esperado, 2) if pd.notnull(esperado) else None,
		"max": round(maximo, 2) if pd.notnull(maximo) else None
	}


# === [Seção estimativas_dados-020: Viés histórico (leitura e aplicação)] ==========
# Objetivo:
#     Ler viés médio de previsão a partir de 'Historico_Precisao_Estimativas.csv'
#     e disponibilizar viés global e por mês, quando houver dados suficientes.
# Fluxo:
#     carregar_vies_historico
# Entradas:
#     - CSV em BASE_DIR/dados_cummins/ (sep=';', decimal=',')
# Saídas:
#     - Tuple (vies_global, dict vies_mensal)
# Contratos:
#     - Usa somente linhas com Tipo == 'Real'; mínimo de observações para cálculo
# ============================================================================
def carregar_vies_historico():
	"""Carrega viés global e mensal a partir do CSV de precisão; tolerante a ausência."""
	caminho = DADOS_CUMMINS_DIR / "Historico_Precisao_Estimativas.csv"
	vies_global = None
	vies_mensal = {}
	if not caminho.exists():
		return vies_global, vies_mensal
	try:
		df_hist = pd.read_csv(caminho, sep=";", decimal=",")
		df_hist = df_hist[df_hist["Tipo"] == "Real"]
		if df_hist.shape[0] >= 6:
			vies_global = df_hist["Erro Consumo (%)"].mean()
			for mes in df_hist["Mês"].unique():
				df_mes = df_hist[df_hist["Mês"] == mes]
				if df_mes.shape[0] >= 3:
					vies_mensal[mes] = df_mes["Erro Consumo (%)"].mean()
	except Exception as e:
		logging.warning(f"Falha ao calcular viés histórico: {e}")
	return vies_global, vies_mensal


# === [Seção estimativas_dados-030: Estimativas — ano vigente] =================
# Objetivo:
#     Estimar consumo para o ano atual: meses passados = Real; mês corrente =
#     Corrigido (projeção parcial); meses futuros = Projetado (histórico + viés).
# Fluxo:
#     carregar_vies_historico -> media_historica_corrigida -> estimar_consumo_mensal
# Entradas:
#     - DataFrame de histórico tratado (Tratamento)
# Saídas:
#     - DataFrame com linhas para os 12 meses do ano corrente e métricas auxiliares
# Contratos:
#     - Meses ordenados por MESES_ORDEM; campos numéricos arredondados ao final
# ============================================================================
def estimar_consumo_ano_vigente(df: pd.DataFrame) -> pd.DataFrame:
	"""Gera estimativas Real/Corrigido/Projetado para o ano atual."""
	hoje = datetime.today()
	ano_atual = hoje.year
	mes_atual = hoje.month

	vies_global, vies_mensal = carregar_vies_historico()
	linhas = []
	for idx, mes_nome in enumerate(MESES_ORDEM, start=1):
		df_mes_atual = df[(df["Ano"] == ano_atual) & (df["Mês"] == mes_nome)]
		temp_hist_corr = media_historica_corrigida(df, "Temp. Média (ºC)", mes_nome, ano_atual)
		horas_hist_corr = media_historica_corrigida(df, "Horas Trabalhadas (h)", mes_nome, ano_atual)

		if idx < mes_atual:
			if not df_mes_atual.empty and pd.notnull(df_mes_atual["Consumo (KWh)"].values[0]):
				real = float(df_mes_atual["Consumo (KWh)"].values[0])
				consumo_dict = {"min": real, "esperado": real, "max": real}
			else:
				consumo_dict = estimar_consumo_mensal(df, mes_nome, ano_atual)
			temp_estim = df_mes_atual["Temp. Média (ºC)"].values[0] if not df_mes_atual.empty else None
			horas_estim = df_mes_atual["Horas Trabalhadas (h)"].values[0] if not df_mes_atual.empty else None
			tipo = "Real"
		elif idx == mes_atual:
			consumo_dict = estimar_consumo_mensal(
				df, mes_nome, ano_atual,
				consumo_parcial=df_mes_atual["Consumo (KWh)"].values[0] if not df_mes_atual.empty else None,
				dias_medidos=hoje.day,
				dias_do_mes=monthrange(ano_atual, mes_atual)[1]
			)
			temp_estim = temp_hist_corr
			horas_estim = horas_hist_corr
			tipo = "Corrigido"
		else:
			consumo_dict = estimar_consumo_mensal(df, mes_nome, ano_atual)
			temp_estim = temp_hist_corr
			horas_estim = horas_hist_corr
			tipo = "Projetado"

		consumo_corrigido = consumo_dict["esperado"]
		if tipo == "Projetado":
			vies_ajuste = vies_mensal.get(mes_nome, vies_global)
			if (vies_ajuste is not None) and (consumo_corrigido is not None):
				consumo_corrigido = round(consumo_corrigido * (1 + vies_ajuste / 100), 2)

		delta_temp, tendencia_temp_pct = calcular_diferenca(temp_estim, temp_hist_corr)
		delta_horas, tendencia_horas_pct = calcular_diferenca(horas_estim, horas_hist_corr)

		linhas.append({
			"Ano": ano_atual, "Mês": mes_nome, "Tipo": tipo,
			"Consumo Min (KWh)": consumo_dict["min"],
			"Consumo Esperado (KWh)": consumo_dict["esperado"],
			"Consumo Corrigido (KWh)": consumo_corrigido,
			"Consumo Max (KWh)": consumo_dict["max"],
			"Temp Estimada (ºC)": temp_estim,
			"Temp Hist Corr (ºC)": temp_hist_corr,
			"Δ Temp (ºC)": delta_temp,
			"Tendência Temp (%)": tendencia_temp_pct,
			"Horas Estimadas (h)": horas_estim,
			"Horas Hist Corr (h)": horas_hist_corr,
			"Δ Horas (h)": delta_horas,
			"Tendência Horas (%)": tendencia_horas_pct
		})
	return pd.DataFrame(linhas)


# === [Seção estimativas_dados-040: Estimativas — ano seguinte] ===============
# Objetivo:
#     Projetar consumo para o próximo ano com base no histórico e ajuste de viés.
# Fluxo:
#     carregar_vies_historico -> media_historica_corrigida -> estimar_consumo_mensal
# Entradas:
#     - DataFrame de histórico tratado (Tratamento)
# Saídas:
#     - DataFrame projetado para os 12 meses do ano seguinte
# Contratos:
#     - Viés aplicado automaticamente quando disponível (global ou mensal)
# ============================================================================
def estimar_consumo_ano_seguinte(df: pd.DataFrame) -> pd.DataFrame:
	"""Gera estimativas projetadas para o próximo ano usando histórico + viés."""
	hoje = datetime.today()
	ano_futuro = hoje.year + 1
	vies_global, vies_mensal = carregar_vies_historico()
	linhas = []
	for mes_nome in MESES_ORDEM:
		temp_hist_corr = media_historica_corrigida(df, "Temp. Média (ºC)", mes_nome, hoje.year)
		horas_hist_corr = media_historica_corrigida(df, "Horas Trabalhadas (h)", mes_nome, hoje.year)
		consumo_dict = estimar_consumo_mensal(df, mes_nome, ano_futuro)
		consumo_corrigido = consumo_dict["esperado"]
		vies_ajuste = vies_mensal.get(mes_nome, vies_global)
		if (vies_ajuste is not None) and (consumo_corrigido is not None):
			consumo_corrigido = round(consumo_corrigido * (1 + vies_ajuste / 100), 2)
		delta_temp, tendencia_temp_pct = calcular_diferenca(temp_hist_corr, temp_hist_corr)
		delta_horas, tendencia_horas_pct = calcular_diferenca(horas_hist_corr, horas_hist_corr)
		linhas.append({
			"Ano": ano_futuro, "Mês": mes_nome, "Tipo": "Projetado",
			"Consumo Min (KWh)": consumo_dict["min"],
			"Consumo Esperado (KWh)": consumo_dict["esperado"],
			"Consumo Max (KWh)": consumo_dict["max"],
			"Consumo Corrigido (KWh)": consumo_corrigido,
			"Temp Estimada (ºC)": temp_hist_corr,
			"Temp Hist Corr (ºC)": temp_hist_corr,
			"Δ Temp (ºC)": delta_temp,
			"Tendência Temp (%)": tendencia_temp_pct,
			"Horas Estimadas (h)": horas_hist_corr,
			"Horas Hist Corr (h)": horas_hist_corr,
			"Δ Horas (h)": delta_horas,
			"Tendência Horas (%)": tendencia_horas_pct
		})
	return pd.DataFrame(linhas)


# === [Seção estimativas_dados-050: Consolidação e exportação] =================
# Objetivo:
#     Montar o arquivo 'Estimativa_Consumo_Consolidado.csv' a partir das
#     estimativas do ano vigente e do ano seguinte.
# Fluxo:
#     estimar_consumo_ano_vigente -> estimar_consumo_ano_seguinte -> concat -> salvar CSV
# Entradas:
#     - Excel 'dados_cummins/Tabela_Historico_Tratada.xlsx'
# Saídas:
#     - CSV 'dados_cummins/Estimativa_Consumo_Consolidado.csv' (utf-8-sig; ';')
# Contratos:
#     - Arredondamento de colunas numéricas a 2 casas; backup antes de sobrescrever
# ============================================================================
def gerar_estimativas():
	"""Orquestra a geração de estimativas e salva o CSV consolidado no disco."""
	logging.info("Iniciando criação e consolidação dos dados de Estimativas...")

	caminho = DADOS_CUMMINS_DIR / "Tabela_Historico_Tratada.xlsx"
	if not caminho.exists():
		logging.warning("Arquivo 'Tabela_Historico_Tratada.xlsx' não encontrado.")
		return
	df = pd.read_excel(caminho, engine="openpyxl")
	df["Mês"] = pd.Categorical(df["Mês"], categories=MESES_ORDEM, ordered=True)

	df_ano = estimar_consumo_ano_vigente(df)
	df_proximo_ano = estimar_consumo_ano_seguinte(df)

	partes = []
	for dfx in (df_ano, df_proximo_ano):
		if dfx is None or dfx.empty:
			continue
		# 🔧 NÃO remova colunas inteiras; preserva o esquema mesmo se estiverem todas NaN.
		#    Apenas descartamos linhas totalmente vazias.
		dfx_limpo = dfx.dropna(axis=0, how="all")
		if not dfx_limpo.empty and dfx_limpo.shape[1] > 0:
			partes.append(dfx_limpo)

	if partes:
		df_consolidado = pd.concat(partes, ignore_index=True)
	else:
		logging.warning("[estimativas] Nenhum dado válido para consolidar estimativas.")
		df_consolidado = pd.DataFrame(columns=[
			"Ano","Mês","Tipo","Consumo Min (KWh)","Consumo Esperado (KWh)",
			"Consumo Corrigido (KWh)","Consumo Max (KWh)","Temp Estimada (ºC)",
			"Temp Hist Corr (ºC)","Δ Temp (ºC)","Tendência Temp (%)",
			"Horas Estimadas (h)","Horas Hist Corr (h)","Δ Horas (h)","Tendência Horas (%)"
		])

	# arredondar numéricos
	for col in df_consolidado.select_dtypes(include=[float, int]).columns:
		df_consolidado[col] = df_consolidado[col].map(lambda x: round(x, 2) if pd.notnull(x) else x)

	caminho_saida = DADOS_CUMMINS_DIR / "Estimativa_Consumo_Consolidado.csv"
	try:
		if caminho_saida.exists():
			backup_saida(caminho_saida)
		df_consolidado.to_csv(caminho_saida, index=False, encoding="utf-8-sig", sep=";", decimal=",")
		logging.info(f"Estimativas salvas com sucesso em: {caminho_saida}\n")
	except PermissionError:
		logging.error(f"Não foi possível salvar '{caminho_saida.name}' pois está aberto.\n")
	except Exception as e:
		logging.critical(f"Erro ao salvar '{caminho_saida.name}': {e}\n")