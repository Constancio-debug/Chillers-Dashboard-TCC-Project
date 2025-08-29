import logging
import shutil
import unicodedata
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np

from _executores.utils import (
	BASE_DIR,
	MESES_ORDEM,
	backup_saida,
	DADOS_CUMMINS_DIR,
	DADOS_INMET_DIR,
	DADOS_SISTEMA_ELETRICO_DIR,
	TRANSICOES_DIR,
)

# === [Seção tratamento_dados-010: Helpers e Normalização] ===================
# Objetivo:
#     Padronizar strings, leitura de planilhas e parsing flexível de formatos
#     de dados do chiller (tokenizado ou tabelado).
# Fluxo:
#     numero_mes_para_nome_pt_br -> normalizar_texto/normalizar_texto_ascii
#     -> ler_primeira_aba_excel -> inferir_formato_das_amostras
#     -> converter_tokens_para_amostras_chiller / converter_tabela_para_amostras_chiller
#     -> estimar_intervalo_de_amostragem_minutos
#     -> (transição) criar_arquivo_transicao_chiller
# ============================================================================

MES_NUM_PARA_PT = {
	1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
	7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

def numero_mes_para_nome_pt_br(n):
	"""Converte número do mês (1–12) para nome PT-BR; None se inválido."""
	try:
		return MES_NUM_PARA_PT.get(int(n))
	except Exception:
		return None

def normalizar_texto(s: str) -> str:
	"""Normaliza string: remove acentos, minúsculas e colapsa espaços."""
	if not isinstance(s, str):
		return ""
	s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
	return " ".join(s.lower().strip().split())

def ler_primeira_aba_excel(caminho: Path) -> pd.DataFrame:
	"""Lê a primeira aba de um Excel usando openpyxl."""
	return pd.read_excel(caminho, engine="openpyxl", sheet_name=0)

def inferir_formato_das_amostras(df: pd.DataFrame) -> str:
	"""Infere formato do DataFrame: 'tokens' (1 coluna) ou 'tabelado'."""
	return "tokens" if df.shape[1] == 1 else "tabelado"

def converter_tokens_para_amostras_chiller(df: pd.DataFrame) -> pd.DataFrame:
	"""Converte linhas tokenizadas em df padrão [dt, Pot_Elet_KW, Pot_Frig_KW, COP]."""
	col = df.columns[0]
	s = df[col].astype(str)
	rows = []
	for ln in s:
		parts = ln.split()
		if len(parts) < 6:
			continue
		rows.append(parts)
	if not rows:
		return pd.DataFrame(columns=["dt", "Pot_Elet_KW", "Pot_Frig_KW", "COP"])

	parsed = pd.DataFrame(rows)
	n = parsed.shape[1]
	parsed["dt"] = pd.to_datetime(parsed[0] + " " + parsed[1], errors="coerce", dayfirst=True)
	parsed["Pot_Frig_KW"] = pd.to_numeric(parsed.iloc[:, n - 3], errors="coerce")
	parsed["Pot_Elet_KW"] = pd.to_numeric(parsed.iloc[:, n - 2], errors="coerce")
	parsed["COP"] = pd.to_numeric(parsed.iloc[:, n - 1], errors="coerce")
	parsed = parsed.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
	return parsed[["dt", "Pot_Elet_KW", "Pot_Frig_KW", "COP"]]

def converter_tabela_para_amostras_chiller(df: pd.DataFrame) -> pd.DataFrame:
	"""Extrai dt/potências/COP de tabela do chiller.
	Prioriza colunas padronizadas (Data, Hora, Pot_Frig_KW, Pot_Elet_KW, COP),
	aceita variações comuns e, se detectar tabela 'achatada', cai para parsing tokenizado.
	"""
	import re
	import warnings

	# --- normalização forte para comparar cabeçalhos -------------------------
	def normalizar_chave_compacta(s: str) -> str:
		s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
		s = s.lower()
		s = re.sub(r"\s+", "", s)
		s = re.sub(r"[^a-z0-9]", "", s)
		return s

	cols_norm = {col: normalizar_chave_compacta(col) for col in df.columns}
	inv_norm = {}
	for orig, nk in cols_norm.items():
		inv_norm.setdefault(nk, orig)  # primeira ocorrência vence

	def achar_por_chave(*alts):
		for alt in alts:
			if alt in inv_norm:
				return inv_norm[alt]
		return None

	colnames_text = " | ".join(map(str, df.columns))
	achatado_suspeito = (
		any(("data" in normalizar_chave_compacta(c) and "hora" in normalizar_chave_compacta(c)) for c in df.columns)
		and not any("poteletkw" in cols_norm[c] or "potfrigkw" in cols_norm[c] or cols_norm[c] == "cop" for c in df.columns)
	)

	# --- 1) Data/Hora --------------------------------------------------------
	c_data = achar_por_chave("data") or next((c for c, nk in cols_norm.items() if nk.startswith("data")), None)
	c_hora = achar_por_chave("hora") or next((c for c, nk in cols_norm.items() if nk.startswith("hora")), None)
	c_datahora = achar_por_chave("datahora", "datahoradatetime", "datetime", "timestamp", "datetimestamp", "datetimetime")

	if c_data is not None and c_hora is not None:
		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning)
			dt = pd.to_datetime(df[c_data].astype(str) + " " + df[c_hora].astype(str),
								errors="coerce", dayfirst=True)
		logging.info("Colunas de data/hora detectadas: '%s' + '%s'", c_data, c_hora)
	elif c_datahora is not None:
		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning)
			dt = pd.to_datetime(df[c_datahora], errors="coerce", dayfirst=True)
		logging.info("Coluna combinada de data/hora detectada: '%s'", c_datahora)
	elif c_data is not None:
		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning)
			dt = pd.to_datetime(df[c_data], errors="coerce", dayfirst=True)
		logging.info("Coluna de data detectada: '%s'", c_data)
	else:
		# heurística: tenta primeiras colunas que parseiem razoavelmente
		dt = None
		for c in df.columns[:5]:
			with warnings.catch_warnings():
				warnings.simplefilter("ignore", UserWarning)
				ser = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
			if ser.notna().mean() >= 0.5:
				dt = ser
				logging.info("Coluna de data/hora inferida por heuristica: '%s'", c)
				break
		if dt is None:
			with warnings.catch_warnings():
				warnings.simplefilter("ignore", UserWarning)
				dt = pd.to_datetime(df.iloc[:, 0], errors="coerce", dayfirst=True)
			logging.info("Data/hora gerada a partir da 1a coluna (fallback).")

	# --- 2) Potências e COP (procurar direto/heurística) ---------------------
	c_pot_elet = achar_por_chave("poteletkw", "potenciaeletricakw", "poteletricakw", "kwe")
	c_pot_frig = achar_por_chave("potfrigkw", "potenciafrigorificakw", "kwf")
	c_cop      = achar_por_chave("cop")

	def eh_frio(nk: str) -> bool:
		return any(t in nk for t in ("frig", "frigor", "refrig", "cool", "kwf", "kfr", "kwt"))

	if c_pot_elet is None or c_pot_frig is None or c_cop is None:
		cmap = cols_norm
		pot_elet_cands = [
			orig for orig, nk in cmap.items()
			if ("kw" in nk and "kwh" not in nk) and not eh_frio(nk)
			   and any(h in nk for h in ("pot", "power", "elet", "eletr", "entrada", "demanda"))
		] or [
			orig for orig, nk in cmap.items()
			if ("kw" in nk and "kwh" not in nk) and not eh_frio(nk)
		]
		pot_frig_cands = [
			orig for orig, nk in cmap.items()
			if ("kw" in nk and "kwh" not in nk) and eh_frio(nk)
		]
		cop_cands = [
			orig for orig, nk in cmap.items()
			if "cop" in nk or ("coef" in nk and ("perf" in nk or "desempenho" in nk))
		]
		c_pot_elet = c_pot_elet or (pot_elet_cands[0] if pot_elet_cands else None)
		c_pot_frig = c_pot_frig or (pot_frig_cands[0] if pot_frig_cands else None)
		c_cop      = c_cop      or (cop_cands[0] if cop_cands else None)

	# --- 3) Fallback tokenizado quando a tabela veio achatada ----------------
	if (c_pot_elet is None and c_pot_frig is None and c_cop is None) or achatado_suspeito:
		logging.info("Tabela do chiller aparenta estar tokenizada/achatada (cabecalhos: %s). "
					 "Aplicando parser de tokens como fallback.", colnames_text)
		joined = df.apply(lambda r: " ".join([str(x) for x in r if pd.notna(x)]).strip(), axis=1)
		jn = joined.str.lower()
		header_mask = jn.str.contains("data") & jn.str.contains("hora") & jn.str.contains("pot", regex=False)
		tok_df = pd.DataFrame({"__linhas__": joined[~header_mask]})
		parsed_tok = converter_tokens_para_amostras_chiller(tok_df)
		return parsed_tok

	# --- 4) Conversões numéricas --------------------------------------------
	if c_pot_elet is None:
		logging.error("Dados do Chiller: Não foi encontrado coluna de potencia eletrica (kW). Cabecalhos: %s",
					  list(df.columns))
		return pd.DataFrame(columns=["dt", "Pot_Elet_KW", "Pot_Frig_KW", "COP"])

	if c_pot_frig is None:
		logging.info("Coluna de potência frigorifica não encontrada; preenchendo como NaN.")
	else:
		logging.info("Coluna de potência frigorifica identificada: '%s'", c_pot_frig)

	if c_cop is None:
		logging.info("Coluna de COP não encontrada; preenchendo como NaN.")
	else:
		logging.info("Coluna de COP identificada: '%s'", c_cop)

	pot_elet = pd.to_numeric(df[c_pot_elet], errors="coerce")
	pot_frig = pd.to_numeric(df[c_pot_frig], errors="coerce") if c_pot_frig else np.nan
	cop      = pd.to_numeric(df[c_cop], errors="coerce") if c_cop else np.nan

	out = pd.DataFrame({"dt": dt, "Pot_Elet_KW": pot_elet, "Pot_Frig_KW": pot_frig, "COP": cop})
	out = out.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
	return out

def estimar_intervalo_de_amostragem_minutos(series_dt: pd.Series) -> float:
	"""Estima o passo (min) entre amostras com base na mediana dos deltas."""
	deltas = series_dt.diff().dt.total_seconds().div(60.0).iloc[1:]
	deltas = deltas[(deltas > 0) & (deltas <= 60)]
	if deltas.empty:
		return 1.0
	med = float(deltas.median())
	candidatos = np.array([1, 5, 10, 15, 30, 60], dtype=float)
	passo = float(candidatos[np.argmin(np.abs(candidatos - med))])
	iq_range = deltas.quantile(0.75) - deltas.quantile(0.25)
	if iq_range > 0.5 * passo:
		logging.warning(f"Dados do Chiller: Dispersao alta dos deltas (IQR={iq_range:.2f} min) para passo ~{passo:.0f} min.")
	return passo

# --- Parsing robusto de dt salvo em CSV (evita UserWarning) -----------------
def interpretar_datetime_intermediario(series: pd.Series) -> pd.Series:
	"""Tenta ISO '%Y-%m-%d %H:%M:%S' primeiro; senão cai para dayfirst=True."""
	import warnings
	with warnings.catch_warnings():
		warnings.simplefilter("ignore", UserWarning)
		dt = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
	if dt.notna().mean() < 0.8:
		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning)
			dt = pd.to_datetime(series, errors="coerce", dayfirst=True)
	return dt


# === [Transição] Config e helpers ===========================================
# (usamos TRANSICOES_DIR do utils)

# Cabeçalhos esperados quando o CSV vier completo e bem tokenizado
COLUNAS_CHILLER_COMPLETO = [
	"Data","Hora","Arref_m3_h","Arref_bar","Arref_C","Arref_Rotacao_B1",
	"Arref_Status_B1","Arref_Status_B2","Arref_Status_B3",
	"Alim_m3_h","Alim_bar","Alim_C","Retorno_bar","Retorno_C",
	"Status_CH1","Status_CH2","Status_CH3","Status_BAG1","Status_BAG2","Status_BAG3","Status_BAG4",
	"Pot_Frig_KW","Pot_Elet_KW","COP"
]

def detectar_planilha_achatada(df: pd.DataFrame) -> bool:
	"""Heurística: cabeçalho inteiro em uma célula ou poucas colunas com header gigante."""
	def _norm_key_strict(s: str) -> str:
		import re
		s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
		s = s.lower()
		s = re.sub(r"\s+", "", s)
		s = re.sub(r"[^a-z0-9]", "", s)
		return s
	if df.shape[1] <= 2:
		nk0 = _norm_key_strict(df.columns[0])
		return ("data" in nk0 and "hora" in nk0 and "pot" in nk0) or len(nk0) > 60
	return False

def tokenizar_linhas(df: pd.DataFrame) -> pd.Series:
	"""Une células por linha -> string; remove NaN; ideal para splitting por espaço.
	É robusta para df vazio ou linhas só com NaN, garantindo dtype string.
	"""
	# Caso df vazio, retorna Série vazia (dtype string) sem quebrar .str
	if df is None or df.empty:
		return pd.Series([], dtype="string")

	# Converte tudo para string, substitui NaN por "", e junta por linha
	df_str = df.astype(str).where(df.notna(), "")
	joined = df_str.apply(lambda r: " ".join([x for x in r if x]).strip(), axis=1)

	# Garante dtype string para permitir .str.*
	joined = joined.astype("string")

	# Máscara para remover possíveis linhas de cabeçalho "achatado"
	jn = joined.str.lower()
	header_mask = (
		jn.str.contains("data", na=False)
		& jn.str.contains("hora", na=False)
		& jn.str.contains("pot", regex=False, na=False)
	)

	return joined[~header_mask]

def separar_colunas_tokenizadas(df: pd.DataFrame) -> pd.DataFrame:
	"""Tenta separar TODAS as colunas conhecidas a partir de linhas tokenizadas."""
	linhas = tokenizar_linhas(df)
	if linhas.empty:
		return pd.DataFrame(columns=COLUNAS_CHILLER_COMPLETO)
	toks = linhas.str.split()
	ok = toks[toks.map(len) >= len(COLUNAS_CHILLER_COMPLETO)]
	if ok.empty:
		return pd.DataFrame(columns=COLUNAS_CHILLER_COMPLETO)

	arr = ok.map(lambda xs: xs[:len(COLUNAS_CHILLER_COMPLETO)]).tolist()
	wide = pd.DataFrame(arr, columns=COLUNAS_CHILLER_COMPLETO)

	with pd.option_context("mode.chained_assignment", None):
		wide["dt"] = pd.to_datetime(wide["Data"] + " " + wide["Hora"], errors="coerce", dayfirst=True)
		for c in ("Pot_Frig_KW","Pot_Elet_KW","COP"):
			wide[c] = pd.to_numeric(wide[c].astype(str).str.replace(",", ".", regex=False), errors="coerce")
	return wide

def criar_arquivo_transicao_chiller(origem: Optional[Path] = None) -> Tuple[Optional[Path], Optional[Path]]:
	"""Gera arquivos de transição do chiller.
	Retorna (caminho_raw_separado, caminho_normalizado).
	"""
	if origem is None:
		origem = DADOS_CUMMINS_DIR / "Dados do Chiller.xlsx"
	if not origem.exists():
		logging.error(f"Arquivo não encontrado: {origem}")
		return None, None

	raw = ler_primeira_aba_excel(origem)

	# Caso 'achatado': separar todas as colunas por tokenização
	raw_sep_path = None
	if detectar_planilha_achatada(raw) or raw.shape[1] == 1:
		wide = separar_colunas_tokenizadas(raw)
		if not wide.empty:
			raw_sep_path = TRANSICOES_DIR / "Chiller_raw_separado.csv"
			try:
				wide.to_csv(raw_sep_path, sep=";", index=False, encoding="latin1")
				logging.info(f"Transição: Chiller_raw_separado.csv gerado em: {raw_sep_path}")
			except Exception as e:
				logging.warning(f"Transição: Falha ao salvar Chiller_raw_separado.csv: {e}")

			norm = wide[["dt","Pot_Elet_KW","Pot_Frig_KW","COP"]].copy()
		else:
			tok_df = pd.DataFrame({"__linhas__": tokenizar_linhas(raw)})
			norm = converter_tokens_para_amostras_chiller(tok_df)
	else:
		norm = converter_tabela_para_amostras_chiller(raw)

	if norm is None or norm.empty:
		logging.warning("Transição: Dados normalizados vazios; nada salvo.")
		return raw_sep_path, None

	norm_path = TRANSICOES_DIR / "Chiller_amostras_normalizadas.csv"
	try:
		norm.to_csv(norm_path, sep=";", index=False, encoding="latin1")
		logging.info(f"Transição: Chiller_amostras_normalizadas.csv gerado em: {norm_path}")
	except Exception as e:
		logging.warning(f"Transição: Falha ao salvar Chiller_amostras_normalizadas.csv: {e}")
		norm_path = None

	return raw_sep_path, norm_path

# --- Aliases de compatibilidade (nomes antigos) -----------------------------
_pt_mes = numero_mes_para_nome_pt_br
_norm = normalizar_texto
_ler_primeira_aba = ler_primeira_aba_excel
_formato = inferir_formato_das_amostras
_parse_tokens = converter_tokens_para_amostras_chiller
_parse_tabelado = converter_tabela_para_amostras_chiller
_detectar_passo_min = estimar_intervalo_de_amostragem_minutos
# Mantém compatibilidade com chamadas existentes na consolidação:
_parse_dt_salvo = interpretar_datetime_intermediario
identificar_formato_amostras = inferir_formato_das_amostras
converter_tokens_em_amostras_chiller = converter_tokens_para_amostras_chiller
converter_tabela_em_amostras_chiller = converter_tabela_para_amostras_chiller
# ---------------------------------------------------------------------------


# === [Seção tratamento_dados-020: Chiller — leitura e agregação] ===========
# Objetivo:
#     Ler amostras do chiller, inferir passo, e agregar consumo/horas por mês.
# Fluxo:
#     carregar_amostras_chiller -> agregar_consumo_e_horas_chiller
# ============================================================================
def carregar_amostras_chiller(caminho: Optional[Path] = None) -> pd.DataFrame:
	"""Lê e padroniza amostras do chiller, incluindo coluna step_h (horas/linha)."""
	if caminho is None:
		caminho = DADOS_CUMMINS_DIR / "Dados do Chiller.xlsx"
	if not caminho.exists():
		logging.error(f"Arquivo não encontrado: {caminho}")
		return pd.DataFrame(columns=["dt", "Pot_Elet_KW", "Pot_Frig_KW", "COP", "step_h"])

	# 1) Tenta usar o arquivo de transição se estiver mais novo que o original
	trans_norm = TRANSICOES_DIR / "Chiller_amostras_normalizadas.csv"
	try:
		if trans_norm.exists() and trans_norm.stat().st_mtime >= caminho.stat().st_mtime:
			df = pd.read_csv(trans_norm, sep=";", encoding="latin1")
			df["dt"] = interpretar_datetime_intermediario(df["dt"])
			for c in ("Pot_Elet_KW","Pot_Frig_KW","COP"):
				df[c] = pd.to_numeric(df[c], errors="coerce")
			if not df.empty:
				passo_min = estimar_intervalo_de_amostragem_minutos(df["dt"])
				df["step_h"] = passo_min / 60.0
				return df[["dt","Pot_Elet_KW","Pot_Frig_KW","COP","step_h"]]
	except Exception as e:
		logging.warning(f"Transição: Falha ao ler normalizado: {e}")

	# 2) (Re)gera arquivos de transição e consome o normalizado
	_, norm_path = criar_arquivo_transicao_chiller(caminho)
	if norm_path and norm_path.exists():
		try:
			df = pd.read_csv(norm_path, sep=";", encoding="latin1")
			df["dt"] = interpretar_datetime_intermediario(df["dt"])
			for c in ("Pot_Elet_KW","Pot_Frig_KW","COP"):
				df[c] = pd.to_numeric(df[c], errors="coerce")
		except Exception as e:
			logging.warning(f"Transição: Falha ao ler normalizado recém-criado: {e}")
			df = pd.DataFrame(columns=["dt","Pot_Elet_KW","Pot_Frig_KW","COP"])
	else:
		# último recurso: fluxo direto da planilha
		raw = ler_primeira_aba_excel(caminho)
		fmt = inferir_formato_das_amostras(raw)
		df = converter_tokens_para_amostras_chiller(raw) if fmt == "tokens" or raw.shape[1] == 1 else converter_tabela_para_amostras_chiller(raw)

	if df.empty:
		logging.warning("Dados do chiller vazios após parse.")
		return pd.DataFrame(columns=["dt", "Pot_Elet_KW", "Pot_Frig_KW", "COP", "step_h"])

	passo_min = estimar_intervalo_de_amostragem_minutos(df["dt"])
	df["step_h"] = passo_min / 60.0
	return df[["dt","Pot_Elet_KW","Pot_Frig_KW","COP","step_h"]]

def agregar_consumo_e_horas_chiller(caminho: Optional[Path] = None, potencia_min_kw: float = 0.0) -> pd.DataFrame:
	"""Agrega consumo (kWh) e horas de operação por mês."""
	parsed = carregar_amostras_chiller(caminho)
	if parsed.empty:
		return pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)", "Horas Trabalhadas (h)"])

	pot = pd.to_numeric(parsed["Pot_Elet_KW"], errors="coerce").fillna(0)
	ligado = pot > potencia_min_kw

	parsed["Ano"] = parsed["dt"].dt.year
	parsed["Mes_num"] = parsed["dt"].dt.month
	parsed["Mês"] = parsed["Mes_num"].map(numero_mes_para_nome_pt_br)

	parsed["kwh_linha"] = (pot * parsed["step_h"]).where(ligado, 0.0)
	parsed["h_linha"] = parsed["step_h"].where(ligado, 0.0)

	monthly = parsed.groupby(["Ano", "Mês"], as_index=False)[["kwh_linha", "h_linha"]].sum()
	monthly = monthly.rename(columns={"kwh_linha": "Consumo (KWh)", "h_linha": "Horas Trabalhadas (h)"})
	monthly = monthly[monthly["Mês"].isin(MESES_ORDEM)]
	monthly["Mês"] = pd.Categorical(monthly["Mês"], categories=MESES_ORDEM, ordered=True)
	return monthly.sort_values(["Ano", "Mês"]).reset_index(drop=True)


# === [Seção tratamento_dados-030: INMET — temperatura mensal] ==============
# Objetivo:
#     Calcular a média mensal das médias diárias de temperatura (Mirante SP).
# Fluxo:
#     normalizar_texto_ascii -> calcular_temperatura_media_mensal_inmet
# ============================================================================
def normalizar_texto_ascii(s: str) -> str:
	"""Normaliza string removendo acentos e espaços duplicados (ASCII)."""
	if not isinstance(s, str):
		return ""
	s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
	return " ".join(s.lower().strip().split())

def calcular_temperatura_media_mensal_inmet(base_dir: Path) -> pd.DataFrame:
	"""Lê CSV do INMET e retorna média mensal das médias diárias."""
	arq_inmet = DADOS_INMET_DIR / "Dados_Tratados_INMET_Mirante_De_São_Paulo.csv"
	if not arq_inmet.exists():
		logging.info(f"Arquivo INMET não encontrado: {arq_inmet}")
		return pd.DataFrame(columns=["Ano", "Mês", "Temp. Média (ºC)"])

	try:
		din = pd.read_csv(arq_inmet, sep=";", encoding="latin1")
	except Exception as e:
		logging.warning(f"Falha ao ler {arq_inmet.name}: {e}")
		return pd.DataFrame(columns=["Ano", "Mês", "Temp. Média (ºC)"])

	if din.empty:
		return pd.DataFrame(columns=["Ano", "Mês", "Temp. Média (ºC)"])

	cmap = {c: normalizar_texto_ascii(c) for c in din.columns}

	# coluna de data
	c_data = None
	for orig, k in cmap.items():
		if k == "data" or k.startswith("data"):
			c_data = orig
			break
	if c_data is None:
		c_data = din.columns[0]

	# coluna de temperatura (bulbo seco preferencial)
	c_temp = None
	for orig, k in cmap.items():
		if "temperatura do ar" in k and "bulbo seco" in k:
			c_temp = orig
			break
	if c_temp is None:
		for orig, k in cmap.items():
			if "temperatura" in k and "orvalho" not in k:
				c_temp = orig
				break
	if c_temp is None:
		logging.warning("Não foi encontrado coluna de temperatura.")
		return pd.DataFrame(columns=["Ano", "Mês", "Temp. Média (ºC)"])

	dt = pd.to_datetime(din[c_data], errors="coerce", dayfirst=True)
	temp = (
		din[c_temp].astype(str)
		.str.replace(",", ".", regex=False)
		.str.replace(" ", "", regex=False)
	)
	temp = pd.to_numeric(temp, errors="coerce")

	df = pd.DataFrame({"data": dt.dt.date, "temp": temp}).dropna()
	if df.empty:
		return pd.DataFrame(columns=["Ano", "Mês", "Temp. Média (ºC)"])

	diarios = df.groupby("data", as_index=False)["temp"].mean()
	diarios["Ano"] = pd.to_datetime(diarios["data"]).dt.year
	diarios["Mes_num"] = pd.to_datetime(diarios["data"]).dt.month
	diarios["Mês"] = diarios["Mes_num"].map(MES_NUM_PARA_PT)

	mensais = (
		diarios.groupby(["Ano", "Mês"], as_index=False)["temp"]
		.mean()
		.rename(columns={"temp": "Temp. Média (ºC)"})
	)
	mensais["Temp. Média (ºC)"] = mensais["Temp. Média (ºC)"].round(2)
	return mensais

# Aliases de compatibilidade para manter chamadas existentes
_norm_ascii = normalizar_texto_ascii
_carregar_temperatura_mensal_inmet = calcular_temperatura_media_mensal_inmet


# === [Seção tratamento_dados-040: Arquivos-base Cummins] ====================
# (Mantidas somente rotinas úteis; removidas rotinas de quarentena/validação/transacional)

def gerar_csv_geolocalizacao_guarulhos():
	"""Gera CSV de geolocalização básico para Guarulhos em dados_cummins/."""
	logging.info("Gerando arquivo de geolocalização de Guarulhos...")
	destino = DADOS_CUMMINS_DIR
	destino.mkdir(parents=True, exist_ok=True)

	dados = {
		"CEP": ["07180-140"],
		"Municipio": ["Guarulhos"],
		"ESTADO": ["São Paulo"],
		"REGIÃO": ["Sudeste"]
	}
	df = pd.DataFrame(dados)
	caminho = destino / "Info_Geografica_Guarulhos.csv"

	if caminho.exists():
		logging.info(f"O arquivo '{caminho.name}' já existe. Geolocalização de Guarulhos não foi sobrescrito.\n")
	else:
		try:
			df.to_csv(caminho, sep=';', index=False, encoding='latin1')
			logging.info(f"Geolocalização de Guarulhos criada com sucesso em: {caminho}\n")
		except Exception as e:
			logging.warning(f"Falha ao criar a Geolocalização de Guarulhos: {e}\n")

def mover_arquivo_chiller_para_dados_cummins():
	"""Procura CHILLERS.xlsx ou CHILLERS.csv no BASE_DIR e salva como
	'dados_cummins/Dados do Chiller.xlsx'.
	- Se for XLSX: move e renomeia direto
	- Se for CSV : converte para XLSX (aba 'CHILLERS')
	"""
	logging.info("Processando arquivo CHILLERS...")

	origem_dir = BASE_DIR
	destino_dir = DADOS_CUMMINS_DIR
	destino_dir.mkdir(parents=True, exist_ok=True)

	origem_xlsx = origem_dir / "CHILLERS.xlsx"
	origem_csv  = origem_dir / "CHILLERS.csv"
	destino_xlsx = destino_dir / "Dados do Chiller.xlsx"

	try:
		if origem_xlsx.exists():
			if destino_xlsx.exists():
				backup_saida(destino_xlsx)
				destino_xlsx.unlink(missing_ok=True)
			shutil.move(str(origem_xlsx), str(destino_xlsx))
			logging.info(f"OK: '{origem_xlsx.name}' → '{destino_xlsx}'")
			return

		if origem_csv.exists():
			# Leitura básica do CSV para converter em Excel
			df = pd.read_csv(origem_csv, sep=None, engine="python")

			if destino_xlsx.exists():
				backup_saida(destino_xlsx)
				destino_xlsx.unlink(missing_ok=True)

			with pd.ExcelWriter(destino_xlsx, engine="openpyxl") as writer:
				df.to_excel(writer, sheet_name="CHILLERS", index=False)

			# remove a origem após sucesso
			origem_csv.unlink(missing_ok=True)
			logging.info(f"OK: '{origem_csv.name}' convertido → '{destino_xlsx}'")
			return

		logging.info("Nenhum 'CHILLERS.xlsx' ou 'CHILLERS.csv' encontrado no BASE_DIR.")

	except PermissionError:
		logging.error(f"Arquivo '{destino_xlsx.name}' está aberto, não foi possível sobrescrever.")
	except Exception as e:
		logging.error(f"Falha ao processar CHILLERS: {e}")

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
			logging.warning(f"O arquivo '{arquivo_origem.name}' não possui colunas suficientes.\n")
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


# === [Seção tratamento_dados-050: Tabela Histórica (consolidação)] =========
def construir_tabela_historico_de_chiller(potencia_min_kw: float = 0.0):
	"""Gera Tabela_Historico_Tratada.xlsx unindo chiller, INMET e custos."""
	logging.info("Iniciando criação e consolidação dos dados de Historico...")

	pasta = DADOS_CUMMINS_DIR
	pasta.mkdir(parents=True, exist_ok=True)
	destino = pasta / "Tabela_Historico_Tratada.xlsx"

	# --- INMET primeiro: usamos para criar a grade-base
	mensais_temp = _carregar_temperatura_mensal_inmet(BASE_DIR)  # ['Ano','Mês','Temp. Média (ºC)']

	# --- Chiller (se existir)
	amostras = carregar_amostras_chiller()
	monthly = pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)", "Horas Trabalhadas (h)"])
	pot_media = pd.DataFrame(columns=["Ano", "Mês", "Potência Média (KW)"])
	cop_medio = pd.DataFrame(columns=["Ano", "Mês", "COP Médio"])

	if not amostras.empty:
		pot = pd.to_numeric(amostras["Pot_Elet_KW"], errors="coerce").fillna(0)
		ligado = pot > potencia_min_kw
		amostras["Ano"] = amostras["dt"].dt.year
		amostras["Mês"] = amostras["dt"].dt.month.map(MES_NUM_PARA_PT)

		# agregados principais
		monthly = agregar_consumo_e_horas_chiller(potencia_min_kw=potencia_min_kw)

		# médias ponderadas
		am = amostras.copy()
		am["w"] = am["step_h"].where(ligado, 0.0)

		def _weighted_avg(g: pd.DataFrame, col: str):
			"""Calcula média ponderada por step_h para a coluna informada."""
			w = pd.to_numeric(g["w"], errors="coerce").fillna(0).values
			x = pd.to_numeric(g[col], errors="coerce").values
			if np.nansum(w) <= 0:
				return np.nan
			return float(np.nansum(x * w) / np.nansum(w))

		pot_media = (
			am.groupby(["Ano", "Mês"])[["w", "Pot_Elet_KW"]]
			  .apply(lambda g: _weighted_avg(g, "Pot_Elet_KW"))
			  .reset_index(name="Potência Média (KW)")
		)
		cop_medio = (
			am.groupby(["Ano", "Mês"])[["w", "COP"]]
			  .apply(lambda g: _weighted_avg(g, "COP"))
			  .reset_index(name="COP Médio")
		)

	# --- Grade-base (Ano, Mês): união dos pares presentes no INMET e/ou chiller
	partes_keys = []
	for dfx in (mensais_temp, monthly, pot_media, cop_medio):
		if dfx is not None and not dfx.empty:
			partes_keys.append(dfx[["Ano", "Mês"]])
	if partes_keys:
		keys = pd.concat(partes_keys, ignore_index=True).drop_duplicates()
	else:
		logging.warning("Nenhuma chave (Ano, Mês) encontrada em INMET ou chiller.")
		return

	keys = keys[keys["Mês"].isin(MESES_ORDEM)].copy()
	keys["Mês"] = pd.Categorical(keys["Mês"], categories=MESES_ORDEM, ordered=True)
	keys = keys.sort_values(["Ano", "Mês"]).reset_index(drop=True)

	df = keys.merge(monthly, on=["Ano", "Mês"], how="left")
	df = df.merge(pot_media, on=["Ano", "Mês"], how="left")
	df = df.merge(cop_medio, on=["Ano", "Mês"], how="left")

	arq_preco = pasta / "Valor_KWh_Ano.xlsx"
	if arq_preco.exists():
		try:
			kwh_tab = pd.read_excel(arq_preco, engine="openpyxl")
			kwh_tab.columns = [c.strip() for c in kwh_tab.columns]
			if {"Ano", "Valor"}.issubset(kwh_tab.columns):
				kwh_tab["Valor"] = pd.to_numeric(
					kwh_tab["Valor"].astype(str).str.replace(",", ".", regex=False),
					errors="coerce"
				)
				df = df.merge(
					kwh_tab[["Ano", "Valor"]].rename(columns={"Valor": "Preco_KWh_R$"}),
					on="Ano", how="left"
				)
				df["Gasto da Operacao (R$)"] = df["Consumo (KWh)"] * df["Preco_KWh_R$"]
		except Exception as e:
			logging.warning(f"Falha ao aplicar preço de kWh: {e}")

	arq_fator = DADOS_SISTEMA_ELETRICO_DIR / "Inventario_Fator_Medio_Anual_CO2_KWh.csv"
	if arq_fator.exists():
		try:
			fator = pd.read_csv(arq_fator, sep=";", encoding="latin1")
			if {"ANO", "Fator Médio Anual (kgCO2/kWh)"}.issubset(fator.columns):
				fator = fator.rename(columns={"ANO": "Ano", "Fator Médio Anual (kgCO2/kWh)": "kgCO2_kWh"})
				df = df.merge(fator[["Ano", "kgCO2_kWh"]], on="Ano", how="left")
				df["CO2 Emitido (Kg)"] = df["Consumo (KWh)"] * df["kgCO2_kWh"]
		except Exception as e:
			logging.warning(f"Falha ao aplicar fator de emissão: {e}")

	if mensais_temp is not None and not mensais_temp.empty:
		if "Temp. Média (ºC)" not in df.columns:
			df["Temp. Média (ºC)"] = pd.NA

		df = df.merge(mensais_temp, on=["Ano", "Mês"], how="left", suffixes=("", "_novo"))

		col_base = pd.to_numeric(df["Temp. Média (ºC)"], errors="coerce")
		col_novo  = pd.to_numeric(df["Temp. Média (ºC)_novo"], errors="coerce")

		if col_base.notna().any() and col_novo.notna().any():
			df["Temp. Média (ºC)"] = col_base.combine_first(col_novo)
		else:
			df["Temp. Média (ºC)"] = col_base.fillna(col_novo)

		df.drop(columns=[c for c in df.columns if c.endswith("_novo")], inplace=True)
	else:
		logging.info("Sem temperaturas INMET para mesclar.")

	df["Mês"] = pd.Categorical(df["Mês"], categories=MESES_ORDEM, ordered=True)
	df = df.sort_values(["Ano", "Mês"]).reset_index(drop=True)

	if destino.exists():
		backup_saida(destino)
	with pd.ExcelWriter(destino, engine="openpyxl") as writer:
		df.to_excel(writer, sheet_name="Histórico Tratado", index=False)

	logging.info(f"Tabela_Historico_Tratada.xlsx gerada : {destino}\n")