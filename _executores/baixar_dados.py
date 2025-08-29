import logging 
import time
import re
import zipfile
from pathlib import Path
from datetime import datetime

import httpx
import pandas as pd

from _executores.utils import DADOS_SISTEMA_ELETRICO_DIR, DADOS_INMET_DIR, extrair_data_final, backup_saida

# Constante global: identificação do cliente HTTP para logs do servidor.
UA = {"User-Agent": "CumminsChillers/1.0 (+diagnostics)"}


# === [Seção baixar_dados-010: Download robusto] =============================
# Objetivo:
#     Fazer download de arquivos com retentativas, backoff simples e checagem
#     básica de integridade (tamanho > 0).
# Fluxo:
#     baixar_arquivo_com_retentativas
# Entradas:
#     - url (str), destino (Path), tentativas (int), delay (s), verificar_ssl (bool)
# Saídas:
#     - Arquivo gravado em disco (True) ou sinalização de falha (False)
# Contratos:
#     - Timeout de 60s; HTTP 200 é obrigatório; arquivo final com st_size > 0
# Erros tratados:
#     - Exceções de rede/IO são capturadas e logadas a cada tentativa
# Observações:
#     Sem efeitos colaterais além de IO em disco/rede; complexidade O(n) no tamanho do arquivo
# ============================================================================
def baixar_arquivo_com_retentativas(url, destino, tentativas=3, delay=5, verificar_ssl=True):
	"""Baixa um arquivo com retentativas e verificação básica.

	Args:
		url (str): URL do recurso.
		destino (Path): Caminho de saída.
		tentativas (int): Número de tentativas.
		delay (int): Atraso entre tentativas (s).
		verificar_ssl (bool): Se deve verificar o certificado SSL.

	Returns:
		bool: True em caso de sucesso; False caso contrário.
	"""
	for tentativa in range(1, tentativas + 1):
		try:
			with httpx.stream("GET", url, timeout=60.0, verify=verificar_ssl, headers=UA) as resposta:
				if resposta.status_code == 200:
					with open(destino, "wb") as f:
						for bloco in resposta.iter_bytes():
							f.write(bloco)
					# checagem básica de tamanho
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


# === [Seção baixar_dados-020: Sistema Elétrico Nacional (SIRENE/MCTI)] =====
# Objetivo:
#     Baixar planilhas oficiais do inventário e consolidar o Fator Médio Anual
#     (kgCO2/kWh) a partir da aba "inventário-todos".
# Fluxo:
#     garantir_base_inventario_2024 ->
#     tratar_inventario_fator_medio_anual_C02_KWh (base) ->
#     baixar_planilhas_sistema_eletrico_ano_atual ->
#     tratar_inventario_fator_medio_anual_C02_KWh (geral)
# Entradas:
#     - Link fixo do arquivo base 2024 (jandez) e página do MCTI para descoberta de .xlsx por ano
# Saídas:
#     - CSV: Inventario_Fator_Medio_Anual_CO2_KWh.csv (sep=';')
# Contratos:
#     - Sempre garantir que 'Inventario_2024_jandez.xlsx' exista no diretório de trabalho.
#     - Tratar a base antes de seguir com os demais downloads.
#     - Descobrir links do ano corrente na página do MCTI; fallback pela URL antiga /arquivo/Inventario_<ano>_<sufixo>.xlsx.
#     - Normalizar pelo mês final (3 letras antes de ".xlsx") → sufixo canônico {janfev, ..., jandez}.
#     - Nome local: Inventario_<ano>_<sufixo_canônico>.xlsx.
#     - Retenção: manter apenas o arquivo mais recente do ano corrente; **nunca** remover arquivos de anos anteriores (ex.: 2024).
# ============================================================================
def garantir_base_inventario_2024():
	"""Garante o arquivo base 'Inventario_2024_jandez.xlsx' antes do fluxo principal."""
	destino = DADOS_SISTEMA_ELETRICO_DIR
	destino.mkdir(parents=True, exist_ok=True)
	alvo = destino / "Inventario_2024_jandez.xlsx"
	url_base = ("https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/sirene/dados-e-ferramentas/"
	            "fatores-de-emissao/arquivo/Inventario_2024_jandez.xlsx/@@download/file")
	if alvo.exists() and alvo.stat().st_size > 0:
		logging.info("Base 2024 já presente: Inventario_2024_jandez.xlsx\n")
		return True
	logging.info("Garantindo base 2024: Inventario_2024_jandez.xlsx...")
	ok = baixar_arquivo_com_retentativas(url_base, alvo, verificar_ssl=True)
	if not ok:
		logging.warning("Falha ao garantir a base 2024 (Inventario_2024_jandez.xlsx).")
	return ok

def baixar_extrair_e_filtrar_dados_sistema_eletrico():
	"""Orquestra download e tratamento do inventário do Sistema Elétrico."""
	logging.info("Iniciando atualização dos dados do Sistema Elétrico Nacional...\n")

	# 1) Base obrigatória (2024 jandez) e tratamento preliminar
	garantir_base_inventario_2024()
	tratar_inventario_fator_medio_anual_C02_KWh()  # trata ao menos a base já garantida

	# 2) Fluxo normal do ano corrente + tratamento consolidado
	baixados = baixar_planilhas_sistema_eletrico_ano_atual()
	if not baixados:
		logging.info("Nenhum novo arquivo foi baixado. Dados já estão atualizados.\n")
	tratar_inventario_fator_medio_anual_C02_KWh()

def baixar_planilhas_sistema_eletrico_ano_atual():
	"""Baixa planilhas do ano corrente e mantém apenas a mais recente (descoberta + normalização)."""

	# --- Helpers internos (nomes padronizados e sem '_' inicial) ------------
	import unicodedata, urllib.parse as _up
	from bs4 import BeautifulSoup  # requer beautifulsoup4 instalado

	def norm_ascii(s: str) -> str:
		"""Remove acentos e normaliza para ASCII simples (lowercase mantido externamente)."""
		s = unicodedata.normalize("NFKD", s)
		return "".join(ch for ch in s if ord(ch) < 128)

	def mes_final_tres_letras(nome_ou_url: str) -> str | None:
		"""Extrai as ÚLTIMAS 3 letras alfabéticas imediatamente antes de '.xlsx' (ignora _v2, -final etc.)."""
		alvo = _up.unquote(str(nome_ou_url))
		alvo = norm_ascii(alvo).lower()
		m = re.search(r'([a-z]{3})(?=[^a-z]*\.xlsx(?:$|\?))', alvo)
		return m.group(1) if m else None

	def sufixo_canonico(mes3: str) -> str | None:
		"""Mapeia 'fev'→'janfev', 'mar'→'janmar', ..., 'dez'→'jandez'."""
		mapa = {
			"fev":"janfev","mar":"janmar","abr":"janabr","mai":"janmai",
			"jun":"janjun","jul":"janjul","ago":"janago","set":"janset",
			"out":"janout","nov":"jannov","dez":"jandez",
		}
		return mapa.get(mes3)

	def descobrir_links_por_ano(pagina: str, ano: int) -> dict:
		"""
		Retorna dict {sufixo_canonico: url_xlsx} para o ano informado.
		Regra: link deve conter .xlsx e o ano no texto/URL; sufixo é derivado do mês final (3 letras).
		"""
		try:
			r = httpx.get(pagina, headers=UA, timeout=60.0)
			r.raise_for_status()
		except Exception as e:
			logging.warning(f"Falha ao carregar página do MCTI para descoberta: {e}")
			return {}

		from bs4 import BeautifulSoup  # reforça import local
		soup = BeautifulSoup(r.text, "html.parser")
		encontrados = {}
		for a in soup.select("a[href]"):
			href = _up.urljoin(pagina, a.get("href"))
			txt = (a.get_text(" ", strip=True) or "") + " " + href
			if str(ano) not in txt:
				continue
			if not re.search(r'\.xlsx(?:$|\?)', href, flags=re.I):
				continue
			mes3 = mes_final_tres_letras(href) or mes_final_tres_letras(txt)
			suf = sufixo_canonico(mes3) if mes3 else None
			if suf:
				encontrados[suf] = href
		return encontrados
	# ------------------------------------------------------------------------

	logging.info("Baixando dados do Sistema Elétrico Nacional...\n")
	ano = datetime.now().year
	sufixos_ordem = ["janfev","janmar","janabr","janmai","janjun","janjul",
					 "janago","janset","janout","jannov","jandez"]
	destino = DADOS_SISTEMA_ELETRICO_DIR
	destino.mkdir(parents=True, exist_ok=True)

	# Descoberta de links .xlsx na página oficial; fallback permanece ativo
	pagina_base = "https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/sirene/dados-e-ferramentas/fatores-de-emissao/"
	links_descobertos = descobrir_links_por_ano(pagina_base, ano)

	base_url = "https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/sirene/dados-e-ferramentas/fatores-de-emissao/arquivo"

	# Detecta o arquivo mais recente já existente localmente (pela convenção canônica)
	arquivos_existentes = list(destino.glob(f"Inventario_{ano}_*.xlsx"))
	sufixo_existente = None
	index_existente = -1
	for arq in arquivos_existentes:
		for i, sufixo in enumerate(sufixos_ordem):
			if sufixo in arq.name and i > index_existente:
				index_existente = i
				sufixo_existente = sufixo
	if sufixo_existente:
		logging.info(f"Arquivo mais recente já existente: Inventario_{ano}_{sufixo_existente}.xlsx\n")
	else:
		logging.info("Nenhum arquivo encontrado localmente para o ano atual.\n")

	baixados = []
	for i, sufixo in enumerate(sufixos_ordem):
		if i <= index_existente:
			continue

		# Preferir URL descoberta para este sufixo; se ausente, tentar padrão antigo
		nome_arquivo = f"Inventario_{ano}_{sufixo}.xlsx"
		url = links_descobertos.get(sufixo) or f"{base_url}/{nome_arquivo}"
		caminho = destino / nome_arquivo

		logging.info(f"Tentando baixar: {nome_arquivo}...")
		sucesso = baixar_arquivo_com_retentativas(url, caminho, verificar_ssl=True)
		if sucesso:
			logging.info(f"Sucesso: {nome_arquivo}")
			baixados.append((sufixo, caminho))
		else:
			logging.info(f"Falha: {nome_arquivo}\n")

	# Manter apenas o mais recente do ano (maior índice canônico encontrado).
	# Observação: a retenção só considera o ano corrente; arquivos de anos anteriores (ex.: 2024) permanecem intactos.
	todos_arquivos = list(destino.glob(f"Inventario_{ano}_*.xlsx"))
	if todos_arquivos:
		def idx_sufixo(f):
			for suf in sufixos_ordem:
				if suf in f.name:
					return sufixos_ordem.index(suf)
			return -1
		mais_recente = max(todos_arquivos, key=idx_sufixo)
		for arquivo in todos_arquivos:
			if arquivo != mais_recente:
				try:
					arquivo.unlink()
					logging.info(f"Removido: {arquivo.name} (mantido {mais_recente.name})")
				except Exception as e:
					logging.warning(f"Erro ao remover {arquivo.name}: {e}")
	return baixados

def tratar_inventario_fator_medio_anual_C02_KWh():
	"""Extrai (ano, fator) da aba 'inventário-todos' e gera CSV consolidado."""
	level = logging.info
	level("Tratando dados do Sistema Elétrico Nacional...")
	pasta = DADOS_SISTEMA_ELETRICO_DIR
	arquivos = list(pasta.glob("Inventario_*.xlsx"))
	todos_os_dados = []
	for arquivo in arquivos:
		try:
			df = pd.read_excel(arquivo, sheet_name="inventário-todos", header=None, engine="openpyxl")
			coluna_14 = df.iloc[:, 14].dropna().reset_index(drop=True)
			for i in range(len(coluna_14) - 1):
				texto = str(coluna_14[i])
				if "ANO - 20" in texto:
					match = re.search(r"20\d{2}", texto)
					if match:
						ano = int(match.group())
						if ano >= 2020:
							try:
								valor_t = float(coluna_14[i + 1])
								todos_os_dados.append({"ANO": ano, "Fator Médio Anual (kgCO2/kWh)": round(valor_t, 4)})
							except ValueError:
								continue
		except Exception as e:
			logging.warning(f"Erro ao processar {arquivo.name}: {e}")

	if todos_os_dados:
		df_final = pd.DataFrame(todos_os_dados).drop_duplicates(subset=["ANO"]).sort_values("ANO")
		output_csv = pasta / "Inventario_Fator_Medio_Anual_CO2_KWh.csv"
		if output_csv.exists():
			backup_saida(output_csv)
		df_final.to_csv(output_csv, sep=';', index=False, encoding='latin1')
		logging.info(f"Dados consolidados e salvos com sucesso em: {output_csv}\n")


# === [Seção baixar_dados-030: INMET — Mirante de São Paulo] ================
# Objetivo:
#     Baixar, extrair, deduplicar e consolidar séries históricas do INMET para
#     a estação "São Paulo - Mirante" a partir de pacotes anuais ZIP.
# Fluxo:
#     baixar_extrair_e_filtrar_dados_clima_mirante_sao_paulo ->
#     baixar_e_processar_anos_dados_clima_mirante_sao_paulo ->
#     tratar_duplicatas_ano_atual_dados_clima_mirante_sao_paulo ->
#     consolidar_dados_clima_mirante_sao_paulo
# Entradas:
#     - ZIPs anuais em portal.inmet.gov.br/uploads/dadoshistoricos/<ano>.zip
# Saídas:
#     - CSV: Dados_Tratados_INMET_Mirante_De_São_Paulo.csv (sep=';')
# Contratos:
#     - Mantém somente arquivos da estação-alvo (padrao_mirante no nome)
#     - Deduplicação no ano corrente baseada em data final do nome do arquivo
# ============================================================================
def baixar_extrair_e_filtrar_dados_clima_mirante_sao_paulo():
	"""Orquestra download, extração e consolidação do INMET (Mirante SP)."""
	logging.info("Iniciando atualização dos dados do clima (INMET - Mirante São Paulo)...\n")
	destino = DADOS_INMET_DIR
	destino.mkdir(parents=True, exist_ok=True)
	ano_atual = datetime.now().year
	padrao_mirante = "INMET_SE_SP_A701_SAO PAULO - MIRANTE_"
	for ano in range(2020, ano_atual + 1):
		baixar_e_processar_anos_dados_clima_mirante_sao_paulo(ano, destino, padrao_mirante)
	tratar_duplicatas_ano_atual_dados_clima_mirante_sao_paulo(destino, padrao_mirante, ano_atual)
	consolidar_dados_clima_mirante_sao_paulo(destino, padrao_mirante)

def baixar_e_processar_anos_dados_clima_mirante_sao_paulo(ano: int, destino: Path, padrao_mirante: str):
	"""Baixa o ZIP do ano, extrai e remove arquivos que não são da estação-alvo.

	Args:
		ano (int): Ano de interesse.
		destino (Path): Pasta de trabalho.
		padrao_mirante (str): Padrão nominal da estação.
	"""
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
	"""Remove CSVs duplicados do ano corrente, mantendo o mais recente.

	Args:
		destino (Path): Pasta com os CSVs.
		padrao_mirante (str): Padrão nominal da estação.
		ano_atual (int): Ano corrente.
	"""
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
	"""Consolida CSVs da estação em um único arquivo tratado.

	Args:
		destino (Path): Pasta com os CSVs.
		padrao_mirante (str): Padrão nominal da estação.
	"""
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

		# Normalização simples de nomes com acentos/caixa para facilitar matching.
		def norm(s):
			return (s.lower()
					  .replace("ã","a").replace("á","a").replace("â","a")
					  .replace("é","e").replace("ê","e").replace("í","i")
					  .replace("ó","o").replace("ô","o").replace("õ","o")
					  .replace("ç","c").strip())
		cols_map = {c: norm(c) for c in consolidado.columns}
		consolidado.columns = [cols_map[c] for c in consolidado.columns]

		# Seleção resiliente das colunas de interesse (podem variar por ano).
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
