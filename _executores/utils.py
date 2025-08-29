import os
import re
import sys
import shutil
import logging
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

# === [Seção utils-010: Configuração base e constantes] ======================
# Objetivo:
#     Definir diretório-base do projeto (com override por env var) e tabelas de
#     meses em PT-BR, além de mapeamento EN→PT para normalização.
# ============================================================================

def _is_frozen():
	return getattr(sys, "frozen", False)

def _default_base_dir() -> Path:
	# 1) Override por variável de ambiente (recomendado p/ ambientes restritos)
	env = os.getenv("CUMMINS_BASE_DIR")
	if env:
		return Path(env).expanduser()

	# 2) Em .exe (PyInstaller --onefile), usar pasta do executável
	if _is_frozen():
		return Path(os.path.dirname(sys.executable))

	# 3) Em desenvolvimento, raiz do projeto (utils.py está em _executores/)
	return Path(__file__).resolve().parents[1]

BASE_DIR = _default_base_dir()

# Pastas do projeto (sempre relativas ao BASE_DIR)
EXECUTORES_DIR = BASE_DIR / "_executores"
LOGS_DIR = BASE_DIR / "_logs"
DADOS_SISTEMA_ELETRICO_DIR = BASE_DIR / "dados_sistema_eletrico_brasil"
DADOS_INMET_DIR = BASE_DIR / "dados_inmet"
DADOS_CUMMINS_DIR = BASE_DIR / "dados_cummins"
TRANSICOES_DIR = DADOS_CUMMINS_DIR / "_transicoes"

# (opcional) agrupamento p/ log/inspeção
PATHS = {
	"BASE_DIR": BASE_DIR,
	"executores": EXECUTORES_DIR,
	"logs": LOGS_DIR,
	"sistema_eletrico": DADOS_SISTEMA_ELETRICO_DIR,
	"inmet": DADOS_INMET_DIR,
	"cummins": DADOS_CUMMINS_DIR,
	"transicoes": TRANSICOES_DIR,
}

MESES_ORDEM = [
	"Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
	"Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]

MESES_INGLES_PARA_PORTUGUES = {
	"january": "Janeiro", "february": "Fevereiro", "march": "Março", "april": "Abril",
	"may": "Maio", "june": "Junho", "july": "Julho", "august": "Agosto",
	"september": "Setembro", "october": "Outubro", "november": "Novembro", "december": "Dezembro"
}

def traduzir_mes_ingles_para_portugues(mes_ingles: str) -> str:
	"""Converte mês em inglês para PT-BR; devolve o original se não mapear."""
	if not isinstance(mes_ingles, str):
		return mes_ingles
	return MESES_INGLES_PARA_PORTUGUES.get(mes_ingles.strip().lower(), mes_ingles)


# === [Seção utils-015: Estrutura de pastas] =================================
# Objetivo:
#     Garantir a criação das pastas mínimas e que _executores seja pacote.
# ============================================================================
def garantir_estrutura_pastas():
	"""Cria a estrutura mínima de pastas do projeto (idempotente)."""
	for nome, p in PATHS.items():
		try:
			Path(p).mkdir(parents=True, exist_ok=True)
			# Garante que _executores é pacote Python
			if nome == "executores":
				init_file = Path(p) / "__init__.py"
				if not init_file.exists():
					init_file.touch()
		except Exception as e:
			logging.warning(f"[pastas] Falha ao criar {nome}: {e}")


# === [Seção utils-020: Logger e verificações de ambiente] ===================
# Objetivo:
#     Inicializar logging padronizado e validar versão do Python/dependências.
# ============================================================================
def inicializar_logger():
	"""Configura logging em arquivo + console e retorna o caminho do log."""
	garantir_estrutura_pastas()  # garante toda a árvore antes de logar
	nome_arquivo = f"log_execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
	arquivo_log = LOGS_DIR / nome_arquivo
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s - %(levelname)s - %(message)s",
		handlers=[logging.FileHandler(arquivo_log, encoding='utf-8'),
		          logging.StreamHandler()]
	)
	logging.info("Logger inicializado.\n")
	# Loga os caminhos atuais para auditoria
	for nome, p in PATHS.items():
		logging.info(f"[path] {nome}: {p}")
	return arquivo_log

def verificar_versao_python(versao_minima=(3, 8), versao_maxima_recomendada=(3, 13)):
	"""Garante versão mínima do Python; sugere máxima recomendada nos logs."""
	if sys.version_info < versao_minima:
		logging.error(f"Versão mínima requerida: {versao_minima}")
		sys.exit(1)
	logging.info(f"Versão do Python OK: {sys.version}")
	if sys.version_info > versao_maxima_recomendada:
		logging.info(f"Sua versão é mais nova que a recomendada {versao_maxima_recomendada}. Teste de compatibilidade sugerido.")

def verificar_dependencias():
	"""Verifica presença de libs essenciais; encerra se faltar alguma."""
	import importlib
	libs = ["httpx", "pandas", "numpy", "openpyxl", "matplotlib"]
	faltando = [lib for lib in libs if importlib.util.find_spec(lib) is None]
	if faltando:
		logging.error(f"Bibliotecas ausentes: {', '.join(faltando)}")
		sys.exit(1)


# === [Seção utils-030: Rede] ================================================
# Objetivo:
#     Checar conectividade básica à internet via HTTP (código 204/200).
# ============================================================================
def verificar_conexao_internet(url_teste="https://www.google.com/generate_204"):
	"""Retorna True se houver internet (HTTP 204/200); loga aviso caso contrário."""
	try:
		r = httpx.get(url_teste, timeout=5.0, headers={"User-Agent": "CumminsChillers/1.0"})
		if r.status_code in (204, 200):
			logging.info("Conexão com internet OK\n")
			return True
		logging.warning(f"Sem conexão com a internet (HTTP {r.status_code})\n")
		return False
	except Exception as e:
		logging.warning(f"Erro ao verificar internet: {e}\n")
		return False


# === [Seção utils-040: Utilidades de backup] ================================
# Objetivo:
#     Criar backups versionados de saídas e limitar retenção automaticamente.
# ============================================================================
def _pasta_backup_saida() -> Path:
	"""Garante/retorna a pasta de backups das saídas (dentro de dados_cummins)."""
	pasta = DADOS_CUMMINS_DIR / "_backup"
	pasta.mkdir(parents=True, exist_ok=True)
	return pasta

def limitar_backups(arquivo: Path, keep: int = 5) -> None:
	"""Mantém apenas os 'keep' backups mais recentes do arquivo informado."""
	try:
		pasta = _pasta_backup_saida()
		stem = arquivo.stem
		suffix = arquivo.suffix
		padrao_nome = re.compile(rf"^{re.escape(stem)}_backup_(\d{{8}}_\d{{6}}){re.escape(suffix)}$")
		candidatos = [p for p in pasta.glob(f"{stem}_backup_*{suffix}") if padrao_nome.match(p.name)]

		def _ts(p: Path) -> datetime:
			m = padrao_nome.match(p.name)
			return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S") if m else datetime.fromtimestamp(0)

		candidatos.sort(key=_ts, reverse=True)
		for velho in candidatos[keep:]:
			try:
				velho.unlink()
				logging.info(f"[backup] Removido backup antigo: {velho.name}")
			except Exception as e:
				logging.warning(f"[backup] Falha ao remover {velho.name}: {e}")
	except Exception as e:
		logging.warning(f"[backup] Erro ao limitar backups de {arquivo.name}: {e}")

def backup_saida(arquivo: Path, keep: int = 5) -> None:
	"""Cria backup timestampado do arquivo e aplica retenção."""
	try:
		if not arquivo.exists() or not arquivo.is_file():
			return
		pasta = _pasta_backup_saida()
		ts = datetime.now().strftime("%Y%m%d_%H%M%S")
		destino = pasta / f"{arquivo.stem}_backup_{ts}{arquivo.suffix}"
		shutil.copy2(arquivo, destino)
		logging.info(f"[backup] {arquivo.name} -> {destino.name}")
		limitar_backups(arquivo, keep=keep)
	except Exception as e:
		logging.warning(f"[backup] Falha ao criar backup de {arquivo.name}: {e}")


# === [Seção utils-050: Outras utilidades] ===================================
# Objetivo:
#     Funções de apoio: parsing de data em nome de arquivo, impressão de scripts
#     utilizados e conversão segura de valores para string.
# ============================================================================
def extrair_data_final(nome_arquivo: str):
	"""Extrai a data final 'dd-mm-aaaa' de nomes *_A_<data>.csv; senão, 0001-01-01."""
	match = re.search(r'_A_(\d{2}-\d{2}-\d{4})\.csv$', nome_arquivo)
	if match:
		return datetime.strptime(match.group(1), "%d-%m-%Y").date()
	return datetime.min.date()

def Imprimir_Todos_Scripts(lista_scripts):
	"""Salva um .txt com o conteúdo de todos os scripts informados (para auditoria)."""
	try:
		logging.info("Iniciando salvamento dos scripts utilizados...\n")
		LOGS_DIR.mkdir(exist_ok=True)
		nome_arquivo_saida = f"Scripts_Utilizados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
		destino = LOGS_DIR / nome_arquivo_saida
		with open(destino, 'w', encoding='utf-8') as f_dest:
			f_dest.write(f"Scripts utilizados - Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
			f_dest.write("="*70 + "\n")
			for script_path in lista_scripts:
				caminho_script = Path(script_path)
				if not caminho_script.exists():
					logging.warning(f"Script não encontrado: {caminho_script}")
					continue
				f_dest.write("\n" + "="*70 + "\n")
				f_dest.write(f">>> INÍCIO DO SCRIPT: {caminho_script.name}\n")
				f_dest.write("="*70 + "\n\n")
				with open(caminho_script, 'r', encoding='utf-8') as f_src:
					f_dest.write(f_src.read())
				f_dest.write("\n\n" + "="*70 + "\n")
				f_dest.write(f">>> FIM DO SCRIPT: {caminho_script.name}\n")
				f_dest.write("="*70 + "\n")
		logging.info(f"Scripts utilizados salvos com sucesso em: {destino}\n")
		return destino
	except Exception as e:
		logging.warning(f"Erro ao salvar scripts utilizados: {e}\n")

def safe_str(valor, decimais=2):
	"""Converte valor para string; números são arredondados, NaN/None → 'Não'."""
	if valor is None or pd.isna(valor):
		return "Não"
	try:
		return str(round(valor, decimais)) if isinstance(valor, (float, int)) else str(valor)
	except Exception:
		return str(valor)