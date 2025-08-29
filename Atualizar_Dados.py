from pathlib import Path 
import logging
import time
import sys

# Importações dos módulos
from _executores.utils import BASE_DIR, inicializar_logger, verificar_versao_python, verificar_dependencias, verificar_conexao_internet, Imprimir_Todos_Scripts, garantir_estrutura_pastas
from _executores.baixar_dados import baixar_extrair_e_filtrar_dados_clima_mirante_sao_paulo, baixar_extrair_e_filtrar_dados_sistema_eletrico
from _executores.tratamento_dados import gerar_csv_geolocalizacao_guarulhos, mover_arquivo_chiller_para_dados_cummins, tratar_arquivo_kwh_para_dados_cummins, inserir_consumo_total_cummins, construir_tabela_historico_de_chiller
from _executores.estimativas_dados import gerar_estimativas
from _executores.validacao_acuracia import atualizar_historico_acuracia

def testar_importacoes_scripts():
	"""Testa apenas os imports dos módulos internos do sistema."""
	logging.info("Iniciando teste de importação dos scripts internos...")

	modulos = {
		"utils": "_executores.utils",
		"baixar_dados": "_executores.baixar_dados",
		"tratamento_dados": "_executores.tratamento_dados",
		"estimativas_dados": "_executores.estimativas_dados",
		"validacao_acuracia": "_executores.validacao_acuracia",
	}

	for nome, modulo in modulos.items():
		try:
			__import__(modulo)
			logging.info(f"Módulo '{nome}' importado com sucesso.")
		except Exception as e:
			logging.error(f"Falha ao importar módulo '{nome}': {e}\n")
			return False

	logging.info("Teste de importação dos scripts concluído com sucesso.\n")
	return True

if __name__ == "__main__":
	inicio = time.time()

	# Garante toda a estrutura de pastas antes de iniciar logs e execuções
	garantir_estrutura_pastas()

	arquivo_log = inicializar_logger()
	logging.info(f"Log sendo salvo em: {arquivo_log}\n")

	if not testar_importacoes_scripts():
		logging.error("Execução interrompida devido a falha nos imports.\n")
		sys.exit(1)

	try:
		# Etapas de verificação
		verificar_versao_python()
		verificar_dependencias()
		verificar_conexao_internet()

		# Funções que dependem de internet
		baixar_extrair_e_filtrar_dados_clima_mirante_sao_paulo()
		baixar_extrair_e_filtrar_dados_sistema_eletrico()

		# Funções locais
		gerar_csv_geolocalizacao_guarulhos()
		mover_arquivo_chiller_para_dados_cummins()
		tratar_arquivo_kwh_para_dados_cummins()
		inserir_consumo_total_cummins()

		# Histórico completo
		construir_tabela_historico_de_chiller(potencia_min_kw=0.0)

		# Funções de estimativas
		gerar_estimativas()
		atualizar_historico_acuracia()
		gerar_estimativas()

	except Exception:
		logging.exception("Erro inesperado durante a execução:\n")

	fim = time.time()
	minutos, segundos = divmod(fim - inicio, 60)

	logging.info("Atualização concluída com sucesso. Salvando scripts utilizados nos logs...\n")
	scripts_utilizados = [
		sys.modules["_executores.utils"].__file__,
		sys.modules["_executores.baixar_dados"].__file__,
		sys.modules["_executores.tratamento_dados"].__file__,
		sys.modules["_executores.estimativas_dados"].__file__,
		sys.modules["_executores.validacao_acuracia"].__file__,
		__file__,  # O próprio Atualizar_Dados.py
	]
	Imprimir_Todos_Scripts(scripts_utilizados)
	logging.info(f"Tempo total de execução: {int(minutos)} minuto(s) e {int(segundos)} segundo(s)\n")