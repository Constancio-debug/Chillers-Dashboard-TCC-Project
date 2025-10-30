# === META =========================================================
# Componente: Orquestrador (Atualizar_dados.py)
# Versão: v8.0.0
# Modelos usados: estimativas_dados@v1.0.0, validacao_acuracia@v1.0.0
# Observação: este arquivo reúne e loga as versões dos módulos
# ==================================================================

from pathlib import Path  
import shutil
import logging
import time
import sys
import os
import stat

# Importações dos módulos
from _executores.utils import (
    BASE_DIR, INSTALADORES_DIR, inicializar_logger, verificar_versao_python,
    verificar_dependencias, verificar_conexao_internet, Imprimir_Todos_Scripts,
    garantir_estrutura_pastas
)

# === AJUSTADO: antes vinha de _executores.baixar_dados ===
from _executores.baixar_inmet import (baixar_inmet)
from _executores.baixar_CO2_sistema_eletrico import (baixar_CO2_sistema_eletrico)
from _executores.baixar_bandeiras_tarifarias import (baixar_csvs_bandeiras_tarifarias)

from _executores.tratamento_dados import (
    gerar_csv_geolocalizacao_guarulhos, 
    mover_arquivo_chiller_para_dados_cummins,
)

from _executores.tratamento_consumo import (  
    tratar_arquivo_kwh_para_dados_cummins, 
    inserir_consumo_total_cummins,
)

from _executores.tratamento_tabela_historica import (   
    construir_tabela_historico_de_chiller,
)

from _executores.tratamento_calendario import gerar_calendario_xlsx

from _executores.estimativas_dados import gerar_estimativas
from _executores.validacao_acuracia import atualizar_historico_acuracia

def testar_importacoes_scripts() -> bool:
    """Testa apenas os imports dos módulos internos do sistema."""
    logging.info("Iniciando teste de importação dos scripts internos...")
    modulos = {
        "utils": "_executores.utils",
        "baixar_inmet": "_executores.baixar_inmet",   
        "baixar_bandeiras_tarifarias": "_executores.baixar_bandeiras_tarifarias",  
        "baixar_CO2_sistema_eletrico": "_executores.baixar_CO2_sistema_eletrico",  
        "tratamento_dados": "_executores.tratamento_dados",
        "tratamento_consumo": "_executores.tratamento_consumo",               
        "tratamento_tabela_historica": "_executores.tratamento_tabela_historica",  
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


# ===== MOVER ARQUIVOS (resiliente a bloqueios do Windows) ===================
def _tornar_gravavel(p: Path):
    try:
        p.chmod(p.stat().st_mode | stat.S_IWRITE)
    except Exception:
        pass

def _mover_com_resiliencia(origem: Path, destino_dir: Path, tentativas: int = 6, espera_s: float = 0.5) -> bool:
    destino_dir.mkdir(parents=True, exist_ok=True)
    destino = destino_dir / origem.name

    if destino.exists():
        _tornar_gravavel(destino)
        try:
            destino.unlink()
            logging.info(f"Destino existente removido: {destino}")
        except Exception as e:
            logging.warning(f"Não consegui remover destino existente: {destino} ({e})")

    for i in range(1, tentativas + 1):
        try:
            shutil.move(str(origem), str(destino))
            logging.info(f"Movido: {origem} -> {destino}")
            return True
        except PermissionError:
            try:
                shutil.copy2(str(origem), str(destino))
                _tornar_gravavel(origem)
                os.remove(str(origem))
                logging.info(f"Copiado e apagado (fallback): {origem} -> {destino}")
                return True
            except Exception as e2:
                if i == tentativas:
                    logging.error(f"Permissão negada ao mover {origem} após {tentativas} tentativas. Último erro: {e2}")
                    return False
                time.sleep(espera_s)
        except Exception as e:
            if i == tentativas:
                logging.error(f"Falha ao mover {origem}: {e}")
                return False
            time.sleep(espera_s)

def mover_arquivos():
    destino = INSTALADORES_DIR
    arquivos = [
        BASE_DIR / "Cummins Chillers Dashboard.rar",
        BASE_DIR / "CumminsDashboardInstaller.exe",
    ]
    for arq in arquivos:
        if arq.exists():
            ok = _mover_com_resiliencia(arq, destino)
            if not ok:
                logging.error(f"Falha ao mover: {arq}")
        else:
            logging.info(f"Arquivo não encontrado: {arq.name}")


if __name__ == "__main__":
    inicio = time.time()

    arquivo_log = inicializar_logger()
    logging.info(f"Log sendo salvo em: {arquivo_log}\n")

    if not testar_importacoes_scripts():
        logging.error("Execução interrompida devido a falha nos imports.\n")
        sys.exit(1)

    try:
        #Funções de verificação de dependências
        verificar_versao_python()
        verificar_dependencias()
        verificar_conexao_internet()
        mover_arquivos()

        #Funções que baixam dados
        #baixar_inmet()
        baixar_CO2_sistema_eletrico()
        baixar_csvs_bandeiras_tarifarias()
        
        #Funções que tratam dados
        gerar_csv_geolocalizacao_guarulhos()
        mover_arquivo_chiller_para_dados_cummins()
        tratar_arquivo_kwh_para_dados_cummins()
        inserir_consumo_total_cummins()

        #Construção de histórico
        construir_tabela_historico_de_chiller(potencia_min_kw=0.0)
        gerar_calendario_xlsx()
        
        #Funções de estimativas e acurácia
        gerar_estimativas()
        atualizar_historico_acuracia()

    except Exception:
        logging.exception("Erro inesperado durante a execução:\n")

    fim = time.time()
    minutos, segundos = divmod(fim - inicio, 60)

    logging.info("Atualização concluída com sucesso. Salvando scripts utilizados nos logs...\n")

    if getattr(sys, "frozen", False):
        logging.warning("Ambiente congelado (PyInstaller): caminhos dos scripts fonte não estão disponíveis.")
        for nome in ["_executores.utils", "_executores.baixar_inmet",
                     "_executores.baixar_CO2_sistema_eletrico",
                     "_executores.baixar_bandeiras_tarifarias",
                     "_executores.tratamento_dados", 
                     "_executores.tratamento_consumo",              
                     "_executores.tratamento_tabela_historica",
                     "_executores.tratamento_calendario",    
                     "_executores.estimativas_dados",
                     "_executores.validacao_acuracia", "__main__"]:
            logging.info(f"Módulo carregado: {nome}")
    else:
        scripts_utilizados = [
            sys.modules["_executores.utils"].__file__,
            sys.modules["_executores.baixar_inmet"].__file__,
            sys.modules["_executores.baixar_CO2_sistema_eletrico"].__file__,
            sys.modules["_executores.baixar_bandeiras_tarifarias"].__file__,
            sys.modules["_executores.tratamento_dados"].__file__,
            sys.modules["_executores.tratamento_consumo"].__file__,          
            sys.modules["_executores.tratamento_tabela_historica"].__file__,
            sys.modules["_executores.tratamento_calendario"].__file__,
            sys.modules["_executores.estimativas_dados"].__file__,
            sys.modules["_executores.validacao_acuracia"].__file__,
            __file__,
        ]
        Imprimir_Todos_Scripts(scripts_utilizados)

    logging.info(f"Tempo total de execução: {int(minutos)} minuto(s) e {int(segundos)} segundo(s)\n")