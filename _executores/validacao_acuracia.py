# === META =========================================================
# Módulo: validacao_acuracia
# Versão: v1.2.0
# Métricas: Erro Consumo/Horas/Temp (%)
# Saída: Historico_Precisao_Estimativas.csv
# Notas desta versão:
# - (NOVO) Gate de escrita: só recalcula e regrava quando houver **novo mês Real**
#   no ano vigente (mais meses com "Consumo (KWh)" preenchido em Tabela_Historico_Tratada.xlsx).
#   Estado leve salvo em: acuracia.state.json
# - Nomes de colunas harmonizados com o consolidado:
#   • "Temperatura Estimada (ºC)" / "Temperatura Real (ºC)"
#   • "Horas Estimadas (h)" / "Horas Reais (h)"
# - Removidas do output: "Potência Média (KW)", "Fator Utilizacao", "COP Médio", "CO2 Emitido (Kg)".
# ==================================================================

import logging
import json
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path
from _executores.utils import DADOS_CUMMINS_DIR, MESES_ORDEM, backup_saida


# === [Seção validacao_acuracia-010: Validação de acurácia das estimativas] ===
# Objetivo:
#     Comparar valores reais (histórico tratado) com estimativas e gerar um
#     histórico de precisão (erros percentuais), mas **só** regravar quando entrar
#     um novo mês Real (ano vigente).
# Fluxo:
#     _count_meses_reais_ano_vigente -> _carregar_state -> _gravar_state -> atualizar_historico_acuracia
# Entradas:
#     - Excel: BASE_DIR/dados_cummins/Tabela_Historico_Tratada.xlsx
#     - CSV  : BASE_DIR/dados_cummins/Estimativa_Consumo_Consolidado.csv
# Saídas:
#     - CSV  : BASE_DIR/dados_cummins/Historico_Precisao_Estimativas.csv (utf-8-sig; ';')
#     - JSON : BASE_DIR/dados_cummins/acuracia.state.json
# Contratos:
#     - Merge por ['Ano','Mês']; meses capitalizados; evita divisão por zero (0 → NaN)
# ============================================================================


# === [Seção validacao_acuracia-015: Estado (.state JSON)] ====================
# Objetivo:
#     Persistir/ler a contagem de meses com Consumo Real no ano vigente
#     para decidir se recalculamos a acurácia.
# ============================================================================
def _state_path() -> Path:
    return DADOS_CUMMINS_DIR / "acuracia.state.json"

def _carregar_state() -> dict:
    p = _state_path()
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _gravar_state(count_real_ano_vigente: int) -> None:
    data = {
        "count_real_ano_vigente": count_real_ano_vigente,
        "last_run": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        p = _state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"[acuracia.state] Atualizado: count_real_ano_vigente={count_real_ano_vigente}")
    except Exception as e:
        logging.warning(f"[acuracia.state] Falha ao gravar estado: {e}")


def _count_meses_reais_ano_vigente(df_real: pd.DataFrame, ano_atual: int) -> int:
    try:
        dfy = df_real[df_real["Ano"] == ano_atual]
        return int(dfy["Consumo (KWh)"].notna().sum())
    except Exception:
        return 0


# === [Seção validacao_acuracia-020: Definições de métricas] =================
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
#         Erro_Temp = ((Temperatura_Estimada - Temperatura_Real) / Temperatura_Real) * 100
# ============================================================================


def atualizar_historico_acuracia() -> None:
    """Gera/atualiza o CSV de precisão comparando Real vs. Estimado, com gate por novo mês Real."""
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
        # Real (histórico tratado)
        df_real = pd.read_excel(caminho_real, sheet_name=0, engine="openpyxl")
        df_real["Mês"] = df_real["Mês"].astype(str).str.capitalize().str.strip()

        # Gate: recalcular apenas se count_real do ano vigente aumentar
        ano_atual = int(df_real["Ano"].max()) if not df_real.empty else datetime.today().year
        count_atual = _count_meses_reais_ano_vigente(df_real, ano_atual)
        state = _carregar_state()
        count_prev = int(state.get("count_real_ano_vigente", -1))

        if count_prev >= 0 and count_atual <= count_prev:
            logging.info(
                f"[Acurácia] Nenhum novo mês Real detectado (atual={count_atual}, anterior={count_prev}) — "
                f"CSV preservado; nada a fazer."
            )
            return

        # Estimado (consolidado)
        df_estimado = pd.read_csv(caminho_estimado, sep=";", decimal=",")
        df_estimado["Mês"] = df_estimado["Mês"].astype(str).str.capitalize().str.strip()

        # Merge: preserva todos os meses com real, mesmo se faltar estimativa
        df_comparado = pd.merge(
            df_real,
            df_estimado,
            on=["Ano", "Mês"],
            how="left",
            suffixes=("_Real", "_Estimado"),
        )

        # --- Erros percentuais (protege contra divisão por zero) -----------------
        # Consumo
        den_c = df_comparado["Consumo (KWh)"].replace(0, np.nan)
        df_comparado["Erro Consumo (%)"] = (
            (df_comparado["Consumo Estimado (KWh)"] - df_comparado["Consumo (KWh)"]) / den_c
        ) * 100

        # Horas
        den_h = df_comparado["Horas Trabalhadas (h)"].replace(0, np.nan)
        df_comparado["Erro Horas (%)"] = (
            (df_comparado["Horas Estimadas (h)"] - df_comparado["Horas Trabalhadas (h)"]) / den_h
        ) * 100

        # Temperatura
        den_t = df_comparado["Temp. Média (ºC)"].replace(0, np.nan)
        if "Temperatura Estimada (ºC)" not in df_comparado.columns:
            logging.warning("Coluna ausente no consolidado: 'Temperatura Estimada (ºC)'. Erro Temp ficará NaN.")
            df_comparado["Erro Temp (%)"] = np.nan
        else:
            df_comparado["Erro Temp (%)"] = (
                (df_comparado["Temperatura Estimada (ºC)"] - df_comparado["Temp. Média (ºC)"]) / den_t
            ) * 100

        # --- Seleção e renomeação de colunas para saída --------------------------
        colunas_saida = {
            "Ano": "Ano",
            "Mês": "Mês",
            # "Tipo" vem do consolidado (pode ficar NaN quando não houver match)
            "Tipo": "Tipo",

            # Consumo
            "Consumo (KWh)": "Consumo Real (KWh)",
            "Consumo Estimado (KWh)": "Consumo Estimado (KWh)",
            "Erro Consumo (%)": "Erro Consumo (%)",

            # Horas
            "Horas Trabalhadas (h)": "Horas Reais (h)",
            "Horas Estimadas (h)": "Horas Estimadas (h)",
            "Erro Horas (%)": "Erro Horas (%)",

            # Temperatura
            "Temp. Média (ºC)": "Temperatura Real (ºC)",
            "Temperatura Estimada (ºC)": "Temperatura Estimada (ºC)",
            "Erro Temp (%)": "Erro Temp (%)",
        }

        colunas_presentes = [c for c in colunas_saida if c in df_comparado.columns]
        faltantes = [c for c in colunas_saida if c not in df_comparado.columns]
        if faltantes:
            logging.warning(f"[Acurácia] Colunas ausentes ao montar saída: {faltantes}")

        df_final = df_comparado[colunas_presentes].rename(columns=colunas_saida)

        # Ordenação por Ano/Mês:
        df_final["Mês"] = pd.Categorical(df_final["Mês"], categories=MESES_ORDEM, ordered=True)
        df_final.sort_values(by=["Ano", "Mês"], inplace=True)

        # Persistência com backup
        if caminho_saida.exists():
            backup_saida(caminho_saida)
        df_final.to_csv(caminho_saida, sep=";", decimal=",", index=False, encoding="utf-8-sig")
        logging.info(f"Histórico de acurácia salvo com sucesso em: {caminho_saida}")

        # Atualiza estado
        _gravar_state(count_atual)

    except Exception as e:
        logging.warning(f"Erro ao atualizar histórico de acurácia: {e}\n")
