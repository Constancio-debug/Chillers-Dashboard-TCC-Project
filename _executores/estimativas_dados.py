# === META =========================================================
# Módulo: estimativas_dados
# Versão: v2.7.1
# Modelo: baseline_historico_v1 (IQR + peso temporal + viés)
# Saída: Estimativa_Consumo_Consolidado.csv
# Notas desta versão:
# - Mantido: mês atual tratado como **Projetado** (sem pró-rata) — v2.6.0+.
# - Congelamento de estimativas para meses não-"Projetado":
#     • Não alteramos as colunas de “Estimado” (Consumo/Temperatura/Horas) quando o **Tipo ATUAL** não é “Projetado”.
#       (Ajuste em v2.7.1: o critério de congelamento passou a olhar o **Tipo atual**, não o anterior,
#        garantindo preservação do último projetado quando o mês vira Real/Estimado-Passado.)
# - Estado leve (.state JSON) para auditoria de entradas: hash do histórico de temperatura e contagem de meses Reais.
# - Tipos possíveis: "Real", "Estimado-Passado", "Projetado".
# - Permanecem removidas do OUTPUT as colunas legadas:
#     • "Temp Estimada (ºC)", "Temp Hist Corr (ºC)", "Horas Hist Corr (h)".
# ==================================================================

import logging
from datetime import datetime
from pathlib import Path
import hashlib
import json
import pandas as pd
import numpy as np

from _executores.utils import DADOS_CUMMINS_DIR, MESES_ORDEM, backup_saida


# === [Seção estimativas_dados-010: Utilitários de estatística e estimativa mensal] ===
# Objetivo:
#     Helpers para média histórica corrigida (IQR), diferença percentual,
#     estimativa mensal (média histórica), média histórica simples do mês e σ_robusto (MAD→σ).
# Fluxo:
#     media_historica_corrigida -> calcular_diferenca -> estimar_consumo_mensal
#     -> consumo_historico_mensal -> sigma_robusto_mensal
# Entradas:
#     - DataFrame histórico consolidado (Tratamento)
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
    """
    Estima consumo mensal com base na média histórica do mês (anos < ano).
    Observação: o pró-rata do mês corrente foi removido em v2.6.0.
    """
    colunas_esperadas = ["Ano", "Mês", "Consumo (KWh)"]
    for col in colunas_esperadas:
        if col not in df.columns:
            return {"min": None, "esperado": None, "max": None}

    df_mes = df[df["Mês"] == mes]
    if ano is not None:
        df_mes = df_mes[df_mes["Ano"] < ano]
    serie = df_mes["Consumo (KWh)"].dropna()
    if serie.empty:
        return {"min": None, "esperado": None, "max": None}

    # Esperado = média simples histórica do mês
    esperado = float(serie.mean())

    # Faixa robusta (σ via MAD; fallback para desvio-padrão)
    mediana = float(serie.median())
    mad = float(np.median(np.abs(serie - mediana)))
    sigma_robusto = 1.4826 * mad if mad > 0 else float(serie.std(ddof=0))
    minimo = max(0, esperado - sigma_robusto)
    maximo = esperado + sigma_robusto

    return {
        "min": round(minimo, 2) if pd.notnull(minimo) else None,
        "esperado": round(esperado, 2) if pd.notnull(esperado) else None,
        "max": round(maximo, 2) if pd.notnull(maximo) else None
    }


def consumo_historico_mensal(df: pd.DataFrame, mes: str, ano_atual: int):
    """Média histórica de Consumo (KWh) para o mês, considerando apenas anos < ano_atual."""
    if "Consumo (KWh)" not in df.columns:
        return None
    serie = df[(df["Mês"] == mes) & (df["Ano"] < ano_atual)]["Consumo (KWh)"].dropna()
    return float(serie.mean()) if not serie.empty else None


def sigma_robusto_mensal(df: pd.DataFrame, mes: str, ano_atual: int):
    """σ_robusto (via MAD) do histórico do mês (anos < ano_atual)."""
    if "Consumo (KWh)" not in df.columns:
        return None
    serie = df[(df["Mês"] == mes) & (df["Ano"] < ano_atual)]["Consumo (KWh)"].dropna()
    if serie.empty:
        return None
    mediana = float(serie.median())
    mad = float(np.median(np.abs(serie - mediana)))
    sigma_robusto = 1.4826 * mad if mad > 0 else float(serie.std(ddof=0))
    return float(sigma_robusto)


# === [Seção estimativas_dados-015: Utilitários de estado (.state JSON)] ==============
# Objetivo:
#     Gerar e persistir metadados de entrada (hash de temperaturas históricas e
#     contagem de meses reais do ano vigente) para auditoria.
# Notas:
#     Nesta versão, o .state é apenas informativo (não bloqueia execução).
# =====================================================================================
def _hash_temp_historica(df_real: pd.DataFrame, ano_atual: int) -> str | None:
    try:
        cols_ok = {"Ano", "Mês", "Temp. Média (ºC)"}
        if not cols_ok.issubset(set(df_real.columns)):
            return None
        base = (
            df_real.loc[df_real["Ano"] < ano_atual, ["Ano", "Mês", "Temp. Média (ºC)"]]
            .dropna(subset=["Temp. Média (ºC)"])
            .copy()
        )
        if base.empty:
            return None
        base["Mês"] = base["Mês"].astype(str).str.capitalize().str.strip()
        base = base.sort_values(["Ano", "Mês"])
        payload = base.to_csv(index=False).encode("utf-8")
        return hashlib.md5(payload).hexdigest()
    except Exception:
        return None


def _count_meses_reais_ano_vigente(df_real: pd.DataFrame, ano_atual: int) -> int:
    try:
        if "Consumo (KWh)" not in df_real.columns or "Ano" not in df_real.columns:
            return 0
        dfy = df_real[df_real["Ano"] == ano_atual]
        return int(dfy["Consumo (KWh)"].notna().sum())
    except Exception:
        return 0


def _gravar_state(caminho_state: Path, hash_temp: str | None, count_real: int) -> None:
    try:
        data = {
            "hash_temp_hist": hash_temp,
            "count_real_ano_vigente": count_real,
            "last_run": datetime.now().isoformat(timespec="seconds"),
        }
        caminho_state.parent.mkdir(parents=True, exist_ok=True)
        with open(caminho_state, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"[state] Estado de entradas atualizado em: {caminho_state.name}")
    except Exception as e:
        logging.warning(f"[state] Falha ao gravar estado: {e}")


# === [Seção estimativas_dados-020: Viés histórico (leitura e aplicação)] =============
# Objetivo:
#     Ler viés médio a partir de 'Historico_Precisao_Estimativas.csv'
#     e disponibilizar viés global e por mês, quando houver dados suficientes.
# Fluxo:
#     carregar_vies_historico
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
#     Estimar consumo/temperatura/horas para o ano atual.
# Notas:
#     - Sem pró-rata no mês corrente. Mês atual é **Projetado** (histórico + viés).
#     - Deltas de temperatura e horas: Estimado vs Real; nulos se não houver Real.
# ============================================================================
def estimar_consumo_ano_vigente(df: pd.DataFrame) -> pd.DataFrame:
    """Gera estimativas para o ano atual (sem pró-rata; mês atual como Projetado)."""
    hoje = datetime.today()
    ano_atual = hoje.year
    mes_atual = hoje.month

    vies_global, vies_mensal = carregar_vies_historico()
    linhas = []
    for idx, mes_nome in enumerate(MESES_ORDEM, start=1):
        df_mes_atual = df[(df["Ano"] == ano_atual) & (df["Mês"] == mes_nome)]

        # Referências históricas (temperatura/horas) — internas
        temp_hist_corr = media_historica_corrigida(df, "Temp. Média (ºC)", mes_nome, ano_atual)
        horas_hist_corr = media_historica_corrigida(df, "Horas Trabalhadas (h)", mes_nome, ano_atual)

        # (Consumo) histórico simples e σ_robusto (para Min/Max do consumo)
        consumo_hist = consumo_historico_mensal(df, mes_nome, ano_atual)
        sigma_hist = sigma_robusto_mensal(df, mes_nome, ano_atual)

        # Real do mês (consumo, temperatura e horas)
        consumo_real = None
        temp_real = None
        horas_reais = None
        if not df_mes_atual.empty:
            if "Consumo (KWh)" in df_mes_atual.columns:
                v = df_mes_atual["Consumo (KWh)"].values[0]
                if pd.notnull(v):
                    consumo_real = float(v)
            if "Temp. Média (ºC)" in df_mes_atual.columns:
                t = df_mes_atual["Temp. Média (ºC)"].values[0]
                if pd.notnull(t):
                    temp_real = float(t)
            if "Horas Trabalhadas (h)" in df_mes_atual.columns:
                h = df_mes_atual["Horas Trabalhadas (h)"].values[0]
                if pd.notnull(h):
                    horas_reais = float(h)

        # Define Tipo e estimativas
        if idx < mes_atual:
            # Meses passados: "Real" se houver Consumo Real; senão "Estimado-Passado"
            tipo = "Real" if consumo_real is not None else "Estimado-Passado"
            # Temperatura/Horas estimadas: se houver real, usa real; senão histórica corrigida
            temp_estim = temp_real if temp_real is not None else temp_hist_corr
            horas_estim = horas_reais if horas_reais is not None else horas_hist_corr
            # Consumo Estimado: média histórica do mês (sem pró-rata)
            consumo_estimado = estimar_consumo_mensal(df, mes_nome, ano_atual)["esperado"]

        else:
            # Mês atual e futuros → Projetado (histórico + viés)
            tipo = "Projetado"
            temp_estim = temp_hist_corr
            horas_estim = horas_hist_corr
            base = estimar_consumo_mensal(df, mes_nome, ano_atual)["esperado"]
            if base is not None:
                ajuste = vies_mensal.get(mes_nome, vies_global)
                consumo_estimado = round(base * (1 + ajuste / 100), 2) if ajuste is not None else base
            else:
                consumo_estimado = None

        # Min/Max ao redor do Estimado (consumo)
        if consumo_estimado is not None and sigma_hist is not None:
            consumo_min = round(max(0.0, consumo_estimado - sigma_hist), 2)
            consumo_max = round(consumo_estimado + sigma_hist, 2)
        else:
            consumo_min = None
            consumo_max = None

        # Deltas Estimado vs Real (consumo)
        if consumo_real is not None and consumo_estimado is not None and consumo_real != 0:
            delta_consumo = round(consumo_estimado - consumo_real, 2)
            tendencia_consumo_pct = round((delta_consumo / consumo_real) * 100, 2)
        else:
            delta_consumo = None
            tendencia_consumo_pct = None

        # Temperatura — colunas + deltas (Estimado vs Real)
        temperatura_real = temp_real
        temperatura_historica = temp_hist_corr
        temperatura_estimada = temp_estim
        if (temperatura_real is not None) and (temperatura_estimada is not None) and (temperatura_real != 0):
            delta_temp = round(temperatura_estimada - temperatura_real, 2)
            tendencia_temp_pct = round((delta_temp / temperatura_real) * 100, 2)
        else:
            delta_temp = None
            tendencia_temp_pct = None

        # Horas — colunas + deltas (Estimado vs Real)
        horas_historicas = horas_hist_corr
        if (horas_reais is not None) and (horas_estim is not None) and (horas_reais != 0):
            delta_horas = round(horas_estim - horas_reais, 2)
            tendencia_horas_pct = round((delta_horas / horas_reais) * 100, 2)
        else:
            delta_horas = None
            tendencia_horas_pct = None

        # Linha final — OUTPUT sem colunas legadas
        linhas.append({
            "Ano": ano_atual, "Mês": mes_nome, "Tipo": tipo,

            # Consumo
            "Consumo Real (KWh)": consumo_real,
            "Consumo Estimado (KWh)": consumo_estimado,
            "Consumo Histórico (KWh)": consumo_hist,
            "Consumo Min (KWh)": consumo_min,
            "Consumo Max (KWh)": consumo_max,
            "Δ Consumo (KWh)": delta_consumo,
            "Tendência Consumo (%)": tendencia_consumo_pct,

            # Temperatura
            "Temperatura Real (ºC)": temperatura_real,
            "Temperatura Estimada (ºC)": temperatura_estimada,
            "Temperatura Histórica (ºC)": temperatura_historica,
            "Δ Temp (ºC)": delta_temp,
            "Tendência Temp (%)": tendencia_temp_pct,

            # Horas
            "Horas Reais (h)": horas_reais,
            "Horas Históricas (h)": horas_historicas,
            "Horas Estimadas (h)": horas_estim,
            "Δ Horas (h)": delta_horas,
            "Tendência Horas (%)": tendencia_horas_pct,
        })
    return pd.DataFrame(linhas)


# === [Seção estimativas_dados-040: Estimativas — ano seguinte] ===============
# Objetivo:
#     Projetar consumo/temperatura/horas para o próximo ano (sem dados reais).
# Notas:
#     - Deltas de temperatura e horas ficam nulos (não há Real no ano seguinte).
# ============================================================================
def estimar_consumo_ano_seguinte(df: pd.DataFrame) -> pd.DataFrame:
    """Gera estimativas projetadas para o próximo ano usando histórico + viés (temperatura e horas sem deltas)."""
    hoje = datetime.today()
    ano_futuro = hoje.year + 1
    vies_global, vies_mensal = carregar_vies_historico()
    linhas = []
    for mes_nome in MESES_ORDEM:
        temp_hist_corr = media_historica_corrigida(df, "Temp. Média (ºC)", mes_nome, hoje.year)
        horas_hist_corr = media_historica_corrigida(df, "Horas Trabalhadas (h)", mes_nome, hoje.year)

        consumo_hist = consumo_historico_mensal(df, mes_nome, hoje.year)
        sigma_hist = sigma_robusto_mensal(df, mes_nome, hoje.year)

        # Consumo estimado + viés
        base = estimar_consumo_mensal(df, mes_nome, ano_futuro)["esperado"]
        if base is not None:
            ajuste = vies_mensal.get(mes_nome, vies_global)
            consumo_estimado = round(base * (1 + ajuste / 100), 2) if ajuste is not None else base
        else:
            consumo_estimado = None

        if consumo_estimado is not None and sigma_hist is not None:
            consumo_min = round(max(0.0, consumo_estimado - sigma_hist), 2)
            consumo_max = round(consumo_estimado + sigma_hist, 2)
        else:
            consumo_min = None
            consumo_max = None

        # Temperatura e horas — no ano seguinte não há real; estimadas = históricas
        temperatura_real = None
        temperatura_historica = temp_hist_corr
        temperatura_estimada = temp_hist_corr

        horas_reais = None
        horas_historicas = horas_hist_corr
        horas_estim = horas_hist_corr

        linhas.append({
            "Ano": ano_futuro, "Mês": mes_nome, "Tipo": "Projetado",

            # Consumo
            "Consumo Real (KWh)": None,
            "Consumo Estimado (KWh)": consumo_estimado,
            "Consumo Histórico (KWh)": consumo_hist,
            "Consumo Min (KWh)": consumo_min,
            "Consumo Max (KWh)": consumo_max,
            "Δ Consumo (KWh)": None,
            "Tendência Consumo (%)": None,

            # Temperatura
            "Temperatura Real (ºC)": temperatura_real,
            "Temperatura Estimada (ºC)": temperatura_estimada,
            "Temperatura Histórica (ºC)": temperatura_historica,
            "Δ Temp (ºC)": None,
            "Tendência Temp (%)": None,

            # Horas
            "Horas Reais (h)": horas_reais,
            "Horas Históricas (h)": horas_historicas,
            "Horas Estimadas (h)": horas_estim,
            "Δ Horas (h)": None,
            "Tendência Horas (%)": None
        })
    return pd.DataFrame(linhas)


# === [Seção estimativas_dados-050: Consolidação, congelamento e exportação] ==========
# Objetivo:
#     Montar o arquivo 'Estimativa_Consumo_Consolidado.csv' a partir das
#     estimativas do ano vigente e do ano seguinte, **congelando** colunas
#     de Estimado para meses não-"Projetado" com base no CSV anterior.
# Fluxo:
#     - ler histórico tratado (input principal)
#     - gerar v_atual (ano vigente) e v_prox (ano seguinte)
#     - (se existir) ler CSV anterior e preservar estimados quando Tipo ATUAL != "Projetado"
#     - salvar CSV + estado (.state JSON) para auditoria
# Contratos:
#     - Arredondamento de colunas numéricas a 2 casas; backup antes de sobrescrever.
#     - Preserva esquema (não remove colunas inteiras se NaN); descarta linhas totalmente vazias.
# =====================================================================================
def gerar_estimativas():
    """Orquestra a geração de estimativas, congela meses não-'Projetado' e salva o CSV."""
    logging.info("Iniciando criação e consolidação dos dados de Estimativas...")

    caminho_real = DADOS_CUMMINS_DIR / "Tabela_Historico_Tratada.xlsx"
    caminho_out = DADOS_CUMMINS_DIR / "Estimativa_Consumo_Consolidado.csv"
    caminho_state = DADOS_CUMMINS_DIR / "estimativas.state.json"

    if not caminho_real.exists():
        logging.warning("Arquivo 'Tabela_Historico_Tratada.xlsx' não encontrado.")
        return

    # --- Leitura do histórico tratado -------------------------------------------------
    df = pd.read_excel(caminho_real, engine="openpyxl")
    df["Mês"] = pd.Categorical(df["Mês"], categories=MESES_ORDEM, ordered=True)

    # Metadados de auditoria (.state)
    hoje = datetime.today()
    ano_atual = hoje.year
    _hash = _hash_temp_historica(df, ano_atual)
    _count_real = _count_meses_reais_ano_vigente(df, ano_atual)

    # --- Geração das estimativas (ano vigente + ano seguinte) -------------------------
    df_ano = estimar_consumo_ano_vigente(df)
    df_proximo_ano = estimar_consumo_ano_seguinte(df)

    partes = []
    for dfx in (df_ano, df_proximo_ano):
        if dfx is None or dfx.empty:
            continue
        dfx_limpo = dfx.dropna(axis=0, how="all")
        if not dfx_limpo.empty and dfx_limpo.shape[1] > 0:
            partes.append(dfx_limpo)

    if partes:
        df_novo = pd.concat(partes, ignore_index=True)
    else:
        logging.warning("[estimativas] Nenhum dado válido para consolidar estimativas.")
        df_novo = pd.DataFrame(columns=[
            "Ano","Mês","Tipo",
            # consumo
            "Consumo Real (KWh)","Consumo Estimado (KWh)","Consumo Histórico (KWh)",
            "Consumo Min (KWh)","Consumo Max (KWh)","Δ Consumo (KWh)","Tendência Consumo (%)",
            # temperatura
            "Temperatura Real (ºC)","Temperatura Estimada (ºC)","Temperatura Histórica (ºC)",
            "Δ Temp (ºC)","Tendência Temp (%)",
            # horas
            "Horas Reais (h)","Horas Históricas (h)","Horas Estimadas (h)",
            "Δ Horas (h)","Tendência Horas (%)"
        ])

    # --- Congelamento: preservar estimados anteriores quando Tipo ATUAL != "Projetado" -
    if caminho_out.exists():
        try:
            df_ant = pd.read_csv(caminho_out, sep=";", decimal=",")
            df_ant["Mês"] = df_ant["Mês"].astype(str).str.capitalize().str.strip()
            df_novo["Mês"] = df_novo["Mês"].astype(str).str.capitalize().str.strip()

            # Merge para ter acesso aos estimados anteriores
            chave = ["Ano", "Mês"]
            df_merge = df_novo.merge(
                df_ant[chave + [
                    "Consumo Estimado (KWh)", "Consumo Min (KWh)", "Consumo Max (KWh)",
                    "Temperatura Estimada (ºC)", "Horas Estimadas (h)"
                ]].rename(columns={
                    "Consumo Estimado (KWh)": "Consumo Estimado (KWh)_ant",
                    "Consumo Min (KWh)": "Consumo Min (KWh)_ant",
                    "Consumo Max (KWh)": "Consumo Max (KWh)_ant",
                    "Temperatura Estimada (ºC)": "Temperatura Estimada (ºC)_ant",
                    "Horas Estimadas (h)": "Horas Estimadas (h)_ant",
                }),
                on=chave, how="left"
            )

            # CONGELAR quando o TIPO ATUAL não é "Projetado"
            mask_freeze = (df_merge["Tipo"].notna()) & (df_merge["Tipo"] != "Projetado")

            for col_new, col_old in [
                ("Consumo Estimado (KWh)", "Consumo Estimado (KWh)_ant"),
                ("Consumo Min (KWh)", "Consumo Min (KWh)_ant"),
                ("Consumo Max (KWh)", "Consumo Max (KWh)_ant"),
                ("Temperatura Estimada (ºC)", "Temperatura Estimada (ºC)_ant"),
                ("Horas Estimadas (h)", "Horas Estimadas (h)_ant"),
            ]:
                if col_new in df_merge.columns and col_old in df_merge.columns:
                    df_merge.loc[mask_freeze & df_merge[col_old].notna(), col_new] = df_merge.loc[
                        mask_freeze & df_merge[col_old].notna(), col_old
                    ]

            # Limpeza de colunas auxiliares
            drop_aux = [c for c in df_merge.columns if c.endswith("_ant")]
            df_novo = df_merge.drop(columns=drop_aux, errors="ignore")

            logging.info("Congelamento aplicado: estimativas preservadas para meses não-'Projetado'.")
        except Exception as e:
            logging.warning(f"Falha ao aplicar congelamento de estimativas: {e}")

    # --- Arredondamento numérico (2 casas) -------------------------------------------
    for col in df_novo.select_dtypes(include=[float, int]).columns:
        df_novo[col] = df_novo[col].map(lambda x: round(x, 2) if pd.notnull(x) else x)

    # --- Persistência ----------------------------------------------------------------
    try:
        if caminho_out.exists():
            backup_saida(caminho_out)
        df_novo.to_csv(caminho_out, index=False, encoding="utf-8-sig", sep=";", decimal=",")
        logging.info(f"Estimativas salvas com sucesso em: {caminho_out}")
    except PermissionError:
        logging.error(f"Não foi possível salvar '{caminho_out.name}' pois está aberto.")
    except Exception as e:
        logging.critical(f"Erro ao salvar '{caminho_out.name}': {e}")

    # --- Gravar estado (.state JSON) -------------------------------------------------
    _gravar_state(caminho_state, _hash, _count_real)