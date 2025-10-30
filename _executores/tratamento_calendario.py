# === META ========================================================= 
# Módulo: tratamento_calendario
# Versão: v2.0.0 (padrão de seções alinhado a tratamento_tabela_historica)
# Consolidação: estrutura base de calendário + INMET + CO₂ + KWh + Chillers (médias diárias)
# Saída: Calendario.xlsx
# Notas desta versão:
#   - Passa a incluir integração direta com as médias diárias do Chiller.
#   - Adicionadas colunas de referência temporal: Consumo/Gasto/COP (Mês e Ano anterior).
#   - "Potencia_Media_Dia_KW" obtida a partir de Dados do Chiller [Médias Diárias].
#   - "Gasto_Operacao_R$" recalculado automaticamente (Consumo_KWh × Valor_do_KWh_Em_Vigor).
#   - Implementado suporte à exceção de 29/02 (buscando 28/02 em lookbacks).
#   - Logs aprimorados para contagem e diagnóstico por coluna e integração.
# ==================================================================

import logging
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np

# Apenas o que é necessário do utils
from _executores.utils import backup_saida

# === [Seção tratamento_calendario-010: Configurações e constantes] ===================
# Objetivo:
#     Centralizar caminhos, nomes de planilhas, lista de colunas alvo e
#     dicionários de meses/dias em pt-BR. Facilita manutenção e reutilização.
# Conteúdo:
#     - ARQUIVO_PADRAO, NOME_ABA, COLUNAS_CALENDARIO
#     - mapas MESES_PT e DIAS_PT
#     - DATA_INICIAL e paths para INMET, KWh, CO2 e Chiller
#     - limiar de ligado (kW) para derivar horas trabalhadas
# =====================================================================================
ARQUIVO_PADRAO = "Calendario.xlsx"
NOME_ABA = "Calendario"

COLUNAS_CALENDARIO = [
    # Chave do dia e metadados
    "Calendario", "Semana do Ano", "Nome do Mês", "Nome do Dia", "Ano", "Mês",
    "Mês_Seguinte", "Ano_Seguinte",
    # Métricas operacionais (preenchidas por INMET/Chiller/CO2/KWh quando disponíveis)
    "COP_Medio", "Gasto_Operacao_R$", "Potencia_Media_Dia_KW", "Chillers",
    "Temperatura_Média_C", "Precipitacao Total (mm)", "Horas_Trabalhadas",
    "Consumo_KWh", "Fator_CO2_por_KWh",
    # Comparativos (reservados para versões futuras)
    "Consumo_Ano_Anterior", "Consumo_Mes_Anterior",
    "Gasto_Ano_Anterior", "Gasto_Mes_Anterior",
    "COP_Medio_Mes_Anterior", "COP_Medio_Ano_Anterior",
    # Derivados e custo
    "CO2_Emitido_em_Operacao", "Valor_do_KWh_Em_Vigor",
    # Médias do chiller (sensores)
    "Potencia_Frigorifica_Media_Dia", "Agua_Retorno_Media",
    "Agua_Alimentacao_Media", "Agua_Arref_Media", "Arref_m³",
    "Arref_Rotacao_%", "Alim_m³",
    # Filtros de conveniência
    "Filtro_Ultimos_5_Anos", "Filtro_Ultimos_12_Meses",
    "Filtro_Mes_Atual_Ou_Anterior", "Filtro_Ano_Atual_Ou_Anterior",
    "Filtro_Mes_Anterior", "Filtro_Mes_Atual",
]

# Nomes de meses e dias — pt-BR, minúsculo, com acento (inclui "março")
MESES_PT = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
    5: "maio", 6: "junho", 7: "julho", 8: "agosto",
    9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
}
DIAS_PT = {
    0: "segunda-feira", 1: "terça-feira", 2: "quarta-feira",
    3: "quinta-feira", 4: "sexta-feira", 5: "sábado", 6: "domingo",
}

DATA_INICIAL = pd.Timestamp("2020-01-01")

# Caminhos padrão (Windows com forward-slash)
CAMINHO_SAIDA = Path("C:/Cummins Chillers Dashboard/dados_cummins")
CAMINHO_INMET_DIARIO_XLSX = Path("C:/Cummins Chillers Dashboard/dados_inmet/Dados_INMET_Media_Diaria.xlsx")
SHEET_INMET_MEDIA_DIARIA = "Média Diária"
CAMINHO_VALOR_KWH_ANO_XLSX = Path("C:/Cummins Chillers Dashboard/dados_cummins/Valor_KWh_Ano.xlsx")
CAMINHO_CO2_ANUAL_CSV = Path("C:/Cummins Chillers Dashboard/dados_sistema_eletrico_brasil/Inventario_Fator_Medio_Anual_CO2_KWh.csv")
CAMINHO_DADOS_CHILLER_XLSX = Path("C:/Cummins Chillers Dashboard/dados_cummins/Dados do Chiller.xlsx")
SHEET_CHILLER_MEDIAS_DIARIAS = "Médias Diárias"
PREFERRED_CHILLER_SHEET = "Chillers"

# Limiar para considerar o chiller "ligado" (Pot_Elet_KW > LIMIAR_LIGADO_KW)
LIMIAR_LIGADO_KW = 0.0

# === [Seção tratamento_calendario-020: Utilitários de data e filtros] ================
# Objetivo:
#     Funções auxiliares para manipular datas, calcular mês/ano seguinte e
#     construir filtros de conveniência (últimos 5 anos, 12 meses, mês/ano atual/anterior).
# Fluxo:
#     _prox_mes_ano -> _calc_filtros -> _gerar_grade_diaria
# =====================================================================================
def _prox_mes_ano(dt: pd.Timestamp) -> tuple[int, int]:
    """Retorna (mês seguinte, ano do mês seguinte) para uma data dt."""
    m = int(dt.month); a = int(dt.year)
    return (1, a + 1) if m == 12 else (m + 1, a)

def _calc_filtros(df: pd.DataFrame, hoje: pd.Timestamp) -> pd.DataFrame:
    """Calcula filtros booleanos de conveniência (últimos períodos, mês/ano atual/anterior)."""
    lim_5_anos = hoje - pd.DateOffset(years=5)
    lim_12_meses = hoje - pd.DateOffset(months=12)

    m_atual, a_atual = int(hoje.month), int(hoje.year)
    m_anterior, a_anterior_mes = (12, a_atual - 1) if m_atual == 1 else (m_atual - 1, a_atual)
    a_anterior = a_atual - 1

    df["Filtro_Ultimos_5_Anos"] = df["Calendario"] >= lim_5_anos.normalize()
    df["Filtro_Ultimos_12_Meses"] = df["Calendario"] >= lim_12_meses.normalize()
    df["Filtro_Mes_Atual_Ou_Anterior"] = (
        ((df["Mês"] == m_atual) & (df["Ano"] == a_atual)) |
        ((df["Mês"] == m_anterior) & (df["Ano"] == a_anterior_mes))
    )
    df["Filtro_Ano_Atual_Ou_Anterior"] = df["Ano"].isin([a_atual, a_anterior])
    df["Filtro_Mes_Anterior"] = (df["Mês"] == m_anterior) & (df["Ano"] == a_anterior_mes)
    df["Filtro_Mes_Atual"] = (df["Mês"] == m_atual) & (df["Ano"] == a_atual)
    return df

def _gerar_grade_diaria(data_inicio: pd.Timestamp, data_fim: pd.Timestamp) -> pd.DataFrame:
    """Cria um DataFrame com uma linha por dia no intervalo solicitado."""
    return pd.DataFrame({"Calendario": pd.date_range(start=data_inicio, end=data_fim, freq="D")})

# === [Seção tratamento_calendario-030: Núcleo - blocos básicos] ======================
# Objetivo:
#     Preencher a grade diária com chaves do dia e metadados (Semana ISO, Nome do Mês,
#     Nome do Dia, Ano, Mês, Mês_Seguinte, Ano_Seguinte e filtros).
# Fluxo:
#     _preencher_blocos_basicos(df, hoje) -> retorna df enriquecido
# =====================================================================================
def _preencher_blocos_basicos(df: pd.DataFrame, hoje: pd.Timestamp) -> pd.DataFrame:
    """Preenche chaves e metadados básicos do calendário."""
    df["Semana do Ano"] = df["Calendario"].dt.isocalendar().week.astype(int)
    df["Nome do Mês"] = df["Calendario"].dt.month.map(MESES_PT)
    df["Nome do Dia"] = df["Calendario"].dt.weekday.map(DIAS_PT)
    df["Ano"] = df["Calendario"].dt.year.astype(int)
    df["Mês"] = df["Calendario"].dt.month.astype(int)

    prox = df["Calendario"].apply(_prox_mes_ano)
    df["Mês_Seguinte"] = prox.apply(lambda x: x[0]).astype(int)
    df["Ano_Seguinte"] = (df["Calendario"].dt.year + 1).astype(int)

    return _calc_filtros(df, hoje=hoje)

# === [Seção tratamento_calendario-040: INMET - leitura e merge diário] ===============
# Objetivo:
#     Carregar a planilha "Média Diária" do INMET e anexar ao Calendário as colunas
#     "Temperatura_Média_C" e "Precipitacao Total (mm)" por data.
# Fluxo:
#     _carregar_inmet_diario() -> normaliza colunas e datas -> _mesclar_inmet(df)
# Notas:
#     Detecta colunas por heurística (contém "data", "temperatura", "precipit").
# =====================================================================================
def _carregar_inmet_diario() -> pd.DataFrame:
    """Carrega o INMET diário do XLSX ('Média Diária'), padroniza e retorna colunas alvo."""
    if not CAMINHO_INMET_DIARIO_XLSX.exists():
        logging.warning(f"INMET diário não encontrado: {CAMINHO_INMET_DIARIO_XLSX}")
        return pd.DataFrame(columns=["Calendario", "Temperatura_Média_C", "Precipitacao Total (mm)"])
    try:
        dfm = pd.read_excel(CAMINHO_INMET_DIARIO_XLSX, sheet_name=SHEET_INMET_MEDIA_DIARIA, engine="openpyxl")
    except Exception as e:
        logging.warning(f"Falha ao ler INMET diário: {e}")
        return pd.DataFrame(columns=["Calendario", "Temperatura_Média_C", "Precipitacao Total (mm)"])

    dfm.columns = [str(c).strip() for c in dfm.columns]
    col_data = next((c for c in dfm.columns if "data" in c.lower()), None)
    col_temp = next((c for c in dfm.columns if "temperatura" in c.lower()), None)
    col_prec = next((c for c in dfm.columns if "precipit" in c.lower()), None)

    if not col_data or not col_temp or not col_prec:
        logging.warning("INMET diário: colunas não identificadas corretamente.")
        return pd.DataFrame(columns=["Calendario", "Temperatura_Média_C", "Precipitacao Total (mm)"])

    dfm["Calendario"] = pd.to_datetime(dfm[col_data], errors="coerce", dayfirst=True).dt.normalize()
    dfm["Temperatura_Média_C"] = pd.to_numeric(dfm[col_temp], errors="coerce")
    dfm["Precipitacao Total (mm)"] = pd.to_numeric(dfm[col_prec], errors="coerce")
    dfm = dfm.dropna(subset=["Calendario"]).drop_duplicates(subset=["Calendario"], keep="last")
    return dfm[["Calendario", "Temperatura_Média_C", "Precipitacao Total (mm)"]]

def _mesclar_inmet(df: pd.DataFrame) -> pd.DataFrame:
    """Mescla INMET diário ao calendário."""
    for col in ["Temperatura_Média_C", "Precipitacao Total (mm)"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df_inmet = _carregar_inmet_diario()
    if not df_inmet.empty:
        df = df.merge(df_inmet, on="Calendario", how="left")
        logging.info("INMET diário anexado ao Calendário.")
    else:
        df["Temperatura_Média_C"], df["Precipitacao Total (mm)"] = pd.NA, pd.NA
        logging.info("INMET diário ausente — colunas preenchidas com NaN.")
    return df

# === [Seção tratamento_calendario-050: Valor do KWh - leitura anual e derivação] =====
# Objetivo:
#     Ler Valor_KWh_Ano.xlsx e anexar "Valor_do_KWh_Em_Vigor" (por Ano), além de
#     calcular "Gasto_Operacao_R$" = Consumo_KWh * Valor_do_KWh_Em_Vigor (2 casas).
# Fluxo:
#     _carregar_valor_kwh_anual() -> _mesclar_valor_kwh(df)
# Notas:
#     Conversão pt-BR para float; arredondamento monetário em 2 casas.
# =====================================================================================
def _carregar_valor_kwh_anual() -> pd.DataFrame:
    """Lê Valor_KWh_Ano.xlsx e retorna (Ano, Valor_do_KWh_Em_Vigor) com 2 casas decimais."""
    if not CAMINHO_VALOR_KWH_ANO_XLSX.exists():
        logging.warning(f"Valor KWh anual não encontrado: {CAMINHO_VALOR_KWH_ANO_XLSX}")
        return pd.DataFrame(columns=["Ano", "Valor_do_KWh_Em_Vigor"])
    try:
        dfv = pd.read_excel(CAMINHO_VALOR_KWH_ANO_XLSX, engine="openpyxl")
    except Exception as e:
        logging.warning(f"Falha ao ler Valor_KWh_Ano.xlsx: {e}")
        return pd.DataFrame(columns=["Ano", "Valor_do_KWh_Em_Vigor"])

    dfv.columns = [str(c).strip() for c in dfv.columns]
    if "Ano" not in dfv.columns or "Valor" not in dfv.columns:
        logging.warning("Valor_KWh_Ano.xlsx: colunas esperadas não encontradas ('Ano' e 'Valor').")
        return pd.DataFrame(columns=["Ano", "Valor_do_KWh_Em_Vigor"])

    dfv["Valor_do_KWh_Em_Vigor"] = (
        pd.to_numeric(
            dfv["Valor"].astype(str).str.replace(",", ".").str.replace(r"[^\d\.\-eE+]", "", regex=True),
            errors="coerce"
        ).round(2)
    )
    dfv = dfv.dropna(subset=["Ano"]).drop_duplicates(subset=["Ano"], keep="last")
    dfv["Ano"] = dfv["Ano"].astype(int)
    return dfv[["Ano", "Valor_do_KWh_Em_Vigor"]]

def _mesclar_valor_kwh(df: pd.DataFrame) -> pd.DataFrame:
    """Anexa Valor_do_KWh_Em_Vigor ao calendário e calcula Gasto_Operacao_R$ (2 casas)."""
    for c in ["Valor_do_KWh_Em_Vigor", "Gasto_Operacao_R$"]:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    df_val = _carregar_valor_kwh_anual()
    if not df_val.empty:
        df = df.merge(df_val, on="Ano", how="left")
        logging.info("Valor do KWh anual anexado ao Calendário.")
    else:
        df["Valor_do_KWh_Em_Vigor"] = pd.NA
        logging.info("Valor do KWh anual ausente — coluna preenchida com NaN.")

    df["Gasto_Operacao_R$"] = (
        pd.to_numeric(df.get("Consumo_KWh"), errors="coerce") *
        pd.to_numeric(df.get("Valor_do_KWh_Em_Vigor"), errors="coerce")
    ).round(2)
    df["Valor_do_KWh_Em_Vigor"] = pd.to_numeric(df["Valor_do_KWh_Em_Vigor"], errors="coerce").round(2)
    return df

# === [Seção tratamento_calendario-060: CO2 - leitura anual e derivação] ==============
# Objetivo:
#     Ler o fator médio anual de CO2 por kWh e anexar "Fator_CO2_por_KWh" (por Ano),
#     derivando "CO2_Emitido_em_Operacao" = Consumo_KWh * Fator_CO2_por_KWh.
# Fluxo:
#     _carregar_fator_co2_anual() -> _mesclar_fator_co2(df)
# =====================================================================================
def _carregar_fator_co2_anual() -> pd.DataFrame:
    """Lê CSV de fator médio anual (CO2/kWh) e retorna (Ano, Fator_CO2_por_KWh)."""
    if not CAMINHO_CO2_ANUAL_CSV.exists():
        logging.warning(f"Fator CO2 anual não encontrado: {CAMINHO_CO2_ANUAL_CSV}")
        return pd.DataFrame(columns=["Ano", "Fator_CO2_por_KWh"])
    try:
        dfc = pd.read_csv(CAMINHO_CO2_ANUAL_CSV, sep=None, engine="python", encoding="latin-1")
    except Exception as e:
        logging.warning(f"Falha ao ler CSV CO2 anual: {e}")
        return pd.DataFrame(columns=["Ano", "Fator_CO2_por_KWh"])

    dfc.columns = [str(c).strip() for c in dfc.columns]
    col_ano = next((c for c in dfc.columns if "ano" in c.lower()), None)
    col_fator = next((c for c in dfc.columns if "fator" in c.lower() or "co2" in c.lower()), None)
    if not col_ano or not col_fator:
        logging.warning("CSV CO2: colunas de Ano/Fator não identificadas.")
        return pd.DataFrame(columns=["Ano", "Fator_CO2_por_KWh"])

    dfc["Ano"] = pd.to_numeric(dfc[col_ano], errors="coerce").astype("Int64")
    dfc["Fator_CO2_por_KWh"] = pd.to_numeric(
        dfc[col_fator].astype(str).str.replace(",", ".").str.replace(r"[^\d\.\-eE+]", "", regex=True),
        errors="coerce"
    )
    dfc = dfc.dropna(subset=["Ano"]).drop_duplicates(subset=["Ano"], keep="last")
    dfc["Ano"] = dfc["Ano"].astype(int)
    return dfc[["Ano", "Fator_CO2_por_KWh"]]

def _mesclar_fator_co2(df: pd.DataFrame) -> pd.DataFrame:
    """Mescla Fator_CO2_por_KWh por Ano ao calendário e calcula CO2_Emitido_em_Operacao."""
    if "Fator_CO2_por_KWh" in df.columns:
        df.drop(columns=["Fator_CO2_por_KWh"], inplace=True)

    df_co2 = _carregar_fator_co2_anual()
    if not df_co2.empty:
        df = df.merge(df_co2, on="Ano", how="left")
        logging.info("Fator CO2 anual anexado ao Calendário.")
    else:
        df["Fator_CO2_por_KWh"] = pd.NA
        logging.info("Fator CO2 anual ausente — coluna preenchida com NaN.")

    df["CO2_Emitido_em_Operacao"] = (
        pd.to_numeric(df.get("Consumo_KWh"), errors="coerce") *
        pd.to_numeric(df.get("Fator_CO2_por_KWh"), errors="coerce")
    )
    return df

# === [Seção tratamento_calendario-070: IO - leitura e gravação de planilha] ==========
# Objetivo:
#     Ler o Calendário existente (incremental) e gravar a saída com backup e
#     formatação de 2 casas nas colunas monetárias.
# Fluxo:
#     _ler_existente(path) -> _gravar_planilha(path, df)
# Notas:
#     Usa openpyxl; aplica number_format 00 em "Valor_do_KWh_Em_Vigor" e "Gasto_Operacao_R$".
# =====================================================================================
def _ler_existente(caminho_arquivo: Path) -> pd.DataFrame:
    """Lê planilha existente do Calendário (sheet 'Calendario'); normaliza data."""
    try:
        existente = pd.read_excel(caminho_arquivo, sheet_name=NOME_ABA, engine="openpyxl")
        existente.columns = [c.strip() for c in existente.columns]
        if "Calendario" in existente.columns:
            existente["Calendario"] = pd.to_datetime(existente["Calendario"], errors="coerce", dayfirst=True)
            existente = existente.dropna(subset=["Calendario"])
            existente["Calendario"] = existente["Calendario"].dt.normalize()
        return existente
    except Exception as e:
        logging.warning(f"Falha ao ler planilha existente ({caminho_arquivo.name}): {e}")
        return pd.DataFrame(columns=COLUNAS_CALENDARIO)

def _gravar_planilha(caminho_arquivo: Path, df: pd.DataFrame):
    """Grava o Calendário, aplicando backup e formato numérico de 2 casas nas colunas de moeda."""
    if caminho_arquivo.exists():
        backup_saida(caminho_arquivo)

    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(
        caminho_arquivo,
        engine="openpyxl",
        datetime_format="dd/mm/yyyy",
        date_format="dd/mm/yyyy"
    ) as writer:
        df.to_excel(writer, sheet_name=NOME_ABA, index=False)

        # Aplica formato de 2 casas decimais nas colunas monetárias
        try:
            ws = writer.sheets[NOME_ABA]
            cols_to_format = ["Valor_do_KWh_Em_Vigor", "Gasto_Operacao_R$"]
            for col in cols_to_format:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

            from openpyxl.styles import numbers
            for col in cols_to_format:
                if col in df.columns:
                    col_idx = df.columns.get_loc(col) + 1
                    for cell in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                        cell[0].number_format = numbers.FORMAT_NUMBER_00
        except Exception as e:
            logging.warning(f"Não foi possível aplicar formato de 2 casas via openpyxl: {e}")

# === [Seção tratamento_calendario-080: Chiller - gerar sheet 'Médias Diárias'] =======
# Objetivo:
#     Ler amostras minuto a minuto e gerar a planilha "Médias Diárias" com:
#       • Horas Trabalhadas Dia (h) = soma de Δt onde Pot_Elet_KW > limiar
#       • Consumo Diário (KWh)      = soma de (Pot_Elet_KW × Δt)
#       • Médias numéricas dos sensores
#       • (085A) Médias ponderadas dos grupos de status (Chillers, Arref_Status_B, Status_BAG)
# Notas:
#     - Remove colunas vazias após expansão
#     - Δt (step_h) limitado a 1 min para evitar saltos
#     - Colunas internas (_step_h, _h_trab, _consumo_kwh e auxiliares) NÃO são gravadas
# =====================================================================================
def _gerar_medias_diarias_chiller():
    def _escrever_medias_diarias(out: pd.DataFrame):
        try:
            if out is None or out.empty:
                out = pd.DataFrame(columns=[
                    "Calendario", "Horas Trabalhadas Dia (h)", "Consumo Diário (KWh)",
                    "Chillers", "Arref_Status_B", "Status_BAG",
                    "Arref_m³/h", "Arref_bar", "Arref_°C", "Arref_Rotacao%_B1",
                    "Alim_m³/h", "Alim_bar", "Alim_°C", "Retorno_bar", "Retorno_°C",
                    "Pot_Frig_KW", "Pot_Elet_KW", "COP",
                    # colunas de status originais permanecem (item 2)
                    "Status_CH1","Status_CH2","Status_CH3",
                    "Arref_Status_B1","Arref_Status_B2","Arref_Status_B3",
                    "Status_BAG1","Status_BAG2","Status_BAG3","Status_BAG4",
                ])
            # Remover colunas internas antes de gravar
            drop_cols = [c for c in out.columns if c.startswith("_")]
            out = out.drop(columns=drop_cols, errors="ignore")

            with pd.ExcelWriter(
                CAMINHO_DADOS_CHILLER_XLSX,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace"
            ) as writer:
                out.to_excel(writer, sheet_name=SHEET_CHILLER_MEDIAS_DIARIAS, index=False)
            logging.info(f"Chiller: sheet '{SHEET_CHILLER_MEDIAS_DIARIAS}' atualizada em: {CAMINHO_DADOS_CHILLER_XLSX}")
        except FileNotFoundError:
            with pd.ExcelWriter(CAMINHO_DADOS_CHILLER_XLSX, engine="openpyxl") as writer:
                out.to_excel(writer, sheet_name=SHEET_CHILLER_MEDIAS_DIARIAS, index=False)
            logging.info(f"Chiller: arquivo novo criado com sheet '{SHEET_CHILLER_MEDIAS_DIARIAS}'.")

    if not CAMINHO_DADOS_CHILLER_XLSX.exists():
        logging.warning(f"Chiller: arquivo não encontrado: {CAMINHO_DADOS_CHILLER_XLSX}")
        _escrever_medias_diarias(pd.DataFrame())
        return

    # --- Auxiliares locais (parsers) -------------------------------------------------
    def _ptbr_to_float(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.replace(",", ".", regex=False)
        s = s.str.replace(r"[^\d\.\-eE+]", "", regex=True)
        return pd.to_numeric(s, errors="coerce")

    def _excel_serial_to_datetime(s: pd.Series) -> pd.Series:
        is_num = pd.to_numeric(s, errors="coerce")
        return pd.to_datetime("1899-12-30") + pd.to_timedelta(is_num, unit="D")

    def _excel_time_to_timedelta_any(s: pd.Series) -> pd.Series:
        s_str = s.astype(str).str.replace(",", ".", regex=False)
        as_num = pd.to_numeric(s_str, errors="coerce")
        td_num = pd.to_timedelta(as_num, unit="D")
        td_str = pd.to_timedelta(s_str.where(as_num.isna(), np.nan), errors="coerce")
        return td_num.combine_first(td_str).fillna(pd.Timedelta(0))

    def _combinar_data_hora(df: pd.DataFrame) -> pd.Series:
        col_data = next((c for c in df.columns if "data" in c.lower()), None)
        col_hora = next((c for c in df.columns if "hora" in c.lower()), None)
        if col_data:
            data = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
            if data.isna().mean() > 0.8:
                data = _excel_serial_to_datetime(df[col_data])
            if col_hora:
                hora = _excel_time_to_timedelta_any(df[col_hora])
                return data + hora
            return data
        for c in df.columns:
            dt_try = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
            if dt_try.notna().mean() > 0.6:
                return dt_try
        return pd.Series(pd.NaT, index=df.index)

    # --- Leitura e expansão ----------------------------------------------------------
    try:
        xls = pd.ExcelFile(CAMINHO_DADOS_CHILLER_XLSX, engine="openpyxl")
        candidate = PREFERRED_CHILLER_SHEET if PREFERRED_CHILLER_SHEET in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=candidate, header=None)

        if df.shape[1] == 1:
            s = df.iloc[:, 0].astype(str).str.replace("\xa0", " ", regex=False)
            df = s.str.split(r"\s+", expand=True)
            header = df.iloc[0].tolist()
            df.columns = header
            df = df.iloc[1:].reset_index(drop=True)
            df = df.loc[:, [c for c in df.columns if str(c).strip() != ""]]  # remove colunas vazias
            #logging.info(f"Chiller: expandi 1 coluna em {df.shape[1]} colunas usando separador='\\s+'.")
        else:
            df = pd.read_excel(xls, sheet_name=candidate)

        df.columns = [str(c).strip().replace("m3", "m³") for c in df.columns]
        #logging.info(f"Chiller: sheet candidata='{candidate}'; colunas={list(df.columns)}")

        # --- Data/Hora, Δt e derivados ---------------------------------------------
        df["dt"] = _combinar_data_hora(df)
        df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
        if df.empty:
            logging.warning("Chiller: após normalização de data/hora, não há linhas válidas.")
            _escrever_medias_diarias(pd.DataFrame())
            return

        df["Calendario"] = df["dt"].dt.normalize()
        df["_step_h"] = df["dt"].diff().dt.total_seconds().div(3600.0)
        df["_step_h"] = df["_step_h"].clip(lower=0, upper=1/60).fillna(1/60)

        for c in df.columns:
            if c.lower() not in ("data", "hora", "dt", "calendario"):
                df[c] = _ptbr_to_float(df[c])

        if "Pot_Elet_KW" not in df.columns:
            logging.warning("Chiller: coluna 'Pot_Elet_KW' ausente.")
            _escrever_medias_diarias(pd.DataFrame())
            return

        df["Pot_Elet_KW"] = df["Pot_Elet_KW"].fillna(0)
        df["_h_trab"] = np.where(df["Pot_Elet_KW"] > LIMIAR_LIGADO_KW, df["_step_h"], 0)
        df["_consumo_kwh"] = df["Pot_Elet_KW"] * df["_step_h"]

        # --- Agregações principais ---------------------------------------------------
        agreg_media = df.groupby("Calendario", as_index=False).mean(numeric_only=True)
        agreg_deriv = df.groupby("Calendario", as_index=False).agg(
            **{
                "Horas Trabalhadas Dia (h)": ("_h_trab", "sum"),
                "Consumo Diário (KWh)": ("_consumo_kwh", "sum"),
            }
        )
        out = agreg_media.merge(agreg_deriv, on="Calendario", how="left")

        # --- (085A) Médias ponderadas de status ------------------------------------
        out = _chiller_medias_status_ponderadas(df, out)

        # --- Remover colunas internas antes de gravar -------------------------------
        drop_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=drop_cols, errors="ignore")

        _escrever_medias_diarias(out)

    except Exception as e:
        logging.warning(f"Chiller: falha ao gerar 'Médias Diárias': {e}")
        try:
            _escrever_medias_diarias(pd.DataFrame())
        except Exception:
            pass

# === [Seção tratamento_calendario-090: Chiller - médias de status (ponderadas)] ====
# Objetivo:
#     Gerar, por dia, as médias ponderadas por tempo dos grupos binários 0/1:
#       • Chillers         = média ponderada do (Status_CH1+Status_CH2+Status_CH3)  → nº médio de chillers ligados
#       • Arref_Status_B   = média ponderada do (Arref_Status_B1+...+B3)            → nº médio de bombas/arref. ativas
#       • Status_BAG       = média ponderada do (Status_BAG1+...+BAG4)              → nº médio de BAGs ativos
# Notas:
#     - Mantemos TODAS as colunas de status originais na saída (a pedido).
#     - Usa _step_h como peso; ignora NaN de forma segura.
#     - Adiciona compatibilidade com pandas >= 2.2 (include_groups=False)
# =====================================================================================
def _chiller_medias_status_ponderadas(df_src: pd.DataFrame, out_daily: pd.DataFrame) -> pd.DataFrame:
	try:
		df = df_src.copy()

		# Garantir pesos válidos
		if "_step_h" not in df.columns:
			logging.warning("Chiller: _step_h ausente para ponderação; assumindo 1 minuto.")
			df["_step_h"] = 1/60

		# Função auxiliar para somar apenas colunas existentes
		def _sum_existing(cols):
			cols = [c for c in cols if c in df.columns]
			if not cols:
				return pd.Series(np.nan, index=df.index)
			return df[cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)

		# Somas instantâneas (0..N) por amostra
		df["_ch_sum"]    = _sum_existing(["Status_CH1","Status_CH2","Status_CH3"])
		df["_arref_sum"] = _sum_existing(["Arref_Status_B1","Arref_Status_B2","Arref_Status_B3"])
		df["_bag_sum"]   = _sum_existing(["Status_BAG1","Status_BAG2","Status_BAG3","Status_BAG4"])

		# Média ponderada por dia: sum(val * step) / sum(step)
		def _wavg(group, col):
			w = pd.to_numeric(group["_step_h"], errors="coerce")
			x = pd.to_numeric(group[col], errors="coerce")
			num = np.nansum(x * w)
			den = np.nansum(w)
			return float(num / den) if den and den > 0 else np.nan

		# Agrupar por dia e aplicar ponderação
		grp = df.groupby("Calendario", as_index=False)

		try:
			# pandas >= 2.2: parâmetro include_groups elimina FutureWarning
			res = grp.apply(
				lambda g: pd.Series({
					"Chillers":        _wavg(g, "_ch_sum"),
					"Arref_Status_B":  _wavg(g, "_arref_sum"),
					"Status_BAG":      _wavg(g, "_bag_sum"),
				}),
				include_groups=False
			)
		except TypeError:
			# pandas < 2.2: mantém compatibilidade
			res = grp.apply(lambda g: pd.Series({
				"Chillers":        _wavg(g, "_ch_sum"),
				"Arref_Status_B":  _wavg(g, "_arref_sum"),
				"Status_BAG":      _wavg(g, "_bag_sum"),
			}))

		# Merge no resultado diário
		out = out_daily.merge(res, on="Calendario", how="left")

		# Limpar auxiliares internos do out (se chegaram até aqui por algum motivo)
		drop_cols = [c for c in out.columns if c.startswith("_")]
		out = out.drop(columns=drop_cols, errors="ignore")

		logging.info("Chiller: médias de status ponderadas calculadas e anexadas.")
		return out

	except Exception as e:
		logging.warning(f"Chiller: falha nas médias ponderadas de status: {e}")
		return out_daily

# === [Seção tratamento_calendario-100: Calendário ⟵ Médias Diárias do Chiller] ======
def _mesclar_chiller_no_calendario(df_cal: pd.DataFrame) -> pd.DataFrame:
    try:
        if not CAMINHO_DADOS_CHILLER_XLSX.exists():
            logging.info("Chiller→Calendário: arquivo de médias diárias não encontrado — mantendo Calendário sem chiller.")
            return df_cal

        md = pd.read_excel(CAMINHO_DADOS_CHILLER_XLSX, sheet_name=SHEET_CHILLER_MEDIAS_DIARIAS, engine="openpyxl")
        if md is None or md.empty:
            logging.info("Chiller→Calendário: sheet de médias diárias vazia.")
            return df_cal

        # Normalização de nomes
        md.columns = [str(c).strip() for c in md.columns]
        if "Calendario" not in md.columns:
            logging.warning("Chiller→Calendário: sheet 'Médias Diárias' sem coluna 'Calendario'.")
            return df_cal

        # Seleção e renomeação
        cols_map = {
            "Calendario": "Calendario",
            "Pot_Elet_KW": "Potencia_Media_Dia_KW",
            "Pot_Frig_KW": "Potencia_Frigorifica_Media_Dia",
            "COP": "COP_Medio",
            "Horas Trabalhadas Dia (h)": "Horas_Trabalhadas",
            "Consumo Diário (KWh)": "Consumo_KWh",
            "Retorno_°C": "Agua_Retorno_Media",
            "Alim_°C": "Agua_Alimentacao_Media",
            "Arref_°C": "Agua_Arref_Media",
            "Arref_m³/h": "Arref_m³",
            "Arref_Rotacao%_B1": "Arref_Rotacao_%",
            "Alim_m³/h": "Alim_m³",
            # se existirem: "Chillers", "Arref_Status_B", "Status_BAG"
            "Chillers": "Chillers",
            "Arref_Status_B": "Arref_Status_B",
            "Status_BAG": "Status_BAG",
        }
        existentes = [c for c in cols_map.keys() if c in md.columns]
        md = md[existentes].rename(columns={c: cols_map[c] for c in existentes})

        # Normalizar data
        md["Calendario"] = pd.to_datetime(md["Calendario"], errors="coerce").dt.normalize()
        df_cal["Calendario"] = pd.to_datetime(df_cal["Calendario"], errors="coerce").dt.normalize()

        # Merge
        antes = len(df_cal)
        df_cal = df_cal.merge(md, on="Calendario", how="left")
        logging.info(
            "Chiller → Calendário: médias diárias integradas ao Calendário."
            if len(df_cal) == antes else
            f"Chiller → Calendário: merge alterou contagem de linhas (antes={antes}, depois={len(df_cal)})."
        )

        # Recalcular (ou preencher) Gasto_Operacao_R$ com proteção a Series ausentes
        # (evita 'numpy.float64' sem .fillna)
        idx = df_cal.index
        cons = (
            pd.to_numeric(df_cal["Consumo_KWh"], errors="coerce")
            if "Consumo_KWh" in df_cal.columns else
            pd.Series(np.nan, index=idx)
        )
        valk = (
            pd.to_numeric(df_cal["Valor_do_KWh_Em_Vigor"], errors="coerce")
            if "Valor_do_KWh_Em_Vigor" in df_cal.columns else
            pd.Series(np.nan, index=idx)
        )
        df_cal["Gasto_Operacao_R$"] = (cons.fillna(0.0) * valk.fillna(0.0)).round(2)

        return df_cal

    except Exception as e:
        logging.warning(f"Chiller → Calendário: falha ao integrar médias diárias: {e}")
        return df_cal


# === [Seção tratamento_calendario-110: Lookbacks (Ano/Mês anterior) por mesma data] =
def _aplicar_lookbacks_mes_ano(df_cal: pd.DataFrame) -> pd.DataFrame:
    try:
        if df_cal is None or df_cal.empty:
            logging.info("Calendário: lookbacks — dataframe vazio; nada a fazer.")
            return df_cal

        df = df_cal.copy()
        df["Calendario"] = pd.to_datetime(df["Calendario"], errors="coerce").dt.normalize()

        # Garante que as colunas-base existam antes de selecionar
        for base_col in ("Consumo_KWh", "Gasto_Operacao_R$", "COP_Medio"):
            if base_col not in df.columns:
                df[base_col] = pd.NA

        def _ajusta_29fev(dt: pd.Timestamp) -> pd.Timestamp:
            if pd.isna(dt):
                return dt
            if dt.month == 2 and dt.day == 29:
                return pd.Timestamp(year=dt.year, month=2, day=28)
            return dt

        # Datas de referência
        df["ref_ano_anterior"] = df["Calendario"].apply(
            lambda d: _ajusta_29fev(pd.Timestamp(year=d.year - 1, month=d.month, day=min(d.day, 28)) if not pd.isna(d) else d)
        )
        df["ref_mes_anterior"] = (df["Calendario"] - pd.DateOffset(months=1)).apply(_ajusta_29fev)

        base = df[["Calendario", "Consumo_KWh", "Gasto_Operacao_R$", "COP_Medio"]].copy()

        # Ano anterior
        ref_y = base.rename(columns={
            "Calendario": "Calendario_ref_y",
            "Consumo_KWh": "Consumo_Ano_Anterior",
            "Gasto_Operacao_R$": "Gasto_Ano_Anterior",
            "COP_Medio": "COP_Medio_Ano_Anterior",
        })
        df = df.merge(ref_y, left_on="ref_ano_anterior", right_on="Calendario_ref_y", how="left")

        # Mês anterior
        ref_m = base.rename(columns={
            "Calendario": "Calendario_ref_m",
            "Consumo_KWh": "Consumo_Mes_Anterior",
            "Gasto_Operacao_R$": "Gasto_Mes_Anterior",
            "COP_Medio": "COP_Medio_Mes_Anterior",
        })
        df = df.merge(ref_m, left_on="ref_mes_anterior", right_on="Calendario_ref_m", how="left")

        # Logs de cobertura
        for col in ["Consumo_Ano_Anterior","Consumo_Mes_Anterior","Gasto_Ano_Anterior",
                    "Gasto_Mes_Anterior","COP_Medio_Ano_Anterior","COP_Medio_Mes_Anterior"]:
            total = len(df)
            filled = pd.Series(df[col]).notna().sum()
            logging.info(f"Calendário: lookback '{col}' — preenchidos {filled}/{total}")

        # Limpeza de auxiliares
        df.drop(columns=["ref_ano_anterior","ref_mes_anterior","Calendario_ref_y","Calendario_ref_m"],
                errors="ignore", inplace=True)

        # Arredonda valores monetários dos lookbacks
        if "Gasto_Ano_Anterior" in df.columns:
            df["Gasto_Ano_Anterior"] = pd.to_numeric(df["Gasto_Ano_Anterior"], errors="coerce").round(2)
        if "Gasto_Mes_Anterior" in df.columns:
            df["Gasto_Mes_Anterior"] = pd.to_numeric(df["Gasto_Mes_Anterior"], errors="coerce").round(2)

        return df

    except Exception as e:
        logging.warning(f"Calendário: lookbacks falharam: {e}")
        return df_cal


# === [Seção tratamento_calendario-120: Compatibilidade de nome (alias)] ============
# Objetivo:
#     Garantir compatibilidade com chamadas existentes no orquestrador que usam
#     o nome _mesclar_medias_diarias_chiller, redirecionando para a função
#     implementada na Seção 086 (_mesclar_chiller_no_calendario).
# =====================================================================================
def _mesclar_medias_diarias_chiller(df_cal: pd.DataFrame) -> pd.DataFrame:
    #logging.info("Calendário: usando alias '_mesclar_medias_diarias_chiller' → '_mesclar_chiller_no_calendario'.")
    return _mesclar_chiller_no_calendario(df_cal)

# === [Seção tratamento_calendario-130: API principal] ================================
# Objetivo:
#     Orquestrar toda a atualização do Calendário:
#       1) criar/estender grade diária,
#       2) blocos básicos (chaves + filtros),
#       3) gerar 'Médias Diárias' do chiller (085/085A) e integrar (086),
#       4) INMET diário (040),
#       5) Valor KWh (050) e Fator CO2 (060),
#       6) lookbacks (110),
#       7) persistir XLSX com backup e formatação (070).
# =====================================================================================
def gerar_calendario_xlsx(
    caminho_saida: Path | None = None,
    nome_arquivo: str = ARQUIVO_PADRAO
) -> Path:
    logging.info("Iniciando geração/atualização do Calendario.xlsx...")

    if caminho_saida is None:
        caminho_saida = CAMINHO_SAIDA
    caminho_saida.mkdir(parents=True, exist_ok=True)

    destino = caminho_saida / nome_arquivo
    hoje = pd.Timestamp(date.today()).normalize()

    # Leitura incremental do calendário existente
    existente = pd.DataFrame()
    if destino.exists():
        logging.info(f"Arquivo existente detectado: {destino.name} — modo incremental.")
        existente = _ler_existente(destino)

    if not existente.empty:
        ultima_data = existente["Calendario"].max()
        data_inicio = (ultima_data + pd.Timedelta(days=1)).normalize()
        logging.info(f"Última data registrada: {ultima_data.date()} — novos dias a partir de: {data_inicio.date()}")
    else:
        data_inicio = DATA_INICIAL
        logging.info(f"Planilha inexistente/vazia — criando desde {DATA_INICIAL.date()}")

    data_fim = hoje

    # Geração dos novos dias (se houver)
    if data_inicio <= data_fim:
        novos = _gerar_grade_diaria(data_inicio, data_fim)
        novos = _preencher_blocos_basicos(novos, hoje=data_fim)
    else:
        novos = pd.DataFrame(columns=["Calendario"])
        logging.info("Nenhum dia novo para acrescentar (planilha já atualizada).")

    # Consolidação incremental
    if not existente.empty and not novos.empty:
        df = pd.concat([existente, novos], ignore_index=True)
    elif not existente.empty and novos.empty:
        df = existente.copy()
    else:
        df = novos.copy()

    # 085/085A/086/040/050/060
    _gerar_medias_diarias_chiller()
    df = _mesclar_inmet(df)
    df = _mesclar_medias_diarias_chiller(df)
    df = _mesclar_valor_kwh(df)
    df = _mesclar_fator_co2(df)

    # >>> NOVO: aplicar lookbacks ANTES do reindex (para garantir preenchimento) <<<
    df = _aplicar_lookbacks_mes_ano(df)

    # Padroniza todas as colunas do calendário (cria vazias se faltarem) e ordena
    for col in COLUNAS_CALENDARIO:
        if col not in df.columns:
            df[col] = pd.NA
    df = df.reindex(columns=COLUNAS_CALENDARIO)

    df = df.sort_values("Calendario").drop_duplicates(subset=["Calendario"], keep="last").reset_index(drop=True)
    _gravar_planilha(destino, df)
    logging.info(f"Calendario.xlsx atualizado em: {destino}\n")

    return destino