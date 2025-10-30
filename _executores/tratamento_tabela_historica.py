# === META =========================================================
# Módulo: tratamento_tabela_historica
# Versão: v1.3.1 (padrão de seções alinhado a tratamento_consumo)
# Consolidação: chiller + INMET + KWh + CO2 + bandeiras
# Saída: Tabela_Historico_Tratada.xlsx
# Notas desta versão:
#   - "Valor do KWh (R$)" passa a ser o único nome para o preço de energia.
#   - Qualquer coluna legada "Preco_KWh_R$" é removida da saída.
#   - "Gasto da Operacao (R$)" passa a usar "Valor do KWh (R$)".
# ==================================================================

import logging
import pandas as pd
import numpy as np
from pathlib import Path

from _executores.utils import (
    BASE_DIR,
    MESES_ORDEM,
    backup_saida,
    DADOS_CUMMINS_DIR,
    DADOS_SISTEMA_ELETRICO_DIR,
)

from _executores.tratamento_dados import (
    exportar_inmet_diario_e_mensal_para_xlsx,  # <-- garantir arquivo XLSX (Média Diária + Média Mensal)
    carregar_amostras_chiller,
    agregar_consumo_e_horas_chiller,
    MES_NUM_PARA_PT,
)

# === [Seção tratamento_tabela_historica-010: Orquestração e INMET (XLSX)] ============
# Objetivo:
#     Orquestrar a criação do histórico e garantir a presença do INMET em XLSX
#     (planilhas "Média Diária" e "Média Mensal"), lendo a "Média Mensal".
# Fluxo:
#     exportar_inmet_diario_e_mensal_para_xlsx -> ler "Média Mensal" -> normalizar colunas
# ======================================================================================
def construir_tabela_historico_de_chiller(potencia_min_kw: float = 0.0):
    """Gera Tabela_Historico_Tratada.xlsx unindo chiller, INMET e custos."""
    logging.info("Iniciando criação e consolidação dos dados de Historico...")

    pasta = DADOS_CUMMINS_DIR
    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / "Tabela_Historico_Tratada.xlsx"

    # === INMET: garantir arquivo XLSX atualizado ============================
    try:
        exportar_inmet_diario_e_mensal_para_xlsx(BASE_DIR)
    except Exception as e:
        logging.warning(f"Não foi possível exportar INMET (XLSX) automaticamente: {e}")

    # === INMET: ler agregados mensais direto do XLSX ========================
    mensais_inmet = pd.DataFrame(columns=["Ano", "Mês", "Temperatura Média (°C)", "Precipitacao Total (mm)"])
    try:
        caminho_inmet_dir = Path(r"C:/Cummins Chillers Dashboard/dados_inmet")
        caminho_inmet_xlsx = caminho_inmet_dir / "Dados_INMET_Media_Diaria.xlsx"
        if caminho_inmet_xlsx.exists():
            mm = pd.read_excel(caminho_inmet_xlsx, sheet_name="Média Mensal", engine="openpyxl")
            # Normalização básica de colunas esperadas
            cols = {c.strip(): c.strip() for c in mm.columns}
            mm = mm.rename(columns=cols)
            faltam = {"Ano", "Mês", "Temperatura Média (°C)", "Precipitacao Total (mm)"} - set(mm.columns)
            if faltam:
                logging.warning(f"INMET (Média Mensal): colunas ausentes {faltam}.")
            else:
                mensais_inmet = mm[["Ano", "Mês", "Temperatura Média (°C)", "Precipitacao Total (mm)"]].copy()
                # Ordenação padrão PT-BR
                mensais_inmet = mensais_inmet[mensais_inmet["Mês"].isin(MESES_ORDEM)]
                mensais_inmet["Mês"] = pd.Categorical(mensais_inmet["Mês"], categories=MESES_ORDEM, ordered=True)
                mensais_inmet = mensais_inmet.sort_values(["Ano", "Mês"]).reset_index(drop=True)
        else:
            logging.info(f"Arquivo INMET (XLSX) não encontrado para agregação mensal: {caminho_inmet_xlsx}")
    except Exception as e:
        logging.warning(f"Falha ao ler INMET (Média Mensal) do XLSX: {e}")

    # === [Seção tratamento_tabela_historica-020: Chiller (amostras e agregações)] =====
    # Objetivo:
    #     Carregar amostras do chiller, agregar consumo/horas, e calcular médias ponderadas.
    # Fluxo:
    #     carregar_amostras_chiller -> agregar_consumo_e_horas_chiller -> médias ponderadas
    # ===================================================================================
    amostras       = carregar_amostras_chiller()
    monthly        = pd.DataFrame(columns=["Ano", "Mês", "Consumo (KWh)", "Horas Trabalhadas (h)"])
    pot_media      = pd.DataFrame(columns=["Ano", "Mês", "Potência Média (KW)"])
    pot_frig_media = pd.DataFrame(columns=["Ano", "Mês", "Potência Frigorifica (KW)"])
    cop_medio      = pd.DataFrame(columns=["Ano", "Mês", "COP Médio"])

    if not amostras.empty:
        pot = pd.to_numeric(amostras["Pot_Elet_KW"], errors="coerce").fillna(0)
        ligado = pot > potencia_min_kw
        amostras["Ano"] = amostras["dt"].dt.year
        amostras["Mês"] = amostras["dt"].dt.month.map(MES_NUM_PARA_PT)

        monthly = agregar_consumo_e_horas_chiller(potencia_min_kw=potencia_min_kw)

        am = amostras.copy()
        am["w"] = am["step_h"].where(ligado, 0.0)

        def _weighted_avg(g: pd.DataFrame, col: str):
            w = pd.to_numeric(g["w"], errors="coerce").fillna(0).values
            x = pd.to_numeric(g[col], errors="coerce").values
            if np.nansum(w) <= 0:
                return np.nan
            return float(np.nansum(x * w) / np.nansum(w))

        # Potência Elétrica Média (KW)
        pot_media = (
            am.groupby(["Ano", "Mês"])[["w", "Pot_Elet_KW"]]
              .apply(lambda g: _weighted_avg(g, "Pot_Elet_KW"))
              .reset_index(name="Potência Média (KW)")
        )

        # Potência Frigorifica (KW)
        pot_frig_media = (
            am.groupby(["Ano", "Mês"])[["w", "Pot_Frig_KW"]]
              .apply(lambda g: _weighted_avg(g, "Pot_Frig_KW"))
              .reset_index(name="Potência Frigorifica (KW)")
        )

        # COP Médio
        cop_medio = (
            am.groupby(["Ano", "Mês"])[["w", "COP"]]
              .apply(lambda g: _weighted_avg(g, "COP"))
              .reset_index(name="COP Médio")
        )

    # === [Seção tratamento_tabela_historica-030: Grade-base (Ano/Mês)] ===============
    # Objetivo:
    #     Construir chaves mensais (Ano, Mês) a partir das partes disponíveis.
    # Fluxo:
    #     concatenar chaves -> deduplicar -> ordenar
    # ================================================================================
    partes_keys = []
    for dfx in (mensais_inmet, monthly, pot_media, pot_frig_media, cop_medio):
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

    # === [Seção tratamento_tabela_historica-040: Merges de métricas do chiller] ======
    # Objetivo:
    #     Unir consumo/horas, potência elétrica média, potência frigorifica e COP.
    # Fluxo:
    #     keys -> merge monthly -> merge pot_media -> merge pot_frig_media -> merge cop_medio
    # ==================================================================================
    df = keys.merge(monthly,          on=["Ano", "Mês"], how="left")
    df = df.merge(pot_media,          on=["Ano", "Mês"], how="left")
    df = df.merge(pot_frig_media,     on=["Ano", "Mês"], how="left")  # após Potência Média
    df = df.merge(cop_medio,          on=["Ano", "Mês"], how="left")

    # === [Seção tratamento_tabela_historica-050: Consumo Total Cummins (mensal)] =====
    # Objetivo:
    #     Anexar "Consumo Total Cummins (KWh)" a partir de Consumo_Total_Cummins.xlsx.
    # Fluxo:
    #     ler sheet "Consumo Total" -> renomear -> merge em ["Ano","Mês"]
    # ==================================================================================
    try:
        arq_consumo_total = DADOS_CUMMINS_DIR / "Consumo_Total_Cummins.xlsx"
        if arq_consumo_total.exists():
            ctot = pd.read_excel(arq_consumo_total, sheet_name="Consumo Total", engine="openpyxl")
            cols_map = {c.strip(): c.strip() for c in ctot.columns}
            ctot = ctot.rename(columns=cols_map)
            if {"Ano", "Nome do Mês", "Consumo (KWh)"}.issubset(ctot.columns):
                aux = (
                    ctot[["Ano", "Nome do Mês", "Consumo (KWh)"]]
                    .rename(columns={
                        "Nome do Mês": "Mês",
                        "Consumo (KWh)": "Consumo Total Cummins (KWh)"
                    })
                )
                df = df.merge(aux, on=["Ano", "Mês"], how="left")
                logging.info("Consumo Total Cummins: planilha encontrada e mesclada.")
            else:
                logging.warning("Consumo Total Cummins: planilha/colunas ausentes; coluna não será preenchida.")
        else:
            logging.info(f"Consumo Total Cummins: arquivo não encontrado em: {arq_consumo_total}")
    except Exception as e:
        logging.warning(f"Consumo Total Cummins: falha ao ler/mesclar: {e}")

    # === [Seção tratamento_tabela_historica-060: Valor do KWh (anual)] ================
    # Objetivo:
    #     Anexar preço anual do kWh (como "Valor do KWh (R$)") e calcular Gasto da Operação.
    # Fluxo:
    #     ler Valor_KWh_Ano.xlsx -> merge "Valor do KWh (R$)" -> calcular gasto
    # ==================================================================================
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
                # Merge direto com o nome final padronizado
                df = df.merge(
                    kwh_tab[["Ano", "Valor"]].rename(columns={"Valor": "Valor do KWh (R$)"}),
                    on="Ano", how="left"
                )
                # Cálculo de custo usando a nova coluna
                df["Gasto da Operacao (R$)"] = df["Consumo (KWh)"] * df["Valor do KWh (R$)"]
            else:
                logging.warning("Valor do KWh: colunas ausentes em Valor_KWh_Ano.xlsx; preço não aplicado.")
        except Exception as e:
            logging.warning(f"Falha ao aplicar preço de kWh: {e}")
    else:
        logging.info(f"Valor do KWh: arquivo não encontrado em: {arq_preco}")

    # === [Seção tratamento_tabela_historica-070: % consumida pelos Chillers] =========
    # Objetivo:
    #     Calcular a participação do consumo do chiller no Consumo Total Cummins.
    # Fluxo:
    #     (Consumo (KWh) / Consumo Total Cummins (KWh)) * 100
    # ==================================================================================
    try:
        if "Consumo Total Cummins (KWh)" in df.columns and "Consumo (KWh)" in df.columns:
            denom = pd.to_numeric(df["Consumo Total Cummins (KWh)"], errors="coerce")
            numer = pd.to_numeric(df["Consumo (KWh)"], errors="coerce")
            with np.errstate(divide="ignore", invalid="ignore"):
                df["Porcentagem Consumida pelo Chillers (%)"] = np.where(
                    denom > 0, (numer / denom) * 100.0, np.nan
                )
        else:
            logging.warning("Porcentagem Consumida pelo Chillers: colunas base ausentes; métrica não será preenchida.")
    except Exception as e:
        logging.warning(f"Porcentagem Consumida pelo Chillers: falha ao calcular: {e}")

    # === [Seção tratamento_tabela_historica-080: INMET Mensal (merge)] ================
    # Objetivo:
    #     Unir temperatura média mensal e precipitação total ao histórico.
    # Fluxo:
    #     renomear temperatura -> drop dups -> merge em ["Ano","Mês"] -> futuridade
    # ==================================================================================
    if mensais_inmet is not None and not mensais_inmet.empty:
        # Renomear temperatura para manter compatibilidade com o nome já usado no histórico
        mm = mensais_inmet.rename(columns={"Temperatura Média (°C)": "Temp. Média (ºC)"}).copy()
        mm = mm.drop_duplicates(subset=["Ano", "Mês"], keep="last")

        # Merge de temperatura e precipitação mensais
        df = df.merge(
            mm[["Ano", "Mês", "Temp. Média (ºC)", "Precipitacao Total (mm)"]],
            on=["Ano", "Mês"], how="left"
        )

        # Blindagem de futuridade usando o máximo (Ano, Mês) presente no INMET mensal
        try:
            MES_PT_PARA_NUM = {v: k for k, v in MES_NUM_PARA_PT.items()}
            mm["_MesNum"] = mm["Mês"].map(MES_PT_PARA_NUM).astype("Int64")
            ref = mm.dropna(subset=["Ano", "_MesNum"])
            if not ref.empty:
                ymax = int(ref["Ano"].max())
                mmax = int(ref[ref["Ano"] == ymax]["_MesNum"].max())
                _mesnum_df = df["Mês"].map(MES_PT_PARA_NUM).astype("Int64")
                mask_futuro = (df["Ano"] > ymax) | ((df["Ano"] == ymax) & (_mesnum_df > mmax))
                # Zera apenas campos vindos do INMET mensal
                for col in ["Temp. Média (ºC)", "Precipitacao Total (mm)"]:
                    if col in df.columns:
                        df.loc[mask_futuro, col] = np.nan
        except Exception as e:
            logging.warning(f"Validação de futuridade (INMET) falhou: {e}")
    else:
        logging.info("Sem dados INMET mensais (XLSX) para mesclar.")

    # === [Seção tratamento_tabela_historica-090: Ordenação/posicionamento] ===========
    # Objetivo:
    #     Ordenar (Ano, Mês) e garantir a posição das novas colunas no layout final.
    # Fluxo:
    #     sort -> reindex para mover "Consumo Total Cummins", "% Chillers", "Valor do KWh (R$)"
    # ==================================================================================
    df["Mês"] = pd.Categorical(df["Mês"], categories=MESES_ORDEM, ordered=True)
    df = df.sort_values(["Ano", "Mês"]).reset_index(drop=True)

    # Remover qualquer sobra legada "Preco_KWh_R$" antes de posicionar colunas
    if "Preco_KWh_R$" in df.columns:
        df.drop(columns=["Preco_KWh_R$"], inplace=True, errors="ignore")

    # Garantir ordem:
    # - "Consumo Total Cummins (KWh)" após "COP Médio"
    # - "Porcentagem Consumida pelo Chillers (%)" após "Consumo Total Cummins (KWh)"
    # - "Valor do KWh (R$)" após "Porcentagem Consumida pelo Chillers (%)"
    colunas = list(df.columns)
    def _move_after(cols, col_to_move, after_col):
        if col_to_move in cols and after_col in cols:
            cols.remove(col_to_move)
            idx = cols.index(after_col) + 1
            cols.insert(idx, col_to_move)
        return cols

    colunas = _move_after(colunas, "Consumo Total Cummins (KWh)", "COP Médio")
    colunas = _move_after(colunas, "Porcentagem Consumida pelo Chillers (%)", "Consumo Total Cummins (KWh)")
    colunas = _move_after(colunas, "Valor do KWh (R$)", "Porcentagem Consumida pelo Chillers (%)")
    df = df.reindex(columns=colunas)

    # === [Seção tratamento_tabela_historica-100: Bandeiras tarifárias] ================
    # Objetivo:
    #     Anexar bandeira acionada e valor adicional mensal (se disponível).
    # Fluxo:
    #     ler CSV -> limpar/normalizar -> dedup por (Ano, MesNum) -> merge
    # ==================================================================================
    MES_PT_PARA_NUM = {v: k for k, v in MES_NUM_PARA_PT.items()}
    df["MesNum"] = df["Mês"].map(MES_PT_PARA_NUM).astype("Int64")

    arq_bandeira = DADOS_SISTEMA_ELETRICO_DIR / "bandeira_tarifaria_acionamento.csv"
    if arq_bandeira.exists():
        try:
            bandeiras = pd.read_csv(arq_bandeira, sep=";", encoding="latin-1")
            bandeiras["DatCompetencia"] = pd.to_datetime(bandeiras["DatCompetencia"], errors="coerce")
            bandeiras["Ano"] = bandeiras["DatCompetencia"].dt.year
            bandeiras["MesNum"] = bandeiras["DatCompetencia"].dt.month

            bandeiras["Valor Adicional da Bandeira"] = (
                bandeiras["VlrAdicionalBandeira"].astype(str)
                .str.replace(r"[^\d,.\-+]", "", regex=True)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.extract(r"([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)")[0]
                .astype(float)
            )
            bandeiras = bandeiras.rename(columns={"NomBandeiraAcionada": "Bandeira Acionada"})

            bandeiras = (
                bandeiras.sort_values(["Ano", "MesNum", "DatCompetencia"])
                .drop_duplicates(subset=["Ano", "MesNum"], keep="last")
                [["Ano", "MesNum", "Bandeira Acionada", "Valor Adicional da Bandeira"]]
            )

            df = df.merge(bandeiras, on=["Ano", "MesNum"], how="left")
        except Exception as e:
            logging.warning(f"Falha ao aplicar bandeiras tarifárias: {e}")

    df.drop(columns=["MesNum"], errors="ignore", inplace=True)

    # === [Seção tratamento_tabela_historica-110: Persistência] ========================
    # Objetivo:
    #     Persistir a planilha final "Histórico Tratado" com backup prévio, se existir.
    # Fluxo:
    #     backup_saida -> ExcelWriter(openpyxl) -> salvar aba única
    # ==================================================================================
    if destino.exists():
        backup_saida(destino)
    with pd.ExcelWriter(destino, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Histórico Tratado", index=False)

    logging.info(f"Tabela_Historico_Tratada.xlsx gerada : {destino}\n")
