# === META ========================================================= 
# Módulo: baixar_CO2_sistema_eletrico (SIRENE/MCTI)
# Versão: v1.2.0 (regras por ano + parser ANUAL robusto)
# Tabela alvo: inventário-todos (xlsx) e equivalentes do layout novo
# Saída chave: Inventario_Fator_Medio_Anual_CO2_KWh.csv
# ==================================================================

import logging
import re
import time
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

from _executores.utils import DADOS_SISTEMA_ELETRICO_DIR, backup_saida

# === [Seção baixar_CO2-005: Função compartilhada de download (retentativas)] =========
# Objetivo:
#     Baixar arquivos com retentativas, timeout e verificações simples.
# Fluxo:
#     baixar_arquivo_com_retentativas(url, destino, tentativas=3, delay=5, verificar_ssl=True)
# Observações:
#     Mantém compatibilidade com demais scripts (User-Agent, timeout 60s).
# ======================================================================================
def baixar_arquivo_com_retentativas(url, destino, tentativas=3, delay=5, verificar_ssl=True):
    """Baixa um arquivo com retentativas e verificação básica."""
    UA = {"User-Agent": "CumminsChillers/1.0 (+diagnostics)"}
    for tentativa in range(1, tentativas + 1):
        try:
            with httpx.stream("GET", url, timeout=60.0, verify=verificar_ssl, headers=UA) as resposta:
                if resposta.status_code == 200:
                    with open(destino, "wb") as f:
                        for bloco in resposta.iter_bytes():
                            f.write(bloco)
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


# === [Seção baixar_CO2-010: Garantia de base 2024 (compatibilidade)] ==================
# Objetivo:
#     Garantir a presença da planilha base 2024 (jandez) antes do fluxo principal.
# Saídas:
#     - Inventario_2024_jandez.xlsx em DADOS_SISTEMA_ELETRICO_DIR
# ======================================================================================
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


# === [Seção baixar_CO2-015: Descoberta e download do ano corrente] ====================
# Objetivo:
#     Descobrir e baixar as planilhas do ano atual, mantendo apenas a mais recente.
# Fluxo:
#     descobrir_links_por_ano -> baixar arquivos -> limpar versões antigas
# ======================================================================================
def baixar_planilhas_sistema_eletrico_ano_atual():
    """Baixa planilhas do ano corrente e mantém apenas a mais recente (descoberta + normalização)."""

    import unicodedata, urllib.parse as _up
    from bs4 import BeautifulSoup

    def norm_ascii(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if ord(ch) < 128)

    def mes_final_tres_letras(nome_ou_url: str) -> str | None:
        alvo = _up.unquote(str(nome_ou_url))
        alvo = norm_ascii(alvo).lower()
        m = re.search(r'([a-z]{3})(?=[^a-z]*\.xlsx(?:$|\?))', alvo)
        return m.group(1) if m else None

    def sufixo_canonico(mes3: str) -> str | None:
        mapa = {
            "fev": "janfev", "mar": "janmar", "abr": "janabr", "mai": "janmai",
            "jun": "janjun", "jul": "janjul", "ago": "janago", "set": "janset",
            "out": "janout", "nov": "jannov", "dez": "jandez",
        }
        return mapa.get(mes3)

    def descobrir_links_por_ano(pagina: str, ano: int) -> dict:
        try:
            UA = {"User-Agent": "CumminsChillers/1.0 (+diagnostics)"}
            r = httpx.get(pagina, headers=UA, timeout=60.0)
            r.raise_for_status()
        except Exception as e:
            logging.warning(f"Falha ao carregar página do MCTI para descoberta: {e}")
            return {}
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

    logging.info("Baixando dados do Sistema Elétrico Nacional...\n...")
    ano = datetime.now().year
    sufixos_ordem = ["janfev","janmar","janabr","janmai","janjun","janjul",
                     "janago","janset","janout","jannov","jandez"]
    destino = DADOS_SISTEMA_ELETRICO_DIR
    destino.mkdir(parents=True, exist_ok=True)

    pagina_base = "https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/sirene/dados-e-ferramentas/fatores-de-emissao/"
    links_descobertos = descobrir_links_por_ano(pagina_base, ano)

    base_url = f"{pagina_base}/arquivo"

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


# === [Seção baixar_CO2-020: Inventário SIRENE — parser ANUAL (layout novo)] ==========
# Objetivo:
#     Detectar o bloco "Fator de Emissão Médio (tCO2/MWh) - ANUAL" em planilhas novas
#     e extrair pares (ANO, Fator Médio Anual) diretamente do quadro anual.
# Fluxo:
#     _norm_ascii_compacto -> _achar_celula_cabecalho_anual -> extrair_fator_anual_layout_novo
# Contratos:
#     - Conversão: 1 tCO2/MWh == 1 kgCO2/kWh (equivalência dimensional).
#     - Aceita vírgula decimal; ignora células com texto do tipo "disponível no início de 20xx".
# Observações:
#     - Não altera o parser antigo; funciona como ramo para a nova diagramação.
# ======================================================================================
import unicodedata

def _norm_ascii_compacto(s: str) -> str:
    """Normaliza: remove acentos, minúsculas e compacta espaços."""
    if not isinstance(s, str):
        s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s.strip().lower())
    return s

def _achar_celula_cabecalho_anual(df: pd.DataFrame):
    """Procura a célula cujo texto contenha 'fator de emissao medio' e 'anual' e 'tco2/mwh'."""
    alvo1, alvo2 = "fator de emissao medio", "anual"
    for r in range(df.shape[0]):
        for c in range(df.shape[1]):
            try:
                txt = _norm_ascii_compacto(df.iat[r, c])
                if alvo1 in txt and alvo2 in txt and "tco2/mwh" in txt.replace(" ", ""):
                    return r, c
            except Exception:
                continue
    return None, None

def extrair_fator_anual_layout_novo(caminho_xlsx: Path, sheet_hint: str | None = None) -> pd.DataFrame:
    """
    Extrai (ANO, Fator Médio Anual (kgCO2/kWh)) a partir do quadro:
        'Fator de Emissão Médio (tCO2/MWh) - ANUAL'
    Estratégia robusta:
      - Localiza o cabeçalho ANUAL.
      - Lê a LINHA IMEDIATAMENTE ABAIXO (e, por segurança, também a seguinte).
      - Varre TODAS as colunas dessas linhas, da esquerda para a direita,
        parando apenas quando capturar (ANO válido >= 2020) e (VALOR numérico).
      - Ignora mensagens do tipo 'disponível no início de 20xx'.
      - Converte vírgula decimal; tCO2/MWh → kgCO2/kWh (1:1).
    """
    try:
        xls = pd.ExcelFile(caminho_xlsx)
    except Exception as e:
        logging.warning(f"CO2/ANUAL: falha ao abrir '{caminho_xlsx.name}': {e}")
        return pd.DataFrame(columns=["ANO", "Fator Médio Anual (kgCO2/kWh)"])

    sheets = [sheet_hint] if (sheet_hint and sheet_hint in xls.sheet_names) else xls.sheet_names
    resultados = []

    for sh in sheets:
        try:
            df = pd.read_excel(xls, sheet_name=sh, header=None, dtype=object, engine="openpyxl")
        except Exception as e:
            logging.warning(f"CO2/ANUAL: erro ao ler aba '{sh}': {e}")
            continue

        r0, c0 = _achar_celula_cabecalho_anual(df)
        if r0 is None:
            continue

        logging.info(f"CO2/ANUAL (layout novo) detectado em '{sh}' [r={r0}, c={c0}].")

        # Vamos olhar a linha logo abaixo (r0+1) e, se necessário, também (r0+2)
        linhas_candidatas = [r0 + 1]
        if r0 + 2 < df.shape[0]:
            linhas_candidatas.append(r0 + 2)

        ano_pat = re.compile(r"\b(20\d{2})\b")  # somente anos 2000+; filtraremos >= 2020
        aviso_pat = re.compile(r"disponivel|disponível|inicio|início", re.I)

        def _nums_from_cell(v):
            """Tenta converter um valor de célula em float (aceita vírgula decimal)."""
            try:
                if isinstance(v, (int, float)):
                    return float(v)
                s = str(v).strip()
                if aviso_pat.search(_norm_ascii_compacto(s)):
                    return None
                s2 = s.replace(".", "").replace(",", ".")
                return float(s2)
            except Exception:
                return None

        ano_capturado = None
        valor_capturado = None

        for r in linhas_candidatas:
            if r >= df.shape[0]:
                continue
            # Varre todas as colunas da linha r
            for c in range(df.shape[1]):
                cell = df.iat[r, c]
                # 1) tenta achar ano(s)
                text = str(cell)
                for m in re.finditer(ano_pat, text):
                    y = int(m.group(1))
                    if y >= 2020:  # ignorar anteriores a 2020
                        ano_capturado = y
                        logging.info(f"CO2/ANUAL: ano {y} detectado em '{sh}' [r={r}, c={c}].")
                        # não quebra aqui; pode ter o valor na mesma célula ou adjacentes
                # 2) tenta achar valor numérico
                num = _nums_from_cell(cell)
                if num is not None and 0 <= num <= 2.0:  # faixa segura
                    valor_capturado = num
                    logging.info(f"CO2/ANUAL: valor anual detectado em '{sh}' [r={r}, c={c}] = {num}.")
                # Se ambos já existem, podemos parar a varredura completa
                if (ano_capturado is not None) and (valor_capturado is not None):
                    break
            if (ano_capturado is not None) and (valor_capturado is not None):
                break

        if (ano_capturado is not None) and (valor_capturado is not None):
            resultados.append({"ANO": int(ano_capturado),
                               "Fator Médio Anual (kgCO2/kWh)": float(valor_capturado)})
        else:
            # Se só houver mensagem de indisponibilidade, não grava nada
            logging.info("CO2/ANUAL: ano/valor não encontrados (provável indisponibilidade).")

        if resultados:
            break  # não precisa varrer outras abas

    if not resultados:
        logging.info("CO2/ANUAL: nenhum fator anual localizado pelo parser de layout novo.")
        return pd.DataFrame(columns=["ANO", "Fator Médio Anual (kgCO2/kWh)"])

    out = (pd.DataFrame(resultados)
             .drop_duplicates(subset=["ANO"], keep="last")
             .sort_values("ANO"))
    return out


# === [Seção baixar_CO2-030: Tratamento consolidado (antigo + layout novo)] ===========
# Objetivo:
#     Extrair (ano, fator) do layout tradicional (apenas 2020–2024, fixo 2024_jandez)
#     e complementar 2025+ com o parser ANUAL do layout novo (apenas 'janago' em diante).
# Fluxo:
#     tratar_inventario_fator_medio_anual_C02_KWh ->
#         _extrair_antigo_2024_jandez -> extrair_novo_2025_em_diante -> saneamento/saida
# Saídas:
#     - CSV: Inventario_Fator_Medio_Anual_CO2_KWh.csv (sep=';')
# ======================================================================================
def tratar_inventario_fator_medio_anual_C02_KWh():
    """Extrai (ano, fator) do layout antigo (2020–2024, arquivo 2024_jandez) e do layout novo (2025+)."""
    logging.info("Tratando dados do Sistema Elétrico Nacional...")
    pasta = DADOS_SISTEMA_ELETRICO_DIR
    pasta.mkdir(parents=True, exist_ok=True)

    # --- 1) Parser "antigo" SOMENTE do arquivo base 2024_jandez (anos 2020–2024) ----
    dados_antigo = []
    base_2024 = pasta / "Inventario_2024_jandez.xlsx"
    if base_2024.exists():
        try:
            df = pd.read_excel(base_2024, sheet_name="inventário-todos", header=None, engine="openpyxl")
            try:
                coluna_14 = df.iloc[:, 14].dropna().reset_index(drop=True)
            except Exception:
                coluna_14 = pd.Series([], dtype=object)

            for i in range(len(coluna_14) - 1):
                texto = str(coluna_14[i])
                if "ANO - 20" in texto:
                    match = re.search(r"20\d{2}", texto)
                    if match:
                        ano = int(match.group())
                        if 2020 <= ano <= 2024:
                            try:
                                prox = coluna_14[i + 1]
                                if isinstance(prox, (int, float)):
                                    valor_t = float(prox)
                                else:
                                    valor_t = float(str(prox).replace(".", "").replace(",", "."))
                                dados_antigo.append({"ANO": ano, "Fator Médio Anual (kgCO2/kWh)": round(valor_t, 6)})
                            except Exception:
                                continue
            if dados_antigo:
                logging.info("CO2/ANTIGO: anos até 2024 carregados apenas de Inventario_2024_jandez.xlsx")
        except Exception as e:
            logging.warning(f"CO2/ANTIGO: falha ao processar Inventario_2024_jandez.xlsx: {e}")
    else:
        logging.warning("CO2/ANTIGO: Inventario_2024_jandez.xlsx não encontrado.")

    df_antigo = (pd.DataFrame(dados_antigo)
                   .drop_duplicates(subset=["ANO"], keep="last")
                   .sort_values("ANO")) if dados_antigo else pd.DataFrame(columns=["ANO", "Fator Médio Anual (kgCO2/kWh)"])

    # --- 2) Parser "layout novo" SOMENTE 2025+ e a partir de 'janago' ----------------
    # Critério de aceitação: nome 'Inventario_{ano}_{sufixo}.xlsx' com ano >= 2025 e sufixo em {janago, janset, janout, jannov, jandez}
    sufixos_validos_novo = {"janago", "janset", "janout", "jannov", "jandez"}
    arquivos = list(pasta.glob("Inventario_*.xlsx"))
    df_novo_total = []
    for arquivo in arquivos:
        nome = arquivo.name
        m = re.match(r"Inventario_(\d{4})_([a-z]+)\.xlsx$", nome, flags=re.I)
        if not m:
            # Pode ser o 2024_jandez (já processado) ou variações — ignorar aqui
            continue
        ano_arq = int(m.group(1))
        sufixo = m.group(2).lower()
        if ano_arq < 2025:
            # Layout novo só entra para 2025+
            continue
        if sufixo not in sufixos_validos_novo:
            logging.info(f"CO2/NOVO: arquivo ignorado para anual (anterior a janago): {nome}")
            continue

        logging.info(f"CO2/NOVO: arquivo aceito para anual: {nome}")
        try:
            extr = extrair_fator_anual_layout_novo(arquivo)
            if extr is not None and not extr.empty:
                # Filtra por segurança: apenas anos >= 2025
                extr = extr[extr["ANO"] >= 2025]
                if not extr.empty:
                    df_novo_total.append(extr)
        except Exception as e:
            logging.warning(f"CO2/NOVO: falha ao extrair de {arquivo.name}: {e}")

    df_novo = (pd.concat(df_novo_total, ignore_index=True)
                 .drop_duplicates(subset=["ANO"], keep="last")
                 .sort_values("ANO")) if df_novo_total else pd.DataFrame(columns=["ANO", "Fator Médio Anual (kgCO2/kWh)"])

    # --- 3) Consolidação com precedência e regras de faixa ---------------------------
    # Base: df_antigo (2020–2024). Complemento: df_novo (>=2025).
    if not df_antigo.empty and not df_novo.empty:
        df_final = pd.concat([df_antigo, df_novo], ignore_index=True)
    elif not df_novo.empty:
        df_final = df_novo.copy()
    else:
        df_final = df_antigo.copy()

    # --- 4) Saneamento forte ---------------------------------------------------------
    if df_final.empty:
        logging.warning("CO2: Nenhum fator médio anual pôde ser extraído (antigo e novo).")
        return

    # Tipos numéricos
    df_final["ANO"] = pd.to_numeric(df_final["ANO"], errors="coerce")
    df_final["Fator Médio Anual (kgCO2/kWh)"] = pd.to_numeric(df_final["Fator Médio Anual (kgCO2/kWh)"], errors="coerce")

    # Filtrar por faixa de interesse (>=2020) e faixa razoável de números
    antes = len(df_final)
    df_final = df_final[(df_final["ANO"].notna()) & (df_final["Fator Médio Anual (kgCO2/kWh)"].notna())]
    df_final = df_final[(df_final["ANO"] >= 2020) & (df_final["ANO"] <= 2100)]
    df_final = df_final[(df_final["Fator Médio Anual (kgCO2/kWh)"] >= 0.0) & (df_final["Fator Médio Anual (kgCO2/kWh)"] <= 2.0)]
    depois = len(df_final)
    if depois < antes:
        logging.warning(f"CO2: {antes - depois} linha(s) descartada(s) por saneamento (ano/fator inválidos).")

    # Deduplicar por ano, mantendo o último (preferência natural: entradas mais recentes)
    df_final = df_final.drop_duplicates(subset=["ANO"], keep="last").sort_values("ANO")

    # Log de cobertura
    anos_list = df_final["ANO"].astype(int).tolist()
    anos_ate_2024 = [a for a in anos_list if a <= 2024]
    anos_pos_2024 = [a for a in anos_list if a >= 2025]
    if anos_ate_2024:
        logging.info(f"CO2: anos consolidados (base 2024) = {min(anos_ate_2024)}..{max(anos_ate_2024)}")
    if anos_pos_2024:
        logging.info(f"CO2: anos consolidados (layout novo) = {min(anos_pos_2024)}..{max(anos_pos_2024)}")

    # --- 5) Persistência --------------------------------------------------------------
    output_csv = pasta / "Inventario_Fator_Medio_Anual_CO2_KWh.csv"
    if output_csv.exists():
        backup_saida(output_csv)
    try:
        df_final.to_csv(output_csv, sep=';', index=False, encoding='latin1')
        logging.info(f"Dados consolidados e salvos com sucesso em: {output_csv}\n")
    except Exception as e:
        logging.warning(f"Falha ao salvar CSV de fator médio anual: {e}")


# === [Seção baixar_CO2-040: Orquestração do fluxo] ================================
# Objetivo:
#     Baixar/atualizar planilhas, extrair fatores (antigo + novo) e salvar CSV.
# Fluxo:
#     garantir_base_inventario_2024 -> tratar(...) -> baixar ano atual -> tratar(...)
# ===================================================================================
def baixar_CO2_sistema_eletrico():
    """Orquestra o fluxo de download e tratamento do inventário do Sistema Elétrico Nacional."""
    logging.info("Iniciando atualização dos dados do Sistema Elétrico Nacional...\n")
    garantir_base_inventario_2024()
    # 1ª passagem: extrai do que já existe localmente (inclui base 2024)
    tratar_inventario_fator_medio_anual_C02_KWh()
    # Baixa/atualiza ano corrente e consolida de novo
    baixados = baixar_planilhas_sistema_eletrico_ano_atual()
    if not baixados:
        logging.info("Nenhum novo arquivo foi baixado. Dados já estão atualizados.\n")
    tratar_inventario_fator_medio_anual_C02_KWh()
