# 🎬 Fluxo do Sistema Cummins Chillers

> Guia completo (com emojis) do pipeline: app → orquestrador → downloaders → tratamento → estimativas → validação → utilidades.

---

## 1️⃣ Início e Configuração 🏁🛠
**📦 App:** `AtualizarCumminsDashboard`  
**📄 Orquestrador:** `Atualizar_dados.py`

- 🚀 Inicia logs em `_logs/` e imprime os diretórios ativos.
- ✅ Valida ambiente: versão do Python, bibliotecas e internet.
- 🧪 Testa import de todos os módulos antes de seguir.
- 📦 Move instaladores produzidos (`Cummins Chillers Dashboard.rar`, `CumminsDashboardInstaller.exe`) para `/_instaladores`.

Diagrama (App): [PNG](sandbox:/mnt/data/fluxo_app_atualizar_cummins_dashboard.png)  
Diagrama (Orquestrador): [PNG](sandbox:/mnt/data/fluxo_atualizar_dados_v2.png)

---

## 2️⃣ Coleta de Dados Online ☁️📥
Módulos:  
- `/_executores/baixar_inmet.py` (Mirante SP — INMET)  
- `/_executores/baixar_CO2_sistema_eletrico.py` (SIRENE/MCTI)  
- `/_executores/baixar_bandeiras_tarifarias.py` (ANEEL)

**🌡 INMET – Mirante SP**  
- Baixa arquivos ZIP anuais (2020 → ano atual), extrai e filtra a estação **A701 – São Paulo/Mirante**.  
- Trata duplicatas do ano corrente e consolida colunas-chave.  
- 📤 Saída: `dados_inmet/Dados_Tratados_INMET_Mirante_De_São_Paulo.csv` (latin-1, `;`).

**⚡ Sistema Elétrico (CO₂/kWh)**  
- Garante a base `Inventario_2024_jandez.xlsx`.  
- Descobre e baixa o **xlsx** mais recente do ano corrente; mantém **apenas o último**.  
- Extrai (ANO, *kgCO2/kWh*) da aba **“inventário-todos”** ≥ 2020.  
- 📤 Saída: `dados_sistema_eletrico_brasil/Inventario_Fator_Medio_Anual_CO2_KWh.csv` (latin-1, `;`).

**🏳️ Bandeiras Tarifárias (ANEEL)**  
- Verifica se o mês atual já está presente; se não, baixa **assíncrono**:  
  - `bandeira_tarifaria_acionamento.csv`  
  - `bandeira_tarifaria_adicional.csv`  
- 📤 Saídas diretas em `dados_sistema_eletrico_brasil/` (latin-1, `;`).

Diagrama Mestre (Downloaders): [PNG](sandbox:/mnt/data/diagrama_mestre_downloads.png)

---

## 3️⃣ Tratamento de Dados Locais 🖥📊
Módulos principais:  
- `/_executores/tratamento_dados.py` (núcleo)  
- `/_executores/tratamento_consumo.py` (KWh/Consumo Total)  
- `/_executores/tratamento_tabela_historica.py` (histórico consolidado)

**📍 Geolocalização** → `Info_Geografica_Guarulhos.csv`  
**🧾 Chiller (entrada)**  
- Move `CHILLERS.xlsx`/`CHILLERS.csv` → `dados_cummins/Dados do Chiller.xlsx` (converte se CSV).

**🥶 Parsing & Normalização do Chiller**  
- Detecta formato **tabelado** ou **tokenizado/achatado**.  
- Gera arquivos de transição quando necessário:  
  - `_transicoes/Chiller_raw_separado.csv`  
  - `_transicoes/Chiller_amostras_normalizadas.csv`  
- Calcula `step_h` (mediana dos deltas → 1/5/10/15/30/60 min).  
- Agrega **mensal**: `Consumo (KWh)` e `Horas Trabalhadas (h)` com limiar `potencia_min_kw`.

**🌡 Temperatura mensal (INMET)**  
- Média mensal das médias diárias.

**💲 KWh por Ano**  
- Processa `KWH.xlsx` → `Valor_KWh_Ano.xlsx` (regra: 2022 = **0,5**; dedup/ordem).

**🔢 Consumo Total**  
- `CONSUMO_TOTAL.xlsx` → `Consumo_Total_Cummins.xlsx` (Ano, Mês, Nome do Mês, Consumo (KWh)).

**📚 Histórico Consolidado** (`construir_tabela_historico_de_chiller`)  
- Une **Chiller + INMET + KWh + CO₂ + Bandeiras**.  
- Calcula `Gasto da Operacao (R$)` e `CO2 Emitido (Kg)`.  
- 📤 Saída: `dados_cummins/Tabela_Historico_Tratada.xlsx` (com backup automático).

Diagrama Mestre (Tratamento): [PNG](sandbox:/mnt/data/diagrama_mestre_tratamento_dados.png)

---

## 4️⃣ Estimativas de Consumo 📐📅
Módulo: `/_executores/estimativas_dados.py`

- **Helpers**:  
  - `media_historica_corrigida()` → IQR + peso temporal.  
  - `estimar_consumo_mensal()` → min/esperado/máx (+ parcial do mês).  
  - `calcular_diferenca()` → Δ e tendência %.  
- **Ano vigente**: passados = **Real**; atual = **Corrigido**; futuros = **Projetado** (com **viés** global/mensal quando disponível).  
- **Ano seguinte**: tudo **Projetado** + **viés** (global/mensal).  
- Concatena, arredonda (2c) e salva.  
- 📤 Saída: `dados_cummins/Estimativa_Consumo_Consolidado.csv` (UTF-8-BOM, `;`, backup).

Diagramas:  
- Fluxo (Estimativas): [PNG](sandbox:/mnt/data/estimativas_fluxo_com_legenda.png)  
- Diagrama Mestre (Estimativa & Acurácia): [PNG](sandbox:/mnt/data/diagrama_mestre_estimativas_acuracia.png)

---

## 5️⃣ Validação de Acurácia 🎯✅
Módulo: `/_executores/validacao_acuracia.py`

- Compara **Real** (`Tabela_Historico_Tratada.xlsx`) vs **Estimado** (`Estimativa_Consumo_Consolidado.csv`).  
- Métricas:  
  - `% Erro Consumo`, `% Erro Horas`, `% Erro Temp` (evita divisão por zero).  
  - `Fator Utilizacao = Consumo / (Potência Média * Horas)`.  
- Normaliza Mês (capitalize), ordena por `MESES_ORDEM` e salva CSV com backup.  
- 📤 Saída: `dados_cummins/Historico_Precisao_Estimativas.csv` (UTF-8-BOM, `;`).

Fluxo (Validação): [PNG](sandbox:/mnt/data/validacao_acuracia_fluxo_com_legenda.png)

---

## 6️⃣ Utilidades e Suporte 🔧📦
Módulo: `/_executores/utils.py`

- 🗂 Diretórios-base: `BASE_DIR`, `DADOS_INMET_DIR`, `DADOS_SISTEMA_ELETRICO_DIR`, `DADOS_CUMMINS_DIR`, `_transicoes/`, `_logs/`, `_instaladores/`.  
- 🌎 `CUMMINS_BASE_DIR` (env var) permite apontar o diretório-base.  
- 🧰 Verificações: Python, libs (`httpx`, `pandas`, `numpy`, `openpyxl`, `matplotlib`).  
- 🌐 `verificar_conexao_internet()` (HTTP 204/200).  
- 💾 `backup_saida()` com retenção automática.  
- 📝 `Imprimir_Todos_Scripts()` salva uma cópia integral dos scripts executados em `_logs/`.

Diagrama (Utils): [PNG](sandbox:/mnt/data/utils_fluxo_vertical.png)

---

## 7️⃣ Encerramento 🏆📝
**📄 Orquestrador** finaliza registrando:  
- Scripts utilizados, tempo total de execução, caminhos e logs completos.  
- Conclusão com rastreabilidade e auditoria.

---

## 📁 Estrutura de Pastas (alto nível)
```
BASE_DIR/
├─ _executores/
│  ├─ utils.py
│  ├─ baixar_inmet.py
│  ├─ baixar_CO2_sistema_eletrico.py
│  ├─ baixar_bandeiras_tarifarias.py
│  ├─ tratamento_dados.py
│  ├─ tratamento_consumo.py
│  ├─ tratamento_tabela_historica.py
│  ├─ estimativas_dados.py
│  └─ validacao_acuracia.py
├─ dados_inmet/
├─ dados_sistema_eletrico_brasil/
├─ dados_cummins/
│  └─ _transicoes/
├─ _instaladores/
├─ _logs/
└─ Atualizar_dados.py
```

---

## ▶️ Execução Rápida
**Via script (dev):**
```bash
python Atualizar_dados.py
```
**Via app (build):** abrir **AtualizarCumminsDashboard**.

> Dica: ajuste `CUMMINS_BASE_DIR` (variável de ambiente) para executar em outro diretório de trabalho.

---

## ⚙️ Parâmetros comuns
- `construir_tabela_historico_de_chiller(potencia_min_kw=0.0)` → defina o limiar de “ligado” do chiller.  
- `KWH.xlsx` → mantém regra fixa para 2022 = **0,5** (fluxo atual).

---

## 🧪 Requisitos
- Python 3.8–3.13
- `pip install httpx pandas numpy openpyxl matplotlib beautifulsoup4 lxml`

> *Observação*: `beautifulsoup4`/`lxml` são utilizados na descoberta de links do inventário MCTI.

---

## 🧭 Troubleshooting
- **Arquivo aberto**: erro ao salvar → feche o Excel e rode de novo.
- **Sem internet**: verifique proxy/firewall; `verificar_conexao_internet()` deve retornar 204/200.
- **Bandeiras não atualizam**: confira se o CSV local já contém o mês corrente.
- **INMET vazio**: verifique se o ZIP do ano foi baixado; atenção à estação **A701**.

---

## 🚀 Melhorias Possíveis
**Documentação**: README/CHANGELOG (SemVer).  
**Arquitetura**: centralizar configs (`.env`/`config.toml`), CLI (`--pot-min`, `--only-estimativas`).  
**Qualidade**: pre-commit (ruff/black/mypy), testes unitários dos parsers e do fluxo.  
**Dados**: validação de esquema, checagem de hash/tamanho, cache local de downloads.  
**Estimativas**: alternativa com regressão linear (variáveis: temp média, horas trabalhadas, mês, consumo anterior) com fallback.  
**Observabilidade**: relatórios automáticos (MAPE/MAE), versão de modelo nos logs.  
**Operação**: agendamento (cron/Task Scheduler) e CI/CD básico.

---

## 🧭 Diagramas (referência rápida)
- App: `fluxo_app_atualizar_cummins_dashboard.png`  
- Orquestrador: `fluxo_atualizar_dados_v2.png`  
- Downloaders: `diagrama_mestre_downloads.png`  
- Tratamento: `diagrama_mestre_tratamento_dados.png`  
- Estimativas (fluxo): `estimativas_fluxo_com_legenda.png`  
- Estimativa & Acurácia (mestre): `diagrama_mestre_estimativas_acuracia.png`  
- Validação: `validacao_acuracia_fluxo_com_legenda.png`
