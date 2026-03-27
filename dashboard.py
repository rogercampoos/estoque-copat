import os
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
import glob as _glob
import re
import random
from datetime import date, timedelta

# --- Configuração ---
PROJECT = "secret-air-472101-f1"
DATASET = "Controle_de_estoque"
SCOPES  = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive.readonly"
]

st.set_page_config(
    page_title="COPAT — Controle de Estoque",
    page_icon="⚖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Design System: Precision & Density / Cool Slate / Borders-only ---
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', system-ui, sans-serif; }

  #MainMenu { visibility: hidden; }
  footer { visibility: hidden; }
  [data-testid="stHeader"] { visibility: hidden; }
  [data-testid="stExpandSidebarButton"] { visibility: visible !important; }
  .block-container { padding: 2rem 2.5rem 2rem 3.5rem !important; }

  /* Header institucional */
  .inst-header {
    display: flex; align-items: center; gap: 1rem;
    padding-bottom: 1.25rem;
    border-bottom: 1px solid rgba(15,23,42,0.10);
    margin-bottom: 1.5rem;
  }
  .inst-badge {
    background: #1e3a8a; color: #fff;
    font-size: 0.75rem; font-weight: 600;
    letter-spacing: 0.08em; text-transform: uppercase;
    padding: 0.3rem 0.7rem; border-radius: 3px;
    white-space: nowrap;
  }
  .inst-title { font-size: 1rem; font-weight: 600; color: #0f172a !important; margin: 0; line-height: 1.3; background: #fff; }
  .inst-sub   { font-size: 0.73rem; color: #64748b !important; margin: 0; }
  .inst-header { background: #fff; padding: 1rem 1.25rem; border-radius: 6px; border: 1px solid rgba(15,23,42,0.08); }

  /* KPI Cards */
  .kpi-grid {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem; margin-bottom: 1.5rem;
  }
  .kpi-card {
    background: #fff;
    border: 1px solid rgba(15,23,42,0.08);
    border-radius: 6px; padding: 1rem 1.25rem;
  }
  .kpi-label {
    font-size: 0.68rem; font-weight: 500; color: #64748b;
    letter-spacing: 0.07em; text-transform: uppercase; margin-bottom: 0.3rem;
  }
  .kpi-value {
    font-size: 1.65rem; font-weight: 600; color: #0f172a;
    font-variant-numeric: tabular-nums; line-height: 1.1;
  }
  .kpi-value.mono {
    font-family: 'SF Mono','Fira Code','Consolas',monospace; font-size: 1.2rem;
  }
  .kpi-sub { font-size: 0.68rem; color: #94a3b8; margin-top: 0.2rem; }

  /* Títulos de seção */
  .sec-title {
    font-size: 0.68rem; font-weight: 600; color: #64748b;
    letter-spacing: 0.08em; text-transform: uppercase;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid rgba(15,23,42,0.08);
    margin-bottom: 1rem;
  }
  .chart-label { font-size: 0.75rem; font-weight: 500; color: #475569; margin-bottom: 0.5rem; }

  /* Divisor */
  hr { border-color: rgba(15,23,42,0.08) !important; margin: 1.25rem 0 !important; }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background: #f8fafc !important;
    border-right: 1px solid rgba(15,23,42,0.08) !important;
  }
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span,
  [data-testid="stSidebar"] h3,
  [data-testid="stSidebar"] input {
    color: #0f172a !important;
  }
  [data-testid="stSidebarContent"] { padding: 1.5rem 1rem !important; }

  /* Inputs */
  [data-testid="stSelectbox"] > div > div,
  [data-testid="stDateInput"] > div > div,
  [data-testid="stTextInput"] input {
    background: #f1f5f9 !important;
    border: 1px solid rgba(15,23,42,0.12) !important;
    border-radius: 5px !important;
    font-size: 0.82rem !important;
  }
  [data-testid="stSelectbox"] { cursor: pointer !important; }
  [data-testid="stSelectbox"] * { cursor: pointer !important; }
  [data-testid="stTextInput"] input:focus {
    border-color: rgba(30,64,175,0.4) !important;
    box-shadow: 0 0 0 3px rgba(30,64,175,0.07) !important;
  }

  /* Toolbars de gráficos e tabelas — sempre visíveis */
  [data-testid="stElementToolbar"] {
    opacity: 1 !important;
  }

  /* Tabelas */
  [data-testid="stDataFrame"] {
    border: 1px solid rgba(15,23,42,0.08) !important;
    border-radius: 6px !important;
    overflow: visible !important;
  }

  /* Tabs */
  [data-testid="stTabs"] [role="tab"] {
    font-size: 0.8rem !important; font-weight: 500 !important; color: #64748b !important;
  }
  [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #1e40af !important; border-bottom-color: #1e40af !important;
  }
</style>
""", unsafe_allow_html=True)

# --- Helpers ---
def fmt_brl(valor):
    try:
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def fmt_cnpj(cnpj):
    digits = re.sub(r"\D", "", str(cnpj))
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    return cnpj

# --- Conexão BigQuery ---
@st.cache_resource
def get_client():
    from google.cloud import bigquery  # import único, deferido
    try:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return bigquery.Client(project=PROJECT, credentials=creds)
    except Exception:
        try:
            sa_paths = (
                _glob.glob(r"C:/Users/p0134255/Documents/*/Backup/Tj/Downloads/service_account.json") +
                _glob.glob("/home/*/bigquery-sa.json") +
                _glob.glob("/home/*/Downloads/service_account.json")
            )
            if sa_paths:
                creds = service_account.Credentials.from_service_account_file(sa_paths[0], scopes=SCOPES)
                return bigquery.Client(project=PROJECT, credentials=creds)
        except Exception:
            pass
        return None

@st.cache_data(ttl=300)
def query(_client, sql):
    return _client.query(sql).to_dataframe()

# --- Mock para demonstração local ---
def gerar_dados_mock():
    random.seed(42)
    fornecedores = [
        "PAPELARIA BRASIL LTDA", "DISTRIBUIDORA MEGA SUPRIMENTOS",
        "COMERCIAL ESCRITÓRIO S.A.", "LIMPEZA TOTAL EIRELI",
        "TECH OFFICE MATERIAIS", "SUPRIMENTOS JUDICIAIS LTDA"
    ]
    unidades = [
        "1ª VARA CÍVEL", "2ª VARA CRIMINAL", "COPAT",
        "SECRETARIA GERAL", "DIRETORIA ADMINISTRATIVA", "GABINETE DA PRESIDÊNCIA"
    ]
    catmas_desc = {
        "3920": "PAPEL SULFITE A4 75G", "4512": "TONER HP LASERJET",
        "2201": "DETERGENTE LÍQUIDO 500ML", "6601": "GRAMPO GALVANIZADO 26/6",
        "1102": "CANETA ESFEROGRÁFICA AZUL", "8801": "PASTA ARQUIVO L",
        "3301": "ENVELOPE KRAFT 26X36", "5501": "FITA ADESIVA TRANSPARENTE",
        "7701": "ÁLCOOL GEL 70% 500ML", "4401": "CAIXA ARQUIVO MORTO"
    }
    base = date(2024, 1, 1)
    notas, itens = [], []
    for i in range(1, 141):
        nf = f"NF{i:05d}"
        cnpj_raw = f"{random.randint(10,99)}{random.randint(100,999)}{random.randint(100,999)}{random.randint(1000,9999)}{random.randint(10,99)}"
        notas.append({
            "NF": nf, "Serie": str(random.randint(1, 3)),
            "Valor_Nota": round(random.uniform(300, 18000), 2),
            "CNPJ_Origem": cnpj_raw, "Fornecedor": random.choice(fornecedores),
            "Unidade_Administrativa": random.choice(unidades),
            "Data_Contabilizacao": base + timedelta(days=random.randint(0, 449))
        })
        for catmas, desc in list(catmas_desc.items())[:random.randint(2, 5)]:
            itens.append({
                "NF": nf, "Serie": notas[-1]["Serie"],
                "CATMAS": catmas, "Descricao": desc,
                "Quantidade": random.randint(5, 500)
            })
    return pd.DataFrame(notas), pd.DataFrame(itens)

# --- Header ---
st.markdown("""
<div class="inst-header">
  <span class="inst-badge"><span style="font-size:1.1rem;vertical-align:middle;">𐄷</span> TJMG · COPAT</span>
  <div>
    <p class="inst-title">Controle de Estoque — Entrada de Notas Fiscais</p>
    <p class="inst-sub">Coordenação de Controle do Patrimônio Mobiliário · Tribunal de Justiça de Minas Gerais</p>
  </div>
</div>
""", unsafe_allow_html=True)

# --- Carrega dados ---
# DEMO_MODE: ativar via variável de ambiente para testes controlados.
# Nunca deve ser True em produção. Ex: set DASHBOARD_DEMO=true
DEMO_MODE = os.getenv("DASHBOARD_DEMO", "false").lower() == "true"

client = get_client()
modo_demo = False
_bq_erro = None

with st.spinner("Consultando BigQuery..."):
    if DEMO_MODE:
        df_notas, df_itens = gerar_dados_mock()
        modo_demo = True
    else:
        try:
            if client is None:
                raise ConnectionError("Credenciais não encontradas")
            df_notas = query(client, f"""
                SELECT NF, Serie, Valor_Nota, CNPJ_Origem, Nome_Empresarial,
                       Unidade_Administrativa, Data_Contabilizacao
                FROM `{PROJECT}.{DATASET}.entradas_notas_fiscais`
                ORDER BY Data_Contabilizacao DESC
            """)
            df_itens = query(client, f"""
                SELECT NF, Serie, CATMAS, Descricao, Quantidade
                FROM `{PROJECT}.{DATASET}.itens_das_notas`
            """)
            df_notas = df_notas.rename(columns={"Nome_Empresarial": "Fornecedor"})
        except Exception as e:
            _bq_erro = str(e)
            df_notas, df_itens = gerar_dados_mock()
            modo_demo = True

# --- Normaliza tipos ---
if "Valor_Nota" in df_notas.columns:
    df_notas["Valor_Nota"] = (
        df_notas["Valor_Nota"]
        .astype(str)
        .str.replace(r"R\$\s*", "", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
if "Data_Contabilizacao" in df_notas.columns:
    df_notas["Data_Contabilizacao"] = pd.to_datetime(
        df_notas["Data_Contabilizacao"], errors="coerce"
    ).dt.date
if "Quantidade" in df_itens.columns:
    df_itens["Quantidade"] = pd.to_numeric(df_itens["Quantidade"], errors="coerce").astype("Int64")

if modo_demo:
    if _bq_erro:
        st.warning(
            f"**ATENÇÃO — dados sintéticos.** BigQuery indisponível: `{_bq_erro}`. "
            "Os valores exibidos são fictícios e não refletem a realidade.",
            icon="⚠️"
        )
    else:
        st.info("Modo demonstração ativado via DASHBOARD_DEMO=true — dados sintéticos.", icon="ℹ")

# --- Sidebar: filtros ---
with st.sidebar:
    st.markdown("### Filtros")

    if "Data_Contabilizacao" in df_notas.columns:
        datas = pd.to_datetime(df_notas["Data_Contabilizacao"].dropna())
        if not datas.empty:
            dmin, dmax = datas.min().date(), datas.max().date()
            periodo = st.date_input("Período", value=(dmin, dmax), min_value=dmin, max_value=dmax)
            if len(periodo) == 2:
                df_notas = df_notas[
                    (pd.to_datetime(df_notas["Data_Contabilizacao"]) >= pd.Timestamp(periodo[0])) &
                    (pd.to_datetime(df_notas["Data_Contabilizacao"]) <= pd.Timestamp(periodo[1]))
                ]

    if "Unidade_Administrativa" in df_notas.columns:
        unidades = ["Todas"] + sorted(df_notas["Unidade_Administrativa"].dropna().unique().tolist())
        unidade_sel = st.selectbox("Unidade Administrativa", unidades)
        if unidade_sel != "Todas":
            df_notas = df_notas[df_notas["Unidade_Administrativa"] == unidade_sel]

    if "Fornecedor" in df_notas.columns:
        fornecedores = ["Todos"] + sorted(df_notas["Fornecedor"].dropna().unique().tolist())
        forn_sel = st.selectbox("Fornecedor", fornecedores)
        if forn_sel != "Todos":
            df_notas = df_notas[df_notas["Fornecedor"] == forn_sel]

    st.divider()
    st.markdown(
        f"<p style='font-size:0.68rem;color:#94a3b8;'>"
        f"{'Demo · Dados sintéticos' if modo_demo else 'BigQuery · TTL 5 min'}</p>",
        unsafe_allow_html=True
    )

# --- Filtra itens ---
nfs_filtradas = set(df_notas["NF"].astype(str).tolist()) if "NF" in df_notas.columns else set()
df_itens_f = df_itens[df_itens["NF"].astype(str).isin(nfs_filtradas)] if nfs_filtradas else df_itens

# --- KPIs ---
total_nfs    = df_notas["NF"].nunique() if "NF" in df_notas.columns else 0
valor_total  = df_notas["Valor_Nota"].sum() if "Valor_Nota" in df_notas.columns else 0
total_itens  = int(df_itens_f["Quantidade"].sum()) if "Quantidade" in df_itens_f.columns else 0
total_catmas = df_itens_f["CATMAS"].nunique() if "CATMAS" in df_itens_f.columns else 0

st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-label">Notas Fiscais</div>
    <div class="kpi-value">{total_nfs:,}</div>
    <div class="kpi-sub">no período selecionado</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Valor Total</div>
    <div class="kpi-value mono">{fmt_brl(valor_total)}</div>
    <div class="kpi-sub">soma das NFs filtradas</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Itens Recebidos</div>
    <div class="kpi-value">{total_itens:,}</div>
    <div class="kpi-sub">unidades no período</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Categorias CATMAS</div>
    <div class="kpi-value">{total_catmas}</div>
    <div class="kpi-sub">tipos de material</div>
  </div>
</div>
""", unsafe_allow_html=True)

# --- Gráficos ---
st.markdown('<div class="sec-title">Análise por Período e Categoria</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2, gap="medium")

with col1:
    if "Data_Contabilizacao" in df_notas.columns:
        df_temp = df_notas.dropna(subset=["Data_Contabilizacao"]).copy()
        df_temp["Mês"] = pd.to_datetime(df_temp["Data_Contabilizacao"]).dt.to_period("M").astype(str)
        by_mes = df_temp.groupby("Mês").agg(NFs=("NF", "nunique")).reset_index()
        st.markdown('<p class="chart-label">Entradas por Mês — Nº de NFs</p>', unsafe_allow_html=True)
        st.bar_chart(by_mes.set_index("Mês")["NFs"], height=260, width='stretch')
    else:
        st.info("Sem dados de data disponíveis.")

with col2:
    if "CATMAS" in df_itens_f.columns and "Quantidade" in df_itens_f.columns:
        top_cat = (
            df_itens_f.groupby("CATMAS")["Quantidade"]
            .sum().sort_values(ascending=False).head(10).reset_index()
        )
        st.markdown('<p class="chart-label">Top 10 CATMAS — Quantidade Recebida</p>', unsafe_allow_html=True)
        st.bar_chart(top_cat.set_index("CATMAS")["Quantidade"], height=260, width='stretch')
    else:
        st.info("Sem dados de CATMAS.")

st.divider()

# --- Tabelas ---
st.markdown('<div class="sec-title">Detalhamento</div>', unsafe_allow_html=True)
tab1, tab2 = st.tabs(["Notas Fiscais", "Itens / Materiais"])

with tab1:
    df_exib = df_notas.copy()
    if "CNPJ_Origem" in df_exib.columns:
        df_exib["CNPJ_Origem"] = df_exib["CNPJ_Origem"].apply(fmt_cnpj)
    if "Valor_Nota" in df_exib.columns:
        df_exib["Valor_Nota"] = df_exib["Valor_Nota"].apply(
            lambda v: fmt_brl(v) if pd.notna(v) else ""
        )
    col_order = [c for c in ["NF","Serie","Data_Contabilizacao","Fornecedor",
                               "CNPJ_Origem","Valor_Nota","Unidade_Administrativa"]
                  if c in df_exib.columns]
    df_sorted = (df_exib[col_order].sort_values("Data_Contabilizacao", ascending=False)
                 if "Data_Contabilizacao" in df_exib.columns else df_exib[col_order])
    st.dataframe(
        df_sorted, width='stretch', height=420, hide_index=True,
        column_config={
            "NF":                     st.column_config.TextColumn("Nº NF",       width="small"),
            "Serie":                  st.column_config.TextColumn("Série",        width="small"),
            "Data_Contabilizacao":    st.column_config.DateColumn("Data",         format="DD/MM/YYYY", width="small"),
            "Fornecedor":             st.column_config.TextColumn("Fornecedor",   width="large"),
            "CNPJ_Origem":            st.column_config.TextColumn("CNPJ",         width="medium"),
            "Valor_Nota":             st.column_config.TextColumn("Valor (R$)",   width="medium"),
            "Unidade_Administrativa": st.column_config.TextColumn("Unidade",      width="medium"),
        }
    )

with tab2:
    busca = st.text_input(
        "Buscar material",
        placeholder="Ex: papel sulfite, toner, álcool gel...",
        label_visibility="collapsed"
    )
    df_show = df_itens_f
    if busca and "Descricao" in df_itens_f.columns:
        df_show = df_itens_f[df_itens_f["Descricao"].str.contains(busca, case=False, na=False)]
    st.dataframe(
        df_show, width='stretch', height=420, hide_index=True,
        column_config={
            "NF":        st.column_config.TextColumn("Nº NF",               width="small"),
            "Serie":     st.column_config.TextColumn("Série",               width="small"),
            "CATMAS":    st.column_config.TextColumn("Cód. CATMAS",         width="small"),
            "Descricao": st.column_config.TextColumn("Descrição do Material", width="large"),
            "Quantidade":st.column_config.NumberColumn("Qtd.",              format="%d", width="small"),
        }
    )
