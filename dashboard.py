import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import glob as _glob
import re
import json

# --- Configuração ---
PROJECT = "secret-air-472101-f1"
DATASET = "Controle_de_estoque"
SCOPES  = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive.readonly"
]

st.set_page_config(page_title="Estoque - Entrada de Notas Fiscais / COPAT", page_icon="📦", layout="wide")
st.title("Estoque - Entrada de Notas Fiscais / COPAT")

# --- Helpers de formatação ---
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
    # Nuvem: credenciais via st.secrets
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # Local: lê o arquivo JSON da máquina
        sa_path = _glob.glob(r"C:/Users/p0134255/Documents/*/Backup/Tj/Downloads/service_account.json")[0]
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)

@st.cache_data(ttl=300)
def query(_client, sql):
    return _client.query(sql).to_dataframe()

try:
    client = get_client()
except Exception as e:
    st.error(f"Erro ao conectar no BigQuery: {e}")
    st.stop()

# --- Carrega dados ---
with st.spinner("Carregando dados do BigQuery..."):
    try:
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
    except Exception as e:
        st.error(f"Erro ao consultar tabelas: {e}")
        st.stop()

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

# --- Renomeia colunas para exibição ---
df_notas = df_notas.rename(columns={"Nome_Empresarial": "Fornecedor"})

# --- Filtros sidebar ---
with st.sidebar:
    st.header("Filtros")

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
        unidade_sel = st.selectbox("Unidade", unidades)
        if unidade_sel != "Todas":
            df_notas = df_notas[df_notas["Unidade_Administrativa"] == unidade_sel]

    if "Fornecedor" in df_notas.columns:
        fornecedores = ["Todos"] + sorted(df_notas["Fornecedor"].dropna().unique().tolist())
        forn_sel = st.selectbox("Fornecedor", fornecedores)
        if forn_sel != "Todos":
            df_notas = df_notas[df_notas["Fornecedor"] == forn_sel]

# --- Filtra itens pelas NFs filtradas ---
nfs_filtradas = set(df_notas["NF"].astype(str).tolist()) if "NF" in df_notas.columns else set()
df_itens_f = df_itens[df_itens["NF"].astype(str).isin(nfs_filtradas)] if nfs_filtradas else df_itens

# --- KPIs ---
st.subheader("Visão Geral")
k1, k2, k3, k4 = st.columns(4)

total_nfs    = df_notas["NF"].nunique() if "NF" in df_notas.columns else 0
valor_total  = df_notas["Valor_Nota"].sum() if "Valor_Nota" in df_notas.columns else 0
total_itens  = int(df_itens_f["Quantidade"].sum()) if "Quantidade" in df_itens_f.columns else 0
total_catmas = df_itens_f["CATMAS"].nunique() if "CATMAS" in df_itens_f.columns else 0

k1.metric("Notas Fiscais", total_nfs)
k2.metric("Valor Total", fmt_brl(valor_total))
k3.metric("Itens Recebidos", total_itens)
k4.metric("Categorias (CATMAS)", total_catmas)

st.divider()

# --- Gráficos ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Entradas por Período")
    if "Data_Contabilizacao" in df_notas.columns:
        df_temp = df_notas.dropna(subset=["Data_Contabilizacao"]).copy()
        df_temp["Mes"] = pd.to_datetime(df_temp["Data_Contabilizacao"]).dt.to_period("M").astype(str)
        by_mes = df_temp.groupby("Mes").agg(NFs=("NF", "nunique")).reset_index()
        st.bar_chart(by_mes.set_index("Mes")["NFs"], height=300)
        st.caption("Número de NFs por mês")
    else:
        st.info("Sem dados de data disponíveis.")

with col2:
    st.subheader("Itens por CATMAS (top 10)")
    if "CATMAS" in df_itens_f.columns and "Quantidade" in df_itens_f.columns:
        top_cat = (
            df_itens_f.groupby("CATMAS")["Quantidade"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        st.bar_chart(top_cat.set_index("CATMAS")["Quantidade"], height=300)
    else:
        st.info("Sem dados de CATMAS.")

st.divider()

# --- Tabelas detalhadas ---
tab1, tab2 = st.tabs(["Notas Fiscais", "Itens / Materiais"])

with tab1:
    df_exib = df_notas.copy()
    if "CNPJ_Origem" in df_exib.columns:
        df_exib["CNPJ_Origem"] = df_exib["CNPJ_Origem"].apply(fmt_cnpj)
    if "Valor_Nota" in df_exib.columns:
        df_exib["Valor_Nota"] = df_exib["Valor_Nota"].apply(
            lambda v: fmt_brl(v) if pd.notna(v) else ""
        )
    st.dataframe(
        df_exib.sort_values("Data_Contabilizacao", ascending=False)
        if "Data_Contabilizacao" in df_exib.columns else df_exib,
        use_container_width=True,
        height=400
    )

with tab2:
    busca = st.text_input("Buscar por descrição", placeholder="Ex: papel, toner, limpeza...")
    df_show = df_itens_f
    if busca and "Descricao" in df_itens_f.columns:
        df_show = df_itens_f[df_itens_f["Descricao"].str.contains(busca, case=False, na=False)]
    st.dataframe(
        df_show,
        use_container_width=True,
        height=400,
        column_config={
            "Quantidade": st.column_config.NumberColumn(
                "Quantidade", format="%d", width="small"
            )
        }
    )
