"""
dashboard/app.py
🏅 Data Warehouse Medallion — Dashboard do Gold Layer
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import subprocess

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

DB_PATH = os.path.join(_ROOT, "data", "warehouse", "medallion.duckdb")
DBT_DIR = os.path.join(_ROOT, "dbt_project")
PYTHON  = sys.executable
IS_WIN  = sys.platform == "win32"

def _venv_bin(name):
    exe  = name + (".exe" if IS_WIN else "")
    full = os.path.join(os.path.dirname(PYTHON), exe)
    return full if os.path.exists(full) else name

DBT = _venv_bin("dbt")

st.set_page_config(
    page_title="Medallion DW · Gold Layer",
    page_icon="🏅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS ─────────────────────────────────────────────────────────
# FIX 1: Usa variáveis CSS explícitas para forçar cor do texto
# independente do tema claro/escuro do Streamlit
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Source+Sans+3:wght@300;400;500;600&display=swap');

* { font-family: 'Source Sans 3', sans-serif; }

/* Fundo geral */
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main,
.block-container {
    background-color: #faf7f2 !important;
    color: #2c1810 !important;
}

/* Força texto visível em todos os elementos Streamlit */
[data-testid="stAppViewContainer"] p,
[data-testid="stAppViewContainer"] span,
[data-testid="stAppViewContainer"] div,
[data-testid="stAppViewContainer"] label {
    color: #2c1810;
}

/* Sidebar */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div {
    background-color: #1e1208 !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] label {
    color: #d4b896 !important;
}

/* Header */
.main-header {
    background: linear-gradient(135deg, #1e1208 0%, #2d1e0f 50%, #1a1008 100%);
    border-radius: 12px;
    padding: 2.4rem 3rem;
    margin-bottom: 1.8rem;
    position: relative;
    overflow: hidden;
}
.main-header h1 {
    font-family: 'Playfair Display', serif;
    font-size: 2.4rem; color: #e8d5b5 !important; margin: 0;
}
.main-header h1 span { color: #c9973a !important; }
.main-header p { color: #8a7260 !important; font-size: 0.9rem; margin: 0.4rem 0 0; }

/* Layer badges */
.layer-badge {
    display: inline-flex; align-items: center; gap: 0.4rem;
    padding: 0.25rem 0.8rem; border-radius: 20px;
    font-size: 0.72rem; font-weight: 600; letter-spacing: 0.1em;
    text-transform: uppercase; margin: 0.15rem;
}
.badge-bronze { background: rgba(176,124,71,0.15); color: #b07c47 !important; border: 1px solid rgba(176,124,71,0.3); }
.badge-silver { background: rgba(140,153,166,0.15); color: #8c99a6 !important; border: 1px solid rgba(140,153,166,0.3); }
.badge-gold   { background: rgba(201,151,58,0.15);  color: #c9973a !important; border: 1px solid rgba(201,151,58,0.3); }

/* KPI cards */
.kpi-card {
    background: #ffffff;
    border: 1px solid #e8e0d5;
    border-top: 3px solid var(--accent, #c9973a);
    border-radius: 8px;
    padding: 1.2rem 1.4rem;
    transition: box-shadow 0.2s;
}
.kpi-card:hover { box-shadow: 0 4px 20px rgba(44,24,16,0.08); }
.kpi-val { font-family: 'Playfair Display', serif; font-size: 2rem; color: var(--accent, #c9973a) !important; margin: 0.2rem 0; }
.kpi-lbl { font-size: 0.72rem; color: #8a7260 !important; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 600; }
.kpi-sub { font-size: 0.8rem; color: #b8a898 !important; margin-top: 0.2rem; }

/* Section titles */
.sec-title {
    font-family: 'Playfair Display', serif;
    font-size: 1.05rem; color: #2c1810 !important;
    border-bottom: 2px solid #e8d5b5;
    padding-bottom: 0.4rem; margin: 1.4rem 0 0.8rem;
}

/* Pipeline cards */
.pipeline-card {
    background: #fff;
    border: 1px solid #e8e0d5;
    border-radius: 8px;
    padding: 1rem 1.2rem;
    margin: 0.3rem 0;
    display: flex; align-items: flex-start; gap: 0.8rem;
}
.pipeline-card .layer-dot {
    width: 10px; height: 10px; border-radius: 50%;
    margin-top: 0.3rem; flex-shrink: 0;
}
.dot-bronze { background: #b07c47; }
.dot-silver { background: #8c99a6; }
.dot-gold   { background: #c9973a; }
.pipeline-card .pc-title { font-weight: 600; font-size: 0.88rem; color: #2c1810 !important; }
.pipeline-card .pc-desc  { font-size: 0.82rem; color: #8a7260 !important; line-height: 1.5; margin-top: 0.15rem; }

/* Botão */
.stButton > button {
    background: #c9973a !important; color: #fff !important;
    border: none !important; border-radius: 6px !important;
    font-weight: 600 !important; font-size: 0.88rem !important;
    padding: 0.55rem 1.5rem !important; width: 100% !important;
}
.stButton > button:hover { background: #a87a2e !important; }

/* Tabs */
div[data-testid="stTabs"] button { color: #8a7260 !important; font-weight: 500 !important; }
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #c9973a !important; border-bottom-color: #c9973a !important;
}

/* Radio buttons na sidebar */
[data-testid="stSidebar"] .stRadio label { color: #d4b896 !important; }
[data-testid="stSidebar"] .stRadio div  { color: #d4b896 !important; }

.stDataFrame { border: 1px solid #e8e0d5 !important; border-radius: 8px !important; }
</style>
""", unsafe_allow_html=True)

# ─── Cores Plotly ─────────────────────────────────────────────────
PL = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(250,247,242,0.8)",
    font=dict(family="Source Sans 3", color="#5c4030", size=12),
    xaxis=dict(gridcolor="#e8e0d5", linecolor="#e8e0d5", tickfont=dict(color="#8a7260")),
    yaxis=dict(gridcolor="#e8e0d5", linecolor="#e8e0d5", tickfont=dict(color="#8a7260")),
    margin=dict(t=36, b=36, l=36, r=16),
)
GOLD   = "#c9973a"
AMBER  = "#e8b84b"
BROWN  = "#7c4a2d"
SAGE   = "#7a9e7e"
SLATE  = "#8c99a6"
RUST   = "#c0533a"
COLORS = [GOLD, BROWN, SAGE, SLATE, RUST, AMBER, "#9b6b3a", "#5c7a6e"]

def fmt_brl(v): return f"R$ {v:,.0f}".replace(",", ".")
def fmt_k(v):   return f"{v/1000:.1f}k" if v >= 1000 else str(int(v))


# ─── DuckDB helpers ───────────────────────────────────────────────
@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    # dbt-duckdb cria schemas com prefixo main_ (main_gold, main_silver, main_bronze)
    # Normaliza o SQL para usar os schemas corretos
    sql = (sql
        .replace("FROM gold.",   "FROM main_gold.")
        .replace("FROM silver.", "FROM main_silver.")
        .replace("FROM bronze.", "FROM main_bronze.")
        .replace("JOIN gold.",   "JOIN main_gold.")
        .replace("JOIN silver.", "JOIN main_silver.")
        .replace("JOIN bronze.", "JOIN main_bronze.")
    )
    for readonly in (True, False):
        try:
            conn = duckdb.connect(DB_PATH, read_only=readonly)
            df = conn.execute(sql).df()
            conn.close()
            return df
        except Exception as e:
            last_err = e
            continue
    raise last_err

def db_exists() -> bool:
    """
    Verifica se o gold layer existe.
    dbt-duckdb cria schemas como 'main_gold', 'main_silver', 'main_bronze'.
    Tenta read_only primeiro; se falhar por lock (Windows), tenta sem read_only.
    """
    if not os.path.exists(DB_PATH):
        return False
    for readonly in (True, False):
        try:
            conn = duckdb.connect(DB_PATH, read_only=readonly)
            # dbt-duckdb usa prefixo 'main_' nos schemas
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema IN ('gold', 'main_gold')"
            ).fetchall()
            conn.close()
            names = [r[0] for r in tables]
            return "gld_saude_por_uf" in names
        except Exception:
            continue
    return False


# ─── Pipeline runner ─────────────────────────────────────────────
# O dbt roda via subprocess com cwd=DBT_DIR — executado pelo run.py
# No dashboard apenas recarregamos o cache após ingestão + bronze.
def executar_pipeline():
    """Roda ingestão + bronze + dbt run como subprocesso do Python do venv."""
    from ingestion.ingest import run_ingestion
    from ingestion.load_to_duckdb import load_raw_to_bronze

    placeholder = st.empty()

    with placeholder.status("Executando pipeline...", expanded=True) as status:
        st.write("📥 Step 1/3 — Ingestão IBGE + DATASUS...")
        run_ingestion()

        st.write("🟤 Step 2/3 — Carregando Bronze layer...")
        load_raw_to_bronze()

        st.write("⚙️  Step 3/3 — dbt run...")
        dw_norm = DB_PATH.replace("\\", "/")
        dbt_env = {**os.environ, "DW_PATH": dw_norm}

        # Roda dbt como subprocesso — captura stdout+stderr juntos para debug
        result = subprocess.run(
            [DBT, "run", "--profiles-dir", ".", "--project-dir", "."],
            cwd=DBT_DIR,
            env=dbt_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        if result.returncode != 0:
            status.update(label="❌ dbt run falhou", state="error")
            st.code(result.stdout or "(sem output)")
            st.error(f"dbt retornou código {result.returncode}. "
                     f"Verifique se `dbt deps` foi executado em dbt_project/.")
            return False

        st.write("✅ Todos os modelos gold criados.")
        status.update(label="✅ Pipeline concluído!", state="complete")

    st.cache_data.clear()
    st.rerun()


# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 1.5rem; border-bottom: 1px solid #3a2210;'>
        <div style='font-family: "Playfair Display", serif; font-size: 1.3rem; color: #c9973a;'>
            🏅 Medallion DW
        </div>
        <div style='font-size: 0.72rem; color: #9a7050; margin-top: 0.3rem; line-height: 1.6;'>
            dbt · DuckDB · Airflow<br>IBGE + DATASUS · 2019–2023
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # FIX 3: label não-vazio + label_visibility="collapsed"
    page = st.radio(
        "Navegação",
        ["🏅 Gold Layer", "🗺️ Por UF", "🦠 Diagnósticos", "📈 Temporal", "⚙️ Arquitetura"],
        label_visibility="collapsed",
    )

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("▶ Executar Pipeline"):
        executar_pipeline()

    # Botão de recarga manual caso o pipeline já tenha rodado pelo run.py
    if st.button("🔄 Recarregar Dashboard"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='margin-top:2rem; padding-top:1rem; border-top:1px solid #3a2210;
         font-size:0.72rem; color:#6a5040; line-height:2.2;'>
        <span style='color:#b07c47;'>●</span> Bronze — views brutas<br>
        <span style='color:#8c99a6;'>●</span> Silver — limpas + enriquecidas<br>
        <span style='color:#c9973a;'>●</span> Gold — analytics-ready<br>
        <span style='color:#3a2210;'>──────────────</span><br>
        dbt-duckdb · Airflow<br>
        GitHub Actions CI<br>
        Streamlit · Plotly
    </div>
    """, unsafe_allow_html=True)


# ─── Tela de bootstrap ───────────────────────────────────────────
if not db_exists():
    db_file_exists = os.path.exists(DB_PATH)

    # Diagnóstico: tenta conectar e listar tabelas gold independente do lock
    gold_tables = []
    connect_err = ""
    if db_file_exists:
        for readonly in (True, False):
            try:
                conn = duckdb.connect(DB_PATH, read_only=readonly)
                rows = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema IN ('gold', 'main_gold')"
                ).fetchall()
                gold_tables = [r[0] for r in rows]
                conn.close()
                break
            except Exception as e:
                connect_err = str(e)

    gold_found = "gld_saude_por_uf" in gold_tables

    # Se o arquivo existe e a tabela gold existe, é só um problema de conexão
    # (ex: arquivo aberto por outro processo) — mostra os dados mesmo assim
    if db_file_exists and gold_found:
        st.warning(
            "⚠️ Warehouse encontrado mas não foi possível abrir em modo leitura exclusiva. "
            "Feche o terminal que está rodando `run.py` e clique em **🔄 Recarregar Dashboard**."
        )
        st.stop()

    st.markdown("""
    <div style='background:#fff8ee; border:1px solid #e8d5b5; border-left:4px solid #c9973a;
         border-radius:8px; padding:2rem 2.5rem; margin-top:2rem;'>
        <div style='font-family:"Playfair Display",serif; font-size:1.4rem; color:#2c1810; margin-bottom:0.5rem;'>
            🏅 Gold layer não encontrado
        </div>
        <p style='color:#6a5040; margin:0 0 1rem;'>
            O warehouse precisa ser construído antes de usar o dashboard.
        </p>
    </div>
    """, unsafe_allow_html=True)

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        st.metric("Arquivo .duckdb existe?", "✅ Sim" if db_file_exists else "❌ Não")
    with col_d2:
        st.metric("Tabelas gold encontradas", str(len(gold_tables)))

    if connect_err:
        st.code(f"Erro de conexão: {connect_err}")

    if gold_tables:
        st.write("Tabelas encontradas:", gold_tables)

    st.info("""
**Execute o pipeline pelo terminal:**
```
cd dbt_project
dbt deps
cd ..
python run.py --only-pipeline
```
Depois clique em **🔄 Recarregar Dashboard** na barra lateral.
    """)
    st.stop()


# ════════════════════════════════════════════════════════════════
# PAGE: GOLD LAYER
# ════════════════════════════════════════════════════════════════
if page == "🏅 Gold Layer":

    st.markdown("""
    <div class="main-header">
        <h1>🏅 Data Warehouse <span>Medallion</span></h1>
        <p>Gold Layer · IBGE + DATASUS · Arquitetura Bronze → Silver → Gold · dbt + DuckDB</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    for col, (accent, label, css_class, tbl, schema) in zip(
        [col1, col2, col3],
        [
            ("#b07c47", "ibge_estados",    "badge-bronze", "main_bronze.brz_ibge_estados", "bronze"),
            ("#8c99a6", "slv_internacoes", "badge-silver", "main_silver.slv_internacoes", "silver"),
            ("#c9973a", "gld_saude_por_uf","badge-gold",   "main_gold.gld_saude_por_uf",  "gold"),
        ],
    ):
        with col:
            layer = schema.capitalize()
            st.markdown(f'<span class="layer-badge {css_class}">● {layer}</span>', unsafe_allow_html=True)
            try:
                n = query(f"SELECT COUNT(*) AS n FROM {tbl}")["n"][0]
                st.markdown(f"""
                <div class="kpi-card" style="--accent:{accent};">
                    <div class="kpi-lbl">{label}</div>
                    <div class="kpi-val">{n:,}</div>
                    <div class="kpi-sub">linhas · camada {layer}</div>
                </div>""", unsafe_allow_html=True)
            except Exception:
                st.warning(f"{schema} ainda não carregado")

    st.markdown("")

    gold = query("SELECT * FROM gold.gld_saude_por_uf WHERE ano = 2023")
    kpis = [
        ("#c9973a", fmt_k(gold["total_internacoes"].sum()),      "Internações 2023",   "Total Brasil"),
        ("#c9973a", fmt_brl(gold["custo_total"].sum()),           "Custo Total SUS",    "2023"),
        ("#c0533a", f"{gold['taxa_obito_pct'].mean():.2f}%",      "Taxa Óbito Média",   "por UF"),
        ("#7a9e7e", f"{gold['dias_medio_internacao'].mean():.1f} d", "Permanência Média","dias"),
        ("#8c99a6", str(gold["sigla_uf"].nunique()),               "UFs com dados",      "2023"),
    ]
    cols = st.columns(5)
    for col, (accent, val, lbl, sub) in zip(cols, kpis):
        with col:
            st.markdown(f"""
            <div class="kpi-card" style="--accent:{accent};">
                <div class="kpi-lbl">{lbl}</div>
                <div class="kpi-val" style="color:{accent};">{val}</div>
                <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown('<div class="sec-title">Custo Total por Região — 2023</div>', unsafe_allow_html=True)
        reg = gold.groupby("regiao").agg(custo=("custo_total","sum"), n=("total_internacoes","sum")).reset_index()
        fig = px.treemap(reg, path=["regiao"], values="custo", color="n",
                         color_continuous_scale=[[0,"#f5ede0"],[1,GOLD]])
        fig.update_traces(textinfo="label+percent entry", textfont_size=13)
        fig.update_layout(**{k:v for k,v in PL.items() if k != 'margin'}, height=340, coloraxis_showscale=False, margin=dict(t=10,b=10,l=10,r=10))
        st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown('<div class="sec-title">IDH × PIB per Capita — UFs 2023</div>', unsafe_allow_html=True)
        fig2 = px.scatter(gold, x="pib_per_capita", y="idh",
                          color="regiao", size="populacao", text="sigla_uf",
                          color_discrete_sequence=COLORS, size_max=30, opacity=0.8)
        fig2.update_traces(textposition="top center", textfont_size=9)
        fig2.update_layout(**PL, height=340, legend=dict(bgcolor="rgba(0,0,0,0)"))
        fig2.update_xaxes(tickformat=",.0f", tickprefix="R$ ")
        st.plotly_chart(fig2, width='stretch')

    st.markdown('<div class="sec-title">Ranking de Performance em Saúde — 2023 (menor = melhor)</div>', unsafe_allow_html=True)
    rank = gold.sort_values("score_performance").reset_index(drop=True)
    rank["posicao"] = rank.index + 1
    fig3 = px.bar(rank, x="sigla_uf", y="score_performance", color="regiao",
                  text="posicao", color_discrete_sequence=COLORS)
    fig3.update_traces(textposition="outside", textfont_size=9)
    fig3.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig3, width='stretch')


# ════════════════════════════════════════════════════════════════
# PAGE: POR UF
# ════════════════════════════════════════════════════════════════
elif page == "🗺️ Por UF":

    st.markdown("""
    <div class="main-header">
        <h1>🗺️ Análise por <span>Unidade Federativa</span></h1>
        <p>gold.gld_saude_por_uf · Indicadores socioeconômicos e de saúde · 2019–2023</p>
    </div>
    """, unsafe_allow_html=True)

    gold_all = query("SELECT * FROM gold.gld_saude_por_uf ORDER BY sigla_uf, ano")
    anos     = sorted(gold_all["ano"].unique())
    ano_sel  = st.select_slider("Ano de referência", options=anos, value=max(anos))
    gold     = gold_all[gold_all["ano"] == ano_sel]

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec-title">Taxa de Internação por 1.000 hab.</div>', unsafe_allow_html=True)
        uf_ord = gold.sort_values("tx_internacao_1k_hab", ascending=False)
        fig = go.Figure(go.Bar(
            x=uf_ord["sigla_uf"], y=uf_ord["tx_internacao_1k_hab"],
            marker=dict(color=uf_ord["tx_internacao_1k_hab"],
                        colorscale=[[0,"#f5ede0"],[0.5,AMBER],[1,RUST]], showscale=False),
            text=uf_ord["tx_internacao_1k_hab"].round(2), textposition="outside", textfont_size=9,
        ))
        fig.update_layout(**PL, height=300, yaxis_title="por 1k hab.")
        st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown('<div class="sec-title">% Internações por Urgência</div>', unsafe_allow_html=True)
        uf_urg = gold.sort_values("pct_urgencia", ascending=False)
        fig2 = go.Figure(go.Bar(
            x=uf_urg["sigla_uf"], y=uf_urg["pct_urgencia"],
            marker=dict(color=uf_urg["pct_urgencia"],
                        colorscale=[[0,"#f5ede0"],[1,BROWN]], showscale=False),
            text=uf_urg["pct_urgencia"].round(1).astype(str) + "%",
            textposition="outside", textfont_size=9,
        ))
        fig2.update_layout(**PL, height=300, yaxis_title="%")
        st.plotly_chart(fig2, width='stretch')

    st.markdown('<div class="sec-title">Evolução de Internações por Região (2019–2023)</div>', unsafe_allow_html=True)
    reg_ev = gold_all.groupby(["ano","regiao"])["total_internacoes"].sum().reset_index()
    fig3 = px.line(reg_ev, x="ano", y="total_internacoes", color="regiao",
                   color_discrete_sequence=COLORS, markers=True)
    fig3.update_traces(line_width=2.5, marker_size=6)
    fig3.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig3, width='stretch')

    st.markdown('<div class="sec-title">Tabela Completa — Gold Layer</div>', unsafe_allow_html=True)
    show = gold[["sigla_uf","nome_uf","regiao","nivel_idh","total_internacoes",
                 "taxa_obito_pct","dias_medio_internacao","custo_medio_aih",
                 "tx_internacao_1k_hab","rank_eficiencia","score_performance"]].copy()
    show.columns = ["UF","Estado","Região","IDH","Internações","Óbito%",
                    "Dias Médios","Custo Médio","Int/1k hab","Rank Efic.","Score"]
    st.dataframe(show, width='stretch', hide_index=True)


# ════════════════════════════════════════════════════════════════
# PAGE: DIAGNÓSTICOS
# ════════════════════════════════════════════════════════════════
elif page == "🦠 Diagnósticos":

    st.markdown("""
    <div class="main-header">
        <h1>🦠 Análise de <span>Diagnósticos CID-10</span></h1>
        <p>gold.gld_diagnosticos_resumo · Ranking por volume, custo e mortalidade</p>
    </div>
    """, unsafe_allow_html=True)

    diag = query("SELECT * FROM gold.gld_diagnosticos_resumo WHERE ano = 2023 ORDER BY rank_volume")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec-title">Top 10 CIDs — Volume de Internações</div>', unsafe_allow_html=True)
        top = diag.head(10)
        fig = px.bar(top, x="total_internacoes", y="descricao_cid", orientation="h",
                     color="taxa_obito_pct",
                     color_continuous_scale=[[0,"#f5ede0"],[0.5,AMBER],[1,RUST]])
        fig.update_layout(**PL, height=340, yaxis_categoryorder="total ascending",
                          coloraxis_colorbar=dict(title="Óbito%", len=0.6))
        st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown('<div class="sec-title">Custo vs. Dias Médios por CID</div>', unsafe_allow_html=True)
        fig2 = px.scatter(diag, x="dias_medio", y="custo_medio",
                          size="total_internacoes", text="cid_principal",
                          color="taxa_obito_pct",
                          color_continuous_scale=[[0,"#f5ede0"],[0.5,AMBER],[1,RUST]],
                          size_max=40, opacity=0.8)
        fig2.update_traces(textposition="top center", textfont_size=9)
        fig2.update_layout(**PL, height=340)
        fig2.update_yaxes(tickformat=",.0f", tickprefix="R$ ")
        st.plotly_chart(fig2, width='stretch')

    st.markdown('<div class="sec-title">Custo Total por Capítulo CID</div>', unsafe_allow_html=True)
    cap = query("""
        SELECT capitulo_cid, SUM(custo_total) AS custo, SUM(total_internacoes) AS n
        FROM gold.gld_diagnosticos_resumo WHERE ano = 2023
        GROUP BY 1 ORDER BY custo DESC
    """)
    fig3 = px.bar(cap, x="capitulo_cid", y="custo", color="n",
                  color_continuous_scale=[[0,"#f5ede0"],[1,GOLD]])
    fig3.update_layout(**PL, height=300)
    fig3.update_yaxes(tickformat=",.0f", tickprefix="R$ ")
    st.plotly_chart(fig3, width='stretch')


# ════════════════════════════════════════════════════════════════
# PAGE: TEMPORAL
# ════════════════════════════════════════════════════════════════
elif page == "📈 Temporal":

    st.markdown("""
    <div class="main-header">
        <h1>📈 Evolução <span>Temporal</span></h1>
        <p>gold.gld_evolucao_temporal · Série mensal com médias móveis · 2019–2023</p>
    </div>
    """, unsafe_allow_html=True)

    temp = query("SELECT * FROM gold.gld_evolucao_temporal ORDER BY regiao, ano, mes")
    temp["periodo"] = temp["ano"].astype(str) + "-" + temp["mes"].astype(str).str.zfill(2)

    regioes = sorted(temp["regiao"].unique())
    sel_reg = st.multiselect("Regiões", regioes, default=regioes[:3])
    df_f = temp[temp["regiao"].isin(sel_reg)] if sel_reg else temp

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec-title">Internações Mensais (com MM3)</div>', unsafe_allow_html=True)
        fig = go.Figure()
        for i, reg in enumerate(sel_reg or regioes[:3]):
            d = df_f[df_f["regiao"] == reg].sort_values(["ano","mes"])
            cor = COLORS[i % len(COLORS)]
            fig.add_trace(go.Scatter(x=d["periodo"], y=d["total_internacoes"],
                mode="lines", name=f"{reg} real",
                line=dict(color=cor, width=1, dash="dot"), opacity=0.5))
            fig.add_trace(go.Scatter(x=d["periodo"], y=d["mm3_internacoes"],
                mode="lines", name=f"{reg} MM3",
                line=dict(color=cor, width=2.5)))
        fig.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)", font_size=10))
        fig.update_xaxes(tickangle=-45, tickfont_size=9)
        st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown('<div class="sec-title">Taxa de Óbito Mensal (%)</div>', unsafe_allow_html=True)
        fig2 = px.line(df_f, x="periodo", y="taxa_obito_pct", color="regiao",
                       color_discrete_sequence=COLORS)
        fig2.update_traces(line_width=2)
        fig2.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
        fig2.update_xaxes(tickangle=-45, tickfont_size=9)
        st.plotly_chart(fig2, width='stretch')

    st.markdown('<div class="sec-title">Sazonalidade — Internações por Mês (média 2019–2023)</div>', unsafe_allow_html=True)
    saz = temp.groupby(["mes","regiao"])["total_internacoes"].mean().reset_index()
    meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    saz["mes_nome"] = saz["mes"].apply(lambda x: meses[x-1])
    fig3 = px.line(saz, x="mes_nome", y="total_internacoes", color="regiao",
                   color_discrete_sequence=COLORS, markers=True,
                   category_orders={"mes_nome": meses})
    fig3.update_traces(line_width=2.5, marker_size=6)
    fig3.update_layout(**PL, height=300, legend=dict(bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig3, width='stretch')


# ════════════════════════════════════════════════════════════════
# PAGE: ARQUITETURA
# ════════════════════════════════════════════════════════════════
elif page == "⚙️ Arquitetura":

    st.markdown("""
    <div class="main-header">
        <h1>⚙️ <span>Arquitetura</span> do Projeto</h1>
        <p>Medallion Architecture · dbt · DuckDB · Airflow · GitHub Actions</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec-title">Arquitetura Medallion</div>', unsafe_allow_html=True)
    steps = [
        ("dot-bronze", "FONTES",        "IBGE/SIDRA — Estados e municípios (2019–2023) · DATASUS/SIH — Internações CID-10"),
        ("dot-bronze", "INGESTÃO",      "ingestion/ingest.py → CSVs em data/raw/ · load_to_duckdb.py → schema bronze"),
        ("dot-bronze", "BRONZE",        "brz_ibge_estados · brz_ibge_municipios · brz_datasus_internacoes — views passthrough"),
        ("dot-silver", "SILVER",        "slv_estados (IDH, quartil PIB, YoY) · slv_municipios (porte, densidade) · slv_internacoes (custo/dia, categorias)"),
        ("dot-gold",   "GOLD",          "gld_saude_por_uf (KPIs + score) · gld_diagnosticos_resumo (rankings) · gld_evolucao_temporal (MM3)"),
        ("dot-gold",   "TESTES dbt",    "not_null · unique · accepted_values · dbt_utils.accepted_range em todas as camadas"),
        ("dot-silver", "ORQUESTRAÇÃO",  "Airflow DAG: ingestão → bronze → dbt run → dbt test · schedule: 0 3 * * *"),
        ("dot-gold",   "CI/CD",         "GitHub Actions: dbt parse → compile → run → test → deploy docs para GitHub Pages"),
    ]
    for dot, title, desc in steps:
        st.markdown(f"""
        <div class="pipeline-card">
            <div class="layer-dot {dot}"></div>
            <div>
                <div class="pc-title">{title}</div>
                <div class="pc-desc">{desc}</div>
            </div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec-title">Modelos dbt — Lineage</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame({
        "Camada":          ["Bronze","Bronze","Bronze","Silver","Silver","Silver","Gold","Gold","Gold"],
        "Modelo":          ["brz_ibge_estados","brz_ibge_municipios","brz_datasus_internacoes",
                            "slv_estados","slv_municipios","slv_internacoes",
                            "gld_saude_por_uf","gld_diagnosticos_resumo","gld_evolucao_temporal"],
        "Materialização":  ["view"]*3 + ["table"]*6,
        "Depende de":      ["source:bronze","source:bronze","source:bronze",
                            "brz_ibge_estados","brz_ibge_municipios + slv_estados",
                            "brz_datasus_internacoes + slv_estados",
                            "slv_estados + slv_internacoes","slv_internacoes","slv_internacoes"],
        "Testes":          ["2","2","4","6","4","6","5","4","3"],
    }), width='stretch', hide_index=True)

    st.markdown('<div class="sec-title">Stack Técnico</div>', unsafe_allow_html=True)
    st.table(pd.DataFrame([
        ["Warehouse",          "DuckDB 0.10 (local) / BigQuery (cloud-ready)"],
        ["Transformação",      "dbt-duckdb 1.8 · SQL + Jinja2 · macros customizados"],
        ["Camadas",            "Bronze (views) → Silver (tables) → Gold (tables)"],
        ["Qualidade de Dados", "dbt tests: not_null, unique, accepted_values, dbt_utils.accepted_range"],
        ["Orquestração",       "Apache Airflow 2.9 · DAG com TaskGroups"],
        ["CI/CD",              "GitHub Actions · dbt parse → compile → run → test → deploy docs"],
        ["Ingestão",           "Python + Pandas · IBGE API / DATASUS FTP (simulado)"],
        ["Dashboard",          "Streamlit · Plotly · DuckDB read_only"],
    ], columns=["Componente", "Tecnologia"]))