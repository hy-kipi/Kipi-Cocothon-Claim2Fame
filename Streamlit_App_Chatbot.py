import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import pandas as pd
import plotly.graph_objects as go
import matplotlib
import base64

try:
    import _snowflake
    HAS_SNOW_API = True
except ImportError:
    HAS_SNOW_API = False

st.set_page_config(layout="wide")

st.markdown("""<style>
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-card: #1E1E2E;
            --bg-card-hover: #252536;
            --border-card: #2D2D3F;
            --text-primary: #E0E0E0;
            --text-secondary: #A0A0B0;
            --text-muted: #6B6B80;
            --bg-subtle: #252536;
            --grid-color: #2D2D3F;
        }
    }
    @media (prefers-color-scheme: light) {
        :root {
            --bg-card: #FFFFFF;
            --bg-card-hover: #F5F7FA;
            --border-card: #ECEFF1;
            --text-primary: #263238;
            --text-secondary: #546E7A;
            --text-muted: #78909C;
            --bg-subtle: #F5F7FA;
            --grid-color: #F5F5F5;
        }
    }

    [data-testid="stAppViewContainer"] {
        color: var(--text-primary);
    }

    div[data-testid="stMetric"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-card) !important;
        border-radius: 10px; padding: 12px 16px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.08);
    }
    div[data-testid="stMetric"] label {
        font-size: 0.85rem !important;
        color: var(--text-secondary) !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.4rem !important; font-weight: 700;
        color: var(--text-primary) !important;
    }

    div[data-testid="stExpander"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-card) !important;
        border-radius: 10px !important;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--border-card) !important;
        border-radius: 8px !important;
    }

    button[data-baseweb="tab"] {
        background: transparent !important; border: none !important;
        border-bottom: 3px solid transparent !important;
        padding: 12px 20px !important; font-size: 0.9rem !important;
        font-weight: 600 !important; color: var(--text-muted) !important;
        transition: all 0.3s ease !important; border-radius: 8px 8px 0 0 !important;
    }
    button[data-baseweb="tab"]:hover {
        background: rgba(21,101,192,0.1) !important; color: #1565C0 !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        background: rgba(21,101,192,0.15) !important;
        border-bottom: 3px solid #1565C0 !important; color: #1565C0 !important;
    }
    div[data-baseweb="tab-list"] {
        background: var(--bg-subtle) !important; border-radius: 10px 10px 0 0 !important;
        padding: 4px 4px 0 4px !important; border-bottom: 2px solid var(--border-card) !important;
    }

    .stChatMessage {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-card) !important;
        border-radius: 12px !important;
    }

    [data-testid="stChatInput"] textarea {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-card) !important;
        color: var(--text-primary) !important;
    }
</style>""", unsafe_allow_html=True)

session = get_active_session()

def load_image_b64(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

c2f_b64 = load_image_b64("images/C2F Logo.png")
vcare_b64 = load_image_b64("images/Vcare-Logo.png")

DEFAULT_DB = "INSURANCE_CLAIM_DB"
DEFAULT_SCHEMA = "CONSUMPTION_LAYER"
DEFAULT_SV = "INSURANCE_CLAIM_DB.CONSUMPTION_LAYER.INSURANCE_CLAIMS_SV"

defaults = {
    "messages": [],
    "active_page": "Summary",
    "selected_db": DEFAULT_DB,
    "selected_schema": DEFAULT_SCHEMA,
    "semantic_view": DEFAULT_SV,
    "selected_model": "mistral-large2",
    "chatbot_mode": "Cortex Analyst",
    "chat_history_enabled": True,
    "custom_questions": [],
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

DB = st.session_state.selected_db
SCH = st.session_state.selected_schema
FQ = f"{DB}.{SCH}"
SV = st.session_state.semantic_view
MODEL = st.session_state.selected_model


@st.cache_data(ttl=300, show_spinner=False)
def cached_query(sql):
    return session.sql(sql).to_pandas()


def fresh_query(sql):
    return session.sql(sql).to_pandas()


def call_cortex_analyst(question, history=None):
    if not HAS_SNOW_API:
        return None
    messages = []
    if history:
        for msg in history[-6:]:
            role = "user" if msg["role"] == "user" else "analyst"
            messages.append({"role": role, "content": [{"type": "text", "text": str(msg.get("content", ""))}]})
    messages.append({"role": "user", "content": [{"type": "text", "text": question}]})
    body = {"messages": messages, "semantic_view": SV}
    try:
        resp = _snowflake.send_snow_api_request("POST", "/api/v2/cortex/analyst/message", {}, {}, body, {}, 30000)
        content = resp.get("content", "")
        if isinstance(content, str):
            return json.loads(content)
        elif isinstance(content, dict):
            return content
        return None
    except Exception:
        return None


def call_cortex_complete(prompt, model=None):
    model = model or MODEL
    escaped = prompt.replace("'", "''")
    return session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped}')").collect()[0][0]


SCHEMA_CONTEXT = f"""You are an insurance claims data analyst. Query from {FQ} using the star schema.
Tables: FACT_CLAIMS, FACT_CLAIM_EXPENSE, DIM_CLAIM_TYPE, DIM_DATE, DIM_GEOGRAPHY, DIM_LOSS_CAUSE, DIM_POLICY, DIM_WEATHER_EVENT.
Join keys: FACT_CLAIMS.DATE_KEY=DIM_DATE.DATE_KEY, FACT_CLAIMS.GEOGRAPHY_KEY=DIM_GEOGRAPHY.GEOGRAPHY_KEY,
FACT_CLAIMS.CLAIM_TYPE_KEY=DIM_CLAIM_TYPE.CLAIM_TYPE_KEY, FACT_CLAIMS.LOSS_CAUSE_KEY=DIM_LOSS_CAUSE.LOSS_CAUSE_KEY,
FACT_CLAIMS.WEATHER_EVENT_KEY=DIM_WEATHER_EVENT.WEATHER_EVENT_KEY, FACT_CLAIMS.POLICY_KEY=DIM_POLICY.POLICY_KEY,
FACT_CLAIM_EXPENSE.CLAIM_KEY=FACT_CLAIMS.CLAIM_KEY.
Always use fully qualified table names ({FQ}.<table>). Return SQL in ```sql blocks."""

with st.sidebar:
    st.markdown("""<style>
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0D1B2A 0%, #1B2838 100%);
        }
        [data-testid="stSidebar"] [data-testid="stMarkdown"] p,
        [data-testid="stSidebar"] [data-testid="stMarkdown"] li,
        [data-testid="stSidebar"] .stCaption p {
            color: #B0BEC5 !important;
        }
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 { color: #ECEFF1 !important; }
        [data-testid="stSidebar"] hr { border-color: #263238 !important; }
        [data-testid="stSidebar"] .stButton>button[kind="secondary"] {
            background: transparent !important; border: 1px solid #37474F !important;
            color: #B0BEC5 !important; transition: all 0.2s;
        }
        [data-testid="stSidebar"] .stButton>button[kind="secondary"]:hover {
            background: #1565C0 !important; border-color: #1565C0 !important;
            color: white !important;
        }
        [data-testid="stSidebar"] .stButton>button[kind="primary"] {
            background: linear-gradient(135deg, #1565C0, #1E88E5) !important;
            border: none !important; color: white !important;
            box-shadow: 0 2px 8px rgba(21,101,192,0.4) !important;
        }
    </style>""", unsafe_allow_html=True)

    if c2f_b64:
        st.markdown(f"""<div style='text-align:center;padding:16px 0 0 0;margin-bottom:-12px'>
            <img src='data:image/png;base64,{c2f_b64}' style='max-width:220px;max-height:90px'>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("<h2 style='text-align:center;color:#ECEFF1;margin:0'>C2F</h2>", unsafe_allow_html=True)

    st.markdown("""<div style='text-align:center;margin-top:20px;margin-bottom:-20px'>
        <p style='color:#ECEFF1;font-size:1.25rem;font-weight:600;margin:0;letter-spacing:0.3px'>Claims Intelligence</p>
        <p style='color:#546E7A;font-size:0.78rem;margin:2px 0 0 0'>Insurance Analytics Platform</p>
    </div>""", unsafe_allow_html=True)
    st.markdown("---")

    pages = {
        "Summary": ":material/dashboard:",
        "Analytics": ":material/analytics:",
        "Chatbot": ":material/chat:",
        "Transform": ":material/build:",
        "DB Explorer": ":material/storage:",
        "Sample Questions": ":material/lightbulb:",
        "Settings": ":material/settings:",
    }
    for page, icon in pages.items():
        btn_type = "primary" if st.session_state.active_page == page else "secondary"
        if st.button(f"{icon}  {page}", use_container_width=True, type=btn_type, key=f"nav_{page}"):
            st.session_state.active_page = page
            st.rerun()

    st.markdown("---")

    try:
        sidebar_kpi = cached_query(f"""
            SELECT COUNT(*) AS TOTAL,
                SUM(CASE WHEN CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_C,
                COALESCE(SUM(PAID_AMOUNT),0) AS PAID
            FROM {FQ}.FACT_CLAIMS
        """)
        sk1,sk2 = st.columns(2)
        with sk1:
            st.markdown(f"<div style='text-align:center'><span style='color:#42A5F5;font-size:1.3rem;font-weight:700'>{int(sidebar_kpi['TOTAL'].iloc[0]):,}</span><br><span style='color:#78909C;font-size:0.7rem'>Total Claims</span></div>", unsafe_allow_html=True)
        with sk2:
            st.markdown(f"<div style='text-align:center'><span style='color:#EF5350;font-size:1.3rem;font-weight:700'>{int(sidebar_kpi['OPEN_C'].iloc[0]):,}</span><br><span style='color:#78909C;font-size:0.7rem'>Open Claims</span></div>", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align:center;margin-top:8px'><span style='color:#66BB6A;font-size:1.3rem;font-weight:700'>${float(sidebar_kpi['PAID'].iloc[0])/1e6:.1f}M</span><br><span style='color:#78909C;font-size:0.7rem'>Total Paid</span></div>", unsafe_allow_html=True)
    except Exception:
        pass

    st.markdown("---")

    mode_color = "#42A5F5" if st.session_state.chatbot_mode == "Cortex Analyst" else "#FF9800"
    st.markdown(f"""
        <div style='background:#1A2332;border-radius:8px;padding:10px 12px;border:1px solid #263238'>
            <div style='color:#78909C;font-size:0.7rem;text-transform:uppercase;letter-spacing:1px'>Connection</div>
            <div style='color:#ECEFF1;font-size:0.85rem;font-weight:600;margin:4px 0'>{DB}.{SCH}</div>
            <div style='margin-top:6px;color:#78909C;font-size:0.7rem;text-transform:uppercase;letter-spacing:1px'>Mode</div>
            <div style='margin-top:2px'><span style='background:{mode_color};color:white;padding:2px 8px;border-radius:10px;font-size:0.75rem;font-weight:600'>{st.session_state.chatbot_mode}</span></div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:#37474F;font-size:0.7rem'>Powered by Snowflake Cortex</p>", unsafe_allow_html=True)

# if vcare_b64:
#     st.markdown(f"""<div style='display:flex;align-items:center;gap:14px;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #ECEFF1'>
#         <img src='data:image/png;base64,{vcare_b64}' style='height:40px'>
#         <div>
#             <span style='font-weight:700;color:#263238;font-size:1.05rem'>VCare</span>
#             <span style='color:#78909C;font-size:0.82rem;margin-left:8px'>Claims Intelligence</span>
#         </div>
#     </div>""", unsafe_allow_html=True)
# else:
#     st.markdown("""<div style='display:flex;align-items:center;gap:14px;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #ECEFF1'>
#         <span style='font-weight:700;color:#263238;font-size:1.05rem'>VCare</span>
#         <span style='color:#78909C;font-size:0.82rem'>Claims Intelligence</span>
#     </div>""", unsafe_allow_html=True)


def render_summary():
    vcare_img_tag = f"<img src='data:image/png;base64,{vcare_b64}' style='height:120px;opacity:0.9'>" if vcare_b64 else ""

    st.markdown("""<style>
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 10px; padding: 12px 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
        div[data-testid="stMetric"] label { font-size: 0.85rem !important; }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: 700; }
    </style>""", unsafe_allow_html=True)

    BLUE = ['#0D47A1','#1565C0','#1976D2','#1E88E5','#42A5F5','#64B5F6','#90CAF9','#BBDEFB']
    SEV_COLORS = {'Minor':'#4CAF50','Moderate':'#2196F3','Significant':'#FF9800','Severe':'#F44336','Catastrophic':'#9C27B0'}
    STATUS_COLORS = {'Open':'#42A5F5','Closed':'#66BB6A','Approved':'#26A69A','Pending Review':'#FFA726','Rejected':'#EF5350','Stalled':'#AB47BC'}

    kpi = cached_query(f"""
        SELECT COUNT(*) AS TOTAL_CLAIMS,
            SUM(CASE WHEN CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
            SUM(CASE WHEN CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS CLOSED_CLAIMS,
            COALESCE(SUM(PAID_AMOUNT),0) AS TOTAL_PAID,
            COALESCE(SUM(RESERVE_AMOUNT),0) AS TOTAL_RESERVES,
            COALESCE(SUM(NET_INCURRED),0) AS NET_INCURRED,
            COALESCE(SUM(RECOVERY_AMOUNT+SUBROGATION_AMOUNT+SALVAGE_AMOUNT),0) AS TOTAL_RECOVERY,
            ROUND(AVG(PAID_AMOUNT),2) AS AVG_COST,
            ROUND(AVG(DAYS_TO_CLOSE),1) AS AVG_CLOSE_DAYS,
            SUM(CASE WHEN LITIGATION_INDICATOR THEN 1 ELSE 0 END) AS LITIGATED,
            SUM(CASE WHEN FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD,
            SUM(CASE WHEN CAT_INDICATOR THEN 1 ELSE 0 END) AS CAT,
            SUM(CASE WHEN IS_WEATHER_RELATED THEN 1 ELSE 0 END) AS WEATHER,
            COALESCE(SUM(INCURRED_LOSS),0) AS TOTAL_INCURRED
        FROM {FQ}.FACT_CLAIMS
    """)
    total = int(kpi['TOTAL_CLAIMS'].iloc[0])
    open_c = int(kpi['OPEN_CLAIMS'].iloc[0])
    closed_c = int(kpi['CLOSED_CLAIMS'].iloc[0])
    total_paid = float(kpi['TOTAL_PAID'].iloc[0])
    total_reserves = float(kpi['TOTAL_RESERVES'].iloc[0])
    net_incurred = float(kpi['NET_INCURRED'].iloc[0])
    total_recovery = float(kpi['TOTAL_RECOVERY'].iloc[0])
    avg_cost = float(kpi['AVG_COST'].iloc[0])
    avg_close = float(kpi['AVG_CLOSE_DAYS'].iloc[0])
    total_incurred = float(kpi['TOTAL_INCURRED'].iloc[0])
    fraud_count = int(kpi['FRAUD'].iloc[0])
    cat_count = int(kpi['CAT'].iloc[0])
    weather_count = int(kpi['WEATHER'].iloc[0])
    litigated_count = int(kpi['LITIGATED'].iloc[0])
    settlement_ratio = round(closed_c / total * 100, 1) if total > 0 else 0
    loss_ratio = round(total_paid / total_incurred * 100, 1) if total_incurred > 0 else 0

    prev_month = cached_query(f"""
        WITH latest AS (
            SELECT MAX(d.YEAR_MONTH) AS CM FROM {FQ}.DIM_DATE d JOIN {FQ}.FACT_CLAIMS fc ON fc.DATE_KEY=d.DATE_KEY
        )
        SELECT COUNT(*) AS CLAIMS,
            SUM(CASE WHEN fc.CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_C,
            COALESCE(SUM(fc.PAID_AMOUNT),0) AS PAID,
            COALESCE(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES
        FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
        WHERE d.YEAR_MONTH=(SELECT TO_CHAR(DATEADD(MONTH,-1,TO_DATE(CM||'-01','YYYY-MM-DD')),'YYYY-MM') FROM latest)
    """)
    curr_month = cached_query(f"""
        WITH latest AS (
            SELECT MAX(d.YEAR_MONTH) AS CM FROM {FQ}.DIM_DATE d JOIN {FQ}.FACT_CLAIMS fc ON fc.DATE_KEY=d.DATE_KEY
        )
        SELECT COUNT(*) AS CLAIMS,
            SUM(CASE WHEN fc.CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_C,
            COALESCE(SUM(fc.PAID_AMOUNT),0) AS PAID,
            COALESCE(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES
        FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
        WHERE d.YEAR_MONTH=(SELECT CM FROM latest)
    """)
    d_claims = int(curr_month['CLAIMS'].iloc[0]) - int(prev_month['CLAIMS'].iloc[0])
    d_open = int(curr_month['OPEN_C'].iloc[0]) - int(prev_month['OPEN_C'].iloc[0])
    d_paid = float(curr_month['PAID'].iloc[0]) - float(prev_month['PAID'].iloc[0])
    d_res = float(curr_month['RESERVES'].iloc[0]) - float(prev_month['RESERVES'].iloc[0])

    st.markdown(f"""<div style='background:linear-gradient(135deg,#0D47A1 0%,#1565C0 40%,#1E88E5 100%);border-radius:16px;padding:28px 32px;margin-bottom:20px;box-shadow:0 8px 32px rgba(13,71,161,0.25);position:relative;overflow:hidden'>
        <div style='position:absolute;top:-40px;right:-40px;width:200px;height:200px;background:rgba(255,255,255,0.05);border-radius:50%'></div>
        <div style='position:absolute;bottom:-60px;right:80px;width:280px;height:280px;background:rgba(255,255,255,0.03);border-radius:50%'></div>
        <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
                <h1 style='color:white;margin:0 0 4px 0;font-size:2rem;font-weight:800;letter-spacing:-0.5px'>Claims Intelligence Dashboard</h1>
                <p style='color:#BBDEFB;margin:0;font-size:1rem'>Real-time overview of <b style="color:#64B5F6">{total:,}</b> insurance claims across all lines of business</p>
            </div>
            <div style='flex-shrink:0;margin-left:24px'>{vcare_img_tag}</div>
        </div>
        <div style='display:flex;gap:32px;margin-top:18px;flex-wrap:wrap'>
            <div style='background:rgba(255,255,255,0.12);border-radius:12px;padding:12px 20px;backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.15)'>
                <div style='color:#90CAF9;font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;font-weight:600'>Total Paid</div>
                <div style='color:white;font-size:1.6rem;font-weight:800;margin-top:2px'>${total_paid/1e6:.1f}M</div>
            </div>
            <div style='background:rgba(255,255,255,0.12);border-radius:12px;padding:12px 20px;backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.15)'>
                <div style='color:#90CAF9;font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;font-weight:600'>Open Claims</div>
                <div style='color:#FFCDD2;font-size:1.6rem;font-weight:800;margin-top:2px'>{open_c:,}</div>
            </div>
            <div style='background:rgba(255,255,255,0.12);border-radius:12px;padding:12px 20px;backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.15)'>
                <div style='color:#90CAF9;font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;font-weight:600'>Settlement Rate</div>
                <div style='color:#C8E6C9;font-size:1.6rem;font-weight:800;margin-top:2px'>{settlement_ratio}%</div>
            </div>
            <div style='background:rgba(255,255,255,0.12);border-radius:12px;padding:12px 20px;backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.15)'>
                <div style='color:#90CAF9;font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;font-weight:600'>Net Incurred</div>
                <div style='color:#FFE0B2;font-size:1.6rem;font-weight:800;margin-top:2px'>${net_incurred/1e6:.1f}M</div>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    # c1,c2,c3,c4 = st.columns(4)
    # c1.metric("Total Claims",f"{total:,}",delta=f"{d_claims:+,} vs last month")
    # c2.metric("Open Claims",f"{open_c:,}",delta=f"{d_open:+,}",delta_color="inverse")
    # c3.metric("Total Paid",f"${total_paid/1e6:.1f}M",delta=f"${d_paid/1e3:+,.0f}K")
    # c4.metric("Total Reserves",f"${total_reserves/1e6:.1f}M",delta=f"${d_res/1e3:+,.0f}K",delta_color="inverse")

    # st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    
    risk_items = [
        ("Litigated", litigated_count, "#E65100"),
        ("Fraud Flagged", fraud_count, "#C62828"),
        ("CAT Claims", cat_count, "#6A1B9A"),
        ("Weather", weather_count, "#01579B"),
        ("Avg Cost", f"${avg_cost:,.0f}", "#2E7D32"),
        ("Avg Close", f"{avg_close}d", "#E65100"),
    ]
    risk_html = "<div style='display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin:8px 0'>"
    for label, value, color in risk_items:
        val_str = f"{value:,}" if isinstance(value, int) else value
        risk_html += f"""<div style='background:linear-gradient(135deg,{color}10,{color}08);border:1px solid {color}30;border-radius:12px;padding:14px;text-align:center'>
            <div style='color:{color};font-size:1.4rem;font-weight:800'>{val_str}</div>
            <div style='color:#546E7A;font-size:0.75rem;font-weight:600;margin-top:2px;text-transform:uppercase;letter-spacing:0.5px'>{label}</div>
        </div>"""
    risk_html += "</div>"
    st.markdown(risk_html, unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    trend_df = cached_query(f"""
        SELECT d.YEAR_MONTH AS MONTH,COUNT(*) AS CLAIMS,
            ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID,
            SUM(CASE WHEN fc.CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_COUNT,
            SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS CLOSED_COUNT,
            ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID,
            SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD_COUNT
        FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
        GROUP BY d.YEAR_MONTH ORDER BY d.YEAR_MONTH DESC LIMIT 12
    """)
    if not trend_df.empty:
        trend_df = trend_df.sort_values('MONTH')

    col_trend, col_gauges = st.columns([3, 2])

    with col_trend:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Monthly Claim Trend & Paid Amount</h3>", unsafe_allow_html=True)
        if not trend_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=trend_df['MONTH'], y=trend_df['TOTAL_PAID'], name='Paid ($)',
                marker=dict(color=trend_df['TOTAL_PAID'], colorscale=[[0,'#BBDEFB'],[0.5,'#42A5F5'],[1,'#0D47A1']], cornerradius=6, line=dict(color='white', width=1)),
                yaxis='y', hovertemplate='<b>%{x}</b><br>Paid: $%{y:,.0f}<extra></extra>'
            ))
            fig.add_trace(go.Scatter(
                x=trend_df['MONTH'], y=trend_df['CLAIMS'], name='Claims Filed',
                mode='lines+markers+text', text=trend_df['CLAIMS'], textposition='top center',
                textfont=dict(size=10, color='#C62828'),
                line=dict(color='#C62828', width=3, shape='spline'),
                marker=dict(size=9, color='#C62828', line=dict(color='white', width=2.5), symbol='circle'),
                yaxis='y2', hovertemplate='<b>%{x}</b><br>Claims: %{y}<extra></extra>'
            ))
            fig.add_trace(go.Scatter(
                x=trend_df['MONTH'], y=trend_df['CLOSED_COUNT'], name='Settled',
                mode='lines+markers',
                line=dict(color='#2E7D32', width=2, dash='dot', shape='spline'),
                marker=dict(size=6, symbol='diamond', color='#2E7D32', line=dict(color='white', width=1.5)),
                yaxis='y2', hovertemplate='<b>%{x}</b><br>Settled: %{y}<extra></extra>'
            ))
            fig.update_layout(
                height=370, margin=dict(t=10, b=50, l=60, r=60),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title='Paid ($)', side='left', gridcolor='#F5F5F5', showgrid=True, zeroline=False),
                yaxis2=dict(title='Claim Count', side='right', overlaying='y', showgrid=False, zeroline=False),
                legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)', font=dict(size=11)),
                bargap=0.3, hovermode='x unified', hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='#E0E0E0')
            )
            st.plotly_chart(fig, use_container_width=True, key="summary_trend_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_gauges:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Performance Gauges</h3>", unsafe_allow_html=True)
        fraud_rate = round(fraud_count / total * 100, 2) if total > 0 else 0
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="gauge+number", value=settlement_ratio,
            title={'text': "<b>Settlement Rate</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#B0BEC5', 'dtick': 20},
                   'bar': {'color': '#1565C0', 'thickness': 0.3}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, 40], 'color': '#FFEBEE'}, {'range': [40, 70], 'color': '#FFF8E1'}, {'range': [70, 100], 'color': '#E8F5E9'}],
                   'threshold': {'line': {'color': '#2E7D32', 'width': 3}, 'thickness': 0.8, 'value': settlement_ratio}},
            domain={'x': [0, 0.45], 'y': [0.55, 1]}
        ))
        fig.add_trace(go.Indicator(
            mode="gauge+number", value=loss_ratio,
            title={'text': "<b>Loss Ratio</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, 150], 'tickwidth': 1, 'tickcolor': '#B0BEC5', 'dtick': 30},
                   'bar': {'color': '#E65100', 'thickness': 0.3}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, 60], 'color': '#E8F5E9'}, {'range': [60, 90], 'color': '#FFF8E1'}, {'range': [90, 150], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#D32F2F', 'width': 3}, 'thickness': 0.8, 'value': loss_ratio}},
            domain={'x': [0.55, 1], 'y': [0.55, 1]}
        ))
        fig.add_trace(go.Indicator(
            mode="gauge+number", value=fraud_rate,
            title={'text': "<b>Fraud Rate</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, max(fraud_rate * 2.5, 10)], 'tickwidth': 1, 'tickcolor': '#B0BEC5'},
                   'bar': {'color': '#C62828', 'thickness': 0.3}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, fraud_rate * 0.5], 'color': '#E8F5E9'}, {'range': [fraud_rate * 0.5, fraud_rate * 1.5], 'color': '#FFF8E1'}, {'range': [fraud_rate * 1.5, max(fraud_rate * 2.5, 10)], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#F44336', 'width': 3}, 'thickness': 0.8, 'value': fraud_rate}},
            domain={'x': [0, 0.45], 'y': [0, 0.42]}
        ))
        fig.add_trace(go.Indicator(
            mode="gauge+number", value=avg_close,
            title={'text': "<b>Avg Days to Close</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': 'd', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, avg_close * 2], 'tickwidth': 1, 'tickcolor': '#B0BEC5'},
                   'bar': {'color': '#FF8F00', 'thickness': 0.3}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, avg_close * 0.7], 'color': '#E8F5E9'}, {'range': [avg_close * 0.7, avg_close * 1.3], 'color': '#FFF8E1'}, {'range': [avg_close * 1.3, avg_close * 2], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#E65100', 'width': 3}, 'thickness': 0.8, 'value': avg_close}},
            domain={'x': [0.55, 1], 'y': [0, 0.42]}
        ))
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=20, r=20), paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Inter, sans-serif'))
        st.plotly_chart(fig, use_container_width=True, key="summary_gauges_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    col_fin, col_pipe = st.columns([3, 2])
    with col_fin:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Financial Flow</h3>", unsafe_allow_html=True)
        wf_data = cached_query(f"""
            SELECT COALESCE(SUM(INCURRED_LOSS),0) AS IL, COALESCE(SUM(RECOVERY_AMOUNT),0) AS REC,
                COALESCE(SUM(SUBROGATION_AMOUNT),0) AS SUB, COALESCE(SUM(SALVAGE_AMOUNT),0) AS SAL,
                COALESCE(SUM(NET_INCURRED),0) AS NI
            FROM {FQ}.FACT_CLAIMS
        """)
        il = float(wf_data['IL'].iloc[0]); rec = float(wf_data['REC'].iloc[0])
        sub = float(wf_data['SUB'].iloc[0]); sal = float(wf_data['SAL'].iloc[0])
        ni = float(wf_data['NI'].iloc[0])
        fig = go.Figure(go.Waterfall(
            x=['Incurred Loss', 'Recovery', 'Subrogation', 'Salvage', 'Net Incurred'],
            y=[il, -rec, -sub, -sal, ni],
            measure=['absolute', 'relative', 'relative', 'relative', 'total'],
            text=[f"<b>${v/1e6:.1f}M</b>" for v in [il, rec, sub, sal, ni]],
            textposition='outside', textfont=dict(size=11),
            connector=dict(line=dict(color='#CFD8DC', width=1.5, dash='dot')),
            increasing=dict(marker=dict(color='#EF5350', line=dict(color='#C62828', width=1))),
            decreasing=dict(marker=dict(color='#66BB6A', line=dict(color='#2E7D32', width=1))),
            totals=dict(marker=dict(color='#1565C0', line=dict(color='#0D47A1', width=1)))
        ))
        fig.update_layout(height=320, margin=dict(t=10, b=50, l=60, r=10),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(gridcolor='#F5F5F5', zeroline=False), showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="summary_waterfall_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_pipe:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims Pipeline</h3>", unsafe_allow_html=True)
        status_df = cached_query(f"SELECT CLAIM_STATUS,COUNT(*) AS COUNT FROM {FQ}.FACT_CLAIMS GROUP BY CLAIM_STATUS ORDER BY COUNT DESC")
        if not status_df.empty:
            fig = go.Figure(go.Funnel(
                y=status_df['CLAIM_STATUS'], x=status_df['COUNT'],
                textinfo='value+percent initial',
                texttemplate='<b>%{value:,}</b> (%{percentInitial})', textfont=dict(size=12),
                marker=dict(color=[STATUS_COLORS.get(s, '#78909C') for s in status_df['CLAIM_STATUS']], line=dict(color='white', width=2)),
                connector=dict(line=dict(color='#E0E0E0', width=1)),
                hovertemplate='<b>%{y}</b><br>Claims: %{x:,}<extra></extra>'
            ))
            fig.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10), paper_bgcolor='rgba(0,0,0,0)', funnelmode='stack')
            st.plotly_chart(fig, use_container_width=True, key="summary_funnel_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    col_sev, col_loss, col_heatmap = st.columns([2, 2, 3])

    with col_sev:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Severity Breakdown</h3>", unsafe_allow_html=True)
        sev_df = cached_query(f"""
            SELECT CLAIM_SEVERITY, COUNT(*) AS COUNT, ROUND(AVG(PAID_AMOUNT),0) AS AVG_PAID,
                ROUND(COUNT(*)::FLOAT/SUM(COUNT(*)) OVER ()*100,1) AS PCT
            FROM {FQ}.FACT_CLAIMS GROUP BY CLAIM_SEVERITY ORDER BY COUNT DESC
        """)
        if not sev_df.empty:
            fig = go.Figure(go.Pie(
                labels=sev_df['CLAIM_SEVERITY'], values=sev_df['COUNT'], hole=0.6,
                textinfo='label+percent', texttemplate='<b>%{label}</b><br>%{percent}', textfont=dict(size=10),
                marker=dict(colors=[SEV_COLORS.get(s, '#78909C') for s in sev_df['CLAIM_SEVERITY']], line=dict(color='white', width=2.5)),
                hovertemplate='<b>%{label}</b><br>Claims: %{value:,}<br>Share: %{percent}<extra></extra>',
                pull=[0.04 if s == 'Catastrophic' else 0 for s in sev_df['CLAIM_SEVERITY']]
            ))
            fig.add_annotation(text=f"<b>{total:,}</b><br><span style='font-size:10px;color:#78909C'>Claims</span>",
                x=0.5, y=0.5, font=dict(size=18, color='#263238'), showarrow=False)
            fig.update_layout(height=310, margin=dict(t=10, b=10, l=10, r=10), showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True, key="summary_sev_donut_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_loss:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Top Loss Causes</h3>", unsafe_allow_html=True)
        lc_df = cached_query(f"""
            SELECT lc.LOSS_CAUSE, lc.LOSS_CATEGORY, COUNT(*) AS COUNT, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_LOSS_CAUSE lc ON fc.LOSS_CAUSE_KEY=lc.LOSS_CAUSE_KEY
            GROUP BY lc.LOSS_CAUSE, lc.LOSS_CATEGORY ORDER BY COUNT DESC LIMIT 8
        """)
        if not lc_df.empty:
            cat_colors = {'Weather-Related': '#1565C0', 'Accident': '#FF9800', 'Crime': '#F44336',
                          'Injury': '#9C27B0', 'Mechanical/Product': '#607D8B', 'Professional/Cyber': '#00897B'}
            fig = go.Figure(go.Bar(
                y=lc_df['LOSS_CAUSE'], x=lc_df['COUNT'], orientation='h',
                text=[f"<b>{c:,}</b>  ${p/1e6:.1f}M" for c, p in zip(lc_df['COUNT'], lc_df['TOTAL_PAID'])],
                textposition='outside', textfont=dict(size=9),
                marker=dict(color=[cat_colors.get(c, '#78909C') for c in lc_df['LOSS_CATEGORY']], cornerradius=5, line=dict(color='white', width=1)),
                hovertemplate='<b>%{y}</b><br>Claims: %{x:,}<extra></extra>'
            ))
            fig.update_layout(height=310, margin=dict(t=10, b=10, l=10, r=90),
                yaxis=dict(autorange='reversed'), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False, showticklabels=False))
            st.plotly_chart(fig, use_container_width=True, key="summary_loss_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_heatmap:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Region x Severity Heatmap</h3>", unsafe_allow_html=True)
        hm_df = cached_query(f"""
            SELECT g.REGION, fc.CLAIM_SEVERITY, COUNT(*) AS CNT
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_GEOGRAPHY g ON fc.GEOGRAPHY_KEY=g.GEOGRAPHY_KEY
            GROUP BY g.REGION, fc.CLAIM_SEVERITY ORDER BY g.REGION, fc.CLAIM_SEVERITY
        """)
        if not hm_df.empty:
            sev_order = ['Minor', 'Moderate', 'Significant', 'Severe', 'Catastrophic']
            hm_pivot = hm_df.pivot_table(index='REGION', columns='CLAIM_SEVERITY', values='CNT', fill_value=0)
            hm_pivot = hm_pivot.reindex(columns=[s for s in sev_order if s in hm_pivot.columns])
            fig = go.Figure(go.Heatmap(
                z=hm_pivot.values, x=hm_pivot.columns.tolist(), y=hm_pivot.index.tolist(),
                colorscale=[[0, '#E3F2FD'], [0.25, '#90CAF9'], [0.5, '#42A5F5'], [0.75, '#1565C0'], [1, '#0D47A1']],
                text=[[f"<b>{int(v):,}</b>" for v in row] for row in hm_pivot.values],
                texttemplate='%{text}', textfont=dict(size=12, color='white'),
                hovertemplate='<b>%{y}</b> - %{x}<br>Claims: %{z:,}<extra></extra>',
                colorbar=dict(thickness=12, len=0.8, title=dict(text='Claims', side='right'), tickfont=dict(size=9))
            ))
            fig.update_layout(height=310, margin=dict(t=10, b=40, l=10, r=10),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(side='bottom'), yaxis=dict(autorange='reversed'))
            st.plotly_chart(fig, use_container_width=True, key="summary_heatmap_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    left2, right2 = st.columns(2)
    with left2:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Paid vs Reserves by Claim Type</h3>", unsafe_allow_html=True)
        ct_df = cached_query(f"""
            SELECT ct.CLAIM_TYPE, ct.CLAIM_CATEGORY, COALESCE(SUM(fc.PAID_AMOUNT),0) AS PAID, COALESCE(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_CLAIM_TYPE ct ON fc.CLAIM_TYPE_KEY=ct.CLAIM_TYPE_KEY
            GROUP BY ct.CLAIM_TYPE, ct.CLAIM_CATEGORY ORDER BY PAID DESC
        """)
        if not ct_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(y=ct_df['CLAIM_TYPE'], x=ct_df['PAID'], name='Paid', orientation='h',
                marker=dict(color='#1565C0', cornerradius=4, line=dict(color='white', width=1)),
                hovertemplate='<b>%{y}</b><br>Paid: $%{x:,.0f}<extra></extra>'))
            fig.add_trace(go.Bar(y=ct_df['CLAIM_TYPE'], x=ct_df['RESERVES'], name='Reserves', orientation='h',
                marker=dict(color='#90CAF9', cornerradius=4, line=dict(color='white', width=1)),
                hovertemplate='<b>%{y}</b><br>Reserves: $%{x:,.0f}<extra></extra>'))
            fig.update_layout(height=380, barmode='group', margin=dict(t=10, b=10, l=10, r=10),
                yaxis=dict(autorange='reversed'), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
                xaxis=dict(gridcolor='#F5F5F5'))
            st.plotly_chart(fig, use_container_width=True, key="summary_ct_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    with right2:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims Treemap by Category & Type</h3>", unsafe_allow_html=True)
        tree_df = cached_query(f"""
            SELECT ct.CLAIM_CATEGORY, ct.CLAIM_TYPE, COUNT(*) AS COUNT, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_CLAIM_TYPE ct ON fc.CLAIM_TYPE_KEY=ct.CLAIM_TYPE_KEY
            GROUP BY ct.CLAIM_CATEGORY, ct.CLAIM_TYPE ORDER BY COUNT DESC
        """)
        if not tree_df.empty:
            cat_labels = []; cat_parents = []; cat_values = []; cat_colors_list = []; cat_text = []
            cat_palette = {'Commercial Lines': '#1565C0', 'Personal Lines': '#2E7D32', 'Specialty Lines': '#E65100'}
            for cat in tree_df['CLAIM_CATEGORY'].unique():
                cat_labels.append(cat); cat_parents.append("")
                cat_subset = tree_df[tree_df['CLAIM_CATEGORY'] == cat]
                cat_values.append(int(cat_subset['COUNT'].sum()))
                cat_colors_list.append(cat_palette.get(cat, '#78909C'))
                cat_text.append(f"{int(cat_subset['COUNT'].sum()):,} claims")
            for _, row in tree_df.iterrows():
                cat_labels.append(row['CLAIM_TYPE']); cat_parents.append(row['CLAIM_CATEGORY'])
                cat_values.append(int(row['COUNT']))
                cat_colors_list.append(cat_palette.get(row['CLAIM_CATEGORY'], '#78909C'))
                cat_text.append(f"{int(row['COUNT']):,} | ${row['TOTAL_PAID']/1e6:.1f}M")
            fig = go.Figure(go.Treemap(
                labels=cat_labels, parents=cat_parents, values=cat_values,
                text=cat_text, textinfo='label+text', textfont=dict(size=11),
                marker=dict(colors=cat_colors_list, line=dict(color='white', width=2), cornerradius=5),
                hovertemplate='<b>%{label}</b><br>Claims: %{value:,}<br>%{text}<extra></extra>',
                pathbar=dict(visible=True, thickness=24, textfont=dict(size=11))
            ))
            fig.update_layout(height=380, margin=dict(t=30, b=10, l=10, r=10), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True, key="summary_treemap_v2")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
    st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Regional Performance</h3>", unsafe_allow_html=True)
    reg_df = cached_query(f"""
        SELECT g.REGION, COUNT(*) AS CLAIMS, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID,
            ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID,
            SUM(CASE WHEN fc.CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
            SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD,
            ROUND(AVG(fc.DAYS_TO_CLOSE),1) AS AVG_CLOSE,
            SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED
        FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_GEOGRAPHY g ON fc.GEOGRAPHY_KEY=g.GEOGRAPHY_KEY
        GROUP BY g.REGION ORDER BY CLAIMS DESC
    """)
    if not reg_df.empty:
        region_colors = {'West': '#1565C0', 'South': '#E65100', 'Southeast': '#2E7D32', 'Northeast': '#6A1B9A', 'Midwest': '#00838F'}
        cols = st.columns(len(reg_df))
        for i, (_, r) in enumerate(reg_df.iterrows()):
            with cols[i]:
                rcolor = region_colors.get(r['REGION'], '#78909C')
                settled_r = int(r['SETTLED']); total_r = int(r['CLAIMS'])
                settle_pct = round(settled_r / total_r * 100, 1) if total_r > 0 else 0
                st.markdown(f"""<div style='background:linear-gradient(135deg,{rcolor}08,{rcolor}04);border:1px solid {rcolor}25;border-radius:12px;padding:16px;text-align:center'>
                    <div style='color:{rcolor};font-weight:800;font-size:1rem;margin-bottom:8px'>{r['REGION']}</div>
                    <div style='color:#263238;font-size:1.8rem;font-weight:800'>{int(r['CLAIMS']):,}</div>
                    <div style='color:#78909C;font-size:0.72rem;margin-bottom:10px'>claims</div>
                    <div style='background:#ECEFF1;border-radius:6px;height:6px;overflow:hidden;margin:0 8px'>
                        <div style='background:{rcolor};height:100%;width:{settle_pct}%;border-radius:6px'></div>
                    </div>
                    <div style='color:#78909C;font-size:0.68rem;margin-top:4px'>{settle_pct}% settled</div>
                </div>""", unsafe_allow_html=True)
                st.markdown(f"""<div style='margin-top:8px;font-size:0.78rem;color:#546E7A;line-height:1.8'>
                    <div style='display:flex;justify-content:space-between'><span>Paid</span><b style='color:#263238'>${r['TOTAL_PAID']/1e6:.1f}M</b></div>
                    <div style='display:flex;justify-content:space-between'><span>Open</span><b style='color:#EF5350'>{int(r['OPEN_CLAIMS']):,}</b></div>
                    <div style='display:flex;justify-content:space-between'><span>Fraud</span><b style='color:#C62828'>{int(r['FRAUD']):,}</b></div>
                    <div style='display:flex;justify-content:space-between'><span>Avg Close</span><b style='color:#E65100'>{r['AVG_CLOSE']}d</b></div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=reg_df['REGION'], y=reg_df['CLAIMS'], name='Total Claims',
            marker=dict(color=[region_colors.get(r, '#78909C') for r in reg_df['REGION']], cornerradius=6, line=dict(color='white', width=1.5), opacity=0.85),
            text=[f"<b>{c:,}</b>" for c in reg_df['CLAIMS']], textposition='outside', textfont=dict(size=10),
            yaxis='y', hovertemplate='<b>%{x}</b><br>Claims: %{y:,}<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=reg_df['REGION'], y=reg_df['TOTAL_PAID'], name='Total Paid ($)',
            mode='lines+markers+text', text=[f"${p/1e6:.1f}M" for p in reg_df['TOTAL_PAID']],
            textposition='top center', textfont=dict(size=9, color='#C62828'),
            line=dict(color='#C62828', width=3),
            marker=dict(size=10, color='#C62828', line=dict(color='white', width=2.5), symbol='diamond'),
            yaxis='y2', hovertemplate='<b>%{x}</b><br>Paid: $%{y:,.0f}<extra></extra>'
        ))
        fig.update_layout(
            height=300, margin=dict(t=10, b=40, l=60, r=60),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(title='Claim Count', gridcolor='#F5F5F5', zeroline=False),
            yaxis2=dict(title='Total Paid ($)', overlaying='y', side='right', showgrid=False, zeroline=False),
            legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
            bargap=0.35, hovermode='x unified', hoverlabel=dict(bgcolor='white', font_size=12)
        )
        st.plotly_chart(fig, use_container_width=True, key="summary_regional_bar_v2")
    st.markdown("</div>", unsafe_allow_html=True)


def render_analytics():
    vcare_img_tag = f"<img src='data:image/png;base64,{vcare_b64}' style='height:50px;opacity:0.9'>" if vcare_b64 else ""

    st.markdown("""<style>
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 10px; padding: 12px 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
        div[data-testid="stMetric"] label { font-size: 0.85rem !important; }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: 700; }
        button[data-baseweb="tab"] {
            background: transparent !important; border: none !important;
            border-bottom: 3px solid transparent !important;
            padding: 12px 20px !important; font-size: 0.9rem !important;
            font-weight: 600 !important; color: #78909C !important;
            transition: all 0.3s ease !important; border-radius: 8px 8px 0 0 !important;
        }
        button[data-baseweb="tab"]:hover {
            background: #E3F2FD !important; color: #1565C0 !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, #E3F2FD, #BBDEFB) !important;
            border-bottom: 3px solid #1565C0 !important; color: #0D47A1 !important;
            box-shadow: 0 2px 4px rgba(21,101,192,0.15) !important;
        }
        div[data-baseweb="tab-list"] {
            background: #FAFAFA !important; border-radius: 10px 10px 0 0 !important;
            padding: 4px 4px 0 4px !important; border-bottom: 2px solid #E0E0E0 !important; gap: 4px !important;
        }
        div[data-baseweb="tab-panel"] { padding-top: 16px !important; }
    </style>""", unsafe_allow_html=True)

    BLUE = ['#0D47A1','#1565C0','#1976D2','#1E88E5','#42A5F5','#64B5F6','#90CAF9','#BBDEFB']
    SEV_COLORS = {'Minor':'#4CAF50','Moderate':'#2196F3','Significant':'#FF9800','Severe':'#F44336','Catastrophic':'#9C27B0'}
    STATUS_COLORS = {'Open':'#42A5F5','Closed':'#66BB6A','Approved':'#26A69A','Pending Review':'#FFA726','Rejected':'#EF5350','Stalled':'#AB47BC'}
    FRAUD_COLORS = {'High Risk':'#F44336','Medium Risk':'#FF9800','No Flag':'#4CAF50'}

    kpi = cached_query(f"""
        SELECT COUNT(*) AS TOTAL_CLAIMS,
            SUM(CASE WHEN CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED,
            SUM(CASE WHEN CLAIM_STATUS IN ('Open','Pending Review','Stalled') THEN 1 ELSE 0 END) AS PENDING,
            COALESCE(SUM(PAID_AMOUNT),0) AS TOTAL_PAID,
            COALESCE(SUM(INCURRED_LOSS),0) AS TOTAL_INCURRED,
            COALESCE(SUM(RESERVE_AMOUNT),0) AS TOTAL_RESERVES,
            COALESCE(SUM(NET_INCURRED),0) AS NET_INCURRED,
            COALESCE(SUM(RECOVERY_AMOUNT+SUBROGATION_AMOUNT+SALVAGE_AMOUNT),0) AS TOTAL_RECOVERY,
            ROUND(AVG(PAID_AMOUNT),2) AS AVG_COST,
            ROUND(AVG(DAYS_TO_CLOSE),1) AS AVG_CLOSE_DAYS,
            SUM(CASE WHEN FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD_COUNT,
            SUM(CASE WHEN IS_WEATHER_RELATED THEN 1 ELSE 0 END) AS WEATHER_COUNT,
            SUM(CASE WHEN CAT_INDICATOR THEN 1 ELSE 0 END) AS CAT_COUNT
        FROM {FQ}.FACT_CLAIMS
    """)
    total = int(kpi['TOTAL_CLAIMS'].iloc[0])
    settled = int(kpi['SETTLED'].iloc[0])
    pending = int(kpi['PENDING'].iloc[0])
    total_paid = float(kpi['TOTAL_PAID'].iloc[0])
    total_incurred = float(kpi['TOTAL_INCURRED'].iloc[0])
    total_reserves = float(kpi['TOTAL_RESERVES'].iloc[0])
    total_recovery = float(kpi['TOTAL_RECOVERY'].iloc[0])
    net_incurred = float(kpi['NET_INCURRED'].iloc[0])
    avg_cost = float(kpi['AVG_COST'].iloc[0])
    avg_close = float(kpi['AVG_CLOSE_DAYS'].iloc[0])
    fraud_count = int(kpi['FRAUD_COUNT'].iloc[0])
    weather_count = int(kpi['WEATHER_COUNT'].iloc[0])
    settlement_ratio = round(settled/total*100,1) if total>0 else 0
    loss_ratio = round(total_paid/total_incurred*100,1) if total_incurred>0 else 0
    pending_ratio = round(pending/total*100,1) if total>0 else 0

    st.markdown(f"""<div style='background:linear-gradient(135deg,#0D47A1 0%,#1565C0 40%,#1E88E5 100%);border-radius:16px;padding:24px 28px;margin-bottom:16px;box-shadow:0 6px 24px rgba(13,71,161,0.2);position:relative;overflow:hidden'>
        <div style='position:absolute;top:-30px;right:-30px;width:160px;height:160px;background:rgba(255,255,255,0.05);border-radius:50%'></div>
        <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
                <h1 style='color:white;margin:0 0 4px 0;font-size:1.8rem;font-weight:800'>Analytics Dashboard</h1>
                <p style='color:#BBDEFB;margin:0;font-size:0.95rem'>Deep-dive analytics across <b style="color:#64B5F6">{total:,}</b> claims with <b style="color:#64B5F6">6</b> analysis dimensions</p>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    g1,g2,g3,g4 = st.columns(4)
    with g1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=settlement_ratio,
            title={'text': "<b>Settlement Rate</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#B0BEC5', 'dtick': 20},
                   'bar': {'color': '#1565C0', 'thickness': 0.25}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, 40], 'color': '#FFEBEE'}, {'range': [40, 70], 'color': '#FFF8E1'}, {'range': [70, 100], 'color': '#E8F5E9'}],
                   'threshold': {'line': {'color': '#2E7D32', 'width': 3}, 'thickness': 0.8, 'value': settlement_ratio}}
        ))
        fig.update_layout(height=170, margin=dict(t=45,b=5,l=25,r=25), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Settled **{settled:,}** of **{total:,}** total claims")
    with g2:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=loss_ratio,
            title={'text': "<b>Loss Ratio</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, 150], 'tickwidth': 1, 'tickcolor': '#B0BEC5', 'dtick': 30},
                   'bar': {'color': '#E65100', 'thickness': 0.25}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, 60], 'color': '#E8F5E9'}, {'range': [60, 90], 'color': '#FFF8E1'}, {'range': [90, 150], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#D32F2F', 'width': 3}, 'thickness': 0.8, 'value': loss_ratio}}
        ))
        fig.update_layout(height=170, margin=dict(t=45,b=5,l=25,r=25), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Paid **${total_paid/1e6:.1f}M** vs Incurred **${total_incurred/1e6:.1f}M**")
    with g3:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=pending_ratio,
            title={'text': "<b>Pending Claims</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'suffix': '%', 'font': {'size': 26, 'color': '#263238'}},
            gauge={'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#B0BEC5', 'dtick': 20},
                   'bar': {'color': '#FF8F00', 'thickness': 0.25}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, 20], 'color': '#E8F5E9'}, {'range': [20, 50], 'color': '#FFF8E1'}, {'range': [50, 100], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#E65100', 'width': 3}, 'thickness': 0.8, 'value': pending_ratio}}
        ))
        fig.update_layout(height=170, margin=dict(t=45,b=5,l=25,r=25), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"**{pending:,}** pending | **{fraud_count:,}** fraud flagged")
    with g4:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=avg_cost,
            title={'text': "<b>Avg Cost / Claim</b>", 'font': {'size': 13, 'color': '#37474F'}},
            number={'prefix': '$', 'font': {'size': 26, 'color': '#263238'}, 'valueformat': ',.0f'},
            gauge={'axis': {'range': [0, avg_cost*2], 'tickwidth': 1, 'tickcolor': '#B0BEC5'},
                   'bar': {'color': '#2E7D32', 'thickness': 0.25}, 'bgcolor': '#FAFAFA', 'borderwidth': 0,
                   'steps': [{'range': [0, avg_cost*0.7], 'color': '#E8F5E9'}, {'range': [avg_cost*0.7, avg_cost*1.3], 'color': '#FFF8E1'}, {'range': [avg_cost*1.3, avg_cost*2], 'color': '#FFEBEE'}],
                   'threshold': {'line': {'color': '#1B5E20', 'width': 3}, 'thickness': 0.8, 'value': avg_cost}}
        ))
        fig.update_layout(height=170, margin=dict(t=45,b=5,l=25,r=25), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Avg close **{avg_close} days** | Reserves **${total_reserves/1e6:.1f}M**")

    # st.divider()

    tab_overview,tab_trends,tab_fraud,tab_financial,tab_weather,tab_regional = st.tabs(
        [":material/dashboard: Overview",":material/trending_up: Trends",":material/gpp_bad: Fraud Detection",
         ":material/payments: Financial",":material/thunderstorm: Weather & CAT",":material/map: Regional"])

    with tab_overview:
        sc_col,pie_col = st.columns([3,2])
        with sc_col:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claim Scorecard</h3>", unsafe_allow_html=True)
            scorecard = cached_query(f"""
                WITH latest AS (SELECT MAX(d.YEAR_MONTH) AS CM FROM {FQ}.DIM_DATE d JOIN {FQ}.FACT_CLAIMS fc ON fc.DATE_KEY=d.DATE_KEY),
                curr AS (
                    SELECT COUNT(*) AS CLAIMS, SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED,
                        ROUND(SUM(fc.PAID_AMOUNT),0) AS PAID, ROUND(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES,
                        ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID, SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD
                    FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY WHERE d.YEAR_MONTH=(SELECT CM FROM latest)
                ),
                prev AS (
                    SELECT COUNT(*) AS CLAIMS, SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED,
                        ROUND(SUM(fc.PAID_AMOUNT),0) AS PAID, ROUND(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES,
                        ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID, SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD
                    FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
                    WHERE d.YEAR_MONTH=(SELECT TO_CHAR(DATEADD(MONTH,-1,TO_DATE(CM||'-01','YYYY-MM-DD')),'YYYY-MM') FROM latest)
                ),
                ytd AS (
                    SELECT COUNT(*) AS CLAIMS, SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED,
                        ROUND(SUM(fc.PAID_AMOUNT),0) AS PAID, ROUND(SUM(fc.RESERVE_AMOUNT),0) AS RESERVES,
                        ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID, SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD
                    FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
                    WHERE d.YEAR=(SELECT YEAR(TO_DATE(CM||'-01','YYYY-MM-DD')) FROM latest)
                )
                SELECT 'Claims Filed' AS KEY_METRIC,c.CLAIMS AS CURRENT_MONTH,p.CLAIMS AS LAST_MONTH,
                    CASE WHEN p.CLAIMS>0 THEN ROUND((c.CLAIMS-p.CLAIMS)::FLOAT/p.CLAIMS*100,1) ELSE 0 END AS VARIANCE_PCT,y.CLAIMS AS YTD_ACTUAL
                FROM curr c,prev p,ytd y
                UNION ALL SELECT 'Claims Settled',c.SETTLED,p.SETTLED,
                    CASE WHEN p.SETTLED>0 THEN ROUND((c.SETTLED-p.SETTLED)::FLOAT/p.SETTLED*100,1) ELSE 0 END,y.SETTLED FROM curr c,prev p,ytd y
                UNION ALL SELECT 'Total Paid ($)',c.PAID,p.PAID,
                    CASE WHEN p.PAID>0 THEN ROUND((c.PAID-p.PAID)/p.PAID*100,1) ELSE 0 END,y.PAID FROM curr c,prev p,ytd y
                UNION ALL SELECT 'Total Reserves ($)',c.RESERVES,p.RESERVES,
                    CASE WHEN p.RESERVES>0 THEN ROUND((c.RESERVES-p.RESERVES)/p.RESERVES*100,1) ELSE 0 END,y.RESERVES FROM curr c,prev p,ytd y
                UNION ALL SELECT 'Avg Claim Cost ($)',c.AVG_PAID,p.AVG_PAID,
                    CASE WHEN p.AVG_PAID>0 THEN ROUND((c.AVG_PAID-p.AVG_PAID)/p.AVG_PAID*100,1) ELSE 0 END,y.AVG_PAID FROM curr c,prev p,ytd y
                UNION ALL SELECT 'Fraud Flagged',c.FRAUD,p.FRAUD,
                    CASE WHEN p.FRAUD>0 THEN ROUND((c.FRAUD-p.FRAUD)::FLOAT/p.FRAUD*100,1) ELSE 0 END,y.FRAUD FROM curr c,prev p,ytd y
            """)
            if not scorecard.empty:
                st.dataframe(
                    scorecard.style.applymap(
                        lambda v: 'color: #4CAF50; font-weight: bold' if isinstance(v,(int,float)) and v>0 else ('color: #F44336; font-weight: bold' if isinstance(v,(int,float)) and v<0 else ''),
                        subset=['VARIANCE_PCT']
                    ), use_container_width=True, hide_index=True, height=260
                )
            st.markdown("</div>", unsafe_allow_html=True)

        with pie_col:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims by Business Line</h3>", unsafe_allow_html=True)
            cat_df = cached_query(f"""
                SELECT ct.CLAIM_CATEGORY,COUNT(*) AS COUNT, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_CLAIM_TYPE ct ON fc.CLAIM_TYPE_KEY=ct.CLAIM_TYPE_KEY
                GROUP BY ct.CLAIM_CATEGORY ORDER BY COUNT DESC
            """)
            if not cat_df.empty:
                fig = go.Figure(go.Pie(
                    labels=cat_df['CLAIM_CATEGORY'], values=cat_df['COUNT'], hole=0.5,
                    textinfo='label+percent+value', texttemplate='<b>%{label}</b><br>%{value:,}<br>%{percent}',
                    marker=dict(colors=['#1565C0','#42A5F5','#90CAF9'], line=dict(color='white', width=2)),
                    textfont=dict(size=11), pull=[0.03,0,0],
                    hovertemplate='<b>%{label}</b><br>Claims: %{value:,}<br>Share: %{percent}<extra></extra>'
                ))
                fig.add_annotation(text=f"<b>{total:,}</b><br>Total", x=0.5, y=0.5, font=dict(size=16, color='#263238'), showarrow=False)
                fig.update_layout(height=290, margin=dict(t=10,b=10,l=10,r=10), showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        col_s,col_sv = st.columns(2)
        with col_s:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims by Status</h3>", unsafe_allow_html=True)
            status_df = cached_query(f"SELECT CLAIM_STATUS,COUNT(*) AS COUNT FROM {FQ}.FACT_CLAIMS GROUP BY CLAIM_STATUS ORDER BY COUNT DESC")
            if not status_df.empty:
                fig = go.Figure(go.Pie(
                    labels=status_df['CLAIM_STATUS'], values=status_df['COUNT'], hole=0.55,
                    textinfo='label+value', texttemplate='<b>%{label}</b><br>%{value:,}',
                    marker=dict(colors=[STATUS_COLORS.get(s,'#78909C') for s in status_df['CLAIM_STATUS']], line=dict(color='white', width=2)),
                    hovertemplate='<b>%{label}</b><br>Claims: %{value:,}<br>Share: %{percent}<extra></extra>'
                ))
                fig.add_annotation(text=f"<b>{total:,}</b><br>Claims", x=0.5, y=0.5, font=dict(size=15, color='#263238'), showarrow=False)
                fig.update_layout(height=320, margin=dict(t=10,b=10,l=10,r=10), showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_sv:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Severity Distribution</h3>", unsafe_allow_html=True)
            sev_dist = cached_query(f"""
                SELECT CLAIM_SEVERITY,COUNT(*) AS COUNT, ROUND(COUNT(*)::FLOAT/SUM(COUNT(*)) OVER ()*100,1) AS PCT,
                    ROUND(AVG(PAID_AMOUNT),0) AS AVG_PAID
                FROM {FQ}.FACT_CLAIMS GROUP BY CLAIM_SEVERITY ORDER BY COUNT DESC
            """)
            if not sev_dist.empty:
                fig = go.Figure(go.Bar(
                    x=sev_dist['CLAIM_SEVERITY'], y=sev_dist['COUNT'], name='Count',
                    text=[f"<b>{r['COUNT']:,}</b><br>{r['PCT']}% | ${r['AVG_PAID']:,.0f} avg" for _,r in sev_dist.iterrows()],
                    textposition='outside', textfont=dict(size=10),
                    marker=dict(color=[SEV_COLORS.get(s,'#78909C') for s in sev_dist['CLAIM_SEVERITY']], line=dict(color='white', width=1), cornerradius=5)
                ))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', showlegend=False, yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with tab_trends:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claim Volume & Paid Amount — Last 12 Months</h3>", unsafe_allow_html=True)
        trend_df = cached_query(f"""
            SELECT d.YEAR_MONTH AS MONTH,COUNT(*) AS CLAIMS, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID,
                ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID,
                SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
            GROUP BY d.YEAR_MONTH ORDER BY d.YEAR_MONTH DESC LIMIT 12
        """)
        if not trend_df.empty:
            trend_df = trend_df.sort_values('MONTH')
            fig = go.Figure()
            fig.add_trace(go.Bar(x=trend_df['MONTH'], y=trend_df['TOTAL_PAID'], name='Total Paid ($)',
                marker=dict(color=trend_df['TOTAL_PAID'], colorscale=[[0,'#BBDEFB'],[0.5,'#42A5F5'],[1,'#0D47A1']], cornerradius=5),
                yaxis='y', hovertemplate='<b>%{x}</b><br>Paid: $%{y:,.0f}<extra></extra>'))
            fig.add_trace(go.Scatter(x=trend_df['MONTH'], y=trend_df['CLAIMS'], name='Claims Filed',
                mode='lines+markers+text', text=trend_df['CLAIMS'], textposition='top center',
                textfont=dict(size=9, color='#C62828'),
                line=dict(color='#C62828', width=3, shape='spline'),
                marker=dict(size=8, color='#C62828', line=dict(color='white', width=2)), yaxis='y2',
                hovertemplate='<b>%{x}</b><br>Claims: %{y}<extra></extra>'))
            fig.add_trace(go.Scatter(x=trend_df['MONTH'], y=trend_df['SETTLED'], name='Claims Settled',
                mode='lines+markers', line=dict(color='#2E7D32', width=2, dash='dot'),
                marker=dict(size=6, symbol='diamond', color='#2E7D32'), yaxis='y2',
                hovertemplate='<b>%{x}</b><br>Settled: %{y}<extra></extra>'))
            fig.update_layout(height=380, margin=dict(t=10,b=40,l=60,r=60),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title='Paid ($)', side='left', gridcolor='#F5F5F5', showgrid=True),
                yaxis2=dict(title='Claim Count', side='right', overlaying='y', showgrid=False),
                legend=dict(orientation='h', y=1.12, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
                bargap=0.35, hovermode='x unified', hoverlabel=dict(bgcolor='white', font_size=12))
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        left,right = st.columns(2)
        with left:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Avg Days to Close by Severity</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT CLAIM_SEVERITY,ROUND(AVG(DAYS_TO_CLOSE),1) AS AVG_DAYS,
                    ROUND(MIN(DAYS_TO_CLOSE),0) AS MIN_DAYS,ROUND(MAX(DAYS_TO_CLOSE),0) AS MAX_DAYS,COUNT(*) AS CLOSED_CLAIMS
                FROM {FQ}.FACT_CLAIMS WHERE DAYS_TO_CLOSE IS NOT NULL GROUP BY CLAIM_SEVERITY ORDER BY AVG_DAYS DESC
            """)
            if not df.empty:
                fig = go.Figure(go.Bar(x=df['CLAIM_SEVERITY'], y=df['AVG_DAYS'], name='Avg Days',
                    text=[f"<b>{d:.0f}</b> days" for d in df['AVG_DAYS']], textposition='outside',
                    marker=dict(color=[SEV_COLORS.get(s,'#78909C') for s in df['CLAIM_SEVERITY']], cornerradius=5, line=dict(color='white', width=1)),
                    error_y=dict(type='data', symmetric=False,
                                 array=(df['MAX_DAYS']-df['AVG_DAYS']).tolist(),
                                 arrayminus=(df['AVG_DAYS']-df['MIN_DAYS']).tolist(),
                                 color='#90A4AE', thickness=1.5, width=4),
                    customdata=list(zip(df['MIN_DAYS'],df['MAX_DAYS'],df['CLOSED_CLAIMS'])),
                    hovertemplate='<b>%{x}</b><br>Avg: %{y:.1f} days<br>Range: %{customdata[0]:.0f}-%{customdata[1]:.0f}<br>Closed: %{customdata[2]:,}<extra></extra>'))
                fig.update_layout(height=340, margin=dict(t=10,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', showlegend=False, yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Open vs Closed by Month</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT d.YEAR_MONTH AS MONTH,
                    SUM(CASE WHEN fc.CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_COUNT,
                    SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS CLOSED_COUNT
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
                GROUP BY d.YEAR_MONTH ORDER BY d.YEAR_MONTH DESC LIMIT 12
            """)
            if not df.empty:
                df = df.sort_values('MONTH')
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df['MONTH'], y=df['OPEN_COUNT'], name='Open',
                    mode='lines+markers', fill='tozeroy', line=dict(color='#EF5350', width=2, shape='spline'),
                    marker=dict(size=6, color='#EF5350', line=dict(color='white', width=1.5)), fillcolor='rgba(239,83,80,0.1)'))
                fig.add_trace(go.Scatter(x=df['MONTH'], y=df['CLOSED_COUNT'], name='Closed',
                    mode='lines+markers', fill='tozeroy', line=dict(color='#66BB6A', width=2, shape='spline'),
                    marker=dict(size=6, color='#66BB6A', line=dict(color='white', width=1.5)), fillcolor='rgba(102,187,106,0.1)'))
                fig.update_layout(height=340, margin=dict(t=10,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
                    yaxis=dict(gridcolor='#F5F5F5'), hovermode='x unified')
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Severity by Quarter</h3>", unsafe_allow_html=True)
        df = cached_query(f"""
            SELECT d.YEAR_QUARTER AS QUARTER,fc.CLAIM_SEVERITY,COUNT(*) AS CNT
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.DATE_KEY=d.DATE_KEY
            GROUP BY d.YEAR_QUARTER,fc.CLAIM_SEVERITY ORDER BY d.YEAR_QUARTER
        """)
        if not df.empty:
            sev_order = ['Minor','Moderate','Significant','Severe','Catastrophic']
            fig = go.Figure()
            for sev in sev_order:
                sdf = df[df['CLAIM_SEVERITY']==sev]
                if not sdf.empty:
                    fig.add_trace(go.Bar(x=sdf['QUARTER'], y=sdf['CNT'], name=sev,
                        marker=dict(color=SEV_COLORS.get(sev,'#78909C'), cornerradius=2),
                        hovertemplate=f'<b>{sev}</b><br>Quarter: %{{x}}<br>Claims: %{{y:,}}<extra></extra>'))
            fig.update_layout(height=350, barmode='stack', margin=dict(t=10,b=40,l=50,r=10),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
                yaxis=dict(title='Claims', gridcolor='#F5F5F5'))
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_fraud:
        fraud_kpi = cached_query(f"""
            SELECT COUNT(*) AS TOTAL, SUM(CASE WHEN FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD_TOTAL,
                SUM(CASE WHEN FRAUD_INDICATOR AND CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS FRAUD_OPEN,
                ROUND(AVG(CASE WHEN FRAUD_INDICATOR THEN FRAUD_SCORE END),1) AS AVG_FRAUD_SCORE,
                ROUND(SUM(CASE WHEN FRAUD_INDICATOR THEN PAID_AMOUNT ELSE 0 END),0) AS FRAUD_PAID
            FROM {FQ}.FACT_CLAIMS
        """)
        ft = int(fraud_kpi['FRAUD_TOTAL'].iloc[0])
        fraud_rate_val = round(ft/total*100,2) if total>0 else 0

        fr_items = [
            ("Fraud Flagged", f"{ft:,}", "#C62828"),
            ("Fraud Rate", f"{fraud_rate_val}%", "#E65100"),
            ("Open Fraud", f"{int(fraud_kpi['FRAUD_OPEN'].iloc[0]):,}", "#F44336"),
            ("Avg Fraud Score", f"{fraud_kpi['AVG_FRAUD_SCORE'].iloc[0]}", "#FF8F00"),
            ("Fraud Exposure", f"${float(fraud_kpi['FRAUD_PAID'].iloc[0]):,.0f}", "#9C27B0"),
        ]
        fr_html = "<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:0 0 16px 0'>"
        for label, value, color in fr_items:
            fr_html += f"""<div style='background:linear-gradient(135deg,{color}10,{color}05);border:1px solid {color}25;border-radius:12px;padding:14px;text-align:center'>
                <div style='color:{color};font-size:1.3rem;font-weight:800'>{value}</div>
                <div style='color:#546E7A;font-size:0.72rem;font-weight:600;margin-top:2px;text-transform:uppercase'>{label}</div>
            </div>"""
        fr_html += "</div>"
        st.markdown(fr_html, unsafe_allow_html=True)

        col_fp,col_ft = st.columns(2)
        with col_fp:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Fraud Risk Tier Breakdown</h3>", unsafe_allow_html=True)
            fraud_df = cached_query(f"""
                SELECT FRAUD_RISK_TIER,COUNT(*) AS COUNT,ROUND(AVG(FRAUD_SCORE),1) AS AVG_SCORE, ROUND(SUM(PAID_AMOUNT),0) AS TOTAL_PAID
                FROM {FQ}.FACT_CLAIMS GROUP BY FRAUD_RISK_TIER ORDER BY COUNT DESC
            """)
            if not fraud_df.empty:
                fig = go.Figure(go.Pie(
                    labels=fraud_df['FRAUD_RISK_TIER'], values=fraud_df['COUNT'], hole=0.55,
                    textinfo='label+value+percent', texttemplate='<b>%{label}</b><br>%{value:,} (%{percent})',
                    marker=dict(colors=[FRAUD_COLORS.get(t,'#78909C') for t in fraud_df['FRAUD_RISK_TIER']], line=dict(color='white', width=2)),
                    hovertemplate='<b>%{label}</b><br>Claims: %{value:,}<br>Share: %{percent}<extra></extra>'
                ))
                fig.add_annotation(text=f"<b>{total:,}</b><br>Total", x=0.5, y=0.5, font=dict(size=14, color='#263238'), showarrow=False)
                fig.update_layout(height=320, margin=dict(t=10,b=10,l=10,r=10), showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_ft:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Fraud Score Distribution</h3>", unsafe_allow_html=True)
            fscore = cached_query(f"""
                SELECT CASE WHEN FRAUD_SCORE<10 THEN '0-9' WHEN FRAUD_SCORE<20 THEN '10-19' WHEN FRAUD_SCORE<30 THEN '20-29'
                    WHEN FRAUD_SCORE<40 THEN '30-39' WHEN FRAUD_SCORE<50 THEN '40-49' WHEN FRAUD_SCORE<60 THEN '50-59'
                    WHEN FRAUD_SCORE<70 THEN '60-69' WHEN FRAUD_SCORE<80 THEN '70-79' WHEN FRAUD_SCORE<90 THEN '80-89'
                    ELSE '90-100' END AS SCORE_RANGE, COUNT(*) AS COUNT
                FROM {FQ}.FACT_CLAIMS GROUP BY SCORE_RANGE ORDER BY SCORE_RANGE
            """)
            if not fscore.empty:
                colors = ['#4CAF50']*3 + ['#FF9800']*3 + ['#F44336']*4
                fig = go.Figure(go.Bar(
                    x=fscore['SCORE_RANGE'], y=fscore['COUNT'],
                    marker=dict(color=colors[:len(fscore)], cornerradius=4, line=dict(color='white', width=1)),
                    text=fscore['COUNT'], textposition='outside', textfont=dict(size=9),
                    hovertemplate='Score: %{x}<br>Claims: %{y:,}<extra></extra>'
                ))
                fig.add_vline(x=2.5, line=dict(color='#FF9800', width=2, dash='dash'), annotation_text="Medium Risk", annotation_position="top")
                fig.add_vline(x=5.5, line=dict(color='#F44336', width=2, dash='dash'), annotation_text="High Risk", annotation_position="top")
                fig.update_layout(height=320, margin=dict(t=30,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    xaxis_title='Fraud Score Range', yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        col_fr,col_fl = st.columns(2)
        with col_fr:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Fraud by Region</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT g.REGION,COUNT(*) AS FRAUD_COUNT,
                    ROUND(COUNT(*)::FLOAT/(SELECT COUNT(*) FROM {FQ}.FACT_CLAIMS fc2
                        JOIN {FQ}.DIM_GEOGRAPHY g2 ON fc2.GEOGRAPHY_KEY=g2.GEOGRAPHY_KEY WHERE g2.REGION=g.REGION)*100,1) AS FRAUD_RATE
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_GEOGRAPHY g ON fc.GEOGRAPHY_KEY=g.GEOGRAPHY_KEY
                WHERE fc.FRAUD_INDICATOR=TRUE GROUP BY g.REGION ORDER BY FRAUD_COUNT DESC
            """)
            if not df.empty:
                fig = go.Figure(go.Bar(x=df['REGION'], y=df['FRAUD_COUNT'], name='Fraud Count',
                    marker=dict(color='#EF5350', cornerradius=5),
                    text=[f"<b>{c:,}</b><br>{r}%" for c,r in zip(df['FRAUD_COUNT'],df['FRAUD_RATE'])],
                    textposition='outside', textfont=dict(size=9),
                    hovertemplate='<b>%{x}</b><br>Fraud: %{y:,}<extra></extra>'))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    showlegend=False, yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_fl:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Fraud by Loss Cause</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT lc.LOSS_CAUSE,COUNT(*) AS FRAUD_COUNT,ROUND(AVG(fc.FRAUD_SCORE),1) AS AVG_SCORE
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_LOSS_CAUSE lc ON fc.LOSS_CAUSE_KEY=lc.LOSS_CAUSE_KEY
                WHERE fc.FRAUD_INDICATOR=TRUE GROUP BY lc.LOSS_CAUSE ORDER BY FRAUD_COUNT DESC LIMIT 10
            """)
            if not df.empty:
                fig = go.Figure(go.Bar(y=df['LOSS_CAUSE'], x=df['FRAUD_COUNT'], orientation='h',
                    marker=dict(color=df['AVG_SCORE'], colorscale=[[0,'#FFCDD2'],[0.5,'#EF9A9A'],[1,'#C62828']],
                                colorbar=dict(thickness=12, title=dict(text='Avg Score', side='right')), cornerradius=4),
                    text=[f"{c:,} (avg {s})" for c,s in zip(df['FRAUD_COUNT'],df['AVG_SCORE'])],
                    textposition='outside', textfont=dict(size=9),
                    hovertemplate='<b>%{y}</b><br>Fraud: %{x:,}<br>Avg Score: %{marker.color:.1f}<extra></extra>'))
                fig.update_layout(height=320, margin=dict(t=10,b=10,l=10,r=80), yaxis=dict(autorange='reversed'),
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with tab_financial:
        fi_items = [
            ("Total Paid", f"${total_paid/1e6:.1f}M", "#1565C0"),
            ("Total Reserves", f"${total_reserves/1e6:.1f}M", "#FF8F00"),
            ("Total Recovery", f"${total_recovery/1e6:.1f}M", "#2E7D32"),
            ("Net Incurred", f"${net_incurred/1e6:.1f}M", "#C62828"),
            ("Avg Cost/Claim", f"${avg_cost:,.0f}", "#6A1B9A"),
        ]
        fi_html = "<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:0 0 16px 0'>"
        for label, value, color in fi_items:
            fi_html += f"""<div style='background:linear-gradient(135deg,{color}10,{color}05);border:1px solid {color}25;border-radius:12px;padding:14px;text-align:center'>
                <div style='color:{color};font-size:1.3rem;font-weight:800'>{value}</div>
                <div style='color:#546E7A;font-size:0.72rem;font-weight:600;margin-top:2px;text-transform:uppercase'>{label}</div>
            </div>"""
        fi_html += "</div>"
        st.markdown(fi_html, unsafe_allow_html=True)

        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Financial Flow — Waterfall</h3>", unsafe_allow_html=True)
        rec_split = cached_query(f"""
            SELECT COALESCE(SUM(INCURRED_LOSS),0) AS IL,COALESCE(SUM(RECOVERY_AMOUNT),0) AS REC,
                COALESCE(SUM(SUBROGATION_AMOUNT),0) AS SUB,COALESCE(SUM(SALVAGE_AMOUNT),0) AS SAL,
                COALESCE(SUM(NET_INCURRED),0) AS NI
            FROM {FQ}.FACT_CLAIMS
        """)
        if not rec_split.empty:
            il=float(rec_split['IL'].iloc[0]);rec=float(rec_split['REC'].iloc[0])
            sub=float(rec_split['SUB'].iloc[0]);sal=float(rec_split['SAL'].iloc[0])
            ni=float(rec_split['NI'].iloc[0])
            fig = go.Figure(go.Waterfall(
                x=['Incurred Loss','Recovery','Subrogation','Salvage','Net Incurred'],
                y=[il,-rec,-sub,-sal,ni], measure=['absolute','relative','relative','relative','total'],
                text=[f"<b>${v/1e6:.1f}M</b>" for v in [il,rec,sub,sal,ni]], textposition='outside', textfont=dict(size=11),
                connector=dict(line=dict(color='#CFD8DC', width=1.5, dash='dot')),
                increasing=dict(marker=dict(color='#EF5350', line=dict(color='#C62828', width=1))),
                decreasing=dict(marker=dict(color='#66BB6A', line=dict(color='#2E7D32', width=1))),
                totals=dict(marker=dict(color='#1565C0', line=dict(color='#0D47A1', width=1)))
            ))
            fig.update_layout(height=350, margin=dict(t=20,b=40,l=60,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title='Amount ($)', gridcolor='#F5F5F5'), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        col_ra,col_rb = st.columns(2)
        with col_ra:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Paid vs Reserves by Severity</h3>", unsafe_allow_html=True)
            sev_df = cached_query(f"""
                SELECT CLAIM_SEVERITY,ROUND(AVG(PAID_AMOUNT),0) AS AVG_PAID, ROUND(AVG(RESERVE_AMOUNT),0) AS AVG_RESERVE
                FROM {FQ}.FACT_CLAIMS GROUP BY CLAIM_SEVERITY ORDER BY AVG_PAID DESC
            """)
            if not sev_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=sev_df['CLAIM_SEVERITY'], y=sev_df['AVG_PAID'], name='Avg Paid', marker=dict(color='#1565C0', cornerradius=4)))
                fig.add_trace(go.Bar(x=sev_df['CLAIM_SEVERITY'], y=sev_df['AVG_RESERVE'], name='Avg Reserve', marker=dict(color='#90CAF9', cornerradius=4)))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=40,r=10), barmode='group',
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'), yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_rb:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Actual vs Budgeted Expenses</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT fe.EXPENSE_CATEGORY,
                    COALESCE(SUM(fe.TOTAL_ACTUAL_EXPENSE),0) AS ACTUAL,
                    COALESCE(SUM(fe.BUDGETED_LEGAL_FEES+fe.BUDGETED_ADJUSTOR_COSTS+fe.BUDGETED_INVESTIGATION_CHARGES+fe.BUDGETED_ULAE),0) AS BUDGETED
                FROM {FQ}.FACT_CLAIM_EXPENSE fe GROUP BY fe.EXPENSE_CATEGORY ORDER BY ACTUAL DESC
            """)
            if not df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=df['EXPENSE_CATEGORY'], y=df['ACTUAL'], name='Actual', marker=dict(color='#EF5350', cornerradius=4)))
                fig.add_trace(go.Bar(x=df['EXPENSE_CATEGORY'], y=df['BUDGETED'], name='Budgeted', marker=dict(color='#90CAF9', cornerradius=4)))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=50,r=10), barmode='group',
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'), yaxis=dict(gridcolor='#F5F5F5'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Payment Trend</h3>", unsafe_allow_html=True)
        df = cached_query(f"""
            SELECT d.FIRST_DAY_OF_MONTH AS MONTH,COALESCE(SUM(fc.PAID_AMOUNT),0) AS MONTHLY_PAID
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_DATE d ON fc.PAYMENT_DATE=d.DATE_KEY
            WHERE fc.PAYMENT_DATE IS NOT NULL GROUP BY d.FIRST_DAY_OF_MONTH ORDER BY d.FIRST_DAY_OF_MONTH
        """)
        if not df.empty:
            fig = go.Figure(go.Scatter(x=df['MONTH'], y=df['MONTHLY_PAID'],
                mode='lines+markers', fill='tozeroy', line=dict(color='#1565C0', width=2, shape='spline'),
                marker=dict(size=4, color='#1565C0'), fillcolor='rgba(21,101,192,0.08)',
                hovertemplate='<b>%{x}</b><br>$%{y:,.0f}<extra></extra>'))
            fig.update_layout(height=300, margin=dict(t=10,b=40,l=60,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title='Paid ($)', gridcolor='#F5F5F5'), hovermode='x')
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_weather:
        wk = cached_query(f"SELECT COALESCE(SUM(CASE WHEN IS_WEATHER_RELATED THEN PAID_AMOUNT ELSE 0 END),0) AS WP FROM {FQ}.FACT_CLAIMS")
        w_items = [
            ("Weather Claims", f"{weather_count:,}", "#01579B"),
            ("CAT Claims", f"{int(kpi['CAT_COUNT'].iloc[0]):,}", "#6A1B9A"),
            ("Weather Paid", f"${float(wk['WP'].iloc[0])/1e6:.1f}M", "#E65100"),
            ("% Weather", f"{round(weather_count/total*100,1)}%", "#2E7D32"),
        ]
        w_html = "<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:0 0 16px 0'>"
        for label, value, color in w_items:
            w_html += f"""<div style='background:linear-gradient(135deg,{color}10,{color}05);border:1px solid {color}25;border-radius:12px;padding:14px;text-align:center'>
                <div style='color:{color};font-size:1.3rem;font-weight:800'>{value}</div>
                <div style='color:#546E7A;font-size:0.72rem;font-weight:600;margin-top:2px;text-transform:uppercase'>{label}</div>
            </div>"""
        w_html += "</div>"
        st.markdown(w_html, unsafe_allow_html=True)

        left,right = st.columns(2)
        with left:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims by Weather Condition</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT we.WEATHER_CONDITION,COUNT(*) AS COUNT,ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_WEATHER_EVENT we ON fc.WEATHER_EVENT_KEY=we.WEATHER_EVENT_KEY
                WHERE fc.IS_WEATHER_RELATED=TRUE GROUP BY we.WEATHER_CONDITION ORDER BY COUNT DESC
            """)
            if not df.empty:
                fig = go.Figure(go.Bar(x=df['WEATHER_CONDITION'], y=df['COUNT'],
                    text=[f"<b>{c:,}</b><br>${p/1e6:.1f}M" for c,p in zip(df['COUNT'],df['TOTAL_PAID'])],
                    textposition='outside', textfont=dict(size=9),
                    marker=dict(color=df['COUNT'], colorscale='Blues', cornerradius=5),
                    hovertemplate='<b>%{x}</b><br>Claims: %{y:,}<extra></extra>'))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=40,r=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    showlegend=False, yaxis=dict(gridcolor='#F5F5F5'), coloraxis_showscale=False)
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
            st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Event Severity Impact</h3>", unsafe_allow_html=True)
            df = cached_query(f"""
                SELECT we.EVENT_SEVERITY_TIER,COUNT(*) AS COUNT,ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
                FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_WEATHER_EVENT we ON fc.WEATHER_EVENT_KEY=we.WEATHER_EVENT_KEY
                WHERE fc.IS_WEATHER_RELATED=TRUE GROUP BY we.EVENT_SEVERITY_TIER ORDER BY COUNT DESC
            """)
            if not df.empty:
                sev_c = {'Catastrophic':'#9C27B0','Severe':'#F44336','Major':'#FF9800','Moderate':'#2196F3'}
                fig = go.Figure()
                fig.add_trace(go.Bar(x=df['EVENT_SEVERITY_TIER'], y=df['COUNT'], name='Claims',
                    marker=dict(color=[sev_c.get(s,'#78909C') for s in df['EVENT_SEVERITY_TIER']], cornerradius=5),
                    text=df['COUNT'], textposition='outside'))
                fig.add_trace(go.Scatter(x=df['EVENT_SEVERITY_TIER'], y=df['TOTAL_PAID'], name='Paid ($)',
                    mode='lines+markers', yaxis='y2', line=dict(color='#D32F2F', width=2),
                    marker=dict(size=10, symbol='diamond', color='#D32F2F')))
                fig.update_layout(height=320, margin=dict(t=10,b=40,l=40,r=50), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    yaxis=dict(title='Claims', gridcolor='#F5F5F5'), yaxis2=dict(title='Paid ($)', overlaying='y', side='right'),
                    legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'))
                st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Catastrophe Events Timeline</h3>", unsafe_allow_html=True)
        df = cached_query(f"""
            SELECT we.EVENT_NAME,we.WEATHER_CONDITION,we.EVENT_DATE,we.EVENT_SEVERITY_TIER,
                we.ESTIMATED_INDUSTRY_LOSS,COUNT(fc.CLAIM_KEY) AS IMPACTED_CLAIMS, ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID
            FROM {FQ}.DIM_WEATHER_EVENT we LEFT JOIN {FQ}.FACT_CLAIMS fc ON we.WEATHER_EVENT_KEY=fc.WEATHER_EVENT_KEY
            GROUP BY we.EVENT_NAME,we.WEATHER_CONDITION,we.EVENT_DATE,we.EVENT_SEVERITY_TIER,we.ESTIMATED_INDUSTRY_LOSS
            ORDER BY we.EVENT_DATE DESC
        """)
        if not df.empty:
            st.dataframe(
                df.style.background_gradient(subset=['IMPACTED_CLAIMS'], cmap='OrRd')
                    .background_gradient(subset=['TOTAL_PAID'], cmap='Blues')
                    .format({'ESTIMATED_INDUSTRY_LOSS':'${:,.0f}','TOTAL_PAID':'${:,.0f}','IMPACTED_CLAIMS':'{:,}'}),
                use_container_width=True, hide_index=True
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_regional:
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims by Region</h3>", unsafe_allow_html=True)
        region_df = cached_query(f"""
            SELECT g.REGION,COUNT(*) AS CLAIMS,ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID,
                ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID, SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_GEOGRAPHY g ON fc.GEOGRAPHY_KEY=g.GEOGRAPHY_KEY
            GROUP BY g.REGION ORDER BY CLAIMS DESC
        """)
        if not region_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=region_df['REGION'], y=region_df['CLAIMS'], name='Claims',
                marker=dict(color=BLUE[1], cornerradius=5), yaxis='y',
                text=[f"<b>{c:,}</b>" for c in region_df['CLAIMS']], textposition='outside'))
            fig.add_trace(go.Scatter(x=region_df['REGION'], y=region_df['TOTAL_PAID'], name='Total Paid ($)',
                mode='lines+markers', yaxis='y2', line=dict(color='#D32F2F', width=3),
                marker=dict(size=10, color='#D32F2F', line=dict(color='white', width=2))))
            fig.update_layout(height=350, margin=dict(t=10,b=40,l=60,r=60), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title='Claim Count', gridcolor='#F5F5F5'),
                yaxis2=dict(title='Total Paid ($)', overlaying='y', side='right'),
                legend=dict(orientation='h', y=1.1, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'), bargap=0.35)
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Claims by Claim Type</h3>", unsafe_allow_html=True)
        type_df = cached_query(f"""
            SELECT ct.CLAIM_TYPE,ct.CLAIM_CATEGORY,COUNT(*) AS CLAIMS,
                ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID,ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_CLAIM_TYPE ct ON fc.CLAIM_TYPE_KEY=ct.CLAIM_TYPE_KEY
            GROUP BY ct.CLAIM_TYPE,ct.CLAIM_CATEGORY ORDER BY CLAIMS DESC
        """)
        if not type_df.empty:
            cat_colors = {'Commercial Lines':'#1565C0','Personal Lines':'#2E7D32','Specialty Lines':'#E65100'}
            fig = go.Figure(go.Bar(y=type_df['CLAIM_TYPE'], x=type_df['CLAIMS'], orientation='h',
                marker=dict(color=[cat_colors.get(c,'#78909C') for c in type_df['CLAIM_CATEGORY']], cornerradius=4, line=dict(color='white', width=1)),
                text=[f"{c:,} | ${p/1e6:.1f}M paid" for c,p in zip(type_df['CLAIMS'],type_df['TOTAL_PAID'])],
                textposition='outside', textfont=dict(size=9),
                hovertemplate='<b>%{y}</b><br>Claims: %{x:,}<extra></extra>'))
            fig.update_layout(height=420, margin=dict(t=10,b=10,l=10,r=120), yaxis=dict(autorange='reversed'),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown("""<div style='background:var(--bg-card);border-radius:14px;padding:4px 0 0 0;box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid var(--border-card)'>""", unsafe_allow_html=True)
        st.markdown("<h3 style='margin:12px 0 0 16px;color:#263238;font-size:1.1rem'>Regional Performance Scorecard</h3>", unsafe_allow_html=True)
        perf_df = cached_query(f"""
            SELECT g.REGION,COUNT(*) AS TOTAL_CLAIMS,
                SUM(CASE WHEN fc.CLAIM_STATUS='Closed' THEN 1 ELSE 0 END) AS SETTLED,
                ROUND(SUM(fc.PAID_AMOUNT),0) AS TOTAL_PAID, ROUND(AVG(fc.PAID_AMOUNT),0) AS AVG_PAID,
                ROUND(AVG(fc.DAYS_TO_CLOSE),1) AS AVG_CLOSE_DAYS,
                SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END) AS FRAUD_COUNT,
                ROUND(SUM(CASE WHEN fc.FRAUD_INDICATOR THEN 1 ELSE 0 END)::FLOAT/COUNT(*)*100,1) AS FRAUD_RATE_PCT
            FROM {FQ}.FACT_CLAIMS fc JOIN {FQ}.DIM_GEOGRAPHY g ON fc.GEOGRAPHY_KEY=g.GEOGRAPHY_KEY
            GROUP BY g.REGION ORDER BY TOTAL_CLAIMS DESC
        """)
        if not perf_df.empty:
            st.dataframe(
                perf_df.style.background_gradient(subset=['TOTAL_CLAIMS'], cmap='Blues')
                    .background_gradient(subset=['FRAUD_RATE_PCT'], cmap='OrRd')
                    .background_gradient(subset=['AVG_CLOSE_DAYS'], cmap='YlOrRd')
                    .format({'TOTAL_PAID':'${:,.0f}','AVG_PAID':'${:,.0f}','FRAUD_RATE_PCT':'{:.1f}%'}),
                use_container_width=True, hide_index=True
            )
        st.markdown("</div>", unsafe_allow_html=True)

            
def render_chatbot():
    mode = st.session_state.chatbot_mode

    mode_gradient = "linear-gradient(135deg,#0D47A1 0%,#1565C0 30%,#6A1B9A 70%,#9C27B0 100%)" if mode == "Cortex Analyst" else "linear-gradient(135deg,#E65100 0%,#FF8F00 40%,#FFB300 100%)"
    mode_badge_color = "#9C27B0" if mode == "Cortex Analyst" else "#FF8F00"
    mode_label = "Cortex Analyst" if mode == "Cortex Analyst" else "Cortex Complete"
    mode_sub = f"Semantic View: <b style='color:#CE93D8'>{SV}</b> | Natural language to SQL" if mode == "Cortex Analyst" else f"Model: <b style='color:#FFE082'>{MODEL}</b> | Free-form chat with schema context"

    st.markdown(f"""<div style='background:{mode_gradient};border-radius:16px;padding:28px 32px;margin-bottom:16px;box-shadow:0 8px 32px rgba(106,27,154,0.3);position:relative;overflow:hidden'>
        <div style='position:absolute;top:-50px;right:-50px;width:220px;height:220px;background:rgba(255,255,255,0.04);border-radius:50%'></div>
        <div style='position:absolute;bottom:-70px;left:40%;width:300px;height:300px;background:rgba(255,255,255,0.03);border-radius:50%'></div>
        <div style='position:absolute;top:20px;right:200px;width:8px;height:8px;background:rgba(255,255,255,0.3);border-radius:50%'></div>
        <div style='position:absolute;top:60px;right:120px;width:5px;height:5px;background:rgba(255,255,255,0.2);border-radius:50%'></div>
        <div style='position:absolute;bottom:30px;left:30%;width:6px;height:6px;background:rgba(255,255,255,0.15);border-radius:50%'></div>
        <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
                <div style='display:flex;align-items:center;gap:12px;margin-bottom:8px'>
                    <div>
                        <h1 style='color:white;margin:0;font-size:1.8rem;font-weight:800;letter-spacing:-0.5px'>Claims Intelligence Chatbot</h1>
                    </div>
                </div>
                <p style='color:rgba(255,255,255,0.85);margin:0 0 12px 0;font-size:0.95rem'>Ask questions in plain English — get instant SQL queries and visual answers powered by <b style="color:white">Snowflake Cortex AI</b></p>
                <div style='display:flex;gap:10px;align-items:center'>
                    <span style='background:{mode_badge_color};color:white;padding:4px 14px;border-radius:20px;font-size:0.8rem;font-weight:700;letter-spacing:0.5px;box-shadow:0 2px 8px rgba(0,0,0,0.2)'>{mode_label}</span>
                    <span style='color:rgba(255,255,255,0.7);font-size:0.82rem'>{mode_sub}</span>
                </div>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    cap_items = []
    if mode == "Cortex Analyst":
        cap_items = [
            ("&#9889;", "NL to SQL", "Converts questions to precise SQL via semantic view"),
            ("&#128202;", "Auto Visualize", "Query results displayed as interactive tables"),
            ("&#128161;", "Suggestions", "AI suggests follow-up questions after each answer"),
            ("&#128274;", "Schema Aware", "Understands your star schema relationships"),
        ]
    else:
        cap_items = [
            ("&#129504;", "Free-form Chat", "Ask anything — explanations, comparisons, advice"),
            ("&#128187;", "SQL Generation", "Extracts and runs SQL from responses automatically"),
            ("&#128218;", "Schema Context", "Injected with full star schema knowledge"),
            ("&#128260;", "Multi-turn", "Chat history enables follow-up conversations"),
        ]

    caps_html = "<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px'>"
    for emoji, title, desc in cap_items:
        caps_html += f"""<div style='background:var(--bg-card);border-radius:12px;padding:14px;border:1px solid var(--border-card);box-shadow:0 2px 8px rgba(0,0,0,0.04);text-align:center;transition:transform 0.2s'>
            <div style='font-size:1.5rem;margin-bottom:4px'>{emoji}</div>
            <div style='color:#263238;font-weight:700;font-size:0.82rem'>{title}</div>
            <div style='color:#78909C;font-size:0.7rem;margin-top:2px'>{desc}</div>
        </div>"""
    caps_html += "</div>"
    st.markdown(caps_html, unsafe_allow_html=True)

    msg_count = len(st.session_state.messages)
    sql_count = sum(1 for m in st.session_state.messages if "sql" in m)
    if msg_count > 0:
        st.markdown(f"""<div style='display:flex;gap:16px;margin-bottom:12px'>
            <div style='background:#E3F2FD;border-radius:8px;padding:6px 14px;font-size:0.8rem'>
                <span style='color:#78909C'>Messages:</span> <b style='color:#1565C0'>{msg_count}</b>
            </div>
            <div style='background:#E8F5E9;border-radius:8px;padding:6px 14px;font-size:0.8rem'>
                <span style='color:#78909C'>SQL Queries:</span> <b style='color:#2E7D32'>{sql_count}</b>
            </div>
        </div>""", unsafe_allow_html=True)

    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                st.code(msg["sql"], language="sql")
            if "results" in msg and msg["results"] is not None:
                st.dataframe(msg["results"], use_container_width=True)

    last_msg = st.session_state.messages[-1] if st.session_state.messages else None
    if last_msg and "suggestions" in last_msg and last_msg["role"] == "assistant":
        st.markdown("""<div style='margin:8px 0 4px 0'><span style='color:#78909C;font-size:0.8rem;font-weight:600'>&#128161; Suggested follow-ups:</span></div>""", unsafe_allow_html=True)
        cols = st.columns(min(len(last_msg["suggestions"]), 3))
        for idx, s in enumerate(last_msg["suggestions"][:3]):
            with cols[idx]:
                if st.button(s, key=f"sug_{idx}", use_container_width=True):
                    st.session_state.pending_question = s
                    st.rerun()

    if msg_count == 0:
        st.markdown("""<div style='text-align:center;padding:40px 20px;margin:20px 0'>
            <div style='font-size:3rem;margin-bottom:12px'>&#129302;</div>
            <h3 style='color:#263238;margin:0 0 8px 0;font-weight:700'>Ready to analyze your claims data</h3>
            <p style='color:#78909C;margin:0 0 20px 0;font-size:0.9rem'>Ask a question below to get started. Try one of these:</p>
        </div>""", unsafe_allow_html=True)

        starter_questions = [
            "How many open claims do we have?",
            "Show total paid by claim type",
            "What is the fraud rate by region?",
            "Top 5 loss causes by claim count",
            "Average days to close by severity",
            "Monthly claim trend for last 12 months",
        ]
        cols = st.columns(3)
        for idx, q in enumerate(starter_questions):
            with cols[idx % 3]:
                if st.button(f"&#128172; {q}", key=f"starter_{idx}", use_container_width=True):
                    st.session_state.pending_question = q
                    st.rerun()

    pending = st.session_state.pop("pending_question", None)
    new_prompt = st.chat_input("Ask about your claims data...")
    prompt = new_prompt or pending

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("&#129504; Thinking..."):
                if mode == "Cortex Analyst" and HAS_SNOW_API:
                    history = st.session_state.messages[:-1] if st.session_state.chat_history_enabled else []
                    resp = call_cortex_analyst(prompt, history)
                    msg_data = {"role": "assistant", "content": ""}

                    if resp is None:
                        msg_data["content"] = "No response from Cortex Analyst. Try rephrasing."
                        st.warning(msg_data["content"])
                    elif isinstance(resp, str):
                        msg_data["content"] = resp
                        st.markdown(resp)
                    elif isinstance(resp, dict):
                        message = resp.get("message", resp)
                        if isinstance(message, str):
                            msg_data["content"] = message
                            st.markdown(message)
                        elif isinstance(message, dict):
                            content_blocks = message.get("content", [])
                            if isinstance(content_blocks, str):
                                msg_data["content"] = content_blocks
                                st.markdown(content_blocks)
                            elif isinstance(content_blocks, list):
                                for block in content_blocks:
                                    if not isinstance(block, dict):
                                        continue
                                    btype = block.get("type", "")
                                    if btype == "text":
                                        txt = block.get("text", "")
                                        st.markdown(txt)
                                        msg_data["content"] += txt + "\n"
                                    elif btype == "sql":
                                        sql = block.get("statement", "")
                                        msg_data["sql"] = sql
                                        try:
                                            result_df = session.sql(sql).to_pandas()
                                            msg_data["results"] = result_df

                                            summary_prompt = f"""You are an insurance claims data analyst. The user asked: "{prompt}"
The SQL query returned the following data (showing first 20 rows):
{result_df.head(20).to_string(index=False)}

Total rows returned: {len(result_df)}

Analyze this data and provide a clear, concise answer to the user's question.
- Lead with the key insight or direct answer
- Highlight important numbers with context
- Note any trends, outliers, or notable patterns
- Keep it conversational and under 150 words
- Do NOT show SQL or mention the query
- Use bullet points for multiple data points"""

                                            try:
                                                ai_summary = call_cortex_complete(summary_prompt)
                                                st.markdown(ai_summary)
                                                msg_data["content"] += ai_summary + "\n"
                                            except Exception:
                                                pass

                                            with st.expander("View data & SQL", expanded=False):
                                                st.code(sql, language="sql")
                                                st.dataframe(result_df, use_container_width=True)
                                        except Exception as e:
                                            st.error(f"Query error: {e}")
                                    elif btype == "suggestions":
                                        msg_data["suggestions"] = block.get("suggestions", [])
                        else:
                            msg_data["content"] = str(message)
                            st.markdown(str(message))
                    else:
                        msg_data["content"] = str(resp)
                        st.markdown(str(resp))

                    st.session_state.messages.append(msg_data)
                else:
                    full_prompt = SCHEMA_CONTEXT + "\n"
                    if st.session_state.chat_history_enabled:
                        for m in st.session_state.messages[-7:]:
                            full_prompt += f"{m['role'].upper()}: {m['content']}\n"
                    else:
                        full_prompt += f"USER: {prompt}\n"

                    response = call_cortex_complete(full_prompt)
                    st.markdown(response)
                    msg_data = {"role": "assistant", "content": response}

                    if "```sql" in response:
                        sql = response.split("```sql")[1].split("```")[0].strip()
                        msg_data["sql"] = sql
                        try:
                            result_df = session.sql(sql).to_pandas()
                            msg_data["results"] = result_df

                            summary_prompt = f"""You are an insurance claims data analyst. The user asked: "{prompt}"
The query returned this data (first 20 rows):
{result_df.head(20).to_string(index=False)}

Total rows: {len(result_df)}

Give a clear, concise analytical answer:
- Lead with the key finding
- Highlight important numbers
- Note trends or outliers
- Keep under 150 words, no SQL"""

                            try:
                                ai_summary = call_cortex_complete(summary_prompt)
                                st.markdown("---")
                                st.markdown(ai_summary)
                                msg_data["content"] += "\n\n" + ai_summary
                            except Exception:
                                pass

                            with st.expander("View data & SQL", expanded=False):
                                st.code(sql, language="sql")
                                st.dataframe(result_df, use_container_width=True)
                        except Exception as e:
                            st.error(f"Query error: {e}")

                    st.session_state.messages.append(msg_data)

    bc1, bc2 = st.columns([1, 5])
    with bc1:
        if st.button(":material/delete_sweep: Clear", use_container_width=True):
            st.session_state.messages = []
            st.rerun()


def generate_semantic_view_ddl(tgt_db, tgt_sch):
    t = f"{tgt_db}.{tgt_sch}"
    return f"""CREATE OR REPLACE SEMANTIC VIEW {t}.INSURANCE_CLAIMS_SV
    tables (
        {t}.DIM_CLAIM_TYPE primary key (CLAIM_TYPE_KEY) with synonyms=('line of business','LOB') comment='Claim type dimension by line of business',
        {t}.DIM_DATE primary key (DATE_KEY) with synonyms=('calendar','dates') comment='Date dimension with day, month, quarter, year grain',
        {t}.DIM_GEOGRAPHY primary key (GEOGRAPHY_KEY) with synonyms=('geography','location') comment='Geographic dimension with state, city, zip, and region',
        {t}.DIM_LOSS_CAUSE primary key (LOSS_CAUSE_KEY) with synonyms=('cause of loss','loss reason') comment='Loss cause with weather-related categorization',
        {t}.DIM_POLICY primary key (POLICY_KEY) with synonyms=('insurance policy','policy') comment='Policy details with coverage and premium',
        DIM_WEATHER as {t}.DIM_WEATHER_EVENT primary key (WEATHER_EVENT_KEY) with synonyms=('catastrophe','weather event') comment='Weather event / catastrophe dimension',
        FACT_CLAIMS as {t}.FACT_CLAIMS primary key (CLAIM_KEY) with synonyms=('claims','insurance claims') comment='Claims fact table with financial, fraud, weather, and litigation data',
        FACT_EXPENSE as {t}.FACT_CLAIM_EXPENSE primary key (EXPENSE_KEY) with synonyms=('claim expenses','expenses') comment='Claim expense fact with actual vs budgeted'
    )
    relationships (
        FACT_CLAIMS (DATE_KEY) references DIM_DATE (DATE_KEY),
        FACT_CLAIMS (GEOGRAPHY_KEY) references DIM_GEOGRAPHY (GEOGRAPHY_KEY),
        FACT_CLAIMS (CLAIM_TYPE_KEY) references DIM_CLAIM_TYPE (CLAIM_TYPE_KEY),
        FACT_CLAIMS (LOSS_CAUSE_KEY) references DIM_LOSS_CAUSE (LOSS_CAUSE_KEY),
        FACT_CLAIMS (WEATHER_EVENT_KEY) references DIM_WEATHER (WEATHER_EVENT_KEY),
        FACT_CLAIMS (POLICY_KEY) references DIM_POLICY (POLICY_KEY),
        FACT_EXPENSE (CLAIM_KEY) references FACT_CLAIMS (CLAIM_KEY),
        FACT_EXPENSE (DATE_KEY) references DIM_DATE (DATE_KEY),
        FACT_EXPENSE (GEOGRAPHY_KEY) references DIM_GEOGRAPHY (GEOGRAPHY_KEY),
        FACT_EXPENSE (CLAIM_TYPE_KEY) references DIM_CLAIM_TYPE (CLAIM_TYPE_KEY),
        FACT_EXPENSE (LOSS_CAUSE_KEY) references DIM_LOSS_CAUSE (LOSS_CAUSE_KEY),
        FACT_EXPENSE (WEATHER_EVENT_KEY) references DIM_WEATHER (WEATHER_EVENT_KEY),
        FACT_EXPENSE (POLICY_KEY) references DIM_POLICY (POLICY_KEY)
    )
    dimensions (
        DIM_CLAIM_TYPE.CLAIM_CATEGORY as dim_claim_type.CLAIM_CATEGORY with synonyms=('business category','claim group') comment='Claim category (Personal, Commercial, Specialty)',
        DIM_CLAIM_TYPE.CLAIM_TYPE as dim_claim_type.CLAIM_TYPE with synonyms=('insurance type','line of business') comment='Type of insurance claim / line of business',
        DIM_DATE.CALENDAR_MONTH as dim_date."MONTH" with synonyms=('month number') comment='Calendar month number (1-12)',
        DIM_DATE.CALENDAR_QUARTER as dim_date."QUARTER" with synonyms=('quarter') comment='Calendar quarter (1-4)',
        DIM_DATE.CALENDAR_YEAR as dim_date."YEAR" with synonyms=('year') comment='Calendar year (2020-2026)',
        DIM_DATE.MONTH_NAME_DIM as dim_date.MONTH_NAME with synonyms=('month','month name') comment='Month name abbreviation',
        DIM_DATE.YEAR_MONTH_PERIOD as dim_date.YEAR_MONTH with synonyms=('monthly period','year-month') comment='Year-month period (YYYY-MM)',
        DIM_DATE.YEAR_QUARTER_PERIOD as dim_date.YEAR_QUARTER with synonyms=('quarterly period','year-quarter') comment='Year-quarter period (YYYY-Q)',
        DIM_GEOGRAPHY.CITY as dim_geography.CITY with synonyms=('city name') comment='City name',
        DIM_GEOGRAPHY.REGION as dim_geography.REGION with synonyms=('area','geographic region') comment='Geographic region (West, South, Southeast, Northeast, Midwest)',
        DIM_GEOGRAPHY.STATE_NAME as dim_geography.STATE_NAME with synonyms=('state') comment='Full state name',
        DIM_LOSS_CAUSE.IS_WEATHER_RELATED as dim_loss_cause.IS_WEATHER_RELATED with synonyms=('weather related flag') comment='Whether the loss cause is weather related (true/false)',
        DIM_LOSS_CAUSE.LOSS_CATEGORY as dim_loss_cause.LOSS_CATEGORY with synonyms=('cause category','loss type') comment='Loss cause category (Weather-Related, Accident, Crime, etc.)',
        DIM_LOSS_CAUSE.LOSS_CAUSE as dim_loss_cause.LOSS_CAUSE with synonyms=('cause of loss','root cause') comment='Root cause of the insurance claim',
        DIM_LOSS_CAUSE.WEATHER_CONDITION_TYPE as dim_loss_cause.WEATHER_CONDITION_MAPPING with synonyms=('weather type') comment='Mapped weather condition for weather-related losses',
        DIM_POLICY.COVERAGE_TYPE as dim_policy.COVERAGE_TYPE with synonyms=('coverage','policy coverage') comment='Type of policy coverage',
        DIM_POLICY.POLICY_STATUS as dim_policy.POLICY_STATUS with synonyms=('policy state') comment='Current policy status',
        DIM_WEATHER.EVENT_NAME as dim_weather.EVENT_NAME with synonyms=('catastrophe name','weather event name') comment='Name of the catastrophe/weather event',
        DIM_WEATHER.EVENT_SEVERITY as dim_weather.EVENT_SEVERITY_TIER with synonyms=('event severity','weather severity') comment='Severity tier (Catastrophic, Severe, Major, Moderate)',
        DIM_WEATHER.PRIMARY_WEATHER_DRIVER_DIM as dim_weather.PRIMARY_WEATHER_DRIVER with synonyms=('weather driver') comment='Primary weather driver (Wind/Rain, Wind/Hail, etc.)',
        DIM_WEATHER.WEATHER_CONDITION as dim_weather.WEATHER_CONDITION with synonyms=('event type','weather category') comment='Type of weather event (Hurricane, Wildfire, Flood, etc.)',
        FACT_CLAIMS.CLAIM_SEVERITY as fact_claims.CLAIM_SEVERITY with synonyms=('severity','severity level') comment='Claim severity (Minor, Moderate, Significant, Severe, Catastrophic)',
        FACT_CLAIMS.CLAIM_STATUS as fact_claims.CLAIM_STATUS with synonyms=('status') comment='Claim status (Open, Closed, Approved, Pending Review, Rejected, Stalled)',
        FACT_CLAIMS.FRAUD_RISK_TIER as fact_claims.FRAUD_RISK_TIER with synonyms=('fraud risk','fraud tier') comment='Fraud risk classification (High Risk, Medium Risk, No Flag)',
        FACT_CLAIMS.PAYMENT_TYPE as fact_claims.PAYMENT_TYPE with synonyms=('payment method') comment='Type of claim payment',
        FACT_EXPENSE.EXPENSE_CATEGORY as fact_expense.EXPENSE_CATEGORY with synonyms=('expense type') comment='Category of the expense'
    )
    metrics (
        FACT_CLAIMS.AVG_CLAIM_AMOUNT as AVG(PAID_AMOUNT) with synonyms=('average claim','average paid') comment='Average paid amount per claim',
        FACT_CLAIMS.AVG_DAYS_TO_CLOSE as AVG(DAYS_TO_CLOSE) with synonyms=('average close time','settlement time') comment='Average days from open to close',
        FACT_CLAIMS.AVG_FRAUD_SCORE as AVG(FRAUD_SCORE) with synonyms=('average fraud score') comment='Average fraud risk score',
        FACT_CLAIMS.CATASTROPHE_CLAIM_COUNT as SUM(CASE WHEN fact_claims.CAT_INDICATOR THEN 1 ELSE 0 END) with synonyms=('cat claims count') comment='Count of catastrophe-linked claims',
        FACT_CLAIMS.CLAIM_COUNT as COUNT(CLAIM_KEY) with synonyms=('claims count','number of claims') comment='Total count of insurance claims',
        FACT_CLAIMS.FRAUD_CLAIM_COUNT as SUM(CASE WHEN fact_claims.FRAUD_INDICATOR THEN 1 ELSE 0 END) with synonyms=('fraud count','fraudulent claims') comment='Count of fraudulent claims',
        FACT_CLAIMS.RESERVE_FUND as SUM(RESERVE_AMOUNT) with synonyms=('reserve','reserves','total reserves') comment='Total reserve fund set aside for outstanding claims',
        FACT_CLAIMS.TOTAL_INCURRED_LOSS as SUM(INCURRED_LOSS) with synonyms=('incurred loss','total incurred') comment='Total incurred loss amount',
        FACT_CLAIMS.TOTAL_NET_INCURRED as SUM(NET_INCURRED) with synonyms=('net incurred','net loss') comment='Net incurred loss after all recoveries',
        FACT_CLAIMS.TOTAL_RECOVERY as SUM(RECOVERY_AMOUNT) with synonyms=('recoveries','total recovered') comment='Total recovery amount',
        FACT_CLAIMS.TOTAL_REVENUE as SUM(PAID_AMOUNT) with synonyms=('paid amount','revenue','total paid') comment='Total paid claim amount - primary revenue metric',
        FACT_CLAIMS.WEATHER_CLAIM_COUNT as SUM(CASE WHEN fact_claims.IS_WEATHER_RELATED THEN 1 ELSE 0 END) with synonyms=('weather claims count') comment='Count of weather-related claims',
        FACT_EXPENSE.AVG_EXPENSE_PER_CLAIM as AVG(TOTAL_ACTUAL_EXPENSE) with synonyms=('average expense') comment='Average actual expense per claim',
        FACT_EXPENSE.TOTAL_BUDGET_VARIANCE as SUM(TOTAL_EXPENSE_VARIANCE) with synonyms=('budget variance','expense variance') comment='Total variance between actual and budgeted expenses',
        FACT_EXPENSE.TOTAL_BUDGETED_EXPENSE as SUM(BUDGETED_LEGAL_FEES + BUDGETED_ADJUSTOR_COSTS + BUDGETED_INVESTIGATION_CHARGES + BUDGETED_ULAE) with synonyms=('budgeted expenses','total budget') comment='Total budgeted expense amount',
        FACT_EXPENSE.TOTAL_EXPENSES as SUM(TOTAL_ACTUAL_EXPENSE) with synonyms=('actual expenses','total actual expenses') comment='Total actual claim expenses (legal, adjustor, investigation, ULAE)'
    )
    comment='Insurance claims analytics semantic view for revenue, expense, loss cause, and fraud analytics'
    ai_sql_generation 'Round all monetary values to 2 decimal places. For fraud analysis use fraud_risk_tier dimension and avg_fraud_score metric. For weather impact use loss_category and weather_condition dimensions. When no date filter is specified, include all available dates.'"""


def render_transformations():
    st.markdown("""<style>
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 10px; padding: 12px 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
    </style>""", unsafe_allow_html=True)

    st.markdown(f"""<div style='background:linear-gradient(135deg,#0D47A1 0%,#1565C0 40%,#1E88E5 100%);border-radius:16px;padding:24px 28px;margin-bottom:16px;box-shadow:0 6px 24px rgba(13,71,161,0.2);position:relative;overflow:hidden'>
        <div style='position:absolute;top:-30px;right:-30px;width:160px;height:160px;background:rgba(255,255,255,0.05);border-radius:50%'></div>
        <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
                <h1 style='color:white;margin:0 0 4px 0;font-size:1.8rem;font-weight:800'>Transformation Pipeline</h1>
                <p style='color:#BBDEFB;margin:0;font-size:0.95rem'>Transform raw claims data into a <b style="color:#64B5F6">star schema</b> consumption layer with semantic view</p>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)
    
    if "transform_status" not in st.session_state:
        st.session_state.transform_status = {}

    tab_arch, tab_pipeline, tab_incremental = st.tabs([":material/schema: Architecture", ":material/build: Pipeline", ":material/sync: Incremental"])

    with tab_arch:
        st.subheader("Source Layer Requirements")
        st.markdown("The transformation expects these **8 source tables** in the raw layer:")

        st.markdown("""<div style='background:#F5F7FA;border-radius:12px;padding:20px;border:1px solid #E0E0E0;margin-bottom:16px'>
            <h4 style='color:#1565C0;margin-top:0'>Required Source Tables</h4>
            <table style='width:100%;border-collapse:collapse'>
                <tr style='background:#E3F2FD'>
                    <th style='padding:8px 12px;text-align:left;border-bottom:2px solid #90CAF9;color:#0D47A1'>Table</th>
                    <th style='padding:8px 12px;text-align:left;border-bottom:2px solid #90CAF9;color:#0D47A1'>Key Columns</th>
                    <th style='padding:8px 12px;text-align:left;border-bottom:2px solid #90CAF9;color:#0D47A1'>Purpose</th>
                </tr>
                <tr><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>CLAIMS</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>CLAIM_ID, LOB_ID, POLICY_ID, CATASTROPHE_ID, LOSS_CAUSE, CLAIM_STATE/CITY/ZIP, CLAIM_STATUS, CLAIM_SEVERITY, FRAUD_INDICATOR, FRAUD_SCORE, CAT_INDICATOR, LOSS_DATE, REPORTED_DATE, OPEN_DATE, CLOSE_DATE</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Core claims fact data</td></tr>
                <tr style='background:#FAFAFA'><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>FINANCIAL_DATA</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>CLAIM_ID, PAID_AMOUNT, RESERVE_AMOUNT, INCURRED_LOSS, RECOVERY_AMOUNT, SUBROGATION_AMOUNT, SALVAGE_AMOUNT, PAYMENT_TYPE, PAYMENT_DATE</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Financial details per claim</td></tr>
                <tr><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>LITIGATION</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>CLAIM_ID, SETTLEMENT_AMOUNT, DEFENSE_COSTS, LITIGATION_STATUS</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Litigation records</td></tr>
                <tr style='background:#FAFAFA'><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>POLICY</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>POLICY_ID, INSURED_ID, POLICY_NUMBER, COVERAGE_TYPE, PREMIUM, POLICY_STATUS, POLICY_EFFECTIVE/EXPIRATION_DATE</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Policy master data</td></tr>
                <tr><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>INSURED</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>INSURED_ID, FIRST_NAME, LAST_NAME</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Insured party details</td></tr>
                <tr style='background:#FAFAFA'><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>LINE_OF_BUSINESS</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>LOB_ID, LOB_NAME, LOB_LEAD, LOB_CATEGORY</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Claim type / LOB reference</td></tr>
                <tr><td style='padding:8px 12px;border-bottom:1px solid #ECEFF1'><b>CATASTROPHE</b></td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;font-size:0.85rem'>CATASTROPHE_ID, CATASTROPHE_CODE, EVENT_NAME, EVENT_TYPE, EVENT_DATE, END_DATE, IMPACTED_STATES, ESTIMATED_INDUSTRY_LOSS</td>
                    <td style='padding:8px 12px;border-bottom:1px solid #ECEFF1;color:#546E7A'>Weather/catastrophe events</td></tr>
                <tr style='background:#FAFAFA'><td style='padding:8px 12px'><b>CLAIMS_EXPENSE</b></td>
                    <td style='padding:8px 12px;font-size:0.85rem'>EXPENSE_ID, CLAIM_ID, EXPENSE_DATE, EXPENSE_CATEGORY, LEGAL_FEES, ADJUSTOR_COSTS, INVESTIGATION_CHARGES, ULAE</td>
                    <td style='padding:8px 12px;color:#546E7A'>Expense breakdown per claim</td></tr>
            </table>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Data Flow Architecture")

        source_labels = ['CLAIMS','FINANCIAL_DATA','LITIGATION','POLICY','INSURED','LINE_OF_BUSINESS','CATASTROPHE','CLAIMS_EXPENSE']
        target_labels = ['DIM_DATE','DIM_CLAIM_TYPE','DIM_LOSS_CAUSE','DIM_GEOGRAPHY','DIM_POLICY','DIM_WEATHER_EVENT','FACT_CLAIMS','FACT_CLAIM_EXPENSE','SEMANTIC_VIEW']
        all_labels = source_labels + target_labels

        src_idx = {n:i for i,n in enumerate(source_labels)}
        tgt_idx = {n:i+len(source_labels) for i,n in enumerate(target_labels)}

        sources = [src_idx['LINE_OF_BUSINESS'], src_idx['CATASTROPHE'], src_idx['CLAIMS'], src_idx['CLAIMS'],
                   src_idx['CLAIMS'], src_idx['POLICY'], src_idx['INSURED'], src_idx['CLAIMS'],
                   src_idx['FINANCIAL_DATA'], src_idx['LITIGATION'], src_idx['CLAIMS_EXPENSE'],
                   tgt_idx['FACT_CLAIMS'], tgt_idx['FACT_CLAIM_EXPENSE'],
                   tgt_idx['DIM_DATE'], tgt_idx['DIM_CLAIM_TYPE'], tgt_idx['DIM_LOSS_CAUSE'],
                   tgt_idx['DIM_GEOGRAPHY'], tgt_idx['DIM_POLICY'], tgt_idx['DIM_WEATHER_EVENT']]
        targets = [tgt_idx['DIM_CLAIM_TYPE'], tgt_idx['DIM_WEATHER_EVENT'], tgt_idx['DIM_GEOGRAPHY'],
                   tgt_idx['DIM_LOSS_CAUSE'], tgt_idx['FACT_CLAIMS'], tgt_idx['DIM_POLICY'],
                   tgt_idx['DIM_POLICY'], tgt_idx['FACT_CLAIMS'], tgt_idx['FACT_CLAIMS'],
                   tgt_idx['FACT_CLAIMS'], tgt_idx['FACT_CLAIM_EXPENSE'],
                   tgt_idx['SEMANTIC_VIEW'], tgt_idx['SEMANTIC_VIEW'],
                   tgt_idx['SEMANTIC_VIEW'], tgt_idx['SEMANTIC_VIEW'], tgt_idx['SEMANTIC_VIEW'],
                   tgt_idx['SEMANTIC_VIEW'], tgt_idx['SEMANTIC_VIEW'], tgt_idx['SEMANTIC_VIEW']]
        values = [1]*len(sources)

        node_colors = ['#FF9800']*len(source_labels) + ['#1565C0']*6 + ['#2E7D32']*2 + ['#9C27B0']

        fig = go.Figure(go.Sankey(
            arrangement='snap',
            node=dict(
                pad=15, thickness=20, line=dict(color='white', width=1),
                label=all_labels, color=node_colors,
                hovertemplate='<b>%{label}</b><extra></extra>'
            ),
            link=dict(
                source=sources, target=targets, value=values,
                color=['rgba(255,152,0,0.15)']*11 + ['rgba(46,125,50,0.15)']*2 + ['rgba(156,39,176,0.15)']*6
            )
        ))
        fig.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10),
            paper_bgcolor='rgba(0,0,0,0)', font=dict(size=11))
        st.plotly_chart(fig, use_container_width=True, key="arch_sankey")

        st.markdown("""<div style='display:flex;gap:20px;justify-content:center;margin-top:8px'>
            <span style='color:#FF9800;font-weight:600'>&#9632; Source Tables (Raw)</span>
            <span style='color:#1565C0;font-weight:600'>&#9632; Dimensions</span>
            <span style='color:#2E7D32;font-weight:600'>&#9632; Facts</span>
            <span style='color:#9C27B0;font-weight:600'>&#9632; Semantic View</span>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Star Schema ERD")

        st.markdown("""<div style='display:flex;flex-direction:column;gap:12px;padding:16px;background:#F5F7FA;border-radius:12px;border:1px solid #E0E0E0'>
            <div style='display:flex;justify-content:center;gap:16px;flex-wrap:wrap'>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_DATE</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: DATE_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>YEAR, QUARTER, MONTH<br>YEAR_MONTH, YEAR_QUARTER<br>MONTH_NAME</div>
                </div>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_GEOGRAPHY</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: GEOGRAPHY_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>STATE_CODE, STATE_NAME<br>REGION, CITY, ZIP</div>
                </div>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_CLAIM_TYPE</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: CLAIM_TYPE_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>CLAIM_TYPE<br>CLAIM_CATEGORY<br>CLAIM_TYPE_LEAD</div>
                </div>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_LOSS_CAUSE</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: LOSS_CAUSE_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>LOSS_CAUSE, LOSS_CATEGORY<br>IS_WEATHER_RELATED<br>WEATHER_CONDITION_MAPPING</div>
                </div>
            </div>
            <div style='display:flex;justify-content:center'>
                <div style='color:#90A4AE;font-size:1.5rem'>&#8595; &#8595; &#8595; &#8595; FK &#8595; &#8595; &#8595; &#8595;</div>
            </div>
            <div style='display:flex;justify-content:center;gap:20px;flex-wrap:wrap'>
                <div style='background:#E8F5E9;border:3px solid #2E7D32;border-radius:10px;padding:14px;min-width:280px;text-align:center'>
                    <div style='color:#2E7D32;font-weight:700;font-size:1rem'>FACT_CLAIMS</div>
                    <div style='color:#1B5E20;font-size:0.7rem;margin-top:4px'>PK: CLAIM_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem;margin-top:4px'>
                        FK: DATE_KEY, GEOGRAPHY_KEY, CLAIM_TYPE_KEY<br>
                        LOSS_CAUSE_KEY, WEATHER_EVENT_KEY, POLICY_KEY<br><br>
                        PAID_AMOUNT, RESERVE_AMOUNT, NET_INCURRED<br>
                        CLAIM_STATUS, CLAIM_SEVERITY, FRAUD_RISK_TIER<br>
                        FRAUD_SCORE, DAYS_TO_CLOSE, IS_WEATHER_RELATED
                    </div>
                </div>
                <div style='background:#E8F5E9;border:3px solid #2E7D32;border-radius:10px;padding:14px;min-width:280px;text-align:center'>
                    <div style='color:#2E7D32;font-weight:700;font-size:1rem'>FACT_CLAIM_EXPENSE</div>
                    <div style='color:#1B5E20;font-size:0.7rem;margin-top:4px'>PK: EXPENSE_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem;margin-top:4px'>
                        FK: CLAIM_KEY, DATE_KEY, GEOGRAPHY_KEY<br>
                        CLAIM_TYPE_KEY, LOSS_CAUSE_KEY, POLICY_KEY<br><br>
                        TOTAL_ACTUAL_EXPENSE, EXPENSE_CATEGORY<br>
                        BUDGETED vs ACTUAL (Legal, Adjustor, etc.)<br>
                        TOTAL_EXPENSE_VARIANCE
                    </div>
                </div>
            </div>
            <div style='display:flex;justify-content:center'>
                <div style='color:#90A4AE;font-size:1.5rem'>&#8593; &#8593; &#8593; &#8593; FK &#8593; &#8593; &#8593; &#8593;</div>
            </div>
            <div style='display:flex;justify-content:center;gap:16px;flex-wrap:wrap'>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_POLICY</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: POLICY_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>COVERAGE_TYPE, PREMIUM<br>POLICY_STATUS<br>INSURED_NAME</div>
                </div>
                <div style='background:#E3F2FD;border:2px solid #1565C0;border-radius:8px;padding:12px;min-width:150px;text-align:center'>
                    <div style='color:#1565C0;font-weight:700;font-size:0.9rem'>DIM_WEATHER_EVENT</div>
                    <div style='color:#0D47A1;font-size:0.7rem;margin-top:4px'>PK: WEATHER_EVENT_KEY</div>
                    <div style='color:#546E7A;font-size:0.7rem'>EVENT_NAME, WEATHER_COND<br>EVENT_SEVERITY_TIER<br>PRIMARY_WEATHER_DRIVER</div>
                </div>
            </div>
            <div style='text-align:center;margin-top:8px'>
                <div style='display:inline-block;background:#F3E5F5;border:2px solid #9C27B0;border-radius:8px;padding:10px 24px'>
                    <div style='color:#9C27B0;font-weight:700;font-size:0.9rem'>INSURANCE_CLAIMS_SV (Semantic View)</div>
                    <div style='color:#6A1B9A;font-size:0.7rem'>26 Dimensions + 16 Metrics + Relationships</div>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Source Validation")
        st.caption("Verify required source tables exist before running the pipeline")

        val_col1, val_col2 = st.columns(2)
        with val_col1:
            val_db = st.selectbox("Validation Database", all_db_list if 'all_db_list' in dir() else ["INSURANCE_CLAIM_DB"],
                index=0, key="val_db")
        with val_col2:
            try:
                val_sch_df = session.sql(f"""
                    SELECT SCHEMA_NAME AS NAME FROM {val_db}.INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME
                """).to_pandas()
                val_sch_list = val_sch_df['NAME'].tolist()
            except Exception:
                val_sch_list = ["CLAIMS_SCHEMA"]
            val_sch = st.selectbox("Validation Schema", val_sch_list,
                index=val_sch_list.index("CLAIMS_SCHEMA") if "CLAIMS_SCHEMA" in val_sch_list else 0,
                key="val_sch")

        required = ['CLAIMS','FINANCIAL_DATA','LITIGATION','POLICY','INSURED','LINE_OF_BUSINESS','CATASTROPHE','CLAIMS_EXPENSE']

        if st.button("Validate Source Schema", type="primary", use_container_width=True, key="validate_src"):
            try:
                existing = fresh_query(f"""
                    SELECT TABLE_NAME, ROW_COUNT
                    FROM {val_db}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{val_sch}' ORDER BY TABLE_NAME
                """)
                existing_names = existing['TABLE_NAME'].tolist() if not existing.empty else []

                cols = st.columns(4)
                for i, tbl in enumerate(required):
                    with cols[i % 4]:
                        found = tbl in existing_names
                        icon = ":material/check_circle:" if found else ":material/cancel:"
                        rows = ""
                        if found and not existing.empty:
                            r = existing[existing['TABLE_NAME'] == tbl]['ROW_COUNT'].values
                            rows = f" ({int(r[0]):,} rows)" if len(r) > 0 and r[0] else ""
                        color = "#4CAF50" if found else "#F44336"
                        st.markdown(f"{icon} **<span style='color:{color}'>{tbl}</span>**{rows}", unsafe_allow_html=True)

                missing = [t for t in required if t not in existing_names]
                if missing:
                    st.error(f"Missing tables: {', '.join(missing)}")
                else:
                    st.success("All 8 required source tables found!")
            except Exception as e:
                st.error(f"Could not validate: {e}")

    with tab_pipeline:
        st.subheader("Configuration")
        col_src, col_tgt = st.columns(2)

        try:
            all_db_df = session.sql("""
                SELECT DATABASE_NAME AS NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME
            """).to_pandas()
            all_db_list = all_db_df['NAME'].tolist()
        except Exception:
            all_db_list = ["INSURANCE_CLAIM_DB"]

        def get_schemas(db_name):
            try:
                sdf = session.sql(f"""
                    SELECT SCHEMA_NAME AS NAME FROM {db_name}.INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME
                """).to_pandas()
                return sdf['NAME'].tolist()
            except Exception:
                return ["CLAIMS_SCHEMA"]

        with col_src:
            st.markdown("**Source (Raw Layer)**")
            src_db = st.selectbox("Source Database", all_db_list,
                index=all_db_list.index("INSURANCE_CLAIM_DB") if "INSURANCE_CLAIM_DB" in all_db_list else 0,
                key="tf_src_db")
            src_sch_list = get_schemas(src_db)
            src_sch = st.selectbox("Source Schema", src_sch_list,
                index=src_sch_list.index("CLAIMS_SCHEMA") if "CLAIMS_SCHEMA" in src_sch_list else 0,
                key="tf_src_sch")

        with col_tgt:
            st.markdown("**Target (Consumption Layer)**")
            tgt_db = st.selectbox("Target Database", all_db_list,
                index=all_db_list.index("INSURANCE_CLAIM_DB") if "INSURANCE_CLAIM_DB" in all_db_list else 0,
                key="tf_tgt_db")
            tgt_sch = st.text_input("Target Schema Name", value="CONSUMPTION_LAYER", key="tf_tgt_sch")

        src = f"{src_db}.{src_sch}"
        tgt = f"{tgt_db}.{tgt_sch}"

        st.markdown(f"""<div style='background:linear-gradient(135deg,#E3F2FD,#BBDEFB);border-radius:10px;padding:14px 20px;
            border-left:4px solid #1565C0;margin:8px 0'>
            <span style='font-size:0.9rem'><b>Pipeline:</b> <code>{src}</code> → <code>{tgt}</code> + Semantic View</span>
        </div>""", unsafe_allow_html=True)

        st.divider()

        steps = [
            {"name":"Create Target Schema","icon":":material/database:","desc":"Creates the target consumption layer schema",
             "sql":f"CREATE SCHEMA IF NOT EXISTS {tgt}","validate":None},
            {"name":"DIM_DATE","icon":":material/calendar_month:","desc":"Generate date dimension (2019-2026)",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_DATE AS
WITH ds AS (SELECT DATEADD(DAY,SEQ4(),'2019-01-01')::DATE AS DATE_KEY FROM TABLE(GENERATOR(ROWCOUNT=>2557)))
SELECT DATE_KEY,YEAR(DATE_KEY) AS YEAR,QUARTER(DATE_KEY) AS QUARTER,MONTH(DATE_KEY) AS MONTH,
    TO_CHAR(DATE_KEY,'MON') AS MONTH_NAME,DAY(DATE_KEY) AS DAY_OF_MONTH,
    DAYOFWEEK(DATE_KEY) AS DAY_OF_WEEK,TO_CHAR(DATE_KEY,'DY') AS DAY_NAME,
    WEEKOFYEAR(DATE_KEY) AS WEEK_OF_YEAR,
    CASE WHEN DAYOFWEEK(DATE_KEY) IN (0,6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    TO_CHAR(DATE_KEY,'YYYY')||'-Q'||QUARTER(DATE_KEY) AS YEAR_QUARTER,
    TO_CHAR(DATE_KEY,'YYYY-MM') AS YEAR_MONTH,
    DATE_TRUNC('MONTH',DATE_KEY) AS FIRST_DAY_OF_MONTH,
    LAST_DAY(DATE_KEY) AS LAST_DAY_OF_MONTH,
    DATE_TRUNC('QUARTER',DATE_KEY) AS FIRST_DAY_OF_QUARTER FROM ds""",
             "validate":f"SELECT COUNT(*) AS ROWS,MIN(DATE_KEY) AS MIN_DATE,MAX(DATE_KEY) AS MAX_DATE FROM {tgt}.DIM_DATE"},
            {"name":"DIM_CLAIM_TYPE","icon":":material/category:","desc":"Claim type / LOB dimension",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_CLAIM_TYPE AS
SELECT LOB_ID AS CLAIM_TYPE_KEY,LOB_NAME AS CLAIM_TYPE,LOB_LEAD AS CLAIM_TYPE_LEAD,LOB_CATEGORY AS CLAIM_CATEGORY
FROM {src}.LINE_OF_BUSINESS ORDER BY LOB_ID""",
             "validate":f"SELECT COUNT(*) AS ROWS,COUNT(DISTINCT CLAIM_CATEGORY) AS CATEGORIES FROM {tgt}.DIM_CLAIM_TYPE"},
            {"name":"DIM_LOSS_CAUSE","icon":":material/warning:","desc":"Loss cause with weather categorization",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_LOSS_CAUSE AS
WITH loss_ref AS (SELECT * FROM (VALUES
    ('Cyber Attack','Professional/Cyber',FALSE,NULL),('Equipment Failure','Mechanical/Product',FALSE,NULL),
    ('Fire Damage','Weather-Related',TRUE,'Wildfire/Lightning'),('Hail Damage','Weather-Related',TRUE,'Severe Storm'),
    ('Medical Injury','Injury',FALSE,NULL),('Natural Disaster','Weather-Related',TRUE,'Multiple Weather Events'),
    ('Product Defect','Mechanical/Product',FALSE,NULL),('Professional Error','Professional/Cyber',FALSE,NULL),
    ('Property Theft','Crime',FALSE,NULL),('Slip and Fall','Accident',FALSE,NULL),
    ('Vandalism','Crime',FALSE,NULL),('Vehicle Collision','Accident',FALSE,NULL),
    ('Water Damage','Weather-Related',TRUE,'Flood/Rain'),('Wind Damage','Weather-Related',TRUE,'Hurricane/Tornado/Derecho'),
    ('Workplace Injury','Injury',FALSE,NULL)) AS t(LOSS_CAUSE,LOSS_CATEGORY,IS_WEATHER_RELATED,WEATHER_CONDITION_MAPPING))
SELECT ROW_NUMBER() OVER (ORDER BY r.LOSS_CAUSE) AS LOSS_CAUSE_KEY,r.LOSS_CAUSE,r.LOSS_CATEGORY,r.IS_WEATHER_RELATED,r.WEATHER_CONDITION_MAPPING
FROM loss_ref r WHERE r.LOSS_CAUSE IN (SELECT DISTINCT LOSS_CAUSE FROM {src}.CLAIMS)""",
             "validate":f"SELECT COUNT(*) AS ROWS,SUM(CASE WHEN IS_WEATHER_RELATED THEN 1 ELSE 0 END) AS WEATHER_CAUSES FROM {tgt}.DIM_LOSS_CAUSE"},
            {"name":"DIM_GEOGRAPHY","icon":":material/map:","desc":"Geography with state/region mapping",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_GEOGRAPHY AS
WITH state_ref AS (SELECT * FROM (VALUES
    ('AL','Alabama','South'),('AK','Alaska','West'),('AZ','Arizona','West'),('AR','Arkansas','South'),
    ('CA','California','West'),('CO','Colorado','West'),('CT','Connecticut','Northeast'),('DE','Delaware','South'),
    ('FL','Florida','Southeast'),('GA','Georgia','Southeast'),('HI','Hawaii','West'),('ID','Idaho','West'),
    ('IL','Illinois','Midwest'),('IN','Indiana','Midwest'),('IA','Iowa','Midwest'),('KS','Kansas','Midwest'),
    ('KY','Kentucky','South'),('LA','Louisiana','South'),('ME','Maine','Northeast'),('MD','Maryland','South'),
    ('MA','Massachusetts','Northeast'),('MI','Michigan','Midwest'),('MN','Minnesota','Midwest'),('MS','Mississippi','South'),
    ('MO','Missouri','Midwest'),('MT','Montana','West'),('NE','Nebraska','Midwest'),('NV','Nevada','West'),
    ('NH','New Hampshire','Northeast'),('NJ','New Jersey','Northeast'),('NM','New Mexico','West'),('NY','New York','Northeast'),
    ('NC','North Carolina','Southeast'),('ND','North Dakota','Midwest'),('OH','Ohio','Midwest'),('OK','Oklahoma','South'),
    ('OR','Oregon','West'),('PA','Pennsylvania','Northeast'),('RI','Rhode Island','Northeast'),('SC','South Carolina','Southeast'),
    ('SD','South Dakota','Midwest'),('TN','Tennessee','South'),('TX','Texas','South'),('UT','Utah','West'),
    ('VT','Vermont','Northeast'),('VA','Virginia','South'),('WA','Washington','West'),('WV','West Virginia','South'),
    ('WI','Wisconsin','Midwest'),('WY','Wyoming','West')) AS t(STATE_CODE,STATE_NAME,REGION)),
geo_raw AS (SELECT DISTINCT CLAIM_STATE,CLAIM_CITY,CLAIM_ZIP FROM {src}.CLAIMS WHERE CLAIM_STATE IS NOT NULL)
SELECT ROW_NUMBER() OVER (ORDER BY g.CLAIM_STATE,g.CLAIM_CITY,g.CLAIM_ZIP) AS GEOGRAPHY_KEY,
    g.CLAIM_STATE AS STATE_CODE,COALESCE(s.STATE_NAME,g.CLAIM_STATE) AS STATE_NAME,
    COALESCE(s.REGION,'Unknown') AS REGION,g.CLAIM_CITY AS CITY,g.CLAIM_ZIP AS ZIP_CODE
FROM geo_raw g LEFT JOIN state_ref s ON g.CLAIM_STATE=s.STATE_CODE""",
             "validate":f"SELECT COUNT(*) AS ROWS,COUNT(DISTINCT REGION) AS REGIONS,COUNT(DISTINCT STATE_CODE) AS STATES FROM {tgt}.DIM_GEOGRAPHY"},
            {"name":"DIM_POLICY","icon":":material/description:","desc":"Policy dimension (POLICY + INSURED)",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_POLICY AS
SELECT p.POLICY_ID AS POLICY_KEY,p.POLICY_NUMBER,p.INSURED_ID,
    i.FIRST_NAME||' '||i.LAST_NAME AS INSURED_NAME,
    p.POLICY_EFFECTIVE_DATE,p.POLICY_EXPIRATION_DATE,p.POLICY_TERM_MONTHS,
    p.COVERAGE_TYPE,p.COVERAGE_LIMIT,p.DEDUCTIBLE,p.PREMIUM,
    p.POLICY_STATE,p.POLICY_ZIP,p.AGENT_NAME,p.AGENCY_NAME,p.POLICY_STATUS,
    CASE WHEN p.POLICY_EXPIRATION_DATE>=CURRENT_DATE() THEN 'Active' ELSE 'Expired' END AS POLICY_ACTIVE_STATUS
FROM {src}.POLICY p LEFT JOIN {src}.INSURED i ON p.INSURED_ID=i.INSURED_ID""",
             "validate":f"SELECT COUNT(*) AS ROWS,COUNT(DISTINCT COVERAGE_TYPE) AS COVERAGE_TYPES FROM {tgt}.DIM_POLICY"},
            {"name":"DIM_WEATHER_EVENT","icon":":material/thunderstorm:","desc":"Weather/catastrophe dimension",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.DIM_WEATHER_EVENT AS
SELECT c.CATASTROPHE_ID AS WEATHER_EVENT_KEY,c.CATASTROPHE_CODE,c.EVENT_NAME,
    c.EVENT_TYPE AS WEATHER_CONDITION,c.EVENT_DATE,c.END_DATE,
    DATEDIFF(DAY,c.EVENT_DATE,COALESCE(c.END_DATE,c.EVENT_DATE))+1 AS EVENT_DURATION_DAYS,
    c.IMPACTED_STATES,c.ESTIMATED_INDUSTRY_LOSS,
    CASE WHEN c.EVENT_TYPE IN ('Hurricane') THEN 'Wind/Rain' WHEN c.EVENT_TYPE IN ('Wildfire') THEN 'Heat/Fire'
         WHEN c.EVENT_TYPE IN ('Winter Storm','Ice Storm') THEN 'Snow/Ice' WHEN c.EVENT_TYPE IN ('Tornado','Derecho') THEN 'Wind/Hail'
         WHEN c.EVENT_TYPE IN ('Flood') THEN 'Rain/Flood' WHEN c.EVENT_TYPE IN ('Severe Storm','Hailstorm') THEN 'Wind/Hail'
         ELSE 'Multiple' END AS PRIMARY_WEATHER_DRIVER,
    CASE WHEN c.ESTIMATED_INDUSTRY_LOSS>=25000000000 THEN 'Catastrophic' WHEN c.ESTIMATED_INDUSTRY_LOSS>=10000000000 THEN 'Severe'
         WHEN c.ESTIMATED_INDUSTRY_LOSS>=5000000000 THEN 'Major' ELSE 'Moderate' END AS EVENT_SEVERITY_TIER
FROM {src}.CATASTROPHE c""",
             "validate":f"SELECT COUNT(*) AS ROWS,COUNT(DISTINCT EVENT_SEVERITY_TIER) AS SEVERITY_TIERS FROM {tgt}.DIM_WEATHER_EVENT"},
            {"name":"FACT_CLAIMS","icon":":material/table_chart:","desc":"Claims fact (CLAIMS+FINANCIAL+LITIGATION+dims)",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.FACT_CLAIMS AS
SELECT c.CLAIM_ID AS CLAIM_KEY,c.CLAIM_NUMBER,c.LOSS_DATE AS DATE_KEY,
    g.GEOGRAPHY_KEY,c.LOB_ID AS CLAIM_TYPE_KEY,lc.LOSS_CAUSE_KEY,
    COALESCE(c.CATASTROPHE_ID,1) AS WEATHER_EVENT_KEY,c.POLICY_ID AS POLICY_KEY,
    c.CLAIM_STATUS,c.CLAIM_STAGE,c.CLAIM_SEVERITY,c.EXPOSURES,
    COALESCE(f.PAID_AMOUNT,0) AS PAID_AMOUNT,COALESCE(f.RESERVE_AMOUNT,0) AS RESERVE_AMOUNT,
    COALESCE(f.INCURRED_LOSS,0) AS INCURRED_LOSS,COALESCE(f.RECOVERY_AMOUNT,0) AS RECOVERY_AMOUNT,
    COALESCE(f.SUBROGATION_AMOUNT,0) AS SUBROGATION_AMOUNT,COALESCE(f.SALVAGE_AMOUNT,0) AS SALVAGE_AMOUNT,
    COALESCE(f.INCURRED_LOSS,0)-COALESCE(f.RECOVERY_AMOUNT,0)-COALESCE(f.SUBROGATION_AMOUNT,0)-COALESCE(f.SALVAGE_AMOUNT,0) AS NET_INCURRED,
    f.PAYMENT_TYPE,f.PAYMENT_DATE,c.FRAUD_INDICATOR,c.FRAUD_SCORE,
    CASE WHEN c.FRAUD_SCORE>=60 THEN 'High Risk' WHEN c.FRAUD_SCORE>=30 THEN 'Medium Risk' ELSE 'No Flag' END AS FRAUD_RISK_TIER,
    c.CAT_INDICATOR,c.LITIGATION_INDICATOR,c.SUBROGATION_INDICATOR,
    COALESCE(lc.IS_WEATHER_RELATED,FALSE) AS IS_WEATHER_RELATED,lc.WEATHER_CONDITION_MAPPING,
    DATEDIFF(DAY,c.LOSS_DATE,c.REPORTED_DATE) AS DAYS_TO_REPORT,
    DATEDIFF(DAY,c.LOSS_DATE,c.OPEN_DATE) AS DAYS_TO_OPEN,
    DATEDIFF(DAY,c.OPEN_DATE,c.CLOSE_DATE) AS DAYS_TO_CLOSE,
    c.LOSS_DATE,c.REPORTED_DATE,c.OPEN_DATE,c.CLOSE_DATE,c.LAST_ACTIVITY_DATE,
    COALESCE(l.SETTLEMENT_AMOUNT,0) AS LITIGATION_SETTLEMENT_AMOUNT,
    COALESCE(l.DEFENSE_COSTS,0) AS LITIGATION_DEFENSE_COSTS,l.LITIGATION_STATUS
FROM {src}.CLAIMS c LEFT JOIN {src}.FINANCIAL_DATA f ON c.CLAIM_ID=f.CLAIM_ID
LEFT JOIN {src}.LITIGATION l ON c.CLAIM_ID=l.CLAIM_ID
LEFT JOIN {tgt}.DIM_GEOGRAPHY g ON c.CLAIM_STATE=g.STATE_CODE AND c.CLAIM_CITY=g.CITY AND c.CLAIM_ZIP=g.ZIP_CODE
LEFT JOIN {tgt}.DIM_LOSS_CAUSE lc ON c.LOSS_CAUSE=lc.LOSS_CAUSE""",
             "validate":f"SELECT COUNT(*) AS ROWS,SUM(CASE WHEN CLAIM_STATUS='Open' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,ROUND(SUM(PAID_AMOUNT),2) AS TOTAL_PAID FROM {tgt}.FACT_CLAIMS"},
            {"name":"FACT_CLAIM_EXPENSE","icon":":material/payments:","desc":"Expense fact with actual vs budgeted",
             "sql":f"""CREATE OR REPLACE TABLE {tgt}.FACT_CLAIM_EXPENSE AS
WITH budget_ref AS (SELECT * FROM (VALUES
    ('Minor',2000,800,500,400),('Moderate',5000,1500,1500,800),('Significant',12000,3000,4000,1500),
    ('Severe',25000,5000,8000,2500),('Catastrophic',50000,8000,15000,4000)) AS t(SEVERITY,B_LEGAL,B_ADJ,B_INV,B_ULAE))
SELECT e.EXPENSE_ID AS EXPENSE_KEY,fc.CLAIM_KEY,fc.DATE_KEY,fc.GEOGRAPHY_KEY,
    fc.CLAIM_TYPE_KEY,fc.LOSS_CAUSE_KEY,fc.WEATHER_EVENT_KEY,fc.POLICY_KEY,
    e.EXPENSE_DATE,e.EXPENSE_CATEGORY,
    e.LEGAL_FEES AS ACTUAL_LEGAL_FEES,e.ADJUSTOR_COSTS AS ACTUAL_ADJUSTOR_COSTS,
    e.INVESTIGATION_CHARGES AS ACTUAL_INVESTIGATION_CHARGES,e.ULAE AS ACTUAL_ULAE,
    (e.LEGAL_FEES+e.ADJUSTOR_COSTS+e.INVESTIGATION_CHARGES+e.ULAE) AS TOTAL_ACTUAL_EXPENSE,
    COALESCE(b.B_LEGAL,5000) AS BUDGETED_LEGAL_FEES,COALESCE(b.B_ADJ,1500) AS BUDGETED_ADJUSTOR_COSTS,
    COALESCE(b.B_INV,1500) AS BUDGETED_INVESTIGATION_CHARGES,COALESCE(b.B_ULAE,800) AS BUDGETED_ULAE,
    e.LEGAL_FEES-COALESCE(b.B_LEGAL,5000) AS LEGAL_FEES_VARIANCE,
    (e.LEGAL_FEES+e.ADJUSTOR_COSTS+e.INVESTIGATION_CHARGES+e.ULAE)
        -(COALESCE(b.B_LEGAL,5000)+COALESCE(b.B_ADJ,1500)+COALESCE(b.B_INV,1500)+COALESCE(b.B_ULAE,800)) AS TOTAL_EXPENSE_VARIANCE,
    fc.CLAIM_SEVERITY,fc.FRAUD_INDICATOR,fc.IS_WEATHER_RELATED
FROM {src}.CLAIMS_EXPENSE e JOIN {tgt}.FACT_CLAIMS fc ON e.CLAIM_ID=fc.CLAIM_KEY
LEFT JOIN budget_ref b ON fc.CLAIM_SEVERITY=b.SEVERITY""",
             "validate":f"SELECT COUNT(*) AS ROWS,COUNT(DISTINCT EXPENSE_CATEGORY) AS CATEGORIES,ROUND(SUM(TOTAL_ACTUAL_EXPENSE),2) AS TOTAL_ACTUAL FROM {tgt}.FACT_CLAIM_EXPENSE"},
            {"name":"Semantic View","icon":":material/hub:","desc":"Create Cortex Analyst semantic view",
             "sql":generate_semantic_view_ddl(tgt_db,tgt_sch),
             "validate":f"SHOW SEMANTIC VIEWS IN SCHEMA {tgt}"},
        ]

        st.subheader("Pipeline Steps")

        completed = sum(1 for i in range(len(steps)) if st.session_state.transform_status.get(i) == 'success')
        failed = sum(1 for i in range(len(steps)) if st.session_state.transform_status.get(i) == 'error')
        pct = int(completed / len(steps) * 100)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Steps", len(steps))
        m2.metric("Completed", completed)
        m3.metric("Failed", failed)
        m4.metric("Progress", f"{pct}%")

        st.progress(pct / 100, text=f"{completed}/{len(steps)} steps completed")

        col_run, col_reset = st.columns([3, 1])
        with col_run:
            run_all = st.button("Run Full Pipeline", type="primary", use_container_width=True)
        with col_reset:
            if st.button("Reset Status", use_container_width=True):
                st.session_state.transform_status = {}
                st.rerun()

        if run_all:
            progress = st.progress(0, text="Starting pipeline...")
            for i, step in enumerate(steps):
                progress.progress(i / len(steps), text=f"Executing: {step['name']}...")
                try:
                    session.sql(step["sql"]).collect()
                    st.session_state.transform_status[i] = "success"
                except Exception as e:
                    st.session_state.transform_status[i] = "error"
                    st.error(f"Step {i+1} ({step['name']}) failed: {e}")
                    break
            else:
                progress.progress(1.0, text="Pipeline complete!")
                st.success(f"All {len(steps)} steps completed!")
                st.info(f"Semantic View: `{tgt}.INSURANCE_CLAIMS_SV`")

        st.divider()

        for i, step in enumerate(steps):
            status = st.session_state.transform_status.get(i, "pending")
            if status == "success":
                status_icon = ":material/check_circle:"
                border = "border-left:4px solid #4CAF50"
            elif status == "error":
                status_icon = ":material/error:"
                border = "border-left:4px solid #F44336"
            else:
                status_icon = ":material/radio_button_unchecked:"
                border = "border-left:4px solid #B0BEC5"

            with st.expander(f"{status_icon} Step {i+1}: {step['icon']} {step['name']} — {step['desc']}", expanded=False):
                st.code(step["sql"], language="sql")
                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("Execute", key=f"exec_{i}", use_container_width=True, type="primary"):
                        try:
                            with st.spinner(f"Running {step['name']}..."):
                                session.sql(step["sql"]).collect()
                            st.session_state.transform_status[i] = "success"
                            st.success(f"{step['name']} completed")
                            st.rerun()
                        except Exception as e:
                            st.session_state.transform_status[i] = "error"
                            st.error(f"Error: {e}")
                with c2:
                    if step["validate"]:
                        if st.button("Validate", key=f"val_{i}", use_container_width=True):
                            try:
                                val_df = session.sql(step["validate"]).to_pandas()
                                st.dataframe(val_df, use_container_width=True, hide_index=True)
                            except Exception as e:
                                st.warning(f"Validation failed: {e}")
                with c3:
                    if i >= 1 and step["validate"]:
                        if st.button("Preview", key=f"prev_{i}", use_container_width=True):
                            table_name = step["name"]
                            if table_name not in ("Create Target Schema", "Semantic View"):
                                try:
                                    prev_df = session.sql(f"SELECT * FROM {tgt}.{table_name} LIMIT 5").to_pandas()
                                    st.dataframe(prev_df, use_container_width=True, hide_index=True)
                                except Exception as e:
                                    st.warning(f"Preview failed: {e}")
                if status == "error":
                    st.error("This step failed. Fix the issue and re-execute.")

        st.divider()
        st.markdown(f"""<div style='background:linear-gradient(135deg,#E8F5E9,#C8E6C9);border-radius:10px;padding:16px 20px;border-left:4px solid #2E7D32'>
            <h4 style='color:#1B5E20;margin-top:0'>Post-Transform: Update App Settings</h4>
            <ol style='color:#2E7D32;margin-bottom:0'>
                <li>Go to <b>Settings</b> → set Database to <code>{tgt_db}</code>, Schema to <code>{tgt_sch}</code></li>
                <li>Set Semantic View to <code>{tgt}.INSURANCE_CLAIMS_SV</code></li>
                <li>Click <b>Apply Settings</b></li>
            </ol>
        </div>""", unsafe_allow_html=True)
    with tab_incremental:
        st.markdown(f"""<div style='background:linear-gradient(135deg,#E8F5E9,#C8E6C9);border-radius:14px;padding:20px 24px;margin-bottom:16px;border-left:4px solid #2E7D32'>
            <h3 style='color:#1B5E20;margin:0 0 6px 0'>Incremental Data Pipeline</h3>
            <p style='color:#2E7D32;margin:0;font-size:0.9rem'>Uses Snowflake <b>Streams</b> (CDC) + <b>Tasks</b> (scheduled MERGE) to incrementally refresh the star schema when source data changes.</p>
        </div>""", unsafe_allow_html=True)

        inc_col1, inc_col2 = st.columns(2)
        with inc_col1:
            inc_src_db = st.selectbox("Source Database", all_db_list if 'all_db_list' in dir() else ["INSURANCE_CLAIM_DB"],
                index=0, key="inc_src_db")
            inc_src_sch_list = get_schemas(inc_src_db) if 'get_schemas' in dir() else ["CLAIMS_SCHEMA"]
            inc_src_sch = st.selectbox("Source Schema", inc_src_sch_list,
                index=inc_src_sch_list.index("CLAIMS_SCHEMA") if "CLAIMS_SCHEMA" in inc_src_sch_list else 0,
                key="inc_src_sch")
        with inc_col2:
            inc_tgt_db = st.selectbox("Target Database", all_db_list if 'all_db_list' in dir() else ["INSURANCE_CLAIM_DB"],
                index=0, key="inc_tgt_db")
            inc_tgt_sch = st.text_input("Target Schema", value="CONSUMPTION_LAYER", key="inc_tgt_sch")

        inc_src = f"{inc_src_db}.{inc_src_sch}"
        inc_tgt = f"{inc_tgt_db}.{inc_tgt_sch}"

        try:
            wh_df = session.sql("SHOW WAREHOUSES").to_pandas()
            wh_col = [c for c in wh_df.columns if c.lower() == 'name'][0]
            wh_list = sorted(wh_df[wh_col].tolist())
        except Exception:
            wh_list = ["COMPUTE_WH"]

        inc_wh = st.selectbox("Warehouse for Tasks", wh_list, key="inc_wh")
        inc_schedule = st.selectbox("Schedule", ["5 MINUTE", "15 MINUTE", "30 MINUTE", "60 MINUTE", "USING CRON 0 */4 * * * UTC", "USING CRON 0 0 * * * UTC"], index=1, key="inc_sched")

        st.divider()

        stream_tables = ['CLAIMS', 'FINANCIAL_DATA', 'LITIGATION', 'POLICY', 'INSURED', 'LINE_OF_BUSINESS', 'CATASTROPHE', 'CLAIMS_EXPENSE']

        stream_sqls = []
        for tbl in stream_tables:
            stream_sqls.append({
                "name": f"{tbl}_STREAM",
                "sql": f"CREATE OR REPLACE STREAM {inc_src}.{tbl}_STREAM ON TABLE {inc_src}.{tbl} SHOW_INITIAL_ROWS = FALSE"
            })

        merge_fact_claims_sql = f"""CREATE OR REPLACE TASK {inc_tgt}.INCR_FACT_CLAIMS
  WAREHOUSE = {inc_wh}
  SCHEDULE = '{inc_schedule}'
  WHEN SYSTEM$STREAM_HAS_DATA('{inc_src}.CLAIMS_STREAM')
AS
MERGE INTO {inc_tgt}.FACT_CLAIMS tgt
USING (
    SELECT c.CLAIM_ID AS CLAIM_KEY, c.CLAIM_NUMBER, c.LOSS_DATE AS DATE_KEY,
        g.GEOGRAPHY_KEY, c.LOB_ID AS CLAIM_TYPE_KEY, lc.LOSS_CAUSE_KEY,
        COALESCE(c.CATASTROPHE_ID,1) AS WEATHER_EVENT_KEY, c.POLICY_ID AS POLICY_KEY,
        c.CLAIM_STATUS, c.CLAIM_STAGE, c.CLAIM_SEVERITY, c.EXPOSURES,
        COALESCE(f.PAID_AMOUNT,0) AS PAID_AMOUNT, COALESCE(f.RESERVE_AMOUNT,0) AS RESERVE_AMOUNT,
        COALESCE(f.INCURRED_LOSS,0) AS INCURRED_LOSS, COALESCE(f.RECOVERY_AMOUNT,0) AS RECOVERY_AMOUNT,
        COALESCE(f.SUBROGATION_AMOUNT,0) AS SUBROGATION_AMOUNT, COALESCE(f.SALVAGE_AMOUNT,0) AS SALVAGE_AMOUNT,
        COALESCE(f.INCURRED_LOSS,0)-COALESCE(f.RECOVERY_AMOUNT,0)-COALESCE(f.SUBROGATION_AMOUNT,0)-COALESCE(f.SALVAGE_AMOUNT,0) AS NET_INCURRED,
        f.PAYMENT_TYPE, f.PAYMENT_DATE, c.FRAUD_INDICATOR, c.FRAUD_SCORE,
        CASE WHEN c.FRAUD_SCORE>=60 THEN 'High Risk' WHEN c.FRAUD_SCORE>=30 THEN 'Medium Risk' ELSE 'No Flag' END AS FRAUD_RISK_TIER,
        c.CAT_INDICATOR, c.LITIGATION_INDICATOR, c.SUBROGATION_INDICATOR,
        COALESCE(lc.IS_WEATHER_RELATED,FALSE) AS IS_WEATHER_RELATED, lc.WEATHER_CONDITION_MAPPING,
        DATEDIFF(DAY,c.LOSS_DATE,c.REPORTED_DATE) AS DAYS_TO_REPORT,
        DATEDIFF(DAY,c.LOSS_DATE,c.OPEN_DATE) AS DAYS_TO_OPEN,
        DATEDIFF(DAY,c.OPEN_DATE,c.CLOSE_DATE) AS DAYS_TO_CLOSE,
        c.LOSS_DATE, c.REPORTED_DATE, c.OPEN_DATE, c.CLOSE_DATE, c.LAST_ACTIVITY_DATE,
        COALESCE(l.SETTLEMENT_AMOUNT,0) AS LITIGATION_SETTLEMENT_AMOUNT,
        COALESCE(l.DEFENSE_COSTS,0) AS LITIGATION_DEFENSE_COSTS, l.LITIGATION_STATUS
    FROM {inc_src}.CLAIMS_STREAM c
    LEFT JOIN {inc_src}.FINANCIAL_DATA f ON c.CLAIM_ID=f.CLAIM_ID
    LEFT JOIN {inc_src}.LITIGATION l ON c.CLAIM_ID=l.CLAIM_ID
    LEFT JOIN {inc_tgt}.DIM_GEOGRAPHY g ON c.CLAIM_STATE=g.STATE_CODE AND c.CLAIM_CITY=g.CITY AND c.CLAIM_ZIP=g.ZIP_CODE
    LEFT JOIN {inc_tgt}.DIM_LOSS_CAUSE lc ON c.LOSS_CAUSE=lc.LOSS_CAUSE
    WHERE c.METADATA$ACTION = 'INSERT'
) src
ON tgt.CLAIM_KEY = src.CLAIM_KEY
WHEN MATCHED THEN UPDATE SET
    tgt.CLAIM_STATUS=src.CLAIM_STATUS, tgt.CLAIM_STAGE=src.CLAIM_STAGE, tgt.CLAIM_SEVERITY=src.CLAIM_SEVERITY,
    tgt.PAID_AMOUNT=src.PAID_AMOUNT, tgt.RESERVE_AMOUNT=src.RESERVE_AMOUNT, tgt.INCURRED_LOSS=src.INCURRED_LOSS,
    tgt.RECOVERY_AMOUNT=src.RECOVERY_AMOUNT, tgt.SUBROGATION_AMOUNT=src.SUBROGATION_AMOUNT,
    tgt.SALVAGE_AMOUNT=src.SALVAGE_AMOUNT, tgt.NET_INCURRED=src.NET_INCURRED,
    tgt.PAYMENT_TYPE=src.PAYMENT_TYPE, tgt.PAYMENT_DATE=src.PAYMENT_DATE,
    tgt.FRAUD_INDICATOR=src.FRAUD_INDICATOR, tgt.FRAUD_SCORE=src.FRAUD_SCORE, tgt.FRAUD_RISK_TIER=src.FRAUD_RISK_TIER,
    tgt.DAYS_TO_CLOSE=src.DAYS_TO_CLOSE, tgt.CLOSE_DATE=src.CLOSE_DATE, tgt.LAST_ACTIVITY_DATE=src.LAST_ACTIVITY_DATE,
    tgt.LITIGATION_SETTLEMENT_AMOUNT=src.LITIGATION_SETTLEMENT_AMOUNT,
    tgt.LITIGATION_DEFENSE_COSTS=src.LITIGATION_DEFENSE_COSTS, tgt.LITIGATION_STATUS=src.LITIGATION_STATUS
WHEN NOT MATCHED THEN INSERT (
    CLAIM_KEY, CLAIM_NUMBER, DATE_KEY, GEOGRAPHY_KEY, CLAIM_TYPE_KEY, LOSS_CAUSE_KEY,
    WEATHER_EVENT_KEY, POLICY_KEY, CLAIM_STATUS, CLAIM_STAGE, CLAIM_SEVERITY, EXPOSURES,
    PAID_AMOUNT, RESERVE_AMOUNT, INCURRED_LOSS, RECOVERY_AMOUNT, SUBROGATION_AMOUNT,
    SALVAGE_AMOUNT, NET_INCURRED, PAYMENT_TYPE, PAYMENT_DATE, FRAUD_INDICATOR, FRAUD_SCORE,
    FRAUD_RISK_TIER, CAT_INDICATOR, LITIGATION_INDICATOR, SUBROGATION_INDICATOR,
    IS_WEATHER_RELATED, WEATHER_CONDITION_MAPPING, DAYS_TO_REPORT, DAYS_TO_OPEN, DAYS_TO_CLOSE,
    LOSS_DATE, REPORTED_DATE, OPEN_DATE, CLOSE_DATE, LAST_ACTIVITY_DATE,
    LITIGATION_SETTLEMENT_AMOUNT, LITIGATION_DEFENSE_COSTS, LITIGATION_STATUS
) VALUES (
    src.CLAIM_KEY, src.CLAIM_NUMBER, src.DATE_KEY, src.GEOGRAPHY_KEY, src.CLAIM_TYPE_KEY, src.LOSS_CAUSE_KEY,
    src.WEATHER_EVENT_KEY, src.POLICY_KEY, src.CLAIM_STATUS, src.CLAIM_STAGE, src.CLAIM_SEVERITY, src.EXPOSURES,
    src.PAID_AMOUNT, src.RESERVE_AMOUNT, src.INCURRED_LOSS, src.RECOVERY_AMOUNT, src.SUBROGATION_AMOUNT,
    src.SALVAGE_AMOUNT, src.NET_INCURRED, src.PAYMENT_TYPE, src.PAYMENT_DATE, src.FRAUD_INDICATOR, src.FRAUD_SCORE,
    src.FRAUD_RISK_TIER, src.CAT_INDICATOR, src.LITIGATION_INDICATOR, src.SUBROGATION_INDICATOR,
    src.IS_WEATHER_RELATED, src.WEATHER_CONDITION_MAPPING, src.DAYS_TO_REPORT, src.DAYS_TO_OPEN, src.DAYS_TO_CLOSE,
    src.LOSS_DATE, src.REPORTED_DATE, src.OPEN_DATE, src.CLOSE_DATE, src.LAST_ACTIVITY_DATE,
    src.LITIGATION_SETTLEMENT_AMOUNT, src.LITIGATION_DEFENSE_COSTS, src.LITIGATION_STATUS
)"""

        merge_fact_expense_sql = f"""CREATE OR REPLACE TASK {inc_tgt}.INCR_FACT_EXPENSE
  WAREHOUSE = {inc_wh}
  SCHEDULE = '{inc_schedule}'
  WHEN SYSTEM$STREAM_HAS_DATA('{inc_src}.CLAIMS_EXPENSE_STREAM')
AS
MERGE INTO {inc_tgt}.FACT_CLAIM_EXPENSE tgt
USING (
    SELECT e.EXPENSE_ID AS EXPENSE_KEY, fc.CLAIM_KEY, fc.DATE_KEY, fc.GEOGRAPHY_KEY,
        fc.CLAIM_TYPE_KEY, fc.LOSS_CAUSE_KEY, fc.WEATHER_EVENT_KEY, fc.POLICY_KEY,
        e.EXPENSE_DATE, e.EXPENSE_CATEGORY,
        e.LEGAL_FEES AS ACTUAL_LEGAL_FEES, e.ADJUSTOR_COSTS AS ACTUAL_ADJUSTOR_COSTS,
        e.INVESTIGATION_CHARGES AS ACTUAL_INVESTIGATION_CHARGES, e.ULAE AS ACTUAL_ULAE,
        (e.LEGAL_FEES+e.ADJUSTOR_COSTS+e.INVESTIGATION_CHARGES+e.ULAE) AS TOTAL_ACTUAL_EXPENSE,
        COALESCE(b.B_LEGAL,5000) AS BUDGETED_LEGAL_FEES, COALESCE(b.B_ADJ,1500) AS BUDGETED_ADJUSTOR_COSTS,
        COALESCE(b.B_INV,1500) AS BUDGETED_INVESTIGATION_CHARGES, COALESCE(b.B_ULAE,800) AS BUDGETED_ULAE,
        e.LEGAL_FEES-COALESCE(b.B_LEGAL,5000) AS LEGAL_FEES_VARIANCE,
        (e.LEGAL_FEES+e.ADJUSTOR_COSTS+e.INVESTIGATION_CHARGES+e.ULAE)
            -(COALESCE(b.B_LEGAL,5000)+COALESCE(b.B_ADJ,1500)+COALESCE(b.B_INV,1500)+COALESCE(b.B_ULAE,800)) AS TOTAL_EXPENSE_VARIANCE,
        fc.CLAIM_SEVERITY, fc.FRAUD_INDICATOR, fc.IS_WEATHER_RELATED
    FROM {inc_src}.CLAIMS_EXPENSE_STREAM e
    JOIN {inc_tgt}.FACT_CLAIMS fc ON e.CLAIM_ID=fc.CLAIM_KEY
    LEFT JOIN (SELECT * FROM (VALUES
        ('Minor',2000,800,500,400),('Moderate',5000,1500,1500,800),('Significant',12000,3000,4000,1500),
        ('Severe',25000,5000,8000,2500),('Catastrophic',50000,8000,15000,4000)) AS t(SEVERITY,B_LEGAL,B_ADJ,B_INV,B_ULAE)
    ) b ON fc.CLAIM_SEVERITY=b.SEVERITY
    WHERE e.METADATA$ACTION = 'INSERT'
) src
ON tgt.EXPENSE_KEY = src.EXPENSE_KEY
WHEN MATCHED THEN UPDATE SET
    tgt.EXPENSE_DATE=src.EXPENSE_DATE, tgt.EXPENSE_CATEGORY=src.EXPENSE_CATEGORY,
    tgt.ACTUAL_LEGAL_FEES=src.ACTUAL_LEGAL_FEES, tgt.ACTUAL_ADJUSTOR_COSTS=src.ACTUAL_ADJUSTOR_COSTS,
    tgt.ACTUAL_INVESTIGATION_CHARGES=src.ACTUAL_INVESTIGATION_CHARGES, tgt.ACTUAL_ULAE=src.ACTUAL_ULAE,
    tgt.TOTAL_ACTUAL_EXPENSE=src.TOTAL_ACTUAL_EXPENSE, tgt.TOTAL_EXPENSE_VARIANCE=src.TOTAL_EXPENSE_VARIANCE
WHEN NOT MATCHED THEN INSERT (
    EXPENSE_KEY, CLAIM_KEY, DATE_KEY, GEOGRAPHY_KEY, CLAIM_TYPE_KEY, LOSS_CAUSE_KEY,
    WEATHER_EVENT_KEY, POLICY_KEY, EXPENSE_DATE, EXPENSE_CATEGORY,
    ACTUAL_LEGAL_FEES, ACTUAL_ADJUSTOR_COSTS, ACTUAL_INVESTIGATION_CHARGES, ACTUAL_ULAE,
    TOTAL_ACTUAL_EXPENSE, BUDGETED_LEGAL_FEES, BUDGETED_ADJUSTOR_COSTS,
    BUDGETED_INVESTIGATION_CHARGES, BUDGETED_ULAE, LEGAL_FEES_VARIANCE,
    TOTAL_EXPENSE_VARIANCE, CLAIM_SEVERITY, FRAUD_INDICATOR, IS_WEATHER_RELATED
) VALUES (
    src.EXPENSE_KEY, src.CLAIM_KEY, src.DATE_KEY, src.GEOGRAPHY_KEY, src.CLAIM_TYPE_KEY, src.LOSS_CAUSE_KEY,
    src.WEATHER_EVENT_KEY, src.POLICY_KEY, src.EXPENSE_DATE, src.EXPENSE_CATEGORY,
    src.ACTUAL_LEGAL_FEES, src.ACTUAL_ADJUSTOR_COSTS, src.ACTUAL_INVESTIGATION_CHARGES, src.ACTUAL_ULAE,
    src.TOTAL_ACTUAL_EXPENSE, src.BUDGETED_LEGAL_FEES, src.BUDGETED_ADJUSTOR_COSTS,
    src.BUDGETED_INVESTIGATION_CHARGES, src.BUDGETED_ULAE, src.LEGAL_FEES_VARIANCE,
    src.TOTAL_EXPENSE_VARIANCE, src.CLAIM_SEVERITY, src.FRAUD_INDICATOR, src.IS_WEATHER_RELATED
)"""

        merge_dim_policy_sql = f"""CREATE OR REPLACE TASK {inc_tgt}.INCR_DIM_POLICY
  WAREHOUSE = {inc_wh}
  SCHEDULE = '{inc_schedule}'
  WHEN SYSTEM$STREAM_HAS_DATA('{inc_src}.POLICY_STREAM')
AS
MERGE INTO {inc_tgt}.DIM_POLICY tgt
USING (
    SELECT p.POLICY_ID AS POLICY_KEY, p.POLICY_NUMBER, p.INSURED_ID,
        COALESCE(i.FIRST_NAME,'')||' '||COALESCE(i.LAST_NAME,'') AS INSURED_NAME,
        p.POLICY_EFFECTIVE_DATE, p.POLICY_EXPIRATION_DATE, p.POLICY_TERM_MONTHS,
        p.COVERAGE_TYPE, p.COVERAGE_LIMIT, p.DEDUCTIBLE, p.PREMIUM,
        p.POLICY_STATE, p.POLICY_ZIP, p.AGENT_NAME, p.AGENCY_NAME, p.POLICY_STATUS,
        CASE WHEN p.POLICY_EXPIRATION_DATE>=CURRENT_DATE() THEN 'Active' ELSE 'Expired' END AS POLICY_ACTIVE_STATUS
    FROM {inc_src}.POLICY_STREAM p
    LEFT JOIN {inc_src}.INSURED i ON p.INSURED_ID=i.INSURED_ID
    WHERE p.METADATA$ACTION = 'INSERT'
) src
ON tgt.POLICY_KEY = src.POLICY_KEY
WHEN MATCHED THEN UPDATE SET
    tgt.POLICY_STATUS=src.POLICY_STATUS, tgt.POLICY_ACTIVE_STATUS=src.POLICY_ACTIVE_STATUS,
    tgt.PREMIUM=src.PREMIUM, tgt.COVERAGE_LIMIT=src.COVERAGE_LIMIT, tgt.DEDUCTIBLE=src.DEDUCTIBLE,
    tgt.INSURED_NAME=src.INSURED_NAME
WHEN NOT MATCHED THEN INSERT (
    POLICY_KEY, POLICY_NUMBER, INSURED_ID, INSURED_NAME, POLICY_EFFECTIVE_DATE, POLICY_EXPIRATION_DATE,
    POLICY_TERM_MONTHS, COVERAGE_TYPE, COVERAGE_LIMIT, DEDUCTIBLE, PREMIUM, POLICY_STATE, POLICY_ZIP,
    AGENT_NAME, AGENCY_NAME, POLICY_STATUS, POLICY_ACTIVE_STATUS
) VALUES (
    src.POLICY_KEY, src.POLICY_NUMBER, src.INSURED_ID, src.INSURED_NAME, src.POLICY_EFFECTIVE_DATE, src.POLICY_EXPIRATION_DATE,
    src.POLICY_TERM_MONTHS, src.COVERAGE_TYPE, src.COVERAGE_LIMIT, src.DEDUCTIBLE, src.PREMIUM, src.POLICY_STATE, src.POLICY_ZIP,
    src.AGENT_NAME, src.AGENCY_NAME, src.POLICY_STATUS, src.POLICY_ACTIVE_STATUS
)"""

        merge_dim_weather_sql = f"""CREATE OR REPLACE TASK {inc_tgt}.INCR_DIM_WEATHER
  WAREHOUSE = {inc_wh}
  SCHEDULE = '{inc_schedule}'
  WHEN SYSTEM$STREAM_HAS_DATA('{inc_src}.CATASTROPHE_STREAM')
AS
MERGE INTO {inc_tgt}.DIM_WEATHER_EVENT tgt
USING (
    SELECT c.CATASTROPHE_ID AS WEATHER_EVENT_KEY, c.CATASTROPHE_CODE, c.EVENT_NAME,
        c.EVENT_TYPE AS WEATHER_CONDITION, c.EVENT_DATE, c.END_DATE,
        DATEDIFF(DAY,c.EVENT_DATE,COALESCE(c.END_DATE,c.EVENT_DATE))+1 AS EVENT_DURATION_DAYS,
        c.IMPACTED_STATES, c.ESTIMATED_INDUSTRY_LOSS,
        CASE WHEN c.EVENT_TYPE IN ('Hurricane') THEN 'Wind/Rain' WHEN c.EVENT_TYPE IN ('Wildfire') THEN 'Heat/Fire'
             WHEN c.EVENT_TYPE IN ('Winter Storm','Ice Storm') THEN 'Snow/Ice' WHEN c.EVENT_TYPE IN ('Tornado','Derecho') THEN 'Wind/Hail'
             WHEN c.EVENT_TYPE IN ('Flood') THEN 'Rain/Flood' WHEN c.EVENT_TYPE IN ('Severe Storm','Hailstorm') THEN 'Wind/Hail'
             ELSE 'Multiple' END AS PRIMARY_WEATHER_DRIVER,
        CASE WHEN c.ESTIMATED_INDUSTRY_LOSS>=25000000000 THEN 'Catastrophic' WHEN c.ESTIMATED_INDUSTRY_LOSS>=10000000000 THEN 'Severe'
             WHEN c.ESTIMATED_INDUSTRY_LOSS>=5000000000 THEN 'Major' ELSE 'Moderate' END AS EVENT_SEVERITY_TIER
    FROM {inc_src}.CATASTROPHE_STREAM c
    WHERE c.METADATA$ACTION = 'INSERT'
) src
ON tgt.WEATHER_EVENT_KEY = src.WEATHER_EVENT_KEY
WHEN MATCHED THEN UPDATE SET
    tgt.END_DATE=src.END_DATE, tgt.EVENT_DURATION_DAYS=src.EVENT_DURATION_DAYS,
    tgt.IMPACTED_STATES=src.IMPACTED_STATES, tgt.ESTIMATED_INDUSTRY_LOSS=src.ESTIMATED_INDUSTRY_LOSS,
    tgt.EVENT_SEVERITY_TIER=src.EVENT_SEVERITY_TIER
WHEN NOT MATCHED THEN INSERT (
    WEATHER_EVENT_KEY, CATASTROPHE_CODE, EVENT_NAME, WEATHER_CONDITION, EVENT_DATE, END_DATE,
    EVENT_DURATION_DAYS, IMPACTED_STATES, ESTIMATED_INDUSTRY_LOSS, PRIMARY_WEATHER_DRIVER, EVENT_SEVERITY_TIER
) VALUES (
    src.WEATHER_EVENT_KEY, src.CATASTROPHE_CODE, src.EVENT_NAME, src.WEATHER_CONDITION, src.EVENT_DATE, src.END_DATE,
    src.EVENT_DURATION_DAYS, src.IMPACTED_STATES, src.ESTIMATED_INDUSTRY_LOSS, src.PRIMARY_WEATHER_DRIVER, src.EVENT_SEVERITY_TIER
)"""

        resume_tasks_sql = f"""ALTER TASK {inc_tgt}.INCR_DIM_POLICY RESUME;
ALTER TASK {inc_tgt}.INCR_DIM_WEATHER RESUME;
ALTER TASK {inc_tgt}.INCR_FACT_CLAIMS RESUME;
ALTER TASK {inc_tgt}.INCR_FACT_EXPENSE RESUME"""

        suspend_tasks_sql = f"""ALTER TASK {inc_tgt}.INCR_FACT_EXPENSE SUSPEND;
ALTER TASK {inc_tgt}.INCR_FACT_CLAIMS SUSPEND;
ALTER TASK {inc_tgt}.INCR_DIM_WEATHER SUSPEND;
ALTER TASK {inc_tgt}.INCR_DIM_POLICY SUSPEND"""

        inc_steps = [
            {"name": "Create Streams", "icon": ":material/stream:", "desc": f"Create CDC streams on all 8 source tables in {inc_src}",
             "sqls": [s["sql"] for s in stream_sqls], "label": "streams"},
            {"name": "MERGE DIM_POLICY", "icon": ":material/description:", "desc": "Incremental upsert for policy dimension",
             "sqls": [merge_dim_policy_sql], "label": "dim_policy"},
            {"name": "MERGE DIM_WEATHER", "icon": ":material/thunderstorm:", "desc": "Incremental upsert for weather/catastrophe dimension",
             "sqls": [merge_dim_weather_sql], "label": "dim_weather"},
            {"name": "MERGE FACT_CLAIMS", "icon": ":material/table_chart:", "desc": "Incremental MERGE for claims fact table",
             "sqls": [merge_fact_claims_sql], "label": "fact_claims"},
            {"name": "MERGE FACT_EXPENSE", "icon": ":material/payments:", "desc": "Incremental MERGE for expense fact table",
             "sqls": [merge_fact_expense_sql], "label": "fact_expense"},
        ]

        st.subheader("Setup Incremental Pipeline")

        setup_col1, setup_col2 = st.columns([3, 1])
        with setup_col1:
            if st.button("Deploy Full Incremental Pipeline", type="primary", use_container_width=True, key="deploy_incr"):
                progress = st.progress(0, text="Deploying streams and tasks...")
                all_sqls = []
                for step in inc_steps:
                    all_sqls.extend(step["sqls"])
                total = len(all_sqls)
                errors = []
                for idx, sql in enumerate(all_sqls):
                    progress.progress((idx+1)/total, text=f"Executing {idx+1}/{total}...")
                    try:
                        session.sql(sql).collect()
                    except Exception as e:
                        errors.append(f"Step {idx+1}: {e}")
                if errors:
                    st.error(f"Completed with {len(errors)} error(s):")
                    for err in errors:
                        st.warning(err)
                else:
                    progress.progress(1.0, text="All streams and tasks created!")
                    st.success(f"Created 8 streams + 4 MERGE tasks in {inc_tgt}")
                    st.info("Tasks are **suspended** by default. Click **Resume All Tasks** to activate.")
        with setup_col2:
            if st.button("Resume All Tasks", use_container_width=True, key="resume_tasks"):
                for line in resume_tasks_sql.split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            session.sql(line).collect()
                        except Exception as e:
                            st.warning(f"Resume error: {e}")
                st.success("All 4 tasks resumed!")
            if st.button("Suspend All Tasks", use_container_width=True, key="suspend_tasks"):
                for line in suspend_tasks_sql.split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            session.sql(line).collect()
                        except Exception as e:
                            st.warning(f"Suspend error: {e}")
                st.success("All 4 tasks suspended.")

        st.divider()

        for step in inc_steps:
            with st.expander(f"{step['icon']} {step['name']} — {step['desc']}", expanded=False):
                for sql in step["sqls"]:
                    st.code(sql, language="sql")
                ec1, ec2 = st.columns(2)
                with ec1:
                    if st.button("Execute", key=f"inc_exec_{step['label']}", use_container_width=True, type="primary"):
                        for sql in step["sqls"]:
                            try:
                                session.sql(sql).collect()
                            except Exception as e:
                                st.error(f"Error: {e}")
                                break
                        else:
                            st.success(f"{step['name']} created successfully!")
                with ec2:
                    if step["label"] == "streams":
                        if st.button("Check Streams", key="check_streams", use_container_width=True):
                            try:
                                sdf = session.sql(f"SHOW STREAMS IN SCHEMA {inc_src}").to_pandas()
                                if not sdf.empty:
                                    display_cols = [c for c in sdf.columns if c.lower() in ('name','stale','type','source_type','table_name','stale_after')]
                                    st.dataframe(sdf[display_cols] if display_cols else sdf, use_container_width=True, hide_index=True)
                                else:
                                    st.info("No streams found.")
                            except Exception as e:
                                st.warning(f"Could not check streams: {e}")

        st.divider()
        st.subheader("Monitoring")

        mon_tab1, mon_tab2, mon_tab3 = st.tabs([":material/visibility: Stream Status", ":material/history: Task History", ":material/analytics: Pipeline Health"])

        with mon_tab1:
            if st.button("Refresh Stream Status", key="ref_streams", use_container_width=True):
                try:
                    sdf = session.sql(f"SHOW STREAMS IN SCHEMA {inc_src}").to_pandas()
                    if not sdf.empty:
                        st.dataframe(sdf, use_container_width=True, hide_index=True)
                    else:
                        st.info("No streams found. Deploy the pipeline first.")
                except Exception as e:
                    st.warning(f"Error: {e}")
            st.caption("Streams track CDC changes on source tables. **stale = FALSE** means the stream is healthy.")

        with mon_tab2:
            if st.button("Refresh Task History", key="ref_tasks", use_container_width=True):
                try:
                    task_names = ['INCR_FACT_CLAIMS', 'INCR_FACT_EXPENSE', 'INCR_DIM_POLICY', 'INCR_DIM_WEATHER']
                    all_hist = []
                    for tn in task_names:
                        try:
                            hdf = session.sql(f"""
                                SELECT '{tn}' AS TASK_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
                                    RETURN_VALUE, ERROR_CODE, ERROR_MESSAGE
                                FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                    TASK_NAME => '{tn}',
                                    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
                                ))
                                ORDER BY SCHEDULED_TIME DESC LIMIT 10
                            """).to_pandas()
                            if not hdf.empty:
                                all_hist.append(hdf)
                        except Exception:
                            pass
                    if all_hist:
                        combined = pd.concat(all_hist, ignore_index=True).sort_values('SCHEDULED_TIME', ascending=False)
                        st.dataframe(combined, use_container_width=True, hide_index=True)

                        succeeded = len(combined[combined['STATE'] == 'SUCCEEDED'])
                        failed = len(combined[combined['STATE'] == 'FAILED'])
                        skipped = len(combined[combined['STATE'] == 'SKIPPED'])
                        h1, h2, h3, h4 = st.columns(4)
                        h1.metric("Total Runs (24h)", len(combined))
                        h2.metric("Succeeded", succeeded)
                        h3.metric("Skipped", skipped)
                        h4.metric("Failed", failed)
                    else:
                        st.info("No task history found. Tasks may not have run yet.")
                except Exception as e:
                    st.warning(f"Error: {e}")

        with mon_tab3:
            if st.button("Check Pipeline Health", key="health_check", use_container_width=True):
                try:
                    tdf = session.sql(f"SHOW TASKS IN SCHEMA {inc_tgt}").to_pandas()
                    if not tdf.empty:
                        name_col = [c for c in tdf.columns if c.lower() == 'name'][0]
                        state_col = [c for c in tdf.columns if c.lower() == 'state'][0]
                        sched_col = [c for c in tdf.columns if c.lower() == 'schedule'][0]

                        for _, row in tdf.iterrows():
                            tname = row[name_col]
                            tstate = row[state_col]
                            tsched = row[sched_col]
                            color = "#4CAF50" if tstate == "started" else "#FF9800"
                            icon = ":material/play_circle:" if tstate == "started" else ":material/pause_circle:"
                            st.markdown(f"""<div style='background:linear-gradient(135deg,{color}08,{color}04);border:1px solid {color}25;border-radius:10px;padding:12px 16px;margin:6px 0;display:flex;justify-content:space-between;align-items:center'>
                                <div><b style='color:#263238'>{tname}</b><br><span style='color:#78909C;font-size:0.8rem'>{tsched}</span></div>
                                <span style='background:{color};color:white;padding:3px 12px;border-radius:12px;font-size:0.8rem;font-weight:600'>{tstate.upper()}</span>
                            </div>""", unsafe_allow_html=True)
                    else:
                        st.info("No tasks found. Deploy the pipeline first.")
                except Exception as e:
                    st.warning(f"Error: {e}")

            st.divider()
            st.markdown(f"""<div style='background:#F5F7FA;border-radius:12px;padding:16px;border:1px solid #E0E0E0'>
                <h4 style='color:#263238;margin-top:0'>How It Works</h4>
                <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;text-align:center'>
                    <div style='background:var(--bg-card);border-radius:10px;padding:12px;border:1px solid #E0E0E0'>
                        <div style='color:#1565C0;font-size:1.5rem;font-weight:800'>1</div>
                        <div style='color:#263238;font-weight:600;font-size:0.85rem'>Streams</div>
                        <div style='color:#78909C;font-size:0.75rem'>Track INSERT/UPDATE/DELETE on source tables</div>
                    </div>
                    <div style='background:var(--bg-card);border-radius:10px;padding:12px;border:1px solid #E0E0E0'>
                        <div style='color:#FF8F00;font-size:1.5rem;font-weight:800'>2</div>
                        <div style='color:#263238;font-weight:600;font-size:0.85rem'>Tasks Check</div>
                        <div style='color:#78909C;font-size:0.75rem'>SYSTEM$STREAM_HAS_DATA skips if no changes</div>
                    </div>
                    <div style='background:var(--bg-card);border-radius:10px;padding:12px;border:1px solid #E0E0E0'>
                        <div style='color:#2E7D32;font-size:1.5rem;font-weight:800'>3</div>
                        <div style='color:#263238;font-weight:600;font-size:0.85rem'>MERGE</div>
                        <div style='color:#78909C;font-size:0.75rem'>Upsert changed rows into dims & facts</div>
                    </div>
                    <div style='background:var(--bg-card);border-radius:10px;padding:12px;border:1px solid #E0E0E0'>
                        <div style='color:#9C27B0;font-size:1.5rem;font-weight:800'>4</div>
                        <div style='color:#263238;font-weight:600;font-size:0.85rem'>Consume</div>
                        <div style='color:#78909C;font-size:0.75rem'>Stream offset advances, ready for next batch</div>
                    </div>
                </div>
            </div>""", unsafe_allow_html=True)

def render_explorer():
    st.markdown("""<style>
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 10px; padding: 12px 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
    </style>""", unsafe_allow_html=True)

    st.markdown("""<div style='background:linear-gradient(135deg,#0D47A1,#1E88E5);border-radius:12px;padding:20px 24px;margin-bottom:16px;box-shadow:0 4px 12px rgba(13,71,161,0.3)'>
        <h2 style='color:white;margin:0'>Database Explorer</h2>
        <p style='color:#BBDEFB;margin:4px 0 0 0;font-size:0.9rem'>Browse tables, columns, and statistics for <b>{DB}.{SCH}</b></p>
    </div>""".format(DB=DB, SCH=SCH), unsafe_allow_html=True)

    try:
        tables_df = fresh_query(f"""
            SELECT TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
            FROM {DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SCH}' ORDER BY TABLE_NAME
        """)
        table_names = tables_df["TABLE_NAME"].tolist()
    except Exception:
        st.warning("Could not list tables for this schema.")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Tables", len(table_names))
    total_rows = int(tables_df['ROW_COUNT'].sum()) if 'ROW_COUNT' in tables_df.columns else 0
    m2.metric("Total Rows", f"{total_rows:,}")
    total_bytes = tables_df['BYTES'].sum() if 'BYTES' in tables_df.columns else 0
    m3.metric("Total Size", f"{total_bytes/1048576:.1f} MB" if total_bytes else "N/A")

    st.divider()

    col_search, col_select = st.columns([2, 3])
    with col_search:
        search = st.text_input(":material/search: Search tables or columns", placeholder="Type to filter...")
    with col_select:
        if search:
            filtered = [t for t in table_names if search.upper() in t.upper()]
            if not filtered:
                try:
                    col_df = fresh_query(f"""
                        SELECT DISTINCT TABLE_NAME FROM {DB}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{SCH}' AND UPPER(COLUMN_NAME) LIKE '%{search.upper()}%'
                    """)
                    filtered = col_df["TABLE_NAME"].tolist() if not col_df.empty else []
                    if filtered:
                        st.caption(f"Tables with columns matching '{search}'")
                except Exception:
                    filtered = []
        else:
            filtered = table_names

        if not filtered:
            st.warning(f"No match for '{search}'")
            return

        selected = st.selectbox("Select Table", filtered)

    if not selected:
        return

    fq_table = f"{FQ}.{selected}"

    sel_row = tables_df[tables_df['TABLE_NAME'] == selected]
    if not sel_row.empty:
        row_count = int(sel_row['ROW_COUNT'].values[0]) if sel_row['ROW_COUNT'].values[0] else 0
        byte_count = int(sel_row['BYTES'].values[0]) if sel_row['BYTES'].values[0] else 0
        last_alt = sel_row['LAST_ALTERED'].values[0]
        st.markdown(f"""<div style='background:#F5F7FA;border-radius:10px;padding:12px 20px;border:1px solid #E0E0E0;margin:8px 0'>
            <span style='font-weight:700;color:#1565C0;font-size:1.1rem'>{selected}</span>
            <span style='color:#78909C;margin-left:16px'>{row_count:,} rows</span>
            <span style='color:#78909C;margin-left:16px'>{byte_count/1024:.1f} KB</span>
            <span style='color:#78909C;margin-left:16px'>Modified: {last_alt}</span>
        </div>""", unsafe_allow_html=True)

    tab_cols, tab_preview, tab_stats, tab_lineage = st.tabs(
        [":material/view_column: Columns", ":material/table_rows: Preview", ":material/analytics: Statistics", ":material/account_tree: Relationships"])

    with tab_cols:
        col_df = fresh_query(f"""
            SELECT ORDINAL_POSITION AS POS, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                COALESCE(COLUMN_DEFAULT, '') AS DEFAULT_VALUE,
                COALESCE(COMMENT, '') AS DESCRIPTION
            FROM {DB}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{SCH}' AND TABLE_NAME = '{selected}'
            ORDER BY ORDINAL_POSITION
        """)
        if not col_df.empty:
            num_count = len(col_df[col_df['DATA_TYPE'].isin(['NUMBER','FLOAT','DECIMAL'])])
            str_count = len(col_df[col_df['DATA_TYPE'].isin(['TEXT','VARCHAR'])])
            date_count = len(col_df[col_df['DATA_TYPE'].isin(['DATE','TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ'])])
            bool_count = len(col_df[col_df['DATA_TYPE'] == 'BOOLEAN'])

            tc1, tc2, tc3, tc4, tc5 = st.columns(5)
            tc1.metric("Total Columns", len(col_df))
            tc2.metric("Numeric", num_count)
            tc3.metric("Text", str_count)
            tc4.metric("Date/Time", date_count)
            tc5.metric("Boolean", bool_count)

            st.dataframe(col_df, use_container_width=True, hide_index=True)

            # st.subheader("Column Type Distribution")
            # type_counts = col_df['DATA_TYPE'].value_counts().reset_index()
            # type_counts.columns = ['DATA_TYPE', 'COUNT']
            # type_colors = {'NUMBER':'#1565C0','TEXT':'#2E7D32','DATE':'#FF8F00','BOOLEAN':'#9C27B0',
            #               'FLOAT':'#42A5F5','DECIMAL':'#64B5F6','VARCHAR':'#66BB6A',
            #               'TIMESTAMP_NTZ':'#FFB300','TIMESTAMP_LTZ':'#FFA726','TIMESTAMP_TZ':'#FF9800'}
            # fig = go.Figure(go.Bar(
            #     x=type_counts['DATA_TYPE'], y=type_counts['COUNT'],
            #     marker=dict(color=[type_colors.get(t,'#78909C') for t in type_counts['DATA_TYPE']], cornerradius=5),
            #     text=type_counts['COUNT'], textposition='outside'
            # ))
            # fig.update_layout(height=250, margin=dict(t=10,b=40,l=40,r=10),
            #     paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            #     yaxis=dict(gridcolor='#ECEFF1'), showlegend=False)
            # st.plotly_chart(fig, use_container_width=True, key="col_type_dist")

    with tab_preview:
        pc1, pc2 = st.columns([3,1])
        with pc1:
            limit = st.slider("Rows to preview", 5, 100, 20)
        with pc2:
            sort_col = st.selectbox("Sort by", ["Default"] + (col_df['COLUMN_NAME'].tolist() if 'col_df' in dir() and not col_df.empty else []), key="sort_col")

        sort_clause = f" ORDER BY {sort_col}" if sort_col != "Default" else ""
        df = fresh_query(f"SELECT * FROM {fq_table}{sort_clause} LIMIT {limit}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.caption(f"Showing {len(df)} of {row_count:,} rows from `{fq_table}`")

    with tab_stats:
        try:
            num_cols = fresh_query(f"""
                SELECT COLUMN_NAME FROM {DB}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{SCH}' AND TABLE_NAME = '{selected}'
                AND DATA_TYPE IN ('NUMBER', 'FLOAT', 'DECIMAL')
            """)

            if not num_cols.empty:
                st.subheader("Numeric Column Summary")
                col_names = num_cols['COLUMN_NAME'].tolist()

                agg_parts = []
                for cn in col_names:
                    agg_parts.append(f"MIN({cn}) AS \"{cn}_MIN\",MAX({cn}) AS \"{cn}_MAX\",ROUND(AVG({cn}),2) AS \"{cn}_AVG\",COUNT(DISTINCT {cn}) AS \"{cn}_DISTINCT\"")

                agg_sql = f"SELECT {','.join(agg_parts)} FROM {fq_table}"
                agg_df = fresh_query(agg_sql)

                rows = []
                for cn in col_names:
                    rows.append({
                        'Column': cn,
                        'Min': agg_df[f'{cn}_MIN'].iloc[0],
                        'Max': agg_df[f'{cn}_MAX'].iloc[0],
                        'Avg': agg_df[f'{cn}_AVG'].iloc[0],
                        'Distinct': int(agg_df[f'{cn}_DISTINCT'].iloc[0])
                    })
                summary_df = pd.DataFrame(rows)
                st.dataframe(summary_df, use_container_width=True, hide_index=True)

                st.subheader("Value Distribution")
                sel_num = st.selectbox("Select numeric column", col_names, key="hist_col")
                hist_df = fresh_query(f"""
                    SELECT {sel_num} AS VAL FROM {fq_table}
                    WHERE {sel_num} IS NOT NULL ORDER BY {sel_num}
                """)
                if not hist_df.empty:
                    fig = go.Figure(go.Histogram(
                        x=hist_df['VAL'], nbinsx=30,
                        marker=dict(color='#1565C0', line=dict(color='white', width=1)),
                        hovertemplate='Range: %{x}<br>Count: %{y}<extra></extra>'
                    ))
                    avg_val = float(hist_df['VAL'].mean())
                    fig.add_vline(x=avg_val, line=dict(color='#F44336', width=2, dash='dash'),
                        annotation_text=f"Avg: {avg_val:,.2f}", annotation_position="top")
                    fig.update_layout(height=300, margin=dict(t=30,b=40,l=50,r=10),
                        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                        xaxis_title=sel_num, yaxis_title='Frequency',
                        yaxis=dict(gridcolor='#ECEFF1'))
                    st.plotly_chart(fig, use_container_width=True, key="num_hist")
            else:
                st.info("No numeric columns found.")

            str_cols = fresh_query(f"""
                SELECT COLUMN_NAME FROM {DB}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{SCH}' AND TABLE_NAME = '{selected}'
                AND DATA_TYPE IN ('TEXT', 'VARCHAR')
            """)
            if not str_cols.empty:
                st.subheader("Categorical Column Distribution")
                sel_str = st.selectbox("Select text column", str_cols['COLUMN_NAME'].tolist(), key="cat_col")
                cat_df = fresh_query(f"""
                    SELECT {sel_str} AS VAL, COUNT(*) AS COUNT
                    FROM {fq_table} WHERE {sel_str} IS NOT NULL
                    GROUP BY {sel_str} ORDER BY COUNT DESC LIMIT 15
                """)
                if not cat_df.empty:
                    fig = go.Figure(go.Bar(
                        y=cat_df['VAL'], x=cat_df['COUNT'], orientation='h',
                        marker=dict(color='#42A5F5', cornerradius=4),
                        text=cat_df['COUNT'], textposition='outside', textfont=dict(size=9)
                    ))
                    fig.update_layout(height=max(250, len(cat_df)*28), margin=dict(t=10,b=10,l=10,r=50),
                        yaxis=dict(autorange='reversed'), paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True, key="cat_dist")

            st.subheader("Null Analysis")
            null_parts = []
            all_cols_df = fresh_query(f"""
                SELECT COLUMN_NAME FROM {DB}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{SCH}' AND TABLE_NAME = '{selected}'
                ORDER BY ORDINAL_POSITION
            """)
            for cn in all_cols_df['COLUMN_NAME'].tolist()[:20]:
                null_parts.append(f"SUM(CASE WHEN {cn} IS NULL THEN 1 ELSE 0 END) AS \"{cn}\"")
            if null_parts:
                null_df = fresh_query(f"SELECT {','.join(null_parts)} FROM {fq_table}")
                null_data = []
                for cn in null_df.columns:
                    null_count = int(null_df[cn].iloc[0])
                    if row_count > 0:
                        null_data.append({'Column': cn, 'Nulls': null_count, 'Null %': round(null_count/row_count*100, 1)})
                null_summary = pd.DataFrame(null_data)
                null_summary = null_summary.sort_values('Nulls', ascending=False)
                has_nulls = null_summary[null_summary['Nulls'] > 0]
                if not has_nulls.empty:
                    fig = go.Figure(go.Bar(
                        x=has_nulls['Column'], y=has_nulls['Null %'],
                        marker=dict(color=['#F44336' if p > 50 else '#FF9800' if p > 10 else '#4CAF50' for p in has_nulls['Null %']],
                                    cornerradius=4),
                        text=[f"{p}%" for p in has_nulls['Null %']], textposition='outside'
                    ))
                    fig.update_layout(height=280, margin=dict(t=10,b=60,l=40,r=10),
                        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                        yaxis=dict(title='Null %', gridcolor='#ECEFF1'),
                        xaxis=dict(tickangle=-45))
                    st.plotly_chart(fig, use_container_width=True, key="null_chart")
                else:
                    st.success("No null values found in any column!")
        except Exception as e:
            st.warning(f"Could not load stats: {e}")

    with tab_lineage:
        st.subheader("Table Relationships")
        st.caption(f"Foreign key relationships for `{selected}` in the star schema")

        fk_map = {
            'FACT_CLAIMS': [
                ('DATE_KEY', 'DIM_DATE', 'DATE_KEY'),
                ('GEOGRAPHY_KEY', 'DIM_GEOGRAPHY', 'GEOGRAPHY_KEY'),
                ('CLAIM_TYPE_KEY', 'DIM_CLAIM_TYPE', 'CLAIM_TYPE_KEY'),
                ('LOSS_CAUSE_KEY', 'DIM_LOSS_CAUSE', 'LOSS_CAUSE_KEY'),
                ('WEATHER_EVENT_KEY', 'DIM_WEATHER_EVENT', 'WEATHER_EVENT_KEY'),
                ('POLICY_KEY', 'DIM_POLICY', 'POLICY_KEY'),
            ],
            'FACT_CLAIM_EXPENSE': [
                ('CLAIM_KEY', 'FACT_CLAIMS', 'CLAIM_KEY'),
                ('DATE_KEY', 'DIM_DATE', 'DATE_KEY'),
                ('GEOGRAPHY_KEY', 'DIM_GEOGRAPHY', 'GEOGRAPHY_KEY'),
                ('CLAIM_TYPE_KEY', 'DIM_CLAIM_TYPE', 'CLAIM_TYPE_KEY'),
                ('LOSS_CAUSE_KEY', 'DIM_LOSS_CAUSE', 'LOSS_CAUSE_KEY'),
                ('WEATHER_EVENT_KEY', 'DIM_WEATHER_EVENT', 'WEATHER_EVENT_KEY'),
                ('POLICY_KEY', 'DIM_POLICY', 'POLICY_KEY'),
            ],
        }

        refs_from = fk_map.get(selected, [])
        refs_to = [(tbl, fks) for tbl, fks in fk_map.items() if any(ref_tbl == selected for _, ref_tbl, _ in fks)]

        if refs_from:
            st.markdown(f"**{selected}** references:")
            for fk_col, ref_tbl, ref_col in refs_from:
                st.markdown(f"""<div style='background:#E3F2FD;border-radius:8px;padding:8px 14px;margin:4px 0;border-left:3px solid #1565C0;display:inline-block;margin-right:8px'>
                    <span style='color:#1565C0;font-weight:600'>{fk_col}</span> → <span style='color:#0D47A1;font-weight:700'>{ref_tbl}</span>.{ref_col}
                </div>""", unsafe_allow_html=True)

        if refs_to:
            st.markdown(f"**Referenced by:**")
            for tbl, fks in refs_to:
                for fk_col, ref_tbl, ref_col in fks:
                    if ref_tbl == selected:
                        st.markdown(f"""<div style='background:#E8F5E9;border-radius:8px;padding:8px 14px;margin:4px 0;border-left:3px solid #2E7D32;display:inline-block;margin-right:8px'>
                            <span style='color:#2E7D32;font-weight:700'>{tbl}</span>.{fk_col} → <span style='color:#1B5E20;font-weight:600'>{ref_col}</span>
                        </div>""", unsafe_allow_html=True)

        if not refs_from and not refs_to:
            dim_tables = ['DIM_DATE','DIM_GEOGRAPHY','DIM_CLAIM_TYPE','DIM_LOSS_CAUSE','DIM_POLICY','DIM_WEATHER_EVENT']
            if selected in dim_tables:
                st.info(f"`{selected}` is a dimension table referenced by FACT_CLAIMS and FACT_CLAIM_EXPENSE via its primary key.")
            else:
                st.info("No predefined relationships found for this table.")

        if refs_from:
            st.divider()
            st.subheader("Join Preview")
            join_target = st.selectbox("Preview join with", [r[1] for r in refs_from], key="join_tgt")
            join_info = next((r for r in refs_from if r[1] == join_target), None)
            if join_info and st.button("Run Join Preview", type="primary", key="run_join"):
                fk_col, ref_tbl, ref_col = join_info
                try:
                    join_df = fresh_query(f"""
                        SELECT a.*, b.*
                        FROM {FQ}.{selected} a
                        JOIN {FQ}.{ref_tbl} b ON a.{fk_col} = b.{ref_col}
                        LIMIT 10
                    """)
                    st.dataframe(join_df, use_container_width=True, hide_index=True)
                    st.caption(f"Showing 10 rows from `{selected}` JOIN `{ref_tbl}` ON {fk_col} = {ref_col}")
                except Exception as e:
                    st.warning(f"Join failed: {e}")


def render_sample_questions():
    st.title("Sample Questions")
    st.caption("Click any question to send it to the Chatbot.")

    categories = {
        "Claims Overview": [
            "How many open claims do we have?",
            "Show claims count by status",
            "What are the top loss causes by claim count?",
            "What is the average days to close by severity?",
        ],
        "Financial Analysis": [
            "What is the total paid amount by claim type?",
            "Show total reserves vs paid amounts by severity",
            "What is the total net incurred loss?",
            "Which claims have the highest paid amounts?",
        ],
        "Fraud & Risk": [
            "How many fraud flagged claims are there?",
            "Show fraud count by region",
            "What is the average fraud score by risk tier?",
            "Which loss causes have the most fraud cases?",
        ],
        "Weather & Catastrophe": [
            "How many weather-related claims do we have?",
            "Show claims by weather condition",
            "What are the catastrophe events and their impacted claims?",
            "Total paid for weather-related claims by region?",
        ],
        "Expenses & Budget": [
            "What is the total actual vs budgeted expenses?",
            "Show expense ratio by claim type",
            "What are the total legal fees?",
            "Which expense category has the highest variance?",
        ],
    }

    for category, questions in categories.items():
        st.subheader(category)
        cols = st.columns(2)
        for i, q in enumerate(questions):
            with cols[i % 2]:
                if st.button(q, key=f"sq_{category}_{i}", use_container_width=True):
                    st.session_state.pending_question = q
                    st.session_state.active_page = "Chatbot"
                    st.rerun()
        st.divider()

    st.subheader("Custom Questions")
    with st.form("add_custom_q", clear_on_submit=True):
        new_q = st.text_input("Add your own question", placeholder="e.g., What is the total premium by coverage type?")
        if st.form_submit_button("Add", use_container_width=True) and new_q.strip():
            st.session_state.custom_questions.append(new_q.strip())
            st.rerun()

    if st.session_state.custom_questions:
        cols = st.columns(2)
        for i, q in enumerate(st.session_state.custom_questions):
            with cols[i % 2]:
                c1, c2 = st.columns([5, 1])
                with c1:
                    if st.button(q, key=f"cq_{i}", use_container_width=True):
                        st.session_state.pending_question = q
                        st.session_state.active_page = "Chatbot"
                        st.rerun()
                with c2:
                    if st.button(":material/delete:", key=f"del_{i}"):
                        st.session_state.custom_questions.pop(i)
                        st.rerun()
    else:
        st.info("No custom questions yet.")


def render_settings():
    st.markdown("""<style>
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 10px; padding: 12px 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
    </style>""", unsafe_allow_html=True)

    st.markdown("""<div style='background:linear-gradient(135deg,#0D47A1,#1E88E5);border-radius:12px;padding:20px 24px;margin-bottom:16px;box-shadow:0 4px 12px rgba(13,71,161,0.3)'>
        <h2 style='color:white;margin:0'>Settings</h2>
        <p style='color:#BBDEFB;margin:4px 0 0 0;font-size:0.9rem'>Configure chatbot, database connection, and app preferences</p>
    </div>""", unsafe_allow_html=True)

    tab_chat, tab_db, tab_about = st.tabs(
        [":material/smart_toy: Chatbot Config", ":material/database: Database Connection", ":material/info: Summary"])

    with tab_chat:
        st.subheader("LLM Configuration")

        cc1, cc2 = st.columns(2)
        with cc1:
            mode_options = ["Cortex Analyst", "Cortex Complete"]
            current_mode = mode_options.index(st.session_state.chatbot_mode) if st.session_state.chatbot_mode in mode_options else 0
            chosen_mode = st.selectbox("Chatbot Mode", mode_options, index=current_mode,
                help="Cortex Analyst uses the semantic view for structured SQL. Cortex Complete uses free-form LLM chat.")
        with cc2:
            model_options = ["mistral-large2", "llama3.1-70b", "llama3.1-8b", "llama3.3-70b", "snowflake-arctic"]
            current_idx = model_options.index(st.session_state.selected_model) if st.session_state.selected_model in model_options else 0
            chosen_model = st.selectbox("LLM Model (Complete mode)", model_options, index=current_idx)

        mode_color = "#1565C0" if chosen_mode == "Cortex Analyst" else "#FF8F00"
        mode_desc = "Generates SQL from natural language using the semantic view. Best for structured data queries." if chosen_mode == "Cortex Analyst" else "Free-form LLM chat with schema context injection. Best for explanations and open-ended questions."
        st.markdown(f"""<div style='background:#F5F7FA;border-radius:10px;padding:14px 18px;border-left:4px solid {mode_color};margin:8px 0'>
            <span style='background:{mode_color};color:white;padding:3px 10px;border-radius:12px;font-size:0.8rem;font-weight:600'>{chosen_mode}</span>
            <p style='color:#546E7A;font-size:0.85rem;margin:8px 0 0 0'>{mode_desc}</p>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Chat Behavior")

        bc1, bc2 = st.columns(2)
        with bc1:
            history_on = st.toggle("Chat History", value=st.session_state.chat_history_enabled,
                help="When enabled, previous messages are sent as context for follow-up questions")
        with bc2:
            if st.button(":material/delete_sweep: Clear Chat History", use_container_width=True):
                st.session_state.messages = []
                st.success("Chat history cleared!")

        msg_count = len(st.session_state.messages)
        sql_count = sum(1 for m in st.session_state.messages if "sql" in m)
        st.markdown(f"""<div style='background:#F5F7FA;border-radius:10px;padding:12px 18px;border:1px solid #E0E0E0;margin-top:8px'>
            <span style='color:#78909C;font-size:0.8rem'>Current session: </span>
            <span style='color:#1565C0;font-weight:600'>{msg_count} messages</span>
            <span style='color:#78909C'> | </span>
            <span style='color:#2E7D32;font-weight:600'>{sql_count} SQL queries</span>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Semantic View")
        sv_input = st.text_input("Semantic View (fully qualified)", value=st.session_state.semantic_view,
            help="Used by Cortex Analyst mode. Must be a valid semantic view.")

        if st.button("Validate Semantic View", key="val_sv"):
            try:
                sv_parts = sv_input.split('.')
                if len(sv_parts) == 3:
                    sv_check = session.sql(f"SHOW SEMANTIC VIEWS IN SCHEMA {sv_parts[0]}.{sv_parts[1]}").to_pandas()
                    sv_col = [c for c in sv_check.columns if c.lower() == 'name'][0]
                    sv_names = sv_check[sv_col].tolist()
                    if sv_parts[2] in sv_names:
                        st.success(f"Semantic view `{sv_input}` exists!")
                    else:
                        st.warning(f"Semantic view `{sv_parts[2]}` not found. Available: {', '.join(sv_names) if sv_names else 'none'}")
                else:
                    st.warning("Please use fully qualified format: DATABASE.SCHEMA.VIEW_NAME")
            except Exception as e:
                st.warning(f"Could not validate: {e}")

    with tab_db:
        st.subheader("Database Connection")

        try:
            db_df = session.sql("""
                SELECT DATABASE_NAME AS NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME
            """).to_pandas()
            db_list = db_df['NAME'].tolist()
        except Exception:
            try:
                db_df = session.sql("SHOW DATABASES").to_pandas()
                db_col = [c for c in db_df.columns if c.lower() == 'name'][0]
                db_list = sorted(db_df[db_col].tolist())
            except Exception:
                db_list = [DB]

        dc1, dc2 = st.columns(2)
        with dc1:
            current_db_idx = db_list.index(DB) if DB in db_list else 0
            chosen_db = st.selectbox("Database", db_list, index=current_db_idx)
        with dc2:
            try:
                sch_df = session.sql(f"""
                    SELECT SCHEMA_NAME AS NAME FROM {chosen_db}.INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME
                """).to_pandas()
                sch_list = sch_df['NAME'].tolist()
            except Exception:
                try:
                    sch_df = session.sql(f"SHOW SCHEMAS IN DATABASE {chosen_db}").to_pandas()
                    sch_col = [c for c in sch_df.columns if c.lower() == 'name'][0]
                    sch_list = [s for s in sorted(sch_df[sch_col].tolist()) if s != 'INFORMATION_SCHEMA']
                except Exception:
                    sch_list = [SCH]
            current_sch_idx = sch_list.index(SCH) if SCH in sch_list else 0
            chosen_sch = st.selectbox("Schema", sch_list, index=current_sch_idx)

        st.markdown(f"""<div style='background:#E3F2FD;border-radius:10px;padding:12px 18px;border-left:4px solid #1565C0;margin:8px 0'>
            <span style='color:#0D47A1;font-weight:600;font-size:0.95rem'>{chosen_db}.{chosen_sch}</span>
            <span style='color:#546E7A;font-size:0.8rem;margin-left:8px'>
                {'(current)' if chosen_db == DB and chosen_sch == SCH else '(changed — click Apply)'}
            </span>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Schema Overview")
        try:
            preview = fresh_query(f"""
                SELECT TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
                FROM {chosen_db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{chosen_sch}' ORDER BY TABLE_NAME
            """)
            if not preview.empty:
                ov1, ov2, ov3 = st.columns(3)
                ov1.metric("Tables", len(preview))
                ov2.metric("Total Rows", f"{int(preview['ROW_COUNT'].sum()):,}")
                total_bytes = preview['BYTES'].sum()
                ov3.metric("Total Size", f"{total_bytes/1048576:.1f} MB" if total_bytes else "N/A")

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=preview['TABLE_NAME'],
                    y=preview['ROW_COUNT'],
                    marker=dict(color='#1565C0', cornerradius=4),
                    text=preview['ROW_COUNT'].apply(lambda x: f"{int(x):,}" if x else "0"),
                    textposition='outside', textfont=dict(size=9)
                ))
                fig.update_layout(height=280, margin=dict(t=10,b=60,l=50,r=10),
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    yaxis=dict(title='Row Count', gridcolor='#ECEFF1'),
                    xaxis=dict(tickangle=-45))
                st.plotly_chart(fig, use_container_width=True, key="settings_schema_chart")

                preview["SIZE"] = preview["BYTES"].apply(
                    lambda b: f"{b/1024:.1f} KB" if b and b < 1048576 else (f"{b/1048576:.1f} MB" if b else "0 KB"))
                display = preview[["TABLE_NAME", "ROW_COUNT", "SIZE", "LAST_ALTERED"]].copy()
                display.columns = ["Table", "Rows", "Size", "Last Modified"]
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.warning("No tables found.")
        except Exception as e:
            st.warning(f"Could not preview: {e}")

    with tab_about:
        st.subheader("About Claims Intelligence")
        st.markdown("""<div style='background:#F5F7FA;border-radius:12px;padding:20px;border:1px solid #E0E0E0'>
            <h4 style='color:#1565C0;margin-top:0'>Insurance Claims Analytics Platform</h4>
            <p style='color:#546E7A'>A comprehensive Streamlit application for insurance claims data analysis, powered by Snowflake Cortex AI.</p>
            <hr style='border-color:#E0E0E0'>
            <div style='display:flex;gap:24px;flex-wrap:wrap'>
                <div>
                    <div style='color:#78909C;font-size:0.75rem;text-transform:uppercase'>Pages</div>
                    <div style='color:#263238;font-size:0.85rem;margin-top:4px'>
                        <b>Summary</b> — KPI dashboard with key metrics<br>
                        <b>Analytics</b> — Deep-dive with 6 analysis tabs<br>
                        <b>Chatbot</b> — NL to SQL via Cortex Analyst/Complete<br>
                        <b>Transform</b> — ETL pipeline with star schema builder<br>
                        <b>DB Explorer</b> — Table browser with stats & profiling<br>
                        <b>Sample Questions</b> — Pre-built query templates<br>
                        <b>Settings</b> — Configuration & connection management
                    </div>
                </div>
                <div>
                    <div style='color:#78909C;font-size:0.75rem;text-transform:uppercase'>Technology</div>
                    <div style='color:#263238;font-size:0.85rem;margin-top:4px'>
                        Streamlit in Snowflake (SiS)<br>
                        Snowflake Cortex Analyst<br>
                        Snowflake Cortex Complete<br>
                        Plotly for visualizations<br>
                        Semantic Views for NL2SQL
                    </div>
                </div>
                <div>
                    <div style='color:#78909C;font-size:0.75rem;text-transform:uppercase'>Data Model</div>
                    <div style='color:#263238;font-size:0.85rem;margin-top:4px'>
                        Star schema with 6 dimensions<br>
                        2 fact tables (Claims + Expenses)<br>
                        16 semantic metrics<br>
                        26 semantic dimensions<br>
                        10,000 claims records
                    </div>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("Current Session")
        try:
            ctx = session.sql("SELECT CURRENT_ROLE() AS ROLE, CURRENT_WAREHOUSE() AS WH, CURRENT_USER() AS USR").to_pandas()
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("User", ctx['USR'].iloc[0])
            sc2.metric("Role", ctx['ROLE'].iloc[0])
            sc3.metric("Warehouse", ctx['WH'].iloc[0])
        except Exception:
            pass

        st.markdown(f"""<div style='background:#F5F7FA;border-radius:10px;padding:14px 18px;border:1px solid #E0E0E0;margin-top:8px'>
            <div style='display:flex;gap:24px;flex-wrap:wrap'>
                <div><span style='color:#78909C;font-size:0.75rem'>Database</span><br><span style='color:#263238;font-weight:600'>{DB}</span></div>
                <div><span style='color:#78909C;font-size:0.75rem'>Schema</span><br><span style='color:#263238;font-weight:600'>{SCH}</span></div>
                <div><span style='color:#78909C;font-size:0.75rem'>Mode</span><br><span style='color:#263238;font-weight:600'>{st.session_state.chatbot_mode}</span></div>
                <div><span style='color:#78909C;font-size:0.75rem'>Model</span><br><span style='color:#263238;font-weight:600'>{MODEL}</span></div>
                <div><span style='color:#78909C;font-size:0.75rem'>History</span><br><span style='color:#263238;font-weight:600'>{"On" if st.session_state.chat_history_enabled else "Off"}</span></div>
                <div><span style='color:#78909C;font-size:0.75rem'>Semantic View</span><br><span style='color:#263238;font-weight:600'>{SV}</span></div>
            </div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    if st.button("Apply Settings", type="primary", use_container_width=True):
        st.session_state.selected_db = chosen_db
        st.session_state.selected_schema = chosen_sch
        st.session_state.selected_model = chosen_model
        st.session_state.chatbot_mode = chosen_mode
        st.session_state.chat_history_enabled = history_on
        st.session_state.semantic_view = sv_input
        if not history_on:
            st.session_state.messages = []
        st.session_state.messages = []
        cached_query.clear()
        st.success(f"Applied: {chosen_db}.{chosen_sch}")
        st.rerun()


page = st.session_state.active_page
if page == "Summary":
    render_summary()
elif page == "Analytics":
    render_analytics()
elif page == "Chatbot":
    render_chatbot()
elif page == "Transform":
    render_transformations()
elif page == "DB Explorer":
    render_explorer()
elif page == "Sample Questions":
    render_sample_questions()
elif page == "Settings":
    render_settings()
