"""
ETL Validation Query Generator  —  Streamlit UI
Color theme: WiseiTech BI style
"""

import json
import traceback

import streamlit as st

# ─────────────────────────────────────────
# Page config
# ─────────────────────────────────────────
st.set_page_config(
    page_title="ETL Validator",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────
# CSS  —  테마
# ─────────────────────────────────────────
st.markdown("""
<style>
/* ══════════════════════════════════════
   전역 — Supabase 라이트 테마
══════════════════════════════════════ */
html, body, [class*="css"] {
    font-family: "Pretendard", "Noto Sans KR", "Malgun Gothic", "Segoe UI", sans-serif;
    font-size: 14px;
}
.stApp {
    background-color: #F8FAFC;
}
/* 기본 텍스트 전부 어둡게 */
.stApp p, .stApp span, .stApp label,
.stApp div, .stApp li, .stApp td, .stApp th {
    color: #1A202C;
}

/* ══════════════════════════════════════
   사이드바 — Supabase 화이트 라이트
══════════════════════════════════════ */
[data-testid="stSidebar"] > div:first-child {
    background-color: #FFFFFF !important;
    border-right: 1px solid #E2E8F0 !important;
    padding-top: 0 !important;
}
/* 사이드바 안 모든 텍스트 */
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div {
    color: #374151 !important;
}
/* 사이드바 selectbox — 화이트 배경 */
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] {
    background-color: #FFFFFF !important;
    border-color: #CBD5E0 !important;
}
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] span {
    color: #1A202C !important;
}
/* 라디오 버튼 */
[data-testid="stSidebar"] .stRadio [data-testid="stWidgetLabel"] p,
[data-testid="stSidebar"] .stRadio label span {
    color: #374151 !important;
}
/* 라디오 항목 — Supabase 네비게이션 스타일 */
[data-testid="stSidebar"] .stRadio label {
    border-radius: 6px !important;
    padding: 5px 8px 5px 10px !important;
    margin: 1px 0 !important;
    border-left: 3px solid transparent !important;
    transition: background 0.12s !important;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background: #F1F5F9 !important;
}
/* 토글 */
[data-testid="stSidebar"] .stToggle label,
[data-testid="stSidebar"] .stToggle [data-testid="stWidgetLabel"] p {
    color: #374151 !important;
}
/* 구분선 */
[data-testid="stSidebar"] hr {
    border-color: #E2E8F0 !important;
    margin: 12px 0 !important;
}
/* 로고 영역 */
.logo-wrap {
    padding: 14px 20px;
    margin-bottom: 16px;
}
.logo-wrap img { width: 120px; }
/* 섹션 레이블 */
.sb-section {
    font-size: 10px !important;
    font-weight: 700 !important;
    color: #94A3B8 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.09em !important;
    margin: 16px 0 4px 0 !important;
    display: block;
}
/* 메뉴 그룹 레이블 */
.sb-group {
    font-size: 9px !important;
    font-weight: 700 !important;
    color: #94A3B8 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    margin: 10px 0 4px 4px !important;
    display: block;
    padding: 2px 0;
    border-bottom: 1px solid #E2E8F0;
}
/* 네비게이션 버튼 스타일 — secondary (비활성) */
[data-testid="stSidebar"] .stButton > button {
    text-align: left !important;
    justify-content: flex-start !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 5px 10px !important;
    border-radius: 6px !important;
    height: auto !important;
    margin: 1px 0 !important;
}
/* 네비게이션 버튼 — primary (활성) */
[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background-color: #EFF6FF !important;
    border-color: #3B82F6 !important;
    color: #1D4ED8 !important;
    font-weight: 700 !important;
}

/* ══════════════════════════════════════
   페이지 헤더 — Supabase 스타일
══════════════════════════════════════ */
.page-header {
    background: #FFFFFF;
    border-bottom: 1px solid #E2E8F0;
    border-radius: 8px 8px 0 0;
    padding: 16px 24px;
    margin: 0 0 20px 0;
    display: flex;
    align-items: center;
    gap: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.page-title {
    font-size: 18px;
    font-weight: 700;
    color: #111827 !important;
    margin: 0;
    letter-spacing: -0.01em;
}
.page-subtitle {
    font-size: 12px;
    color: #6B7280 !important;
    margin: 3px 0 0 0;
}
.header-badge {
    background: #EFF6FF;
    color: #0070C0;
    font-size: 11px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 12px;
    border: 1px solid #BFDBFE;
    margin-left: auto;
    white-space: nowrap;
}

/* ══════════════════════════════════════
   스텝 인디케이터
══════════════════════════════════════ */
.step-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 20px 0 10px 0;
}
.step-num {
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: #0070C0;
    color: #FFFFFF !important;
    font-size: 12px;
    font-weight: 700;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
}
.step-text {
    font-size: 14px;
    font-weight: 600;
    color: #1A202C !important;
}

/* ══════════════════════════════════════
   카드 패널 — Supabase 스타일
══════════════════════════════════════ */
.card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 8px;
    padding: 20px 22px;
    margin-bottom: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}

/* ══════════════════════════════════════
   테이블 헤더 배지
══════════════════════════════════════ */
.tbl-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 9px 14px;
    border-radius: 6px;
    margin-bottom: 8px;
}
.tbl-header.src { background: #EFF6FF; border: 1px solid #BFDBFE; }
.tbl-header.tgt { background: #F0FDF4; border: 1px solid #BBF7D0; }
.tbl-name {
    font-size: 13px;
    font-weight: 700;
    color: #111827 !important;
    font-family: "Consolas", "D2Coding", monospace;
}
.tbl-meta {
    font-size: 12px;
    color: #6B7280 !important;
    margin-left: auto;
}
.badge-src {
    background: #0070C0;
    color: #FFFFFF !important;
    font-size: 10px;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 4px;
    letter-spacing: 0.03em;
}
.badge-tgt {
    background: #16A34A;
    color: #FFFFFF !important;
    font-size: 10px;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 4px;
    letter-spacing: 0.03em;
}

/* ══════════════════════════════════════
   웰컴 카드 — Supabase 스타일
══════════════════════════════════════ */
.welcome-card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-top: 3px solid #0070C0;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.welcome-num {
    font-size: 28px;
    font-weight: 800;
    color: #E2E8F0 !important;
    line-height: 1;
    margin-bottom: 8px;
}
.welcome-title {
    font-size: 14px;
    font-weight: 600;
    color: #111827 !important;
    margin-bottom: 6px;
}
.welcome-desc {
    font-size: 12px;
    color: #6B7280 !important;
    line-height: 1.6;
}

/* ══════════════════════════════════════
   탭
══════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 2px solid #E2E8F0;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    font-size: 13px !important;
    font-weight: 500 !important;
    color: #6B7280 !important;
    padding: 8px 18px !important;
    border-radius: 0 !important;
    border-bottom: 2px solid transparent !important;
    margin-bottom: -2px !important;
    background: transparent !important;
}
.stTabs [aria-selected="true"] {
    color: #0070C0 !important;
    border-bottom: 2px solid #0070C0 !important;
    font-weight: 600 !important;
    background: transparent !important;
}
/* 탭 패널 배경 */
.stTabs [data-baseweb="tab-panel"] {
    background: #FFFFFF !important;
    border: 1px solid #E2E8F0;
    border-top: none;
    border-radius: 0 0 8px 8px;
    padding: 20px 20px 16px 20px !important;
}

/* ══════════════════════════════════════
   버튼 — 모든 텍스트 가시성 완전 보장
══════════════════════════════════════ */
/* Primary */
div.stButton > button[kind="primary"],
div.stButton > button[data-testid="baseButton-primary"] {
    background-color: #0070C0 !important;
    color: #FFFFFF !important;
    border: none !important;
    border-radius: 6px !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    padding: 9px 22px !important;
    letter-spacing: 0.01em;
    box-shadow: 0 1px 2px rgba(0,112,192,0.2) !important;
}
div.stButton > button[kind="primary"] p,
div.stButton > button[kind="primary"] span,
div.stButton > button[kind="primary"] div,
div.stButton > button[data-testid="baseButton-primary"] p,
div.stButton > button[data-testid="baseButton-primary"] span,
div.stButton > button[data-testid="baseButton-primary"] div {
    color: #FFFFFF !important;
}
div.stButton > button[kind="primary"]:hover {
    background-color: #005A9E !important;
    box-shadow: 0 2px 4px rgba(0,112,192,0.3) !important;
}
/* Secondary */
div.stButton > button[kind="secondary"],
div.stButton > button[data-testid="baseButton-secondary"] {
    background-color: #FFFFFF !important;
    color: #374151 !important;
    border: 1px solid #D1D5DB !important;
    border-radius: 6px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 8px 18px !important;
}
div.stButton > button[kind="secondary"] p,
div.stButton > button[kind="secondary"] span,
div.stButton > button[kind="secondary"] div,
div.stButton > button[data-testid="baseButton-secondary"] p,
div.stButton > button[data-testid="baseButton-secondary"] span,
div.stButton > button[data-testid="baseButton-secondary"] div {
    color: #374151 !important;
}
div.stButton > button[kind="secondary"]:hover {
    background-color: #F9FAFB !important;
    border-color: #0070C0 !important;
    color: #0070C0 !important;
}
div.stButton > button[kind="secondary"]:hover p,
div.stButton > button[kind="secondary"]:hover span,
div.stButton > button[kind="secondary"]:hover div {
    color: #0070C0 !important;
}
/* Download 버튼 */
div.stDownloadButton > button {
    background-color: #FFFFFF !important;
    color: #374151 !important;
    border: 1px solid #D1D5DB !important;
    border-radius: 6px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
}
div.stDownloadButton > button p,
div.stDownloadButton > button span,
div.stDownloadButton > button div {
    color: #374151 !important;
}
div.stDownloadButton > button:hover {
    border-color: #0070C0 !important;
    color: #0070C0 !important;
}
div.stDownloadButton > button:hover p,
div.stDownloadButton > button:hover span,
div.stDownloadButton > button:hover div {
    color: #0070C0 !important;
}
/* Download 버튼 — Primary (파란색) */
div.dl-primary div.stDownloadButton > button {
    background-color: #0070C0 !important;
    color: #FFFFFF !important;
    border-color: #0070C0 !important;
}
div.dl-primary div.stDownloadButton > button p,
div.dl-primary div.stDownloadButton > button span,
div.dl-primary div.stDownloadButton > button div {
    color: #FFFFFF !important;
}
div.dl-primary div.stDownloadButton > button:hover {
    background-color: #005A9E !important;
    border-color: #005A9E !important;
}

/* ══════════════════════════════════════
   파일 업로더
══════════════════════════════════════ */
[data-testid="stFileUploader"] {
    border: 1px dashed #CBD5E0 !important;
    border-radius: 8px !important;
    background: #F8FAFC !important;
}
[data-testid="stFileUploader"] span,
[data-testid="stFileUploader"] p,
[data-testid="stFileUploader"] small {
    color: #6B7280 !important;
}

/* ══════════════════════════════════════
   입력 요소 — 화이트 배경 통일
══════════════════════════════════════ */
.stTextInput input,
.stTextArea textarea {
    border-color: #D1D5DB !important;
    border-radius: 6px !important;
    background: #FFFFFF !important;
    color: #1A202C !important;
}
.stTextInput input:focus,
.stTextArea textarea:focus {
    border-color: #0070C0 !important;
    box-shadow: 0 0 0 2px rgba(0,112,192,0.12) !important;
}
/* 셀렉박스 — 화이트 배경 + 다크 텍스트 (핵심 수정) */
.stSelectbox div[data-baseweb="select"] {
    border-color: #D1D5DB !important;
    border-radius: 6px !important;
    background: #FFFFFF !important;
}
.stSelectbox div[data-baseweb="select"] span,
.stSelectbox div[data-baseweb="select"] div,
.stSelectbox div[data-baseweb="select"] p,
.stSelectbox div[data-baseweb="select"] input {
    color: #1A202C !important;
}
/* 드롭다운 팝업 목록 아이템 */
[data-baseweb="menu"] [role="option"] span,
[data-baseweb="menu"] [role="option"] div,
[data-baseweb="popover"] [role="option"] span {
    color: #1A202C !important;
}
/* 라벨 */
.stTextInput label p, .stSelectbox label p,
.stFileUploader label p, .stToggle label p,
.stTextArea label p, .stNumberInput label p {
    color: #374151 !important;
    font-size: 13px !important;
    font-weight: 500 !important;
}

/* ══════════════════════════════════════
   dataframe
══════════════════════════════════════ */
[data-testid="stDataFrame"] {
    border: 1px solid #E2E8F0 !important;
    border-radius: 6px !important;
}
[data-testid="stDataFrame"] th {
    background-color: #F1F5F9 !important;
    color: #374151 !important;
    font-weight: 600 !important;
    font-size: 12px !important;
}
[data-testid="stDataFrame"] td {
    color: #1A202C !important;
    font-size: 12px !important;
}

/* ══════════════════════════════════════
   알림 메시지 — Supabase 왼쪽 테두리 스타일
══════════════════════════════════════ */
.stAlert [data-testid="stMarkdownContainer"] p {
    color: inherit !important;
}
[data-testid="stAlert"] p { color: inherit !important; }
[data-testid="stAlert"][data-type="info"] {
    background: #EFF6FF !important;
    border: 1px solid #BFDBFE !important;
    border-left: 4px solid #3B82F6 !important;
    border-radius: 6px !important;
}
[data-testid="stAlert"][data-type="success"] {
    background: #F0FDF4 !important;
    border: 1px solid #BBF7D0 !important;
    border-left: 4px solid #16A34A !important;
    border-radius: 6px !important;
}
[data-testid="stAlert"][data-type="warning"] {
    background: #FFFBEB !important;
    border: 1px solid #FDE68A !important;
    border-left: 4px solid #F59E0B !important;
    border-radius: 6px !important;
}
[data-testid="stAlert"][data-type="error"] {
    background: #FEF2F2 !important;
    border: 1px solid #FECACA !important;
    border-left: 4px solid #EF4444 !important;
    border-radius: 6px !important;
}

/* ══════════════════════════════════════
   expander — Supabase 스타일
══════════════════════════════════════ */
.stExpander {
    border: 1px solid #E2E8F0 !important;
    border-radius: 8px !important;
    background: #FFFFFF !important;
    margin-bottom: 8px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
.stExpander summary {
    background: #F8FAFC !important;
    border-radius: 8px 8px 0 0 !important;
    padding: 10px 14px !important;
}
.stExpander summary span,
.stExpander summary p,
.stExpander summary div {
    color: #374151 !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}
.stExpander [data-testid="stExpanderDetails"] {
    background: #FFFFFF !important;
    border-top: 1px solid #E2E8F0 !important;
    padding: 12px 14px !important;
}

/* ══════════════════════════════════════
   구분선
══════════════════════════════════════ */
hr { border-color: #E2E8F0 !important; }

/* ══════════════════════════════════════
   코드 블록 — DBeaver 스타일 다크 테마
══════════════════════════════════════ */
[data-testid="stCode"],
.stCode {
    border-radius: 8px !important;
    overflow: hidden !important;
    border: 1px solid #2D3748 !important;
}
[data-testid="stCode"] pre,
.stCode pre {
    background-color: #1E1E2E !important;
    padding: 14px 16px !important;
    margin: 0 !important;
    border-radius: 8px !important;
    overflow-x: auto !important;
}
[data-testid="stCode"] code,
.stCode code {
    background: transparent !important;
    color: #CDD6F4 !important;
    font-family: "D2Coding", "Consolas", "JetBrains Mono", "Fira Code", monospace !important;
    font-size: 13px !important;
    line-height: 1.65 !important;
}
/* Pygments 구문 강조 ─ DBeaver 유사 색상 */
/* 키워드 (SELECT FROM WHERE JOIN ...) */
[data-testid="stCode"] .k,
[data-testid="stCode"] .kw,
[data-testid="stCode"] .kd,
[data-testid="stCode"] .kn,
[data-testid="stCode"] .kr,
[data-testid="stCode"] .kt {
    color: #89B4FA !important;
    font-weight: bold !important;
}
/* 문자열 */
[data-testid="stCode"] .s,
[data-testid="stCode"] .s1,
[data-testid="stCode"] .s2,
[data-testid="stCode"] .sa,
[data-testid="stCode"] .sb,
[data-testid="stCode"] .sc,
[data-testid="stCode"] .sd,
[data-testid="stCode"] .se,
[data-testid="stCode"] .sh,
[data-testid="stCode"] .si,
[data-testid="stCode"] .sx {
    color: #A6E3A1 !important;
}
/* 주석 */
[data-testid="stCode"] .c,
[data-testid="stCode"] .c1,
[data-testid="stCode"] .cm,
[data-testid="stCode"] .cs,
[data-testid="stCode"] .cp {
    color: #6C7086 !important;
    font-style: italic !important;
}
/* 숫자 */
[data-testid="stCode"] .mi,
[data-testid="stCode"] .mf,
[data-testid="stCode"] .m {
    color: #FAB387 !important;
}
/* 내장 함수 / 이름 */
[data-testid="stCode"] .nf,
[data-testid="stCode"] .nb {
    color: #89DCEB !important;
}
/* 연산자 */
[data-testid="stCode"] .o,
[data-testid="stCode"] .ow {
    color: #F38BA8 !important;
}
/* 식별자(테이블명·컬럼명) */
[data-testid="stCode"] .n,
[data-testid="stCode"] .na,
[data-testid="stCode"] .nc,
[data-testid="stCode"] .nd,
[data-testid="stCode"] .ni,
[data-testid="stCode"] .ne,
[data-testid="stCode"] .nn,
[data-testid="stCode"] .nx {
    color: #CDD6F4 !important;
}
/* 괄호·구두점 */
[data-testid="stCode"] .p {
    color: #CBA6F7 !important;
}

/* caption */
[data-testid="stCaptionContainer"] p {
    color: #6B7280 !important;
    font-size: 12px !important;
}

/* ══════════════════════════════════════
   DB 연결 상태 바 — Supabase 스타일
══════════════════════════════════════ */
.conn-bar {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 8px;
    padding: 10px 16px;
    margin-bottom: 18px;
    display: flex;
    align-items: center;
    gap: 12px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.conn-bar-icon img { width: 28px !important; height: 28px !important; }
.conn-dot-on  { color: #16A34A; font-size: 11px; }
.conn-dot-off { color: #EF4444; font-size: 11px; }
.conn-info {
    font-size: 12px;
    color: #374151 !important;
    line-height: 1.4;
}
.conn-info b { color: #0070C0 !important; }
.conn-info small { color: #6B7280 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Session state
# ─────────────────────────────────────────
for key in ("source_meta", "target_meta", "mapping", "queries"):
    if key not in st.session_state:
        st.session_state[key] = None
# Agent / Profile 상태
if "agent_history" not in st.session_state:
    st.session_state["agent_history"] = []
if "profile_result" not in st.session_state:
    st.session_state["profile_result"] = None
if "profile_rules" not in st.session_state:
    st.session_state["profile_rules"] = None

# 검증 실행 상태
for _k in ("exec_result", "exec_diagnosis", "exec_sql_type"):
    if _k not in st.session_state:
        st.session_state[_k] = None

# 매핑 자동화 상태
for _k in ("export_excel_bytes", "export_ddl", "export_merge_sql", "export_report_bytes",
           "export_json_str", "export_csv_str",
           "tpl_structure", "tpl_bytes", "tpl_suggestions"):
    if _k not in st.session_state:
        st.session_state[_k] = None

# DW 설계 상태
for _k in ("designer_entities", "designer_design", "designer_ddl"):
    if _k not in st.session_state:
        st.session_state[_k] = None

# 리니지 분석 상태
for _k in ("lineage_result", "lineage_graph", "lineage_explanation"):
    if _k not in st.session_state:
        st.session_state[_k] = None

# ETL Flow Map — 등록된 매핑 목록
if "flow_map_mappings" not in st.session_state:
    st.session_state["flow_map_mappings"] = []  # list[dict] — build_flow_data_from_mappings 입력 형식

# DB 연결 설정 (Agent/Profile 페이지용)
if "db_conn_config" not in st.session_state:
    _default_conn = {"db_type": "mariadb", "host": "localhost", "port": 3306,
                     "user": "", "password": "", "database": "", "connected": False}
    try:
        with open("db_config.json", encoding="utf-8") as _f:
            _cfg = json.load(_f)
        _default_conn.update({
            "db_type": _cfg.get("db_type", "mariadb"),
            "host":    _cfg["connection"].get("host", "localhost"),
            "port":    int(_cfg["connection"].get("port", 3306)),
            "user":    _cfg["connection"].get("user", ""),
            "password": _cfg["connection"].get("password", ""),
            "database": _cfg["connection"].get("database", ""),
        })
    except Exception:
        pass
    st.session_state["db_conn_config"] = _default_conn


# ─────────────────────────────────────────
# 페이지 상태 초기화
# ─────────────────────────────────────────
if "page" not in st.session_state:
    st.session_state["page"] = "검증 쿼리 생성"
if "global_mode" not in st.session_state:
    st.session_state["global_mode"] = "파일 업로드"
if "_prev_global_mode" not in st.session_state:
    st.session_state["_prev_global_mode"] = st.session_state["global_mode"]
if "_sidebar_conn_open" not in st.session_state:
    st.session_state["_sidebar_conn_open"] = False

# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="color:#0070C0;font-weight:700;font-size:18px;padding:10px 0 6px;">AETL</div>', unsafe_allow_html=True)
    st.caption("AI-driven Sub ETL Platform")
    st.divider()

    # ── 전역 데이터 소스 ────────────────────────────────────
    st.markdown('<span class="sb-section">데이터 소스</span>', unsafe_allow_html=True)
    mode = st.radio(
        "데이터 소스",
        options=["파일 업로드", "DB 직접 연결"],
        index=0 if st.session_state["global_mode"] == "파일 업로드" else 1,
        label_visibility="collapsed",
        key="global_mode_radio",
    )
    st.session_state["global_mode"] = mode

    # ── 데이터 소스 변경 시 전체 초기화 ──────────────
    if mode != st.session_state.get("_prev_global_mode"):
        _reset_keys_none = [
            "source_meta", "target_meta", "mapping", "queries",
            "profile_result", "profile_rules",
            "exec_result", "exec_diagnosis", "exec_sql_type",
            "export_excel_bytes", "export_ddl", "export_merge_sql",
            "export_report_bytes", "export_json_str", "export_csv_str",
            "tpl_structure", "tpl_bytes", "tpl_suggestions",
            "designer_entities", "designer_design", "designer_ddl",
            "lineage_result", "lineage_graph", "lineage_explanation",
        ]
        for _rk in _reset_keys_none:
            st.session_state[_rk] = None
        st.session_state["agent_history"] = []
        st.session_state["flow_map_mappings"] = []
        st.session_state["_prev_global_mode"] = mode
        st.rerun()

    if mode == "DB 직접 연결":
        _db_type_opts = ["oracle", "mariadb", "postgresql"]
        _cur_db = st.session_state["db_conn_config"].get("db_type", "oracle")
        db_type = st.selectbox(
            "DB 종류",
            options=_db_type_opts,
            index=_db_type_opts.index(_cur_db) if _cur_db in _db_type_opts else 0,
            label_visibility="collapsed",
            key="global_db_type",
        )
        # ── 연결 상태 표시 ──────────────────────────────
        _cfg = st.session_state["db_conn_config"]
        if _cfg.get("connected", False):
            _db_lbl = {"oracle": "Oracle", "mariadb": "MariaDB", "postgresql": "PostgreSQL"}.get(
                _cfg.get("db_type", ""), "DB")
            st.markdown(
                f'<div style="font-size:11px;color:#16A34A;padding:2px 0 4px;">'
                f'● 연결됨 &nbsp;·&nbsp; {_db_lbl} &nbsp;·&nbsp; '
                f'{_cfg.get("host","?")}:{_cfg.get("port","?")}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="font-size:11px;color:#DC2626;padding:2px 0 4px;">'
                '● 연결 안됨 &nbsp;—&nbsp; 아래 버튼으로 연결하세요'
                '</div>',
                unsafe_allow_html=True,
            )
        if st.button("Connect to DB", key="sb_conn_btn", type="primary", use_container_width=True):
            st.session_state["_sidebar_conn_open"] = True
            st.rerun()
    else:
        # 파일 업로드 모드: db_type 변수는 기존 config에서 유지
        db_type = st.session_state["db_conn_config"].get("db_type", "oracle")

    st.markdown('<span class="sb-section">생성 방식</span>', unsafe_allow_html=True)
    use_llm = st.toggle(
        "AI 강화 생성",
        value=True,
        help="ON: LLM이 컨텍스트를 이해하여 정교한 쿼리를 생성합니다.\nOFF: 템플릿 기반 즉시 생성 (API 키 불필요)",
        key="global_use_llm",
    )

    st.divider()

    # ── 메뉴 네비게이션 ─────────────────────────────────────
    def _nav(label: str):
        is_active = st.session_state["page"] == label
        if st.button(
            label,
            key=f"nav__{label.replace(' ', '_')}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
        ):
            st.session_state["page"] = label
            st.rerun()

    # [Copilot]
    st.markdown('<div class="sb-group">Copilot</div>', unsafe_allow_html=True)
    _nav("AI 챗봇")

    st.divider()

    # [Automation]
    st.markdown('<div class="sb-group">Automation</div>', unsafe_allow_html=True)
    _nav("데이터 프로파일")
    _nav("검증 쿼리 생성")
    _nav("검증 실행")
    _nav("매핑 자동화")
    _nav("ETL Lineage")

    st.divider()

    # [Modeling]
    st.markdown('<div class="sb-group">Modeling</div>', unsafe_allow_html=True)
    _nav("DW 설계")

    page = st.session_state["page"]

    st.divider()

    # ── 페이지별 도움말 ─────────────────────────────────────
    if page == "AI 챗봇":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">AI 챗봇 안내</b><br><br>
자연어로 ETL 태스크를 요청하세요.<br><br>
<b style="color:#374151;font-weight:600;">지원 기능</b><br>
· 테이블 스키마 조회<br>
· 검증 쿼리 자동 생성<br>
· 데이터 프로파일 분석<br>
· 검증 규칙 자동 제안<br>
· 소스/타겟 건수 비교
</div>""", unsafe_allow_html=True)
    elif page == "데이터 프로파일":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">데이터 프로파일 안내</b><br><br>
DB에 직접 연결하여 테이블의<br>데이터 통계를 분석합니다.<br><br>
<b style="color:#374151;font-weight:600;">수집 항목</b><br>
· NULL 비율, Distinct 수<br>
· 최소/최대값, Top Values<br>
· 도메인 자동 추론<br>
· AI 검증 규칙 자동 제안
</div>""", unsafe_allow_html=True)
    elif page == "검증 쿼리 생성":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">업로드 파일 안내</b><br><br>
<b style="color:#374151;font-weight:600;">매핑정의서</b> (DM/DW/ODS 표준)<br>
Excel 파일 1개로 소스·타겟·매핑 자동 추출<br><br>
<b style="color:#374151;font-weight:600;">테이블 정의서</b> (Excel / CSV)<br>
컬럼명, 데이터타입, PK, NULL여부 포함
</div>""", unsafe_allow_html=True)
    elif page == "검증 실행":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">검증 실행 안내</b><br><br>
SQL을 분류하고 안전하게 실행합니다.<br><br>
<b style="color:#374151;font-weight:600;">핵심 원칙</b><br>
· SELECT → 자동 실행<br>
· DML/DDL → 사용자 승인 후 실행<br>
· 오류 시 AI 진단 + 수정 SQL 제안<br>
· 실행 이력 SQLite 저장
</div>""", unsafe_allow_html=True)
    elif page == "매핑 자동화":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">매핑 자동화 안내</b><br><br>
컬럼 매핑 기반 문서를 자동 생성합니다.<br><br>
<b style="color:#374151;font-weight:600;">생성 항목</b><br>
· 매핑정의서 Excel (6시트)<br>
· DDL Script · MERGE SQL<br>
· 검증 리포트 Excel
</div>""", unsafe_allow_html=True)
    elif page == "ETL Lineage":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">ETL Lineage 안내</b><br><br>
매핑 자동화에서 등록된 파이프라인을<br>인터랙티브 그래프로 시각화합니다.<br><br>
<b style="color:#374151;font-weight:600;">기능</b><br>
· 테이블 노드 클릭 → 컬럼 펼치기<br>
· 레이어별 색상 구분<br>
· ODS → DW → DM 자동 배치<br><br>
<b style="color:#374151;font-weight:600;">등록 방법</b><br>
매핑 자동화 → [Flow Map 생성] 버튼
</div>""", unsafe_allow_html=True)
    elif page == "DW 설계":
        st.markdown("""
<div style="font-size:11px;line-height:1.8;color:#475569;">
<b style="color:#0070C0;font-size:10px;">DW 설계 안내</b><br><br>
AI가 Star Schema를 자동 설계합니다.<br><br>
<b style="color:#374151;font-weight:600;">입력 우선순위</b><br>
· 1순위: Swagger/OpenAPI<br>
· 2순위: 자유 텍스트 (AI 파싱)<br><br>
<b style="color:#374151;font-weight:600;">출력</b><br>
· ODS / Fact / Dim / DM 테이블<br>
· Mermaid ERD + DDL
</div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

@st.cache_resource
def get_llm():
    from etl_sql_generator import _get_llm
    return _get_llm()


# ─────────────────────────────────────────
# DB 연결 다이얼로그 & 상태 바
# ─────────────────────────────────────────

@st.dialog("DB 연결 설정", width="large")
def db_connection_dialog():
    # DB 연결 정보 입력 모달 다이얼로그
    cfg = st.session_state["db_conn_config"]

    st.markdown("#### 데이터베이스 연결 정보")

    db_t = st.selectbox(
        "DB 종류",
        options=["oracle", "mariadb", "postgresql"],
        index=["oracle", "mariadb", "postgresql"].index(cfg.get("db_type", "mariadb")),
        key="dlg_db_type",
    )

    col_h, col_p = st.columns([3, 1])
    with col_h:
        host = st.text_input("Host", value=cfg.get("host", "localhost"), key="dlg_host")
    with col_p:
        port = st.number_input("Port", value=int(cfg.get("port", 3306)),
                               min_value=1, max_value=65535, key="dlg_port")

    col_u, col_pw = st.columns(2)
    with col_u:
        user = st.text_input("Username", value=cfg.get("user", ""), key="dlg_user")
    with col_pw:
        password = st.text_input("Password", value=cfg.get("password", ""),
                                  type="password", key="dlg_pw")

    database = st.text_input(
        "Database / SID",
        value=cfg.get("database", ""),
        key="dlg_db",
        help="Oracle: SID 또는 서비스명 / MariaDB·PostgreSQL: 데이터베이스명",
    )

    if db_t == "oracle":
        owner = st.text_input(
            "스키마(Owner)", value=cfg.get("owner", ""),
            key="dlg_owner",
            help="Oracle 스키마 명 (비워두면 로그인 사용자 기준)",
        )
    elif db_t == "postgresql":
        owner = st.text_input(
            "스키마 필터 (선택)",
            value=cfg.get("owner", ""),
            key="dlg_owner",
            placeholder="예: public,public_marts",
            help="쉼표로 여러 스키마 지정 가능. 비워두면 시스템 스키마를 제외한 전체 스키마 조회",
        )
    else:
        owner = ""

    st.divider()
    col_test, col_save, col_cancel = st.columns([1, 1, 1])

    with col_test:
        if st.button("연결 테스트", key="dlg_test", type="secondary"):
            with st.spinner("연결 확인 중..."):
                ok, msg = _test_db_connection(db_t, host, int(port), user, password, database)
            if ok:
                st.success(f"연결 성공: {msg}")
            else:
                st.error(f"연결 실패: {msg}")

    with col_save:
        if st.button("저장 & 연결", key="dlg_save", type="primary"):
            new_cfg = {
                "db_type": db_t, "host": host, "port": int(port),
                "user": user, "password": password, "database": database,
                "owner": owner, "connected": True,
            }
            st.session_state["db_conn_config"] = new_cfg
            _write_db_config(new_cfg)
            st.rerun()

    with col_cancel:
        if st.button("닫기", key="dlg_cancel"):
            st.rerun()


# ── 사이드바 DB 연결 버튼 트리거 ─────────────────────────
if st.session_state.get("_sidebar_conn_open", False):
    st.session_state["_sidebar_conn_open"] = False
    db_connection_dialog()


def _test_db_connection(db_type: str, host: str, port: int,
                        user: str, password: str, database: str):
    # DB 연결 테스트 — (success: bool, message: str)
    try:
        if db_type == "oracle":
            import oracledb
            dsn = f"{host}:{port}/{database}"
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            conn.close()
            return True, f"Oracle {host}/{database}"
        elif db_type == "mariadb":
            import mariadb
            conn = mariadb.connect(host=host, port=port, user=user,
                                   password=password, database=database)
            conn.close()
            return True, f"MariaDB {host}/{database}"
        elif db_type == "postgresql":
            import psycopg2
            conn = psycopg2.connect(host=host, port=port, user=user,
                                    password=password, dbname=database)
            conn.close()
            return True, f"PostgreSQL {host}/{database}"
        else:
            return False, f"지원하지 않는 DB 종류: {db_type}"
    except Exception as e:
        return False, str(e)


def _write_db_config(cfg: dict):
    """db_config.json 에 연결 정보 저장"""
    doc = {
        "db_type": cfg["db_type"],
        "connection": {
            "host":     cfg["host"],
            "port":     cfg["port"],
            "user":     cfg["user"],
            "password": cfg["password"],
            "database": cfg["database"],
        },
        "schema_options": {
            "owner":          cfg.get("owner") or None,
            "include_tables": [],
            "exclude_tables": [],
            "include_views":  False,
        },
        "cache": {"enabled": True, "ttl_seconds": 3600},
    }
    try:
        with open("db_config.json", "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=4)
    except Exception:
        pass


def render_conn_bar(page_key: str):
    # AI Agent / 데이터 프로파일 페이지 상단 연결 상태 바
    cfg = st.session_state["db_conn_config"]
    is_connected = cfg.get("connected", False)
    db_label = {"oracle": "Oracle", "mariadb": "MariaDB", "postgresql": "PostgreSQL"}.get(
        cfg.get("db_type", ""), cfg.get("db_type", "?"))
    host_info = f"{cfg.get('host','?')}:{cfg.get('port','?')}/{cfg.get('database','?')}"

    col_icon, col_info, col_btn = st.columns([0.4, 5.5, 1.5])

    with col_icon:
        try:
            st.image("documents/conn.png", width=38)
        except Exception:
            st.markdown("🔌")

    with col_info:
        if is_connected:
            st.markdown(
                f'<div class="conn-info">'
                f'<span class="conn-dot-on">●</span> &nbsp;'
                f'<b>{db_label}</b> &nbsp;연결됨 &nbsp;'
                f'<small>({host_info})</small>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="conn-info">'
                f'<span class="conn-dot-off">●</span> &nbsp;'
                f'<b>{db_label}</b> &nbsp;연결 안됨 &nbsp;'
                f'<small>— 연결 설정 버튼을 눌러 DB 정보를 입력하세요.</small>'
                f'</div>',
                unsafe_allow_html=True,
            )

    with col_btn:
        if st.button("🔗  연결 설정", key=f"conn_btn_{page_key}", type="secondary"):
            db_connection_dialog()


def render_table_header(meta: dict, kind: str):
    pk_str  = ", ".join(meta["pk_columns"]) if meta["pk_columns"] else "없음"
    css_cls = "src" if kind == "source" else "tgt"
    badge   = f'<span class="badge-src">SOURCE</span>' if kind == "source" else f'<span class="badge-tgt">TARGET</span>'
    st.markdown(f"""
<div class="tbl-header {css_cls}">
  <span class="tbl-name">{meta['table_name']}</span>
  {badge}
  <span class="tbl-meta">컬럼 {len(meta['columns'])}개 &nbsp;|&nbsp; PK: {pk_str}</span>
</div>
""", unsafe_allow_html=True)


def render_metadata_table(meta: dict, kind: str):
    from etl_metadata_parser import metadata_to_display
    render_table_header(meta, kind)
    df = metadata_to_display(meta)
    st.dataframe(df, width='stretch', height=min(230, 36 * len(df) + 42), hide_index=True)


def render_query_results(queries: dict):
    from etl_sql_generator import QUERY_LABELS

    st.markdown('<div class="step-row"><span class="step-num">3</span><span class="step-text">생성된 검증 쿼리</span></div>', unsafe_allow_html=True)

    for key, info in queries.items():
        label = QUERY_LABELS.get(key, key)
        desc  = info.get("description", "")
        sql   = info.get("sql", "")
        with st.expander(f"{label}  ·  {desc}", expanded=True):
            st.code(sql, language="sql")

    st.divider()
    full_sql = "\n\n".join(
        f"-- ════ {QUERY_LABELS.get(k,'?')} ════\n{v['sql']}"
        for k, v in queries.items()
    )
    col1, col2, _ = st.columns([2, 2, 3])
    with col1:
        st.download_button(
            "SQL 다운로드 (.sql)", data=full_sql,
            file_name="etl_validation_queries.sql", mime="text/plain",
            width='stretch',
        )
    with col2:
        st.download_button(
            "JSON 다운로드", data=json.dumps(queries, ensure_ascii=False, indent=2),
            file_name="etl_validation_queries.json", mime="application/json",
            width='stretch',
        )


# ─────────────────────────────────────────
# Page header
# ─────────────────────────────────────────
_db_badge = {"oracle": "Oracle", "mariadb": "MariaDB", "postgresql": "PostgreSQL"}.get(db_type, db_type)
_page_meta = {
    "AI 챗봇":        ("AETL AI Copilot",
                       "자연어로 ETL 태스크를 요청하면 AI가 도구를 활용하여 자동으로 수행합니다.",
                       "Copilot"),
    "데이터 프로파일": ("Data Profiler",
                        "DB에 직접 연결하여 테이블 데이터 통계 및 품질 규칙을 자동 분석합니다.",
                        "Profiler"),
    "검증 쿼리 생성": ("ETL Validation Query Generator",
                       "테이블 정의서 또는 DB 스키마를 기반으로 ETL 검증 쿼리를 자동 생성합니다.",
                       "AI 생성" if use_llm else "템플릿 생성"),
    "검증 실행":      ("SQL 검증 실행기",
                       "SQL을 분류하고 안전하게 실행합니다. 오류 시 AI가 원인 분석 및 수정 방안을 제안합니다.",
                       "Human-in-the-Loop"),
    "매핑 자동화":    ("매핑 자동화 & 문서 생성",
                       "컬럼 매핑을 기반으로 매핑정의서 Excel, DDL, MERGE SQL, 검증 리포트를 자동 생성합니다.",
                       "Export Engine"),
    "ETL Lineage":    ("ETL Pipeline Lineage",
                       "등록된 매핑 기반 파이프라인을 React Flow 인터랙티브 그래프로 시각화합니다.",
                       "Lineage"),
    "DW 설계":        ("DW Star Schema 설계",
                       "Swagger/OpenAPI 또는 텍스트에서 ODS/DW/DM 스타 스키마를 AI가 자동 설계합니다.",
                       "DW Designer"),
}
_title, _subtitle, _badge_text = _page_meta.get(page, ("ETL Validator", "", ""))
st.markdown(f"""
<div class="page-header">
  <div>
    <p class="page-title">{_title}</p>
    <p class="page-subtitle">{_subtitle}</p>
  </div>
  <span class="header-badge">{_db_badge} &nbsp;|&nbsp; {_badge_text}</span>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# AI 챗봇 페이지
# ═══════════════════════════════════════════════════════════
if page == "AI 챗봇":
    # DB 연결 안내 (연결 안됐을 때만)
    if st.session_state["global_mode"] == "DB 직접 연결" and \
            not st.session_state["db_conn_config"].get("connected", False):
        st.info(
            "ℹ️ DB가 연결되지 않았습니다. 사이드바의 **[Connect to DB]** 버튼으로 연결하면 "
            "테이블 스키마 조회·프로파일링 등 DB 기반 기능을 사용할 수 있습니다.",
            icon=None,
        )

    st.markdown('<div class="step-row"><span class="step-num">1</span><span class="step-text">AI Agent 대화</span></div>', unsafe_allow_html=True)
    st.caption("자연어로 ETL 관련 질문이나 작업을 입력하세요. AI가 도구를 호출하여 자동으로 처리합니다.")

    # 대화 이력 표시
    chat_container = st.container(height=420)
    with chat_container:
        if not st.session_state["agent_history"]:
            st.markdown("""
<div style="text-align:center;padding:40px 20px;color:#64748B;">
  <div style="font-size:32px;margin-bottom:12px;">💬</div>
  <div style="font-size:14px;font-weight:600;margin-bottom:8px;">AETL AI Agent</div>
  <div style="font-size:12px;line-height:1.8;">
    예시 질문:<br>
    · "CUSTOMER 테이블의 스키마를 조회해줘"<br>
    · "ODS_SALES → DW_SALES 검증 쿼리 생성해줘"<br>
    · "ORDERS 테이블 데이터 품질 규칙을 제안해줘"<br>
    · "'BRC' 키워드로 테이블을 검색해줘"
  </div>
</div>""", unsafe_allow_html=True)
        else:
            for msg in st.session_state["agent_history"]:
                role = msg["role"]
                content = msg["content"]
                if role == "user":
                    st.markdown(f"""
<div style="display:flex;justify-content:flex-end;margin:8px 0;">
  <div style="background:#0070C0;color:#FFFFFF;padding:10px 14px;border-radius:12px 12px 2px 12px;
              max-width:75%;font-size:13px;line-height:1.5;">{content}</div>
</div>""", unsafe_allow_html=True)
                else:
                    st.markdown(f"""
<div style="display:flex;justify-content:flex-start;margin:8px 0;">
  <div style="background:#F0F4F8;color:#1A202C;padding:10px 14px;border-radius:12px 12px 12px 2px;
              max-width:85%;font-size:13px;line-height:1.6;border:1px solid #D0D9E4;">{content}</div>
</div>""", unsafe_allow_html=True)

    # 입력 폼
    with st.form("agent_form", clear_on_submit=True):
        col_input, col_btn = st.columns([8, 1])
        with col_input:
            user_input = st.text_input(
                "메시지", placeholder="ETL 관련 질문을 입력하세요...",
                label_visibility="collapsed",
            )
        with col_btn:
            submitted = st.form_submit_button("전송", type="primary", width='stretch')

    if submitted and user_input.strip():
        with st.spinner("AI Agent 처리 중..."):
            try:
                from aetl_agent import run_agent
                _agent_db_type = st.session_state["db_conn_config"].get("db_type", "oracle")
                answer, updated_history = run_agent(
                    user_message=user_input.strip(),
                    db_type=_agent_db_type,
                    chat_history=st.session_state["agent_history"],
                )
                st.session_state["agent_history"] = updated_history
                st.rerun()
            except Exception as e:
                st.error(f"Agent 오류: {e}")
                st.code(traceback.format_exc())

    # 대화 초기화
    col_clear, _ = st.columns([2, 6])
    with col_clear:
        if st.button("대화 초기화", key="clear_agent"):
            st.session_state["agent_history"] = []
            st.rerun()

    st.stop()


# ═══════════════════════════════════════════════════════════
# 데이터 프로파일 페이지
# ═══════════════════════════════════════════════════════════
if page == "데이터 프로파일":
    # 파일 업로드 모드에서는 DB 프로파일링 불가 안내
    if st.session_state["global_mode"] == "파일 업로드":
        st.warning(
            "⚠️ **데이터 프로파일**은 DB 직접 연결이 필요합니다.\n\n"
            "사이드바에서 **'DB 직접 연결'** 을 선택하고 **[Connect to DB]** 버튼으로 연결 정보를 입력하세요.",
            icon=None,
        )
        st.stop()

    # DB 연결 여부 확인
    _profile_cfg = st.session_state["db_conn_config"]
    if not _profile_cfg.get("connected", False):
        st.info(
            "ℹ️ DB가 연결되지 않았습니다. 사이드바의 **[Connect to DB]** 버튼으로 먼저 연결하세요.",
            icon=None,
        )

    st.markdown('<div class="step-row"><span class="step-num">1</span><span class="step-text">프로파일링 대상 설정</span></div>', unsafe_allow_html=True)

    col_tbl, col_btn_p = st.columns([4, 1])
    with col_tbl:
        profile_table_name = st.text_input(
            "테이블명",
            placeholder="예: TB_CUSTOMER",
            help="DB에 존재하는 테이블명을 입력하세요.",
        )
    with col_btn_p:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        run_profile = st.button("프로파일 실행", type="primary", width='stretch')

    if run_profile and profile_table_name.strip():
        with st.spinner(f"{profile_table_name} 프로파일링 중..."):
            try:
                from aetl_profiler import profile_table_from_config
                st.session_state["profile_result"] = profile_table_from_config(
                    "db_config.json", profile_table_name.strip(), top_n=10
                )
                st.session_state["profile_rules"] = None
                st.success(f"프로파일링 완료: {st.session_state['profile_result']['row_count']:,} 건")
            except Exception as e:
                st.error(f"프로파일링 오류: {e}")
                st.code(traceback.format_exc())

    if st.session_state["profile_result"]:
        import pandas as pd
        profile = st.session_state["profile_result"]

        st.markdown('<div class="step-row"><span class="step-num">2</span><span class="step-text">프로파일 결과</span></div>', unsafe_allow_html=True)

        # 요약 카드
        col_r1, col_r2, col_r3 = st.columns(3)
        col_cnt = len(profile["columns"])
        null_cols = sum(1 for c in profile["columns"] if c["null_pct"] > 0)
        dom_counts: dict = {}
        for c in profile["columns"]:
            d = c.get("inferred_domain", "unknown")
            dom_counts[d] = dom_counts.get(d, 0) + 1
        top_domain = max(dom_counts, key=lambda k: dom_counts[k]) if dom_counts else "-"

        for col_w, metric, val in [
            (col_r1, "전체 건수",    f"{profile['row_count']:,}"),
            (col_r2, "컬럼 수",      f"{col_cnt}개  (NULL존재: {null_cols}개)"),
            (col_r3, "주요 도메인",  top_domain),
        ]:
            with col_w:
                st.markdown(f"""
<div class="card" style="text-align:center;">
  <div style="font-size:11px;color:#64748B;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;">{metric}</div>
  <div style="font-size:20px;font-weight:700;color:#0070C0;margin-top:6px;">{val}</div>
</div>""", unsafe_allow_html=True)

        # 컬럼 상세 테이블
        rows = []
        for c in profile["columns"]:
            rows.append({
                "컬럼명":        c["name"],
                "타입":          c["type"],
                "도메인":        c.get("inferred_domain", "-"),
                "NULL%":         f"{c['null_pct']*100:.1f}%",
                "Distinct":      f"{c['distinct_count']:,}",
                "최소값":        c.get("min") or "-",
                "최대값":        c.get("max") or "-",
            })
        df_profile = pd.DataFrame(rows)
        st.dataframe(df_profile, width='stretch', hide_index=True, height=min(420, 36 * len(rows) + 42))

        # Top Values expander (컬럼별)
        with st.expander("컬럼별 상위 빈도 값 상세"):
            for c in profile["columns"]:
                if c.get("top_values"):
                    tv_df = pd.DataFrame(c["top_values"])
                    tv_df.columns = ["값", "건수"]
                    st.caption(f"**{c['name']}** ({c['type']})")
                    st.dataframe(tv_df, width='stretch', hide_index=True, height=180)

        # 규칙 자동 제안
        st.markdown('<div class="step-row"><span class="step-num">3</span><span class="step-text">검증 규칙 자동 제안</span></div>', unsafe_allow_html=True)

        col_tgt_p, col_btn_r = st.columns([4, 1])
        with col_tgt_p:
            target_for_rules = st.text_input(
                "비교 타겟 테이블명 (선택)",
                placeholder="없으면 소스 기반 단독 규칙만 생성",
                key="profile_tgt_table",
            )
        with col_btn_r:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            suggest_btn = st.button("규칙 자동 제안", type="primary", width='stretch')

        if suggest_btn:
            with st.spinner("AI 규칙 분석 중..."):
                try:
                    from etl_sql_generator import suggest_validation_rules
                    tgt_profile = None
                    if target_for_rules.strip():
                        from aetl_profiler import profile_table_from_config
                        tgt_profile = profile_table_from_config("db_config.json", target_for_rules.strip(), top_n=5)
                    st.session_state["profile_rules"] = suggest_validation_rules(
                        profile, tgt_profile, db_type
                    )
                    st.success(f"{len(st.session_state['profile_rules'])}개 규칙 자동 제안 완료")
                except Exception as e:
                    st.error(f"규칙 제안 오류: {e}")
                    st.code(traceback.format_exc())

        if st.session_state["profile_rules"]:
            rules = st.session_state["profile_rules"]
            tier_labels = {1: "기술검증", 2: "정합성", 3: "비즈니스"}
            severity_colors = {"CRITICAL": "#DC2626", "WARNING": "#D97706", "INFO": "#2563EB"}

            rules_df = pd.DataFrame([{
                "Tier":    tier_labels.get(r["tier"], "?"),
                "규칙명":  r["rule_name"],
                "유형":    r["rule_type"],
                "심각도":  r["severity"],
                "근거":    r["reason"],
            } for r in rules])
            st.dataframe(rules_df, width='stretch', hide_index=True)

            # 규칙 SQL 상세
            with st.expander(f"규칙 SQL 상세 ({len(rules)}건)"):
                for r in rules:
                    color = severity_colors.get(r["severity"], "#374151")
                    st.markdown(
                        f'<div style="font-size:12px;font-weight:600;color:{color};margin-top:8px;">'
                        f'[{tier_labels.get(r["tier"],"?")}] {r["rule_name"]}</div>',
                        unsafe_allow_html=True,
                    )
                    st.code(r["sql"], language="sql")

            # 저장 버튼
            col_save, _ = st.columns([3, 5])
            with col_save:
                if st.button("규칙 DB 저장", key="save_rules_btn", type="primary"):
                    try:
                        from aetl_store import get_or_create_datasource, save_validation_rules
                        src_id = get_or_create_datasource("default", db_type, "db_config.json")
                        ids = save_validation_rules(rules)
                        st.success(f"{len(ids)}개 규칙 저장 완료 (rule_id: {ids[0]}~{ids[-1]})")
                    except Exception as e:
                        st.error(f"저장 오류: {e}")

    st.stop()


# ═══════════════════════════════════════════════════════════
# 검증 실행 페이지
# ═══════════════════════════════════════════════════════════
if page == "검증 실행":
    import pandas as pd
    exec_db_type = st.session_state["db_conn_config"].get("db_type", "oracle")

    st.markdown('<div class="step-row"><span class="step-num">1</span><span class="step-text">SQL 입력 및 분류</span></div>', unsafe_allow_html=True)

    sql_input = st.text_area(
        "SQL 입력", height=200,
        placeholder="SELECT, DML, DDL 등 SQL을 입력하세요...",
        key="exec_sql_input",
    )

    col_classify, col_exec_btn, col_dml_btn, _ = st.columns([2, 2, 3, 2])
    with col_classify:
        if st.button("SQL 분류", key="btn_classify", type="secondary"):
            if sql_input.strip():
                from aetl_executor import classify_sql
                st.session_state["exec_sql_type"] = classify_sql(sql_input, exec_db_type)

    if "exec_sql_type" in st.session_state and st.session_state["exec_sql_type"]:
        sql_type_val = st.session_state["exec_sql_type"]
        type_colors = {"SELECT": "#16A34A", "DML": "#D97706", "DDL": "#DC2626", "UNKNOWN": "#6B7280"}
        color = type_colors.get(sql_type_val, "#6B7280")
        st.markdown(
            f'<span style="background:{color};color:#fff;padding:4px 12px;border-radius:12px;'
            f'font-size:12px;font-weight:700;">{sql_type_val}</span>',
            unsafe_allow_html=True,
        )

    st.divider()
    st.markdown('<div class="step-row"><span class="step-num">2</span><span class="step-text">실행 (Human-in-the-Loop)</span></div>', unsafe_allow_html=True)
    st.caption("⚠ SELECT는 자동 실행 가능합니다. DML/DDL은 반드시 SQL 내용을 확인한 후 아래 [DML/DDL 실행 승인] 버튼을 클릭하세요.")

    col_sel_btn, col_dml_approve, _ = st.columns([2, 3, 3])
    with col_sel_btn:
        run_select_btn = st.button("SELECT 실행", key="btn_exec_select", type="primary")
    with col_dml_approve:
        approve_dml_btn = st.button(
            "⚠ DML/DDL 실행 승인 (확인 완료)", key="btn_exec_dml", type="secondary",
            help="이 버튼은 DML/DDL을 직접 실행합니다. SQL 내용을 반드시 확인 후 클릭하세요.",
        )

    if run_select_btn and sql_input.strip():
        with st.spinner("SELECT 실행 중..."):
            try:
                from aetl_executor import execute_query
                st.session_state["exec_result"] = execute_query(sql_input.strip(), row_limit=500)
                st.session_state["exec_diagnosis"] = None
            except Exception as e:
                st.error(f"실행 오류: {e}")

    if approve_dml_btn and sql_input.strip():
        with st.spinner("DML/DDL 실행 중..."):
            try:
                from aetl_executor import execute_dml
                dml_result = execute_dml(sql_input.strip())
                if dml_result["ok"]:
                    st.success(f"실행 완료 — 영향 행수: {dml_result['affected_rows']} / 소요시간: {dml_result['elapsed_sec']}초")
                else:
                    st.error(f"실행 실패: {dml_result['error']}")
            except Exception as e:
                st.error(f"실행 오류: {e}")

    if st.session_state["exec_result"]:
        result = st.session_state["exec_result"]
        st.markdown('<div class="step-row"><span class="step-num">3</span><span class="step-text">실행 결과</span></div>', unsafe_allow_html=True)

        if result["ok"]:
            col_m1, col_m2, col_m3 = st.columns(3)
            col_m1.metric("결과 건수", f"{result['row_count']:,}건")
            col_m2.metric("소요 시간", f"{result['elapsed_sec']}초")
            col_m3.metric("SQL 유형", result["sql_type"])
            if result["columns"] and result["rows"]:
                df_result = pd.DataFrame(result["rows"], columns=result["columns"])
                st.dataframe(df_result, width='stretch', height=min(400, 36 * len(df_result) + 42), hide_index=True)
        else:
            st.error(f"오류: {result['error']}")
            st.markdown('<div class="step-row"><span class="step-num">4</span><span class="step-text">AI 오류 진단</span></div>', unsafe_allow_html=True)
            st.caption("AI가 오류 원인을 분석하고 수정 SQL을 '제안'합니다. 실행은 사용자가 결정합니다.")
            col_diag_src, col_diag_tgt = st.columns(2)
            with col_diag_src:
                diag_src = st.text_input("소스 테이블명", key="diag_src")
            with col_diag_tgt:
                diag_tgt = st.text_input("타겟 테이블명", key="diag_tgt")
            if st.button("AI 진단 요청", key="btn_diagnose", type="primary"):
                with st.spinner("AI 진단 중..."):
                    try:
                        from aetl_executor import diagnose_failure
                        st.session_state["exec_diagnosis"] = diagnose_failure(
                            "실행 오류", result,
                            diag_src or "SOURCE", diag_tgt or "TARGET", exec_db_type,
                        )
                    except Exception as e:
                        st.error(f"진단 오류: {e}")

            if st.session_state["exec_diagnosis"]:
                diag = st.session_state["exec_diagnosis"]
                confidence = diag.get("confidence", "LOW")
                conf_color = {"HIGH": "#16A34A", "MEDIUM": "#D97706", "LOW": "#DC2626"}.get(confidence, "#6B7280")
                st.markdown(
                    f'<div class="card">'
                    f'<div style="font-size:11px;color:#64748B;margin-bottom:8px;">'
                    f'신뢰도: <span style="color:{conf_color};font-weight:700;">{confidence}</span></div>'
                    f'<div style="font-size:13px;line-height:1.6;">{diag.get("diagnosis","")}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                if diag.get("probing_results"):
                    with st.expander("근거 조회 결과"):
                        for pr in diag["probing_results"]:
                            st.caption(pr.get("purpose", ""))
                            st.code(pr.get("sql", ""), language="sql")
                            pr_res = pr.get("result", {})
                            if pr_res.get("ok") and pr_res.get("rows"):
                                st.dataframe(pd.DataFrame(pr_res["rows"], columns=pr_res["columns"]),
                                             width='stretch', hide_index=True, height=150)
                if diag.get("fix_sqls"):
                    st.markdown("**수정 SQL 제안** (위 SQL 입력란에 붙여넣기 후 승인 버튼 클릭)")
                    for i, fix in enumerate(diag["fix_sqls"]):
                        with st.expander(f"방안 {i+1}: {fix.get('description','')}"):
                            st.code(fix.get("sql", ""), language="sql")

    with st.expander("실행 이력 (최근 20건)"):
        try:
            from aetl_executor import get_execution_log
            logs = get_execution_log(20)
            if logs:
                df_log = pd.DataFrame(logs)
                show_cols = [c for c in ["executed_at", "sql_type", "status", "affected_rows", "sql_text"] if c in df_log.columns]
                df_log = df_log[show_cols].copy()
                if "sql_text" in df_log.columns:
                    df_log["sql_text"] = df_log["sql_text"].str[:60] + "..."
                st.dataframe(df_log, width='stretch', hide_index=True, height=200)
            else:
                st.info("실행 이력이 없습니다.")
        except Exception as e:
            st.warning(f"이력 조회 오류: {e}")

    st.stop()


# ═══════════════════════════════════════════════════════════
# 매핑 자동화 페이지  (v2 — Single Source of Truth + Template Profile)
# ═══════════════════════════════════════════════════════════
if page == "매핑 자동화":
    import pandas as pd
    from aetl_export import (
        generate_mapping_excel, generate_merge_sql, generate_ddl,
        generate_validation_report, generate_mapping_json, generate_mapping_csv,
    )
    from etl_sql_generator import generate_validation_queries_no_llm
    import aetl_template_profile as tpl_engine

    # ── Step 1: 메타데이터 ──────────────────────────────────
    st.markdown('<div class="step-row"><span class="step-num">1</span>'
                '<span class="step-text">소스 / 타겟 메타데이터</span></div>',
                unsafe_allow_html=True)

    src_meta = st.session_state.get("source_meta")
    tgt_meta = st.session_state.get("target_meta")

    if not src_meta or not tgt_meta:
        st.info("소스/타겟 메타데이터가 없습니다. **검증 쿼리 생성** 페이지에서 파일을 업로드하거나 샘플을 로드하세요.")
        if st.button("샘플 데이터 로드", key="mapping_sample_load"):
            st.session_state.source_meta = {
                "table_name": "ODS_ORDER",
                "columns": [
                    {"name": "ORDER_ID",  "type": "VARCHAR2(20)", "nullable": False, "description": "주문ID"},
                    {"name": "CUST_ID",   "type": "VARCHAR2(20)", "nullable": False, "description": "고객ID"},
                    {"name": "ORDER_AMT", "type": "NUMBER(15,2)", "nullable": True,  "description": "주문금액"},
                    {"name": "ORDER_DT",  "type": "DATE",         "nullable": False, "description": "주문일자"},
                    {"name": "STATUS_CD", "type": "CHAR(1)",      "nullable": False, "description": "상태코드"},
                ],
                "pk_columns": ["ORDER_ID"],
            }
            st.session_state.target_meta = {
                "table_name": "DW_FACT_ORDER",
                "columns": [
                    {"name": "ORDER_SK",   "type": "NUMBER(10)",   "nullable": False, "description": "주문 대리키"},
                    {"name": "ORDER_ID",   "type": "VARCHAR2(20)", "nullable": False, "description": "주문ID"},
                    {"name": "CUST_ID",    "type": "VARCHAR2(20)", "nullable": False, "description": "고객ID"},
                    {"name": "ORDER_AMT",  "type": "NUMBER(15,2)", "nullable": True,  "description": "주문금액"},
                    {"name": "ORDER_DATE", "type": "DATE",         "nullable": False, "description": "주문일자"},
                    {"name": "STATUS_CD",  "type": "CHAR(1)",      "nullable": False, "description": "상태코드"},
                    {"name": "ETL_DT",     "type": "TIMESTAMP",    "nullable": True,  "description": "ETL 적재일시"},
                ],
                "pk_columns": ["ORDER_SK"],
            }
            st.session_state.mapping = None
            st.rerun()
        st.stop()

    col_src_m, col_arr_m, col_tgt_m = st.columns([10, 1, 10])
    with col_src_m:
        render_metadata_table(src_meta, "source")
    with col_arr_m:
        st.markdown("<div style='text-align:center;padding-top:56px;font-size:20px;color:#A8BCCC;'>→</div>",
                    unsafe_allow_html=True)
    with col_tgt_m:
        render_metadata_table(tgt_meta, "target")

    # ── Step 2: 컬럼 매핑 편집 (Single Source of Truth) ────
    st.markdown('<div class="step-row"><span class="step-num">2</span>'
                '<span class="step-text">컬럼 매핑 편집 — Single Source of Truth</span></div>',
                unsafe_allow_html=True)
    st.caption("이 표가 모든 산출물의 기반입니다. 수정하면 아래 MERGE SQL 미리보기가 즉시 반영됩니다.")

    existing_mapping = st.session_state.get("mapping") or []
    src_col_names = {c["name"] for c in src_meta["columns"]}

    if existing_mapping and isinstance(existing_mapping[0], dict) and "타겟 컬럼" in existing_mapping[0]:
        mapping_rows = existing_mapping
    else:
        mapping_rows = []
        for tc in tgt_meta["columns"]:
            src_match = tc["name"] if tc["name"] in src_col_names else ""
            mapping_rows.append({
                "타겟 컬럼": tc["name"],
                "소스 컬럼": src_match,
                "변환식":   "",
                "필수":     not tc.get("nullable", True),
                "비고":     tc.get("description", ""),
            })

    mapping_df = pd.DataFrame(mapping_rows)
    if "타겟 컬럼" not in mapping_df.columns:
        mapping_df = pd.DataFrame([{
            "타겟 컬럼": r.get("target_col", ""),
            "소스 컬럼": r.get("source_col", ""),
            "변환식":   r.get("transform", ""),
            "필수":     r.get("required", False),
            "비고":     r.get("description", ""),
        } for r in mapping_rows])

    edited_mapping = st.data_editor(
        mapping_df,
        height=min(420, 40 * len(mapping_df) + 50),
        hide_index=True,
        key="mapping_editor",
        column_config={
            "타겟 컬럼": st.column_config.TextColumn("타겟 컬럼", width="medium"),
            "소스 컬럼": st.column_config.TextColumn("소스 컬럼", width="medium"),
            "변환식":   st.column_config.TextColumn("변환식 (SQL)", width="large"),
            "필수":     st.column_config.CheckboxColumn("필수", width="small"),
            "비고":     st.column_config.TextColumn("비고", width="large"),
        },
        num_rows="dynamic",
    )

    # SOT: 매핑 테이블에서 파생된 col_mappings — 모든 산출물의 원천
    col_mappings = [
        {
            "target_col":  str(row.get("타겟 컬럼", "")),
            "source_col":  str(row.get("소스 컬럼", "")),
            "transform":   str(row.get("변환식", "")),
            "required":    bool(row.get("필수", False)),
            "description": str(row.get("비고", "")),
        }
        for _, row in edited_mapping.iterrows()
        if str(row.get("타겟 컬럼", "")).strip()
    ]

    # MERGE SQL 자동 미리보기 (버튼 없이 실시간 반영)
    _db_type_preview = st.session_state.get("db_conn_config", {}).get("db_type", "oracle")
    with st.expander("MERGE SQL 미리보기 (매핑 수정 시 자동 갱신)", expanded=False):
        try:
            _preview_sql = generate_merge_sql(src_meta, tgt_meta, col_mappings, _db_type_preview)
            st.code(_preview_sql, language="sql")
        except Exception as _e:
            st.warning(f"미리보기 생성 실패: {_e}")

    # ── Step 3: 생성 설정 + 전체 산출물 생성 ────────────────
    st.markdown('<div class="step-row"><span class="step-num">3</span>'
                '<span class="step-text">산출물 생성</span></div>',
                unsafe_allow_html=True)

    col_mid, col_auth, col_dbtype_exp = st.columns([3, 3, 2])
    with col_mid:
        mapping_id = st.text_input("매핑 ID", value="MAP_001", key="export_mapping_id")
    with col_auth:
        author = st.text_input("작성자", value="AETL", key="export_author")
    with col_dbtype_exp:
        export_db_type = st.selectbox(
            "DB 종류", options=["oracle", "mariadb", "postgresql"],
            index=["oracle", "mariadb", "postgresql"].index(
                st.session_state["db_conn_config"].get("db_type", "oracle")),
            key="export_db_type",
        )

    # ── [산출물 생성] + [Flow Map 생성] — 두 버튼으로 분리 ──
    _btn_col1, _btn_col2, _btn_col3 = st.columns([3, 1, 2])
    with _btn_col1:
        _do_export = st.button("산출물 생성", key="gen_all_exports", type="primary", use_container_width=True)
    with _btn_col2:
        _selected_load_type = st.selectbox(
            "로드 유형", ["MERGE", "INSERT", "FULL_LOAD", "UPSERT"],
            key="export_load_type", label_visibility="collapsed",
        )
    with _btn_col3:
        _do_flowmap = st.button("Flow Map 생성", key="gen_flow_map", type="secondary", use_container_width=True)
        st.caption("'ETL Lineage' 메뉴에서 Lineage 시각화를 생성합니다.")

    if _do_export:
        with st.spinner("산출물 생성 중..."):
            try:
                # 1) MERGE SQL (SOT → 파생)
                merge_sql = generate_merge_sql(src_meta, tgt_meta, col_mappings, export_db_type)
                st.session_state["export_merge_sql"] = merge_sql

                # 2) 검증 SQL
                val_qs = generate_validation_queries_no_llm(src_meta, tgt_meta, col_mappings, export_db_type)

                # 3) 매핑정의서 Excel (MERGE SQL 포함 — Stage 1 fix)
                st.session_state["export_excel_bytes"] = generate_mapping_excel(
                    src_meta, tgt_meta, col_mappings,
                    merge_sql,   # ← Stage 1: 빈 문자열이 아닌 실제 MERGE SQL 전달
                    val_qs, mapping_id, author,
                )

                # 4) DDL
                ddl_src = generate_ddl(src_meta, export_db_type)
                ddl_tgt = generate_ddl(tgt_meta, export_db_type)
                st.session_state["export_ddl"] = f"-- Source\n{ddl_src}\n\n-- Target\n{ddl_tgt}"

                # 5) 검증 리포트 (빈 결과로 초기 생성)
                sample_results = [{"rule_name": "건수 비교", "status": "PASS",
                                   "actual_value": "—", "expected_value": "—", "sql": ""}]
                st.session_state["export_report_bytes"] = generate_validation_report(
                    sample_results, mapping_id, src_meta["table_name"], tgt_meta["table_name"]
                )

                # 6) JSON raw export (Stage 2)
                st.session_state["export_json_str"] = generate_mapping_json(
                    src_meta, tgt_meta, col_mappings, merge_sql, val_qs, mapping_id
                )

                # 7) CSV raw export (Stage 2)
                st.session_state["export_csv_str"] = generate_mapping_csv(
                    src_meta, tgt_meta, col_mappings
                )

                st.success("산출물 생성 완료! 아래에서 다운로드하세요.")
            except Exception as e:
                st.error(f"생성 오류: {e}")
                st.code(traceback.format_exc())

    if _do_flowmap:
        try:
            # ETL Flow Map 등록 (SOT 기반 mapping_result 추가/업데이트)
            _flow_entry = {
                "mapping_id":  mapping_id,
                "source_meta": src_meta,
                "target_meta": tgt_meta,
                "load_type":   st.session_state.get("export_load_type", "MERGE"),
            }
            _existing = st.session_state["flow_map_mappings"]
            _ids = [m["mapping_id"] for m in _existing]
            if mapping_id in _ids:
                _existing[_ids.index(mapping_id)] = _flow_entry
            else:
                _existing.append(_flow_entry)
            st.session_state["flow_map_mappings"] = _existing
            st.success(f"Flow Map 등록 완료! 'ETL Lineage' 메뉴에서 확인하세요.")
        except Exception as e:
            st.error(f"Flow Map 등록 오류: {e}")

    # ── 다운로드 버튼 ────────────────────────────────────────
    st.divider()
    dl_cols = st.columns(4)
    with dl_cols[0]:
        if st.session_state["export_excel_bytes"]:
            st.download_button(
                "매핑정의서 Excel",
                data=st.session_state["export_excel_bytes"],
                file_name=f"{mapping_id}_mapping.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    with dl_cols[1]:
        if st.session_state["export_ddl"]:
            st.download_button(
                "DDL Script (.sql)",
                data=st.session_state["export_ddl"],
                file_name=f"{mapping_id}_ddl.sql",
                mime="text/plain",
                use_container_width=True,
            )
    with dl_cols[2]:
        if st.session_state["export_merge_sql"]:
            st.download_button(
                "MERGE SQL (.sql)",
                data=st.session_state["export_merge_sql"],
                file_name=f"{mapping_id}_merge.sql",
                mime="text/plain",
                use_container_width=True,
            )
    with dl_cols[3]:
        if st.session_state["export_report_bytes"]:
            st.download_button(
                "검증 리포트 Excel",
                data=st.session_state["export_report_bytes"],
                file_name=f"{mapping_id}_validation_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    # Raw export (Stage 2)
    raw_cols = st.columns(2)
    with raw_cols[0]:
        if st.session_state["export_json_str"]:
            st.download_button(
                "JSON Raw Export",
                data=st.session_state["export_json_str"],
                file_name=f"{mapping_id}_mapping.json",
                mime="application/json",
                use_container_width=True,
            )
    with raw_cols[1]:
        if st.session_state["export_csv_str"]:
            st.download_button(
                "CSV Raw Export",
                data=st.session_state["export_csv_str"],
                file_name=f"{mapping_id}_mapping.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # SQL 미리보기 (토글)
    if st.session_state["export_ddl"] or st.session_state["export_merge_sql"]:
        with st.expander("생성된 SQL 보기", expanded=False):
            if st.session_state["export_ddl"]:
                st.markdown("**DDL**")
                st.code(st.session_state["export_ddl"], language="sql")
            if st.session_state["export_merge_sql"]:
                st.markdown("**MERGE SQL**")
                st.code(st.session_state["export_merge_sql"], language="sql")

    # ── Step 4: Template Profile (Stage 3) ──────────────────
    st.divider()
    st.markdown('<div class="step-row"><span class="step-num">4</span>'
                '<span class="step-text">Template Profile — 회사 양식 등록 & 적용</span></div>',
                unsafe_allow_html=True)
    st.caption("회사 고유 엑셀 양식을 한 번 등록하면, 이후 매핑 데이터를 해당 양식에 자동으로 기입합니다.")

    tpl_tab_apply, tpl_tab_register = st.tabs(["기존 프로파일 적용", "새 양식 등록"])

    # ── 탭 A: 기존 프로파일로 Export ──
    with tpl_tab_apply:
        profiles = tpl_engine.list_profiles()
        if not profiles:
            st.info("등록된 프로파일이 없습니다. '새 양식 등록' 탭에서 회사 양식을 등록하세요.")
        else:
            col_p1, col_p2 = st.columns([3, 1])
            with col_p1:
                selected_profile = st.selectbox("프로파일 선택", options=profiles, key="tpl_select")
            with col_p2:
                if st.button("프로파일 삭제", key="tpl_delete", type="secondary"):
                    tpl_engine.delete_profile(selected_profile)
                    st.success(f"'{selected_profile}' 삭제 완료")
                    st.rerun()

            if st.button("이 양식으로 Export", key="tpl_apply_btn", type="primary"):
                with st.spinner("양식에 데이터 기입 중..."):
                    try:
                        profile_data, tpl_bytes = tpl_engine.load_profile(selected_profile)
                        if not profile_data:
                            st.error("프로파일 파일을 찾을 수 없습니다.")
                        else:
                            # SOT에서 MERGE SQL / 검증 SQL 파생
                            _merge_sql = generate_merge_sql(src_meta, tgt_meta, col_mappings, export_db_type)
                            _val_qs    = generate_validation_queries_no_llm(src_meta, tgt_meta, col_mappings, export_db_type)
                            mapping_result = {
                                "mapping_id":      mapping_id,
                                "author":          author,
                                "load_type":       "MERGE",
                                "source_meta":     src_meta,
                                "target_meta":     tgt_meta,
                                "column_mappings": col_mappings,
                                "load_sql":        _merge_sql,
                                "validation_sqls": _val_qs if isinstance(_val_qs, list)
                                                   else [{"name": k, "sql": v.get("sql",""), "expected": ""}
                                                         for k, v in _val_qs.items()],
                            }
                            result_bytes = tpl_engine.apply_profile(profile_data, tpl_bytes, mapping_result)
                            st.download_button(
                                f"'{selected_profile}' 양식으로 다운로드",
                                data=result_bytes,
                                file_name=f"{mapping_id}_{selected_profile}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )
                            st.success("기입 완료! 다운로드 버튼을 클릭하세요.")
                    except Exception as e:
                        st.error(f"Template 적용 오류: {e}")
                        st.code(traceback.format_exc())

    # ── 탭 B: 새 양식 등록 ──
    with tpl_tab_register:
        st.caption("빈 엑셀 양식을 업로드하면 AETL이 헤더를 분석하고 필드 매핑을 제안합니다. 확인 후 저장하세요.")

        tpl_upload = st.file_uploader(
            "회사 빈 양식 업로드 (.xlsx)",
            type=["xlsx", "xls"],
            key="tpl_upload_file",
        )

        if tpl_upload:
            tpl_bytes_raw = tpl_upload.read()

            if st.button("양식 분석", key="tpl_analyze_btn", type="primary"):
                with st.spinner("헤더 분석 중..."):
                    try:
                        structure = tpl_engine.detect_template_structure(tpl_bytes_raw)
                        st.session_state["tpl_structure"] = structure
                        st.session_state["tpl_bytes"]     = tpl_bytes_raw

                        # 시트별 초안 제안 생성
                        suggestions: dict[str, dict] = {}
                        for sname, sinfo in structure.items():
                            suggestions[sname] = tpl_engine.suggest_field_mapping(sinfo["headers"])
                        st.session_state["tpl_suggestions"] = suggestions
                        st.success(f"분석 완료 — {len(structure)}개 시트 감지")
                    except Exception as e:
                        st.error(f"분석 오류: {e}")

        if st.session_state.get("tpl_structure"):
            structure   = st.session_state["tpl_structure"]
            suggestions = st.session_state.get("tpl_suggestions", {})

            st.markdown("**헤더 → AETL 필드 매핑 확인** (틀린 항목은 드롭다운으로 수정하세요)")

            field_options = ["__ignore__"] + list(tpl_engine.AETL_FIELDS.keys())
            field_labels  = {k: f"{k}  ({v})" for k, v in tpl_engine.AETL_FIELDS.items()}
            field_labels["__ignore__"] = "— 무시 —"

            sheet_configs_final: list[dict] = []

            for sheet_name, sinfo in structure.items():
                with st.expander(f"시트: {sheet_name}  ({len(sinfo['headers'])}개 헤더)", expanded=True):
                    col_sheet_type, _ = st.columns([2, 3])
                    with col_sheet_type:
                        sheet_type = st.selectbox(
                            "시트 유형",
                            options=["column_mapping", "overview", "sql_load", "sql_validation", "(제외)"],
                            key=f"tpl_stype_{sheet_name}",
                            help="컬럼 매핑 반복 행: column_mapping / 개요 단일값: overview / SQL블록: sql_load/sql_validation",
                        )

                    if sheet_type == "(제외)":
                        continue

                    col_field_map: dict[str, str] = {}
                    header_row = sinfo.get("header_row", 1)

                    if sheet_type in ("sql_load", "sql_validation"):
                        sql_row = st.number_input("SQL 기입 행", min_value=1, value=header_row + 1,
                                                  key=f"tpl_sqlrow_{sheet_name}")
                        sql_col = st.number_input("SQL 기입 열", min_value=1, value=1,
                                                  key=f"tpl_sqlcol_{sheet_name}")
                        sheet_configs_final.append({
                            "sheet_name": sheet_name,
                            "sheet_type": sheet_type,
                            "header_row": header_row,
                            "sql_cell":   {"row": int(sql_row), "col": int(sql_col)},
                            "col_field_map": {},
                        })
                        continue

                    # 헤더별 필드 선택
                    hdr_cols = st.columns(min(4, len(sinfo["headers"])))
                    for idx, hdr in enumerate(sinfo["headers"]):
                        col_idx = str(hdr["col"])
                        suggested = suggestions.get(sheet_name, {}).get(col_idx, "__ignore__")
                        with hdr_cols[idx % len(hdr_cols)]:
                            chosen = st.selectbox(
                                f'열{hdr["col"]}: "{hdr["cell_value"]}"',
                                options=field_options,
                                index=field_options.index(suggested) if suggested in field_options else 0,
                                format_func=lambda k: field_labels.get(k, k),
                                key=f"tpl_field_{sheet_name}_{col_idx}",
                            )
                        col_field_map[col_idx] = chosen

                    data_start = header_row + 1
                    if sheet_type == "column_mapping":
                        data_start = st.number_input(
                            "데이터 시작 행 (헤더 다음 행)",
                            min_value=1, value=header_row + 1,
                            key=f"tpl_dstart_{sheet_name}",
                        )

                    sheet_configs_final.append({
                        "sheet_name":    sheet_name,
                        "sheet_type":    sheet_type,
                        "header_row":    header_row,
                        "data_start_row": int(data_start),
                        "col_field_map": col_field_map,
                    })

            st.divider()
            col_pname, col_psave = st.columns([3, 1])
            with col_pname:
                new_profile_name = st.text_input("프로파일 이름", value="my_company_template",
                                                  key="tpl_new_name")
            with col_psave:
                st.markdown("<div style='padding-top:28px'></div>", unsafe_allow_html=True)
                if st.button("프로파일 저장", key="tpl_save_btn", type="primary"):
                    if not new_profile_name.strip():
                        st.error("프로파일 이름을 입력하세요.")
                    elif not sheet_configs_final:
                        st.error("저장할 시트 설정이 없습니다.")
                    else:
                        tpl_engine.save_profile(
                            new_profile_name.strip(),
                            sheet_configs_final,
                            st.session_state["tpl_bytes"],
                        )
                        st.success(f"프로파일 '{new_profile_name}' 저장 완료!")
                        st.session_state["tpl_structure"]  = None
                        st.session_state["tpl_bytes"]      = None
                        st.session_state["tpl_suggestions"] = None
                        st.rerun()

    st.stop()


# ═══════════════════════════════════════════════════════════
# DW 설계 페이지
# ═══════════════════════════════════════════════════════════
if page == "DW 설계":
    import pandas as pd

    st.markdown('<div class="step-row"><span class="step-num">1</span><span class="step-text">입력 소스</span></div>', unsafe_allow_html=True)
    st.caption("Swagger/OpenAPI 명세(JSON/YAML)가 가장 정확합니다. 없으면 자유 텍스트를 입력하세요.")

    input_tab_sw, input_tab_txt = st.tabs(["Swagger / OpenAPI", "자유 텍스트 (PDF 등)"])

    with input_tab_sw:
        swagger_file = st.file_uploader(
            "Swagger/OpenAPI 파일 (.json / .yaml / .yml)",
            type=["json", "yaml", "yml"],
            key="swagger_file",
        )
        if swagger_file:
            if st.button("Swagger 파싱", key="parse_swagger_btn", type="primary"):
                with st.spinner("Swagger 파싱 중..."):
                    try:
                        from aetl_designer import parse_swagger
                        parsed = parse_swagger(swagger_file.read())
                        st.session_state["designer_entities"] = parsed.get("entities", [])
                        st.session_state["designer_design"] = None
                        if parsed.get("error"):
                            st.error(f"파싱 오류: {parsed['error']}")
                        else:
                            st.success(f"엔티티 {len(parsed['entities'])}개 추출 완료")
                    except Exception as e:
                        st.error(f"파싱 오류: {e}")

    with input_tab_txt:
        # ── PDF 파일 업로드 (네이티브 분석) ──
        pdf_file = st.file_uploader(
            "PDF 파일 업로드 (테이블 정의서, ERD, 명세서 등)",
            type=["pdf"],
            key="designer_pdf_file",
        )
        if pdf_file:
            if st.button("PDF 전체 분석 (AI)", key="parse_pdf_native_btn", type="primary"):
                with st.spinner("AI가 PDF 문서를 분석하고 있습니다... (문서 크기에 따라 시간이 걸릴 수 있습니다)"):
                    try:
                        from aetl_designer import parse_pdf_document
                        pdf_bytes = pdf_file.read()
                        parsed = parse_pdf_document(pdf_bytes)
                        st.session_state["designer_entities"] = parsed.get("entities", [])
                        st.session_state["designer_design"] = None
                        if parsed.get("warning"):
                            st.warning(parsed["warning"])
                        if parsed.get("entities"):
                            st.success(f"엔티티 {len(parsed['entities'])}개 추출 완료")
                        elif parsed.get("error"):
                            st.error(parsed["error"])
                    except Exception as e:
                        st.error(f"PDF 분석 오류: {e}")

        st.divider()

        # ── 자유 텍스트 입력 ──
        text_input_designer = st.text_area(
            "테이블/엔티티 정의 텍스트 직접 입력",
            height=200,
            placeholder="예: 고객 테이블: 고객ID(PK), 고객명, 이메일, 가입일...",
            key="designer_text",
        )

        st.warning("⚠ AI 초안으로 파싱됩니다. 결과를 반드시 검토 및 수정하세요.")
        if st.button("AI 텍스트 파싱", key="parse_text_designer", type="secondary"):
            if text_input_designer.strip():
                with st.spinner("AI 파싱 중..."):
                    try:
                        from aetl_designer import parse_table_definition_text
                        parsed = parse_table_definition_text(text_input_designer)
                        st.session_state["designer_entities"] = parsed.get("entities", [])
                        st.session_state["designer_design"] = None
                        if parsed.get("warning"):
                            st.warning(parsed["warning"])
                        st.success(f"엔티티 {len(parsed.get('entities', []))}개 추출")
                    except Exception as e:
                        st.error(f"파싱 오류: {e}")
            else:
                st.info("텍스트를 입력하거나 위에서 PDF 파일을 업로드하세요.")

    if st.session_state["designer_entities"]:
        entities = st.session_state["designer_entities"]
        st.markdown('<div class="step-row"><span class="step-num">2</span><span class="step-text">추출된 엔티티 확인</span></div>', unsafe_allow_html=True)

        entity_summary = pd.DataFrame([{
            "엔티티명": e["name"],
            "필드 수": len(e.get("fields", [])),
            "필드 목록": ", ".join(f["name"] for f in e.get("fields", [])[:5])
                         + ("..." if len(e.get("fields", [])) > 5 else ""),
        } for e in entities])
        st.dataframe(entity_summary, width='stretch', hide_index=True)

        context_input = st.text_area(
            "추가 맥락 / 비즈니스 설명 (선택)",
            placeholder="예: 이커머스 플랫폼, 일별 판매 분석 목적...",
            height=80, key="designer_context",
        )

        st.markdown('<div class="step-row"><span class="step-num">3</span><span class="step-text">Star Schema 설계 (AI)</span></div>', unsafe_allow_html=True)
        st.caption("AI가 ODS / DW (Fact + Dim) / DM 3-Layer 스타 스키마를 설계합니다.")

        # 스키마 설계 가이드 로드 상태 표시
        try:
            from aetl_designer import _load_schema_guide
            _guide_text = _load_schema_guide()
            if _guide_text:
                st.caption("📖 Star Schema 설계 가이드 적용됨 — Kimball 방법론 기반")
            else:
                st.caption("⚠ documents/architecture/schema_doc.md 파일을 찾을 수 없습니다. 기본 설계 규칙으로 진행합니다.")
        except Exception:
            pass

        if st.button("AI 스키마 설계 실행", key="design_schema_btn", type="primary"):
            with st.spinner("AI 설계 중... (30초 이내 완료)"):
                try:
                    from aetl_designer import design_star_schema
                    design = design_star_schema(entities, context_input)
                    st.session_state["designer_design"] = design
                    total = sum(len(design.get(k, [])) for k in ["ods_tables", "fact_tables", "dim_tables", "dm_tables"])
                    st.success(f"설계 완료 — 총 {total}개 테이블")
                except Exception as e:
                    st.error(f"설계 오류: {e}")
                    st.code(traceback.format_exc())

    if st.session_state["designer_design"]:
        design = st.session_state["designer_design"]
        st.markdown('<div class="step-row"><span class="step-num">4</span><span class="step-text">설계 결과 확인</span></div>', unsafe_allow_html=True)
        st.caption("⚠ AI 초안입니다. 내용을 반드시 검토하세요.")

        def _render_tbl_list(tables):
            if not tables:
                st.info("설계된 테이블이 없습니다.")
                return
            for tbl in tables:
                with st.expander(f"{tbl.get('name','?')} — {tbl.get('comment','')}"):
                    cols_data = tbl.get("columns", [])
                    if cols_data:
                        col_df = pd.DataFrame([{
                            "컬럼명": c.get("name", ""), "타입": c.get("type", ""),
                            "PK": "✓" if c.get("pk") else "",
                            "Null허용": "✓" if c.get("nullable", True) else "",
                            "설명": c.get("desc", ""),
                        } for c in cols_data])
                        st.dataframe(col_df, width='stretch', hide_index=True,
                                     height=min(300, 36 * len(col_df) + 42))

        layer_tab1, layer_tab2, layer_tab3, layer_tab4 = st.tabs(["ODS", "DW (Fact/Dim)", "DM", "ERD 시각화"])
        with layer_tab1:
            _render_tbl_list(design.get("ods_tables", []))
        with layer_tab2:
            st.markdown("**Fact 테이블**")
            _render_tbl_list(design.get("fact_tables", []))
            st.markdown("**Dimension 테이블**")
            _render_tbl_list(design.get("dim_tables", []))
        with layer_tab3:
            _render_tbl_list(design.get("dm_tables", []))
        with layer_tab4:
            from erd_flow_component import erd_flow_map, build_erd_data, build_flow_data
            erd_sub1, erd_sub2 = st.tabs(["ERD 다이어그램", "레이어 흐름도"])
            with erd_sub1:
                erd_layer = st.selectbox(
                    "표시 레이어",
                    options=["all", "ods", "dw", "dm"],
                    format_func={"all": "전체", "ods": "ODS", "dw": "DW", "dm": "DM"}.get,
                    key="erd_layer_select",
                )
                try:
                    erd_nodes, erd_edges = build_erd_data(design, erd_layer)
                    erd_flow_map(
                        nodes=erd_nodes,
                        edges=erd_edges,
                        height=620,
                        direction="TB",
                        mode="erd",
                        key="erd_diagram_main",
                    )
                except Exception as e:
                    st.error(f"ERD 생성 오류: {e}")
            with erd_sub2:
                try:
                    flow_nodes, flow_edges = build_flow_data(design)
                    erd_flow_map(
                        nodes=flow_nodes,
                        edges=flow_edges,
                        height=520,
                        direction="LR",
                        mode="flow",
                        key="layer_flow_main",
                    )
                except Exception as e:
                    st.error(f"흐름도 생성 오류: {e}")

        st.markdown('<div class="step-row"><span class="step-num">5</span><span class="step-text">DDL 생성</span></div>', unsafe_allow_html=True)
        col_ddl_db, col_ddl_btn, _ = st.columns([2, 2, 4])
        with col_ddl_db:
            ddl_db_type = st.selectbox("DDL DB 종류", options=["oracle", "mariadb", "postgresql"],
                                        key="ddl_db_type_designer")
        with col_ddl_btn:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("DDL 전체 생성", key="gen_ddl_designer", type="primary"):
                with st.spinner("DDL 생성 중..."):
                    try:
                        from aetl_designer import design_to_ddl
                        st.session_state["designer_ddl"] = design_to_ddl(design, ddl_db_type)
                        st.success("DDL 생성 완료!")
                    except Exception as e:
                        st.error(f"DDL 생성 오류: {e}")

        if st.session_state["designer_ddl"]:
            st.code(st.session_state["designer_ddl"], language="sql")
            st.download_button(
                "DDL 다운로드 (.sql)",
                data=st.session_state["designer_ddl"],
                file_name="dw_schema_ddl.sql",
                mime="text/plain",
            )

    st.stop()


# (리니지 분석 페이지 제거됨 — ETL Lineage로 대체)

# ═══════════════════════════════════════════════════════════
# ETL Lineage 페이지
# ═══════════════════════════════════════════════════════════
if page == "ETL Lineage":
    from etl_flow_component import etl_flow_map, build_flow_data_from_mappings

    mappings = st.session_state.get("flow_map_mappings", [])

    # ── 컨트롤 바 ─────────────────────────────────────────
    ctrl_cols = st.columns([2, 2, 1, 1])
    with ctrl_cols[0]:
        direction = st.selectbox(
            "방향", options=["LR", "TB"],
            format_func=lambda x: "좌→우 (LR)" if x == "LR" else "위→아래 (TB)",
            key="flowmap_direction",
        )
    with ctrl_cols[1]:
        height = st.slider("높이 (px)", min_value=300, max_value=900, value=600, step=50,
                           key="flowmap_height")
    with ctrl_cols[2]:
        if st.button("전체 초기화", key="btn_flowmap_clear", type="secondary"):
            st.session_state["flow_map_mappings"] = []
            st.rerun()
    with ctrl_cols[3]:
        st.metric("등록 매핑", f"{len(mappings)}건")

    st.divider()

    # ── 빈 상태 안내 ──────────────────────────────────────
    if not mappings:
        st.info(
            "등록된 매핑이 없습니다.  \n"
            "**매핑 자동화** 페이지에서 **Flow Map 생성** 버튼을 클릭하면 자동으로 등록됩니다.",
            icon="ℹ️",
        )
    else:
        # ── 노드/엣지 빌드 ────────────────────────────────
        nodes, edges = build_flow_data_from_mappings(mappings)

        # ── React Flow 컴포넌트 렌더링 ────────────────────
        clicked = etl_flow_map(
            nodes=nodes,
            edges=edges,
            height=height,
            direction=direction,
            key="etl_flow_map_main",
        )

        # ── 클릭된 노드 정보 표시 ────────────────────────
        if clicked and clicked.get("clicked_node"):
            node_id = clicked["clicked_node"]
            node_info = next((n for n in nodes if n["id"] == node_id), None)
            if node_info:
                st.divider()
                st.markdown(
                    f'<div class="step-row"><span class="step-num">i</span>'
                    f'<span class="step-text">선택 노드: <code>{node_id}</code> '
                    f'({node_info.get("layer","").upper()} · {node_info.get("col_count",0)}열)</span></div>',
                    unsafe_allow_html=True,
                )
                if node_info.get("columns"):
                    import pandas as pd
                    df_cols = pd.DataFrame(node_info["columns"])
                    st.dataframe(df_cols, hide_index=True, use_container_width=True)

        # ── 등록 매핑 목록 ────────────────────────────────
        with st.expander(f"등록된 매핑 목록 ({len(mappings)}건)", expanded=False):
            import pandas as pd
            rows = []
            for m in mappings:
                src = m.get("source_meta", {})
                tgt = m.get("target_meta", {})
                rows.append({
                    "매핑 ID":   m.get("mapping_id", ""),
                    "소스 테이블": src.get("table_name", ""),
                    "타겟 테이블": tgt.get("table_name", ""),
                    "로드 유형":  m.get("load_type", "MERGE"),
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    st.stop()


# ═══════════════════════════════════════════════════════════
# Step 1 — Data Source
# ═══════════════════════════════════════════════════════════
st.markdown('<div class="step-row"><span class="step-num">1</span><span class="step-text">데이터 소스 설정</span></div>', unsafe_allow_html=True)

if mode == "파일 업로드":

    tab1, tab2, tab3 = st.tabs([
        "매핑정의서  (DM/DW/ODS 표준)",
        "테이블 정의서  (분리 파일)",
        "테이블 정의서  (통합 파일)",
    ])

    # ── Tab 1: 매핑정의서 ──
    with tab1:
        st.caption("매핑정의서 Excel 1개 업로드만으로 소스·타겟 메타데이터와 컬럼 매핑을 자동 추출합니다.")
        mapdef_file = st.file_uploader(
            "매핑정의서 파일 (.xlsx)",
            type=["xlsx", "xls"],
            key="mapdef_file",
        )
        if mapdef_file:
            from etl_metadata_parser import get_excel_sheets
            mapdef_sheets = get_excel_sheets(mapdef_file)
            mapdef_file.seek(0)

            col_a, col_b = st.columns([2, 3])
            with col_a:
                data_sheets = [s for s in mapdef_sheets if "sql" not in s.lower()]
                default_idx = mapdef_sheets.index(data_sheets[0]) if data_sheets else 0
                mapdef_sheet = st.selectbox("시트 선택", mapdef_sheets, index=default_idx, key="mapdef_sheet")
            with col_b:
                st.caption(f"시트 목록: {', '.join(mapdef_sheets)}")

            if st.button("파싱 실행", key="parse_mapdef", type="primary"):
                from etl_metadata_parser import parse_mapping_definition_excel
                try:
                    mapdef_file.seek(0)
                    result = parse_mapping_definition_excel(mapdef_file, sheet_name=mapdef_sheet)
                    st.session_state.source_meta = result["source_meta"]
                    st.session_state.target_meta = result["target_meta"]
                    st.session_state.mapping     = result["mapping"]
                    st.session_state.queries     = None
                    tgt = result["target_meta"]
                    src = result["source_meta"]
                    st.success(
                        f"파싱 완료  —  "
                        f"타겟: {tgt['table_name']} ({len(tgt['columns'])}컬럼)  /  "
                        f"소스: {src['table_name']} ({len(src['columns'])}컬럼)  /  "
                        f"매핑: {len(result['mapping'])}건"
                    )
                except Exception as e:
                    st.error(f"파싱 오류: {e}")
                    st.code(traceback.format_exc())

    # ── Tab 2: 분리 파일 ──
    with tab2:
        col_src, col_tgt = st.columns(2)
        with col_src:
            st.markdown("**소스 테이블 정의서**")
            src_file = st.file_uploader("소스 파일", type=["xlsx", "xls", "csv"], key="src_file")
            src_table_hint = st.text_input("테이블명 힌트", value="SOURCE_TABLE", key="src_hint")
            src_sheet = None
            if src_file and src_file.name.endswith((".xlsx", ".xls")):
                from etl_metadata_parser import get_excel_sheets
                sheets = get_excel_sheets(src_file); src_file.seek(0)
                if len(sheets) > 1:
                    src_sheet = st.selectbox("시트 선택 (소스)", sheets, key="src_sheet")

        with col_tgt:
            st.markdown("**타겟 테이블 정의서**")
            tgt_file = st.file_uploader("타겟 파일", type=["xlsx", "xls", "csv"], key="tgt_file")
            tgt_table_hint = st.text_input("테이블명 힌트", value="TARGET_TABLE", key="tgt_hint")
            tgt_sheet = None
            if tgt_file and tgt_file.name.endswith((".xlsx", ".xls")):
                from etl_metadata_parser import get_excel_sheets
                sheets = get_excel_sheets(tgt_file); tgt_file.seek(0)
                if len(sheets) > 1:
                    tgt_sheet = st.selectbox("시트 선택 (타겟)", sheets, key="tgt_sheet")

        if src_file and tgt_file:
            if st.button("파싱 실행", key="parse_sep", type="primary"):
                from etl_metadata_parser import parse_table_file
                try:
                    st.session_state.source_meta = parse_table_file(src_file, src_table_hint, src_sheet)
                    src_file.seek(0)
                    st.session_state.target_meta = parse_table_file(tgt_file, tgt_table_hint, tgt_sheet)
                    st.session_state.queries = None
                    st.success("파싱 완료")
                except Exception as e:
                    st.error(f"파싱 오류: {e}")
                    st.code(traceback.format_exc())

        st.divider()
        st.markdown("**컬럼 매핑 정의서** (선택)")
        mapping_file = st.file_uploader(
            "매핑 파일", type=["xlsx", "xls", "csv"], key="mapping_file",
            help="없으면 동일 컬럼명으로 가정합니다.",
        )
        if mapping_file:
            if st.button("매핑 파싱", key="parse_mapping_sep"):
                from etl_metadata_parser import parse_mapping_file
                try:
                    st.session_state.mapping = parse_mapping_file(mapping_file)
                    st.success(f"매핑 {len(st.session_state.mapping)}건 파싱 완료")
                    st.dataframe(st.session_state.mapping, width='stretch', hide_index=True)
                except Exception as e:
                    st.error(f"매핑 파싱 오류: {e}")

    # ── Tab 3: 통합 파일 ──
    with tab3:
        combo_file = st.file_uploader(
            "소스+타겟 통합 Excel 파일", type=["xlsx", "xls"], key="combo_file",
            help="시트명을 'source'/'target' 또는 '소스'/'타겟'으로 구분하세요.",
        )
        if combo_file:
            from etl_metadata_parser import get_excel_sheets
            sheets = get_excel_sheets(combo_file); combo_file.seek(0)
            st.caption(f"시트 목록: {', '.join(sheets)}")
            col_a, col_b = st.columns(2)
            with col_a:
                src_sheet_combo = st.selectbox("소스 시트", sheets, key="combo_src_sheet")
                src_hint_combo  = st.text_input("소스 테이블명", value="SOURCE_TABLE", key="combo_src_hint")
            with col_b:
                tgt_sheet_combo = st.selectbox("타겟 시트", sheets, index=min(1, len(sheets)-1), key="combo_tgt_sheet")
                tgt_hint_combo  = st.text_input("타겟 테이블명", value="TARGET_TABLE", key="combo_tgt_hint")

            if st.button("파싱 실행", key="parse_combo", type="primary"):
                from etl_metadata_parser import parse_table_file
                try:
                    st.session_state.source_meta = parse_table_file(combo_file, src_hint_combo, src_sheet_combo)
                    combo_file.seek(0)
                    st.session_state.target_meta = parse_table_file(combo_file, tgt_hint_combo, tgt_sheet_combo)
                    st.session_state.queries = None
                    st.success("파싱 완료")
                except Exception as e:
                    st.error(f"파싱 오류: {e}")

elif mode == "DB 직접 연결":

    _conn_cfg = st.session_state["db_conn_config"]
    _is_connected = _conn_cfg.get("connected", False)

    if not _is_connected:
        st.warning(
            "⚠️ DB가 연결되지 않았습니다. 사이드바의 **[Connect to DB]** 버튼으로 먼저 연결하세요.",
            icon=None,
        )

    # 연결 정보 fingerprint — 연결이 바뀌면 캐시 자동 갱신
    _conn_fp = f"{_conn_cfg.get('db_type')}:{_conn_cfg.get('host')}:{_conn_cfg.get('port')}:{_conn_cfg.get('database')}"

    @st.cache_data(ttl=300, show_spinner="DB 스키마 조회 중...")
    def load_db_schema(_fp: str):
        import importlib, os
        config_path = os.path.join(os.path.dirname(__file__), "db_config.json")
        if not os.path.exists(config_path):
            return None, "db_config.json 파일이 없습니다."
        try:
            db_schema_mod = importlib.import_module("db_schema")
            return db_schema_mod.get_schema(config_path), None
        except Exception as e:
            return None, str(e)

    if st.button("스키마 조회", type="primary", disabled=not _is_connected,
                 help="사이드바에서 연결한 DB의 테이블 목록을 불러옵니다."):
        schema_raw, err = load_db_schema(_conn_fp)
        if err:
            st.error(f"스키마 조회 실패: {err}")
        else:
            # get_schema() → {"tables": {...}, "joins": [...], "synonyms": {}, "_db_type": "..."}
            # "tables" 안에 실제 테이블 정보가 있음
            _tables_dict = schema_raw.get("tables", schema_raw) if isinstance(schema_raw, dict) else {}
            st.session_state["db_schema_raw"]   = schema_raw       # 전체 원본 보관
            st.session_state["db_schema"]        = _tables_dict     # 테이블 dict만 저장
            st.success(f"스키마 조회 완료: {len(_tables_dict)}개 테이블")

    if "db_schema" in st.session_state and st.session_state["db_schema"]:
        # schema = {table_name: {"columns": [...], "pk": [...], "fk": [...]}, ...}
        schema     = st.session_state["db_schema"]
        table_list = sorted(schema.keys())

        # ── 역할 기반 추천 라벨 생성 ──
        def _make_role_display_list(tables: list, role: str) -> tuple[list[str], dict[str, str]]:
            """역할 추천 라벨이 붙은 표시 리스트와 display→실제 테이블명 매핑 반환."""
            # 메타데이터 우선, 없으면 classify_table_role()로 즉시 분류
            role_map: dict[str, str] = {}
            try:
                from aetl_metadata_engine import get_tables_with_roles
                roles = get_tables_with_roles()
                if roles:
                    role_map = {r["table_name"]: r["effective_role"] for r in roles}
            except Exception:
                pass
            if not role_map:
                try:
                    from aetl_metadata_engine import classify_table_role
                    role_map = {t: classify_table_role(t) for t in tables}
                except Exception:
                    pass
            label = "source 추천" if role == "source" else "target 추천"
            recommended, others = [], []
            display_to_real: dict[str, str] = {}
            for t in tables:
                eff = role_map.get(t, "unknown")
                if eff == role:
                    display = f"{t}  ({label})"
                    recommended.append(display)
                else:
                    display = t
                    others.append(display)
                display_to_real[display] = t
            return recommended + others, display_to_real

        # ── 타겟 테이블 자동 필터링 (PK + 컬럼 유사도 기반) ──
        import re as _re

        _ALL_PREFIXES = _re.compile(
            r"^(ods_|stg_|dw_|raw_|src_|ext_|load_|dm_|fact_|dim_|f_|d_|rpt_|agg_|mart_)",
            _re.IGNORECASE,
        )

        def _extract_entity(table_name: str) -> str:
            """schema.table에서 프리픽스를 제거한 엔티티명 추출."""
            tbl = table_name.rsplit(".", 1)[-1]          # schema 부분 제거
            return _ALL_PREFIXES.sub("", tbl).lower()

        def _get_col_names(tbl: str) -> set[str]:
            """테이블의 컬럼명 집합 반환 (소문자)."""
            cols = schema.get(tbl, {}).get("columns", [])
            return {c["name"].lower() for c in cols if isinstance(c, dict)}

        def _get_pk_names(tbl: str) -> set[str]:
            """테이블의 PK 컬럼명 집합 반환 (소문자)."""
            pk = schema.get(tbl, {}).get("pk", [])
            return {p.lower() for p in pk}

        def _score_target(src: str, tgt: str) -> float:
            """소스-타겟 유사도 점수 (0.0 ~ 1.0)."""
            if src == tgt:
                return 0.0  # 자기 자신은 타겟이 될 수 없음

            src_cols = _get_col_names(src)
            tgt_cols = _get_col_names(tgt)
            src_pk   = _get_pk_names(src)

            if not tgt_cols:
                return 0.0

            # 1) PK 포함 점수 (0.4): 소스 PK가 타겟 컬럼에 존재하는 비율
            pk_score = 0.0
            if src_pk:
                pk_overlap = src_pk & tgt_cols
                pk_score = len(pk_overlap) / len(src_pk)

            # 2) 컬럼 겹침 점수 (0.4): 소스 컬럼 중 타겟에도 있는 비율
            col_score = 0.0
            if src_cols:
                col_overlap = src_cols & tgt_cols
                col_score = len(col_overlap) / len(src_cols)

            # 3) 엔티티명 유사도 보너스 (0.2): 프리픽스 제거 후 이름 일치
            src_entity = _extract_entity(src)
            tgt_entity = _extract_entity(tgt)
            name_score = 0.0
            if src_entity and tgt_entity:
                if src_entity == tgt_entity:
                    name_score = 1.0
                elif src_entity in tgt_entity or tgt_entity in src_entity:
                    name_score = 0.5

            return pk_score * 0.4 + col_score * 0.4 + name_score * 0.2

        def _filter_targets(src: str, tables: list[str], threshold: float = 0.3) -> list[str]:
            """소스 테이블과 유사도가 threshold 이상인 타겟 테이블만 반환 (점수 내림차순)."""
            scored = []
            for t in tables:
                s = _score_target(src, t)
                if s >= threshold:
                    scored.append((t, s))
            scored.sort(key=lambda x: x[1], reverse=True)
            return [t for t, _ in scored]

        src_display, src_map = _make_role_display_list(table_list, "source")

        col_src, col_tgt = st.columns(2)
        with col_src:
            src_sel = st.selectbox("소스 테이블", src_display, key="db_src_tbl")
            src_tbl = src_map.get(src_sel, src_sel) if src_sel else None

        with col_tgt:
            # 타겟 필터링 토글
            _filter_on = st.checkbox(
                "타겟 자동 필터링",
                value=False,
                key="tgt_filter_enabled",
                help="소스 테이블의 PK·컬럼 유사도를 기반으로 관련 타겟 테이블만 표시합니다.",
            )

            if _filter_on and src_tbl:
                _filtered = _filter_targets(src_tbl, table_list)
                if _filtered:
                    tgt_base = _filtered
                else:
                    st.caption("⚠ 유사 타겟 없음 — 전체 목록 표시")
                    tgt_base = table_list
            else:
                tgt_base = table_list

            tgt_display, tgt_map = _make_role_display_list(tgt_base, "target")
            tgt_sel = st.selectbox("타겟 테이블", tgt_display, key="db_tgt_tbl")
            tgt_tbl = tgt_map.get(tgt_sel, tgt_sel) if tgt_sel else None

        if st.button("메타데이터 로드", key="load_db_meta", type="primary"):
            from etl_metadata_parser import schema_to_metadata
            st.session_state.source_meta = schema_to_metadata(schema, src_tbl)
            st.session_state.target_meta = schema_to_metadata(schema, tgt_tbl)
            st.session_state.mapping     = None
            st.session_state.queries     = None
            st.success("메타데이터 로드 완료")
    elif _is_connected:
        st.info("ℹ️ **스키마 조회** 버튼을 눌러 DB 테이블 목록을 불러오세요.")


# ═══════════════════════════════════════════════════════════
# Step 2 — Metadata Preview
# ═══════════════════════════════════════════════════════════
if st.session_state.source_meta or st.session_state.target_meta:
    st.markdown('<div class="step-row"><span class="step-num">2</span><span class="step-text">테이블 메타데이터 확인</span></div>', unsafe_allow_html=True)

    col_src, col_arr, col_tgt = st.columns([10, 1, 10])
    with col_src:
        if st.session_state.source_meta:
            render_metadata_table(st.session_state.source_meta, "source")
        else:
            st.warning("소스 테이블 정보 없음")
    with col_arr:
        st.markdown(
            "<div style='text-align:center;padding-top:56px;font-size:20px;color:#A8BCCC;'>→</div>",
            unsafe_allow_html=True,
        )
    with col_tgt:
        if st.session_state.target_meta:
            render_metadata_table(st.session_state.target_meta, "target")
        else:
            st.warning("타겟 테이블 정보 없음")

    # 테이블명 수정
    with st.expander("테이블명 수정"):
        col_n1, col_n2 = st.columns(2)
        with col_n1:
            if st.session_state.source_meta:
                new_src = st.text_input("소스 테이블명", value=st.session_state.source_meta["table_name"], key="edit_src")
                if st.button("적용", key="apply_src"):
                    st.session_state.source_meta["table_name"] = new_src.upper()
                    st.rerun()
        with col_n2:
            if st.session_state.target_meta:
                new_tgt = st.text_input("타겟 테이블명", value=st.session_state.target_meta["table_name"], key="edit_tgt")
                if st.button("적용", key="apply_tgt"):
                    st.session_state.target_meta["table_name"] = new_tgt.upper()
                    st.rerun()

    if st.session_state.mapping:
        with st.expander(f"컬럼 매핑 확인  ({len(st.session_state.mapping)}건)"):
            import pandas as pd
            st.dataframe(pd.DataFrame(st.session_state.mapping), width='stretch', hide_index=True)

    # ── 생성 버튼 ──
    st.divider()
    col_gen, col_reset = st.columns([5, 1])
    with col_gen:
        generate_btn = st.button(
            "검증 쿼리 생성",
            type="primary",
            width='stretch',
            disabled=(st.session_state.source_meta is None or st.session_state.target_meta is None),
        )
    with col_reset:
        if st.button("초기화", width='stretch'):
            for k in ("source_meta", "target_meta", "mapping", "queries"):
                st.session_state[k] = None
            st.rerun()

    if generate_btn:
        with st.spinner("쿼리 생성 중..."):
            try:
                from etl_sql_generator import generate_validation_queries, generate_validation_queries_no_llm
                if use_llm:
                    queries = generate_validation_queries(
                        source_meta=st.session_state.source_meta,
                        target_meta=st.session_state.target_meta,
                        column_mapping=st.session_state.mapping,
                        db_type=db_type,
                        llm=get_llm(),
                    )
                    st.success("AI 기반 검증 쿼리 생성 완료")
                else:
                    queries = generate_validation_queries_no_llm(
                        source_meta=st.session_state.source_meta,
                        target_meta=st.session_state.target_meta,
                        column_mapping=st.session_state.mapping,
                        db_type=db_type,
                    )
                    st.success("템플릿 기반 검증 쿼리 생성 완료")
                st.session_state.queries = queries
            except Exception as e:
                st.error(f"쿼리 생성 오류: {e}")
                st.code(traceback.format_exc())


# ═══════════════════════════════════════════════════════════
# Step 3 — Query Results
# ═══════════════════════════════════════════════════════════
if st.session_state.queries:
    st.divider()
    render_query_results(st.session_state.queries)


# ═══════════════════════════════════════════════════════════
# Welcome (초기 화면)
# ═══════════════════════════════════════════════════════════
if st.session_state.source_meta is None and st.session_state.target_meta is None:
    st.divider()

    col1, col2, col3 = st.columns(3)
    cards = [
        ("01", "데이터 소스 선택",
         "왼쪽 사이드바에서 파일 업로드 또는 DB 직접 연결을 선택합니다."),
        ("02", "소스 / 타겟 설정",
         "매핑정의서 또는 테이블 정의서를 업로드하여 메타데이터를 추출합니다."),
        ("03", "검증 쿼리 생성",
         "건수 비교, PK 누락, NULL 체크, 중복 검증, 체크섬, 전체 비교 쿼리가 자동 생성됩니다."),
    ]
    for col, (num, title, desc) in zip([col1, col2, col3], cards):
        with col:
            st.markdown(f"""
<div class="welcome-card">
  <div class="welcome-num">{num}</div>
  <div class="welcome-title">{title}</div>
  <div class="welcome-desc">{desc}</div>
</div>
""", unsafe_allow_html=True)

    st.divider()
    st.markdown("**샘플 데이터로 빠르게 테스트**")
    col_btn, _ = st.columns([3, 5])
    with col_btn:
        if st.button("샘플 로드  (ODS_CUSTOMER → DW_CUSTOMER)", width='stretch'):
            st.session_state.source_meta = {
                "table_name": "ODS_CUSTOMER",
                "columns": [
                    {"name": "CUSTOMER_ID",   "type": "VARCHAR2(20)",  "pk": True,  "nullable": False, "description": "고객ID"},
                    {"name": "CUSTOMER_NAME", "type": "VARCHAR2(100)", "pk": False, "nullable": False, "description": "고객명"},
                    {"name": "PHONE",         "type": "VARCHAR2(20)",  "pk": False, "nullable": True,  "description": "연락처"},
                    {"name": "EMAIL",         "type": "VARCHAR2(200)", "pk": False, "nullable": True,  "description": "이메일"},
                    {"name": "REG_DATE",      "type": "DATE",          "pk": False, "nullable": False, "description": "등록일"},
                    {"name": "STATUS_CD",     "type": "CHAR(1)",       "pk": False, "nullable": False, "description": "상태코드"},
                ],
                "pk_columns": ["CUSTOMER_ID"],
            }
            st.session_state.target_meta = {
                "table_name": "DW_CUSTOMER",
                "columns": [
                    {"name": "CUSTOMER_ID",   "type": "VARCHAR2(20)",  "pk": True,  "nullable": False, "description": "고객ID"},
                    {"name": "CUSTOMER_NAME", "type": "VARCHAR2(100)", "pk": False, "nullable": False, "description": "고객명"},
                    {"name": "PHONE",         "type": "VARCHAR2(20)",  "pk": False, "nullable": True,  "description": "연락처"},
                    {"name": "EMAIL",         "type": "VARCHAR2(200)", "pk": False, "nullable": True,  "description": "이메일"},
                    {"name": "REG_DATE",      "type": "DATE",          "pk": False, "nullable": False, "description": "등록일"},
                    {"name": "STATUS_CD",     "type": "CHAR(1)",       "pk": False, "nullable": False, "description": "상태코드"},
                    {"name": "ETL_LOAD_DT",   "type": "TIMESTAMP",     "pk": False, "nullable": True,  "description": "ETL 적재일시"},
                ],
                "pk_columns": ["CUSTOMER_ID"],
            }
            st.session_state.mapping = None
            st.rerun()
