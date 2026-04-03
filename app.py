import os
import re
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

# =========================
# Page config
# =========================
st.set_page_config(
    page_title="DSP Invoice Upload",
    page_icon="📄",
    layout="wide"
)

# =========================
# Custom CSS
# =========================
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%);
    }

    .hero-wrap {
        display: flex;
        align-items: center;
        gap: 18px;
        margin-bottom: 22px;
    }

    .hero-brand {
        font-size: 2.4rem;
        font-weight: 800;
        color: #111827;
        line-height: 1;
    }

    .hero-badge {
        font-size: 1rem;
        font-weight: 700;
        color: #2563eb;
        background: #dbeafe;
        padding: 8px 14px;
        border-radius: 999px;
        display: inline-block;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: #475569;
        margin-top: 0.35rem;
        margin-bottom: 1.2rem;
    }

    .main-card {
        background: white;
        padding: 28px 28px 24px 28px;
        border-radius: 22px;
        box-shadow: 0 10px 30px rgba(30, 41, 59, 0.08);
        border: 1px solid #e2e8f0;
        margin-bottom: 24px;
    }

    .section-title {
        font-size: 1.2rem;
        font-weight: 700;
        color: #0f172a;
        margin-bottom: 0.9rem;
    }

    .status-good {
        background: #ecfdf5;
        border: 1px solid #a7f3d0;
        color: #065f46;
        padding: 14px 16px;
        border-radius: 14px;
        font-weight: 700;
        margin-top: 12px;
        margin-bottom: 10px;
    }

    .status-bad {
        background: #fef2f2;
        border: 1px solid #fecaca;
        color: #991b1b;
        padding: 14px 16px;
        border-radius: 14px;
        font-weight: 700;
        margin-top: 12px;
        margin-bottom: 10px;
    }

    .status-warn {
        background: #fff7ed;
        border: 1px solid #fdba74;
        color: #9a3412;
        padding: 14px 16px;
        border-radius: 14px;
        font-weight: 700;
        margin-top: 12px;
        margin-bottom: 10px;
    }

    .metric-card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
    }

    .metric-title {
        color: #64748b;
        font-size: 0.92rem;
        margin-bottom: 6px;
    }

    .metric-value {
        color: #0f172a;
        font-size: 1.3rem;
        font-weight: 800;
    }

    .stButton > button {
        width: 100%;
        background: linear-gradient(90deg, #2563eb 0%, #1d4ed8 100%);
        color: white;
        border: none;
        border-radius: 14px;
        padding: 0.82rem 1rem;
        font-weight: 700;
        font-size: 1rem;
        box-shadow: 0 8px 22px rgba(37, 99, 235, 0.22);
    }

    .stButton > button:hover {
        background: linear-gradient(90deg, #1d4ed8 0%, #1e40af 100%);
        color: white;
    }

    div[data-testid="stFileUploader"] {
        background: #f8fafc;
        border: 1.5px dashed #93c5fd;
        border-radius: 18px;
        padding: 8px 10px 2px 10px;
    }

    .footer-note {
        color: #64748b;
        font-size: 0.92rem;
        margin-top: 8px;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# Config
# =========================
BASE_PATH = "/Users/chenyi/Desktop/财务工作/派送"

REGIONS = [
    "ORD", "IND", "CVG", "CMH", "MSP", "SDF",
    "LEX", "DTW", "CLE", "TOL", "STL", "OMA", "FWA"
]

# 按你的 Excel 实际列名改这里
COLUMN_MAP = {
    "teamid": "team_id",
    "salary": "salary",
    "region": "warehouse",
    "dsp_name": "dsp_name",  # 可选，没有也没关系
}

AMOUNT_TOLERANCE = 0.01

# =========================
# Helpers
# =========================
def get_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())

def monday_str(d: date) -> str:
    return get_monday(d).strftime("%Y%m%d")

def parse_yyyymmdd(s: str):
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except Exception:
        return None

def is_monday_string(s: str) -> bool:
    d = parse_yyyymmdd(s)
    return bool(d and d.weekday() == 0)

def clean_teamid(value) -> str:
    s = str(value).strip()
    s = re.sub(r"\.0$", "", s)
    return s

def normalize_money(v) -> float:
    if pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    s = s.replace(",", "").replace("$", "")
    s = s.replace("(", "-").replace(")", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    return float(s) if s not in ["", "-", "."] else 0.0

def get_extension(filename: str) -> str:
    _, ext = os.path.splitext(filename)
    return ext.lower()

def ensure_folder(path: str):
    os.makedirs(path, exist_ok=True)

def get_week_folder(week_monday: str) -> str:
    return os.path.join(BASE_PATH, week_monday)

def get_invoice_folder(week_monday: str) -> str:
    return os.path.join(BASE_PATH, week_monday, "invoice")

def get_excel_path(week_monday: str) -> str:
    return os.path.join(BASE_PATH, week_monday, "invoice", "Teams_merged.xlsx")

def load_weekly_teams(week_monday: str) -> pd.DataFrame:
    excel_path = get_excel_path(week_monday)

    if not os.path.exists(excel_path):
        raise FileNotFoundError(
            f"Teams_merged.xlsx not found: {excel_path}"
        )

    df = pd.read_excel(excel_path)
    df.columns = [str(c).strip() for c in df.columns]

    required = [COLUMN_MAP["teamid"], COLUMN_MAP["salary"], COLUMN_MAP["region"]]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Teams_merged.xlsx missing required columns: {missing}")

    df[COLUMN_MAP["teamid"]] = df[COLUMN_MAP["teamid"]].astype(str).map(clean_teamid)
    df[COLUMN_MAP["region"]] = df[COLUMN_MAP["region"]].astype(str).str.strip().str.upper()
    df[COLUMN_MAP["salary"]] = df[COLUMN_MAP["salary"]].map(normalize_money)

    return df

def get_expected_salary(df: pd.DataFrame, teamid: str, region: str):
    team_col = COLUMN_MAP["teamid"]
    salary_col = COLUMN_MAP["salary"]
    region_col = COLUMN_MAP["region"]

    subset = df[
        (df[team_col] == clean_teamid(teamid)) &
        (df[region_col] == str(region).strip().upper())
    ]

    if subset.empty:
        return None, None

    row = subset.iloc[0]
    return float(row[salary_col]), row

def save_uploaded_file(uploaded_file, save_path: str):
    with open(save_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

# =========================
# UI
# =========================
default_week = monday_str(date.today())

top1, top2 = st.columns([1, 6])
with top1:
    try:
        st.image("logo.png", width=110)
    except Exception:
        pass

with top2:
    st.markdown('<div class="hero-brand">UniUni</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="hero-subtitle">Upload invoice, validate against the weekly Teams_merged file, and save locally. / 上传发票，校验每周 Teams_merged，并保存到本地文件夹。</div>',
        unsafe_allow_html=True
    )

st.markdown('<div class="main-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Upload Invoice / 上传发票</div>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    input_teamid = st.text_input("Team ID / 团队编号", placeholder="例如 Example: 1206")

with col2:
    input_region = st.selectbox("Warehouse / 仓库", REGIONS)

with col3:
    input_week = st.text_input("Week Monday / 周一日期 (YYYYMMDD)", value=default_week)

st.markdown(
    f'<div class="hero-badge" style="margin-bottom:16px;">Region / 区域：{input_region}</div>',
    unsafe_allow_html=True
)

uploaded_file = st.file_uploader(
    "Upload invoice file / 上传发票",
    type=["pdf", "xlsx", "xls", "csv", "png", "jpg", "jpeg"],
    help="支持拖拽上传 / Drag & drop supported",
)

amount = st.number_input(
    "Invoice amount / 发票金额",
    min_value=0.0,
    step=0.01,
    value=0.0,
)

submit = st.button("Submit Invoice / 提交发票")

if submit:
    if not input_teamid.strip():
        st.error("Please enter Team ID. / 请输入 Team ID。")
        st.stop()

    if not is_monday_string(input_week):
        st.error("Week Monday must be a Monday in YYYYMMDD format. / 日期必须是周一，格式为 YYYYMMDD。")
        st.stop()

    if uploaded_file is None:
        st.error("Please upload an invoice file. / 请上传发票文件。")
        st.stop()

    if amount <= 0:
        st.error("Please input the invoice amount. / 请输入发票金额。")
        st.stop()

    invoice_folder = get_invoice_folder(input_week)
    ensure_folder(invoice_folder)

    ext = get_extension(uploaded_file.name)
    if not ext:
        ext = ".pdf"

    new_filename = f"{clean_teamid(input_teamid)}{input_region}{input_week}{ext}"
    save_path = os.path.join(invoice_folder, new_filename)

    # 先保存文件
    try:
        save_uploaded_file(uploaded_file, save_path)
    except Exception as e:
        st.exception(e)
        st.stop()

    # 再读取 Excel 校验
    try:
        teams_df = load_weekly_teams(input_week)
    except FileNotFoundError:
        st.markdown(
            '<div class="status-warn">⚠️ Teams_merged.xlsx not found in this week’s invoice folder / 本周 invoice 文件夹中没有 Teams_merged.xlsx</div>',
            unsafe_allow_html=True
        )
        st.success(f"Saved locally: {save_path}")
        st.stop()
    except Exception as e:
        st.exception(e)
        st.stop()

    expected_salary, matched_row = get_expected_salary(teams_df, input_teamid, input_region)

    if expected_salary is None:
        st.markdown(
            '<div class="status-warn">⚠️ Team ID + Warehouse not found in Teams_merged.xlsx / 未在 Teams_merged.xlsx 中找到该 Team ID + 仓库组合</div>',
            unsafe_allow_html=True
        )
        st.success(f"Saved locally: {save_path}")
        st.stop()

    diff = abs(amount - expected_salary)

    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(
            f'<div class="metric-card"><div class="metric-title">Expected Salary / 应付金额</div><div class="metric-value">${expected_salary:,.2f}</div></div>',
            unsafe_allow_html=True
        )
    with m2:
        st.markdown(
            f'<div class="metric-card"><div class="metric-title">Invoice Amount / 发票金额</div><div class="metric-value">${amount:,.2f}</div></div>',
            unsafe_allow_html=True
        )
    with m3:
        st.markdown(
            f'<div class="metric-card"><div class="metric-title">Difference / 差额</div><div class="metric-value">${diff:,.2f}</div></div>',
            unsafe_allow_html=True
        )

    if diff <= AMOUNT_TOLERANCE:
        st.markdown(
            '<div class="status-good">✅ Matched / 金额匹配正确</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="status-bad">❌ Mismatch / 金额不匹配</div>',
            unsafe_allow_html=True
        )

    st.success(f"Saved locally: {save_path}")

st.markdown(
    f'<div class="footer-note">Local save path / 本地保存路径：{BASE_PATH}</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="footer-note">Weekly structure / 每周结构：派送 / 周一日期 / invoice / Teams_merged.xlsx + invoices</div>',
    unsafe_allow_html=True
)
st.markdown('</div>', unsafe_allow_html=True)
