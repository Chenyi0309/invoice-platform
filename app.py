import io
import os
import re
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import yagmail
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

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

    .hero-logo {
        height: 72px;
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
REGIONS = ["ORD", "IND", "CVG", "CMH", "MSP", "SDF", "LEX", "DTW", "CLE", "TOL", "STL", "OMA", "FWA"]

# 按你的 Teams_merged 实际列名修改
COLUMN_MAP = {
    "teamid": "team_id",
    "salary": "salary",
    "dsp_name": "dsp_name",
    "region": "warehouse",
}

ROOT_FOLDER_NAME = "DSP_Invoices"
AMOUNT_TOLERANCE = 0.01

# =========================
# Secrets
# =========================
GDRIVE_PROJECT_ID = st.secrets["gdrive"]["project_id"]
GDRIVE_PRIVATE_KEY_ID = st.secrets["gdrive"]["private_key_id"]
GDRIVE_PRIVATE_KEY = st.secrets["gdrive"]["private_key"].replace("\\n", "\n")
GDRIVE_CLIENT_EMAIL = st.secrets["gdrive"]["client_email"]
GDRIVE_CLIENT_ID = st.secrets["gdrive"]["client_id"]
GDRIVE_ROOT_FOLDER_ID = st.secrets["gdrive"]["root_folder_id"]

EMAIL_USER = st.secrets["gmail"]["user"]
EMAIL_PASSWORD = st.secrets["gmail"]["app_password"]
ALERT_TO_EMAIL = st.secrets["gmail"]["alert_to"]

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

def send_email(subject: str, body: str):
    yag = yagmail.SMTP(user=EMAIL_USER, password=EMAIL_PASSWORD)
    yag.send(to=ALERT_TO_EMAIL, subject=subject, contents=body)

# =========================
# Google Drive Auth
# =========================
@st.cache_resource
def init_drive_service():
    service_account_info = {
        "type": "service_account",
        "project_id": GDRIVE_PROJECT_ID,
        "private_key_id": GDRIVE_PRIVATE_KEY_ID,
        "private_key": GDRIVE_PRIVATE_KEY,
        "client_email": GDRIVE_CLIENT_EMAIL,
        "client_id": GDRIVE_CLIENT_ID,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GDRIVE_CLIENT_EMAIL.replace('@', '%40')}",
    }

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )

    return build("drive", "v3", credentials=credentials)

drive_service = init_drive_service()

# =========================
# Google Drive functions
# =========================
def get_root_folder():
    return {"id": GDRIVE_ROOT_FOLDER_ID, "name": ROOT_FOLDER_NAME}

def find_folder_by_name(name: str, parent_id: str = None):
    safe_name = name.replace("'", "\\'")
    if parent_id:
        query = (
            f"name = '{safe_name}' and mimeType = 'application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents and trashed = false"
        )
    else:
        query = (
            f"name = '{safe_name}' and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        )

    results = drive_service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = results.get("files", [])
    return files[0] if files else None

def create_folder(name: str, parent_id: str = None):
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    return drive_service.files().create(
        body=metadata,
        fields="id, name",
        supportsAllDrives=True,
    ).execute()

def get_or_create_week_folder(week_monday: str):
    root = get_root_folder()
    folder = find_folder_by_name(week_monday, parent_id=root["id"])
    if folder:
        return folder
    return create_folder(week_monday, parent_id=root["id"])

def find_file_in_folder(filename: str, folder_id: str):
    safe_filename = filename.replace("'", "\\'")
    query = f"name = '{safe_filename}' and '{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = results.get("files", [])
    return files[0] if files else None

def download_excel_from_drive(filename: str, folder_id: str) -> pd.DataFrame:
    file = find_file_in_folder(filename, folder_id)
    if not file:
        raise FileNotFoundError(f"{filename} not found in this week folder.")

    request = drive_service.files().get_media(
        fileId=file["id"],
        supportsAllDrives=True,
    )
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return pd.read_excel(buffer)

def upload_file_to_drive(file_bytes: bytes, filename: str, folder_id: str):
    existing = find_file_in_folder(filename, folder_id)
    if existing:
        return "duplicate"

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }

    ext = os.path.splitext(filename)[1].lower()
    mime_map = {
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".csv": "text/csv",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }
    mimetype = mime_map.get(ext, "application/octet-stream")

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=mimetype,
        resumable=False,
    )

    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name",
        supportsAllDrives=True,
    ).execute()

    return "uploaded"

# =========================
# Business logic
# =========================
def load_weekly_teams(week_monday: str) -> pd.DataFrame:
    week_folder = get_or_create_week_folder(week_monday)
    df = download_excel_from_drive("Teams_merged.xlsx", week_folder["id"])
    df.columns = [str(c).strip() for c in df.columns]

    required = [COLUMN_MAP["teamid"], COLUMN_MAP["salary"], COLUMN_MAP["region"]]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Teams_merged.xlsx missing required columns: {missing}")

    df[COLUMN_MAP["teamid"]] = df[COLUMN_MAP["teamid"]].astype(str).map(clean_teamid)
    df[COLUMN_MAP["salary"]] = df[COLUMN_MAP["salary"]].map(normalize_money)
    df[COLUMN_MAP["region"]] = df[COLUMN_MAP["region"]].astype(str).str.strip().str.upper()

    return df

def get_expected_salary(df: pd.DataFrame, teamid: str, region: str):
    team_col = COLUMN_MAP["teamid"]
    salary_col = COLUMN_MAP["salary"]
    region_col = COLUMN_MAP["region"]

    subset = df[(df[team_col] == teamid) & (df[region_col] == region)]
    if subset.empty:
        return None, None

    row = subset.iloc[0]
    return float(row[salary_col]), row

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
    st.markdown(
        f"""
        <div class="hero-wrap">
            <div class="hero-brand">UniUni</div>
            <div class="hero-badge">Region / 区域：{REGIONS[0] if False else "Region"}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown('<div class="hero-subtitle">Upload invoice, validate against the weekly Teams_merged file, and save to the correct Google Drive folder. / 上传发票，校验每周 Teams_merged，并保存到对应 Google Drive 文件夹。</div>', unsafe_allow_html=True)

st.markdown('<div class="main-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Upload Invoice / 上传发票</div>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    input_teamid = st.text_input("Team ID / 团队编号", placeholder="例如 Example: 1206")

with col2:
    input_region = st.selectbox("Warehouse / 仓库", REGIONS)

with col3:
    input_week = st.text_input("Week Monday / 周一日期 (YYYYMMDD)", value=default_week)

# 动态显示 region
st.markdown(
    f"""
    <script>
    const badges = window.parent.document.querySelectorAll('.hero-badge');
    if (badges.length > 0) {{
        badges[0].innerText = 'Region / 区域：{input_region}';
    }}
    </script>
    """,
    unsafe_allow_html=True,
)

uploaded_file = st.file_uploader(
    "Upload invoice file / 上传发票",
    type=["pdf", "xlsx", "xls", "csv", "png", "jpg", "jpeg"],
    help="支持拖拽上传 / Drag & drop supported",
)

manual_amount = st.number_input(
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

    if manual_amount <= 0:
        st.error("Please input the invoice amount. / 请输入发票金额。")
        st.stop()

    with st.spinner("Checking weekly Teams_merged and validating invoice... / 正在校验本周 Teams_merged 与发票金额..."):
        try:
            teams_df = load_weekly_teams(input_week)
        except Exception as e:
            st.error(f"Could not load weekly Teams_merged.xlsx: {e}")
            st.stop()

        teamid = clean_teamid(input_teamid)
        expected_salary, _ = get_expected_salary(teams_df, teamid, input_region)

        if expected_salary is None:
            st.error("This Team ID + Warehouse was not found in this week's Teams_merged.xlsx. / 本周 Teams_merged 中未找到该 Team ID + 仓库组合。")
            st.stop()

        diff = abs(manual_amount - expected_salary)
        ext = get_extension(uploaded_file.name)
        new_filename = f"{teamid}{input_region}{input_week}{ext}"
        week_folder = get_or_create_week_folder(input_week)

        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(
                f'<div class="metric-card"><div class="metric-title">Expected Salary / 应付金额</div><div class="metric-value">${expected_salary:,.2f}</div></div>',
                unsafe_allow_html=True
            )
        with m2:
            st.markdown(
                f'<div class="metric-card"><div class="metric-title">Invoice Amount / 发票金额</div><div class="metric-value">${manual_amount:,.2f}</div></div>',
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

            file_bytes = uploaded_file.read()
            try:
                result = upload_file_to_drive(file_bytes, new_filename, week_folder["id"])
            except Exception as e:
                st.exception(e)
                st.stop()

            if result == "duplicate":
                st.warning(f"File already exists: {new_filename} / 文件已存在。")
            else:
                st.balloons()
                st.success(f"Uploaded successfully: {new_filename} / 上传成功。")
        else:
            st.markdown(
                '<div class="status-bad">❌ Mismatch / 金额不匹配</div>',
                unsafe_allow_html=True
            )

            subject = f"[Invoice Mismatch] {teamid} | {input_region} | {input_week}"
            body = (
                f"Invoice mismatch detected.\n\n"
                f"Team ID: {teamid}\n"
                f"Warehouse: {input_region}\n"
                f"Week Monday: {input_week}\n"
                f"Expected salary: {expected_salary:,.2f}\n"
                f"Invoice amount: {manual_amount:,.2f}\n"
                f"Difference: {diff:,.2f}\n"
                f"Original file: {uploaded_file.name}\n"
                f"Renamed file: {new_filename}\n"
            )
            send_email(subject, body)
            st.error("Mismatch email sent. / 不匹配提醒邮件已发送。")

st.markdown(
    '<div class="footer-note">Current version uses manual amount input. OCR can be added later for automatic invoice total extraction. / 当前版本需要手动输入金额，后续可增加 OCR 自动识别发票金额。</div>',
    unsafe_allow_html=True
)
st.markdown('</div>', unsafe_allow_html=True)
