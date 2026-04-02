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
st.set_page_config(page_title="DSP Invoice Upload Platform", layout="wide")
st.title("DSP Invoice Upload Platform")
st.caption(
    "Upload invoice → validate with weekly Teams_merged → rename → save to Google Drive → monitor missing submissions"
)

# =========================
# Config
# =========================
REGIONS = ["ORD", "IND", "CVG", "CMH", "MSP", "SDF", "LEX", "DTW", "CLE", "TOL", "STL", "OMA", "FWA"]

# 按你的 Teams_merged 实际列名改这里
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
GDRIVE_PRIVATE_KEY = st.secrets["gdrive"]["private_key"]
GDRIVE_CLIENT_EMAIL = st.secrets["gdrive"]["client_email"]
GDRIVE_CLIENT_ID = st.secrets["gdrive"]["client_id"]

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

    service = build("drive", "v3", credentials=credentials)
    return service


drive_service = init_drive_service()

# =========================
# Google Drive functions
# =========================
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

    folder = drive_service.files().create(
        body=metadata,
        fields="id, name",
    ).execute()
    return folder


def get_or_create_root_folder():
    folder = find_folder_by_name(ROOT_FOLDER_NAME)
    if folder:
        return folder
    return create_folder(ROOT_FOLDER_NAME)


def get_or_create_week_folder(week_monday: str):
    root = get_or_create_root_folder()
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
    ).execute()
    files = results.get("files", [])
    return files[0] if files else None


def list_files_in_folder(folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name, mimeType)",
    ).execute()
    return results.get("files", [])


def download_excel_from_drive(filename: str, folder_id: str) -> pd.DataFrame:
    file = find_file_in_folder(filename, folder_id)
    if not file:
        raise FileNotFoundError(f"{filename} not found in this week folder.")

    request = drive_service.files().get_media(fileId=file["id"])
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

    media = MediaIoBaseUpload(io.BytesIO(file_bytes), resumable=True)

    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
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


def parse_submitted_teamids(folder_id: str, week_monday: str):
    files = list_files_in_folder(folder_id)
    submitted = set()

    for f in files:
        title = f["name"]
        if title == "Teams_merged.xlsx":
            continue

        match = re.match(r"^(\d+)([A-Z]+)" + re.escape(week_monday) + r"\.[A-Za-z0-9]+$", title)
        if match:
            submitted.add(clean_teamid(match.group(1)))

    return submitted


def build_missing_report(teams_df: pd.DataFrame, submitted_teamids: set):
    team_col = COLUMN_MAP["teamid"]
    cols = [team_col]

    if COLUMN_MAP["dsp_name"] in teams_df.columns:
        cols.append(COLUMN_MAP["dsp_name"])
    if COLUMN_MAP["region"] in teams_df.columns:
        cols.append(COLUMN_MAP["region"])

    expected = teams_df[cols].drop_duplicates().copy()
    missing = expected[~expected[team_col].isin(submitted_teamids)].copy()
    return missing.reset_index(drop=True)


# =========================
# UI
# =========================
default_week = monday_str(date.today())

st.subheader("1) Upload Invoice")

col1, col2, col3 = st.columns(3)
with col1:
    input_teamid = st.text_input("Team ID", placeholder="例如 1206")
with col2:
    input_region = st.selectbox("Warehouse", REGIONS)
with col3:
    input_week = st.text_input("Week Monday (YYYYMMDD)", value=default_week)

uploaded_file = st.file_uploader(
    "Upload invoice file",
    type=["pdf", "xlsx", "xls", "csv", "png", "jpg", "jpeg"],
)

manual_amount = st.number_input(
    "Invoice amount (manual input for now)",
    min_value=0.0,
    step=0.01,
    value=0.0,
)

if st.button("Validate and Upload", type="primary"):
    if not input_teamid.strip():
        st.error("Please enter Team ID.")
        st.stop()

    if not is_monday_string(input_week):
        st.error("Week Monday must be a Monday in YYYYMMDD format.")
        st.stop()

    if uploaded_file is None:
        st.error("Please upload an invoice file.")
        st.stop()

    try:
        teams_df = load_weekly_teams(input_week)
    except Exception as e:
        st.error(f"Could not load weekly Teams_merged.xlsx: {e}")
        st.stop()

    teamid = clean_teamid(input_teamid)
    expected_salary, _ = get_expected_salary(teams_df, teamid, input_region)

    if expected_salary is None:
        st.error("This team_id + warehouse was not found in this week's Teams_merged.xlsx.")
        st.stop()

    if manual_amount <= 0:
        st.error("Please input invoice amount manually for now.")
        st.stop()

    diff = abs(manual_amount - expected_salary)
    ext = get_extension(uploaded_file.name)
    new_filename = f"{teamid}{input_region}{input_week}{ext}"

    week_folder = get_or_create_week_folder(input_week)

    st.write(f"Expected salary: **{expected_salary:,.2f}**")
    st.write(f"Invoice amount: **{manual_amount:,.2f}**")
    st.write(f"Difference: **{diff:,.2f}**")

    if diff <= AMOUNT_TOLERANCE:
        file_bytes = uploaded_file.read()
        result = upload_file_to_drive(file_bytes, new_filename, week_folder["id"])

        if result == "duplicate":
            st.warning(f"File already exists: {new_filename}")
        else:
            st.success(f"Uploaded successfully as {new_filename}")
    else:
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
        )
        send_email(subject, body)
        st.error("Invoice amount does not match salary. Alert email sent.")

# =========================
# Dashboard
# =========================
st.subheader("2) Weekly Dashboard")

dashboard_week = st.text_input("Select week (YYYYMMDD)", value=default_week, key="dashboard_week")

if is_monday_string(dashboard_week):
    try:
        teams_df = load_weekly_teams(dashboard_week)
        week_folder = get_or_create_week_folder(dashboard_week)
        submitted_teamids = parse_submitted_teamids(week_folder["id"], dashboard_week)
        missing_df = build_missing_report(teams_df, submitted_teamids)

        c1, c2 = st.columns(2)
        c1.metric("Submitted team count", len(submitted_teamids))
        c2.metric("Missing team count", len(missing_df))

        st.markdown("### Missing Teams")
        if missing_df.empty:
            st.success("All teams have submitted invoices for this week.")
        else:
            st.dataframe(missing_df, use_container_width=True)

    except Exception as e:
        st.error(f"Could not build dashboard: {e}")
else:
    st.info("Please enter a valid Monday in YYYYMMDD format.")
    
