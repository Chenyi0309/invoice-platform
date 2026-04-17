import io
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

st.set_page_config(page_title="DSP Invoice Upload", layout="wide")

# =========================================================
# Config / 配置
# =========================================================
APP_TITLE = "DSP Upload Invoices / DSP 发票上传"
APP_SUBTITLE = (
    "Upload invoice files, auto-match expected amount from Teams_merged, "
    "and save into the selected warehouse folder in Google Drive. / "
    "上传发票，自动匹配 Teams_merged 金额，并保存到 Google Drive 对应仓库文件夹。"
)

# 你可以按需改这里
WAREHOUSE_OPTIONS = [
    "ORD", "IND", "FWA", "OMA", "CMH", "CVG", "SDF", "LEX", "DTW", "CLE", "TOL", "MSP"
]

# Teams_merged 常见列名候选（会自动模糊匹配）
COLUMN_CANDIDATES = {
    "team_id": ["team id", "team_id", "teamid", "team", "route team id"],
    "warehouse": ["warehouse", "station", "site", "hub", "segment", "branch"],
    "amount": ["amount", "invoice amount", "pay", "total", "total amount", "expected amount"],
    "week": ["week", "week monday", "monday", "week_start", "week start", "date", "period"],
}

SCOPES = ["https://www.googleapis.com/auth/drive"]

# =========================================================
# Helpers / 通用函数
# =========================================================

def normalize_text(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def clean_team_id(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    # 防止 Excel 把 team id 变成 1206.0
    if s.endswith(".0"):
        s = s[:-2]
    return s


def clean_week(value) -> str:
    if pd.isna(value):
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")

    s = str(value).strip()

    # excel serial 或日期字符串的兜底
    try:
        dt = pd.to_datetime(s)
        if pd.notna(dt):
            return dt.strftime("%Y%m%d")
    except Exception:
        pass

    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        return digits[:8]
    return digits


def clean_warehouse(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def clean_amount(value):
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def format_amount(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}"


def get_extension(filename: str) -> str:
    if "." not in filename:
        return ""
    return filename.rsplit(".", 1)[-1].lower()


def safe_filename_part(value: str) -> str:
    value = str(value).strip().replace(" ", "_")
    value = re.sub(r"[^A-Za-z0-9_\-]", "", value)
    return value


def detect_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    normalized_map = {normalize_text(col): col for col in df.columns}

    for cand in candidates:
        cand_norm = normalize_text(cand)
        if cand_norm in normalized_map:
            return normalized_map[cand_norm]

    for col in df.columns:
        norm_col = normalize_text(col)
        for cand in candidates:
            if normalize_text(cand) in norm_col:
                return col
    return None


# =========================================================
# Google Drive / Google Drive 相关
# =========================================================
@st.cache_resource(show_spinner=False)
def get_drive_service():
    secrets = st.secrets

    # 支持两种写法：
    # 1) [gcp_service_account] 形式
    # 2) GOOGLE_SERVICE_ACCOUNT_JSON = "{...json...}"
    if "gcp_service_account" in secrets:
        service_account_info = dict(secrets["gcp_service_account"])
    elif "GOOGLE_SERVICE_ACCOUNT_JSON" in secrets:
        service_account_info = json.loads(secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
    else:
        raise ValueError(
            "Missing Google service account credentials in Streamlit secrets. "
            "Please add [gcp_service_account] or GOOGLE_SERVICE_ACCOUNT_JSON."
        )

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES,
    )
    return build("drive", "v3", credentials=credentials)


def get_root_folder_id() -> str:
    if "DRIVE_ROOT_FOLDER_ID" not in st.secrets:
        raise ValueError("Missing DRIVE_ROOT_FOLDER_ID in Streamlit secrets.")
    return st.secrets["DRIVE_ROOT_FOLDER_ID"]


def find_folder(service, parent_id: str, folder_name: str) -> Optional[Dict]:
    q = (
        f"'{parent_id}' in parents and trashed=false and "
        f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    )
    result = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id, name)",
        pageSize=10,
    ).execute()
    files = result.get("files", [])
    return files[0] if files else None


def create_folder(service, parent_id: str, folder_name: str) -> Dict:
    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    return service.files().create(body=metadata, fields="id, name").execute()


def get_or_create_folder(service, parent_id: str, folder_name: str) -> Dict:
    existing = find_folder(service, parent_id, folder_name)
    if existing:
        return existing
    return create_folder(service, parent_id, folder_name)


def find_file_by_name(service, parent_id: str, filename: str) -> Optional[Dict]:
    q = f"'{parent_id}' in parents and trashed=false and name='{filename}'"
    result = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id, name, mimeType)",
        pageSize=20,
    ).execute()
    files = result.get("files", [])
    return files[0] if files else None


def upload_file_to_drive(
    service,
    parent_id: str,
    filename: str,
    file_bytes: bytes,
    mime_type: str = "application/octet-stream",
) -> Dict:
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=True)
    metadata = {
        "name": filename,
        "parents": [parent_id],
    }
    return service.files().create(body=metadata, media_body=media, fields="id, name, webViewLink").execute()


def download_file_bytes(service, file_id: str) -> bytes:
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer.read()


def find_teams_merged_file(service, week_folder_id: str) -> Optional[Dict]:
    result = service.files().list(
        q=f"'{week_folder_id}' in parents and trashed=false",
        spaces="drive",
        fields="files(id, name, mimeType)",
        pageSize=100,
    ).execute()

    for file in result.get("files", []):
        name_lower = file["name"].lower()
        if "teams_merged" in name_lower and name_lower.endswith((".xlsx", ".xls", ".csv")):
            return file
    return None


# =========================================================
# Teams_merged 处理
# =========================================================
@st.cache_data(show_spinner=False, ttl=300)
def load_teams_merged_from_drive(week_value: str) -> Tuple[Optional[pd.DataFrame], str]:
    try:
        service = get_drive_service()
        root_folder_id = get_root_folder_id()
    except Exception as e:
        return None, f"Google Drive config error: {e}"

    try:
        week_folder = find_folder(service, root_folder_id, week_value)
        if not week_folder:
            return None, f"Week folder {week_value} not found in Google Drive root."

        teams_file = find_teams_merged_file(service, week_folder["id"])
        if not teams_file:
            return None, f"Teams_merged file not found under week folder {week_value}."

        raw_bytes = download_file_bytes(service, teams_file["id"])
        filename = teams_file["name"].lower()

        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(raw_bytes))
        else:
            df = pd.read_excel(io.BytesIO(raw_bytes))

        return df, ""
    except Exception as e:
        return None, f"Failed to load Teams_merged: {e}"


def standardize_teams_merged(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], str, Dict[str, str]]:
    mapping = {}
    for key, candidates in COLUMN_CANDIDATES.items():
        detected = detect_column(df, candidates)
        if detected:
            mapping[key] = detected

    required = ["team_id", "warehouse", "amount"]
    missing = [k for k in required if k not in mapping]
    if missing:
        return None, f"Teams_merged is missing required columns: {', '.join(missing)}", mapping

    standardized = pd.DataFrame()
    standardized["team_id"] = df[mapping["team_id"]].apply(clean_team_id)
    standardized["warehouse"] = df[mapping["warehouse"]].apply(clean_warehouse)
    standardized["amount"] = df[mapping["amount"]].apply(clean_amount)

    if "week" in mapping:
        standardized["week"] = df[mapping["week"]].apply(clean_week)
    else:
        standardized["week"] = ""

    standardized = standardized.dropna(subset=["amount"])
    standardized = standardized[standardized["team_id"] != ""]
    standardized = standardized[standardized["warehouse"] != ""]

    return standardized, "", mapping


def match_expected_amount(df_std: pd.DataFrame, week: str, warehouse: str, team_id: str) -> Tuple[Optional[float], str]:
    team_id = clean_team_id(team_id)
    warehouse = clean_warehouse(warehouse)
    week = clean_week(week)

    if not team_id or not warehouse:
        return None, "Incomplete"

    # 优先按 week + warehouse + team_id 匹配
    filtered = df_std[
        (df_std["team_id"] == team_id)
        & (df_std["warehouse"] == warehouse)
    ].copy()

    if "week" in df_std.columns and df_std["week"].fillna("").astype(str).str.len().gt(0).any():
        filtered_week = filtered[filtered["week"] == week]
        if not filtered_week.empty:
            if len(filtered_week) == 1:
                return filtered_week.iloc[0]["amount"], "Matched"
            return filtered_week["amount"].sum(), "Multiple rows summed"

    # 如果 Teams_merged 没有 week 列，或者 week 没匹配上，就退回 team+warehouse
    if not filtered.empty:
        if len(filtered) == 1:
            return filtered.iloc[0]["amount"], "Matched (no week filter)"
        return filtered["amount"].sum(), "Multiple rows summed"

    return None, "Not found"


# =========================================================
# Session State / 动态多行
# =========================================================

def init_rows():
    if "invoice_rows" not in st.session_state:
        st.session_state.invoice_rows = [
            {"warehouse": "ORD", "team_id": "", "file": None}
        ]


def add_row():
    st.session_state.invoice_rows.append({"warehouse": "ORD", "team_id": "", "file": None})


def remove_row(index: int):
    if len(st.session_state.invoice_rows) > 1:
        st.session_state.invoice_rows.pop(index)


# =========================================================
# UI
# =========================================================
st.title(APP_TITLE)
st.caption(APP_SUBTITLE)

init_rows()

with st.sidebar:
    st.markdown("### Setup / 配置说明")
    st.markdown(
        "**Required Streamlit secrets / 必填 secrets**\n"
        "- `DRIVE_ROOT_FOLDER_ID`\n"
        "- `[gcp_service_account]` or `GOOGLE_SERVICE_ACCOUNT_JSON`"
    )
    st.markdown(
        "**Google Drive folder structure / Google Drive 文件结构**\n"
        "- Root / 根目录: `DSP_Invoices`\n"
        "- Week folder / 周目录: `20260413`\n"
        "- Warehouse subfolder / 仓库子目录: `IND`, `ORD` ...\n"
        "- `Teams_merged.xlsx` should be under the week folder root / `Teams_merged.xlsx` 请放在周目录根下"
    )

week_monday = st.text_input(
    "Week Monday / 周一日期 (YYYYMMDD)",
    value=datetime.today().strftime("%Y%m%d"),
    help="Example: 20260413",
)

if not re.fullmatch(r"\d{8}", week_monday.strip()):
    st.warning("Please enter a valid week Monday in YYYYMMDD format. / 请输入正确的 YYYYMMDD 日期。")

teams_df_raw = None
teams_df_std = None
teams_mapping = {}
load_error = ""

if re.fullmatch(r"\d{8}", week_monday.strip()):
    with st.spinner("Loading Teams_merged from Google Drive... / 正在加载 Teams_merged..."):
        teams_df_raw, load_error = load_teams_merged_from_drive(week_monday.strip())
        if teams_df_raw is not None:
            teams_df_std, std_error, teams_mapping = standardize_teams_merged(teams_df_raw)
            if std_error:
                load_error = std_error

if load_error:
    st.error(load_error)
else:
    st.success("Teams_merged loaded successfully. / Teams_merged 加载成功。")

st.markdown("---")
st.subheader("Invoice Lines / 发票明细")

row_results = []
files_to_upload = []

for i, row in enumerate(st.session_state.invoice_rows):
    with st.container(border=True):
        top_cols = st.columns([1.2, 1.2, 0.8, 0.8])

        warehouse = top_cols[0].selectbox(
            f"Warehouse / 仓库 #{i+1}",
            options=WAREHOUSE_OPTIONS,
            index=WAREHOUSE_OPTIONS.index(row.get("warehouse", "ORD")) if row.get("warehouse", "ORD") in WAREHOUSE_OPTIONS else 0,
            key=f"warehouse_{i}",
        )

        team_id = top_cols[1].text_input(
            f"Team ID / 团队编号 #{i+1}",
            value=row.get("team_id", ""),
            key=f"team_id_{i}",
            placeholder="e.g. 1206",
        )

        uploaded = top_cols[2].file_uploader(
            f"Invoice File / 发票文件 #{i+1}",
            type=["pdf", "xlsx", "xls", "csv", "png", "jpg", "jpeg"],
            key=f"file_{i}",
        )

        if top_cols[3].button(f"Remove / 删除 #{i+1}", key=f"remove_{i}"):
            remove_row(i)
            st.rerun()

        expected_amount = None
        match_status = "Waiting"

        if teams_df_std is not None:
            expected_amount, match_status = match_expected_amount(
                teams_df_std,
                week=week_monday,
                warehouse=warehouse,
                team_id=team_id,
            )

        result_df = pd.DataFrame([
            {
                "Warehouse": warehouse,
                "Team ID": clean_team_id(team_id),
                "Expected Amount": format_amount(expected_amount),
                "Status": match_status,
                "File Selected": "Yes" if uploaded is not None else "No",
            }
        ])
        st.dataframe(result_df, use_container_width=True, hide_index=True)

        st.session_state.invoice_rows[i] = {
            "warehouse": warehouse,
            "team_id": team_id,
            "file": uploaded,
        }

        row_results.append(
            {
                "warehouse": warehouse,
                "team_id": clean_team_id(team_id),
                "expected_amount": expected_amount,
                "status": match_status,
                "file": uploaded,
            }
        )

st.button("+ Add Another Site / 新增站点", on_click=add_row)

# 汇总预览
summary_df = pd.DataFrame([
    {
        "Warehouse": r["warehouse"],
        "Team ID": r["team_id"],
        "Expected Amount": format_amount(r["expected_amount"]),
        "Status": r["status"],
        "File Selected": "Yes" if r["file"] is not None else "No",
    }
    for r in row_results
])

st.markdown("---")
st.subheader("Review / 提交预览")
if not summary_df.empty:
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

valid_for_submit = True
validation_errors = []

seen_keys = set()
for idx, r in enumerate(row_results, start=1):
    key = (r["warehouse"], r["team_id"])
    if not r["team_id"]:
        valid_for_submit = False
        validation_errors.append(f"Row {idx}: Team ID is required.")
    if r["file"] is None:
        valid_for_submit = False
        validation_errors.append(f"Row {idx}: Invoice file is required.")
    if key in seen_keys:
        valid_for_submit = False
        validation_errors.append(f"Row {idx}: Duplicate warehouse + team ID in the same submission.")
    seen_keys.add(key)

if teams_df_std is None:
    valid_for_submit = False
    validation_errors.append("Teams_merged is not loaded, so submission is blocked.")

if validation_errors:
    for err in validation_errors:
        st.warning(err)

submit_btn = st.button("Submit All Invoices / 提交全部发票", type="primary", disabled=not valid_for_submit)

if submit_btn and valid_for_submit:
    try:
        service = get_drive_service()
        root_folder_id = get_root_folder_id()

        with st.spinner("Submitting files to Google Drive... / 正在上传到 Google Drive..."):
            # 1) 获取或创建 week folder
            week_folder = get_or_create_folder(service, root_folder_id, week_monday)
            uploaded_results = []

            for item in row_results:
                warehouse = clean_warehouse(item["warehouse"])
                team_id = clean_team_id(item["team_id"])
                uploaded_file = item["file"]
                expected_amount = item["expected_amount"]
                status = item["status"]

                # 2) 获取或创建 warehouse folder
                warehouse_folder = get_or_create_folder(service, week_folder["id"], warehouse)

                # 3) 重命名文件
                ext = get_extension(uploaded_file.name)
                base_filename = f"{safe_filename_part(team_id)}_{safe_filename_part(warehouse)}_{safe_filename_part(week_monday)}"
                final_filename = f"{base_filename}.{ext}" if ext else base_filename

                # 如果重名，加时间戳避免覆盖
                existing = find_file_by_name(service, warehouse_folder["id"], final_filename)
                if existing:
                    timestamp = datetime.now().strftime("%H%M%S")
                    final_filename = f"{base_filename}_{timestamp}.{ext}" if ext else f"{base_filename}_{timestamp}"

                file_bytes = uploaded_file.getvalue()
                mime_type = getattr(uploaded_file, "type", None) or "application/octet-stream"
                uploaded_meta = upload_file_to_drive(
                    service=service,
                    parent_id=warehouse_folder["id"],
                    filename=final_filename,
                    file_bytes=file_bytes,
                    mime_type=mime_type,
                )

                uploaded_results.append({
                    "Warehouse": warehouse,
                    "Team ID": team_id,
                    "Expected Amount": format_amount(expected_amount),
                    "Match Status": status,
                    "Saved Folder": f"{week_monday}/{warehouse}",
                    "Saved File": final_filename,
                    "Drive Link": uploaded_meta.get("webViewLink", ""),
                })

        st.success("All invoices uploaded successfully. / 所有发票已成功上传。")
        uploaded_df = pd.DataFrame(uploaded_results)
        st.dataframe(uploaded_df, use_container_width=True, hide_index=True)

        # 成功后重置表单
        st.session_state.invoice_rows = [{"warehouse": "ORD", "team_id": "", "file": None}]

    except Exception as e:
        st.error(f"Upload failed / 上传失败: {e}")

st.markdown("---")
with st.expander("Optional: Preview Teams_merged / 可选：预览 Teams_merged"):
    if teams_df_std is not None:
        st.write("Detected column mapping / 识别到的列映射:")
        st.json(teams_mapping)
        st.dataframe(teams_df_std.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Teams_merged not available.")
