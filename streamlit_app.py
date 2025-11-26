import streamlit as st
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import secrets
import hashlib
from datetime import datetime, timedelta
import os
import re
from io import BytesIO
from dotenv import load_dotenv
from typing import List

# --- SQLAlchemy / DB ---
import pymysql  # needed for mysql+pymysql
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Date,
    Text,
    Boolean,
    Float,
    ForeignKey,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker
import hashlib


# ===============================
# CONFIG & CONSTANTS
# ===============================
load_dotenv()

# Email (use app passwords for Gmail)
EMAIL_HOST = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
EMAIL_PORT = int(os.getenv('MAIL_PORT', 587))
FROM_EMAIL = os.getenv('MAIL_DEFAULT_SENDER', os.getenv('MAIL_USERNAME', 'no-reply@example.com'))
EMAIL_USERNAME = os.getenv('MAIL_USERNAME', '')
EMAIL_PASSWORD = os.getenv('MAIL_PASSWORD', '')

# MySQL settings
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "rejection_reports")

BASE_EXCEL_DIR = os.getenv("EXCEL_BASE_DIR", "excels")
MC_REGISTRY_TABLE = "mc_table_registry"

# Only these two sheets are processed
ACCEPTED_SHEETS = ["Rejection DIP-1", "Rejection DIP-2","Prod-Rej"]

# Canonical columns we want to keep in DB and per-MC tables
REQUIRED_COLUMNS = [
    'PLANT', 'DATE','SHIFT','SHIFT I/C','DN/CLASS','LENGTH','CATEGORY',
    'STAGE','Y','M','DD','M/C#','PN','BATCH','CASTING SHIFT',
    'MOULD NO','RUN NO','WEIGHT','VISUAL DEFECT','DEFECT LOC',
    'DEFECT AT LEAK POINT','LINING','ENTRY'
]

PROD_REJ_REQUIRED_COLUMNS = [
    "PLANT", "DATE", "SHIFT", "M/C#", "DN/CLASS",
    "LENGTH", "CAST NOs", "Casting Rej NOs", "Casting Rej%",
    "CONV NOs", "HPTM Testing", "HPTM Rej NOs", "HPTM Rej%",
    "Annealing Rej NOs", "Online Rej NOs", "Final Rej NOs",
    "Rework Rej NOs", "Yard Rej NOs", "S/R Rej NOs",
    "Other Rej NOs", "Other Rej%", "Total Rej NOs", "Total Rej%",
    "S/L Cut Loss MT"
]

# The truly mandatory key(s) to proceed (we must have M/C#)
REQUIRED_KEY_COLS = ['M/C#']
OPTIONAL_COLS = [c for c in REQUIRED_COLUMNS if c not in REQUIRED_KEY_COLS]

# Expanded alias map so minor header variations won’t break loads
HEADER_ALIASES = {
    'DATE': {'DATE','Date','date','Dt','DT'},
    'SHIFT': {'SHIFT','Shift'},
    'SHIFT I/C': {'SHIFT I/C','SHIFT IC','SHIFT I-C','SHIFT I C','SHIFT_INCHARGE','SHIFT INCHARGE','SHIFT IN-CHARGE'},
    'DN/CLASS': {'DN/CLASS','DN CLASS','DN-CLASS','DN_CLASS','DN / CLASS'},
    'LENGTH': {'LENGTH','Length'},
    'CATEGORY': {'CATEGORY','Category'},
    'STAGE': {'STAGE','Stage'},
    'Y': {'Y','Year','YEAR'},
    'M': {'M','Month','MONTH'},
    'DD': {'DD','Day','DAY'},

    'M/C#': {'M/C#','MC#','M/C NO','M/C NO.','M/C','M C#','M-C#','M C #','MACHINE','MACHINE#','MACHINE NO','MACHINE NO.'},

    'PN': {'PN','P/N','PIPE NO','PIPE NO.'},
    'BATCH': {'BATCH','Batch','LOT','LOT NO','LOT NO.'},

    'CASTING SHIFT': {'CASTING SHIFT','Casting Shift','CASTING_SHIFT','CAST SHIFT','CAST-SHIFT','CASTINGSHIFT'},
    'MOULD NO': {'MOULD NO','MOULD NO.','MOULD#','MOULD','MOULD NUMBER','MOULD NUM','MOULD_NO','MOULD-NO'},
    'RUN NO': {'RUN NO','RUN NO.','RUN#','RUN','RUN NUMBER','RUN_NO','RUN-NO','RUNNO'},

    'WEIGHT': {'WEIGHT','Weight','Wt','WT'},

    'VISUAL DEFECT': {'VISUAL DEFECT','Visual Defect','VISUAL_DEFECT','VISUAL-DEFECT','DEFECT (VISUAL)'},
    'DEFECT LOC': {'DEFECT LOC','DEFECT LOCATION','Defect Loc','DEFECT_LOC','DEFECT-LOC','DEFECT PLACE'},
    'DEFECT AT LEAK POINT': {'DEFECT AT LEAK POINT','LEAK POINT DEFECT','LEAK_POINT_DEFECT','DEFECT@LEAK POINT','LEAK AT DEFECT POINT'},

    'LINING': {'LINING','Lining','LINNING','LINING STATUS','LINING-STATUS','LINING/COATING'},
    'ENTRY': {'ENTRY','Entry','ENTRY DATE','ENTRY DT','ENTRYDATE','ENTRY_DT','ENTERED ON','ENTERED DATE'}
}

PROD_REJ_ALIASES = {
    "PLANT": {"PLANT", "Plant"},
    "DATE": {"DATE", "Date"},
    "SHIFT": {"SHIFT", "Shift"},

    # MC#
    "M/C#": {"M/C#", "MC#", "M/C NO", "M/C NO.", "MACHINE", "Machine No"},

    # DN/CLASS
    "DN/CLASS": {"DN/CLASS", "DN CLASS", "DN-Class", "DN\\CLASS"},

    # LENGTH
    "LENGTH": {"LENGTH", "Len", "Length"},

    # CAST NOs
    "CAST NOs": {"CAST NOs", "CAST NOS", "CAST\nNOs", "CAST Nos", "CAST_NOs"},

    # CAST MT → treat as Casting Rej NOs OR keep as separate?
    # Most clients treat CAST MT as Casting MT (NOT rejection).
    # But you don't have a CAST MT column in DB model.
    # So we map it to LENGTH? NO.
    # We will IGNORE it (leave as-is) because your DB doesn't store CAST MT.

    # Casting Rej NOs
    "Casting Rej NOs": {
        "Casting Rej NOs",
        "Casting Rej Nos",
        "Casting Rej NO's",
        "Casting Rejection NOs"
    },

    # Casting Rej %
    "Casting Rej%": {
        "Casting Rej%",
        "Casting Rej %",
        "Casting Rejection %",
        "Casting % Rej"
    },

    # CONV NOs
    "CONV NOs": {"CONV NOs", "Conv Nos", "CONV\nNOs", "Conv_Nos"},

    # HPTM Testing
    "HPTM Testing": {"HPTM Testing", "HPTM Test"},

    # HPTM Rej NOs
    "HPTM Rej NOs": {"HPTM Rej NOs", "HPTM Rej", "HPTM\nRej", "HPTM Rejection"},

    # HPTM Rej %
    "HPTM Rej%": {"HPTM Rej%", "HPTM Rej %", "HPTM\nRej %", "HPTM Rejection %"},

    # Annealing Rej NOs
    "Annealing Rej NOs": {
        "Annealing Rej NOs",
        "Annealing Rej",
        "Annealing Rej Nos"
    },

    # Online Rej
    "Online Rej NOs": {"Online Rej NOs", "Online Rej", "Online Rejection"},

    # Final Rej
    "Final Rej NOs": {"Final Rej NOs", "Final Rej", "Final Rejection"},

    # Rework
    "Rework Rej NOs": {"Rework Rej NOs", "Rework Rej", "Rework Rejection"},

    # Yard
    "Yard Rej NOs": {"Yard Rej NOs", "Yard Rej", "Yard Rejection"},

    # S/R
    "S/R Rej NOs": {"S/R Rej NOs", "SR Rej Nos", "S R Rej NOs"},

    # Other Rej %
    "Other Rej%": {"Other Rej%", "Other Rej %", "Other Rejection %"},

    # Other Rej NOs
    "Other Rej NOs": {"Other Rej NOs", "Other Rej Nos", "Other Rejection"},

    # Total Rej
    "Total Rej NOs": {"Total Rej NOs", "Total Rej NOS", "Total Rej"},
    "Total Rej%": {"Total Rej%", "Total Rej %", "Total Rejection %"},


    # Cut Loss
    "S/L Cut Loss MT": {
        "S/L Cut Loss MT",
        "SL Cut Loss MT",
        "S L Cut Loss MT"
    },
}

# ===============================
# SQLALCHEMY SETUP
# ===============================
DATABASE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    last_login = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class OTPVerification(Base):
    __tablename__ = "otp_verification"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False)
    otp_code = Column(String(10), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    is_used = Column(Boolean, default=False)


class FileVersion(Base):
    __tablename__ = "file_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    stored_filename = Column(String(255), nullable=False)
    file_path = Column(Text, nullable=False)
    version_number = Column(Integer, nullable=False)
    file_hash = Column(String(64), nullable=False)
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    file_size = Column(Integer)
    record_count = Column(Integer)


class RejectionData(Base):
    __tablename__ = "rejection_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(255), nullable=False)

    plant = Column(String(50))              # <-- FIXED
    date_field = Column(Date)               # <-- GOOD
    shift = Column(String(10))
    shift_ic = Column(String(50))
    dn_class = Column(String(50))
    length = Column(String(50))
    category = Column(String(255))
    stage = Column(String(255))
    y = Column(String(4))
    m = Column(String(4))
    dd = Column(String(4))
    mc_number = Column(Integer)             # <-- FIXED
    pn = Column(String(50))
    batch = Column(String(50))
    casting_shift = Column(String(50))
    mould_no = Column(String(50))
    run_no = Column(String(50))
    weight = Column(String(50))
    visual_defect = Column(String(255))
    defect_loc = Column(String(255))
    defect_at_leak_point = Column(String(255))
    lining = Column(String(255))
    entry = Column(String(255))
    source_sheet = Column(String(255))
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    original_filename = Column(String(255))
    file_version = Column(Integer)
    row_hash = Column(String(64), index=True)

class ProdRejData(Base):
    __tablename__ = "prod_rej_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(255), nullable=False)

    plant = Column(String(50), nullable=True)
    date_field = Column(Date, nullable=True)
    shift = Column(String(50), nullable=True)
    mc_number = Column(Integer, nullable=True)
    dn_class = Column(String(100), nullable=True)
    length = Column(Float, nullable=True)

    cast_nos = Column(Integer, nullable=True)
    casting_rej_nos = Column(Integer, nullable=True)
    casting_rej_percent = Column(Float, nullable=True)

    conv_nos = Column(Integer, nullable=True)

    hptm_testing = Column(Integer, nullable=True)
    hptm_rej_nos = Column(Integer, nullable=True)
    hptm_rej_percent = Column(Float, nullable=True)

    annealing_rej_nos = Column(Integer, nullable=True)
    online_rej_nos = Column(Integer, nullable=True)
    final_rej_nos = Column(Integer, nullable=True)
    rework_rej_nos = Column(Integer, nullable=True)
    yard_rej_nos = Column(Integer, nullable=True)
    sr_rej_nos = Column(Integer, nullable=True)

    other_rej_nos = Column(Integer, nullable=True)
    other_rej_percent = Column(Float, nullable=True)

    total_rej_nos = Column(Integer, nullable=True)
    total_rej_percent = Column(Float, nullable=True)

    sl_cut_loss_mt = Column(Float, nullable=True)

    source_sheet = Column(String(255), nullable=True)
    original_filename = Column(String(255), nullable=True)
    file_version = Column(Integer, nullable=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    row_hash = Column(String(64), index=True)

class MachineRegistry(Base):
    __tablename__ = MC_REGISTRY_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True)
    mc_value = Column(String(255), nullable=False)
    table_name = Column(String(255), nullable=False)


def get_session():
    return SessionLocal()


def init_database():
    Base.metadata.create_all(bind=engine)


# ===========================================
# HASHING THE ROWS TO AVOID DUPLICATES INSERT
# ===========================================

import hashlib

def compute_row_hash(row):
    # Use the REAL columns in your rejection_data table
    fields = [
        'plant', 
        'date_field', 'shift', 'shift_ic', 'dn_class', 'length', 'category',
        'stage', 'y', 'm', 'dd', 'mc_number', 'pn', 'batch', 'casting_shift',
        'mould_no', 'run_no', 'weight', 'visual_defect', 'defect_loc',
        'defect_at_leak_point', 'lining', 'entry', 'source_sheet'
    ]

    concat = "|".join(str(row.get(f, "")).strip() for f in fields)
    return hashlib.md5(concat.encode('utf-8')).hexdigest()

# ===============================
# STREAMLIT PAGE SETUP
# ===============================
st.set_page_config(page_title="Rejection Report Automation", page_icon="📊", layout="wide")
st.title("📊 Rejection Report Automation System")
st.caption("Only sheets processed: **Rejection DIP-1** and **Rejection DIP-2**")

# ===============================
# EMAIL / OTP
# ===============================
def send_otp_email(email, otp):
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = email
        msg['Subject'] = "OTP Verification - Rejection Report System"
        body = f"""
        Dear User,

        Your OTP for accessing the Rejection Report System is: {otp}

        This OTP will expire in 10 minutes.

        Best regards,
        Welspun Team
        """
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        if EMAIL_USERNAME and EMAIL_PASSWORD:
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        st.error(f"Failed to send OTP: {str(e)}")
        return False


def generate_otp():
    return f"{secrets.randbelow(10**6):06d}"


def store_otp(email, otp):
    session = get_session()
    try:
        session.query(OTPVerification).filter(OTPVerification.email == email).delete()
        expires_at = datetime.now() + timedelta(minutes=10)
        otp_obj = OTPVerification(
            email=email,
            otp_code=otp,
            expires_at=expires_at,
            is_used=False,
        )
        session.add(otp_obj)
        session.commit()
    finally:
        session.close()


def verify_otp(email, otp):
    session = get_session()
    try:
        now = datetime.now()
        otp_obj = (
            session.query(OTPVerification)
            .filter(
                OTPVerification.email == email,
                OTPVerification.otp_code == otp,
                OTPVerification.expires_at > now,
                OTPVerification.is_used == False,
            )
            .first()
        )
        if otp_obj:
            otp_obj.is_used = True
            session.commit()
            return True
        return False
    finally:
        session.close()


def update_user_login(email):
    session = get_session()
    try:
        user = session.query(User).filter(User.email == email).first()
        if not user:
            user = User(email=email)
            session.add(user)
        user.last_login = datetime.now()
        session.commit()
    finally:
        session.close()


def get_user_info(email):
    session = get_session()
    try:
        user = session.query(User).filter(User.email == email).first()
        return user.last_login if user else None
    finally:
        session.close()

# ===============================
# HEADERS / CLEANUP / HASH / FILE IO
# ===============================
def normalize_headers(df):
    # Step 1: clean raw column names first
    cleaned = {c: clean_header(c) for c in df.columns}
    df = df.rename(columns=cleaned)

    rename_map = {}

    # Step 2: DIP alias matching
    for canonical, aliases in HEADER_ALIASES.items():
        for c in df.columns:
            if clean_header(c) in aliases:
                rename_map[c] = canonical

    # Step 3: Prod-Rej alias matching
    for canonical, aliases in PROD_REJ_ALIASES.items():
        for c in df.columns:
            if clean_header(c) in aliases:
                rename_map[c] = canonical

    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def ensure_columns(df: pd.DataFrame, cols: List[str], fill: str = "") -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = fill
    return df


def coerce_text_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all values to text and trim whitespace.
    Fully vectorized — no applymap().
    """
    df = df.fillna("")
    df = df.astype(str)

    # strip whitespace for all string columns
    df = df.apply(lambda col: col.str.strip())

    return df

def calculate_file_hash(df: pd.DataFrame) -> str:
    """Stable content hash regardless of row/column order and data types."""
    df_norm = normalize_headers(df.copy())
    df_norm = ensure_columns(df_norm, REQUIRED_COLUMNS, fill="")
    df_norm = coerce_text_df(df_norm)

    keep = [c for c in REQUIRED_COLUMNS if c in df_norm.columns]
    if keep:
        df_norm = df_norm[keep].sort_values(by=keep).reset_index(drop=True)
    else:
        df_norm = pd.DataFrame(columns=REQUIRED_COLUMNS)

    payload = pd.util.hash_pandas_object(df_norm, index=False).values.tobytes()
    return hashlib.md5(payload).hexdigest()


def create_excel_folders():
    if not os.path.exists(BASE_EXCEL_DIR):
        os.makedirs(BASE_EXCEL_DIR)
    return BASE_EXCEL_DIR


def get_user_folder(user_email):
    base = create_excel_folders()
    safe_email = re.sub(r'[<>:"/\\|?*]', '_', user_email.split('@')[0])
    p = os.path.join(base, safe_email)
    if not os.path.exists(p):
        os.makedirs(p)
    return p


def get_next_version_number(user_email, original_filename):
    session = get_session()
    try:
        max_ver = (
            session.query(FileVersion.version_number)
            .filter(
                FileVersion.user_email == user_email,
                FileVersion.original_filename == original_filename,
            )
            .order_by(FileVersion.version_number.desc())
            .first()
        )
        if not max_ver or max_ver[0] is None:
            return 1
        return int(max_ver[0]) + 1
    finally:
        session.close()


def generate_versioned_filename(original_filename, version_number):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name, ext = os.path.splitext(original_filename)
    safe_name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return f"{safe_name}_v{version_number}_{timestamp}{ext}"


def save_excel_file(uploaded_file, user_email):
    try:
        folder = get_user_folder(user_email)
        version_number = get_next_version_number(user_email, uploaded_file.name)
        versioned_filename = generate_versioned_filename(uploaded_file.name, version_number)
        file_path = os.path.join(folder, versioned_filename)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        file_size = os.path.getsize(file_path)
        return {
            'file_path': file_path,
            'versioned_filename': versioned_filename,
            'version_number': version_number,
            'file_size': file_size
        }
    except Exception as e:
        st.error(f"Error saving file: {str(e)}")
        return None

def process_prod_rej_sheet(df, sheet_name):

    df = normalize_headers(df)
    st.write("RAW Prod-Rej Columns:", list(df.columns))
    # Required columns check
    missing = [c for c in PROD_REJ_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        st.error(f"Prod-Rej sheet missing columns: {missing}")
        return pd.DataFrame()

    # Clean DATE
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date

    df = df[df["M/C#"].notna()]
    df["M/C#"] = df["M/C#"].astype(int)


    # Clean numeric columns
    numeric_cols = [
        "LENGTH", "CAST NOs", "Casting Rej NOs", "Casting Rej%", "CONV NOs",
        "HPTM Testing", "HPTM Rej NOs", "HPTM Rej%",
        "Annealing Rej NOs", "Online Rej NOs", "Final Rej NOs",
        "Rework Rej NOs", "Yard Rej NOs", "S/R Rej NOs",
        "Other Rej NOs", "Other Rej%", "Total Rej NOs", "Total Rej%", 
        "S/L Cut Loss MT"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["source_sheet"] = sheet_name

    return df

def store_file_version_info(
    user_email,
    original_filename,
    stored_filename,
    file_path,
    version_number,
    file_hash,
    file_size,
    record_count,
):
    session = get_session()
    try:
        fv = FileVersion(
            user_email=user_email,
            original_filename=original_filename,
            stored_filename=stored_filename,
            file_path=file_path,
            version_number=version_number,
            file_hash=file_hash,
            file_size=file_size,
            record_count=record_count,
        )
        session.add(fv)
        session.commit()
    finally:
        session.close()

# ===============================
# REJECTION DATA PERSISTENCE
# ===============================
def store_excel_data_to_db(df: pd.DataFrame, user_email, original_filename, version_number):
    session = get_session()
    try:
        # Compute row-hash based on DB schema columns
        df["row_hash"] = df.apply(lambda r: compute_row_hash(r), axis=1)

        # Find existing rows in DB
        existing_hashes = set(
            h[0] for h in session.query(RejectionData.row_hash)
                .filter(RejectionData.row_hash.in_(df["row_hash"].tolist()))
                .all()
        )

        new_records = []

        for _, r in df.iterrows():
            h = r["row_hash"]

            if h in existing_hashes:
                continue  # Skip duplicate

            rec = RejectionData(
                user_email=user_email,
                plant=r.get('plant', ''),
                date_field=r.get('date_field', ''),
                shift=r.get('shift', ''),
                shift_ic=r.get('shift_ic', ''),
                dn_class=r.get('dn_class', ''),
                length=r.get('length', ''),
                category=r.get('category', ''),
                stage=r.get('stage', ''),
                y=r.get('y', ''),
                m=r.get('m', ''),
                dd=r.get('dd', ''),
                mc_number=r.get('mc_number', None),
                pn=r.get('pn', ''),
                batch=r.get('batch', ''),
                casting_shift=r.get('casting_shift', ''),
                mould_no=r.get('mould_no', ''),
                run_no=r.get('run_no', ''),
                weight=r.get('weight', ''),
                visual_defect=r.get('visual_defect', ''),
                defect_loc=r.get('defect_loc', ''),
                defect_at_leak_point=r.get('defect_at_leak_point', ''),
                lining=r.get('lining', ''),
                entry=r.get('entry', ''),
                source_sheet=r.get('source_sheet', ''),
                original_filename=original_filename,
                file_version=version_number,
                row_hash=h
            )

            new_records.append(rec)

        if new_records:
            session.add_all(new_records)
            session.commit()

        return len(new_records)

    finally:
        session.close()
        
def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and (pd.isna(v) or str(v) == "nan"):
        return None
    return v

def store_prod_rej_rows(df, user_email, fname, version):
    session = get_session()
    try:
        # Convert NaN → None (first pass)
        df = df.where(pd.notnull(df), None)

        # Generate row hash
        df["row_hash"] = df.apply(
            lambda r: hashlib.md5(
                f"{r.get('DATE')}|{r.get('M/C#')}|{r.get('SHIFT')}|{r.get('Total Rej NOs')}".encode()
            ).hexdigest(),
            axis=1
        )

        existing = set(
            h[0]
            for h in session.query(ProdRejData.row_hash)
                .filter(ProdRejData.row_hash.in_(df["row_hash"].tolist()))
        )

        new_rows = []

        def to_int(v):
            if v is None:
                return None
            try:
                if isinstance(v, float) and pd.isna(v):
                    return None
                return int(v)
            except:
                return None

        def to_float(v):
            if v is None:
                return None
            try:
                if isinstance(v, float) and pd.isna(v):
                    return None
                return float(v)
            except:
                return None

        def clean(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            return v

        for _, r in df.iterrows():

            if r["row_hash"] in existing:
                continue

            new_rows.append(
                ProdRejData(
                    user_email=user_email,

                    plant=clean(r.get("PLANT")),
                    date_field=r.get("DATE"),
                    shift=clean(r.get("SHIFT")),
                    mc_number=to_int(r.get("M/C#")),
                    dn_class=clean(r.get("DN/CLASS")),
                    length=to_float(r.get("LENGTH")),

                    cast_nos=to_int(r.get("CAST NOs")),
                    casting_rej_nos=to_int(r.get("Casting Rej NOs")),
                    casting_rej_percent=to_float(r.get("Casting Rej%")),

                    conv_nos=to_int(r.get("CONV NOs")),
                    hptm_testing=to_int(r.get("HPTM Testing")),
                    hptm_rej_nos=to_int(r.get("HPTM Rej NOs")),
                    hptm_rej_percent=to_float(r.get("HPTM Rej%")),

                    annealing_rej_nos=to_int(r.get("Annealing Rej NOs")),
                    online_rej_nos=to_int(r.get("Online Rej NOs")),
                    final_rej_nos=to_int(r.get("Final Rej NOs")),
                    rework_rej_nos=to_int(r.get("Rework Rej NOs")),
                    yard_rej_nos=to_int(r.get("Yard Rej NOs")),
                    sr_rej_nos=to_int(r.get("S/R Rej NOs")),

                    other_rej_nos=to_int(r.get("Other Rej NOs")),
                    other_rej_percent=to_float(r.get("Other Rej%")),

                    total_rej_nos=to_int(r.get("Total Rej NOs")),
                    total_rej_percent=to_float(r.get("Total Rej%")),

                    sl_cut_loss_mt=to_float(r.get("S/L Cut Loss MT")),

                    source_sheet=clean(r.get("source_sheet")),
                    original_filename=fname,
                    file_version=version,
                    row_hash=r["row_hash"]
                )
            )

        if new_rows:
            session.add_all(new_rows)
            session.commit()

        return len(new_rows)

    finally:
        session.close()

def get_rejection_data(user_email, limit=None):
    # Use pandas + SQLAlchemy text for convenience
    base_query = """
        SELECT * FROM rejection_data
        WHERE user_email = :user_email
        ORDER BY uploaded_at DESC
    """
    if limit:
        base_query += " LIMIT :limit_val"
        params = {"user_email": user_email, "limit_val": int(limit)}
    else:
        params = {"user_email": user_email}

    df = pd.read_sql(text(base_query), engine, params=params)
    return df


def get_file_versions_data(user_email):
    q = """
        SELECT original_filename, stored_filename, version_number,
               upload_timestamp, file_size, record_count, file_hash
        FROM file_versions
        WHERE user_email = :user_email
        ORDER BY upload_timestamp DESC
    """
    df = pd.read_sql(text(q), engine, params={"user_email": user_email})
    return df

# ===============================
# PER-MACHINE TABLES (TEXT-ONLY)
# ===============================
def sanitize_table_name(raw: str) -> str:
    if raw is None:
        raw = "UNKNOWN"
    s = str(raw).strip()
    if not s:
        s = "UNKNOWN"
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    if not re.match(r'^[A-Za-z_]', s):
        s = f"_{s}"
    s = re.sub(r'_+', '_', s).strip('_')
    safe = f"mc_{s}" if not s.lower().startswith("mc_") else s
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', safe):
        safe = "mc_unknown"
    return safe


def create_mc_table(conn, table_name: str, columns: List[str]):
    cols_sql = ", ".join([f"`{c}` TEXT" for c in columns])
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols_sql})"))


def bulk_insert_text(conn, table_name: str, columns: List[str], rows):
    cols_sql = ", ".join([f"`{c}`" for c in columns])
    placeholders = ", ".join([f":{c}" for c in columns])
    stmt = text(f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})")

    for row in rows:
        params = {col: val for col, val in zip(columns, row)}
        conn.execute(stmt, params)


def drop_existing_mc_tables():
    with engine.begin() as conn:
        res = conn.execute(text(f"SELECT table_name FROM {MC_REGISTRY_TABLE}"))
        for (tname,) in res.fetchall():
            conn.execute(text(f"DROP TABLE IF EXISTS `{tname}`"))
        conn.execute(text(f"DELETE FROM {MC_REGISTRY_TABLE}"))


def recreate_mc_tables_from_df(df_filtered: pd.DataFrame, original_filename: str, version_number: int):

    base_cols = [
        'plant','date_field','shift','shift_ic','dn_class','length','category','stage',
        'y','m','dd','mc_number','pn','batch','casting_shift','mould_no','run_no',
        'weight','visual_defect','defect_loc','defect_at_leak_point','lining',
        'entry','source_sheet'
    ]

    df_tmp = df_filtered.copy()
    df_tmp = ensure_columns(df_tmp, base_cols, fill="")

    df_tmp["original_filename"] = original_filename
    df_tmp["file_version"] = str(version_number)

    export_cols = base_cols + ["original_filename", "file_version"]

    df_tmp = coerce_text_df(df_tmp)

    with engine.begin() as conn:
        drop_existing_mc_tables()

        for mc_val, df_mc in df_tmp.groupby("mc_number", dropna=False):
            tname = sanitize_table_name(mc_val)
            create_mc_table(conn, tname, export_cols)
            rows = df_mc[export_cols].itertuples(index=False, name=None)
            bulk_insert_text(conn, tname, export_cols, rows)
            conn.execute(
                text(f"INSERT INTO {MC_REGISTRY_TABLE} (mc_value, table_name) VALUES (:mc, :tn)"),
                {"mc": str(mc_val), "tn": tname},
            )

# ===============================
# EMAIL NOTIFICATION
# ===============================
def send_upload_notification(user_email, file_info, record_count):
    try:
        recipients = [user_email]  # add team DLs if needed
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = "File Upload Notification - Rejection Report System"
        body = f"""
        Dear Team,

        A new rejection report file has been uploaded:

        📄 Original File: {file_info['original_filename']}
        📁 Stored As: {file_info['versioned_filename']}
        📊 Version: {file_info['version_number']}
        👤 Uploaded By: {user_email}
        📅 Upload Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        📈 Data Rows: {record_count}
        💾 File Size: {file_info['file_size'] / 1024:.1f} KB

        Regards,
        Rejection Report Automation System
        """
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        if EMAIL_USERNAME and EMAIL_PASSWORD:
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        st.warning(f"File uploaded, but email notification failed: {str(e)}")
        return False

def clean_header(col: str) -> str:
                if not isinstance(col, str):
                    col = str(col)

                col = col.replace("\n", " ")      # remove newlines
                col = col.replace("\r", " ")
                col = col.replace("\t", " ")
                col = re.sub(r'\s+', ' ', col)    # collapse multiple spaces
                col = col.strip()                 # remove leading/trailing spaces
                return col
# ===============================
# EXCEL PROCESSOR (DIP-1 & DIP-2 ONLY, tolerant)
# ===============================

def process_excel_file(uploaded_file, user_email):
    try:
        # ------------------------------------------------------------
        # STEP 1: Read excel first (DO NOT SAVE FILE YET)
        # ------------------------------------------------------------
        xls = pd.ExcelFile(uploaded_file)

        dip_frames = []          # DIP-1 & DIP-2
        prod_rej_frames = []     # Prod-Rej

        # ------------------------------------------------------------
        # STEP 2: Loop through sheets
        # ------------------------------------------------------------
        for sheet in ACCEPTED_SHEETS:
            
            if sheet not in xls.sheet_names:
                st.warning(f"Sheet '{sheet}' not found — skipping.")
                continue

            df = pd.read_excel(xls, sheet_name=sheet).dropna(how='all')
            if df.empty:
                continue

            df = normalize_headers(df)

            # ========================================================
            # PROCESS PROD-REJ SHEET
            # ========================================================
            if sheet == "Prod-Rej":

                df_pr = process_prod_rej_sheet(df, sheet)
                if not df_pr.empty:
                    prod_rej_frames.append(df_pr)
                    st.success("Prod-Rej sheet processed.")

                continue   # IMPORTANT: skip DIP logic

            # ========================================================
            # PROCESS DIP-1 / DIP-2
            # ========================================================

            # Convert DATE → pure date
            if "DATE" in df.columns:
                df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date

            # Mandatory column check (MC#)
            missing_keys = [c for c in REQUIRED_KEY_COLS if c not in df.columns]
            if missing_keys:
                st.error(f"Sheet '{sheet}' missing required column: {', '.join(missing_keys)}")
                return False

            # Fill missing optional columns
            df = ensure_columns(df, OPTIONAL_COLS, fill="")

            # Keep only needed DIP fields
            keep_cols = [c for c in REQUIRED_COLUMNS if c in df.columns]
            df = df[keep_cols].copy()

            # Clean text
            df = coerce_text_df(df)
            df["source_sheet"] = sheet

            # Remove garbage rows (only essential checks)
            df = df[df["DATE"].notna()]
            df = df[df["SHIFT"].astype(str).str.strip() != ""]
            df = df[df["DN/CLASS"].astype(str).str.strip() != ""]

            dip_frames.append(df)

        # ------------------------------------------------------------
        # STEP 3: Allow DIP-only, Prod-Rej-only, or mixed uploads
        # ------------------------------------------------------------
        dip_df = pd.concat(dip_frames, ignore_index=True) if dip_frames else pd.DataFrame()
        pr_df = pd.concat(prod_rej_frames, ignore_index=True) if prod_rej_frames else pd.DataFrame()

        if dip_df.empty and pr_df.empty:
            st.error("No valid sheets found in this file.")
            return False

        # ------------------------------------------------------------
        # STEP 4: Compute hash only on DIP data (NOT Prod-Rej)
        # ------------------------------------------------------------
        dip_for_hash = dip_df.copy() if not dip_df.empty else pd.DataFrame(columns=REQUIRED_COLUMNS)
        file_hash = calculate_file_hash(dip_for_hash)

        # ------------------------------------------------------------
        # STEP 5: Check duplicate BEFORE saving file
        # ------------------------------------------------------------
        q = text("""
            SELECT stored_filename, upload_timestamp
            FROM file_versions
            WHERE user_email = :user_email AND file_hash = :file_hash
            ORDER BY upload_timestamp DESC
            LIMIT 1
        """)
        with engine.connect() as conn:
            existed = conn.execute(q, {
                "user_email": user_email,
                "file_hash": file_hash
            }).fetchone()

        if existed:
            st.warning("⚠️ This file (by DIP content) seems to be a duplicate.")
            with st.form("dup_confirm"):
                go = st.form_submit_button("Upload anyway")
                cancel = st.form_submit_button("Cancel")
                if cancel:
                    return False
                if not go:
                    return False

        # ------------------------------------------------------------
        # STEP 6: SAVE FILE — EXACTLY ONCE
        # ------------------------------------------------------------
        file_info = save_excel_file(uploaded_file, user_email)
        if not file_info:
            return False

        version_number = file_info["version_number"]

        # ------------------------------------------------------------
        # STEP 7: Store DIP rows (if any)
        # ------------------------------------------------------------
        if not dip_df.empty:

            # rename to DB schema
            dip_df = dip_df.rename(columns={
                'PLANT': 'plant',
                'DATE': 'date_field',
                'SHIFT': 'shift',
                'SHIFT I/C': 'shift_ic',
                'DN/CLASS': 'dn_class',
                'LENGTH': 'length',
                'CATEGORY': 'category',
                'STAGE': 'stage',
                'Y': 'y',
                'M': 'm',
                'DD': 'dd',
                'M/C#': 'mc_number',
                'PN': 'pn',
                'BATCH': 'batch',
                'CASTING SHIFT': 'casting_shift',
                'MOULD NO': 'mould_no',
                'RUN NO': 'run_no',
                'WEIGHT': 'weight',
                'VISUAL DEFECT': 'visual_defect',
                'DEFECT LOC': 'defect_loc',
                'DEFECT AT LEAK POINT': 'defect_at_leak_point',
                'LINING': 'lining',
                'ENTRY': 'entry'
            })

            dip_df = dip_df.loc[:, ~dip_df.columns.duplicated()]

            # MC# convert to int or None
            dip_df["mc_number"] = dip_df["mc_number"].apply(
                lambda x: int(float(x)) if str(x).replace(".", "", 1).isdigit() else None
            )

            # Store DIP data
            store_excel_data_to_db(
                dip_df, user_email, uploaded_file.name, version_number
            )

            # Generate per-MC tables (only for DIP)
            recreate_mc_tables_from_df(
                df_filtered=dip_df,
                original_filename=uploaded_file.name,
                version_number=version_number
            )

        # ------------------------------------------------------------
        # STEP 8: Store PROD-REJ rows (if any)
        # ------------------------------------------------------------
        if not pr_df.empty:
            store_prod_rej_rows(
                pr_df,
                user_email=user_email,
                fname=uploaded_file.name,
                version=version_number
            )
            st.success("Prod-Rej data stored successfully.")

        # ------------------------------------------------------------
        # STEP 9: Save file version metadata
        # ------------------------------------------------------------
        record_count = len(dip_df)

        store_file_version_info(
            user_email,
            uploaded_file.name,
            file_info["versioned_filename"],
            file_info["file_path"],
            version_number,
            file_hash,
            file_info["file_size"],
            record_count
        )

        st.success("All sheets processed successfully.")
        return True

    except Exception as e:
        st.error(f"Error: {e}")
        return False

# ===============================
# UI: AUTH + TABS
# ===============================
def login_view():
    st.header("🔐 Login")
    if not st.session_state.get("otp_sent", False):
        email = st.text_input("Email Address", placeholder="Enter your @welspun.com email")
        if st.button("Send OTP"):
            if email and email.endswith("@welspun.com"):
                otp = generate_otp()
                if send_otp_email(email, otp):
                    store_otp(email, otp)
                    st.session_state.email = email
                    st.session_state.otp_sent = True
                    st.success("OTP sent successfully! Check your email.")
                    st.rerun()
                else:
                    st.error("Failed to send OTP. Please try again.")
            else:
                st.error("Please enter a valid @welspun.com email address")
    else:
        st.info(f"OTP sent to: {st.session_state.email}")
        otp_input = st.text_input("Enter OTP", max_chars=6)
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Verify OTP"):
                if verify_otp(st.session_state.email, otp_input):
                    update_user_login(st.session_state.email)
                    st.session_state.authenticated = True
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid or expired OTP")
        with col2:
            if st.button("Resend OTP"):
                otp = generate_otp()
                if send_otp_email(st.session_state.email, otp):
                    store_otp(st.session_state.email, otp)
                    st.success("OTP resent successfully!")
                else:
                    st.error("Failed to resend OTP")


def upload_tab():
    st.header("📁 Upload Rejection Report")
    st.caption("Accepted sheets: **Rejection DIP-1** and **Rejection DIP-2**")
    uploaded_file = st.file_uploader(
        "Choose Excel file",
        type=['xlsx', 'xls'],
        help="Upload Excel with the two sheets"
    )
    if uploaded_file is not None:
        st.info(f"File uploaded: {uploaded_file.name}")
        if st.button("Process File"):
            with st.spinner("Processing Excel..."):
                success = process_excel_file(uploaded_file, st.session_state.email)
                if success:
                    st.balloons()


def view_data_tab():
    st.header("📊 Rejection Data")
    df_records = get_rejection_data(st.session_state.email)

    if not df_records.empty:
        st.subheader("📈 Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(df_records))
        with col2:
            st.metric("Unique Files", df_records['original_filename'].nunique())
        with col3:
            st.metric("Defect Types", df_records['visual_defect'].nunique())
        with col4:
            latest_upload = df_records['uploaded_at'].max()
            try:
                st.metric("Latest Upload", pd.to_datetime(latest_upload).strftime('%Y-%m-%d'))
            except Exception:
                st.metric("Latest Upload", str(latest_upload))

        st.subheader("🔍 Filter Data")
        c1, c2, c3 = st.columns(3)
        with c1:
            unique_shifts = ['All'] + list(df_records['shift'].dropna().unique())
            selected_shift = st.selectbox("Filter by Shift", unique_shifts)
        with c2:
            unique_categories = ['All'] + list(df_records['category'].dropna().unique())
            selected_category = st.selectbox("Filter by Category", unique_categories)
        with c3:
            unique_defects = ['All'] + list(df_records['visual_defect'].dropna().unique())
            selected_defect = st.selectbox("Filter by Visual Defect", unique_defects)

        filtered = df_records.copy()
        if selected_shift != 'All':
            filtered = filtered[filtered['shift'] == selected_shift]
        if selected_category != 'All':
            filtered = filtered[filtered['category'] == selected_category]
        if selected_defect != 'All':
            filtered = filtered[filtered['visual_defect'] == selected_defect]

        st.subheader(f"📋 Filtered Results ({len(filtered)} records)")
        if not filtered.empty:
            show_records = st.slider("Number of records to display", 10, min(100, len(filtered)), 25)

            display_columns = [
                'date_field','shift','dn_class','category','visual_defect',
                'defect_loc','weight','batch','source_sheet','original_filename','file_version'
            ]
            for c in display_columns:
                if c not in filtered.columns:
                    filtered[c] = ""
            df_display = filtered[display_columns].head(show_records).copy()
            df_display.columns = [
                'Date','Shift','DN/Class','Category','Visual Defect',
                'Defect Location','Weight','Batch','Source Sheet','Source File','Version'
            ]
            st.dataframe(df_display, width='stretch')


            # Export
            st.subheader("📤 Export Data")
            c1, c2 = st.columns(2)
            with c1:
                csv_data = filtered.to_csv(index=False)
                st.download_button(
                    label="📊 Download as CSV",
                    data=csv_data,
                    file_name=f"rejection_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            with c2:
                excel_buffer = BytesIO()
                filtered.to_excel(excel_buffer, index=False, engine='openpyxl')
                st.download_button(
                    label="📈 Download as Excel",
                    data=excel_buffer.getvalue(),
                    file_name=f"rejection_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.info("No records found for current filters.")
    else:
        st.info("No database records found. Upload some Excel files to see data here!")


def file_versions_tab():
    st.header("📁 File Versions")
    df_versions = get_file_versions_data(st.session_state.email)

    if not df_versions.empty:
        df_versions['file_size_mb'] = (df_versions['file_size'] / 1024 / 1024).round(2)
        try:
            df_versions['upload_timestamp'] = pd.to_datetime(df_versions['upload_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            df_versions['upload_timestamp'] = df_versions['upload_timestamp'].astype(str)

        for index, row in df_versions.iterrows():
            with st.container():
                c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                with c1:
                    st.write(f"**📄 {row['original_filename']}**")
                    st.caption(f"Version {row['version_number']} • {row['upload_timestamp']}")
                with c2:
                    st.metric("Size", f"{row['file_size_mb']:.1f} MB")
                with c3:
                    st.metric("Rows", f"{int(row['record_count']):,}")
                with c4:
                    file_path = os.path.join(get_user_folder(st.session_state.email), row['stored_filename'])
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            st.download_button(
                                label="⬇️ Download",
                                data=f.read(),
                                file_name=row['stored_filename'],
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"download_{index}"
                            )
                st.divider()
    else:
        st.info("No files uploaded yet. Upload your first rejection report!")


def mc_browser_tab():
    st.header("🛠 MC Browser (Per-Machine Tables)")
    session = get_session()
    try:
        rows = session.query(MachineRegistry.table_name, MachineRegistry.mc_value).all()
    finally:
        session.close()

    if not rows:
        st.info("No per-machine tables found. Upload a file to generate them.")
        return

    items = sorted(
        [{"table": r[0], "mc_value": r[1]} for r in rows],
        key=lambda x: x["table"].lower()
    )
    st.subheader("Available MC Tables")
    st.dataframe(pd.DataFrame(items), use_container_width=True, height=240)

    names = [x["table"] for x in items]
    selected = st.selectbox("Select MC Table", names)

    limit = st.number_input("Limit", min_value=1, max_value=10000, value=200, step=50)
    offset = st.number_input("Offset", min_value=0, max_value=10_000_000, value=0, step=100)

    if st.button("Fetch MC Data"):
        try:
            with engine.connect() as conn:
                total_res = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM `{selected}`")
                ).fetchone()
                total = int(total_res.cnt if hasattr(total_res, "cnt") else total_res[0])

                data_res = conn.execute(
                    text(f"SELECT * FROM `{selected}` LIMIT :limit_val OFFSET :offset_val"),
                    {"limit_val": int(limit), "offset_val": int(offset)},
                )
                rows = [dict(row._mapping) for row in data_res.fetchall()]
                df = pd.DataFrame(rows)

            st.caption(f"Total rows: {total} • Showing: {len(df)} • Offset: {offset}")
            st.dataframe(df, use_container_width=True)

            # Export current page
            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    label="📥 Download current page (CSV)",
                    data=df.to_csv(index=False),
                    file_name=f"{selected}_page.csv",
                    mime="text/csv"
                )
            with c2:
                buf = BytesIO()
                df.to_excel(buf, index=False, engine='openpyxl')
                st.download_button(
                    label="📥 Download current page (Excel)",
                    data=buf.getvalue(),
                    file_name=f"{selected}_page.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        except Exception as e:
            st.error(f"Error reading table '{selected}': {e}")
    
def prod_rej_data_tab():
    st.header("📘 Prod-Rej Data")

    df = pd.read_sql(
        text("SELECT * FROM prod_rej_data WHERE user_email=:u"),
        engine, params={"u": st.session_state.email}
    )

    st.dataframe(df, use_container_width=True)

# ===============================
# MAIN
# ===============================
def main():
    init_database()

    # session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'email' not in st.session_state:
        st.session_state.email = ""
    if 'otp_sent' not in st.session_state:
        st.session_state.otp_sent = False

    if not st.session_state.authenticated:
        login_view()
        return

    # Header with user info + logout
    last_login = get_user_info(st.session_state.email)
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        st.write(f"**Logged in as:** {st.session_state.email}")
        if last_login:
            st.write(f"**Last login:** {last_login}")
    with c3:
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.email = ""
            st.session_state.otp_sent = False
            st.rerun()

    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📁 Upload Files",
        "📊 View Data",
        "📈 File Versions",
        "🛠 MC Browser",
        "📘 Prod-Rej Data"
    ])
    with tab1:
        upload_tab()
    with tab2:
        view_data_tab()
    with tab3:
        file_versions_tab()
    with tab4:
        mc_browser_tab()
    with tab5:
        prod_rej_data_tab()



if __name__ == "__main__":
    main()
