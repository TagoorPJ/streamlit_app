import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
import hashlib
import numpy as np
import os
from dotenv import load_dotenv

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DB")

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine)

# -------------------------------
# Import your existing models
# -------------------------------
from streamlit_app import RejectionDIP1, RejectionDIP2  # adjust path if needed

# -------------------------------
# File paths
# -------------------------------
FILE_DIP1 = r"C:\Users\01688\Downloads\DIP1_Rejection 1.xlsx"
FILE_DIP2 = r"C:\Users\01688\Downloads\DIP2_rejection_june_july 2.xlsx"


# -------------------------------
# Column mapping
# -------------------------------
COLUMN_MAP = {
    "PLANT": "plant",
    "DATE": "date_field",
    "SHIFT": "shift",
    "SHIFT I/C": "shift_ic",
    "DN/CLASS": "dn_class",
    "LENGTH": "length",
    "CATEGORY": "category",
    "STAGE": "stage",
    "Y": "y",
    "M": "m",
    "DD": "dd",
    "M/C#": "mc_number",
    "PN": "pn",
    "BATCH": "batch",
    "PIPE NO": "pipe_no",
    "MOULD NO": "mould_no",
    "WEIGHT": "weight",
    "VISUAL DEFECT": "visual_defect",
    "DEFECT LOC": "defect_loc",
    "DEFECT AT LEAK POINT": "defect_at_leak_point",
    "ENTRY": "entry",
}


# -------------------------------
# NaN Cleaner
# -------------------------------
def clean_nan(val):
    if val is None:
        return None
    if isinstance(val, float) and (np.isnan(val)):
        return None
    if isinstance(val, str) and val.strip().lower() in ["", "nan", "none", "null"]:
        return None
    return val


# -------------------------------
# Date Parser
# -------------------------------
def parse_date(val):
    if pd.isna(val) or val is None:
        return None

    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val

    try:
        return pd.to_datetime(val).date()
    except:
        pass

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except:
            pass

    print(f"⚠️ WARNING: Could not parse date '{val}', storing NULL")
    return None


# -------------------------------
# Row Hash Function (your function)
# -------------------------------
def compute_row_hash(row):
    fields = [
        'plant', 'date_field', 'shift', 'shift_ic', 'dn_class', 'length',
        'category', 'stage', 'y', 'm', 'dd', 'mc_number', 'pn', 'batch',
        'pipe_no', 'mould_no', 'weight', 'visual_defect', 'defect_loc',
        'defect_at_leak_point'
    ]

    concat = "|".join(str(row.get(f, "")).strip() for f in fields)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


# -------------------------------
# Insert into DB
# -------------------------------
def insert_into_table(df, model, fname):
    session = SessionLocal()
    inserted = 0

    try:
        for _, row in df.iterrows():
            data = {}

            # Apply column mapping and data cleaning
            for excel_col, db_col in COLUMN_MAP.items():
                if excel_col in df.columns:
                    value = row.get(excel_col)
                    if db_col == "date_field":
                        data[db_col] = parse_date(value)
                    else:
                        data[db_col] = clean_nan(value)

            # Compute row hash AFTER cleaning
            data["row_hash"] = compute_row_hash(data)

            record = model(
                user_email="manual_import@welspun.com",
                original_filename=fname,
                file_version=1,
                **data,
            )

            session.add(record)
            inserted += 1

        session.commit()
        print(f"✔ Inserted {inserted} rows into {model.__tablename__}")

    finally:
        session.close()


# -------------------------------
# Load DIP File
# -------------------------------
def load_dip_file(file_path, model):
    print(f"\n📥 Loading file: {file_path}")

    df = pd.read_excel(file_path)
    insert_into_table(df, model, os.path.basename(file_path))


# -------------------------------
# Run imports
# -------------------------------
load_dip_file(FILE_DIP1, RejectionDIP1)
load_dip_file(FILE_DIP2, RejectionDIP2)

print("\n🎉 ALL DIP FILES IMPORTED SUCCESSFULLY!")
