import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()
# -------------------------------
# 1. MySQL Connection Settings
# -------------------------------
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DB")

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# -------------------------------
# 2. Excel File Paths
# -------------------------------
FILE_1 = r"C:\Users\01688\Downloads\DIP1_Rejection 1.xlsx"
FILE_2 = r"C:\Users\01688\Downloads\DIP2_rejection_june_july 2.xlsx"

# -------------------------------
# 3. Corresponding MySQL Tables
# -------------------------------
TABLE_1 = "Rejection_DIP-1"
TABLE_2 = "Rejection_DIP-2"

# -------------------------------
# 4. Function to Load Excel → MySQL
# -------------------------------
def load_excel_to_mysql(file_path, table_name):
    print(f"Loading {file_path} into {table_name} ...")

    df = pd.read_excel(file_path)

    # Clean column names (MySQL cannot take spaces or special chars)
    df.columns = (
        df.columns.str.strip()
                  .str.replace(" ", "_")
                  .str.replace("-", "_")
                  .str.replace("/", "_")
    )

    # Write to MySQL
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",   # change to "append" if needed
        index=False
    )

    print(f"✔ Successfully loaded into table: {table_name}")


# -------------------------------
# 5. Run For Both Excel Files
# -------------------------------
load_excel_to_mysql(FILE_1, TABLE_1)
load_excel_to_mysql(FILE_2, TABLE_2)

print("All Excel files successfully uploaded to MySQL.")
