import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# DB Connection
# -----------------------------
def get_connection():
    return mysql.connector.connect(
        host = os.getenv("MYSQL_HOST", "localhost"),
        port = int(os.getenv("MYSQL_PORT", 3306)),
        user = os.getenv("MYSQL_USER", "root"),
        password = os.getenv("MYSQL_PASSWORD", ""),
        database = os.getenv("MYSQL_DATABASE", "rejection_reports")
    )

# -----------------------------
# MAIN LOGIC
# -----------------------------
try:
    conn = get_connection()

    query = """
        SELECT 
            plant,
            date_field,
            shift,
            mc_number,
            cast_nos,
            casting_rej_nos
        FROM prod_rej_data;
    """

    df = pd.read_sql(query, conn)

    # -------------------------------------
    # SAME GROUPBY LOGIC AS EXCEL SCRIPT
    # -------------------------------------
    df_grouped = df.groupby(
        ['plant', 'date_field', 'shift', 'mc_number']
    ).agg(
        Total_Cast=('cast_nos', 'sum'),
        Total_Rej=('casting_rej_nos', 'sum')
    ).reset_index()

    # Compute rejection percentage
    df_grouped['Rej%'] = (df_grouped['Total_Rej'] / df_grouped['Total_Cast']) * 100

    print(df_grouped)

except Error as e:
    print("Error:", e)

finally:
    if conn.is_connected():
        conn.close()
