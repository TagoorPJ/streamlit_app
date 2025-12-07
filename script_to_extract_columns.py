import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------
# CREATE SQLALCHEMY ENGINE
# ------------------------------------
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DB")

DATABASE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

engine = create_engine(DATABASE_URL)


# ------------------------------------
# MAIN LOGIC USING SQLALCHEMY ONLY
# ------------------------------------
with engine.connect() as conn:

    # Read base data from prod_rej_data table
    query = text("""
        SELECT 
            plant,
            date_field,
            shift,
            mc_number,
            cast_nos,
            casting_rej_nos
        FROM prod_rej_data;
    """)

    df = pd.read_sql(query, conn)

    # -------------------------------------
    # SAME GROUPBY LOGIC
    # -------------------------------------
    df_grouped = (
        df.groupby(['plant', 'date_field', 'shift', 'mc_number'])
          .agg(
              Total_Cast=('cast_nos', 'sum'),
              Total_Rej=('casting_rej_nos', 'sum')
          )
          .reset_index()
    )

    df_grouped["Rej%"] = (
        df_grouped["Total_Rej"] / df_grouped["Total_Cast"] * 100
    )

    print(df_grouped)

    # -------------------------------------
    # SAVE AS SQL TABLE (SQLAlchemy only)
    # -------------------------------------
    df_grouped["Rej%"] = (df_grouped["Total_Rej"] / df_grouped["Total_Cast"]) * 100

# ---- FIX division by zero results ----
    df_grouped = df_grouped.replace([float("inf"), float("-inf")], pd.NA)
    df_grouped = df_grouped.where(pd.notnull(df_grouped), None)

    print(df_grouped)

    df_grouped.to_sql(
        name="prod_rej_summary",
        con=engine,
        if_exists="replace",
        index=False
    )


    print("prod_rej_summary table created/updated successfully!")
