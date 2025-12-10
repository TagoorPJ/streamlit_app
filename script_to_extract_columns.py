import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------
# CREATE SQLALCHEMY ENGINE (outside function for reuse)
# ----------------------------------------------------
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


# ----------------------------------------------------
# FUNCTION TO GENERATE PROD_REJ SUMMARY
# ----------------------------------------------------
def generate_prod_rej_summary():
    """
    Reads prod_rej_data from MySQL, performs groupby summary,
    computes rejection percentage, fixes NaN/Infinity values,
    and writes result into prod_rej_summary table.

    Returns:
        DataFrame: Cleaned and grouped summary table.
    """

    with engine.connect() as conn:

        # ------------------------------------
        # READ EXISTING prod_rej_data TABLE
        # ------------------------------------
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

        if df.empty:
            print("No data found in prod_rej_data table.")
            return pd.DataFrame()

        # ------------------------------------
        # GROUP BY LOGIC
        # ------------------------------------
        df_grouped = (
            df.groupby(['plant', 'date_field', 'shift', 'mc_number'])
            .agg(
                Total_Cast=('cast_nos', 'sum'),
                Total_Rej=('casting_rej_nos', 'sum')
            )
            .reset_index()
        )

        # Calculate rejection %
        df_grouped["Rej%"] = (
            df_grouped["Total_Rej"] / df_grouped["Total_Cast"] * 100
        )

        # ------------------------------------
        # HANDLE division-by-zero or NaN
        # ------------------------------------
        df_grouped = df_grouped.replace([float("inf"), float("-inf")], pd.NA)
        df_grouped = df_grouped.where(pd.notnull(df_grouped), None)

        # ------------------------------------
        # WRITE TO prod_rej_summary TABLE
        # ------------------------------------
        df_grouped.to_sql(
            name="prod_rej_summary",
            con=engine,
            if_exists="replace",   # replace entire table each time
            index=False
        )

        print("prod_rej_summary table created/updated successfully!")

        return df_grouped
