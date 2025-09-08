import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, event, text
from databricks.sdk.core import Config

cfg = Config()

PGHOST = os.getenv("PGHOST")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = "jsd-lakebase-demo"
PGUSER = cfg.client_id

token_present = bool(cfg.token and len(cfg.token) > 10)
st.title("Lakebase Demo: Holiday Requests")

# Debug prints
st.write("PGUSER:", PGUSER)
st.write("PGHOST:", PGHOST)
st.write("PGPORT:", PGPORT)
st.write("PGDATABASE:", PGDATABASE)
st.write("Token present:", token_present)

# engine = create_engine(
#     f"postgresql+psycopg2://{PGUSER}:@{PGHOST}:{PGPORT}/{PGDATABASE}",
#     pool_pre_ping=True,
# )

# @event.listens_for(engine, "do_connect")
# def inject_token(dialect, conn_rec, cargs, cparams):
#     cparams["password"] = cfg.token

# st.title("Lakebase Demo: Holiday Requests")

# try:
#     with engine.begin() as conn:
#         df = pd.read_sql_query(
#             text("SELECT * FROM holidays.holiday_requests LIMIT 10;"),
#             conn,
#         )
#     st.dataframe(df)
# except Exception as e:
    # st.error(f"Error querying Lakebase: {e}")