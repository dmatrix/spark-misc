import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, event, text
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient

cfg = Config()
workspace_client = WorkspaceClient()

PGHOST = os.getenv("PGHOST")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = "databricks_postgres"
# PGDATABASE = "jsd-lakebase-demo"
PGUSER = cfg.client_id

workspace_token_present = bool(workspace_client.config.oauth_token().access_token and len(workspace_client.config.oauth_token().access_token) > 1)
st.title("Lakebase Demo: Holiday Requests")

# Debug prints
st.write("PGUSER:", PGUSER)
st.write("PGHOST:", PGHOST)
st.write("PGPORT:", PGPORT)
st.write("PGDATABASE:", PGDATABASE)
st.write("Workspace token present:", workspace_token_present)


engine = create_engine(
    f"postgresql+psycopg://{PGUSER}:@{PGHOST}:{PGPORT}/{PGDATABASE}", 
                pool_pre_ping=True)


def get_holiday_requests():
    df = pd.read_sql_query("SELECT * FROM holidays.holiday_requests;", engine)
    return df

@event.listens_for(engine, "do_connect")
def inject_token(dialect, conn_rec, cargs, cparams):
    st.write("Injecting Workspace token", workspace_client.config.oauth_token().access_token)
    cparams["password"] = workspace_client.config.oauth_token().access_token

st.dataframe(get_holiday_requests())