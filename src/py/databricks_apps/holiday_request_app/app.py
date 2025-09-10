import os
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from sqlalchemy import create_engine, event, text, inspect

app_config = Config()
workspace_client = WorkspaceClient()

postgres_username = app_config.client_id
postgres_host = os.getenv("PGHOST")
postgres_port = 5432
#postgres_database = "jsd-lakebase-demo"
postgres_database = "databricks_postgres"

postgres_pool = create_engine(f"postgresql+psycopg://{postgres_username}:@{postgres_host}:{postgres_port}/{postgres_database}",
     pool_pre_ping=True)


@event.listens_for(postgres_pool, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    """Provide the App's OAuth token. Caching is managed by WorkspaceClient"""
    cparams["password"] = workspace_client.config.oauth_token().access_token

def get_engine():
    """Return the postgres engine."""
    return postgres_pool

def get_holiday_requests():
    """Fetch all holiday requests from the database."""
    engine = get_engine()
    df = pd.read_sql_query("SELECT * FROM holidays.holiday_requests;", engine)
    return df

def update_request_status(request_id, status, comment):
    """Update the status and manager note for a specific holiday request."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE holidays.holiday_requests
                SET status = :status, manager_note = :comment
                WHERE request_id = :request_id
                """
            ),
            {"status": status, "comment": comment or "", "request_id": request_id}
        )

def get_available_schemas():
    """Get list of available schemas in the database."""
    try:
        engine = get_engine()
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        return sorted(schemas)
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

def get_tables_in_schema(schema_name):
    """Get list of tables in a specific schema."""
    try:
        engine = get_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=schema_name)
        return sorted(tables)
    except Exception as e:
        st.error(f"Error fetching tables from schema '{schema_name}': {str(e)}")
        return []

def get_table_data(schema_name, table_name, limit=1000):
    """Fetch data from a specific table."""
    try:
        engine = get_engine()
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}"
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        st.error(f"Error fetching data from table '{schema_name}.{table_name}': {str(e)}")
        return None

def get_table_info(schema_name, table_name):
    """Get column information for a specific table."""
    try:
        engine = get_engine()
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name, schema=schema_name)
        return columns
    except Exception as e:
        st.error(f"Error fetching table info for '{schema_name}.{table_name}': {str(e)}")
        return []

# Streamlit App
def main():
    st.set_page_config(
        page_title="Florida BrickCon 2025 and Offsite-holiday Shindig Request Manager",
        page_icon="🏖️🏖️🏖️",
        layout="wide"
    )
    
    st.title("Holiday Request Manager for Orlando, Florida BrickCon 2025 and Offsite-holiday Shindig")
    st.markdown("Olaf, Please review, approve, or decline holiday requests from your team.")
    
    # Initialize session state for selected request
    if "selected_request_id" not in st.session_state:
        st.session_state.selected_request_id = None
    
    # Fetch holiday requests
    try:
        df = get_holiday_requests()
        
        if df.empty:
            st.warning("No holiday requests found.")
            return
            
        # Display the holiday requests table
        st.subheader("DevRel Team Holiday Requests")
        
        # Create columns for the table display
        col_headers = ["Select ID", "Request ID", "Employee", "Start Date", "End Date", "Status", "Manager Comment"]
        
        # Create header row
        header_cols = st.columns([0.5, 1, 1.5, 1.5, 1.5, 1, 2])
        for i, header in enumerate(col_headers):
            with header_cols[i]:
                st.markdown(f"**{header}**")
        
        # Display each request as a row with individual radio buttons
        for idx, row in df.iterrows():
            cols = st.columns([0.5, 1, 1.5, 1.5, 1.5, 1, 2])
            
            with cols[0]:
                # Individual radio button for each row
                is_selected = st.session_state.selected_request_id == row['request_id']
                if st.button("⚪" if not is_selected else "🔘", 
                           key=f"radio_btn_{row['request_id']}", 
                           help="Select this request"):
                    # When clicked, update selection
                    if st.session_state.selected_request_id == row['request_id']:
                        # If already selected, deselect
                        st.session_state.selected_request_id = None
                    else:
                        # Select this request
                        st.session_state.selected_request_id = row['request_id']
                    st.rerun()
            
            with cols[1]:
                st.write(row['request_id'])
            with cols[2]:
                st.write(row.get('employee_name', 'N/A'))
            with cols[3]:
                st.write(row.get('start_date', 'N/A'))
            with cols[4]:
                st.write(row.get('end_date', 'N/A'))
            with cols[5]:
                # Color code the status
                status = row.get('status', 'Unknown')
                if status.lower() == 'pending':
                    st.markdown(f"🟡 {status}")
                elif status.lower() == 'approved':
                    st.markdown(f"🟢 {status}")
                elif status.lower() == 'declined':
                    st.markdown(f"🔴 {status}")
                else:
                    st.write(status)
            with cols[6]:
                st.write(row.get('manager_note', ''))
        
        # Action section
        st.subheader("Action")
        
        if st.session_state.selected_request_id:
            # Action radio buttons
            action_col1, action_col2 = st.columns([1, 3])
            
            with action_col1:
                action = st.radio(
                    "Choose action:",
                    ["Approve", "Decline"],
                    key="action_radio"
                )
            
            # Comment text area
            comment = st.text_area(
                "Add a comment (optional)...",
                placeholder="Add a comment (optional)...",
                height=100,
                key="comment_text"
            )
            
            # Submit button
            if st.button("Submit", type="primary"):
                try:
                    # Update the request status
                    status = action.lower() + "d"  # "approved" or "declined"
                    update_request_status(st.session_state.selected_request_id, status, comment)
                    
                    st.success(f"Request {st.session_state.selected_request_id} has been {status}!")
                    
                    # Clear selection and rerun to refresh data
                    st.session_state.selected_request_id = None
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error updating request: {str(e)}")
        else:
            st.info("Please select a request to take action on.")
            
    except Exception as e:
        st.error(f"Error loading holiday requests: {str(e)}")
        st.info("Make sure the database connection is properly configured and the holidays.holiday_requests table exists.")

if __name__ == "__main__":
    main()