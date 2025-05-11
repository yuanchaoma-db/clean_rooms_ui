import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service import jobs
from databricks import sql
import json
from code_editor import code_editor
from streamlit_ace import st_ace

# --- App-wide clients & config ---
cfg = Config()
w = WorkspaceClient()

st.set_page_config(layout="wide")

# --- Global CSS styling ---
st.markdown("""
    <style>
    /* ── App background ───────────────────────────────────────────────── */
    [data-testid="stAppViewContainer"] {
        background-color: #f5f7fa;
        padding: 1rem 2rem;
    }

    /* ── Titles & headers ────────────────────────────────────────────── */
    h1, h2 {
        font-family: 'Source Sans Pro', sans-serif;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    h1 {
        font-size: 2.5rem;
    }
    h2 {
        font-size: 1.75rem;
    }

    /* ── Body text ──────────────────────────────────────────────────── */
    .stText, .stMarkdown {
        font-family: 'Inter', sans-serif;
        color: #374151;
    }

    /* ── Narrow selectboxes ─────────────────────────────────────────── */
    div.stSelectbox > div[data-baseweb="select"] {
        max-width: 250px !important;
    }

    /* ── Button styling ──────────────────────────────────────────────── */
    button[kind="primary"], button.css-18ni7ap {
        background-color: #2563eb !important;
        color: #ffffff !important;
        border-radius: 0.5rem !important;
        padding: 0.6rem 1.2rem !important;
        font-weight: 600;
    }
    button[kind="secondary"], button.css-1v0mbdj {
        background-color: #e5e7eb !important;
        color: #374151 !important;
        border-radius: 0.5rem !important;
        padding: 0.5rem 1rem !important;
    }

    /* ── Divider spacing ────────────────────────────────────────────── */
    .stDivider {
        margin-top: 2rem;
        margin-bottom: 1rem;
    }

    /* ── DataFrame full-width & styling ─────────────────────────────── */
    [data-testid="stDataFrame"] {
        width: 100% !important;
    }
    [data-testid="stDataFrame"] th {
        background-color: #1f78b4 !important;
        color: white !important;
        padding: 0.75rem !important;
        text-align: left !important;
    }
    [data-testid="stDataFrame"] td {
        padding: 0.6rem 0.75rem !important;
    }
    [data-testid="stDataFrame"] tbody tr:nth-child(odd) {
        background-color: #f7fbff;
    }
    [data-testid="stDataFrame"] tbody tr:hover {
        background-color: #e3f2fd;
    }
    </style>
""", unsafe_allow_html=True)


# --- Session state for simple page routing & storage ---
if "page" not in st.session_state:
    st.session_state.page = "landing"
if "clean_room" not in st.session_state:
    st.session_state.clean_room = None
if "selection" not in st.session_state:
    st.session_state.selection = None

def landing_page():
    st.title("Welcome to Your Clean Room Portal")
    st.write("Choose one of the Clean Rooms you have access to:")
    try:
        clean_rooms = list(w.clean_rooms.list())
        room_names = [cr.name for cr in clean_rooms if cr.name]
    except Exception as e:
        st.error(f"Unable to load clean rooms: {e}")
        room_names = []

    selected = st.selectbox("Clean room:", [""] + room_names)
    if st.button("Next"):
        if selected:
            st.session_state.clean_room = selected
            st.session_state.page = "main"
            st.rerun()
        else:
            st.warning("Please select a clean room to continue.")

def main_page():
    st.write(f"**Clean Room:** {st.session_state.clean_room}")
    st.header("TV Viewership Data Portal")

    # ——— query scope dropdown ———
    dropdown_options = ["All", "NBCU"]
    st.session_state.selection = st.selectbox(
        "Choose the Scope of Your Dataset to Query",
        dropdown_options
    )

    # ——— SQL editor pane ———
    col1, col2 = st.columns(2)
    with col1:
        st.write("SQL Editor:")
        code = st_ace(
            language="sql",
            theme="monokai",
            height=300,
            wrap=True,
            auto_update=True,
            key="ace-editor"
        )
    with col2:
        st.write("Data Inputs:")
        # … your inputs …

    # ——— Databricks job invocation ———
    clean_room_name = "comcast_cleanroom_do_not_delete"
    notebook_name = "KAnon_Demo_Query_Notebook"
    etag = "55f069d3cfc9506734515f64271021a674b7a749cfd760cc628d2dae693e8fc5"
    http_path_input = '/sql/1.0/warehouses/c2dfc2e4e98e142c'

    @st.cache_resource
    def get_connection(http_path):
        return sql.connect(
            server_hostname=cfg.host,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate,
        )

    def read_table(table_name, conn):
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetchall_arrow().to_pandas()

    def run_as_sp():
        return w.jobs.submit_and_wait(
            run_name="App-Triggered Cleanroom Job",
            tasks=[
                jobs.SubmitTask(
                    task_key="cleanroomapp",
                    clean_rooms_notebook_task=jobs.CleanRoomsNotebookTask(
                        clean_room_name=clean_room_name,
                        notebook_name=notebook_name,
                        etag=etag,
                        notebook_base_parameters={
                            'sql': code,
                            'Dataset_Scope': st.session_state.selection,
                        }
                    )
                )
            ]
        )

    def display_output_tables(run: jobs.Run):
        output = w.jobs.get_run_output(run.tasks[0].run_id).clean_rooms_notebook_output.output_schema_info
        conn = get_connection(http_path_input)
        df = read_table(
            f"`{output.catalog_name}`.`{output.schema_name}`.cr_output_table",
            conn
        )
        st.dataframe(df)

    if st.button("Run Query"):
        try:
            st.write("Running the query.. please wait.")
            resp = run_as_sp()
            st.success("Run successful!")
            st.write(f"[View details]({resp.tasks[0].run_page_url})")
            display_output_tables(resp)
        except Exception as err:
            st.error(f"Error during run: {err}")

    # ——— bottom-right Back button ———
    st.divider()
    _, back_col = st.columns([9, 1])
    with back_col:
        if st.button("Back"):
            st.session_state.page = "landing"
            st.rerun()

# ——— dispatch between pages ———
if st.session_state.page == "landing":
    landing_page()
else:
    main_page()
