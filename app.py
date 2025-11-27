import streamlit as st
import duckdb
import os

# ------------------------------
# CONFIG: hardcoded S3 DB file
# ------------------------------
S3_DB_PATH = "s3://duckdb-s3-database/leads.db"   # <-- CHANGE THIS

# Initialize session state
if 'connection' not in st.session_state:
    st.session_state.connection = None
if 'db_analyzed' not in st.session_state:
    st.session_state.db_analyzed = False
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# ------------------------------
# LOGIN SCREEN
# ------------------------------
if not st.session_state.authenticated:
    st.title("Login")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    if st.button("Login"):
        # Get credentials from environment variables
        valid_username = os.getenv("APP_USERNAME")
        valid_password = os.getenv("APP_PASSWORD")
        
        if not valid_username or not valid_password:
            st.error("Server configuration error: Credentials not set. Please contact administrator.")
        elif username == valid_username and password == valid_password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Invalid username or password")
    
    st.stop()

st.title("Internal use only")

# ------------------------------
# SIDEBAR: S3 AUTH
# ------------------------------
st.sidebar.header("AWS Credentials")
aws_access = st.sidebar.text_input("AWS Access Key", type="password")
aws_secret = st.sidebar.text_input("AWS Secret Key", type="password")
aws_region = st.sidebar.text_input("AWS Region", value="us-east-1")

# ------------------------------
# FILTERS
# ------------------------------
st.subheader("Filters")

country = st.selectbox("Country", ["United States", "Canada"])

sector = st.text_input("Sector")
industry = st.text_input("Industry")

name = st.text_input("Company Name")
contact = st.text_input("Contact Name")

limit = st.number_input("Row Limit", value=500, min_value=5, max_value=50000)

# ------------------------------
# QUERY FUNCTION
# ------------------------------
def build_sql():
    sql = """SELECT 
        name,
        email,
        company_location_text,
        company_name,
        company_website,
        linkedin_url,
        company_sector
    FROM leads.leads WHERE 1=1"""

    if country:
        sql += f" AND company_location_text ILIKE '%{country}%'"

    if sector.strip():
        sql += f" AND company_sector ILIKE '%{sector.strip()}%'"

    if industry.strip():
        sql += f" AND company_industry ILIKE '%{industry.strip()}%'"

    if name.strip():
        sql += f" AND company_name ILIKE '%{name.strip()}%'"

    if contact.strip():
        sql += f" AND (name ILIKE '%{contact.strip()}%' OR email ILIKE '%{contact.strip()}%')"

    sql += f" LIMIT {limit}"

    return sql


# ------------------------------
# EXECUTE QUERY
# ------------------------------
if st.button("Run Query"):

    #if not aws_access or not aws_secret:
    #    st.error("AWS credentials are required.")
    #    st.stop()

    # Check if connection already exists
    if st.session_state.connection is None:
        st.write("Connecting to DuckDB in S3…")

        con = duckdb.connect()

        # Enable S3 support
        con.execute("""
            INSTALL httpfs;
            LOAD httpfs;
        """)

        #con.execute(f"""
        #    SET s3_region='{aws_region}';
        #    SET s3_access_key_id='{aws_access}';
        #    SET s3_secret_access_key='{aws_secret}';
        #    SET s3_use_ssl=true;
        #    SET s3_url_style='path';
        #""")

        # Attach the remote DB
        try:
            con.execute(f"ATTACH '{S3_DB_PATH}' (READ_ONLY);")

            # Save connection to session state
            st.session_state.connection = con
            st.session_state.db_analyzed = True
            st.success("Database connected and analyzed!")
            
        except Exception as e:
            st.error(f"Unable to attach S3 DuckDB file: {e}")
            st.stop()
    else:
        st.write("Using existing connection...")
        con = st.session_state.connection

    sql = build_sql()

    st.code(sql, language="sql")

    st.write("Running query…")

    try:
        df = con.execute(sql).fetchdf()
        st.success(f"Returned {len(df)} rows")
        st.dataframe(df, hide_index=True)
    except Exception as e:
        st.error(f"Query failed: {e}")
