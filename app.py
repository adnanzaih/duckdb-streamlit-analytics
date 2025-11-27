import streamlit as st
import duckdb

# ------------------------------
# CONFIG: hardcoded S3 DB file
# ------------------------------
S3_DB_PATH = "s3://duckdb-s3-database/leads.db"   # <-- CHANGE THIS

st.title("S3 DuckDB Query App")

st.write("Query the large 5GB DuckDB database hosted in S3 with optional filters.")

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
    sql = "SELECT * FROM leads.leads WHERE 1=1"

    if country:
        sql += f" AND country = '{country}'"

    if company_size:
        sql += f" AND company_size = '{company_size}'"

    if sector.strip():
        sql += f" AND sector ILIKE '%{sector.strip()}%'"

    if industry.strip():
        sql += f" AND industry ILIKE '%{industry.strip()}%'"

    if revenue_min > 0:
        sql += f" AND revenue >= {revenue_min}"

    if revenue_max > 0:
        sql += f" AND revenue <= {revenue_max}"

    sql += f" LIMIT {limit}"

    return sql


# ------------------------------
# EXECUTE QUERY
# ------------------------------
if st.button("Run Query"):

    #if not aws_access or not aws_secret:
    #    st.error("AWS credentials are required.")
    #    st.stop()

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
    except Exception as e:
        st.error(f"Unable to attach S3 DuckDB file: {e}")
        st.stop()

    sql = build_sql()

    st.code(sql, language="sql")

    st.write("Running query…")

    try:
        df = con.execute(sql).fetchdf()
        st.success(f"Returned {len(df)} rows")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Query failed: {e}")
