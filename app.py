import streamlit as st
import duckdb
import pandas as pd
import os
import urllib.request

st.set_page_config(layout="wide")

PARQUET_FILE = "harvard_merged.parquet"

DATA_URL = "https://drive.google.com/uc?export=download&id=10vnUERLJmDgb_xFX0qKFaTcR1tkFNvHF"

# download dataset if missing
if not os.path.exists(PARQUET_FILE):
    print("Downloading dataset...")
    urllib.request.urlretrieve(DATA_URL, PARQUET_FILE)

@st.cache_resource
def get_con():
    return duckdb.connect()

con = get_con()

st.title("Harvard Donor Explorer")

DEFAULT_COLS = [
"Entity Id",
"Preferred Mail Name",
"First Name",
"Last Name",
"Latest Degree Year",
"Gender",
"Preferred Email",
"Business Email",
"Preferred Phone",
"Mobile Phone",
"City",
"State",
"Country",
"Harvard Degree Name",
"House/Section",
"UNV Rating",
"FAS Total Affinity Score",
"Summary Industries",
"Employer",
"Record Status"
]

st.sidebar.header("Search")

name = st.sidebar.text_input("Name")
entity_id = st.sidebar.text_input("Entity Id")

limit = st.sidebar.slider("Results limit", 10, 200, 50)

selected_id = None

# -----------------------------
# Default view: Top donors
# -----------------------------

if not name and not entity_id:

    st.subheader("Top Donors")

    query = f"""
    SELECT
        "Entity Id",
        ANY_VALUE("Preferred Mail Name") AS name,
        SUM(CAST("Total Giving" AS DOUBLE)) AS total_donations
    FROM parquet_scan('{PARQUET_FILE}')
    GROUP BY "Entity Id"
    ORDER BY total_donations DESC
    LIMIT {limit}
    """

    df = con.execute(query).df()

    st.dataframe(df, width="stretch")

    if len(df) > 0:
        selected_id = st.selectbox("Select donor", df["Entity Id"])

# -----------------------------
# Search mode
# -----------------------------

else:

    where = []

    if name:
        where.append(f"""
        (
        "Preferred Mail Name" ILIKE '%{name}%'
        OR "First Name" ILIKE '%{name}%'
        OR "Last Name" ILIKE '%{name}%'
        )
        """)

    if entity_id:
        where.append(f'"Entity Id" = \'{entity_id}\'')

    where_clause = ""

    if where:
        where_clause = "WHERE " + " AND ".join(where)

    query = f"""
    SELECT *
    FROM parquet_scan('{PARQUET_FILE}')
    {where_clause}
    """

    df = con.execute(query).df()

    people = df.drop_duplicates(subset="Entity Id")

    display_cols = [c for c in DEFAULT_COLS if c in people.columns]

    st.subheader("Results")

    st.dataframe(people[display_cols], width="stretch")

    if len(people) > 0:
        selected_id = st.selectbox("Select person", people["Entity Id"])

# -----------------------------
# Profile view
# -----------------------------

if selected_id:

    profile = con.execute(f"""
    SELECT *
    FROM parquet_scan('{PARQUET_FILE}')
    WHERE "Entity Id" = '{selected_id}'
    """).df()

    st.subheader("Profile")

    row = profile.iloc[0]

    profile_df = pd.DataFrame({
        "Field": row.index,
        "Value": row.values
    }).dropna()

    st.dataframe(profile_df, width="stretch")

    st.subheader("Underlying Records")

    st.dataframe(profile, width="stretch")