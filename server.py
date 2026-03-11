import duckdb
import pandas as pd
from fastapi import FastAPI

app = FastAPI()

DATA_URL = "https://huggingface.co/datasets/dhruvtkpatel1/parquetfile/resolve/main/harvard_merged.parquet"

con = duckdb.connect()

SELECT_COLUMNS = """
"Entity Id",
"Preferred Mail Name",
"Institutional Suffix",
"Name Sort",
"Prefix",
"First Name",
"Middle Name",
"Last Name",
"Former Last Name",
"Harvard Degree Name",
"Maiden Name",
"Marital Status",
"Salutation",
"NickName",
"Gender",
"Relationships",
"Harvard Children",
"Harvard Relationships",
"Age",
"Birth Date",
"Birth Place",
"Ethnicity",
"LinkedIn URL",
"Specialties",
"Pref Email Address",
"All Active Email Addresses",
"Additional Email",
"Preferred City",
"Preferred State",
"Preferred Zip",
"Preferred Phone",
"Cell Phone",
"Company Name 1",
"Job Title",
"Summary Industries",
"Parent Industries",
"Industries",
"UNV Rating",
"University Lifetime Recognition",
"University Household Giving",
"House/Section",
"Freshman Dorm",
"Most Recent Graduation Date",
"COL Degree Info"
"""


@app.get("/search")
def search(name: str = "", limit: int = 100):

    query = f"""
    SELECT
    {SELECT_COLUMNS}
    FROM parquet_scan('{DATA_URL}')
    WHERE
        "Preferred Mail Name" ILIKE ?
        OR "First Name" ILIKE ?
        OR "Last Name" ILIKE ?
    LIMIT ?
    """

    df = con.execute(
        query,
        [f"%{name}%", f"%{name}%", f"%{name}%", limit]
    ).df()

    return df.to_dict(orient="records")


@app.get("/profile")
def profile(entity_id: str):

    query = f"""
    SELECT *
    FROM parquet_scan('{DATA_URL}')
    WHERE "Entity Id" = ?
    """

    df = con.execute(query, [entity_id]).df()

    return df.to_dict(orient="records")