"""
Export EDWCustomFieldMapping + MetricMasterMapping per client into Excel.

For the first N active clients (from FLOWDIM + CLIENTDIM), this script:
  1. Runs the CustomField query for that client's FLOWKEY
  2. Runs the MetricMasterMapping query for that client's FLOWKEY
  3. Writes both results into ONE Excel file named after the client,
     as two sheets: "EDWCustomFieldMapping" and "MetricMasterMapping"
  4. If the same client name appears again (multiple flowkeys, e.g.
     several "Humana" rows), the new rows are appended to that same
     client's file/sheets instead of creating a new file.

Requires: pip install pyodbc pandas openpyxl
"""

import re
import pyodbc
import pandas as pd
from pathlib import Path

# ----------------------- CONFIG -----------------------
SERVER = r"VC03-B20-SQL09\sync"
DATABASE = "PI_EDWDDS"
TRUSTED_CONNECTION = True          # Windows auth (matches SSMS screenshot)
SQL_USERNAME = ""                  # only used if TRUSTED_CONNECTION = False
SQL_PASSWORD = ""                  # only used if TRUSTED_CONNECTION = False
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

OUTPUT_DIR = Path(r"\\cfile09.sciohealthanalytics.net\SCIOMine\Mukeshwar")
TOP_N_CLIENTS = 20
# --------------------------------------------------------

CUSTOMFIELD_QUERY = """
SELECT MP.CLIENTID, FLOWID, F.FLOWKEY, F.CLIENTKEY, M.EDWCUSTOMFIELDID,
       M.EDWCUSTOMFIELD, DATA_TYPE, CUSTOMFIELD, CUSTOMFIELDTABLE,
       ISFIELD_TWICE, IS_CALCULATEDFIELD
FROM EDWCUSTOMFIELDMASTER M
INNER JOIN EDWCUSTOMFIELDMAPPING MP ON M.EDWCUSTOMFIELDID = MP.EDWCUSTOMFIELDID
INNER JOIN FLOWDIM F ON F.CLIENTID = MP.CLIENTID AND F.FLOWID = MP.FLOW_ID
WHERE F.FLOWKEY = ?
"""

METRICMAPPING_QUERY = """
SELECT F.CLIENTKEY, M.*, MD.*
FROM METRICMASTERMAPPING M
INNER JOIN METRICMASTERDESCRIPTION MD ON M.METRICID = MD.[METRIC ID]
INNER JOIN FLOWDIM F ON F.FLOWKEY = M.FLOWKEY
WHERE F.FLOWKEY = ?
"""

ACTIVE_CLIENTS_QUERY = """
SELECT C.ClientName, F.*
FROM FLOWDIM F
INNER JOIN CLIENTDIM C ON F.CLIENTKEY = C.CLIENTKEY
WHERE F.IsActive_ScioMine = 1
"""


def get_connection():
    if TRUSTED_CONNECTION:
        conn_str = (
            f"DRIVER={{{ODBC_DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{ODBC_DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
            f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


def safe_filename(name: str) -> str:
    """Turn a client name into a safe Excel filename."""
    name = name.strip()
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return name


def write_client_excel(path: Path, custom_df: pd.DataFrame, metric_df: pd.DataFrame):
    """
    Write/append the two dataframes into the client's Excel file.
    If the file already exists (from a previous run), existing rows
    are preserved and new rows are appended below them.
    """
    if path.exists():
        existing_custom = pd.read_excel(path, sheet_name="EDWCustomFieldMapping")
        existing_metric = pd.read_excel(path, sheet_name="MetricMasterMapping")
        custom_df = pd.concat([existing_custom, custom_df], ignore_index=True)
        metric_df = pd.concat([existing_metric, metric_df], ignore_index=True)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        custom_df.to_excel(writer, sheet_name="EDWCustomFieldMapping", index=False)
        metric_df.to_excel(writer, sheet_name="MetricMasterMapping", index=False)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    # Step 1: get the active client/flow list, take the first N
    active_df = pd.read_sql(ACTIVE_CLIENTS_QUERY, conn)
    active_df = active_df.head(TOP_N_CLIENTS)

    # Accumulate results per client name within this run, so if the same
    # client appears on multiple rows (multiple flowkeys), they combine
    # into one file instead of overwriting each other.
    per_client = {}   # {client_name: {"custom": [df,...], "metric": [df,...]}}

    for _, row in active_df.iterrows():
        client_name = str(row["ClientName"])
        flowkey = row["FLOWKEY"]

        print(f"Fetching FLOWKEY={flowkey} ({client_name}) ...")

        custom_df = pd.read_sql(CUSTOMFIELD_QUERY, conn, params=[flowkey])
        metric_df = pd.read_sql(METRICMAPPING_QUERY, conn, params=[flowkey])

        per_client.setdefault(client_name, {"custom": [], "metric": []})
        per_client[client_name]["custom"].append(custom_df)
        per_client[client_name]["metric"].append(metric_df)

    conn.close()

    # Step 2: write one Excel file per client, two sheets each
    for client_name, data in per_client.items():
        combined_custom = pd.concat(data["custom"], ignore_index=True)
        combined_metric = pd.concat(data["metric"], ignore_index=True)

        out_path = OUTPUT_DIR / f"{safe_filename(client_name)}.xlsx"
        write_client_excel(out_path, combined_custom, combined_metric)
        print(f"Saved: {out_path}")

    print("Done.")


if __name__ == "__main__":
    main()
