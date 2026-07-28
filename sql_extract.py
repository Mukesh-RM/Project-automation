import json
import logging
import sys
import time
from pathlib import Path

import pyodbc

# ==========================================================
# CONFIGURATION (shared across all jobs -- rarely needs editing)
# ==========================================================

JOBS_CONFIG_FILE = Path("Config") / "Jobs_Config.json"
CHECKPOINT_FOLDER = Path("Checkpoints")

# Deadlock / transient error retry settings (applies to every batch)
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 3          # doubles every retry: 3, 6, 12, 24, 48
DEADLOCK_SQLSTATE = "40001"
DEADLOCK_ERROR_CODE = "1205"

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ==========================================================
# CONFIG LOADING
# ==========================================================

def load_jobs_config(jobs_config_file):
    """
    Reads Jobs_Config.json. This is the ONLY place you add a new
    source-table -> destination-table job. Add a new object to the
    "jobs" array, no code changes needed.
    """
    if not jobs_config_file.exists():
        raise FileNotFoundError(f"Jobs config file not found: {jobs_config_file}")

    with open(jobs_config_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "jobs" not in data or not isinstance(data["jobs"], list):
        raise ValueError("Jobs_Config.json must contain a top-level 'jobs' array.")

    return data["jobs"]


def find_job(jobs, job_name):
    for job in jobs:
        if job.get("job_name", "").strip().lower() == job_name.strip().lower():
            return job

    available = ", ".join(j.get("job_name", "?") for j in jobs)
    raise ValueError(f"Job '{job_name}' not found. Available jobs: {available}")


def validate_job(job):
    required_top = {"job_name", "source", "destination", "column_config_file",
                     "load_mode", "batch_size", "batch_key_column"}
    missing = required_top - set(job.keys())
    if missing:
        raise ValueError(f"Job '{job.get('job_name')}' is missing keys: {missing}")

    for side in ("source", "destination"):
        required_side = {"server", "database", "table"}
        missing_side = required_side - set(job[side].keys())
        if missing_side:
            raise ValueError(f"Job '{job['job_name']}' -> '{side}' missing keys: {missing_side}")

    valid_modes = ("append", "truncate_and_load", "create_if_missing", "drop_and_recreate")
    if job["load_mode"] not in valid_modes:
        raise ValueError(
            f"Job '{job['job_name']}' has invalid load_mode '{job['load_mode']}'. "
            f"Must be one of: {', '.join(valid_modes)}."
        )


def load_column_config(column_config_file):
    """
    Reads the per-job column-mapping / filter JSON file.
    Structure:
    {
      "columns": [ {"source_column": "...", "output_column": "..."}, ... ],
      "filters": [ {"column": "...", "operator": "...", "value": ...}, ... ],
      "filter_logic": "AND" | "OR"
    }
    """
    config_file = Path("Config") / column_config_file
    if not config_file.exists():
        raise FileNotFoundError(f"Column config file not found: {config_file}")

    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    if "columns" not in config or not config["columns"]:
        raise ValueError(f"{config_file} must define at least one column in 'columns'.")

    for col in config["columns"]:
        if "source_column" not in col or "output_column" not in col:
            raise ValueError(
                f"{config_file}: each column entry needs 'source_column' and 'output_column'."
            )

    config.setdefault("filters", [])
    config.setdefault("filter_logic", "AND")

    if config["filter_logic"].upper() not in ("AND", "OR"):
        raise ValueError(f"{config_file}: filter_logic must be 'AND' or 'OR'.")

    return config

# ==========================================================
# CONNECTION HELPERS
# ==========================================================

def get_connection(server, database, autocommit=False):
    conn_str = (
        "DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=Yes;"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str, timeout=120, autocommit=autocommit)

# ==========================================================
# WHERE CLAUSE / FILTER BUILDING
# (parameterized -- never string-formats values directly into SQL)
# ==========================================================

SUPPORTED_OPERATORS = {
    "=", "!=", "<", "<=", ">", ">=", "LIKE", "IN", "NOT IN",
    "BETWEEN", "IS NULL", "IS NOT NULL"
}


def build_filter_clause(filters, filter_logic):
    """
    Converts the JSON 'filters' list into a parameterized SQL fragment
    and a matching list of parameter values. Returns ("", []) if no
    filters are defined.
    """
    if not filters:
        return "", []

    clauses = []
    params = []

    for f in filters:
        column = f.get("column")
        operator = f.get("operator", "").upper()
        value = f.get("value")

        if not column:
            raise ValueError(f"Filter is missing 'column': {f}")
        if operator not in SUPPORTED_OPERATORS:
            raise ValueError(
                f"Unsupported filter operator '{operator}' on column '{column}'. "
                f"Supported: {sorted(SUPPORTED_OPERATORS)}"
            )

        if operator in ("IS NULL", "IS NOT NULL"):
            clauses.append(f"[{column}] {operator}")

        elif operator == "BETWEEN":
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError(f"BETWEEN filter on '{column}' needs value: [low, high]")
            clauses.append(f"[{column}] BETWEEN ? AND ?")
            params.extend(value)

        elif operator in ("IN", "NOT IN"):
            if not isinstance(value, list) or not value:
                raise ValueError(f"{operator} filter on '{column}' needs a non-empty list value")
            placeholders = ", ".join(["?"] * len(value))
            clauses.append(f"[{column}] {operator} ({placeholders})")
            params.extend(value)

        else:
            clauses.append(f"[{column}] {operator} ?")
            params.append(value)

    joiner = f" {filter_logic.upper()} "
    return joiner.join(clauses), params

# ==========================================================
# CHECKPOINTING (lets a huge job resume after a crash/restart)
# ==========================================================

def checkpoint_path(job_name):
    CHECKPOINT_FOLDER.mkdir(parents=True, exist_ok=True)
    safe_name = job_name.strip().lower().replace(" ", "_")
    return CHECKPOINT_FOLDER / f"{safe_name}.checkpoint"


def read_checkpoint(job_name):
    path = checkpoint_path(job_name)
    if path.exists():
        value = path.read_text(encoding="utf-8").strip()
        logging.info(f"Resuming job '{job_name}' from checkpoint key: {value}")
        return value
    return None


def write_checkpoint(job_name, last_key_value):
    checkpoint_path(job_name).write_text(str(last_key_value), encoding="utf-8")


def clear_checkpoint(job_name):
    path = checkpoint_path(job_name)
    if path.exists():
        path.unlink()

# ==========================================================
# DESTINATION TABLE CREATION (matches source column types)
# ==========================================================

def get_source_column_definitions(source_cursor, schema, table, wanted_columns):
    """
    Reads the real SQL types of the wanted columns from the source
    table's INFORMATION_SCHEMA, so the destination table is created
    with matching types instead of a blanket NVARCHAR(MAX) dump.
    """
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    source_cursor.execute(query, schema, table)
    rows = {r.COLUMN_NAME.lower(): r for r in source_cursor.fetchall()}

    definitions = {}
    for source_col, output_col in wanted_columns:
        info = rows.get(source_col.lower())
        if not info:
            raise ValueError(f"Source column '{source_col}' not found in {schema}.{table}")

        dtype = info.DATA_TYPE.lower()

        if dtype in ("varchar", "nvarchar", "char", "nchar"):
            length = info.CHARACTER_MAXIMUM_LENGTH
            length_sql = "MAX" if (length is None or length == -1) else str(length)
            definitions[output_col] = f"{dtype.upper()}({length_sql})"
        elif dtype in ("decimal", "numeric"):
            definitions[output_col] = f"{dtype.upper()}({info.NUMERIC_PRECISION},{info.NUMERIC_SCALE})"
        else:
            # int, bigint, datetime, datetime2, bit, float, date, etc.
            # carry over as-is -- these don't need length/precision.
            definitions[output_col] = dtype.upper()

    return definitions


def get_destination_columns(dest_cursor, schema, table):
    query = """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    dest_cursor.execute(query, schema, table)
    return {r.COLUMN_NAME.lower() for r in dest_cursor.fetchall()}


def drop_destination_table_if_exists(dest_cursor, job):
    schema, table = job["destination"]["table"].split(".")
    check_sql = """
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    dest_cursor.execute(check_sql, schema, table)
    if dest_cursor.fetchone():
        dest_cursor.execute(f"DROP TABLE {job['destination']['table']}")
        logging.info(
            f"Dropped existing destination table {job['destination']['table']} "
            f"(load_mode=drop_and_recreate)."
        )


def ensure_destination_table(dest_cursor, source_cursor, job, wanted_columns):
    schema, table = job["destination"]["table"].split(".")
    src_schema, src_table = job["source"]["table"].split(".")
    output_cols = [o for _, o in wanted_columns]

    check_sql = """
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    dest_cursor.execute(check_sql, schema, table)
    exists = dest_cursor.fetchone()

    if exists:
        # CRITICAL: don't just reuse the table blindly. If someone edited
        # the column config after the table was first created (e.g. cut
        # it down from 7 columns to 3), the OLD table still has the old
        # columns and old leftover data sitting in it. Catch that here
        # instead of silently loading into a mismatched table.
        existing_cols = get_destination_columns(dest_cursor, schema, table)
        expected_cols = {c.lower() for c in output_cols}

        if existing_cols != expected_cols:
            extra = existing_cols - expected_cols
            missing = expected_cols - existing_cols
            raise RuntimeError(
                f"Destination table {job['destination']['table']} already exists but its "
                f"columns don't match your current column_config_file.\n"
                f"  Columns in table but NOT in your config : {sorted(extra) or 'none'}\n"
                f"  Columns in your config but NOT in table : {sorted(missing) or 'none'}\n"
                f"This usually happens when the column config was changed after the table "
                f"was first created. Fix by either:\n"
                f"  1) Setting load_mode to 'drop_and_recreate' to rebuild the table exactly "
                f"     matching your current config (this deletes existing data in it), or\n"
                f"  2) Pointing 'destination.table' at a new table name."
            )

        logging.info(f"Destination table {job['destination']['table']} already exists and matches config.")
        return

    if job["load_mode"] == "append":
        raise RuntimeError(
            f"Destination table {job['destination']['table']} does not exist, "
            f"but load_mode is 'append'. Set load_mode to 'create_if_missing' "
            f"to auto-create it, or create the table manually first."
        )

    definitions = get_source_column_definitions(source_cursor, src_schema, src_table, wanted_columns)
    columns_sql = ", ".join(f"[{col}] {definitions[col]}" for _, col in wanted_columns)
    create_sql = f"CREATE TABLE {job['destination']['table']} ({columns_sql})"

    dest_cursor.execute(create_sql)
    logging.info(f"Destination table {job['destination']['table']} created.")


def truncate_destination_table(dest_cursor, job):
    logging.info(f"Truncating destination table {job['destination']['table']} (load_mode=truncate_and_load).")
    dest_cursor.execute(f"TRUNCATE TABLE {job['destination']['table']}")

# ==========================================================
# RETRY HELPER (deadlocks / transient SQL errors)
# ==========================================================

def execute_with_retry(cursor, sql, params, batch_description):
    attempt = 0
    while True:
        try:
            if isinstance(params, list) and params and isinstance(params[0], (list, tuple)):
                cursor.executemany(sql, params)
            else:
                cursor.execute(sql, params)
            return
        except pyodbc.Error as e:
            sqlstate = e.args[0] if e.args else ""
            message = str(e)
            is_deadlock = (sqlstate == DEADLOCK_SQLSTATE) or (DEADLOCK_ERROR_CODE in message)

            attempt += 1
            if not is_deadlock or attempt > MAX_RETRIES:
                logging.error(f"{batch_description} failed permanently: {e}")
                raise

            wait_seconds = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logging.warning(
                f"{batch_description}: deadlock detected (attempt {attempt}/{MAX_RETRIES}). "
                f"Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)

# ==========================================================
# CORE BATCH EXTRACT + LOAD LOOP
# ==========================================================

def run_job(job_name):
    jobs = load_jobs_config(JOBS_CONFIG_FILE)
    job = find_job(jobs, job_name)
    validate_job(job)

    column_config = load_column_config(job["column_config_file"])
    wanted_columns = [(c["source_column"], c["output_column"]) for c in column_config["columns"]]
    filter_sql, filter_params = build_filter_clause(column_config["filters"], column_config["filter_logic"])

    batch_size = int(job["batch_size"])
    key_col = job["batch_key_column"]
    batch_delay = float(job.get("batch_delay_seconds", 0))
    resume = bool(job.get("resume", True))

    source_select_cols = ", ".join(f"[{s}]" for s, _ in wanted_columns)
    output_cols = [o for _, o in wanted_columns]

    logging.info(f"Starting job '{job_name}': {job['source']['table']} -> {job['destination']['table']}")

    with get_connection(job["source"]["server"], job["source"]["database"]) as source_conn, \
         get_connection(job["destination"]["server"], job["destination"]["database"]) as dest_conn:

        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        dest_cursor.fast_executemany = True

        if job["load_mode"] == "drop_and_recreate":
            drop_destination_table_if_exists(dest_cursor, job)
            dest_conn.commit()
            clear_checkpoint(job_name)  # a fresh table means the old checkpoint is meaningless

        ensure_destination_table(dest_cursor, source_cursor, job, wanted_columns)
        dest_conn.commit()

        if job["load_mode"] == "truncate_and_load":
            truncate_destination_table(dest_cursor, job)
            dest_conn.commit()
            clear_checkpoint(job_name)  # a fresh load means the old checkpoint is meaningless

        last_key = read_checkpoint(job_name) if resume else None

        insert_cols_sql = ", ".join(f"[{c}]" for c in output_cols)
        insert_placeholders = ", ".join(["?"] * len(output_cols))
        insert_sql = f"INSERT INTO {job['destination']['table']} ({insert_cols_sql}) VALUES ({insert_placeholders})"

        total_rows = 0
        batch_number = 0

        while True:
            batch_number += 1

            where_parts = [f"[{key_col}] > ?"] if last_key is not None else []
            batch_params = [last_key] if last_key is not None else []

            if filter_sql:
                where_parts.append(f"({filter_sql})")
                batch_params.extend(filter_params)

            where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

            select_sql = f"""
                SELECT TOP ({batch_size}) {source_select_cols}
                FROM {job['source']['table']} WITH (NOLOCK)
                {where_clause}
                ORDER BY [{key_col}] ASC
            """

            source_cursor.execute(select_sql, batch_params)
            rows = source_cursor.fetchall()

            if not rows:
                break

            insert_rows = [tuple(row) for row in rows]

            execute_with_retry(
                dest_cursor, insert_sql, insert_rows,
                f"Job '{job_name}' batch #{batch_number}"
            )
            dest_conn.commit()

            key_index = output_cols.index(
                next(o for s, o in wanted_columns if s.lower() == key_col.lower())
            )
            last_key = insert_rows[-1][key_index]
            write_checkpoint(job_name, last_key)

            total_rows += len(rows)
            logging.info(
                f"Job '{job_name}': batch #{batch_number} loaded ({len(rows)} rows, "
                f"{total_rows} total, checkpoint key={last_key})"
            )

            if len(rows) < batch_size:
                break  # last batch was partial -> no more data

            if batch_delay > 0:
                time.sleep(batch_delay)

        clear_checkpoint(job_name)  # job finished cleanly -- next run starts fresh
        logging.info(f"Job '{job_name}' complete. Total rows loaded: {total_rows}")

    return total_rows

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    try:
        if len(sys.argv) < 2:
            raise ValueError(
                "Please provide a job name to run.\n"
                "Usage: python sql_extract.py <job_name>\n"
                "Example: python sql_extract.py ticket_extract\n"
                "(Job names are listed in Config/Jobs_Config.json)"
            )

        job_name_arg = sys.argv[1]
        rows_loaded = run_job(job_name_arg)

        print("\n====================================")
        print("PROCESS COMPLETED SUCCESSFULLY")
        print(f"Job Name    : {job_name_arg}")
        print(f"Rows Loaded : {rows_loaded}")
        print("====================================\n")

    except Exception as error:
        print("\n====================================")
        print("PROCESS FAILED")
        print(error)
        print("====================================\n")
        print("Note: if this job was mid-run, a checkpoint file was saved in")
        print("the Checkpoints folder. Simply re-run the same command and it")
        print("will resume from where it stopped instead of starting over.")
