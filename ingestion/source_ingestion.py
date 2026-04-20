"""CSV and API source ingestion helpers for raw schema loads."""

import csv
import json
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from urllib.parse import urlparse


def to_sql_identifier(name):
    """Convert arbitrary text into a Snowflake-friendly identifier token."""
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    cleaned = cleaned.strip("_")
    return (cleaned or "CSV_TABLE").upper()


def infer_csv_data_type(values):
    """Infer a simple Snowflake data type from sampled CSV values."""
    non_empty = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not non_empty:
        return "VARCHAR"

    lower_values = [v.lower() for v in non_empty]
    if all(v in {"true", "false", "0", "1", "yes", "no", "y", "n"} for v in lower_values):
        return "BOOLEAN"

    try:
        for v in non_empty:
            int(v)
        return "NUMBER"
    except ValueError:
        pass

    try:
        for v in non_empty:
            float(v)
        return "FLOAT"
    except ValueError:
        pass

    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"]
    for fmt in date_formats:
        try:
            for v in non_empty:
                datetime.strptime(v, fmt)
            return "DATE"
        except ValueError:
            continue

    datetime_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
    ]
    for fmt in datetime_formats:
        try:
            for v in non_empty:
                datetime.strptime(v, fmt)
            return "TIMESTAMP_NTZ"
        except ValueError:
            continue

    return "VARCHAR"


def extract_csv_schema(uploaded_file, sample_rows=200, custom_table_name=None):
    """Return target raw table name, inferred columns, and normalized records from CSV file."""
    if not uploaded_file.name.lower().endswith(".csv"):
        raise ValueError("Only .csv files are supported")

    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        raise ValueError("CSV file is empty")

    content = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(StringIO(content))

    if not reader.fieldnames:
        raise ValueError("CSV file must include a header row")

    if any((name is None) or (str(name).strip() == "") for name in reader.fieldnames):
        raise ValueError("CSV header contains empty column names")

    normalized_columns = [to_sql_identifier(col) for col in reader.fieldnames]
    duplicate_columns = sorted({
        col for col in normalized_columns if normalized_columns.count(col) > 1
    })
    if duplicate_columns:
        raise ValueError(
            "CSV contains duplicate column names after normalization: "
            + ", ".join(duplicate_columns)
        )

    samples_by_col = {col: [] for col in normalized_columns}
    has_data_rows = False
    normalized_records = []

    for idx, row in enumerate(reader):
        has_data_rows = True
        normalized_row = {}
        for original_col, normalized_col in zip(reader.fieldnames, normalized_columns):
            value = row.get(original_col, "")
            normalized_row[normalized_col] = value
            if idx < sample_rows:
                samples_by_col[normalized_col].append(value)
        normalized_records.append(normalized_row)

    if not has_data_rows:
        raise ValueError("CSV must contain at least one data row")

    columns = [
        {
            "column_name": col,
            "data_type": infer_csv_data_type(samples_by_col[col]),
        }
        for col in normalized_columns
    ]

    if not columns:
        raise ValueError("CSV schema inference failed: no columns detected")

    preferred_name = (custom_table_name or "").strip()
    if preferred_name:
        table_base_name = to_sql_identifier(preferred_name)
    else:
        table_base_name = to_sql_identifier(Path(uploaded_file.name).stem)

    table_name = f"CSV_{table_base_name}"
    return table_name, columns, normalized_records


def normalize_api_records(payload):
    """Normalize common API JSON payloads into a list of record dicts."""
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return payload
        raise ValueError("API response list must contain JSON objects")

    if isinstance(payload, dict):
        list_candidates = [
            value for value in payload.values()
            if isinstance(value, list) and all(isinstance(item, dict) for item in value)
        ]
        if list_candidates:
            return list_candidates[0]
        return [payload]

    raise ValueError("Unsupported API response format. Expected JSON object or list of objects.")


def infer_api_table_name(api_url):
    """Infer Snowflake raw table name from API URL path."""
    parsed = urlparse(api_url)
    path_name = Path(parsed.path or "").name
    base_name = to_sql_identifier(Path(path_name).stem or "API_SOURCE")
    return f"API_{base_name}"


def build_api_ingestion_script(api_url, selected_database):
    """Build preview Python script for API fetch and Snowflake raw ingestion using pandas."""
    target_table = infer_api_table_name(api_url)
    database_name = selected_database or "<DATABASE>"

    return f'''import json
import pandas as pd
from urllib.request import Request, urlopen
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas


def fetch_api_payload(api_url, api_token):
    """Fetch JSON data from API with Bearer token authentication."""
    request = Request(api_url)
    request.add_header("Authorization", f"Bearer {{api_token}}")
    request.add_header("Accept", "application/json")
    with urlopen(request, timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset, errors="replace")
    return json.loads(body)


# Runtime configuration
API_URL = "{api_url}"
API_TOKEN = "<API_TOKEN>"
TARGET_DATABASE = "{database_name}"
TARGET_SCHEMA = "AI_ETL_RAW"
TARGET_TABLE = "{target_table}"

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = "<ACCOUNT>"
SNOWFLAKE_USER = "<USER>"
SNOWFLAKE_PASSWORD = "<PASSWORD>"
SNOWFLAKE_WAREHOUSE = "<WAREHOUSE>"

# Fetch API data
payload = fetch_api_payload(API_URL, API_TOKEN)

# Convert payload to pandas DataFrame
# Adjust logic based on your API response structure
if isinstance(payload, list):
    df = pd.DataFrame(payload)
elif isinstance(payload, dict):
    # If API returns nested structure, extract the data array
    # Example: if payload = {{"data": [{{...}}, {{...}}]}}, use payload["data"]
    df = pd.DataFrame([payload])
else:
    raise ValueError("Unexpected API response format")

# Connect to Snowflake
conn = connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE
)

# Ingest DataFrame into Snowflake raw schema
# auto_create_table=True creates table if it doesn't exist
# overwrite=True replaces existing table
success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    TARGET_TABLE,
    database=TARGET_DATABASE,
    schema=TARGET_SCHEMA,
    auto_create_table=True,
    overwrite=True
)

conn.close()

print(f"Successfully ingested {{nrows}} rows into {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.{{TARGET_TABLE}}")
'''


def infer_api_columns(records):
    """Infer table columns and SQL types from normalized API records."""
    all_keys = []
    for record in records:
        for key in record.keys():
            normalized = to_sql_identifier(key)
            if normalized not in all_keys:
                all_keys.append(normalized)

    if not all_keys:
        raise ValueError("API response records do not contain columns")

    samples_by_col = {col: [] for col in all_keys}
    for record in records[:200]:
        normalized_record = {
            to_sql_identifier(k): v for k, v in record.items()
        }
        for col in all_keys:
            raw_val = normalized_record.get(col)
            if raw_val is None:
                samples_by_col[col].append("")
            elif isinstance(raw_val, (dict, list)):
                samples_by_col[col].append(json.dumps(raw_val, ensure_ascii=True))
            else:
                samples_by_col[col].append(str(raw_val))

    columns = []
    for col in all_keys:
        inferred = infer_csv_data_type(samples_by_col[col])
        if inferred in {"DATE", "TIMESTAMP_NTZ"}:
            inferred = "VARCHAR"
        columns.append({"column_name": col, "data_type": inferred})

    return columns


def cast_value_for_sql(raw_val, data_type):
    """Cast API value into Python value compatible with Snowflake bindings."""
    if raw_val is None:
        return None

    if isinstance(raw_val, (dict, list)):
        raw_val = json.dumps(raw_val, ensure_ascii=True)

    if data_type == "BOOLEAN":
        val = str(raw_val).strip().lower()
        if val in {"true", "1", "yes", "y"}:
            return True
        if val in {"false", "0", "no", "n"}:
            return False
        return None

    if data_type == "NUMBER":
        try:
            return int(str(raw_val).strip())
        except Exception:
            return None

    if data_type == "FLOAT":
        try:
            return float(str(raw_val).strip())
        except Exception:
            return None

    return str(raw_val)


def ingest_records_to_raw(connection, database_name, table_name, columns, records):
    """Create/replace AI_ETL_RAW table and ingest records into Snowflake."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"USE DATABASE {database_name}")
        cursor.execute(f"USE SCHEMA {database_name}.AI_ETL_RAW")

        column_defs = ", ".join(
            f"{col['column_name']} {col['data_type']}" for col in columns
        )
        cursor.execute(f"CREATE OR REPLACE TABLE {database_name}.AI_ETL_RAW.{table_name} ({column_defs})")

        column_names = [col["column_name"] for col in columns]
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_sql = (
            f"INSERT INTO {database_name}.AI_ETL_RAW.{table_name} "
            f"({', '.join(column_names)}) VALUES ({placeholders})"
        )

        rows = []
        for record in records:
            normalized_record = {to_sql_identifier(k): v for k, v in record.items()}
            row = []
            for col in columns:
                row.append(cast_value_for_sql(normalized_record.get(col["column_name"]), col["data_type"]))
            rows.append(tuple(row))

        if rows:
            cursor.executemany(insert_sql, rows)
    finally:
        cursor.close()
