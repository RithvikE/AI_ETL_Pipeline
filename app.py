"""Streamlit entry point"""

import streamlit as st
import re
import time
import csv
import json
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from io import StringIO
from datetime import datetime
from pathlib import Path
from config.settings import get_ollama_config
from snowflake_client.connection import get_connection
from snowflake_client.validator import validate_tables
from snowflake_client.schema_fetcher import fetch_schemas, list_tables
from llm.prompt_builder import build_prompt
from llm.ollama_client import generate, list_ollama_models
from generation.sql_generator import extract_sql
from execution.sql_executor import execute_sql_statements


def fetch_databases(connection):
    """Fetch list of databases from Snowflake."""
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        results = cursor.fetchall()
        return [row[1] for row in results]
    finally:
        cursor.close()


def fetch_schemas_for_db(connection, database):
    """Fetch list of schemas for a specific database."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        results = cursor.fetchall()
        return [row[1] for row in results]
    finally:
        cursor.close()


def fetch_tables_for_db_schema(connection, database, schema):
    """Fetch table list for a specific database and schema."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {database}.{schema}")
        return list_tables(connection)
    finally:
        cursor.close()


def _normalize_model_name(model_name):
    """Normalize Ollama model name for reliable matching."""
    return (model_name or "").strip().lower()


def _candidate_model_names(model_name):
    """Return acceptable aliases for a configured model name."""
    normalized = _normalize_model_name(model_name)
    if not normalized:
        return set()

    candidates = {normalized}
    if ":" in normalized:
        candidates.add(normalized.split(":", 1)[0])
    else:
        candidates.add(f"{normalized}:latest")

    return candidates


def _group_tables_by_db_schema(qualified_tables):
    """Group fully-qualified table names by (database, schema)."""
    grouped = {}
    for qualified_name in qualified_tables:
        parts = qualified_name.split(".", 2)
        if len(parts) != 3:
            continue

        database, schema, table = parts[0], parts[1], parts[2]
        key = (database, schema)
        grouped.setdefault(key, []).append(table)

    return grouped


def _build_sql_chat_prompt(user_message, staging_sql, transform_sql, business_sql):
    """Build chatbot prompt with generated SQL context."""
    return f"""You are an SQL assistant helping users understand generated Snowflake ETL SQL.

Use the SQL context below to answer the user question clearly and accurately.

STAGING SQL:
{staging_sql}

TRANSFORM SQL:
{transform_sql}

BUSINESS SQL:
{business_sql}

USER QUESTION:
{user_message}

Answer directly and concisely based on the SQL above.
"""


def _build_validation_prompt(requirement, table_schemas, staging_sql, transform_sql, business_sql):
    """Build prompt for LLM-based SQL validation scoring."""
    schema_lines = []
    for table_name, columns in table_schemas.items():
        cols = ", ".join(f"{col['column_name']} ({col['data_type']})" for col in columns)
        schema_lines.append(f"- {table_name}: {cols}")

    schema_text = "\n".join(schema_lines)

    return f"""You are a strict SQL validator for Snowflake ETL.

Validate the generated SQL against the user requirement and source schemas.

You MUST evaluate:
1) Requirement coverage accuracy
2) Relevant columns coverage
3) KPI/metric coverage
4) Layer dependency correctness (STG -> WI -> BR)
5) SQL completeness (expected CREATE/MERGE/INSERT patterns)

Return ONLY valid JSON (no markdown, no extra text) using this exact schema:
{{
  "score": <integer 0-100>,
  "summary": "<short summary>",
  "checks": [
    {{"name": "Requirement Coverage", "status": "PASS|PARTIAL|FAIL", "details": "..."}},
    {{"name": "Relevant Columns Coverage", "status": "PASS|PARTIAL|FAIL", "details": "..."}},
    {{"name": "KPI Coverage", "status": "PASS|PARTIAL|FAIL", "details": "..."}},
    {{"name": "Layer Dependency Integrity", "status": "PASS|PARTIAL|FAIL", "details": "..."}},
    {{"name": "SQL Completeness", "status": "PASS|PARTIAL|FAIL", "details": "..."}}
  ],
  "missing_items": ["..."]
}}

USER REQUIREMENT:
{requirement}

SOURCE SCHEMAS:
{schema_text}

STAGING SQL:
{staging_sql}

TRANSFORM SQL:
{transform_sql}

BUSINESS SQL:
{business_sql}
"""


def _parse_validation_response(raw_text):
    """Parse validation JSON from LLM response with fallback."""
    try:
        parsed = json.loads(raw_text)
    except Exception:
        match = re.search(r"\{[\s\S]*\}", raw_text or "")
        if not match:
            return {
                "score": 0,
                "summary": "Validation parser could not read structured response.",
                "checks": [{
                    "name": "Validation Output Format",
                    "status": "FAIL",
                    "details": "LLM did not return parseable JSON format.",
                }],
                "missing_items": [],
            }
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return {
                "score": 0,
                "summary": "Validation parser could not read structured response.",
                "checks": [{
                    "name": "Validation Output Format",
                    "status": "FAIL",
                    "details": "LLM did not return parseable JSON format.",
                }],
                "missing_items": [],
            }

    score = parsed.get("score", 0)
    try:
        score = int(score)
    except Exception:
        score = 0
    score = max(0, min(100, score))

    checks = parsed.get("checks") if isinstance(parsed.get("checks"), list) else []
    normalized_checks = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        normalized_checks.append({
            "name": str(check.get("name", "Unnamed Check")),
            "status": str(check.get("status", "PARTIAL")).upper(),
            "details": str(check.get("details", "")),
        })

    missing_items = parsed.get("missing_items") if isinstance(parsed.get("missing_items"), list) else []
    missing_items = [str(item) for item in missing_items]

    return {
        "score": score,
        "summary": str(parsed.get("summary", "Validation complete.")),
        "checks": normalized_checks,
        "missing_items": missing_items,
    }


def _to_sql_identifier(name):
    """Convert arbitrary text into a Snowflake-friendly identifier token."""
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    cleaned = cleaned.strip("_")
    return (cleaned or "CSV_TABLE").upper()


def _infer_csv_data_type(values):
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


def _extract_csv_schema(uploaded_file, sample_rows=200, custom_table_name=None):
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

    normalized_columns = [_to_sql_identifier(col) for col in reader.fieldnames]
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
            "data_type": _infer_csv_data_type(samples_by_col[col]),
        }
        for col in normalized_columns
    ]

    if not columns:
        raise ValueError("CSV schema inference failed: no columns detected")

    preferred_name = (custom_table_name or "").strip()
    if preferred_name:
        table_base_name = _to_sql_identifier(preferred_name)
    else:
        table_base_name = _to_sql_identifier(Path(uploaded_file.name).stem)

    table_name = f"CSV_{table_base_name}"
    return table_name, columns, normalized_records


def _normalize_api_records(payload):
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


def _infer_api_table_name(api_url):
    """Infer Snowflake raw table name from API URL path."""
    parsed = urlparse(api_url)
    path_name = Path(parsed.path or "").name
    base_name = _to_sql_identifier(Path(path_name).stem or "API_SOURCE")
    return f"API_{base_name}"


def _build_api_ingestion_script(api_url, selected_database):
    """Build preview Python script for API fetch and Snowflake raw ingestion."""
    target_table = _infer_api_table_name(api_url)
    database_name = selected_database or "<DATABASE>"

    return f'''import json
from urllib.request import Request, urlopen
from snowflake.connector import connect


def fetch_api_payload(api_url, api_token):
    request = Request(api_url)
    request.add_header("Authorization", f"Bearer {{api_token}}")
    request.add_header("Accept", "application/json")
    with urlopen(request, timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset, errors="replace")
    return json.loads(body)


# Replace with your runtime values
API_URL = "{api_url}"
API_TOKEN = "<API_TOKEN>"
TARGET_DATABASE = "{database_name}"
TARGET_SCHEMA = "AI_ETL_RAW"
TARGET_TABLE = "{target_table}"

payload = fetch_api_payload(API_URL, API_TOKEN)

# Normalize payload records, infer columns/types, create table,
# and insert rows into: TARGET_DATABASE.AI_ETL_RAW.TARGET_TABLE
# (This app executes this logic when you click 'Fetch and Ingest API Source')
'''


def _infer_api_columns(records):
    """Infer table columns and SQL types from normalized API records."""
    all_keys = []
    for record in records:
        for key in record.keys():
            normalized = _to_sql_identifier(key)
            if normalized not in all_keys:
                all_keys.append(normalized)

    if not all_keys:
        raise ValueError("API response records do not contain columns")

    samples_by_col = {col: [] for col in all_keys}
    for record in records[:200]:
        normalized_record = {
            _to_sql_identifier(k): v for k, v in record.items()
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
        inferred = _infer_csv_data_type(samples_by_col[col])
        if inferred in {"DATE", "TIMESTAMP_NTZ"}:
            inferred = "VARCHAR"
        columns.append({"column_name": col, "data_type": inferred})

    return columns


def _cast_value_for_sql(raw_val, data_type):
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


def _ingest_api_to_raw(connection, database_name, table_name, columns, records):
    """Create/replace AI_ETL_RAW table and ingest API records into Snowflake."""
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
            normalized_record = {_to_sql_identifier(k): v for k, v in record.items()}
            row = []
            for col in columns:
                row.append(_cast_value_for_sql(normalized_record.get(col["column_name"]), col["data_type"]))
            rows.append(tuple(row))

        if rows:
            cursor.executemany(insert_sql, rows)
    finally:
        cursor.close()


def main():
    st.set_page_config(page_title="AI ETL Pipeline", layout="wide")
    
    st.title("AI ETL Pipeline")
    st.write("Enter your details")

    st.subheader("Database Connection")
    selected_db_connection = st.selectbox(
        "Select Connection Type:",
        options=[
            "Snowflake",
            "PostgreSQL (Prototype)",
            "BigQuery (Prototype)",
            "Redshift (Prototype)",
            "Databricks (Prototype)",
        ],
        key="db_connection_type"
    )

    if selected_db_connection != "Snowflake":
        st.info(
            f"{selected_db_connection} is currently a prototype option. "
            "Please select Snowflake to use the working workflow."
        )
        return
    
    # Snowflake Connection Form
    st.subheader("Snowflake Connection")
    
    # Show success message if already connected
    if st.session_state.get("snowflake_connected"):
        st.success("✅ Connected to Snowflake successfully!")
    
    # Collapsible form - expanded when not connected, collapsed when connected
    with st.expander("Enter Snowflake Credentials", expanded=not st.session_state.get("snowflake_connected", False)):
        col1, col2 = st.columns(2)
        
        with col1:
            sf_account = st.text_input(
                "Account *",
                key="sf_account",
                placeholder="your-account.snowflakecomputing.com"
            )
            sf_user = st.text_input(
                "User *",
                key="sf_user",
                placeholder="username"
            )
            sf_password = st.text_input(
                "Password *",
                type="password",
                key="sf_password",
                placeholder="password",
                autocomplete="new-password"
            )
            sf_warehouse = st.text_input(
                "Warehouse *",
                key="sf_warehouse",
                placeholder="COMPUTE_WH"
            )
        
        with col2:
            sf_role = st.text_input(
                "Role (optional)",
                key="sf_role",
                placeholder="ACCOUNTADMIN"
            )
            sf_database = st.text_input(
                "Database (optional)",
                key="sf_database",
                placeholder="MY_DATABASE"
            )
            sf_schema = st.text_input(
                "Schema (optional)",
                key="sf_schema",
                placeholder="PUBLIC"
            )
        
        st.caption("* Required fields")
        
        # Connect button
        if st.button("Connect to Snowflake", type="primary"):
            # Validate required fields
            if not sf_account or not sf_user or not sf_password or not sf_warehouse:
                st.error("Please fill in all required fields (Account, User, Password, Warehouse)")
            else:
                try:
                    # Build config
                    snowflake_config = {
                        "account": sf_account,
                        "user": sf_user,
                        "password": sf_password,
                        "warehouse": sf_warehouse,
                        "role": sf_role or None,
                        "database": sf_database or None,
                        "schema": sf_schema or None
                    }
                    
                    # Test connection
                    test_connection = get_connection(snowflake_config)
                    test_connection.close()
                    
                    # Store config in session state
                    st.session_state.snowflake_config = snowflake_config
                    st.session_state.snowflake_connected = True
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Failed to connect to Snowflake: {str(e)}")
                    st.session_state.snowflake_connected = False
    
    st.divider()
    
    # Block UI until connection is established
    if not st.session_state.get("snowflake_connected"):
        st.info("ℹ️ Please connect to Snowflake to continue.")
        return
    
    # ETL Requirements input
    st.subheader("ETL Requirements")

    llm_provider = st.selectbox(
        "LLM Model:",
        options=["ollama", "openai", "Claude Opus 4.6", "Claude Sonnet 4.6", "Claude Haiku 4.5"],
        key="llm_provider"
    )

    ollama_model_ready = True
    if llm_provider == "ollama":
        try:
            ollama_model = get_ollama_config()["model"].strip()
            st.caption(f"Ollama model being used: {ollama_model}")

            installed_models, model_error = list_ollama_models()
            if model_error:
                ollama_model_ready = False
                st.error(model_error)
            else:
                normalized_installed = {
                    _normalize_model_name(model) for model in installed_models
                }
                configured_candidates = _candidate_model_names(ollama_model)

                if configured_candidates.intersection(normalized_installed):
                    st.success(f"✅ Required Ollama model is available: {ollama_model}")
                else:
                    ollama_model_ready = False
                    st.warning(f"Required Ollama model not found: {ollama_model}")
        except Exception as e:
            ollama_model_ready = False
            st.error(f"Ollama model configuration error: {str(e)}")

    st.session_state.ollama_model_ready = ollama_model_ready

    requirement = st.text_area(
        "Describe your Requirements:",
        height=200,
        placeholder="Provide your details here"
    )
    
    st.divider()
    
    # Establish connection and handle database/schema selection
    connection = None
    available_tables = []
    selected_database = None
    selected_schema = None
    selected_tables = []
    
    st.subheader("Source Tables")
    
    try:
        # Use stored connection config
        connection = get_connection(st.session_state.snowflake_config)
        
        # Fetch and display databases
        if "databases_list" not in st.session_state:
            st.session_state.databases_list = fetch_databases(connection)

        if "selected_source_tables" not in st.session_state:
            st.session_state.selected_source_tables = []
        if "csv_ingested_tables" not in st.session_state:
            st.session_state.csv_ingested_tables = {}
        if "selected_database" not in st.session_state and st.session_state.databases_list:
            st.session_state.selected_database = st.session_state.databases_list[0]

        enable_csv_source = st.checkbox("Upload CSV file", key="enable_csv_source")
        enable_api_source = st.checkbox("API source", key="enable_api_source")

        if enable_csv_source:
            uploaded_csv = st.file_uploader("Upload source CSV file", type=["csv"], key="source_csv_file")
            if uploaded_csv is not None:
                custom_csv_table_name = st.text_input(
                    "Optional: Enter table name for CSV in AI_ETL_RAW",
                    key="csv_custom_table_name",
                    placeholder="Leave blank to auto-generate from file name"
                )

                if st.button("Ingest CSV Source", type="secondary"):
                    try:
                        selected_database_for_ingest = st.session_state.get("selected_database")
                        if not selected_database_for_ingest:
                            raise ValueError("Please select Database")

                        csv_table_name, csv_columns, csv_records = _extract_csv_schema(
                            uploaded_csv,
                            custom_table_name=custom_csv_table_name,
                        )
                        qualified_csv_table = f"{selected_database_for_ingest}.AI_ETL_RAW.{csv_table_name}"

                        if (
                            qualified_csv_table in st.session_state.selected_source_tables
                            and qualified_csv_table not in st.session_state.csv_ingested_tables
                        ):
                            raise ValueError(
                                f"Source table name collision: {qualified_csv_table} already exists in selected Snowflake sources"
                            )

                        if qualified_csv_table in st.session_state.csv_ingested_tables:
                            st.info(f"CSV source already added: {qualified_csv_table}")
                        else:
                            _ingest_api_to_raw(
                                connection=connection,
                                database_name=selected_database_for_ingest,
                                table_name=csv_table_name,
                                columns=csv_columns,
                                records=csv_records,
                            )
                            if qualified_csv_table not in st.session_state.selected_source_tables:
                                st.session_state.selected_source_tables.append(qualified_csv_table)
                            st.session_state.csv_ingested_tables[qualified_csv_table] = True
                            st.success(f"CSV source ingested successfully into {qualified_csv_table}")

                        st.success("CSV validation checks passed. File is good to proceed.")
                        st.caption("Validation checks completed: .csv extension, non-empty file, header row present, no empty/duplicate normalized column names, at least one data row, schema detected, and no source-name collision.")

                        st.caption(f"Detected source table name: {qualified_csv_table}")
                        st.caption(f"Detected columns: {len(csv_columns)}")
                        st.caption(f"Ingested rows: {len(csv_records)}")
                    except Exception as e:
                        st.error(f"Failed to process CSV source: {str(e)}")

        if enable_api_source:
            api_url = st.text_input("API URL", key="api_source_url", placeholder="https://api.example.com/data")
            api_token = st.text_input("API Token", key="api_source_token", type="password")

            if "api_script_last_signature" not in st.session_state:
                st.session_state.api_script_last_signature = ""
            if "api_script_collapsed" not in st.session_state:
                st.session_state.api_script_collapsed = False

            api_signature = f"{api_url.strip()}|{len(api_token.strip())}"
            if api_signature != st.session_state.api_script_last_signature:
                st.session_state.api_script_last_signature = api_signature
                st.session_state.api_script_collapsed = False

            if api_url.strip() and api_token.strip():
                preview_script = _build_api_ingestion_script(
                    api_url=api_url.strip(),
                    selected_database=st.session_state.get("selected_database")
                )

                with st.expander(
                    "API Ingestion Python Script (Preview)",
                    expanded=not st.session_state.api_script_collapsed,
                ):
                    st.caption("Review this script before executing API ingestion.")
                    st.code(preview_script, language="python")

            if st.button("Fetch and Ingest API Source", type="secondary"):
                try:
                    st.session_state.api_script_collapsed = True

                    selected_database_for_ingest = st.session_state.get("selected_database")
                    if not selected_database_for_ingest:
                        raise ValueError("Please select Database")

                    if not api_url or not api_url.strip():
                        raise ValueError("Please provide API URL")
                    if not api_token or not api_token.strip():
                        raise ValueError("Please provide API token")

                    request = Request(api_url.strip())
                    request.add_header("Authorization", f"Bearer {api_token.strip()}")
                    request.add_header("Accept", "application/json")

                    with urlopen(request, timeout=60) as response:
                        charset = response.headers.get_content_charset() or "utf-8"
                        body = response.read().decode(charset, errors="replace")

                    payload = json.loads(body)
                    records = _normalize_api_records(payload)
                    if not records:
                        raise ValueError("API returned no records")

                    api_table_name = _infer_api_table_name(api_url.strip())
                    columns = _infer_api_columns(records)

                    _ingest_api_to_raw(
                        connection=connection,
                        database_name=selected_database_for_ingest,
                        table_name=api_table_name,
                        columns=columns,
                        records=records,
                    )

                    qualified_api_table = f"{selected_database_for_ingest}.AI_ETL_RAW.{api_table_name}"
                    if qualified_api_table not in st.session_state.selected_source_tables:
                        st.session_state.selected_source_tables.append(qualified_api_table)

                    st.success(f"API source ingested successfully into {qualified_api_table}")
                    st.caption(f"Fetched rows: {len(records)}")
                    st.caption(f"Detected columns: {len(columns)}")
                except Exception as e:
                    st.error(f"Failed to ingest API source: {str(e)}")

        selected_database = st.selectbox(
            "Select Database:",
            options=st.session_state.databases_list,
            key="selected_database"
        )

        # Fetch and display schemas for selected database
        if selected_database:
            # Fetch schemas for the selected database
            if "schemas_list" not in st.session_state or \
               st.session_state.get("previous_database") != selected_database:
                cursor = connection.cursor()
                try:
                    cursor.execute(f"USE DATABASE {selected_database}")
                    st.session_state.schemas_list = fetch_schemas_for_db(connection, selected_database)
                    st.session_state.previous_database = selected_database
                finally:
                    cursor.close()

            selected_schema = st.selectbox(
                "Select Schema:",
                options=st.session_state.schemas_list,
                key="selected_schema"
            )

            # Fetch tables from selected schema
            if selected_schema:
                cursor = connection.cursor()
                try:
                    cursor.execute(f"USE SCHEMA {selected_database}.{selected_schema}")
                    available_tables = list_tables(connection)
                finally:
                    cursor.close()
            
    except Exception as e:
        st.error(f"Database/schema operation failed: {str(e)}")
        return

    # Source Tables input - multiselect from available tables
    browse_selected_tables = st.multiselect(
        "Select source tables from current database/schema:",
        options=available_tables,
        placeholder="Choose one or more tables",
        key=f"browse_tables_{selected_database}_{selected_schema}"
    )

    if st.button("Add Selected Source Tables", type="secondary"):
        if not browse_selected_tables:
            st.warning("Please select at least one table from current database/schema to add")
        else:
            newly_added = 0
            for table_name in browse_selected_tables:
                qualified_table = f"{selected_database}.{selected_schema}.{table_name}"
                if qualified_table not in st.session_state.selected_source_tables:
                    st.session_state.selected_source_tables.append(qualified_table)
                    newly_added += 1

            if newly_added > 0:
                st.success(f"Added {newly_added} table(s)")
            else:
                st.info("Selected table(s) are already added")

    selected_tables = st.multiselect(
        "Selected source tables (across databases/schemas):",
        options=st.session_state.selected_source_tables,
        default=st.session_state.selected_source_tables,
        placeholder="Added tables appear here"
    )
    st.session_state.selected_source_tables = selected_tables
    st.session_state.csv_ingested_tables = {
        table_name: True
        for table_name, is_csv in st.session_state.csv_ingested_tables.items()
        if is_csv and table_name in selected_tables
    }
    
    # Target Tables input
    st.subheader("Target Tables")

    target_tables_mode = st.radio(
        "Target table option:",
        options=["Create New Target Tables", "Select Existing Target Tables"],
        key="target_tables_mode"
    )

    transform_table_name = None
    business_table_name = None

    if target_tables_mode == "Select Existing Target Tables":
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Transform Target Table**")
            transform_target_db = st.selectbox(
                "Transform Target Database:",
                options=st.session_state.databases_list,
                key="transform_target_db"
            )

            transform_target_schemas = fetch_schemas_for_db(connection, transform_target_db)
            transform_target_schema = st.selectbox(
                "Transform Target Schema:",
                options=transform_target_schemas,
                key="transform_target_schema"
            )

            transform_target_tables = fetch_tables_for_db_schema(
                connection,
                transform_target_db,
                transform_target_schema
            )
            if transform_target_tables:
                transform_table_name = st.selectbox(
                    "Transform Target Table:",
                    options=transform_target_tables,
                    key="transform_target_table"
                )
            else:
                st.warning("No tables found in selected Transform target database/schema")

        with col2:
            st.markdown("**Business Target Table**")
            business_target_db = st.selectbox(
                "Business Target Database:",
                options=st.session_state.databases_list,
                key="business_target_db"
            )

            business_target_schemas = fetch_schemas_for_db(connection, business_target_db)
            business_target_schema = st.selectbox(
                "Business Target Schema:",
                options=business_target_schemas,
                key="business_target_schema"
            )

            business_target_tables = fetch_tables_for_db_schema(
                connection,
                business_target_db,
                business_target_schema
            )
            if business_target_tables:
                business_table_name = st.selectbox(
                    "Business Target Table:",
                    options=business_target_tables,
                    key="business_target_table"
                )
            else:
                st.warning("No tables found in selected Business target database/schema")
    
    # Generate button
    if st.button("Generate", type="primary"):
        # Start timer
        start_time = time.perf_counter()

        if llm_provider == "ollama" and not st.session_state.get("ollama_model_ready", False):
            st.error("Required Ollama model is not available. Please download the model and try again.")
            return
        
        # Validate inputs
        if not requirement or not requirement.strip():
            st.error("Please enter an ETL requirement")
            return
        
        if not selected_tables:
            st.error("Please select at least one source table")
            return
        
        # Determine if tables exist based on whether names are provided
        
        # Show processing status
        with st.spinner("Processing..."):
            
            try:
                # Step 1: Validate selected source tables in Snowflake
                st.info("Validating selected source tables across databases/schemas...")
                table_groups = _group_tables_by_db_schema(selected_tables)
                if not table_groups:
                    raise ValueError("Please select valid source tables")

                for (database_name, schema_name), table_names in table_groups.items():
                    cursor = connection.cursor()
                    try:
                        cursor.execute(f"USE DATABASE {database_name}")
                        cursor.execute(f"USE SCHEMA {database_name}.{schema_name}")
                    finally:
                        cursor.close()

                    validate_tables(connection, table_names)

                # Step 2: Fetch schemas
                st.info("Fetching source table schemas...")
                schemas = {}
                for (database_name, schema_name), table_names in table_groups.items():
                    cursor = connection.cursor()
                    try:
                        cursor.execute(f"USE DATABASE {database_name}")
                        cursor.execute(f"USE SCHEMA {database_name}.{schema_name}")
                    finally:
                        cursor.close()

                    grouped_schemas = fetch_schemas(connection, table_names)
                    for table_name, columns in grouped_schemas.items():
                        qualified_table = f"{database_name}.{schema_name}.{table_name}"
                        schemas[qualified_table] = columns
                
                # Step 3: Build LLM prompt
                prompt = build_prompt(
                    requirement, 
                    schemas,
                    transform_table_name,
                    business_table_name)
                
                # Step 4: Generate code using selected provider
                llm_response = generate(prompt, provider=llm_provider)
                
                # Step 5: Extract SQL sections
                st.info("Extracting SQL sections...")
                sql_sections = extract_sql(llm_response)

                # Step 6: Validate generated SQL with LLM
                st.info("Running LLM validation checks...")
                validation_prompt = _build_validation_prompt(
                    requirement=requirement,
                    table_schemas=schemas,
                    staging_sql=sql_sections["staging"],
                    transform_sql=sql_sections["transform"],
                    business_sql=sql_sections["business"],
                )
                validation_raw = generate(validation_prompt, provider=llm_provider)
                validation_result = _parse_validation_response(validation_raw)
                
                # Store in session state (overwrite on new generation)
                st.session_state.staging_sql = sql_sections["staging"]
                st.session_state.transform_sql = sql_sections["transform"]
                st.session_state.business_sql = sql_sections["business"]
                st.session_state.validation_result = validation_result
                
                # Stop timer and calculate elapsed time
                end_time = time.perf_counter()
                elapsed = end_time - start_time
                minutes = int(elapsed // 60)
                seconds = int(elapsed % 60)
                
                st.success("Code generation complete!")
                st.info(f"⏱️ Generation time: {minutes:02d}:{seconds:02d}")
                
            except ValueError as e:
                # Stop timer on error
                end_time = time.perf_counter()
                elapsed = end_time - start_time
                minutes = int(elapsed // 60)
                seconds = int(elapsed % 60)
                
                st.error(f"Validation error: {str(e)}")
                st.info(f"⏱️ Time elapsed: {minutes:02d}:{seconds:02d}")
            except Exception as e:
                # Stop timer on error
                end_time = time.perf_counter()
                elapsed = end_time - start_time
                minutes = int(elapsed // 60)
                seconds = int(elapsed % 60)
                
                st.error(f"Error: {str(e)}")
                st.info(f"⏱️ Time elapsed: {minutes:02d}:{seconds:02d}")
    
    # Display SQL sections
    if "staging_sql" in st.session_state:
        st.divider()
        
        # Create tabs for the three SQL sections
        sql_tab1, sql_tab2, sql_tab3 = st.tabs(["Staging SQL", "Transform SQL", "Business SQL"])
        
        with sql_tab1:
            st.text_area(
                "Staging SQL (editable)",
                height=400,
                key="staging_sql",
                label_visibility="collapsed"
            )
        
        with sql_tab2:
            st.text_area(
                "Transform SQL (editable)",
                height=400,
                key="transform_sql",
                label_visibility="collapsed"
            )
        
        with sql_tab3:
            st.text_area(
                "Business SQL (editable)",
                height=400,
                key="business_sql",
                label_visibility="collapsed"
            )

        validation_result = st.session_state.get("validation_result")
        if validation_result:
            st.markdown("### Validation Report")
            st.metric("Confidence Score", f"{validation_result.get('score', 0)}%")
            st.write(validation_result.get("summary", "Validation complete."))

            checks = validation_result.get("checks", [])
            if checks:
                st.markdown("**Validation Checks**")
                for check in checks:
                    status = check.get("status", "PARTIAL")
                    icon = "✅" if status == "PASS" else ("⚠️" if status == "PARTIAL" else "❌")
                    st.write(f"{icon} {check.get('name', 'Check')}: {status}")
                    if check.get("details"):
                        st.caption(check.get("details"))

            missing_items = validation_result.get("missing_items", [])
            if missing_items:
                st.markdown("**Missing/Weak Items**")
                for item in missing_items:
                    st.write(f"- {item}")

            st.divider()

        chat_col_left, chat_col_right = st.columns([9, 1])
        with chat_col_right:
            open_sql_chat = st.button("Open SQL Assistant", key="open_sql_chat_btn", type="secondary")

        if "sql_chat_messages" not in st.session_state:
            st.session_state.sql_chat_messages = []

        @st.dialog("SQL Assistant")
        def show_sql_chat_dialog():
            for message in st.session_state.sql_chat_messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            user_message = st.chat_input("Ask about the generated SQL...")
            if user_message:
                st.session_state.sql_chat_messages.append({"role": "user", "content": user_message})

                with st.chat_message("user"):
                    st.markdown(user_message)

                with st.chat_message("assistant"):
                    try:
                        chat_prompt = _build_sql_chat_prompt(
                            user_message,
                            st.session_state.get("staging_sql", ""),
                            st.session_state.get("transform_sql", ""),
                            st.session_state.get("business_sql", ""),
                        )
                        assistant_reply = generate(chat_prompt, provider=llm_provider)
                        st.markdown(assistant_reply)
                        st.session_state.sql_chat_messages.append({"role": "assistant", "content": assistant_reply})
                    except Exception as e:
                        error_message = f"Chat error: {str(e)}"
                        st.error(error_message)
                        st.session_state.sql_chat_messages.append({"role": "assistant", "content": error_message})

            if st.button("Close Chat", key="close_sql_chat_btn"):
                st.rerun()

        if open_sql_chat:
            show_sql_chat_dialog()

        st.divider()

        confirm_execute = st.checkbox(
            f"I confirm that I want to execute the generated SQL in {selected_db_connection}",
            key="confirm_execute_pipeline"
        )

        if st.button("Execute ETL Pipeline", type="primary"):
            if not confirm_execute:
                st.warning("Please confirm execution before running the ETL pipeline")
            elif not connection:
                st.error("Snowflake connection is not available")
            elif not st.session_state.get("staging_sql"):
                st.error("Staging SQL is missing")
            elif not st.session_state.get("transform_sql"):
                st.error("Transform SQL is missing")
            elif not st.session_state.get("business_sql"):
                st.error("Business SQL is missing")
            else:
                try:
                    st.info("Executing Staging Layer...")
                    execute_sql_statements(connection, st.session_state.staging_sql)
                    st.success("Staging Layer executed successfully")

                    st.info("Executing Transform Layer...")
                    execute_sql_statements(connection, st.session_state.transform_sql)
                    st.success("Transform Layer executed successfully")

                    st.info("Executing Business Layer...")
                    execute_sql_statements(connection, st.session_state.business_sql)
                    st.success("Business Layer executed successfully")

                    st.success("ETL pipeline executed successfully")
                except Exception as e:
                    st.error(f"ETL execution failed: {str(e)}")
        
        # Save Output button
        st.divider()
        if st.button("Save Output", type="secondary"):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                folder_name = f"AI_ETL_Output_{timestamp}"
                
                # Create folder
                base_path = Path.cwd()
                folder_path = base_path / folder_name
                folder_path.mkdir(parents=True, exist_ok=False)
                
                # Write SQL files
                (folder_path / "staging.sql").write_text(st.session_state.staging_sql, encoding="utf-8")
                (folder_path / "transform.sql").write_text(st.session_state.transform_sql, encoding="utf-8")
                (folder_path / "business.sql").write_text(st.session_state.business_sql, encoding="utf-8")
                
                st.success(f"✅ SQL files saved successfully to: {folder_path}")
                
            except Exception as e:
                st.error(f"Failed to save files: {str(e)}")

    # Close connection at the end
    if connection:
        try:
            connection.close()
        except:
            pass


if __name__ == "__main__":
    main()
