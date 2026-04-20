"""Streamlit entry point"""

import streamlit as st
import time
import subprocess
import tempfile
import os
import difflib
from datetime import datetime
from pathlib import Path
from config.settings import get_ollama_config
from snowflake_client.connection import get_connection
from snowflake_client.discovery import (
    fetch_databases,
    fetch_schemas_for_db,
    fetch_tables_for_db_schema,
)
from snowflake_client.validator import validate_tables
from snowflake_client.schema_fetcher import fetch_schemas, list_tables
from ingestion.source_ingestion import (
    extract_csv_schema,
    build_api_ingestion_script,
    ingest_records_to_raw,
)
from llm.prompt_builder import build_prompt
from llm.ollama_client import generate, list_ollama_models
from llm.model_utils import normalize_model_name, candidate_model_names
from llm.chat_utils import (
    build_sql_chat_prompt,
    build_validation_prompt,
    parse_validation_response,
)
from generation.sql_generator import extract_sql
from execution.sql_executor import execute_sql_statements
from utils.table_utils import group_tables_by_db_schema
from utils.output_manager import write_sql_output_files, save_outputs_in_github
from self_healing.registry import list_pipelines, load_pipeline, save_pipeline
from self_healing.drift_detector import detect_schema_drift


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

    st.markdown("## Self-Healing Pipelines")
    try:
        saved_pipelines = list_pipelines()
    except Exception as e:
        st.error(f"Failed to load saved pipelines: {str(e)}")
        saved_pipelines = []

    if not saved_pipelines:
        st.info("No saved pipelines found. Generate a pipeline first.")
    else:
        pipeline_ids = [pipeline.get("pipeline_id", "") for pipeline in saved_pipelines]
        selected_pipeline_id = st.selectbox(
            "Select Existing Pipeline",
            options=pipeline_ids,
            key="self_healing_selected_pipeline",
        )

        if selected_pipeline_id:
            try:
                selected_pipeline = load_pipeline(selected_pipeline_id)
                with st.expander("Pipeline Details", expanded=False):
                    st.markdown("**Requirement:**")
                    st.write(selected_pipeline.get("requirement", ""))

                    st.markdown("**Source Tables:**")
                    for table_name in selected_pipeline.get("source_tables", []):
                        st.write(table_name)

                    st.markdown("**Last Updated:**")
                    st.write(selected_pipeline.get("last_updated", ""))
            except Exception as e:
                st.error(f"Failed to load selected pipeline: {str(e)}")

            if st.button("Check for Schema Drift", type="secondary", key="check_schema_drift_btn"):
                try:
                    pipeline_for_drift = load_pipeline(selected_pipeline_id)
                except Exception:
                    st.error("Failed to load pipeline.")
                else:
                    old_schema = pipeline_for_drift.get("schema_snapshot", {})
                    source_tables = pipeline_for_drift.get("source_tables", [])
                    new_schema = {}
                    schema_connection = None

                    try:
                        schema_connection = get_connection(st.session_state.snowflake_config)
                        table_groups = group_tables_by_db_schema(source_tables)

                        for (database_name, schema_name), table_names in table_groups.items():
                            cursor = schema_connection.cursor()
                            try:
                                cursor.execute(f"USE DATABASE {database_name}")
                                cursor.execute(f"USE SCHEMA {database_name}.{schema_name}")
                            finally:
                                cursor.close()

                            grouped_schemas = fetch_schemas(schema_connection, table_names)
                            for table_name, columns in grouped_schemas.items():
                                qualified_table = f"{database_name}.{schema_name}.{table_name}"
                                new_schema[qualified_table] = columns
                    except Exception:
                        st.error("Failed to fetch latest schema.")
                    else:
                        drift_result = detect_schema_drift(old_schema, new_schema)
                        if not drift_result.get("has_drift", False):
                            st.success("No schema drift detected.")
                        else:
                            st.warning("Schema drift detected.")
                            for table_name, changes in drift_result.get("tables", {}).items():
                                st.markdown(f"**Table: {table_name}**")

                                added_columns = changes.get("added", [])
                                if added_columns:
                                    st.markdown("**Added Columns:**")
                                    for col in added_columns:
                                        st.write(f"- {col.get('column_name', '')} ({col.get('data_type', '')})")

                                removed_columns = changes.get("removed", [])
                                if removed_columns:
                                    st.markdown("**Removed Columns:**")
                                    for col in removed_columns:
                                        st.write(f"- {col.get('column_name', '')} ({col.get('data_type', '')})")

                                modified_columns = changes.get("modified", [])
                                if modified_columns:
                                    st.markdown("**Modified Columns:**")
                                    for col in modified_columns:
                                        st.write(
                                            f"- {col.get('column_name', '')} "
                                            f"({col.get('old_type', '')} -> {col.get('new_type', '')})"
                                        )

                                st.write("")

                            saved_requirement = pipeline_for_drift.get("requirement", "")
                            if not saved_requirement or not str(saved_requirement).strip():
                                st.error("Pipeline requirement not found.")
                            else:
                                try:
                                    regeneration_prompt = build_prompt(
                                        saved_requirement,
                                        new_schema,
                                        None,
                                        None,
                                    )
                                    regenerated_response = generate(
                                        regeneration_prompt,
                                        provider="openai",
                                    )
                                    regenerated_sections = extract_sql(regenerated_response)

                                    st.session_state.regenerated_staging_sql = regenerated_sections["staging"]
                                    st.session_state.regenerated_transform_sql = regenerated_sections["transform"]
                                    st.session_state.regenerated_business_sql = regenerated_sections["business"]
                                    st.session_state.old_saved_sql = pipeline_for_drift.get("sql", {})
                                    st.session_state.drift_result = drift_result
                                    st.session_state.regenerated_schema_snapshot = new_schema

                                    st.success("Updated SQL regenerated successfully.")
                                except Exception:
                                    st.error("SQL regeneration failed.")
                                    st.error("Failed to regenerate SQL.")
                    finally:
                        if schema_connection:
                            try:
                                schema_connection.close()
                            except Exception:
                                pass

            regenerated_keys = [
                "regenerated_staging_sql",
                "regenerated_transform_sql",
                "regenerated_business_sql",
            ]
            if all(key in st.session_state for key in regenerated_keys):
                old_saved_sql = st.session_state.get("old_saved_sql")
                if not isinstance(old_saved_sql, dict) or not old_saved_sql:
                    st.error("Previous SQL not found.")
                else:
                    st.markdown("### SQL Changes Detected")

                    diff_pairs = [
                        (
                            "Staging SQL Diff",
                            "Staging",
                            str(old_saved_sql.get("staging", "")),
                            str(st.session_state.get("regenerated_staging_sql", "")),
                        ),
                        (
                            "Transform SQL Diff",
                            "Transform",
                            str(old_saved_sql.get("transform", "")),
                            str(st.session_state.get("regenerated_transform_sql", "")),
                        ),
                        (
                            "Business SQL Diff",
                            "Business",
                            str(old_saved_sql.get("business", "")),
                            str(st.session_state.get("regenerated_business_sql", "")),
                        ),
                    ]

                    diff_summaries = []
                    for title, layer_name, old_sql, new_sql in diff_pairs:
                        with st.expander(title, expanded=False):
                            diff_lines = list(
                                difflib.unified_diff(
                                    old_sql.splitlines(),
                                    new_sql.splitlines(),
                                    fromfile="old",
                                    tofile="new",
                                    lineterm="",
                                )
                            )
                            if diff_lines:
                                st.code("\n".join(diff_lines), language="diff")
                            else:
                                st.write("No changes detected.")

                        added_count = 0
                        removed_count = 0
                        for line in diff_lines:
                            if line.startswith("+++") or line.startswith("---") or line.startswith("@@"):
                                continue
                            if line.startswith("+"):
                                added_count += 1
                            elif line.startswith("-"):
                                removed_count += 1

                        diff_summaries.append((layer_name, added_count, removed_count))

                    st.markdown("#### High-Level Changes")
                    for layer_name, added_count, removed_count in diff_summaries:
                        if added_count == 0 and removed_count == 0:
                            st.write(f"{layer_name}: No changes detected.")
                        else:
                            st.write(f"{layer_name}:")
                            st.write(f"--- {removed_count} line(s) removed/changed")
                            st.write(f"+++ {added_count} line(s) added/updated")

                    accept_col, ignore_col = st.columns(2)
                    with accept_col:
                        accept_clicked = st.button("Accept Changes", type="primary", key="accept_regenerated_changes")
                    with ignore_col:
                        ignore_clicked = st.button("Ignore", type="secondary", key="ignore_regenerated_changes")

                    if accept_clicked:
                        regenerated_staging_sql = st.session_state.get("regenerated_staging_sql")
                        regenerated_transform_sql = st.session_state.get("regenerated_transform_sql")
                        regenerated_business_sql = st.session_state.get("regenerated_business_sql")

                        if not all([
                            regenerated_staging_sql,
                            regenerated_transform_sql,
                            regenerated_business_sql,
                        ]):
                            st.warning("Regenerated SQL is missing. Cannot export or push changes.")
                        else:
                            pipeline_id_to_update = selected_pipeline_id
                            pipeline_updated = False

                            try:
                                pipeline_to_update = load_pipeline(selected_pipeline_id)
                                pipeline_id_to_update = pipeline_to_update.get("pipeline_id", selected_pipeline_id)
                                save_pipeline(
                                    pipeline_id=pipeline_id_to_update,
                                    requirement=pipeline_to_update.get("requirement", ""),
                                    source_tables=pipeline_to_update.get("source_tables", []),
                                    schema_snapshot=st.session_state.get(
                                        "regenerated_schema_snapshot",
                                        pipeline_to_update.get("schema_snapshot", {}),
                                    ),
                                    staging_sql=regenerated_staging_sql,
                                    transform_sql=regenerated_transform_sql,
                                    business_sql=regenerated_business_sql,
                                )
                                pipeline_updated = True
                            except Exception:
                                st.error("Failed to update pipeline.")

                            if pipeline_updated:
                                export_succeeded = False
                                try:
                                    output_folder = Path.cwd() / "etl_outputs" / str(pipeline_id_to_update)
                                    output_folder.mkdir(parents=True, exist_ok=True)
                                    (output_folder / "staging.sql").write_text(regenerated_staging_sql, encoding="utf-8")
                                    (output_folder / "transform.sql").write_text(regenerated_transform_sql, encoding="utf-8")
                                    (output_folder / "business.sql").write_text(regenerated_business_sql, encoding="utf-8")
                                    export_succeeded = True
                                except Exception as e:
                                    st.warning("Pipeline updated, but SQL export failed.")
                                    st.error(f"SQL export error: {str(e)}")

                                if export_succeeded:
                                    try:
                                        subprocess.run(
                                            ["git", "--version"],
                                            capture_output=True,
                                            text=True,
                                            check=True,
                                        )
                                    except FileNotFoundError:
                                        st.warning("Git is not available.")
                                    except Exception:
                                        st.warning("Git is not available.")
                                    else:
                                        repo_check = subprocess.run(
                                            ["git", "-C", str(Path.cwd()), "rev-parse", "--is-inside-work-tree"],
                                            capture_output=True,
                                            text=True,
                                            check=False,
                                        )

                                        if repo_check.returncode != 0:
                                            st.warning("Pipeline updated and SQL exported, but Git repository is not initialized.")
                                        else:
                                            pipeline_json_path = os.path.join(
                                                "self_healing",
                                                "storage",
                                                f"{pipeline_id_to_update}.json",
                                            )
                                            pipeline_sql_folder_path = os.path.join(
                                                "etl_outputs",
                                                str(pipeline_id_to_update),
                                            )

                                            git_add_json = subprocess.run(
                                                ["git", "-C", str(Path.cwd()), "add", pipeline_json_path],
                                                capture_output=True,
                                                text=True,
                                                check=False,
                                            )
                                            git_add_sql = subprocess.run(
                                                ["git", "-C", str(Path.cwd()), "add", pipeline_sql_folder_path],
                                                capture_output=True,
                                                text=True,
                                                check=False,
                                            )

                                            if git_add_json.returncode != 0 or git_add_sql.returncode != 0:
                                                details = (
                                                    git_add_json.stderr
                                                    or git_add_sql.stderr
                                                    or git_add_json.stdout
                                                    or git_add_sql.stdout
                                                    or "Failed to stage files."
                                                ).strip()
                                                st.warning("Pipeline updated and SQL exported, but Git push failed.")
                                                st.error(details)
                                            else:
                                                commit_message = f"[Auto-Heal] {pipeline_id_to_update} schema drift updated"
                                                git_commit = subprocess.run(
                                                    [
                                                        "git",
                                                        "-C",
                                                        str(Path.cwd()),
                                                        "commit",
                                                        "-m",
                                                        commit_message,
                                                    ],
                                                    capture_output=True,
                                                    text=True,
                                                    check=False,
                                                )

                                                if git_commit.returncode != 0:
                                                    commit_output = (git_commit.stderr or git_commit.stdout or "").strip()
                                                    if "nothing to commit" in commit_output.lower():
                                                        git_push = subprocess.run(
                                                            ["git", "-C", str(Path.cwd()), "push", "origin", "main"],
                                                            capture_output=True,
                                                            text=True,
                                                            check=False,
                                                        )
                                                        if git_push.returncode != 0:
                                                            details = (git_push.stderr or git_push.stdout or "Git push failed.").strip()
                                                            st.warning("Pipeline updated and SQL exported, but Git push failed.")
                                                            st.error(details)
                                                        else:
                                                            st.success("Pipeline updated and pushed to GitHub successfully.")
                                                    else:
                                                        st.warning("Pipeline updated and SQL exported, but Git push failed.")
                                                        st.error(commit_output)
                                                else:
                                                    git_push = subprocess.run(
                                                        ["git", "-C", str(Path.cwd()), "push", "origin", "main"],
                                                        capture_output=True,
                                                        text=True,
                                                        check=False,
                                                    )
                                                    if git_push.returncode != 0:
                                                        details = (git_push.stderr or git_push.stdout or "Git push failed.").strip()
                                                        st.warning("Pipeline updated and SQL exported, but Git push failed.")
                                                        st.error(details)
                                                    else:
                                                        st.success("Pipeline updated and pushed to GitHub successfully.")

                    if ignore_clicked:
                        st.session_state.pop("regenerated_staging_sql", None)
                        st.session_state.pop("regenerated_transform_sql", None)
                        st.session_state.pop("regenerated_business_sql", None)
                        st.session_state.pop("drift_result", None)
                        st.session_state.pop("old_saved_sql", None)
                        st.session_state.pop("regenerated_schema_snapshot", None)
                        st.info("Changes discarded.")

    st.divider()
    
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
                    normalize_model_name(model) for model in installed_models
                }
                configured_candidates = candidate_model_names(ollama_model)

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

    @st.dialog("Prompt Guidelines")
    def show_prompt_guidelines_dialog():
        tab1, tab2, tab3, tab4 = st.tabs([
            "Core Structure",
            "Join & Dedup Rules",
            "KPI & Metrics",
            "Quality Checklist",
        ])

        with tab1:
            st.markdown("""
Write prompts using clear sections. Include all items below:

1. BUSINESS OBJECTIVE
- State what business outcome should be optimized.
- Keep objective specific and measurable.

2. TARGET DATA GRAIN
- Define exactly one row per key (single key or composite key).
- Use explicit key format such as SALES_ORDER_NO + PRODUCTID + SUPPLIERID.

3. MERGE KEY
- Declare merge key explicitly.
- Ensure merge key matches target grain exactly.

4. FINAL DATASET REQUIREMENT
- Require exactly one row per merge key after all joins.
- Require final deduplication step after full transformation.

5. OUTPUT EXPECTATIONS
- Ask for complete executable SQL only.
- Avoid placeholders and avoid incomplete sections.
""")

        with tab2:
            st.markdown("""
Define join behavior and dedup logic before KPIs.

1. JOIN RULES
- Identify base table.
- For each joined table, specify cardinality (1-to-1 or 1-to-many).
- For 1-to-many tables, require latest-record selection and key used.

2. DEDUPLICATION RULES
- Declare dedup keys for each source table.
- Declare ordering column for latest record logic.
- Require that joins must not increase row count beyond target grain.

3. SAFE TRANSFORM PATTERN
- Require final ROW_NUMBER based dedup over merge key.
- Require filtering to one record per merge key.

4. DATA TYPE AND DATE HANDLING
- If date formats vary, request explicit parsing/conversion logic.
""")

        with tab3:
            st.markdown("""
Define KPI formulas explicitly so LLM cannot guess.

Guidelines:
- Provide KPI name.
- Provide exact formula expression.
- Provide denominator safety expectation where relevant.
- Mention required columns for each KPI.

Recommended KPI format:
- KPI_NAME: <exact formula>

Example style:
- OTIF_FLAG: CASE WHEN ON_TIME_DELIVERY = TRUE AND ACCURATE_ORDER = TRUE THEN 1 ELSE 0
- LANDED_COST_PER_UNIT: UNIT_PRICE + SHIPPING_COST + INVENTORY_SERVICE_COSTS + STORAGE_COST
- GMROI: ((FINAL_PRICE - UNIT_PRICE) * SALES_QUANTITY) / (UNIT_PRICE * INVENTORY_UNITS)
""")

        with tab4:
            st.markdown("""
Before clicking Generate, check prompt quality:

- Objective is clear and domain-specific.
- Grain and merge key are explicitly defined and aligned.
- Join rules identify all many-side tables and latest-record logic.
- Dedup rules are present for every table that can duplicate rows.
- KPI definitions are explicit and formula-based.
- Final requirement states exactly one row per merge key.
- No ambiguous words such as "optimize smartly" without rules.
- Prompt avoids contradictions between grain, join, and merge instructions.
""")

            st.markdown("""
Suggested prompt template:

BUSINESS OBJECTIVE:
<business goal>

TARGET DATA GRAIN:
- One row per: <key>

JOIN RULES:
- <base table>
- <table>: <cardinality + join behavior>

MERGE KEY:
- <key>

DEDUPLICATION RULES:
- <table>: dedup on <key> using latest <column>

FINAL DATASET REQUIREMENT:
- EXACTLY ONE ROW per merge key
- Apply final ROW_NUMBER based dedup after all joins

KPI DEFINITIONS:
1. <KPI_NAME>: <formula>
2. <KPI_NAME>: <formula>
""")

    if st.button("Prompt Guidelines", type="secondary"):
        show_prompt_guidelines_dialog()
    
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

                        csv_table_name, csv_columns, csv_records = extract_csv_schema(
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
                            ingest_records_to_raw(
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
            if "api_editable_script" not in st.session_state:
                st.session_state.api_editable_script = ""

            api_signature = f"{api_url.strip()}|{len(api_token.strip())}"
            if api_signature != st.session_state.api_script_last_signature:
                st.session_state.api_script_last_signature = api_signature
                st.session_state.api_script_collapsed = False
                st.session_state.api_editable_script = ""  # Reset when URL changes

            if api_url.strip() and api_token.strip():
                preview_script = build_api_ingestion_script(
                    api_url=api_url.strip(),
                    selected_database=st.session_state.get("selected_database")
                )

                with st.expander(
                    "API Ingestion Python Script (Edit & Execute)",
                    expanded=not st.session_state.api_script_collapsed,
                ):
                    st.caption("Review and customize the script below if needed, then click 'Fetch and Ingest API Source' to execute it.")
                    
                    if st.button("Reset to Default", type="secondary", key="reset_api_script"):
                        st.session_state.api_editable_script = preview_script
                        st.rerun()
                    
                    # Initialize editable script with preview if empty
                    if not st.session_state.api_editable_script:
                        st.session_state.api_editable_script = preview_script
                    
                    editable_script = st.text_area(
                        "Python Script:",
                        value=st.session_state.api_editable_script,
                        height=400,
                        key="api_script_editor"
                    )
                    
                    # Update session state when user edits
                    st.session_state.api_editable_script = editable_script

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
                    
                    if not st.session_state.api_editable_script or not st.session_state.api_editable_script.strip():
                        raise ValueError("Please ensure the script is not empty")

                    # Execute the edited script
                    script_to_execute = st.session_state.api_editable_script
                    
                    # Create a temporary script file
                    temp_script = tempfile.NamedTemporaryFile(
                        mode='w',
                        suffix='.py',
                        delete=False,
                        encoding='utf-8'
                    )
                    temp_script.write(script_to_execute)
                    temp_script.close()
                    
                    try:
                        # Execute the script
                        result = subprocess.run(
                            ["python", temp_script.name],
                            capture_output=True,
                            text=True,
                            timeout=300,
                            cwd=str(Path.cwd())
                        )
                        
                        # Check for execution errors
                        if result.returncode != 0:
                            error_output = result.stderr or result.stdout or "Unknown error"
                            raise ValueError(f"Script execution failed:\n{error_output}")
                        
                        # Display success message and script output
                        st.success("✅ API ingestion script executed successfully!")
                        if result.stdout:
                            st.info(f"Script output:\n{result.stdout}")
                        
                        # Re-fetch to update selected sources if needed
                        if selected_database_for_ingest in st.session_state.databases_list:
                            try:
                                cursor_refresh = connection.cursor()
                                cursor_refresh.execute(f"USE DATABASE {selected_database_for_ingest}")
                                cursor_refresh.execute(f"USE SCHEMA {selected_database_for_ingest}.AI_ETL_RAW")
                                tables = list_tables(connection)
                                for table in tables:
                                    qualified_table = f"{selected_database_for_ingest}.AI_ETL_RAW.{table}"
                                    if qualified_table not in st.session_state.selected_source_tables:
                                        st.session_state.selected_source_tables.append(qualified_table)
                                cursor_refresh.close()
                            except Exception:
                                pass  # Silently continue if refresh fails
                                
                    finally:
                        # Clean up temporary file
                        try:
                            os.unlink(temp_script.name)
                        except Exception:
                            pass
                            
                except Exception as e:
                    st.error(f"Failed to execute API ingestion script: {str(e)}")
                    st.warning("You can edit the script above and try again.")

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
                table_groups = group_tables_by_db_schema(selected_tables)
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
                validation_prompt = build_validation_prompt(
                    requirement=requirement,
                    table_schemas=schemas,
                    staging_sql=sql_sections["staging"],
                    transform_sql=sql_sections["transform"],
                    business_sql=sql_sections["business"],
                )
                validation_raw = generate(validation_prompt, provider=llm_provider)
                validation_result = parse_validation_response(validation_raw)
                
                # Store in session state (overwrite on new generation)
                st.session_state.staging_sql = sql_sections["staging"]
                st.session_state.transform_sql = sql_sections["transform"]
                st.session_state.business_sql = sql_sections["business"]
                st.session_state.validation_result = validation_result
                st.session_state.pipeline_requirement = requirement
                st.session_state.pipeline_source_tables = selected_tables
                st.session_state.pipeline_schema_snapshot = schemas
                
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
                        chat_prompt = build_sql_chat_prompt(
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
        
        # Save Output buttons
        st.divider()

        @st.dialog("Save Pipeline")
        def show_save_pipeline_dialog():
            pipeline_name = st.text_input("Pipeline Name", key="save_pipeline_name_input")

            if st.button("Save Pipeline", type="primary", key="confirm_save_pipeline_btn"):
                try:
                    save_pipeline(
                        pipeline_id=pipeline_name if pipeline_name and pipeline_name.strip() else "default_pipeline",
                        requirement=st.session_state.get("pipeline_requirement", ""),
                        source_tables=st.session_state.get("pipeline_source_tables", []),
                        schema_snapshot=st.session_state.get("pipeline_schema_snapshot", {}),
                        staging_sql=st.session_state.staging_sql,
                        transform_sql=st.session_state.transform_sql,
                        business_sql=st.session_state.business_sql,
                    )
                    st.success("Pipeline saved successfully.")
                except Exception as e:
                    st.error(f"Failed to save pipeline: {str(e)}")

        save_col1, save_col2 = st.columns(2)

        with save_col1:
            save_github_clicked = st.button("Save Outputs in GitHub", type="secondary")
        with save_col2:
            save_pipeline_clicked = st.button("Save Pipeline", type="secondary")

        if save_pipeline_clicked:
            show_save_pipeline_dialog()

        if save_github_clicked:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                folder_name = f"AI_ETL_Output_{timestamp}"

                base_path = Path.cwd()
                etl_outputs_root = base_path / "etl_outputs"
                etl_outputs_root.mkdir(parents=True, exist_ok=True)
                folder_path = etl_outputs_root / folder_name

                write_sql_output_files(
                    folder_path,
                    st.session_state.staging_sql,
                    st.session_state.transform_sql,
                    st.session_state.business_sql,
                )

                save_outputs_in_github(base_path, folder_path)
                st.success(f"✅ SQL files saved and pushed to GitHub: {folder_path}")
            except Exception as e:
                st.error(f"Failed to save outputs in GitHub: {str(e)}")

    # Close connection at the end
    if connection:
        try:
            connection.close()
        except:
            pass


if __name__ == "__main__":
    main()
