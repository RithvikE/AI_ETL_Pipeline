"""LLM chat and validation prompt helpers."""

import json
import re


def build_sql_chat_prompt(user_message, staging_sql, transform_sql, business_sql):
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


def build_validation_prompt(requirement, table_schemas, staging_sql, transform_sql, business_sql):
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


def parse_validation_response(raw_text):
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
