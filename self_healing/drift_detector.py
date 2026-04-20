"""Schema drift detection utilities for self-healing pipelines."""

from typing import Any


def _normalize_schema(schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Normalize schema into a table/column map for case-insensitive comparison.

    Normalization rules:
    - Table names are compared case-insensitively.
    - Column names are normalized to lowercase.
    - Data types are normalized to uppercase.
    """
    normalized: dict[str, dict[str, Any]] = {}

    if not isinstance(schema, dict):
        return normalized

    for raw_table_name, raw_columns in schema.items():
        table_name = str(raw_table_name).strip()
        if not table_name:
            continue

        table_key = table_name.lower()
        table_display_name = table_name.upper()

        if table_key not in normalized:
            normalized[table_key] = {
                "display_name": table_display_name,
                "columns": {},
            }

        if not isinstance(raw_columns, list):
            continue

        for raw_col in raw_columns:
            if not isinstance(raw_col, dict):
                continue

            col_name = str(raw_col.get("column_name", "")).strip().lower()
            if not col_name:
                continue

            data_type = str(raw_col.get("data_type", "")).strip().upper()
            normalized[table_key]["columns"][col_name] = {
                "column_name": col_name,
                "data_type": data_type,
            }

    return normalized


def _compare_table_columns(
    old_columns: dict[str, dict[str, str]],
    new_columns: dict[str, dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    """Compare two normalized table column maps and return added/removed/modified drift."""
    added: list[dict[str, str]] = []
    removed: list[dict[str, str]] = []
    modified: list[dict[str, str]] = []

    old_keys = set(old_columns.keys())
    new_keys = set(new_columns.keys())

    for column_name in sorted(new_keys - old_keys):
        added.append(
            {
                "column_name": new_columns[column_name]["column_name"],
                "data_type": new_columns[column_name]["data_type"],
            }
        )

    for column_name in sorted(old_keys - new_keys):
        removed.append(
            {
                "column_name": old_columns[column_name]["column_name"],
                "data_type": old_columns[column_name]["data_type"],
            }
        )

    for column_name in sorted(old_keys & new_keys):
        old_type = old_columns[column_name]["data_type"]
        new_type = new_columns[column_name]["data_type"]
        if old_type != new_type:
            modified.append(
                {
                    "column_name": column_name,
                    "old_type": old_type,
                    "new_type": new_type,
                }
            )

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
    }


def detect_schema_drift(old_schema: dict[str, Any], new_schema: dict[str, Any]) -> dict[str, Any]:
    """Detect column-level schema drift between previous and current schemas.

    Args:
        old_schema: Previously saved schema snapshot.
        new_schema: Current schema snapshot fetched from source.

    Returns:
        A dictionary with drift summary:
        {
            "has_drift": bool,
            "tables": {
                "TABLE_NAME": {
                    "added": [{"column_name": str, "data_type": str}],
                    "removed": [{"column_name": str, "data_type": str}],
                    "modified": [{"column_name": str, "old_type": str, "new_type": str}],
                }
            }
        }
    """
    normalized_old = _normalize_schema(old_schema)
    normalized_new = _normalize_schema(new_schema)

    all_table_keys = sorted(set(normalized_old.keys()) | set(normalized_new.keys()))
    table_drift: dict[str, dict[str, list[dict[str, str]]]] = {}

    for table_key in all_table_keys:
        old_table = normalized_old.get(table_key, {"display_name": table_key.upper(), "columns": {}})
        new_table = normalized_new.get(table_key, {"display_name": table_key.upper(), "columns": {}})

        old_columns = old_table.get("columns", {})
        new_columns = new_table.get("columns", {})

        comparison = _compare_table_columns(old_columns, new_columns)
        if comparison["added"] or comparison["removed"] or comparison["modified"]:
            table_name = new_table.get("display_name") or old_table.get("display_name") or table_key.upper()
            table_drift[str(table_name)] = comparison

    return {
        "has_drift": bool(table_drift),
        "tables": table_drift,
    }
