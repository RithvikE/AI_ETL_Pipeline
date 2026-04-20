"""Helpers for grouping and working with table references."""


def group_tables_by_db_schema(qualified_tables):
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
