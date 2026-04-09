"""Execute generated SQL against Snowflake."""

def split_sql_statements(sql_text):
    """Split multi-statement SQL into executable statements."""
    if not sql_text:
        return []

    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False

    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]

        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(ch)
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
        elif ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(ch)

        i += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)

    return statements


def execute_sql_statements(connection, sql_text):
    """Execute SQL statements sequentially using Snowflake cursor.execute()."""
    statements = split_sql_statements(sql_text)
    if not statements:
        raise ValueError("No executable SQL statements found")

    cursor = connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
    finally:
        cursor.close()
