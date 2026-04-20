"""Snowflake discovery helpers for databases, schemas, and tables."""

from snowflake_client.schema_fetcher import list_tables


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
