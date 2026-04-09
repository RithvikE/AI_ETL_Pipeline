"""Fetch table schemas"""


def list_tables(connection, schema_name=None):
    """
    Fetch list of tables from specified schema or current schema.
    
    Args:
        connection: Active Snowflake connection object
        schema_name: Optional schema name. If None, uses CURRENT_SCHEMA() or defaults to 'AI_ETL_RAW'
        
    Returns:
        list: List of table names in the specified schema
    """
    cursor = connection.cursor()
    
    try:
        if schema_name:
            query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = UPPER(%s)
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            cursor.execute(query, (schema_name,))
        else:
            # Use current schema or fall back to AI_ETL_RAW
            query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = COALESCE(CURRENT_SCHEMA(), 'AI_ETL_RAW')
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    
    finally:
        cursor.close()


def fetch_schemas(connection, table_names):
    """
    Fetch schema information for specified tables from Snowflake.
    
    Args:
        connection: Active Snowflake connection object
        table_names: List of table names (unqualified, e.g., ['table1', 'table2'])
        
    Returns:
        dict: Schema information in format:
              {
                  "table1": [
                      {"column_name": "col1", "data_type": "VARCHAR"},
                      {"column_name": "col2", "data_type": "NUMBER"}
                  ],
                  "table2": [...]
              }
    """
    schemas = {}
    cursor = connection.cursor()
    
    try:
        for table_name in table_names:
            # Query INFORMATION_SCHEMA.COLUMNS for table schema
            query = """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
                AND TABLE_NAME = UPPER(%s)
                ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (table_name,))
            results = cursor.fetchall()
            
            # Build list of column information
            columns = []
            for row in results:
                columns.append({
                    "column_name": row[0],
                    "data_type": row[1]
                })
            
            schemas[table_name] = columns
    
    finally:
        cursor.close()
    
    return schemas
