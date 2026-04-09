"""Table existence validation"""


def validate_tables(connection, table_names):
    """
    Validate that all specified tables exist in the current Snowflake schema.
    
    Args:
        connection: Active Snowflake connection object
        table_names: List of table names to validate (unqualified, e.g., ['table1', 'table2'])
        
    Raises:
        ValueError: If any table does not exist in the current schema
    """
    if not table_names:
        raise ValueError("No table names provided for validation")
    
    cursor = connection.cursor()
    invalid_tables = []
    
    try:
        for table_name in table_names:
            # Query INFORMATION_SCHEMA to check if table exists in current schema
            query = """
                SELECT COUNT(*) as table_count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
                AND TABLE_NAME = UPPER(%s)
            """
            
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            
            if result[0] == 0:
                invalid_tables.append(table_name)
    
    finally:
        cursor.close()
    
    if invalid_tables:
        raise ValueError(
            f"The following tables do not exist in the current schema: {', '.join(invalid_tables)}"
        )
