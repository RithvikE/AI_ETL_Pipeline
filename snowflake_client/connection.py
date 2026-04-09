"""Snowflake connection logic"""

import snowflake.connector
from config.settings import get_snowflake_config


def get_connection(custom_config=None):
    """
    Create and return a Snowflake database connection.
    
    Args:
        custom_config: Optional dictionary with custom Snowflake credentials.
                      If None, uses credentials from .env file.
                      Expected keys: account, user, password, warehouse, role (opt), database (opt), schema (opt)
    
    Returns:
        snowflake.connector.connection.SnowflakeConnection: Active Snowflake connection
        
    Raises:
        Exception: If connection fails with descriptive error message
    """
    try:
        if custom_config:
            config = custom_config
        else:
            config = get_snowflake_config()
        
        # Build connection parameters
        conn_params = {
            "account": config["account"],
            "user": config["user"],
            "password": config["password"],
            "warehouse": config["warehouse"]
        }
        
        # Add optional parameters if present
        if config.get("role"):
            conn_params["role"] = config["role"]
        if config.get("database"):
            conn_params["database"] = config["database"]
        if config.get("schema"):
            conn_params["schema"] = config["schema"]
        
        connection = snowflake.connector.connect(**conn_params)
        
        return connection
        
    except snowflake.connector.errors.DatabaseError as e:
        raise Exception(f"Snowflake connection failed: {str(e)}")
    except snowflake.connector.errors.ProgrammingError as e:
        raise Exception(f"Snowflake connection failed: {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to establish Snowflake connection: {str(e)}")
