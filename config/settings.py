"""Env/config loading (Snowflake, Ollama)"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_snowflake_config():
    """
    Load Snowflake configuration from environment variables.
    
    Returns:
        dict: Snowflake configuration with keys:
              account, user, password, database, schema, warehouse, role (optional)
              
    Raises:
        ValueError: If required configuration is missing
    """
    config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE")
    }
    
    # Validate required fields
    required_fields = ["account", "user", "password", "database", "schema", "warehouse"]
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        raise ValueError(
            f"Missing required Snowflake configuration: {', '.join(missing_fields)}. "
            f"Provide via environment variables (SNOWFLAKE_*) in .env file or system environment."
        )
    
    return config


def get_ollama_config():
    """
    Load Ollama configuration from environment variables.
    
    Returns:
        dict: Ollama configuration with keys: model, host
              
    Raises:
        ValueError: If required configuration is missing
    """
    config = {
        "model": os.getenv("OLLAMA_MODEL"),
        "host": os.getenv("OLLAMA_HOST", "http://localhost:11434")
    }
    
    # Validate required fields
    if not config.get("model"):
        raise ValueError(
            "Missing required Ollama model configuration. "
            "Provide via OLLAMA_MODEL environment variable in .env file or system environment."
        )
    
    # Set default host if not provided
    if not config.get("host"):
        config["host"] = "http://localhost:11434"
    
    return config


def get_openai_config():
    """
    Load OpenAI configuration from environment variables.

    Returns:
        dict: OpenAI configuration with keys:
              api_key, model, base_url (optional)

    Raises:
        ValueError: If required configuration is missing
    """
    config = {
        "api_key": os.getenv("OPENAI_API_KEY"),
        "model": os.getenv("OPENAI_MODEL", "gpt-4.1"),
        "base_url": os.getenv("OPENAI_BASE_URL"),
    }

    if not config.get("api_key"):
        raise ValueError(
            "Missing required OpenAI API key configuration. "
            "Provide via OPENAI_API_KEY environment variable in .env file or system environment."
        )

    if not config.get("model"):
        config["model"] = "gpt-4.1"

    return config
