"""Handles SQL-specific generation logic"""

import re


def extract_sql(llm_response):
    """
    Extract the three SQL sections from the LLM response.
    
    Args:
        llm_response: Raw text response from the LLM
        
    Returns:
        dict: Extracted SQL code with keys: "staging", "transform", "business"
        
    Raises:
        ValueError: If any SQL section cannot be found or extracted
    """
    if not llm_response or not llm_response.strip():
        raise ValueError("LLM response is empty")
    
    response = llm_response
    
    # Look for the three section markers (case-insensitive)
    staging_pattern = r"===\s*STAGING\s*SQL\s*==="
    transform_pattern = r"===\s*TRANSFORM\s*SQL\s*==="
    business_pattern = r"===\s*BUSINESS\s*SQL\s*==="

    staging_match = re.search(staging_pattern, response, re.IGNORECASE)
    transform_match = re.search(transform_pattern, response, re.IGNORECASE)
    business_match = re.search(business_pattern, response, re.IGNORECASE)
    
    # Validate all sections are present
    if not staging_match:
        raise ValueError("Could not find 'Staging SQL' in LLM response")
    if not transform_match:
        raise ValueError("Could not find 'Transform SQL' in LLM response")
    if not business_match:
        raise ValueError("Could not find 'Business SQL' in LLM response")
    
    # Extract Staging SQL (between Section 1 and Section 2)
    staging_start = staging_match.end()
    staging_end = transform_match.start()
    staging_sql = response[staging_start:staging_end]
    
    # Extract Transform SQL (between Section 2 and Section 3)
    transform_start = transform_match.end()
    transform_end = business_match.start()
    transform_sql = response[transform_start:transform_end]
    
    # Extract Business SQL (after Section 3)
    business_start = business_match.end()
    business_sql = response[business_start:]
    
    # Clean up each section
    staging_sql = _clean_sql(staging_sql)
    transform_sql = _clean_sql(transform_sql)
    business_sql = _clean_sql(business_sql)
    
    # Validate all sections have content
    if not staging_sql:
        raise ValueError("Staging SQL section is empty after extraction")
    if not transform_sql:
        raise ValueError("Transform SQL section is empty after extraction")
    if not business_sql:
        raise ValueError("Business SQL section is empty after extraction")
    
    return {
        "staging": staging_sql,
        "transform": transform_sql,
        "business": business_sql
    }


def _clean_sql(sql_code):
    """
    Clean SQL code by removing markdown code fences and extra whitespace.
    
    Args:
        sql_code: Raw SQL string
        
    Returns:
        str: Cleaned SQL code
    """
    sql_code = sql_code.strip()
    
    # Remove common markdown code block markers if present
    sql_code = re.sub(r'^```sql\s*\n?', '', sql_code, flags=re.IGNORECASE)
    sql_code = re.sub(r'^```\s*\n?', '', sql_code)
    sql_code = re.sub(r'\n?```\s*$', '', sql_code)
    
    return sql_code.strip()
