"""Handles Python script generation logic"""

import re


def extract_python(llm_response):
    """
    Extract the Python section from the LLM response.
    
    Args:
        llm_response: Raw text response from the LLM
        
    Returns:
        str: Extracted Python code
        
    Raises:
        ValueError: If Python section cannot be found or extracted
    """
    if not llm_response or not llm_response.strip():
        raise ValueError("LLM response is empty")
    
    # Normalize response for easier parsing
    response = llm_response
    
    # Look for Python section marker (case-insensitive)
    python_pattern = r"(?i)(section\s*2\s*[-:]*\s*python|python\s*[:]\s*|### Python|##\s*Python)"
    python_match = re.search(python_pattern, response)
    
    if not python_match:
        raise ValueError("Could not find Python section in LLM response")
    
    # Extract everything after the Python marker
    python_start = python_match.end()
    python_code = response[python_start:]
    
    # Clean up the extracted Python code
    python_code = python_code.strip()
    
    # Remove common markdown code block markers if present
    python_code = re.sub(r'^```python\s*\n?', '', python_code, flags=re.IGNORECASE)
    python_code = re.sub(r'^```\s*\n?', '', python_code)
    python_code = re.sub(r'\n?```\s*$', '', python_code)
    python_code = python_code.strip()
    
    if not python_code:
        raise ValueError("Python section is empty after extraction")
    
    return python_code
