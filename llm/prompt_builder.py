"""Builds LLM prompt from inputs + schema"""


def build_prompt(requirement, table_schemas, transform_table_name=None, business_table_name=None):
    """
    Build a structured prompt for the LLM to generate Snowflake SQL and Python code.
    
    Args:
        requirement: Natural language ETL requirement from user
        table_schemas: Dictionary of table schemas from fetch_schemas()
                      Format: {table_name: [{"column_name": "...", "data_type": "..."}]}
        transform_table_name: Optional name of existing transform layer table (AI_ETL_WI)
        business_table_name: Optional name of existing business layer table (AI_ETL_BR)
    
    Returns:
        str: Complete prompt string ready to send to LLM
    """
    # Extract table names
    table_names = list(table_schemas.keys())

    # Build source-to-staging fully-qualified mapping details
    source_staging_mapping = ""
    for table_name in table_names:
        parts = table_name.split(".", 2)
        if len(parts) == 3:
            database_name, source_schema, base_table_name = parts
            source_staging_mapping += (
                f"- Source: {database_name}.{source_schema}.{base_table_name} "
                f"-> Target: {database_name}.AI_ETL_STG.STG_{base_table_name}\n"
            )
        else:
            source_staging_mapping += (
                f"- Source: {table_name} -> Target: <database>.AI_ETL_STG.STG_{table_name}\n"
            )
    
    # Build compact schema details section (one line per table)
    schema_details = ""
    for table_name, columns in table_schemas.items():
      column_pairs = ", ".join(
        f"{col['column_name']} ({col['data_type']})" for col in columns
      )
      schema_details += f"\nTable: {table_name} | Columns: {column_pairs}\n"
    
    # Build conditional instructions for TRANSFORM layer
    if transform_table_name and transform_table_name.strip():
        transform_instructions = f"""- The target table <database>.AI_ETL_WI.{transform_table_name.strip()} already exists
    - Generate ONLY the MERGE INTO statement to merge data into <database>.AI_ETL_WI.{transform_table_name.strip()}
- Do NOT generate CREATE TABLE statement for transform layer"""
        
    else:
        transform_instructions = """The target table does NOT exist yet

- You MUST generate BOTH statements in this exact order:
  1. CREATE TABLE IF NOT EXISTS <table_name> AS SELECT statement with all relevant joins (this is mandatory), derived columns, and filters
  2. MERGE INTO <table_name> statement with the same SELECT logic from step 1 in the USING clause 

- Use fully qualified 3-part table name in <database>.AI_ETL_WI schema (e.g., AI_ETL.AI_ETL_WI.TRF_SALES_ENRICHED)

- Perform joins, derived columns, and data shaping required by the user requirement

- The SELECT logic used in CREATE TABLE must be reusable and consistent with MERGE
- You SHOULD define a reusable logical SELECT structure (e.g., subquery or CTE) and reuse it in both CREATE and MERGE to ensure consistency
- Prefer using a CTE (WITH clause) to define the transformation logic once, and reuse it in both CREATE TABLE and MERGE

---------------------------------------------------------------------

RULES FOR TRANSFORM LAYER SQL:

- Be aware of data types when performing transformations

- For DATE columns (e.g., DATE, EXPECTED_DELIVERY_DATE, ACTUAL_DELIVERY_DATE), use explicit parsing with TRY_TO_DATE and fallback formats.

- Use this pattern for date-like source fields:
    COALESCE(
        TRY_TO_DATE(TO_VARCHAR(<col>), 'DD-MM-YYYY'),
        TRY_TO_DATE(TO_VARCHAR(<col>), 'YYYY-MM-DD'),
        TRY_TO_DATE(<col>)
    ) AS <col>

- Apply the same date parsing logic consistently in both CREATE TABLE AS SELECT and MERGE USING SELECT parts.

---------------------------------------------------------------------

CRITICAL MERGE KEY RULE (MANDATORY):

- You MUST identify a clear business key for the MERGE condition (e.g., SALES_ORDER_NO, ORDER_ID).
- This key MUST uniquely identify a row in the target table.
- The same key MUST be used in:
    - ROW_NUMBER() PARTITION BY
    - MERGE ON condition
- NEVER use non-unique or ambiguous columns as merge keys.

---------------------------------------------------------------------

CRITICAL DEDUPLICATION RULE (MANDATORY):

- The source dataset used in MERGE MUST produce EXACTLY ONE ROW per MERGE key.

- If joins introduce duplicates (e.g., 1-to-many joins), you MUST enforce deduplication using ROW_NUMBER.

- The rn column used for deduplication MUST NOT appear in the final output table schema.
- It should only be used internally for filtering.

---------------------------------------------------------------------

MANDATORY DEDUPLICATION STRUCTURE:

- You MUST implement deduplication using the following nested subquery pattern:

    SELECT <columns_without_rn>
    FROM (
        SELECT 
            <columns>,
            ROW_NUMBER() OVER (
                PARTITION BY <merge_key>
                ORDER BY <relevant_timestamp_or_date_column> DESC
            ) AS rn
        FROM <joined tables>
    )
    WHERE rn = 1

- This structure is MANDATORY inside the MERGE USING clause.

- DO NOT apply WHERE rn = 1 outside the nested subquery.
- Filtering on rn MUST only happen inside the nested subquery.

---------------------------------------------------------------------

CRITICAL ALIAS USAGE RULE (MANDATORY):

- Column aliases created in SELECT (e.g., ORDER_DATE) MUST NOT be reused in the SAME SELECT

- If a derived column is needed again, you MUST:

    ✅ Option 1: Repeat the full expression
    ✅ Option 2 (PREFERRED): Use multi-step CTE

---------------------------------------------------------------------

MERGE INSERT RULE (MANDATORY):

- DO NOT use "INSERT ALL BY NAME" or any shorthand syntax

- You MUST explicitly define INSERT columns and VALUES:

    WHEN NOT MATCHED THEN INSERT (
        <column_list>
    )
    VALUES (
        <corresponding S.column_list>
    )

- The number and order of columns MUST match exactly between INSERT and VALUES

---------------------------------------------------------------------

DATA GRANULARITY RULE (MANDATORY):

- The MERGE key MUST match the granularity of the dataset.

- If the dataset contains multiple rows per business entity (e.g., order lines), you MUST use a composite key.

- Examples:
    - Order-level → SALES_ORDER_NO
    - Order-line-level → SALES_ORDER_NO + PRODUCTID + SUPPLIERID

- The SAME composite key MUST be used in:
    - MERGE ON condition
    - ROW_NUMBER() PARTITION BY

- NEVER use a partial key that results in multiple rows per key.

---------------------------------------------------------------------

INSERT COLUMN VALIDATION RULE:

- The INSERT column list MUST NOT contain duplicate column names
- Each column must appear EXACTLY once
- The number of INSERT columns MUST match the number of VALUES columns exactly

---------------------------------------------------------------------

SQL SYNTAX RULE (MANDATORY):

- You MUST use valid Snowflake SQL syntax ONLY
- DO NOT use programming language syntax such as:
    - ? : (ternary operators)
    - == (use = instead)
    - && (use AND)
    - || (use OR)

- ALL conditional logic MUST use:
    CASE WHEN <condition> THEN <value> ELSE <value> END

- Any invalid syntax will cause execution failure

---------------------------------------------------------------------

CREATE TABLE REQUIREMENTS:

- The CREATE TABLE AS SELECT MUST use the same nested deduplication pattern:

    SELECT <columns_without_rn>
    FROM (
        SELECT 
            <columns>,
            ROW_NUMBER() OVER (...) AS rn
        FROM ...
    )
    WHERE rn = 1

- The final CREATE TABLE output MUST NOT include the rn column.

---------------------------------------------------------------------

JOIN SAFETY RULES:

- Before performing joins, you MUST consider join cardinality:
    - If a table can produce multiple rows per join key, it MUST be deduplicated BEFORE joining.

- If a joined table (e.g., inventory, logs, history tables) contains multiple records per key:
    - You MUST pre-deduplicate that table using ROW_NUMBER before joining.

- NEVER allow join logic to create multiple rows per MERGE key.

---------------------------------------------------------------------

GENERAL RULES:

- Every SQL statement MUST end with a semicolon (;)
- Both CREATE TABLE and MERGE statements MUST end with ';'
- Make sure NOT to create duplicate columns in the SELECT statement
- Read ONLY from fully qualified staging tables (<database>.AI_ETL_STG.STG_*)
- Do NOT use metadata or watermark logic
- Use fully qualified 3-part names for all objects (DB.SCHEMA.TABLE)
"""

    
    # Build conditional instructions for BUSINESS layer
    if business_table_name and business_table_name.strip():
        business_instructions = f"""- The target table <database>.AI_ETL_BR.{business_table_name.strip()} already exists
    - Generate ONLY the MERGE INTO statement to merge data into <database>.AI_ETL_BR.{business_table_name.strip()}
- Do NOT generate CREATE TABLE statement for business layer"""
    else:
        business_instructions = """- The target table does NOT exist yet

- You MUST generate BOTH statements in this exact order:
  1. CREATE TABLE IF NOT EXISTS <table_name> (column definitions with actual column names and data types)
  2. MERGE INTO <table_name> statement with aggregation/KPI logic

- Use fully qualified 3-part table name in <database>.AI_ETL_BR schema

- The CREATE statement MUST have REAL column names (based on the aggregation logic), NOT placeholders

---------------------------------------------------------------------

RULES FOR BUSINESS LAYER SQL:

- Read ONLY from fully qualified transform tables (<database>.AI_ETL_WI.*)
- Do NOT reference AI_ETL_STG or AI_ETL_RAW tables
- Do NOT perform JOINs in business layer
- Business layer must aggregate directly from transformed table(s)
- If required dimensions are missing, assume they exist in transform layer (DO NOT re-join)

---------------------------------------------------------------------

CRITICAL AGGREGATION RULES (MANDATORY):

- ALL aggregations MUST use GROUP BY
- Window functions (e.g., SUM() OVER, AVG() OVER, COUNT() OVER) are STRICTLY NOT ALLOWED

- The SELECT query inside MERGE USING MUST follow this structure:

    SELECT
        <grouping_columns>,
        AGG_FUNCTION(...) AS <metric>
    FROM <transform_table>
    GROUP BY <grouping_columns>

---------------------------------------------------------------------

MERGE KEY RULE (MANDATORY):

- You MUST identify a clear business key for the aggregation (e.g., REGION, PRODUCT_ID, CUSTOMER_ID)
- The GROUP BY columns MUST match the MERGE key
- The USING query MUST produce EXACTLY ONE ROW per MERGE key

---------------------------------------------------------------------

COLUMN CONSISTENCY RULES:

- Every non-aggregated column in SELECT MUST be included in GROUP BY
- Do NOT mix aggregated and non-aggregated columns incorrectly

---------------------------------------------------------------------

CREATE TABLE RULES:

- The CREATE TABLE must define columns that EXACTLY match the SELECT output:
    - Grouping columns → dimensions
    - Aggregated columns → metrics

---------------------------------------------------------------------

GENERAL RULES:
- DO NOT use generic names like COL1, COL2
- Use meaningful business column names
- Use fully qualified 3-part names (DB.SCHEMA.TABLE)
"""
    
    # Construct the complete prompt
    prompt = f"""You are a Snowflake ETL SQL generator.

USER REQUIREMENT:
{requirement}

SOURCE TABLES:
{', '.join(table_names)}

SOURCE TO STAGING MAPPING (USE THIS EXACT FULLY-QUALIFIED MAPPING):
{source_staging_mapping}

TABLE SCHEMAS:
{schema_details}

SCHEMA ARCHITECTURE:
AI_ETL_RAW → AI_ETL_STG → AI_ETL_WI → AI_ETL_BR

LAYER RESPONSIBILITIES:

STAGING (AI_ETL_STG):

For EACH source table, you MUST generate a STRICT 4-step incremental load SQL pattern.
You MUST follow the EXACT structure below. DO NOT DEVIATE from this format.

----------------------------------------
STEP 1: CREATE TABLE
----------------------------------------
CREATE TABLE IF NOT EXISTS AI_ETL_STG.STG_<table>
LIKE AI_ETL_RAW.<table>;

----------------------------------------
STEP 2: INSERT INTO DATALOAD (IF NOT EXISTS)
----------------------------------------
INSERT INTO AI_ETL_STG.DATALOAD (TABLE_NAME, LAST_LOAD_DATE)
SELECT '<table>', CURRENT_TIMESTAMP()
WHERE NOT EXISTS (
    SELECT 1 FROM AI_ETL_STG.DATALOAD WHERE TABLE_NAME = '<table>'
);

----------------------------------------
STEP 3: INSERT OVERWRITE WITH WATERMARK (MANDATORY)
----------------------------------------

- You MUST use INSERT OVERWRITE
- You MUST include a WHERE clause with a watermark filter
- You MUST use ONLY a TIMESTAMP/DATE column from schema for incremental logic

INCREMENTAL COLUMN SELECTION RULE (STRICT AND MANDATORY):

- ALWAYS use LAST_UPDATED_DATE if it exists in the table
- If LAST_UPDATED_DATE does NOT exist, then use:
  UPDATED_AT → MODIFIED_DATE → CREATED_AT (in this exact priority order)

- You MUST NOT use:
  - ID columns (e.g., CUSTOMERID, PRODUCTID, ORDER_ID)
  - STRING columns (e.g., NAME, EMAIL, PHONE)
  - Numeric columns
  - Derived expressions or functions (NO HASH, NO TRY_TO_TIMESTAMP on random columns)

- If no valid timestamp/date column exists:
  → DO NOT invent logic
  → Instead use FULL LOAD (SELECT * without WHERE)

MANDATORY SQL FORMAT:

INSERT OVERWRITE INTO AI_ETL_STG.STG_<table>
SELECT *
FROM AI_ETL_RAW.<table>
WHERE <incremental_column> > (
    SELECT MAX(LAST_LOAD_DATE)
    FROM AI_ETL_STG.DATALOAD
    WHERE TABLE_NAME = '<table>'
);

----------------------------------------
STEP 4: UPDATE DATALOAD (MANDATORY)
----------------------------------------

- You MUST update using MAX(<incremental_column>)
- DO NOT use CURRENT_TIMESTAMP()

UPDATE AI_ETL_STG.DATALOAD
SET LAST_LOAD_DATE = (
    SELECT MAX(<incremental_column>)
    FROM AI_ETL_STG.STG_<table>
)
WHERE TABLE_NAME = '<table>';

----------------------------------------
CRITICAL RULES (DO NOT VIOLATE):

- DO NOT skip the WHERE clause in STEP 3
- DO NOT choose incorrect columns for incremental logic
- DO NOT use CUSTOMERID, PRODUCTID, or any ID column
- DO NOT generate partial SQL
- DO NOT change SQL structure
- DO NOT abbreviate anything
- Use fully qualified table names ONLY (AI_ETL_RAW, AI_ETL_STG)

----------------------------------------
OUTPUT REQUIREMENT:

- Generate this 4-step SQL separately for EACH source table
- Use REAL column names from schema
- Ensure consistency across all tables

TRANSFORM (AI_ETL_WI):
{transform_instructions}

BUSINESS (AI_ETL_BR):
{business_instructions}

MANDATORY OUTPUT FORMAT:

Your response MUST contain EXACTLY the following three delimiter lines,
in this exact order, each on its own line:

=== STAGING SQL ===
=== TRANSFORM SQL ===
=== BUSINESS SQL ===

Between the delimiters, output ONLY executable Snowflake SQL.

FINAL OUTPUT RULES:
- The FIRST line of output MUST be === STAGING SQL ===
- ALL three sections MUST be present in their respective sections
- Use each delimiter exactly once
- NO placeholders such as <table>, <column>, or ...
- Generate REAL, FULL SQL based on the provided schemas
- Do NOT omit any transform or business instructions - follow ALL instructions above carefully to generate the SQ

FINAL SELF-CHECK (MANDATORY):
Before returning SQL:
- Ensure no alias is used outside its scope
- Ensure all columns exist
- Ensure SQL is syntactically complete
- Ensure no invalid identifiers

If any issue exists, FIX it before outputting.

START YOUR RESPONSE NOW:
"""
    
    return prompt
