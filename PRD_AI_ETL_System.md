AI-DRIVEN ETL GENERATOR
PRODUCT REQUIREMENTS DOCUMENT


1. EXECUTIVE SUMMARY
1.1 Product Overview
The AI-Driven ETL SQL Generator is a desktop-based application that assists data engineers in generating production-ready Snowflake SQL scripts using natural language requirements. The system supports both locally-hosted and API-based Large Language Models (LLMs) to produce schema-aware, multi-layered ETL SQL code with full human review, optional in-app staged execution, an SQL assistant for post-generation guidance, CSV/API source ingestion into the raw schema, and editable API ingestion scripts before execution.

1.2 Business Objective
The system reduces the time and effort required to write repetitive ETL SQL by automating the generation of staging, transformation, and business logic layers while maintaining full human oversight and control.

1.3 Target Users
• Data Engineers
• Analytics Engineers
• Database Developers
• Technical users with SQL and Snowflake knowledge


2. PROBLEM STATEMENT
Manual ETL SQL development for Snowflake is:
• Time-consuming and repetitive
• Prone to human error in naming conventions and schema references
• Requires deep knowledge of multi-layer data warehousing patterns
• Involves boilerplate code that follows similar patterns across projects

Solution: An intelligent assistant that generates Snowflake-compliant, layered SQL scripts by interpreting natural language requirements and real table schemas, while allowing data engineers to review, edit, and manually execute the output.


3. SYSTEM ARCHITECTURE

3.1 Technology Stack
Frontend/UI: Streamlit (Python-based web framework)
Database: Snowflake Data Warehouse
LLM: Ollama (local inference engine), OpenAI API, and Claude options exposed in the UI
Programming Language: Python 3.10+
Key Libraries:
• streamlit - User interface
• snowflake-connector-python - Snowflake integration
• pandas - API payload normalization and Snowflake ingestion
• ollama - Local LLM client
• openai - OpenAI API client
• python-dotenv - Environment variable management

3.2 Data Flow Architecture

The ETL pipeline follows a four-layer architecture:
AI_ETL_RAW → AI_ETL_STG → AI_ETL_WI → AI_ETL_BR
(Source) → (Staging) → (Work/Intermediate) → (Business)

4. USER WORKFLOW

4.1 Standard User Journey

Application Launch:
User launches the Streamlit app locally.
App loads with Snowflake connection options and source-ingestion controls.

Snowflake Connection Setup
User provides credentials via the UI.
• Enters account, user, password, warehouse (required)
• Optionally enters role, database, schema
• System validates connection and displays success message

Database & Schema Selection
User selects a database from the dropdown.
User selects a schema from the dropdown.
Available tables are fetched and displayed.

Source Ingestion
User can browse Snowflake source tables from the selected database/schema and add them to a persistent source list.
User can also upload a CSV file for ingestion into the selected database's AI_ETL_RAW schema.
For CSV upload, the app validates the file, infers columns, supports an optional custom target table name, and ingests the file as a new raw table.
User can also enter an API URL and token.
The app generates an editable Python ingestion script, allows reset to the auto-generated version, and executes the edited script on demand.
The API ingestion script uses pandas and writes the resulting dataframe to the AI_ETL_RAW schema.

Requirement Input
User describes the ETL task in natural language.
The requirements field is free-text and supports detailed prompt engineering guidance.
The user can open a Prompt Guidelines dialog below the requirements field for structure, joins, dedup rules, KPI formulas, and template examples.

Transform and Business layer target selection
For each target layer, user first chooses one mode:
• Create New Target Table
• Select Existing Target Table
If user selects existing target table, user then selects target database, target schema, and one target table.
If user selects create new, the LLM generates CREATE + MERGE logic for that layer.

LLM Provider Selection
User selects the LLM provider in the UI (ollama, openai, or supported Claude options).
If ollama is selected, the system checks whether the configured OLLAMA_MODEL is installed and blocks generation if not available.

User clicks "Generate"
Timer starts automatically.

Processing Workflow
System validates inputs (requirement not empty, tables selected).
System validates all selected source tables exist in Snowflake across selected databases/schemas.
System fetches column-level schema for each selected source table.
System constructs a comprehensive LLM prompt with schema context.
System sends the prompt to the selected LLM provider.
System extracts and parses SQL sections from the LLM response.
System runs an LLM-based validation pass that scores the generated SQL and reports checks.
Timer stops and displays elapsed time in mm:ss format.

Output Review & Edit
Generated SQL is displayed in 3 tabs:
• Staging SQL: incremental load patterns
• Transform SQL: data transformation logic
• Business SQL: aggregation and reporting logic
All SQL is editable directly in the UI and persists in session state until regenerated.

Save Output
User can save outputs locally or save outputs in GitHub.
Local save writes staging.sql, transform.sql, and business.sql into a timestamped folder in the project root.
GitHub save writes the same files into an etl_outputs folder, commits them, and pushes to the main branch.

SQL Assistant (Post-Generation)
After SQL is generated, user can open the SQL Assistant dialog.
User asks questions about generated SQL (logic, joins, mappings, syntax intent).
Assistant uses the generated Staging/Transform/Business SQL as context.
Assistant responses are generated by the currently selected LLM provider.

Pipeline Execution (Optional)
User checks the execution confirmation checkbox.
User clicks "Execute ETL Pipeline".
System executes layers sequentially: Staging → Transform → Business.
Execution stops immediately if any layer fails.
Success/failure feedback is shown for each layer and for the overall pipeline.


5. FUNCTIONAL REQUIREMENTS

FR-1: Snowflake Connection Management
FR-1.1 Custom Connection Mode
• UI displays credential input form with 7 fields: Account (required), User (required), Password (required, masked), Warehouse (required), Role (optional), Database (optional), Schema (optional)
• Credentials stored only in session state (ephemeral)
• Credentials never written to disk
• Connection validated on submission

FR-1.2 Connection Validation
• Display connection status (success/failure)
• Show user-friendly error messages for connection failures
• Handle authentication errors gracefully

FR-2: Database & Schema Discovery
FR-2.1 Database Selection (Custom connection mode only)
• Fetch all accessible databases using SHOW DATABASES
• Display databases in a dropdown selector
• Cache database list in session state
• Allow single database selection

FR-2.2 Schema Selection (Custom connection mode only)
• Fetch schemas for selected database using SHOW SCHEMAS IN DATABASE
• Display schemas in a dropdown selector
• Update schema list when database changes
• Cache schema list per database in session state
• Allow single schema selection

FR-2.3 Table Discovery
• Fetch all tables from the currently selected database and schema
• Query INFORMATION_SCHEMA.TABLES
• Display tables sorted alphabetically
• Support multi-select for source-table browsing and single-select for existing target-table selection

FR-3: Requirement Input
FR-3.1 Natural Language Input
• Free-text area for ETL requirements
• Minimum height: 200px
• No maximum length restriction
• Placeholder text guides user input

FR-3.2 Input Validation
• Requirement must not be empty
• Whitespace-only input rejected
• Error message displayed if validation fails

FR-4: Source Table Selection
FR-4.1 Table Multiselect
• Display tables from currently selected database/schema for browsing
• Allow selection of 1 to N tables per browse action
• Add selected tables into a persistent source list
• Allow selected source list to contain tables from multiple databases/schemas
• Store selected source tables in fully-qualified format: DATABASE.SCHEMA.TABLE

FR-4.2 Table Validation
• Verify existence of each selected source table in its selected database/schema
• Query INFORMATION_SCHEMA.TABLES per database/schema group for validation
• Case-insensitive table name matching
• Display specific error if any table is invalid
• Block generation if validation fails

FR-4.3 Target Table Selection
• For Transform and Business layers, provide radio mode selection:
	- Create New Target Table
	- Select Existing Target Table
• If existing-table mode is selected, user must select one database, one schema, and one table for that layer
• If create-new mode is selected, no table selection is required for that layer

FR-4.4 External Source Ingestion
• CSV upload is validated before ingestion: .csv extension, non-empty file, header row, no duplicate normalized column names, and at least one data row
• CSV files are ingested into the selected database's AI_ETL_RAW schema with an optional custom table name
• API source ingestion accepts an API URL and token, fetches JSON payloads, normalizes them into pandas DataFrames, and ingests them into AI_ETL_RAW
• The API ingestion script is editable before execution and can be reset to the auto-generated version
• Ingested source tables are added to the persistent source-table list for downstream generation

FR-5: Schema Extraction
FR-5.1 Column-Level Metadata Fetching
• For each selected table, fetch: Column name and Data type
• Query INFORMATION_SCHEMA.COLUMNS
• Maintain column ordinal position
• Handle all Snowflake data types

FR-5.2 Schema Context Preparation
• Build structured schema dictionary
• Format: {table_name: [{column_name, data_type}]}
• Include all columns for each table
• Pass complete schema to LLM

FR-6: LLM Prompt Construction
FR-6.1 Prompt Components
• User requirement (natural language)
• List of source table names
• Complete table schemas with columns and data types
• The prompt in prompt_builder.py
• Data architecture pattern (4-layer: RAW → STG → WI → BR)
• Layer-specific generation rules: Staging (4-step incremental load pattern per table), Transform (Joins and derived columns), Business (Aggregations and KPIs)
• Output format instructions with delimiters
• SQL best practices and constraints

FR-6.2 Prompt Engineering
• Explicitly forbid placeholders and ellipses
• Require full, executable SQL
• Request proper use of incremental columns from the schema
• Demand Snowflake-specific syntax
• Prohibit LLM from asking clarifying questions (best-effort)

FR-6.3 Prompt Guidance UI
• Provide an in-app Prompt Guidelines dialog below the requirements field
• Include guidance for business objective, target grain, merge key, join rules, dedup rules, KPI definitions, and prompt quality checklist
• Provide a reusable prompt template to help users structure requirements for better SQL generation

FR-7: SQL Generation
FR-7.1 LLM Invocation
• Send constructed prompt to selected provider (ollama, openai, or Claude options exposed in the UI)
• Ollama model configured via OLLAMA_MODEL; host via OLLAMA_HOST (default: http://localhost:11434)
• OpenAI model configured via OPENAI_MODEL (default: gpt-4.1)
• OpenAI API key configured via OPENAI_API_KEY
• Handle LLM response errors gracefully

FR-7.1.1 Ollama Model Availability Check
• When provider is ollama, system checks whether configured OLLAMA_MODEL exists on user machine
• If model is not found, system shows warning and blocks generation
• Model-name matching supports base-name and :latest variants

FR-7.2 SQL Extraction & Parsing
• Extract three distinct SQL sections using regex delimiters
• Remove markdown code fences
• Trim whitespace
• Validate all sections are non-empty
• Raise error if any section missing

FR-7.6 LLM Validation Scoring
• After SQL generation, run a second LLM pass that evaluates the output against the user requirement and source schemas
• Produce a structured confidence score, summary, check list, and missing-items list
• Parse the validation response from JSON and display the result in the UI
• Keep the validation report visible alongside the editable SQL output

FR-7.3 Staging SQL Patterns
• One staging script per source table
• 4-step incremental load pattern:

CREATE TABLE IF NOT EXISTS (LIKE source)
INSERT INTO DATALOAD (metadata tracking)
INSERT OVERWRITE (incremental records based on watermark)
UPDATE DATALOAD (update watermark)
• Uses actual columns from fetched schema
• Fully qualified table names

FR-7.4 Transform SQL Patterns
• Reads only from AI_ETL_STG.STG_* tables
• Performs joins, filters, and derived columns
• Writes to AI_ETL_WI schema

FR-7.5 Business SQL Patterns
• Reads only from AI_ETL_WI tables
• Performs aggregations, window functions, and KPIs
• Writes to AI_ETL_BR schema
• Uses CREATE OR REPLACE TABLE

FR-8: Output Display & Editing
FR-8.1 Tabbed Output Interface
• Three tabs for three SQL layers: "Staging SQL", "Transform SQL", "Business SQL"
• Each tab contains a text area (400px height)
• All SQL is editable post-generation
• Edits persist in session state until new generation

FR-8.2 Output Persistence
• SQL stored in st.session_state
• Persists across Streamlit reruns
• Tabs remain visible after generation
• Only cleared on new generation or app reload

FR-8.3 Validation Report Display
• Display the validation score as a confidence metric
• Show per-check PASS/PARTIAL/FAIL results
• Show missing or weak items when present

FR-9: Performance Tracking
FR-9.1 Generation Timer
• Timer starts on "Generate" button click
• Timer stops after SQL extraction completes
• Elapsed time calculated using time.perf_counter()
• Time displayed in mm:ss format
• Displayed immediately after generation completes

FR-9.2 Error-Case Timing
• Timer also stops on validation or generation errors
• Displays "Time elapsed" instead of "Generation time"
• Helps users understand performance even on failure

FR-10: Save Functionality
FR-10.1 Folder Naming
• Auto-generate folder name from requirement text
• Sanitize special characters
• Replace spaces with underscores
• Limit length to 50 characters
• Convert to lowercase
• Append counter if folder exists (_1, _2, etc.)

FR-10.2 File Writing
• Create three .sql files in generated folder: staging.sql, transform.sql, business.sql
• Use UTF-8 encoding
• Write edited SQL content (not original)
• Display success message with full folder path

FR-10.3 Save Validation
• Only enabled after successful generation
• Handle file system errors gracefully
• Display user-friendly error messages

FR-10.4 GitHub Save
• Save outputs in a project-level etl_outputs folder before pushing
• Commit the generated output folder to the main branch
• Push the commit to origin/main
• Display an error if the current branch is not main or if git push fails

FR-11: SQL Assistant
FR-11.1 Assistant Availability
• "Open SQL Assistant" button is available only after SQL generation output exists
• Assistant opens in a dialog without navigating away from the current page

FR-11.2 Assistant Context and Behavior
• Assistant prompt includes generated Staging, Transform, and Business SQL sections
• Assistant uses currently selected LLM provider for responses
• Conversation history is retained in session state during the current app session
• Assistant is read-only with respect to SQL execution (no direct query execution)

FR-12: In-App SQL Execution
FR-12.1 Execution Trigger and Confirmation
• "Execute ETL Pipeline" is available only after SQL generation
• User must explicitly confirm execution via checkbox before execution can start

FR-12.2 Execution Order and Failure Handling
• SQL execution runs in fixed order: Staging, then Transform, then Business
• If any stage fails, subsequent stages are not executed
• User receives stage-level success messages and a final pipeline status

FR-12.3 Statement Execution Engine
• Multi-statement SQL text is split by semicolon boundaries
• Statement splitting ignores semicolons inside single or double quoted strings
• Statements are executed sequentially using Snowflake cursor.execute()
• Empty SQL payloads are rejected with clear validation errors


6. NON-FUNCTIONAL REQUIREMENTS

NFR-1: Performance
• Connection establishment: < 5 seconds
• Table list fetch: < 3 seconds
• Schema fetch for 10 tables: < 5 seconds
• LLM generation: Variable (depends on model, typically 10-60 seconds)
• UI responsiveness: Immediate feedback for all actions

NFR-2: Usability
• Single-page UI (no navigation required)
• Progressive disclosure (conditional UI elements)
• Clear visual hierarchy
• Descriptive error messages
• Tooltips and placeholder text for guidance

NFR-3: Reliability
• Graceful error handling at every step
• No silent failures
• Connection errors do not crash the app
• Invalid input blocked before processing

NFR-4: Security
• Credentials never logged to console or files
• Password fields masked in UI
• Session-only credential storage (cleared on reload)
• No credential transmission outside local network

NFR-5: Portability
• Runs with local provider (Ollama) or API-based provider (OpenAI)
• No Docker required
• Cross-platform (Windows, macOS, Linux)
• Single virtual environment installation

NFR-6: Maintainability
• Modular architecture with clear separation of concerns
• Readable, commented code
• Standard Python project structure


7. ERROR HANDLING & VALIDATION

7.1 Input Validation Errors

Error Condition | User Message | Action
Empty requirement | "Please enter an ETL requirement" | Block generation
No tables selected | "Please select at least one source table" | Block generation
Missing credentials (custom mode) | "Please fill in all required Snowflake credentials" | Show warning, block connection

7.2 Connection Errors

Error Condition | User Message | Action
Invalid credentials | "Snowflake connection failed: [error details]" | Stop execution, allow retry
Network timeout | "Failed to connect to Snowflake: [error details]" | Stop execution, allow retry
Invalid account/warehouse | "Snowflake connection failed: [error details]" | Stop execution, display details

7.3 Validation Errors

Error Condition | User Message | Action
Table does not exist | "Validation error: [table_names] do not exist in selected database/schema" | Display elapsed time, block generation
Invalid schema reference | Connection or query error message | Stop execution

7.4 Generation Errors

Error Condition | User Message | Action
LLM unavailable | "Failed to generate LLM response: [error details]" | Display elapsed time, stop execution
Missing SQL section | "Could not find '[Section Name] SQL' in LLM response" | Display error, stop execution
Empty SQL section | "[Section Name] SQL section is empty after extraction" | Display error, stop execution

7.5 Validation Report Errors

Error Condition | User Message | Action
Validation JSON parse failure | "Validation parser could not read structured response." | Display default validation report with score 0
Validation service failure | "Validation error: [error details]" | Display error and keep generated SQL visible

7.6 File System Errors

Error Condition | User Message | Action
Permission denied | "Failed to save files: [error details]" | Display error, files not saved
Disk full | "Failed to save files: [error details]" | Display error, files not saved
Git commit or push failure | "Failed to save outputs in GitHub: [error details]" | Display error, files not saved or pushed

7.7 Assistant Errors

Error Condition | User Message | Action
Provider call failure in assistant | "Chat error: [error details]" | Display error in dialog; keep session chat history

7.8 Execution Errors

Error Condition | User Message | Action
Execution without confirmation | "Please confirm execution before running the ETL pipeline" | Block execution
Missing SQL section before execution | "[Layer] SQL is missing" | Block execution
Statement execution failure | "ETL execution failed: [error details]" | Stop current run and do not execute remaining layers


8. SECURITY CONSIDERATIONS

8.1 Credential Management
• Storage: Environment variables (.env) or session state only
• Transmission: Snowflake over network; OpenAI requests over API endpoint when OpenAI provider is selected
• Logging: Credentials never logged or displayed in plain text (except password field masking)
• Persistence: Custom credentials cleared on app reload

8.2 SQL Injection Prevention
• User input (requirement) does not directly execute SQL
• Table names validated against Snowflake metadata
• Generated SQL reviewed by user before optional in-app execution
• Execution requires explicit user confirmation in UI

8.3 LLM Security
• If Ollama is selected, LLM runs locally
• If OpenAI is selected, prompt data is sent to external API endpoint configured by environment variables
• Prompt contains only table schemas (no sensitive data)
• Generated SQL reviewed before execution

8.4 File System Security
• Files written to user-controlled directory
• No arbitrary file path injection
• Folder names sanitized before creation
• GitHub save uses the local repository and existing git remote configuration


9. OUT-OF-SCOPE ITEMS

The following features are explicitly NOT included in the current implementation:

9.1 Advanced SQL Execution Features
Automatic execution without explicit user confirmation
Query result preview grids
Execution log persistence and audit trail
Transactional rollback orchestration across layers

9.2 Code Validation
SQL syntax validation before display
Snowflake compatibility checking
Performance optimization analysis

9.3 Version Control
SQL version history
Change tracking


9.4 Python Code Generation
General-purpose Python ETL orchestration code generation
Airflow/dbt integration
Orchestration code

9.5 Advanced UI Features
Multi-language support
Keyboard shortcuts

9.6 Collaboration
Multi-user support
Shared workspaces
Output sharing/export to cloud

9.7 Analytics
Usage tracking
Generation history
Success/failure metrics


10. FUTURE ENHANCEMENTS

10.1 Short-Term (Low-Complexity)
• Add "Copy to Clipboard" buttons for each SQL section
• Add SQL syntax highlighting in output
• Add "Clear All" button to reset form
• Add "Dark Mode” toggle button

10.2 Medium-Term (Moderate-Complexity)
• SQL syntax validation using Snowflake SQL parser
• Expand provider support beyond current options (for example, Anthropic, Azure OpenAI)
• Support for additional flat file types such as XLSX

10.3 Long-Term (High-Complexity)
• Transactional SQL execution with rollback capability
• Multi-step wizard for complex ETL pipelines
• AI-powered SQL optimization suggestions


11. ASSUMPTIONS & CONSTRAINTS

11.1 Technical Assumptions
• Ollama is installed and running locally
• User has configured a model in Ollama (e.g., llama3, codellama)
• For OpenAI provider usage: OPENAI_API_KEY is configured and network access to OpenAI API is available
• Snowflake account is accessible from user's network
• Python 3.10+ is installed
• pandas is installed in the active environment for API source ingestion
• Git is installed and the repository has a configured origin remote for GitHub save

11.2 User Assumptions
• User has basic SQL knowledge
• User understands Snowflake layered architecture concepts
• User will manually review all generated SQL before execution
• User has appropriate Snowflake permissions (read metadata, execute SQL)

11.3 Data Assumptions
• Source tables exist in Snowflake
• Schema metadata is accurate and up-to-date
• Tables contain appropriate incremental columns (timestamp, date, etc.)
• Table relationships are understandable from schema

11.4 Constraints
• Execution is Snowflake-only and user-triggered from UI; no background scheduler/orchestrator
• Desktop app usage model: no web-hosted SaaS deployment in current scope
• Single-user: No concurrent user support
• Snowflake-only: No support for other data warehouses (Redshift, BigQuery, etc.)
• English-only: LLM prompts and UI in English


12. SUCCESS CRITERIA

The system is considered successful if:
User can connect to Snowflake using custom credentials
User can browse and select databases, schemas, and tables
User can select source tables across multiple databases/schemas
User can upload CSV files and ingest them into AI_ETL_RAW with optional custom table names
User can ingest API data into AI_ETL_RAW using an editable pandas-based script
User can input natural language ETL requirements
User can open the Prompt Guidelines dialog and use the prompt template to improve requirements
User can choose target mode (create new or existing) for transform and business layers
User can select one existing target table per layer when existing-table mode is chosen
User can choose LLM provider (ollama or openai)
System verifies configured Ollama model availability when ollama is selected
System validates table existence before generation
System fetches accurate schema metadata from Snowflake
LLM generates valid, schema-aware SQL in 3 layers
Generated SQL is editable in the UI
System displays an LLM-based validation score and check report for generated SQL
User can ask follow-up SQL questions through SQL Assistant using generated SQL context
User can execute generated SQL pipeline in-app with explicit confirmation and staged feedback
User can save SQL to disk locally or save and push outputs to GitHub
System displays generation time for performance awareness
All errors are handled gracefully with user-friendly messages


13. PROJECT DELIVERABLES

13.1 Software Deliverables
• Fully functional Streamlit application (app.py)
• Modular codebase with clear separation of concerns
• Configuration management system (.env support)
• Requirements file (requirements.txt)

13.2 Documentation Deliverables
• README with setup and usage instructions
• Product Requirements Document (this document)
• Inline code comments and docstrings

13.3 Supporting Files
• .env.example template (if applicable)
• Project context and architecture documentation


14. STAKEHOLDERS

Data Engineers | Primary users; use generated SQL in production ETL workflows
Analytics Engineers | Secondary users; generate reporting and business layer SQL
Project Supervisor | Internship evaluation and guidance
Academic Evaluators | College assessment and grading


15. CONCLUSION

The AI-Driven ETL SQL Generator successfully addresses the challenge of repetitive SQL development by combining real-time schema awareness with AI-powered code generation. The system maintains a strong emphasis on human oversight, security, and flexible provider execution (local Ollama or OpenAI API), making it suitable for enterprise data environments while providing a streamlined developer experience.

The modular architecture, clear separation between UI and business logic, and comprehensive error handling ensure the system is maintainable, extensible, and suitable for academic demonstration and professional internship documentation.