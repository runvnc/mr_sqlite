from lib.providers.services import service
from lib.providers.commands import command
from lib.pipelines.pipe import pipe
import asyncio
from typing import Dict, List, Any, Optional, Union
import os
import json
import traceback
from .client import SQLiteClient, DEFAULT_DB_DIR, DEFAULT_DB_FILE, DEFAULT_SCHEMA_FILE
from .utils import (
    load_agent_db_settings,
    extract_schema_info,
    clean_db_schema_from_messages,
    DB_SCHEMA_START_DELIMITER,
    DB_SCHEMA_END_DELIMITER,
    format_error_response
)
from lib.utils.debug import debug_box

# Create a global lock for database write operations
db_write_lock = asyncio.Lock()

# Initialize SQLite client service
@service()
async def get_db_client(db_path=None, schema_path=None):
    # Force shared memory database for consistency
    """Get the SQLite client instance.
    
    Args:
        db_path: Optional path to SQLite database file
        schema_path: Optional path to SQL schema file
    """
    try:
        in_memory_mode = os.environ.get("SQLITE_IN_MEMORY", "false").lower() == "true"
         
        if db_path is None:
            if in_memory_mode:
                db_path = 'file:mindroot_shared_db?mode=memory&cache=shared'
            else: 
                db_path = os.path.join(DEFAULT_DB_DIR, DEFAULT_DB_FILE)

        return SQLiteClient.get_instance(db_path, schema_path)
    except Exception as e:
        print(f"Error initializing SQLite client: {e}")
        traceback.print_exc()
        return None

# Helper function to get all table names
async def get_all_table_names(db_client=None) -> List[str]:
    """Get all table names from the database.
    
    Args:
        db_client: SQLite client instance
        
    Returns:
        List of table names
    """
    try:
        if db_client:
            tables = db_client.list_tables()
            if tables and isinstance(tables[0], dict) and 'table_name' in tables[0]:
                return [t.get('table_name') for t in tables if t.get('table_name')]
            return tables
        return []
    except Exception as e:
        print(f"Error getting table names: {e}")
        return []
        
# Service to inject schema info
@service()
async def db_inject_schema_info(agent_name: str, tables: List[str] = None):
    """Inject database schema information into agent context.

    Args:
        agent_name: Name of the agent
        tables: Optional list of tables to include (if None, uses agent settings)

    Returns:
        Formatted schema information
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # If no tables specified, load from agent settings
        if tables is None:
            settings = load_agent_db_settings(agent_name)
            enabled_tables = settings.get("enabled_tables", [])
            tables = enabled_tables if enabled_tables else None

        if not tables:
            # Get all tables instead of returning None
            tables = await get_all_table_names(db_client)
            if not tables:
                debug_box("No tables found in database")
                return "No tables found in database."
            else:
                debug_box(f"Found {len(tables)} tables in database")

        # Get schema information for each table
        debug_box(f"Getting schema info for {len(tables)} tables")
            
        tables_info = {}
        tables_with_errors = []
        
        for table in tables:
            debug_box(f"Getting schema for table: {table}")
            columns = None
            relationships = None
            
            # Get columns
            try:
                columns = db_client.describe_table(table)
                relationships = db_client.get_table_relationships(table)
            except Exception as e:
                print(f"Error getting schema for table {table}: {e}")
                tables_with_errors.append(table)
                continue
            
            # Only add table info if we successfully got columns
            if columns is not None:
                tables_info[table] = {
                    "columns": columns,
                    "relationships": relationships or []
                }
                
        # Check if we got any table information successfully
        if not tables_info:
            debug_box(f"Failed to get schema for any tables. Errors in {len(tables_with_errors)} tables.")
            if tables_with_errors:
                debug_box(f"Tables with errors: {', '.join(tables_with_errors)}")
            return "Could not retrieve schema information for any tables."

        # Format schema information
        return db_client.format_schema_for_agent(tables_info)
            
    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error injecting schema info: {str(e)}\n{trace}")
        return None

# DB Commands
@command()
async def query_db(table: str, select: str = "*", filters: Dict[str, Any] = None, 
                  order: str = None, limit: int = None, offset: int = None,
                  raw_filters: str = None, context=None):
    """Query records from a database table.

    Args:
        table: Name of the table to query
        select: Columns to select (default: "*")
        filters: Dictionary of column-value pairs to filter by
        order: Column to order by (format: "column.asc" or "column.desc")
        limit: Maximum number of records to return
        offset: Number of records to skip
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"
                    This provides more advanced filtering than the simple filters parameter.

    Example:
        {"query_db": {"table": "users", "select": "*", "filters": {"role": "admin"}, "limit": 10}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        results = db_client.query_table(
            table=table,
            select=select,
            filters=filters,
            order=order,
            limit=limit,
            offset=offset,
            raw_filters=raw_filters
        )

        # Format results
        if not results:
            return f"No records found in table '{table}' matching the criteria."

        # Return as formatted string (easier for AI to read)
        formatted_results = json.dumps(results, indent=2)
        return f"Query results from '{table}':\n\n```json\n{formatted_results}\n```"

    except Exception as e:
        return format_error_response(e)

@command()
async def insert_db(table: str, data: Dict[str, Any], context=None):
    """Insert a new record into a database table.

    Args:
        table: Name of the table to insert into
        data: Dictionary of column-value pairs

    Example:
        {"insert_db": {"table": "tasks", "data": {"title": "New task", "status": "pending"} } }

    WARNING: Be VERY careful with escaping in the data field. Note that this has to be valid
    JSON. Don't include unnecessary newlines/indendation, and make sure that strings are properly
    escapped!
 
    """
    # This is a write operation, so we need to acquire the lock
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # Use the lock to ensure only one write operation happens at a time
        async with db_write_lock:
            result = db_client.insert_record(table=table, data=data)

        if not result:
            return f"Record was inserted into '{table}', but no data was returned."

        formatted_result = json.dumps(result, indent=2)
        return f"Successfully inserted record into '{table}':\n\n```json\n{formatted_result}\n```"

    except asyncio.CancelledError:
        return "Operation was cancelled while waiting for database lock."
    except Exception as e:
        return format_error_response(e)

@command()
async def update_db(table: str, data: Dict[str, Any], filters: Dict[str, Any] = None, 
                   raw_filters: str = None, context=None):
    """Update existing records in a database table.

    Args:
        table: Name of the table to update
        data: Dictionary of column-value pairs to update
        filters: Dictionary of column-value pairs to filter by using equality (column = value)
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"

    Example:
        { "update_db":
            {  "table": "tasks", "data": {"status": "completed"}, 
               "filters": {"id": 123 }
            }
        }

    WARNING: Be VERY careful with escaping in the data field. Note that this has to be valid
    JSON. Don't include unnecessary newlines/indendation, and make sure that strings are properly
    escaped!

    REMINDER: count the number of curly braces!

    Warning: Multiline String Handling in update_db or insert_db

    When updating fields that contain multiline text (e.g., summaries, notes, or quotes), you must ensure the string is properly escaped to conform to JSON standards. Failure to do so will result in parsing errors.

    DO NOT include actual newline characters in the JSON string.
    In strings, DO encode newlines as \n (a single backslash followed by 'n').
    In strings, DO escape internal double quotes as ".
    DO NOT use unescaped multiline formatting or raw line breaks.

    This applies to all string fields in update_db that may contain multiple paragraphs or formatted text.
    """
    # This is a write operation, so we need to acquire the lock
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # Use the lock to ensure only one write operation happens at a time
        rowcount = 0
        async with db_write_lock:
            rowcount = db_client.update_records(
                table=table,
                data=data,
                filters=filters or {},
                raw_filters=raw_filters
            )

        if rowcount == 0:
            return f"No records in '{table}' were updated matching the filter criteria."

        return f"Successfully updated {rowcount} record(s) in '{table}'"

    except asyncio.CancelledError:
        return "Operation was cancelled while waiting for database lock."
    except Exception as e:
        return format_error_response(e)

@command()
async def delete_db(table: str, filters: Dict[str, Any] = None, 
                   raw_filters: str = None, context=None):
    """Delete records from a database table.

    Args:
        table: Name of the table to delete from
        filters: Dictionary of column-value pairs to filter by using equality (column = value)
        raw_filters: Comma-separated list of raw filter expressions in the format 
                    "column.operator.value". Supports all Supabase filter operators.
                    Example: "status.eq.active,created_at.gt.2025-01-01,email.like.%example.com"

    Example:
        {"delete_db": {"table": "tasks", "filters": {"id": 123}}}
    """
    # This is a write operation, so we need to acquire the lock
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"

        # Use the lock to ensure only one write operation happens at a time
        async with db_write_lock:
            results = db_client.delete_records(
                table=table,
                filters=filters or {},
                raw_filters=raw_filters
            )

        if not results:
            return f"No records in '{table}' were deleted matching the filter criteria."

        count = len(results)
        formatted_results = json.dumps(results, indent=2)
        return f"Successfully deleted {count} record(s) from '{table}':\n\n```json\n{formatted_results}\n```"

    except asyncio.CancelledError:
        return "Operation was cancelled while waiting for database lock."
    except Exception as e:
        return format_error_response(e)
@command()
async def list_db_tables(context=None):
    """List all available tables in the database.

    Example:
        {"list_db_tables": {}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"
            
        tables = db_client.list_tables()

        if not tables:
            return "No tables found in the database."

        # Extract table names from table information
        if isinstance(tables[0], dict) and 'table_name' in tables[0]:
            table_names = [t.get('table_name') for t in tables if t.get('table_name')]
        else:
            table_names = tables

        return "Available tables in database:\n\n" + "\n".join([f"- {table}" for table in table_names])

    except Exception as e:
        return format_error_response(e)

@command()
async def describe_db_table(table: str, context=None):
    """Get detailed schema information for a specific table.

    Args:
        table: Name of the table to describe

    Example:
        {"describe_db_table": {"table": "users"}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"
            
        # Get columns
        columns = db_client.describe_table(table)
        # Get relationships
        relationships = db_client.get_table_relationships(table)

        if not columns:
            return f"Table '{table}' not found or has no columns."

        # Format schema information
        output = f"Schema for table '{table}':\n\n"

        # Add columns
        output += "Columns:\n"
        for col in columns:
            nullable = "NULL" if col.get("is_nullable") == "YES" else "NOT NULL"
            default = f" DEFAULT {col.get('column_default')}" if col.get("column_default") else ""
            output += f"  - {col.get('column_name')}: {col.get('data_type')} {nullable}{default}\n"

        # Add relationships
        if relationships:
            output += "\nRelationships:\n"
            for rel in relationships:
                output += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"

        return output

    except Exception as e:
        return format_error_response(e)
@command()
async def get_db_relationships(table: str = None, context=None):
    """Get information about relationships between tables.

    Args:
        table: Optional name of the table to get relationships for

    Example:
        {"get_db_relationships": {"table": "posts"}}
    """
    try:
        db_client = await get_db_client()
        if not db_client:
            return "Error: Database client unavailable"
            
        relationships = db_client.get_table_relationships(table)

        if not relationships:
            if table:
                return f"No relationships found for table '{table}'."
            else:
                return "No relationships found in the database."

        # Format relationships
        output = f"Relationships{' for table ' + table if table else ''}:\n\n"

        current_table = None
        for rel in relationships:
            table_name = rel.get('table_name')
            if table_name != current_table:
                current_table = table_name
                output += f"Table '{table_name}':\n"

            output += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"

        return output

    except Exception as e:
        return format_error_response(e)

# Pipe to inject schema info into the first system message
@pipe(name='filter_messages', priority=10)
async def inject_db_schema(data: dict, context=None) -> dict:
    """Inject database schema information into the system message."""
    try:
        debug_box("Starting inject_db_schema pipe")

        # Skip if no messages
        if 'messages' not in data or not isinstance(data['messages'], list) or not data['messages']:
            debug_box("Aborting schema injection, missing messages")
            return data

        has_system_message = data['messages'] and data['messages'][0]['role'] == 'system'

        # Get agent name from context
        try:
            agent_name = context.agent_name
            if not agent_name:
                debug_box("Aborting inject schema because no agent name")
                return data
        except Exception as e:
            print(f"Error accessing agent_name from context: {e}")
            return data

        # Load agent DB settings
        settings = load_agent_db_settings(agent_name)
        enabled_tables = settings.get("enabled_tables", [])
        
        # Check if schema information already exists in system message
        schema_exists = False
        if has_system_message and isinstance(data['messages'][0].get('content'), str):
            system_content = data['messages'][0].get('content', '')
            schema_exists = DB_SCHEMA_START_DELIMITER in system_content and DB_SCHEMA_END_DELIMITER in system_content
        
        debug_box(f"Schema exists in system message: {schema_exists}")

        # Only query database for schema if it doesn't already exist in system message
        schema_info = None
        if not schema_exists:
            # If no tables are specifically enabled, we'll get all tables
            tables_to_use = enabled_tables if enabled_tables else None
            schema_info = await db_inject_schema_info(agent_name, tables_to_use)
            debug_box(f"Generated schema info: {schema_info is not None}")

        # Skip if no schema info
        if not schema_info:
            debug_box("No schema info generated")
            return data

        # Add schema info to system message (first message)
        if has_system_message:
            system_msg = data['messages'][0]
            debug_box("Adding schema to system message")
            
            # Add delimited schema info
            delimited_schema = f"\n\n{DB_SCHEMA_START_DELIMITER}\n{schema_info}\n{DB_SCHEMA_END_DELIMITER}"

            if isinstance(system_msg.get('content'), str):
                system_msg['content'] += delimited_schema
                debug_box("Added schema to system message content")
            elif isinstance(system_msg.get('content'), list):
                system_msg['content'][0]['text'] += "\n\n" + delimited_schema

            debug_box("Schema injection complete")
        else:
            debug_box("No system message to add schema to")

        return data

    except Exception as e:
        trace = traceback.format_exc()
        print(f"Error in inject_db_schema pipe: {str(e)}\n{trace}")
        return data
