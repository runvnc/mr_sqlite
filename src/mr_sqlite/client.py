import os
import sqlite3
from typing import Dict, List, Any, Optional, Union, Tuple
import json
from contextlib import contextmanager
from lib.utils.debug import debug_box

from .filters.parser import FilterParser

# Default paths for SQLite database
DEFAULT_DB_DIR = "data/sqlite"
DEFAULT_DB_FILE = "mindroot.db"
DEFAULT_SCHEMA_FILE = "create_db.sql"

class SQLiteClient:
    """Client for interacting with SQLite database with zero data retention."""
    
    _instance = None
    
    @classmethod
    def reset_instance(cls):
        cls._instance = None
    
    @classmethod
    def get_instance(cls, db_path=None, schema_path=None) -> 'SQLiteClient':
        """Get or create a singleton instance of the SQLite client.
        
        Args:
            db_path: Optional path to SQLite database file
            schema_path: Optional path to SQL schema file
        """
        # If we're explicitly requesting a different database, reset the instance
        if cls._instance is not None and db_path is not None and cls._instance.db_path != db_path:
            print(f"Resetting SQLite client instance because db_path changed from {cls._instance.db_path} to {db_path}")
            cls.reset_instance()
        
        # Create a new instance if needed
        if cls._instance is None:
            # Check for in-memory mode environment variable
            in_memory_mode = os.environ.get("SQLITE_IN_MEMORY", "false").lower() == "true"
            
            # Use in-memory database if specified
            if in_memory_mode:
                print("SQLite running in in-memory mode (zero data retention)")
                # Always use the shared memory database in in-memory mode
                db_path = 'file:mindroot_shared_db?mode=memory&cache=shared'
            elif db_path is not None and db_path.startswith('file:'):
                db_path = 'file:mindroot_shared_db?mode=memory&cache=shared'
            # Otherwise use default paths if not specified
            elif db_path is None:
                os.makedirs(DEFAULT_DB_DIR, exist_ok=True)
                db_path = os.path.join(DEFAULT_DB_DIR, DEFAULT_DB_FILE)
                print(f"SQLite using file-based database at {db_path}")
            
            if schema_path is None:
                schema_path = os.path.join(DEFAULT_DB_DIR, DEFAULT_SCHEMA_FILE)
                
            cls._instance = cls(db_path, schema_path)
        return cls._instance
    
    def __init__(self, db_path, schema_path=None):
        """Initialize the SQLite client.
        
        Args:
            db_path: Path to SQLite database file or ':memory:' for in-memory database
            schema_path: Optional path to SQL schema file to initialize the database
        """
        self.db_path = db_path
        self.schema_path = schema_path
        self.conn = None
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize the SQLite database connection and schema. Uses shared memory for in-memory mode."""
        debug_box("SQLITE ---------------------------")
        # Create directory if it doesn't exist (only for file-based databases)
        if self.db_path != ':memory:' and not self.db_path.startswith('file:'):
            db_dir = os.path.dirname(self.db_path)
            os.makedirs(db_dir, exist_ok=True)
        
        # Connect to database
        # Use URI mode for shared memory database
        print(f"Connecting to database: {self.db_path}")
        if self.db_path.startswith('file:'):
            print(f"Connecting to shared memory database: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path, uri=True)
        else:
            self.conn = sqlite3.connect(self.db_path)
            
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")
        # Configure for better performance
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        # Set row factory to return dictionaries
        self.conn.row_factory = self._dict_factory
        
        # Initialize schema if schema file exists
        if self.schema_path and os.path.exists(self.schema_path):
            try:
                with open(self.schema_path, 'r') as f:
                    schema_sql = f.read()
                
                # Execute schema SQL
                if schema_sql.strip():
                    with self.get_cursor() as cursor:
                        cursor.executescript(schema_sql)
                    print(f"Initialized database schema from {self.schema_path}")
            except Exception as e:
                print(f"Error initializing database schema: {e}")
    
    @staticmethod
    def _dict_factory(cursor, row):
        """Convert SQLite row to dictionary."""
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    
    @contextmanager
    def get_cursor(self):
        """Get a database cursor with transaction handling."""
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()
    def query_table(
        self,
        table: str,
        select: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        raw_filters: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query records from a table.
        
        Args:
            table: Name of the table to query
            select: Columns to select (default: "*")
            filters: Dictionary of column-value pairs to filter by
            order: Column to order by (format: "column.asc" or "column.desc")
            limit: Maximum number of records to return
            offset: Number of records to skip
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            List of records matching the criteria
        """
        # Build query
        query = f"SELECT {select} FROM {table}"
        params = []
        where_clauses = []
        
        # Apply simple filters (column = value)
        if filters:
            for column, value in filters.items():
                where_clauses.append(f"{column} = ?")
                params.append(value)
        
        # Apply raw filters
        if raw_filters:
            raw_where, raw_params = FilterParser.parse_raw_filters(raw_filters)
            if raw_where:
                where_clauses.append(raw_where)
                params.extend(raw_params)
        
        # Add WHERE clause if needed
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add ORDER BY
        if order:
            if "." in order:
                col, direction = order.split(".")
                query += f" ORDER BY {col} {'DESC' if direction.lower() == 'desc' else 'ASC'}"
            else:
                query += f" ORDER BY {order}"
        
        # Add LIMIT and OFFSET
        if limit is not None:
            query += f" LIMIT {limit}"
        
        if offset is not None:
            query += f" OFFSET {offset}"
        
        # Execute query
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def insert_record(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert a new record into a table.
        
        Args:
            table: Name of the table to insert into
            data: Dictionary of column-value pairs
            
        Returns:
            The inserted record
        """
        columns = list(data.keys())
        placeholders = ["?"] * len(columns)
        values = [data[col] for col in columns]
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        with self.get_cursor() as cursor:
            cursor.execute(query, values)
            # Get the inserted record
            last_id = cursor.lastrowid
            return self.query_table(table, filters={"rowid": last_id})[0] if last_id else {}
    def update_records(
        self,
        table: str,
        data: Dict[str, Any],
        filters: Dict[str, Any],
        raw_filters: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Update records in a table.
        
        Args:
            table: Name of the table to update
            data: Dictionary of column-value pairs to update
            filters: Dictionary of column-value pairs to filter by
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            Count of updated records
        """
        # Get records before update
        records_before = self.query_table(table, filters=filters, raw_filters=raw_filters)
        
        if not records_before:
            return []
        
        # Build update query
        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        query = f"UPDATE {table} SET {set_clause}"
        params = list(data.values())
        
        where_clauses = []
        
        # Apply simple filters
        if filters:
            for column, value in filters.items():
                where_clauses.append(f"{column} = ?")
                params.append(value)
        
        # Apply raw filters
        if raw_filters:
            raw_where, raw_params = FilterParser.parse_raw_filters(raw_filters)
            if raw_where:
                where_clauses.append(raw_where)
                params.extend(raw_params)
        
        # Add WHERE clause
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Execute update
        # we need to record the number of updated records
        updated_count = 0
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            updated_count = cursor.rowcount
       
        return updated_count

    def delete_records(
        self,
        table: str,
        filters: Dict[str, Any],
        raw_filters: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Delete records from a table.
        
        Args:
            table: Name of the table to delete from
            filters: Dictionary of column-value pairs to filter by
            raw_filters: Raw filters in the format "column.operator.value,column.operator.value"
            
        Returns:
            List of deleted records
        """
        # Get records before deletion
        records_to_delete = self.query_table(table, filters=filters, raw_filters=raw_filters)
        
        if not records_to_delete:
            return []
        
        # Build delete query
        query = f"DELETE FROM {table}"
        params = []
        where_clauses = []
        
        # Apply simple filters
        if filters:
            for column, value in filters.items():
                where_clauses.append(f"{column} = ?")
                params.append(value)
        
        # Apply raw filters
        if raw_filters:
            raw_where, raw_params = FilterParser.parse_raw_filters(raw_filters)
            if raw_where:
                where_clauses.append(raw_where)
                params.extend(raw_params)
        
        # Add WHERE clause
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Execute delete
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
        
        return records_to_delete
    def list_tables(self) -> List[Dict[str, str]]:
        """List all tables in the database.
        
        Returns:
            List of table information dictionaries
        """
        with self.get_cursor() as cursor:
            cursor.execute("SELECT name as table_name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return cursor.fetchall()
    
    def describe_table(self, table: str) -> List[Dict[str, Any]]:
        """Get schema information for a table.
        
        Args:
            table: Name of the table to describe
            
        Returns:
            List of column descriptions
        """
        with self.get_cursor() as cursor:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            # Convert to format compatible with Supabase client
            result = []
            for col in columns:
                result.append({
                    "column_name": col["name"],
                    "data_type": col["type"],
                    "is_nullable": "YES" if col["notnull"] == 0 else "NO",
                    "column_default": col["dflt_value"]
                })
            
            return result
    
    def get_table_relationships(self, table: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get foreign key relationships for a table or all tables.
        
        Args:
            table: Optional name of the table to get relationships for
            
        Returns:
            List of relationship descriptions
        """
        with self.get_cursor() as cursor:
            if table:
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                fks = cursor.fetchall()
                
                # Convert to format compatible with Supabase client
                result = []
                for fk in fks:
                    result.append({
                        "table_name": table,
                        "column_name": fk["from"],
                        "foreign_table_name": fk["table"],
                        "foreign_column_name": fk["to"]
                    })
                
                return result
            else:
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = [row["name"] for row in cursor.fetchall()]
                
                # Get foreign keys for all tables
                result = []
                for t in tables:
                    cursor.execute(f"PRAGMA foreign_key_list({t})")
                    fks = cursor.fetchall()
                    
                    for fk in fks:
                        result.append({
                            "table_name": t,
                            "column_name": fk["from"],
                            "foreign_table_name": fk["table"],
                            "foreign_column_name": fk["to"]
                        })
                
                return result
    def format_schema_for_agent(self, tables_info: Dict[str, Any]) -> str:
        """Format schema information for injection into agent context.
        
        Args:
            tables_info: Dictionary with table schemas and relationships
            
        Returns:
            Formatted schema string
        """
        schema_text = "DATABASE SCHEMA INFORMATION:\n\n"
        
        # Add tables and columns
        for table_name, table_info in tables_info.items():
            schema_text += f"Table: {table_name}\n"
            
            # Add columns
            schema_text += "Columns:\n"
            for column in table_info.get("columns", []):
                nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL"
                default = f" DEFAULT {column.get('column_default')}" if column.get("column_default") else ""
                schema_text += f"  - {column.get('column_name')}: {column.get('data_type')} {nullable}{default}\n"
            
            # Add relationships
            relations = table_info.get("relationships", [])
            if relations:
                schema_text += "Relationships:\n"
                for rel in relations:
                    schema_text += f"  - {rel.get('column_name')} → {rel.get('foreign_table_name')}.{rel.get('foreign_column_name')}\n"
            
            schema_text += "\n"
        
        return schema_text
    
    def execute_sql(self, query: str, params: List[Any] = None, unsafe: bool = False) -> List[Dict[str, Any]]:
        """Execute a raw SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            unsafe: Whether to allow potentially unsafe operations (default: False)
            
        Returns:
            Query results
        """
        if not unsafe:
            # Simple security check to prevent destructive operations
            query_lower = query.lower().strip()
            if any(keyword in query_lower for keyword in ["drop", "truncate", "delete", "update", "alter"]):
                raise ValueError("Potentially destructive SQL operations are not allowed")
        
        with self.get_cursor() as cursor:
            cursor.execute(query, params or [])
            return cursor.fetchall()
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
