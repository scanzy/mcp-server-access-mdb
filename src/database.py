"""Tools for managing database connections and data operations."""

import shutil
import typing as t
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import Context
from fastmcp.exceptions import FastMCPError
from src.notes import ReadNotes



# CONNECTIONS STORAGE AND RETRIEVAL
# =================================


@dataclass
class DBConnection:
    """Dataclass to hold information about a database connection."""

    key: str            # Unique identifier for the connection
    engine: sa.Engine   # SQLAlchemy engine for the connection
    path: str           # Path to the database file



def GetConnection(ctx: Context, key: str) -> DBConnection:
    """Retrieve the DBConnection object for the given key, if it exists."""

    connections = getattr(ctx.fastmcp, "connections", {})
    if key not in connections:
        raise FastMCPError(f"Not connected to the database with key '{key}'. Please use connect first.")
    return connections[key]


def GetEngine(ctx: Context, key: str) -> sa.Engine:
    """Retrieve the SQLAlchemy engine for the given key, if it exists."""
    return GetConnection(ctx, key).engine


def ListConnections(ctx: Context) -> list[dict[str, t.Any]]:
    """List all active database connections, returning key and path for each."""

    connections = getattr(ctx.fastmcp, "connections", {})
    return [{"key": conn.key, "path": conn.path} for conn in connections.values()]



# CONNECTION MANAGEMENT
# =====================



def CreateDatabase(targetPath: str, ctx: Context) -> str:
    """Create a new MS Access database by copying empty.mdb to the specified path."""

    # Ensure the empty template exists
    emptyTemplate = Path(__file__).parent.parent / "empty.mdb"
    if not emptyTemplate.exists():
        raise FastMCPError(f"Template database not found: {emptyTemplate}")

    # Check if the target path is valid and does not already exist
    target = Path(targetPath)
    if target.exists():
        raise FastMCPError(f"Target file already exists: {target}")

    # Creates the database by copying the template
    try:
        shutil.copy(str(emptyTemplate), str(target))
        return f"Database created at {target}"
    except Exception as e:
        raise FastMCPError(f"Failed to create database: {e}")


def Connect(key: str, ctx: Context, databasePath: str = "", readNotes: bool = False) -> str:
    """Connect to a database and store the engine under the given key, for future use.
    If readNotes is True, reads notes associated with the database (same name, with .AInotes.* suffix).
    If you already read the notes, do not read them again to go faster.
    To create a temporary in-memory database, do not specify the databasePath.
    """

    # Check if the key already exists in the engines dictionary
    connections = getattr(ctx.fastmcp, "connections")
    if key in connections:
        raise FastMCPError(f"Database connection with key '{key}' already exists.")

    # If no database path is specified, create an in-memory database
    # This allows us to load CSV data without writing to disk
    if databasePath == "":
        connectionUrl = "sqlite:///:memory:"
    
    # For Microsoft Access files, use the ODBC driver
    elif databasePath.endswith(".mdb") or databasePath.endswith(".accdb"):
        connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={databasePath};"
        connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})
    
    # Handle other unknown file types
    else: raise FastMCPError(f"Unsupported database file extension: {databasePath}")

    try:
        # Create a new SQLAlchemy engine and store it
        engine = sa.create_engine(connectionUrl)

        # test the connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))

        # store the connection
        connections[key] = DBConnection(key=key, engine=engine, path=databasePath)
        message = f"Successfully connected to the database with key '{key}'."
        
        # read notes associated with the database
        if readNotes:
            try:
                notes = ReadNotes(databasePath)
                message += f"\nNotes: {notes}"
            except FastMCPError as e:
                message += f"\nError reading notes: {e}"
        
        return message

    except Exception as e:
        raise FastMCPError(f"Error connecting to database: {str(e)}")


def Disconnect(key: str, ctx: Context) -> str:
    """Disconnect from the MS Access database identified by key."""

    # Ensure the connection exists
    connections = getattr(ctx.fastmcp, "connections", {})
    if key not in connections:
        raise FastMCPError(f"No active database connection with key '{key}' to disconnect.")
    
    # Dispose of the engine
    connections[key].engine.dispose()
    del connections[key]
    return f"Disconnected from the database with key '{key}'."



# DATA MANAGEMENT
# ===============


def Query(key: str, sql: str, ctx: Context, params: dict[str, t.Any] = {}) -> list[dict]:
    """Execute a SELECT query on the database identified by key and return results as a list of records.
    Use backticks to escape table and column names.
    Insert named parameters (:param_name) in the SQL query to avoid SQL injection.
    Pass a dictionary as params to provide values for the SQL query.
    Before executing a query, make sure to know the record count, using SELECT TOP (Access)
    or LIMIT (SQLite) to limit the number of records returned and avoid large responses.
    IMPORTANT: Do not use this tool to discover existing tables, query system objects or schema.
    Instead, ask the user about existing tables, their purpose, structure and content.
    To discover the structure of a table, use SELECT TOP 1 * FROM <table_name>.
    """

    # Use pandas to execute query and convert results to dict format
    # This automatically handles proper data type conversion
    with GetEngine(ctx, key).begin() as conn:
        df = pd.read_sql_query(sa.text(sql), conn, params=params)
        return df.to_dict("records")


def Update(key: str, sql: str, ctx: Context, params: list[dict[str, t.Any]] = []) -> bool:
    """Execute an UPDATE/INSERT/DELETE statement on the database identified by key.
    Use backticks to escape table and column names.
    Insert named parameters (:param_name) in the SQL statement to avoid SQL injection.
    Pass a list of dictionaries as params to provide values for the SQL statement.
    The tool will repeat the statement execution for each dictionary in the list.
    If one statement fails, the entire transaction will be rolled back.
    """

    # Execute the update in a transaction
    # SQLAlchemy automatically commits if no errors occur
    with GetEngine(ctx, key).begin() as conn:
        conn.execute(sa.text(sql), parameters=params)
        return True
