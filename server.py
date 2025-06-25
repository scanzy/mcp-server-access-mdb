"""MCP server for Microsoft Access databases and CSV files."""

import shutil
from typing import Any, Literal
from pathlib import Path
from dataclasses import dataclass, field

import csv
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError


# Initialize the MCP server for protocol-level communication
mcp = FastMCP("MS Access Databases and CSV Files", dependencies=["pandas", "sqlalchemy-access"])

# Set up a dictionary to hold DBConnection objects for different database connections
setattr(mcp, "connections", {})



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


@mcp.tool(name="list")
def ListConnections(ctx: Context) -> list[dict[str, Any]]:
    """List all active database connections, returning key and path for each."""

    connections = getattr(ctx.fastmcp, "connections", {})
    return [{"key": conn.key, "path": conn.path} for conn in connections.values()]



# CONNECTION MANAGEMENT
# =====================



@mcp.tool(name="create")
def CreateDatabase(targetPath: str, ctx: Context) -> str:
    """Create a new MS Access database by copying empty.mdb to the specified path."""

    # Ensure the empty template exists
    emptyTemplate = Path(__file__).parent / "empty.mdb"
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


@mcp.tool(name="connect")
def Connect(key: str, databasePath: str, ctx: Context) -> str:
    """Connect to a database and store the engine under the given key, for future use.
    If the databasePath is not specified, a new in-memory database will be created.
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
        connections[key] = DBConnection(key=key, engine=engine, path=databasePath)
        return f"Successfully connected to the database with key '{key}'."

    except Exception as e:
        raise FastMCPError(f"Error connecting to database: {str(e)}")


@mcp.tool(name="disconnect")
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


@mcp.tool(name="query")
def Query(key: str, sql: str, ctx: Context, parameters: dict | None = None) -> list[dict]:
    """Execute a SELECT query on the database identified by key and return results as a list of records."""

    # Use pandas to execute query and convert results to dict format
    # This automatically handles proper data type conversion
    with GetEngine(ctx, key).begin() as conn:
        df = pd.read_sql_query(sa.text(sql), conn, params=parameters or {})
        return df.to_dict("records")


@mcp.tool(name="update")
def Update(key: str, sql: str, ctx: Context, parameters: dict | None = None) -> bool:
    """Execute an UPDATE/INSERT/DELETE query on the database identified by key."""

    # Execute the update in a transaction
    # SQLAlchemy automatically commits if no errors occur
    with GetEngine(ctx, key).begin() as conn:
        conn.execute(sa.text(sql), parameters or {})
        return True


@mcp.tool(name="import")
def Import(key: str, tableName: str, csvPath: str, ctx: Context) -> str:
    """Import data from a CSV file to the specified database connection."""

    # Get the engine for the database connection
    engine = GetEngine(ctx, key)

    # Autodetect encoding and separator
    encoding = DetectEncoding(csvPath) or "utf-8"
    delimiter = DetectSeparator(csvPath, encoding)

    # Load CSV into a DataFrame, handilng empty or bad formatted CSV files
    try:
        df = pd.read_csv(csvPath, delimiter=delimiter, encoding=encoding)
    except pd.errors.EmptyDataError:
        raise FastMCPError("No data found in CSV file, table has not been created.")
    except pd.errors.ParserError as e:
        raise FastMCPError(f"Error parsing CSV file: {e}")

    # Load the DataFrame into the database
    df.to_sql(tableName, engine, index=False, if_exists="fail")
    # TODO: log the operation
    return f"CSV file analysis: encoding={encoding}, delimiter={delimiter}\n" \
        f"CSV file loaded as table '{tableName}' into database '{key}'."


@mcp.tool(name="export")
def Export(key: str, tableName: str, csvPath: str, ctx: Context) -> str:
    """Export data from a database table to a CSV file."""

    # Get the data from the database
    engine = GetEngine(ctx, key)
    df = pd.read_sql_table(tableName, engine)

    # Save the data to the CSV file
    df.to_csv(csvPath, index=False)
    # TODO: log the operation
    return f"Data exported from table '{tableName}' in database '{key}' to CSV file '{csvPath}'."



# CSV UTILITIES
# ============



def DetectEncoding(filePath: str) -> str:
    """Autodetect the encoding of a CSV file (uses chardet if available).
    Returns an empty string if chardet is not installed or encoding cannot be detected.
    """
    try:
        import chardet
        with open(filePath, 'rb') as f:
            return chardet.detect(f.read(4096))["encoding"] or ""
    except ImportError:
        return ""


def DetectSeparator(csvPath: str, encoding: str) -> str:
    """Autodetect the separator of a CSV file using csv.Sniffer.
    Returns a comma (",") if no separator can be detected.
    """
    with open(csvPath, 'r', encoding=encoding) as f:
        sample = f.read(2048)
        try:
            return csv.Sniffer().sniff(sample).delimiter
        except Exception as e:
            return ","



# Run the MCP server event loop
if __name__ == "__main__":
    mcp.run()
