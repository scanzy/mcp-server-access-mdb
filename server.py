"""MCP server for Microsoft Access databases."""

import shutil
from typing import Any
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError


# Initialize the MCP server for protocol-level communication
mcp = FastMCP("MS Access Database", dependencies=["pandas", "sqlalchemy-access"])

# Set up a dictionary to hold DBConnection objects for different database connections
setattr(mcp, "connections", {})



# CONNECTIONS STORAGE AND RETRIEVAL
# =================================


@dataclass
class DBConnection:
    """Dataclass to hold information about a database connection."""

    key: str            # Unique identifier for the connection
    engine: sa.Engine   # SQLAlchemy engine for the connection
    path: str           # Path to the database file, with extension

    # Additional data that can be stored with the connection
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def type(self) -> str:
        """Type of the database connection, determined by the file extension."""
        return self.path.split(".")[-1].lower()



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



# Run the MCP server event loop
if __name__ == "__main__":
    mcp.run()
