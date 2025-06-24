"""MCP server for Microsoft Access databases."""

import shutil
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError


# Initialize the MCP server for protocol-level communication
mcp = FastMCP("MS Access Database", dependencies=["pandas", "sqlalchemy-access"])

# Set up a dictionary to hold SQLAlchemy engines for different database connections
setattr(mcp, "engines", {})


def GetEngine(ctx: Context, key: str):
    """Retrieve the SQLAlchemy engine for the given key, if it exists."""

    engines = getattr(ctx.fastmcp, "engines", {})
    if key not in engines:
        raise FastMCPError(f"Not connected to the database with key '{key}'. Please use connect first.")
    return engines[key]


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
    """Connect to an MS Access database and store the engine under the given key, for future use."""

    try:
        # Build the connection string for MS Access
        # Using ODBC driver that supports both .mdb and .accdb files
        connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={databasePath};"
        connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})

        # Check if the key already exists in the engines dictionary
        engines = getattr(ctx.fastmcp, "engines")
        if key in engines:
            raise FastMCPError(f"Database connection with key '{key}' already exists.")

        # Create a new SQLAlchemy engine and store it
        engines[key] = sa.create_engine(connectionUrl)
        return f"Successfully connected to the database with key '{key}'."

    except Exception as e:
        raise FastMCPError(f"Error connecting to database: {str(e)}")


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


@mcp.tool(name="disconnect")
def Disconnect(key: str, ctx: Context) -> str:
    """Disconnect from the MS Access database identified by key."""

    # Ensure the key exists in the engines dictionary
    engines = getattr(ctx.fastmcp, "engines", {})
    if key not in engines:
        raise FastMCPError(f"No active database connection with key '{key}' to disconnect.")

    # Dispose of the engine and remove it from the dictionary
    engines[key].dispose()
    del engines[key]
    return f"Disconnected from the database with key '{key}'."


if __name__ == "__main__":
    # Run the MCP server event loop
    mcp.run()
