"""MCP server for Microsoft Access databases."""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError


# Initialize the MCP server - this handles all protocol-level communication
mcp = FastMCP("MS Access Database", dependencies=["pandas", "sqlalchemy-access"])


@mcp.tool(name="connect")
def Connect(databasePath: str, ctx: Context) -> str:
    """Connect to an MS Access database."""

    try:
        # Build the connection string for MS Access
        # Using ODBC driver that supports both .mdb and .accdb files
        connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={databasePath};"
        connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})

        # Create and store the SQLAlchemy engine in the context
        # This allows other tools to reuse the connection
        setattr(ctx.fastmcp, "engine", sa.create_engine(connectionUrl))
        return "Successfully connected to the database."
    except Exception as e:
        # Raise a FastMCPError with a descriptive message
        raise FastMCPError(f"Error connecting to database: {str(e)}")


@mcp.tool(name="query")
def Query(sql: str, ctx: Context, parameters: dict | None = None) -> list[dict]:
    """Execute a SELECT query and return results as a list of records."""

    # Get the engine from context - it's stored there by Connect()
    engine = getattr(ctx.fastmcp, "engine", None)
    if not engine:
        raise FastMCPError("Not connected to the database. Please use connect first.")

    # Use pandas to execute query and convert results to dict format
    # This automatically handles proper data type conversion
    with engine.begin() as conn:
        df = pd.read_sql_query(sa.text(sql), conn, params=parameters or {})
        return df.to_dict("records")


@mcp.tool(name="update")
def Update(sql: str, ctx: Context, parameters: dict | None = None) -> bool:
    """Execute an UPDATE/INSERT/DELETE query."""

    # Get the engine from context - it's stored there by Connect()
    engine = getattr(ctx.fastmcp, "engine", None)
    if not engine:
        raise FastMCPError("Not connected to the database. Please use connect first.")

    # Execute the update in a transaction
    # SQLAlchemy automatically commits if no errors occur
    with engine.begin() as conn:
        conn.execute(sa.text(sql), parameters or {})
        return True


if __name__ == "__main__":
    # Run the MCP server event loop
    mcp.run()
