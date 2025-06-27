"""MCP server for Microsoft Access databases and CSV files."""

from fastmcp import FastMCP
from tools_database import *
from tools_csv import *
from tools_excel import *


# Initialize the MCP server for protocol-level communication
mcp = FastMCP(
    name="MCP Server for MS Access, Excel, CSV files",
    dependencies=["pandas", "sqlalchemy-access", "openpyxl"],
    instructions="""
    This server allows you to manage MS Access databases, Excel files and CSV files.
    With important databases, ensure there is a backup (or create it) before modifying data.

    Before starting, collect additional info about the use case, and the goal of the user.
    For complex tasks, first discuss with the user about the method to follow.
    When working on long tasks, create a dedicated database to log operations,
    to keep track of current status and progress.
    
    The server cannot discover existing tables in MS Access databases, ask the user about them.
    To work with Excel or CSV files, create an in-memory database and load data into it.
    To export data into Excel files, use haris-musa/excel-mcp-server instead.
    """
)

# Set up a dictionary to hold DBConnection objects for different database connections
setattr(mcp, "connections", {})


# Register tools
mcp.tool(name="list")(ListConnections)
mcp.tool(name="create")(CreateDatabase)
mcp.tool(name="connect")(Connect)
mcp.tool(name="disconnect")(Disconnect)
mcp.tool(name="query")(Query)
mcp.tool(name="update")(Update)
mcp.tool(name="import_csv")(ImportCSV)
mcp.tool(name="export_csv")(ExportCSV)
mcp.tool(name="import_excel")(ImportExcel)


# Run the MCP server event loop
if __name__ == "__main__":
    mcp.run()
