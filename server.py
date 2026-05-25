"""MCP server for Microsoft Access databases and CSV files."""

from fastmcp import FastMCP

from src.database     import *
from src.csv          import *
from src.excel        import *
from src.notes        import *


# Initialize the MCP server for protocol-level communication
mcp = FastMCP(
    name="MCP Server for MS Access, SQLite 3, Excel, CSV files",
    instructions="""
    This server allows you to manage MS Access and SQLite 3 databases, Excel files, CSV files.

    It also allows you to manage notes containing informations about databases.
    If you cannot find notes, collect additional info about the use case, and the goal of the user.
    Once you have clear understanding of the problem, summarize it and write it to the notes.
    Keep notes updated during the task, to help you remember what you did and why.
    Ensure every database has notes, so you can remember its structure and purpose.

    Note files should be concise (about 5000 characters max) but complete, without repetitions.
    For complex databases, organize notes into multiple files if needed.
    For such cases, always keep a main note file, describing which info is stored in which file.

    For complex tasks, first discuss with the user about the method to follow, and write it to the notes.
    When working on long tasks, create also a dedicated database to log operations,
    to keep track of current status and progress, without bloating the notes.
    
    The server cannot discover existing tables in MS Access databases.
    If you don't find information about tables in the notes, you must ask the user about them.

    To work with Excel or CSV files, create an in-memory database and load data into it.
    To export data into Excel files, use haris-musa/excel-mcp-server instead.

    NOTE: for important databases, ensure there is a backup (or create it) before modifying data.
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
mcp.tool(name="read_notes")(ReadNotes)
mcp.tool(name="write_notes")(WriteNotes)


# Run the MCP server event loop
if __name__ == "__main__":
    mcp.run()
