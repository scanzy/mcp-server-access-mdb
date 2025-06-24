"""
This script tests the MCP server's functionality for connecting to an MS Access database, creating a table, inserting data, querying the table, and deleting the table.

The script:
- Connects to the database
- Creates a new table
- Adds sample data to the table
- Prints the data in the table
- Deletes the table
"""

import os
import asyncio
from pathlib import Path
from fastmcp import Client
from fastmcp.exceptions import FastMCPError


def main():
    dbPath = Path("test.db").absolute()
    asyncio.run(RunTest(str(dbPath)))


async def RunTest(dbPath: str) -> None:
    """Run the test for MS Access database operations using MCP."""

    # run the MCP server in the background
    from server import mcp

    # create a client to connect to the MCP server
    async with Client(mcp) as mcpClient:
        try:
            # Create a new database to run tests
            print(f"Creating new database at {dbPath}...")
            await mcpClient.call_tool("create", {"targetPath": str(dbPath)})
            print("Test database created.")

            await PerformTest(mcpClient, dbPath)

        except FastMCPError as e:
            print(f"Operation failed: {e}")
        
        finally:
            # Delete the test database file at the end
            if not Path(dbPath).exists(): return
            os.remove(dbPath)
            print(f"Deleted test database: {dbPath}")


async def PerformTest(mcpClient: Client, dbPath: str) -> None:
    """Perform a series of operations on the MS Access database."""

    print("Connecting to database...")
    await mcpClient.call_tool("connect", {"databasePath": dbPath})
    print("Connected successfully.")

    print("Creating TestTable...")
    createTableSql = """
        CREATE TABLE TestTable (
            ID INT,
            Name VARCHAR(255),
            Age INT
        )
    """
    await mcpClient.call_tool("update", {"sql": createTableSql})
    print("TestTable created")

    print("Inserting sample data...")
    insertData = [
        "INSERT INTO TestTable (ID, Name, Age) VALUES (1, 'John', 30)",
        "INSERT INTO TestTable (ID, Name, Age) VALUES (2, 'Jane', 25)"
    ]
    for sql in insertData:
        await mcpClient.call_tool("update", {"sql": sql})
    print("Sample data inserted.")

    print("Querying TestTable...")
    results = await mcpClient.call_tool("query", {"sql": "SELECT * FROM TestTable"})
    print("TestTable contents:")

    from mcp.types import TextContent
    assert type(results[0]) is TextContent
    print(results[0].text)

    print("Dropping TestTable...")
    await mcpClient.call_tool("update", {"sql": "DROP TABLE TestTable"})
    print("TestTable dropped.")

    print("Disconnecting from database...")
    await mcpClient.call_tool("disconnect", {})
    print("Disconnected from database.")


if __name__ == "__main__":
    main()
