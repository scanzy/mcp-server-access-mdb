"""
This script tests the MCP server's functionality for connecting to test databases.

2 tests are performed:
- the first connects to a single database
- the second connects to two databases simultaneously

For every test, the following operations are performed:
- Creates a new database
- Connects to the database
- Creates a new table
- Adds sample data to the table
- Prints the data in the table
- Deletes the table
- Disconnects from the database
- Deletes the database file
"""

import os
import asyncio
from pathlib import Path
from fastmcp import Client
from fastmcp.exceptions import FastMCPError


# Define paths for the test databases
dbPath1 = str(Path("test.mdb").absolute())  # file database
dbPath2 = ""                                # in-memory database


def main():
    asyncio.run(RunTests())


async def RunTests() -> None:
    """Run tests for database operations using MCP."""

    # run the MCP server in the background
    from server import mcp

    # create a client to connect to the MCP server
    async with Client(mcp) as mcpClient:
        await PerformTest1(mcpClient, dbPath1, "test1")
        await PerformTest2(mcpClient, dbPath1, dbPath2, "test1", "test2")


async def PerformTest1(mcpClient: Client, dbPath: str, key: str) -> None:
    """Perform a series of operations on one database."""

    try:
        # setup the test database
        await TestCreateDatabase(mcpClient, dbPath)
        await TestConnect(mcpClient, key, dbPath)

        # perform operations on the database
        await TestCreateTable(mcpClient, key)
        await TestInsert(mcpClient, key)
        await TestQuery(mcpClient, key)

        # clean up the database
        await TestDropTable(mcpClient, key)
        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key)

        await TestListConnections(mcpClient)
        print("Test 1 completed successfully.")

    except FastMCPError as e:
        print(f"Operation failed: {e}")
        raise e

    finally:
        await TestDeleteDatabase(dbPath)


async def PerformTest2(mcpClient: Client, dbPath1: str, dbPath2: str, key1: str, key2: str) -> None:
    """Perform a series of operations on 2 databases opened at the same time."""

    try:
        # setup the test databases
        await TestCreateDatabase(mcpClient, dbPath1)
        await TestCreateDatabase(mcpClient, dbPath2)

        await TestConnect(mcpClient, key1, dbPath1)
        await TestConnect(mcpClient, key2, dbPath2)

        # perform operations on both databases
        await TestCreateTable(mcpClient, key1)
        await TestCreateTable(mcpClient, key2)

        await TestInsert(mcpClient, key1)
        await TestInsert(mcpClient, key2)

        await TestQuery(mcpClient, key1)
        await TestQuery(mcpClient, key2)

        # clean up both databases
        await TestDropTable(mcpClient, key1)
        await TestDropTable(mcpClient, key2)

        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key1)

        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key2)

        await TestListConnections(mcpClient)
        print("Test 2 completed successfully.")

    except FastMCPError as e:
        print(f"Operation failed: {e}")
        raise e

    finally:
        await TestDeleteDatabase(dbPath1)
        await TestDeleteDatabase(dbPath2)


async def TestCreateDatabase(mcpClient: Client, dbPath: str) -> None:
    """Create a new database for testing. No need to create an in-memory database."""
    
    if dbPath == "": return
    print(f"Creating test database at {dbPath}...")
    await mcpClient.call_tool("create", {"targetPath": dbPath})
    print("Test database created.")


async def TestDeleteDatabase(dbPath: str) -> None:
    """Delete the test database file. No need to delete an in-memory database."""

    if dbPath == "": return
    if Path(dbPath).exists():
        print(f"Deleting test database: {dbPath}...")
        os.remove(dbPath)
        print(f"Deleted test database: {dbPath}")
    else:
        print(f"Test database does not exist at {dbPath}.")


async def TestConnect(mcpClient: Client, key: str, dbPath: str) -> None:
    print("Connecting to database...")
    await mcpClient.call_tool("connect", {"key": key, "databasePath": dbPath})
    print("Connected successfully.")


async def TestListConnections(mcpClient: Client) -> None:
    print("Retrieving active database connections...")
    connections = await mcpClient.call_tool("list")

    print(f"Active connections: {len(connections)}")
    from mcp.types import TextContent
    for conn in connections:
        assert isinstance(conn, TextContent)
        print(conn.text)


async def TestCreateTable(mcpClient: Client, key: str) -> None:
    print("Creating TestTable...")
    createTableSql = """
        CREATE TABLE TestTable (
            ID INT,
            Name VARCHAR(255),
            Age INT
        )
    """
    await mcpClient.call_tool("update", {"key": key, "sql": createTableSql})
    print("TestTable created")


async def TestInsert(mcpClient: Client, key: str) -> None:
    print("Inserting sample data...")
    await mcpClient.call_tool("update", {"key": key, "sql": "INSERT INTO TestTable (ID, Name, Age) VALUES (1, 'John', 30)"})
    await mcpClient.call_tool("update", {"key": key, "sql": "INSERT INTO TestTable (ID, Name, Age) VALUES (2, 'Jane', 25)"})
    print("Sample data inserted.")


async def TestQuery(mcpClient: Client, key: str) -> None:
    print("Querying TestTable...")
    results = await mcpClient.call_tool("query", {"key": key, "sql": "SELECT * FROM TestTable"})
    print("TestTable contents:")

    from mcp.types import TextContent
    assert isinstance(results[0], TextContent)
    print(results[0].text)


async def TestDropTable(mcpClient: Client, key: str) -> None:
    print("Dropping TestTable...")
    await mcpClient.call_tool("update", {"key": key, "sql": "DROP TABLE TestTable"})
    print("TestTable dropped.")


async def TestDisconnect(mcpClient: Client, key: str) -> None:
    print("Disconnecting from database...")
    await mcpClient.call_tool("disconnect", {"key": key})
    print("Disconnected from database.")


if __name__ == "__main__":
    main()
