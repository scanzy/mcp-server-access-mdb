"""
This script tests the MCP server's functionality for connecting to test databases.

The following operations are performed:

1. New database:
- Creates a new database
- Connects to the database
- Creates a new table
- Adds sample data to the table
- Prints the data in the table

2. Database export:
- Exports the data to a CSV file
- Deletes the table
- Disconnects from the database

3. Database import:
- Reconnects to the database
- Imports the data from the CSV file
- Deletes the CSV file
- Prints the data in the table

4. Connection list:
- Lists the active connections
- Disconnects from the database
- Lists the active connections again
- Deletes the database

5. Notes:
- Writes a note to a file
- Reads the note from the file
- Lists the notes found in the directory
- Deletes the note from the file
"""

import asyncio
from pathlib import Path
from fastmcp import Client
from fastmcp.exceptions import FastMCPError, ToolError

from test_tools import *


# Define paths for the test files
dbPathAccess  = str(Path("test.mdb").absolute())   # MS Access database
dbPathSQLite  = str(Path("test.db").absolute())    # SQLite database
dbPathMemory  = ""                                 # in-memory database

csvPath1 = str(Path("test1.csv").absolute())
csvPath2 = str(Path("test2.csv").absolute())


async def RunTests() -> None:
    """Run tests for database operations using MCP."""

    # run the MCP server in the background
    from server import mcp

    # create a client to connect to the MCP server
    async with Client(mcp) as mcpClient:

        # single database
        await PerformTest1(mcpClient, dbPathAccess, csvPath1, "test_ACCESS")
        await PerformTest1(mcpClient, dbPathSQLite, csvPath1, "test_SQLITE")

        # multiple databases, one in-memory
        await PerformTest2(mcpClient, dbPathAccess, dbPathMemory,
            csvPath1, csvPath2, key1="test_ACCESS", key2="test_MEMORY")

        # same database, two connections
        await PerformTest3(mcpClient, dbPathAccess, key1="test_ACCESS_1", key2="test_ACCESS_2")
        await PerformTest3(mcpClient, dbPathSQLite, key1="test_SQLITE_1", key2="test_SQLITE_2")


async def PerformTest1(mcpClient: Client, dbPath: str, csvPath: str, key: str) -> None:
    """Perform a series of operations on one database."""

    try:
        # 1. New database operations
        await TestCreateDatabase(mcpClient, dbPath)
        await TestConnect(mcpClient, key, dbPath)
        await TestCreateTable(mcpClient, key)
        await TestInsert(mcpClient, key)
        await TestQuery(mcpClient, key)

        # 2. Database export operations
        await TestExportCSV(mcpClient, key, csvPath)
        await TestDropTable(mcpClient, key)
        await TestDisconnect(mcpClient, key)

        # 3. Database import operations
        await TestConnect(mcpClient, key, dbPath)
        await TestImportCSV(mcpClient, key, csvPath)
        await TestDeleteCSV(csvPath)
        await TestQuery(mcpClient, key)

        # 4. Connection list operations
        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key)
        await TestListConnections(mcpClient)

        # 5. Notes operations
        await TestNotesReadWrite(mcpClient, dbPath or key)
        await TestNotesList(mcpClient)
        await TestNotesDelete(mcpClient, dbPath or key)
        await TestNotesList(mcpClient)
        print("Test 1 completed successfully.")

    except (FastMCPError, ToolError) as e:
        print(f"Operation failed: {e}")
        raise e

    finally:
        await TestDeleteDatabase(dbPath)


async def PerformTest2(mcpClient: Client,
    dbPath1:  str,   dbPath2:  str,
    csvPath1: str,   csvPath2: str,
    key1:     str,   key2:     str
) -> None:
    """Perform a series of operations on 2 databases opened at the same time."""

    try:
        # 1. New database operations
        await TestCreateDatabase(mcpClient, dbPath1)
        await TestCreateDatabase(mcpClient, dbPath2)

        await TestConnect(mcpClient, key1, dbPath1)
        await TestConnect(mcpClient, key2, dbPath2)

        await TestCreateTable(mcpClient, key1)
        await TestCreateTable(mcpClient, key2)

        await TestInsert(mcpClient, key1)
        await TestInsert(mcpClient, key2)

        await TestQuery(mcpClient, key1)
        await TestQuery(mcpClient, key2)

        # 2. Database export operations
        await TestExportCSV(mcpClient, key1, csvPath1)
        await TestExportCSV(mcpClient, key2, csvPath2)

        await TestDropTable(mcpClient, key1)
        await TestDropTable(mcpClient, key2)

        await TestDisconnect(mcpClient, key1)
        await TestDisconnect(mcpClient, key2)

        # 3. Database import operations
        await TestConnect(mcpClient, key1, dbPath1)
        await TestConnect(mcpClient, key2, dbPath2)

        await TestImportCSV(mcpClient, key1, csvPath1)
        await TestImportCSV(mcpClient, key2, csvPath2)

        await TestDeleteCSV(csvPath1)
        await TestDeleteCSV(csvPath2)

        await TestQuery(mcpClient, key1)
        await TestQuery(mcpClient, key2)

        # 4. Connection list operations
        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key1)
        await TestListConnections(mcpClient)
        await TestDisconnect(mcpClient, key2)        
        await TestListConnections(mcpClient)

        # 5. Notes operations
        await TestNotesReadWrite(mcpClient, dbPath1 or key1)
        await TestNotesList(mcpClient)
    
        await TestNotesReadWrite(mcpClient, dbPath2 or key2)
        await TestNotesList(mcpClient)
    
        await TestNotesDelete(mcpClient, dbPath1 or key1)
        await TestNotesList(mcpClient)

        await TestNotesDelete(mcpClient, dbPath2 or key2)
        await TestNotesList(mcpClient)
        print("Test 2 completed successfully.")

    except (FastMCPError, ToolError) as e:
        print(f"Operation failed: {e}")
        raise e

    finally:
        # Delete the database and CSV file
        await TestDeleteDatabase(dbPath1)
        await TestDeleteDatabase(dbPath2)


async def PerformTest3(mcpClient: Client, dbPath: str, key1: str, key2: str) -> None:
    """Create two connections to the same database file using different keys."""

    try:
        # One database with 2 connections: one is read/write, the other is read-only
        await TestCreateDatabase(mcpClient, dbPath)
        await TestConnect(mcpClient, key1, dbPath, readOnly=False)
        await TestConnect(mcpClient, key2, dbPath, readOnly=True)

        # Use improperly update and query tools
        await TestUpdateWrong(mcpClient, key1)

        try:
            # Writing using the read-only connection should fail
            await TestInsert(mcpClient, key2)
            raise AssertionError("No error was raised for writing on read-only connection.")
        except ToolError as e:
            assert "read-only" in str(e)
            print(f"Correctly caught error for writing on read-only connection")

        # Fill data with the read/write connection, read data with the read-only connection
        # Read data with the read-only connection
        await TestCreateTable(mcpClient, key1)
        await TestInsert(mcpClient, key1)
        await TestQuery(mcpClient, key2)

        # Disconnect both connections
        await TestDisconnect(mcpClient, key1)
        await TestDisconnect(mcpClient, key2)
        print("Test 3 completed successfully.")

    except (FastMCPError, ToolError) as e:
        print(f"Operation failed: {e}")
        raise e

    finally:
        await TestDeleteDatabase(dbPath)


if __name__ == "__main__":
    asyncio.run(RunTests())
