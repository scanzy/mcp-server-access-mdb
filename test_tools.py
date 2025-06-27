import os
from pathlib import Path
from fastmcp import Client
from mcp.types import TextContent


async def TestCreateDatabase(mcpClient: Client, dbPath: str) -> None:
    """Create a new database for testing. No need to create an in-memory database."""
    
    if dbPath == "": return
    await mcpClient.call_tool("create", {"targetPath": dbPath})
    print("Test database created at", dbPath)


async def TestDeleteDatabase(dbPath: str) -> None:
    """Delete the test database file. No need to delete an in-memory database."""

    if dbPath == "": return
    if Path(dbPath).exists():
        os.remove(dbPath)
        print(f"Deleted test database: {dbPath}")
    else:
        print(f"Test database does not exist at {dbPath}.")


async def TestConnect(mcpClient: Client, key: str, dbPath: str) -> None:
    await mcpClient.call_tool("connect", {"key": key, "databasePath": dbPath})
    print(f"Connected to database '{key}'")


async def TestListConnections(mcpClient: Client) -> None:
    connections = await mcpClient.call_tool("list")

    print(f"Active connections: {len(connections)}")
    from mcp.types import TextContent
    for conn in connections:
        assert isinstance(conn, TextContent)
        print(conn.text)


async def TestCreateTable(mcpClient: Client, key: str) -> None:
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
    await mcpClient.call_tool("update", {"key": key, "sql": "INSERT INTO TestTable (ID, Name, Age) VALUES (1, 'John', 30)"})
    await mcpClient.call_tool("update", {"key": key, "sql": "INSERT INTO TestTable (ID, Name, Age) VALUES (2, 'Jane', 25)"})
    print("Sample data inserted.")


async def TestQuery(mcpClient: Client, key: str) -> None:
    results = await mcpClient.call_tool("query", {"key": key, "sql": "SELECT * FROM TestTable"})
    print("TestTable contents:")
    assert isinstance(results[0], TextContent)
    print(results[0].text)


async def TestDropTable(mcpClient: Client, key: str) -> None:
    await mcpClient.call_tool("update", {"key": key, "sql": "DROP TABLE TestTable"})
    print("TestTable dropped.")


async def TestDisconnect(mcpClient: Client, key: str) -> None:
    await mcpClient.call_tool("disconnect", {"key": key})
    print("Disconnected from database.")


async def TestExportCSV(mcpClient: Client, key: str, csvPath: str) -> None:
    await mcpClient.call_tool("export_csv", {"key": key, "dbTableName": "TestTable", "csvPath": csvPath})
    print(f"Data exported to {csvPath}")


async def TestImportCSV(mcpClient: Client, key: str, csvPath: str) -> None:
    result = await mcpClient.call_tool("import_csv", {"key": key, "dbTableName": "TestTable", "csvPath": csvPath})
    print(f"Data imported from {csvPath} to TestTable")
    assert isinstance(result[0], TextContent)
    print(f"Import details: {result[0].text}")
    
    from mcp.types import TextContent
    if result and isinstance(result[0], TextContent):
        print(f"Import details: {result[0].text}")


async def TestDeleteCSV(csvPath: str) -> None:
    if Path(csvPath).exists():
        os.remove(csvPath)
        print(f"Deleted CSV file: {csvPath}")
    else:
        print(f"CSV file does not exist at {csvPath}.")
