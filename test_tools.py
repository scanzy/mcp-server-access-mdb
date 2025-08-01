import os
from pathlib import Path
from fastmcp import Client
from mcp.types import TextContent


# DATABASE MANAGEMENT
# ===================


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


async def TestDisconnect(mcpClient: Client, key: str) -> None:
    await mcpClient.call_tool("disconnect", {"key": key})
    print("Disconnected from database.")


async def TestListConnections(mcpClient: Client) -> None:
    connections = await mcpClient.call_tool("list")

    print(f"Active connections: {len(connections)}")
    from mcp.types import TextContent
    for conn in connections:
        assert isinstance(conn, TextContent)
        print(conn.text)



# DATA MANAGEMENT
# ===============


async def TestCreateTable(mcpClient: Client, key: str) -> None:
    createTableSql = """
        CREATE TABLE TestTable (
            ID INT,
            Name VARCHAR(255),
            Age INT,
            Emoji VARCHAR(10)
        )
    """
    await mcpClient.call_tool("update", {"key": key, "sql": createTableSql})
    print("TestTable created")


async def TestInsertDirect(mcpClient: Client, key: str) -> None:
    sql = "INSERT INTO TestTable (ID, Name, Age, Emoji) VALUES (1, 'John', 30, '😀')"
    await mcpClient.call_tool("update", {"key": key, "sql": sql})
    print("Sample data inserted (single statement).")


async def TestInsertSingle(mcpClient: Client, key: str) -> None:
    sql = "INSERT INTO TestTable (ID, Name, Age, Emoji) VALUES (:id, :name, :age, :emoji)"
    data = [{"id": 2, "name": "Jane", "age": 25, "emoji": "🚀"}]
    await mcpClient.call_tool("update", {"key": key, "sql": sql, "params": data})
    print("Sample data inserted (single statement).")


async def TestInsertMany(mcpClient: Client, key: str) -> None:
    sql = "INSERT INTO TestTable (ID, Name, Age, Emoji) VALUES (:id, :name, :age, :emoji)"
    data = [
        {"id": 3, "name": "Jim", "age": 40, "emoji": "🤖"},
        {"id": 4, "name": "Jill", "age": 35, "emoji": "🐶"},
    ]
    await mcpClient.call_tool("update", {"key": key, "sql": sql, "params": data})
    print("Sample data inserted (multiple statements).")


async def TestInsert(mcpClient: Client, key: str) -> None:
    await TestInsertDirect(mcpClient, key)
    await TestInsertSingle(mcpClient, key)
    await TestInsertMany(mcpClient, key)


async def TestQueryDirect(mcpClient: Client, key: str) -> None:
    results = await mcpClient.call_tool("query", {"key": key, "sql": "SELECT * FROM TestTable"})
    print("TestTable contents:")
    assert isinstance(results[0], TextContent)
    print(results[0].text)


async def TestQueryParams(mcpClient: Client, key: str) -> None:
    sql = "SELECT * FROM TestTable WHERE ID = :id"
    results = await mcpClient.call_tool("query", {"key": key, "sql": sql, "params": {"id": 1}})
    print("First row contents:")
    assert isinstance(results[0], TextContent)
    print(results[0].text)


async def TestQuery(mcpClient: Client, key: str) -> None:
    await TestQueryDirect(mcpClient, key)
    await TestQueryParams(mcpClient, key)


async def TestDropTable(mcpClient: Client, key: str) -> None:
    await mcpClient.call_tool("update", {"key": key, "sql": "DROP TABLE TestTable"})
    print("TestTable dropped.")



# DATA IMPORT/EXPORT
# ==================


async def TestExportCSV(mcpClient: Client, key: str, csvPath: str) -> None:
    await mcpClient.call_tool("export_csv",
        {"key": key, "dbTableName": "TestTable", "csvPath": csvPath, "encoding": "utf-8"})
    print(f"Data exported to {csvPath}")


async def TestImportCSV(mcpClient: Client, key: str, csvPath: str) -> None:
    result = await mcpClient.call_tool("import_csv",
        {"key": key, "dbTableName": "TestTable", "csvPath": csvPath, "encoding": "utf-8"})
    print(f"Data imported from {csvPath} to TestTable")
    assert isinstance(result[0], TextContent)
    print(f"Import details: {result[0].text}")


async def TestDeleteCSV(csvPath: str) -> None:
    if Path(csvPath).exists():
        os.remove(csvPath)
        print(f"Deleted CSV file: {csvPath}")
    else:
        print(f"CSV file does not exist at {csvPath}.")



# NOTES MANAGEMENT
# ================


async def TestNoteWrite(mcpClient: Client, filePath: str, note: str = "") -> None:
    if note == "":
        note = "This is a test note into file " + filePath
    await mcpClient.call_tool("write_notes", {"filePath": filePath, "content": note})
    print(f"Notes written to {filePath}. Length: {len(note)}")


async def TestNoteRead(mcpClient: Client, filePath: str) -> str:
    readNotes = await mcpClient.call_tool("read_notes", {"fileOrDirectory": filePath})
    assert isinstance(readNotes[0], TextContent)
    note = readNotes[0].text
    print(f"Notes read from {filePath}: {note}")
    return note


async def TestNotesReadWrite(mcpClient: Client, filePath: str) -> None:
    await TestNoteWrite(mcpClient, filePath)
    readNotes = await TestNoteRead(mcpClient, filePath)
    assert "test note" in readNotes


async def TestNotesList(mcpClient: Client, directory: str = "") -> None:
    readNotes = await mcpClient.call_tool("read_notes", {"fileOrDirectory": directory})
    assert isinstance(readNotes, list)
    if len(readNotes) > 0:
        print(f"Notes in directory '{directory}':")
        assert isinstance(readNotes[0], TextContent)
        print(readNotes[0].text)
    else:
        print(f"No notes found in directory '{directory}'.")


async def TestNotesDelete(mcpClient: Client, filePath: str) -> None:
    await mcpClient.call_tool("write_notes", {"filePath": filePath, "content": ""})
    print(f"Notes deleted from {filePath}.")
