# Microsoft Access Database MCP Server

A simple MCP server to let AI interact with Microsoft Access databases.
Supports import/export with CSV and Excel files, and store human-readable notes about files.

**WARNING**: This server has full access to databases, so it can read and modify any data in it. **Use with caution** to avoid data loss!


## Configuration

To use this MCP server with Claude Desktop (or any other MCP host), clone the repo and add the following to your `config.json`:

```json
{
  "mcpServers": {
    "access-mdb": {
      "command": "uv",
      "args": [
        "run",
        "--with", "fastmcp",
        "--with", "pandas",
        "--with", "sqlalchemy-access",
        "--with", "openpyxl",
        "fastmcp", "run",
        "path/to/repo/server.py"
      ],
    }
  }
}
```

Dev note: to use with uvx, we need to create a package and publish it to PyPI.


## Available Tools

Database management:
- `list`: List all active databases available in the server.
- `create`: Create a new database file (for Microsoft Access, copies the empty.mdb template).
- `connect`: Connect to an existing database file, or creates an in-memory database if the file is not specified.
- `disconnect`: Close a database connection. For in-memory databases, this will clear all its data.

Data management:
- `query`: Execute a SQL query to retrieve data from a database.
- `update`: Execute a SQL query to insert/update/delete data in a database.
- `import_csv`: Imports data from a CSV file into a database table.
- `export_csv`: Exports data from a database table to a CSV file.
- `import_excel`: Imports data from an Excel file into a database table.

Notes management:
- `read_notes`: Reads notes from the specified file, or discovers notes in the specified directory.
- `write_notes`: Writes notes to the specified file, or linked to the specified database.

Note: Excel import/export is not implemented, use haris-musa/excel-mcp-server instead.
The main problem is tracking the index of the rows and columns in the Excel file, to correctly import/export data to the same cells, and/or insert new rows/columns.
In addition, merged cells complicate the process, it would be too complex to implement.


## Project structure

Main files:
- [`server.py`](/server.py): MCP server implementation.

Tests:
- [`test_tools.py`](/test_tools.py): Functions to test individual MCP tools.
- [`test_mcp.py`](/test_mcp.py): Tests all MCP tools in a typical workflow.

Documentation:
- [`README.md`](/README.md): This file, with general information about the project.
- [`LICENSE`](/LICENSE): MIT license.

Scouting scripts, used in the first stages to develop basic functionality:
- [`scouting_mdb.py`](/scouting_mdb.py): SQLAlchemy and pandas to interact with Microsoft Access databases.
- [`scouting_csv.py`](/scouting_csv.py): SQLAlchemy and pandas to interact with CSV files.


## TODO

- [x] Add tool to create a new database, copying empty.mdb to the specified path.
- [x] Add the ability to connect to multiple databases at the same time.
- [x] Add tool to list all tables in the database.
- [x] Add tools to import/export data from/to CSV files.
- [x] Add tools to import data from/to Excel files.
- [x] Add prompt to guide AI asking info to the user about the database.
- [x] Store info about files (.AInotes files), to retrieve it later.
- [ ] Add tool to remember imported/exported CSV and Excel files.
