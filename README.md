# Microsoft Access Database MCP Server

A simple MCP server to let AI interact with Microsoft Access databases.

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
        "fastmcp", "run",
        "path/to/repo/server.py"
      ],
    }
  }
}
```

Dev note: to use with uvx, we need to create a package and publish it to PyPI.


## Available Tools

- create: Create a new Microsoft Access database, copying the empty.mdb template.
- list: List all active databases available in the server.
- connect: Connect to a Microsoft Access database, from its path.
- query: Execute a SQL query to retrieve data from a database.
- update: Execute a SQL query to insert/update/delete data in a database.
- disconnect: Disconnect from a database, closing the connection.


## TODO

- [x] Add tool to create a new database, copying empty.mdb to the specified path.
- [x] Add the ability to connect to multiple databases at the same time.
- [x] Add tool to list all tables in the database.
- [ ] Add support to read/write data from/to CSV files.
- [ ] Add prompt to ask AI to read chunks of data, to avoid large responses.
- [ ] Add prompt to guide AI asking info to the user about the database.
- [ ] Add memory support to store info about the database, to retrieve it later.
