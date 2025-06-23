"""
This script demonstrates how to use pandas to
query and update a Microsoft Access database using SQLAlchemy.

The script:
- Creates a new table
- Adds sample data to the table
- Prints the data in the table
- Deletes the table
"""

import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from sqlalchemy.engine import URL


# Create connection string for MS Access
dbPath = Path("empty.mdb").absolute()
connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={dbPath};"
connection_url = URL.create("access+pyodbc", query={"odbc_connect": connection_string})


# Create engine to connect to the database
print("Creating engine")
engine = sa.create_engine(connection_url)
print("Engine created")


def ExecuteQuery(query: str) -> pd.DataFrame:
    """Execute a query and return the result as a pandas dataframe"""

    with engine.begin() as conn:
        try:
            return pd.read_sql_query(sa.text(query), conn)
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            exit(1)


def ExecuteUpdate(query: str) -> None:
    """Execute an update query"""

    with engine.begin() as conn:
        try:
            conn.execute(sa.text(query))
        except Exception as e:
            print(f"Error executing update: {str(e)}")
            exit(1)


# Create a new table in the database
ExecuteUpdate("CREATE TABLE TestTable (ID INT, Name VARCHAR(255), Age INT)")
print("Created TestTable")

# add sample data to table TestTable
ExecuteUpdate("INSERT INTO TestTable (ID, Name, Age) VALUES (1, 'John', 30)")
ExecuteUpdate("INSERT INTO TestTable (ID, Name, Age) VALUES (2, 'Jane', 25)")
print("Inserted data into TestTable")

# query the table
df = ExecuteQuery("SELECT * FROM TestTable")
print("TestTable contents:")
print(df)

# delete the table
ExecuteUpdate("DROP TABLE TestTable")
print("Deleted TestTable")
