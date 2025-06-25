"""
This script demonstrates how to use pandas to
query and update a CSV file as a database using SQLAlchemy (in-memory SQLite).

The script:
- Autodetects separator and encoding
- Loads CSV data into a SQLAlchemy in-memory database
- Creates a new table
- Adds sample data to the table
- Prints the data in the table
- Deletes the table
"""

import csv
from pathlib import Path

import pandas as pd
import sqlalchemy as sa


# Create in-memory SQLite engine
print("Creating in-memory SQLite engine")
engine = sa.create_engine("sqlite:///:memory:")
print("Engine created")


def LoadCSV(csvPath: Path, tableName: str) -> None:
    """Load CSV data from file to the specified table."""

    # Load CSV into DataFrame
    try:
        df = pd.read_csv(csvPath)
        print("Loaded CSV: shape = ", df.shape)

        # Load DataFrame into SQLite as table 'TestTable'
        df.to_sql(tableName, engine, index=False, if_exists="replace")
        print(f"Data loaded into table '{tableName}'")

    # Handle bad formatting in CSV file
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {str(e)}")
        exit(1)

    # Handle empty CSV file
    except pd.errors.EmptyDataError:
        print("No data found in CSV file, table not created.")
        exit(1)


def SaveCSV(csvPath: Path, tableName: str) -> None:
    """Save a table into a CSV file."""
    with engine.begin() as conn:
        df = pd.read_sql_table(tableName, conn)
        df.to_csv(csvPath, index=False)
        print(f"Saved database to {csvPath}")


def ExecuteQuery(query: str) -> pd.DataFrame:
    """Execute a query and return the result as a pandas dataframe."""
    with engine.begin() as conn:
        try:
            return pd.read_sql_query(sa.text(query), conn)
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            exit(1)


def ExecuteUpdate(query: str) -> None:
    """Execute an update query."""
    with engine.begin() as conn:
        try:
            conn.execute(sa.text(query))
        except Exception as e:
            print(f"Error executing update: {str(e)}")
            exit(1)


# Creates a test CSV file
csvPath = Path("test.csv").absolute()
df = pd.DataFrame({
    "ID": [1, 2, 3],
    "Name": ["Alice", "Bob", "Charlie"],
    "Age": [25, 30, 35],
})
df.to_csv(csvPath, index=False)

# Load the CSV file into the in-memory SQLite database
LoadCSV(csvPath, "TestTable")

# Query the loaded table
df = ExecuteQuery("SELECT * FROM TestTable")
print("TestTable contents:")
print(df)

# Add sample data
ExecuteUpdate("INSERT INTO TestTable (ID, Name, Age) VALUES (4, 'John', 30)")
ExecuteUpdate("INSERT INTO TestTable (ID, Name, Age) VALUES (5, 'Jane', 25)")
print("Inserted data into TestTable")

# Query the loaded table
df = ExecuteQuery("SELECT * FROM TestTable")
print("TestTable contents:")
print(df)

# Saves data to file
SaveCSV(csvPath, "TestTable")

# Reset the engine to clear the in-memory database
engine.dispose()
engine = sa.create_engine("sqlite:///:memory:")

# Load the CSV file again to restore the data
LoadCSV(csvPath, "TestTable")

# Query the loaded table
df = ExecuteQuery("SELECT * FROM TestTable")
print("TestTable contents:")
print(df)

# Delete the table and the CSV file
ExecuteUpdate("DROP TABLE TestTable")
csvPath.unlink(missing_ok=True)
print("Deleted TestTable and removed CSV file")
