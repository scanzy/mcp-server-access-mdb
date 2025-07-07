"""Tools for importing and exporting CSV files."""

from typing import Literal, Any
from dataclasses import dataclass

import csv
import pandas as pd

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError
from tools_database import GetEngine


# CSV IMPORT/EXPORT
# =================


def ImportCSV(
    key: str, dbTableName: str, csvPath: str,
    ctx: Context,

    # CURSOR IDE gives error for union types, so we use Any as a workaround
    columnsToImport: list[Any] | None = None, # list[int] | list[str]

    dbColumnNames: list[str] | None = None,
    dtype:    dict[str, str] | None = None,
    encoding:  str = "",
    delimiter: str = "",
) -> str:
    """Import data from a CSV file to the specified database connection.
    If the table already exists, the data will be appended to it.

    Optional arguments:
        columnsToImport: names or list of indices (0-indexed) of columns to import 
        dbColumnNames: list of column names for database, defaults to CSV column names
        dtypes: dictionary specifying pandas data types for database columns
        encoding: encoding of the CSV file, leave empty to autodetect (default: utf-8)
        delimiter: separator of the CSV file, leave empty to autodetect (default: ",")
    """

    # Get the engine for the database connection
    engine = GetEngine(ctx, key)

    # Autodetect encoding and separator, if needed
    if encoding  == "": encoding  = DetectEncoding(csvPath) or "utf-8"
    if delimiter == "": delimiter = DetectSeparator(csvPath, encoding) or ","


    # Load CSV into a DataFrame, handilng empty or bad formatted CSV files
    try:
        # Build read_csv parameters
        read_kwargs = {
            'usecols': columnsToImport,
            'names': dbColumnNames,
            'header': 0,
            'dtype': dtype,
            'encoding': encoding,
            'delimiter': delimiter,
        }
        df = pd.read_csv(csvPath, **read_kwargs)
    except pd.errors.EmptyDataError:
        raise FastMCPError("No data found in CSV file, table has not been created.")
    except pd.errors.ParserError as e:
        raise FastMCPError(f"Error parsing CSV file: {e}")

    # Load the DataFrame into the database
    df.to_sql(dbTableName, engine, index=False, if_exists="append")
    # TODO: log the operation
    return f"CSV file loaded as table '{dbTableName}' into database '{key}'. \n" \
        f"Total columns: {len(df.columns)}, Total rows: {len(df)}.\n" \
        f"Import options: encoding='{encoding}', delimiter='{delimiter}'."


def ExportCSV(key: str, dbTableName: str, csvPath: str, ctx: Context,
        overwrite: bool = False, encoding: str = "utf-8", delimiter: str = ",") -> str:
    """Export all data from a database table to a CSV file.
    To export a specific subset of data, first create a temporary table with the desired data.
    When overwriting a file, use the same encoding and delimiter as the original file, if possible.
    """

    # Get the data from the database
    engine = GetEngine(ctx, key)
    df = pd.read_sql_table(dbTableName, engine)

    # Save the data to the CSV file, allowing overwriting if needed
    save_kwargs = {
        "index": False,
        "mode": "w" if overwrite else "x",
        "encoding": encoding,
        "sep": delimiter,
    }
    df.to_csv(csvPath, **save_kwargs)
    # TODO: log the operation
    return f"Data exported from table '{dbTableName}' in database '{key}' to CSV file '{csvPath}'. \n" \
        f"Total columns: {len(df.columns)}, Total rows: {len(df)}.\n" \
        f"Export options: encoding='{encoding}', delimiter='{delimiter}'."



# CSV UTILITIES
# ============


def DetectEncoding(filePath: str) -> str:
    """Autodetect the encoding of a CSV file (uses chardet if available).
    Returns an empty string if chardet is not installed or encoding cannot be detected.
    """
    try:
        import chardet
        with open(filePath, 'rb') as f:
            return chardet.detect(f.read(4096))["encoding"] or ""
    except ImportError:
        return ""


def DetectSeparator(csvPath: str, encoding: str) -> str:
    """Autodetect the separator of a CSV file using csv.Sniffer.
    Returns an empty string if no separator can be detected.
    """
    with open(csvPath, 'r', encoding=encoding) as f:
        sample = f.read(2048)
        try:
            return csv.Sniffer().sniff(sample).delimiter
        except Exception as e:
            return ""


# TODO: add a tool to log import/export operations
@dataclass(frozen=True)
class CSVFileOperation:
    """Dataclass to hold information about an operation performed on a CSV file."""

    # what and when (timestamp in ISO format)
    action: Literal["import", "export"]
    when: str

    # file information
    path: str
    encoding: str
    delimiter: str

    # database information
    key: str
    dbTableName: str
