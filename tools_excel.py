"""Tools for importing Excel files."""

from typing import Any
import openpyxl
import pandas as pd

from fastmcp import FastMCP, Context
from fastmcp.exceptions import FastMCPError
from tools_database import GetEngine


def ImportExcel(
    key: str, 
    dbTableName: str, 
    dbColumnNames: list[str],
    excelPath: str, 
    sheetName: str, 
    ctx: Context,
    
    # CURSOR IDE gives error for union types, so we use Any as a workaround
    rowsToSkip: Any = 0, # int or list[int]
    
    columnsToImport: list[int] | None = None,
    naValues:        list[str] | None = None,
    dtypes:     dict[str, str] | None = None,
    fillMergedCells:             bool = False,
) -> str:
    """Import data from an Excel file to the specified database table.
    If the table already exists, the data will be appended to it, otherwise it will be created.
    Before importing, use other tools to analyze the file:
    - read workbook metadata to discover sheet names
    - read the first rows of the file to check its data and structure
    - if you found empty cells, check merged cells to understand better the data structure
    
    Args:
        key: Database connection key
        dbTableName: Name of the table to create/append to
        dbColumnNames: List of unique column names to use for the database table
        excelPath: Path to the Excel file
        sheetName: Name of the worksheet to read
        rowsToSkip: Number of rows to skip from start (int) OR list of specific row indices to skip (0-indexed)
        columnsToImport: List of indices of columns to import (0-indexed)
        naValues: Additional strings to recognize as NaN/NULL values
        dtypes: Optional dictionary specifying pandas data types for columns
        fillMergedCells: Fill all merged cells with the value of the merged range

    Example:
        ImportExcel(
            key="my_database",
            dbTableName="People",
            dbColumnNames=["Name", "Age", "City"],
            excelPath="path/to/my_excel_file.xlsx",
            sheetName="Sheet1",
            rowsToSkip=[0, 3], # skip the first and fourth row
            columnsToImport=[0, 1, 2], # import the first three columns
            dtypes = {'Name': 'string', 'Age': 'int64', 'City': 'string'},
        )

    IMPORTANT - Merged Cell Handling:
    By default, all merged cells are null, except the top-left one, filled with the value of the merged range.
    When fillMergedCells=True, the tool copies the value of the top-left cell to all cells in the merged range.
    If the top-left cell of a merged range is in a skipped row or column, its value won't be available for filling.
    
    Best practice for complex merged layouts:
    1. Import all relevant rows/columns first (avoid skipping merged headers)
    2. Use fillMergedCells=True to handle merges
    3. Use the update tool to drop unwanted columns/rows after import
    """

    # Get the engine for the database connection
    engine = GetEngine(ctx, key)

    # Checks excel and database column count mismatch
    if columnsToImport is not None:
        if len(dbColumnNames) != len(columnsToImport):
            raise FastMCPError(f"Column count mismatch: {len(dbColumnNames)} "
                f"names provided for {len(columnsToImport)} columns")
    
    try:
        # Build read_excel parameters
        read_kwargs = {
            'sheet_name': sheetName,
            'header': None,
            'skiprows': rowsToSkip,
            'usecols': columnsToImport,
            'names': columnsToImport,    # imports columns names as excel indices
            'na_values': naValues,
            'dtype': dtypes,
        }
        df = pd.read_excel(excelPath, **read_kwargs)
        
        # Update DataFrame index to match Excel row numbers (0-indexed)
        # This allows FillMergedCells to match DataFrame rows with Excel rows
        if isinstance(rowsToSkip, int):
            df.index = range(rowsToSkip, len(df) + rowsToSkip)
        elif isinstance(rowsToSkip, list):
            df.index = [x for x in range(len(df) + len(rowsToSkip)) if x not in rowsToSkip]

        # Fill merged cells in Excel coordinate space
        if fillMergedCells:
            worksheet = openpyxl.load_workbook(excelPath, data_only=True)[sheetName]
            df = FillMergedCells(worksheet, df)
            
        # Set final database column names 
        df.columns = dbColumnNames
            
        # Restore sequential index for database import
        # to_sql expects 0-based sequential index
        df.reset_index(drop=True, inplace=True)

        # Load the DataFrame into the database
        df.to_sql(dbTableName, engine, index=False, if_exists="append")

        return f"Excel file loaded as table '{dbTableName}' into database '{key}'.\n" \
               f"Total columns: {len(df.columns)}, Total rows: {len(df)}."

    except Exception as e:
        raise FastMCPError(f"Error parsing Excel file: {e}")


def FillMergedCells(worksheet, df: pd.DataFrame) -> pd.DataFrame:
    """Fill all merged cells in DataFrame with the value of the top-left cell.
    This function expects the DataFrame index and columns to match Excel row and column numbers (0-indexed).
    """
    
    # Get the merged ranges from the worksheet
    for mergedRange in worksheet.merged_cells.ranges:

        # Convert to 0-indexed Excel coordinates
        startRow = mergedRange.min_row - 1
        endRow   = mergedRange.max_row - 1
        startCol = mergedRange.min_col - 1
        endCol   = mergedRange.max_col - 1
        
        # Check if the top-left cell of the merged range exists in our DataFrame
        if startRow not in df.index or startCol not in df.columns:
            continue
        
        # Get value from top-left cell
        value = df.loc[startRow, startCol]
        if pd.isna(value):
            continue
        
        # For every column of the range, except the skipped ones
        for col in range(startCol, endCol + 1):
            if col not in df.columns: continue

            # For every row of the range, except the skipped ones
            for row in range(startRow, endRow + 1):
                if row not in df.index: continue
                
                # Fill the cell with the value of the top-left cell
                df.loc[row, col] = value

    return df
