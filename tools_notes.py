"""Tools for managing notes associated with database files."""

from pathlib import Path
from fastmcp.exceptions import FastMCPError



def SearchNotes(directory: Path, fileNameFilter: str = "*") -> list[Path]:
    """Search notes in the given directory, returning the list of note files found.
    If the filename is not a note file, searches for notes with the same name.
    Providing database file name as filter, returns all notes associated with it.
    """
    if ".AInotes" not in fileNameFilter:
        fileNameFilter = f"{fileNameFilter}.AInotes.*"
    return [file for file in list(directory.glob(fileNameFilter)) if file.is_file()]    
    

def ReadNotes(fileOrDirectory: str = "") -> list[dict[str, str]]:
    """Specify a note file (*.AInotes.*) to read it, or a directory to list all notes files in it.
    Specify the path of the database file to read notes associated with it (same name, with .AInotes.* suffix).
    Always use absolute paths, except for global notes files (example: "global.AInotes.txt").
    If you find long notes (more than 5000 characters), consider splitting them into multiple files.
    """

    # convert to absolute path
    path = Path(fileOrDirectory).absolute()

    # no direct note file: search for notes
    if ".AInotes" not in path.suffixes:
        noteFiles = SearchNotes(path) if path.is_dir() else SearchNotes(path.parent, path.name)

        # no notes found
        if len(noteFiles) == 0:
            return []

        # multiple notes found, return the list of note files
        if len(noteFiles) > 1:
            return [{"path": f.name} for f in noteFiles]
    
        # single note file, read it
        path = noteFiles[0]

    # direct note file: read it
    try:
        return [{"path": path.name, "content": path.read_text()}]
    except Exception as e:
        raise FastMCPError(f"Error reading notes from '{path}': {e}")

    
def WriteNotes(filePath: str, content: str) -> str:
    """Write notes to a note file (*.AInotes.*). Specify the path of the note file to write.
    If '.AInotes' is not in the path, '.AInotes.txt' will be appended automatically.
    Recommended: use database file names to create associated notes (my_database.mdb -> 'my_database.AInotes.txt').
    Always use absolute paths, except for global notes files (example: "global.AInotes.txt").
    Keep note files concise (about 5000 characters max) but complete, without repetitions.
    For complex databases, organize notes into multiple files if needed (my_database.mdb.tables.AInotes.txt').
    For such cases, always keep a main note file, describing which info is stored in which file.
    WARNING: this tool overwrites existing notes. Note file is deleted if content is empty.
    """

    # add .AInotes.* suffix to the path, if not present
    if ".AInotes" not in Path(filePath).suffixes:
        filePath = filePath + ".AInotes.txt"

    # update or delete the note file
    try:
        if content:
            Path(filePath).write_text(content)
            return f"Notes file '{filePath}' created/updated."
        else:
            Path(filePath).unlink()
            return f"Notes file '{filePath}' deleted."
    except Exception as e:
        raise FastMCPError(f"Error writing notes to '{filePath}': {e}")
