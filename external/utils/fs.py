from pathlib import Path


def get_filepaths(dirpath):
    dirpath = Path(dirpath)
    if not dirpath.exists():
        print(f"The provided path: {dirpath} does not exist.")
        return None
    files_roots = [file for file in dirpath.rglob('*')]
    return files_roots