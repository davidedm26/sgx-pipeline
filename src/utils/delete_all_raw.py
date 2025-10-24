import shutil

import sys
from pathlib import Path

# Adding the 'src' directory to PYTHONPATH to run this file directly
sys.path.append(str(Path(__file__).resolve().parent.parent))
print(str(Path(__file__).resolve().parent.parent))

from config.settings import RAW_DATA_DIR

def reset_data_folder():
    data_folder = RAW_DATA_DIR
    print(f"Resetting data folder at: {data_folder}")
    data_folder = Path(data_folder)
    if data_folder.exists() and data_folder.is_dir():
        for item in data_folder.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print(f"Contents of the folder '{data_folder}' have been deleted.")
    else:
        print(f"The folder '{data_folder}' does not exist.")

# Example usage:
reset_data_folder()

