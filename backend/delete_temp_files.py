import os
import time
from backend.config import TEMP_DIR, DELETE_INTERVAL

def delete_temp_files():
    while True:
        time.sleep(DELETE_INTERVAL)
        for filename in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                    print(f"Deleted {file_path}")
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")