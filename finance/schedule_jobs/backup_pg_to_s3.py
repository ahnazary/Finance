import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.s3_interface import S3Interface
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    s3_interface = S3Interface()
    s3_interface.backup_tables()
