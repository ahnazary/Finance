import glob
import os
import sqlite3

PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))
conn = sqlite3.connect(PROJECT_PATH + '/database/Tickers.sqlite', check_same_thread=False)
cur = conn.cursor()
