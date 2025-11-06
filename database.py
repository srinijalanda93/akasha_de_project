import pandas as pd
import sqlalchemy
import sys
from datetime import datetime, timedelta
import warnings
import os                     
from dotenv import load_dotenv   

# --- 1. CONFIGURATION ---
load_dotenv()  # Load the .env file

# READ credentials from the environment
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") # <-- This is the one we are checking
DB_NAME = os.getenv("DB_NAME")

# --- !!! NEW DEBUG TEST !!! ---
# We will print what Python sees BEFORE trying to connect.
print("--- DEBUGGING .ENV FILE ---")
print(f"DB_USER is: {DB_USER}")
print(f"DB_PASS is: {DB_PASS}")
print(f"DB_HOST is: {DB_HOST}")
print(f"DB_PORT is: {DB_PORT}") # <-- This is the important line
print(f"DB_NAME is: {DB_NAME}")
print("---------------------------\n")

# Check if port is missing
if DB_PORT is None:
    print("ERROR: DB_PORT is missing from your .env file!")
    print("Please add 'DB_PORT=3306' to your .env file and save it.")
    sys.exit(1) # Stop the script

# --- 2. DATABASE CONNECTION ---
def create_db_engine():
    """
    Creates a SQLAlchemy engine to connect to the MySQL database.
    """
    try:
        # We construct a "connection string"
        # We must convert DB_PORT to an int here, which is safe now.
        connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{int(DB_PORT)}/{DB_NAME}"
        
        engine = sqlalchemy.create_engine(connection_string)
        
        with engine.connect() as conn:
            print("Successfully connected to MySQL database.")
        return engine
    except ImportError:
        print("Error: 'mysql-connector-python' library not found. Please install it.")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("---")
        print("Please check your DB_USER, DB_PASS, DB_NAME and that MySQL is running.")
        sys.exit(1)

# (The rest of your 'main.py' file stays exactly the same)
# --- 3. DATA PREPROCESSING ... 
# ...