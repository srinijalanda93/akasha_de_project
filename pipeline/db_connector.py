import os
import sys
import sqlalchemy
from dotenv import load_dotenv

# Load .env file from the parent directory
load_dotenv() 

# READ credentials from the environment
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def create_db_engine():
    """
    Creates a SQLAlchemy engine to connect to the MySQL database.
    """
    try:
        # Check if any credentials are None
        if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
            print("Error: Database credentials are not fully set in .env file.")
            sys.exit(1)

        connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{int(DB_PORT)}/{DB_NAME}"
        engine = sqlalchemy.create_engine(connection_string)
        
        # Test the connection
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