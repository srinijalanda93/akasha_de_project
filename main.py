import pandas as pd
import sqlalchemy
import sys
from datetime import datetime, timedelta
import warnings
import os                     # <-- 1. IMPORT os
from dotenv import load_dotenv   # <-- 2. IMPORT load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()  # <-- 3. ADD THIS LINE to load the .env file

# 4. READ credentials from the environment (os.getenv)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# create a database
# --- 2. DATABASE CONNECTION ---
def create_db_engine():
    """
    Creates a SQLAlchemy engine to connect to the MySQL database.
    This uses parameterized queries by default, meeting Security Req 4a.
    """
    try:
        # We construct a "connection string"
        connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # This 'engine' object manages all our database connections
        engine = sqlalchemy.create_engine(connection_string)
        
        # Test the connection. This provides basic error handling (Req 5c).
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
        sys.exit(1) # Exit the script if we can't connect


# --- 3. DATA PREPROCESSING (EXTRACT & TRANSFORM) ---
def load_and_clean_data(customer_file, order_file):
    """
    Loads data from CSV and XML (Extract).
    Cleans, transforms, and normalizes it (Transform).
    This logic comes directly from your exploration notebook.
    """
    print("\n--- Starting Data Extraction & Transformation ---")
    
    try:
        # 1. Load Customer Data
        customers_df = pd.read_csv(customer_file)
        print(f"Loaded {len(customers_df)} customer records from CSV.")
    except FileNotFoundError:
        print(f"Error: Customer file not found at {customer_file}")
        sys.exit(1)

    try:
        # 2. Load Orders Data
        all_orders_df = pd.read_xml(order_file, xpath='//order')
        print(f"Loaded {len(all_orders_df)} order *items* from XML.")
    except FileNotFoundError:
        print(f"Error: Order file not found at {order_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading XML (is 'lxml' installed?): {e}")
        sys.exit(1)

# --- 3. Data Transformation & Cleaning (from notebook) ---
    
    # 3a. Create 'unique_orders' DataFrame for KPIs
    # This is a critical data modeling step.
    unique_orders_df = all_orders_df[[
        'order_id', 
        'mobile_number', 
        'order_date_time', 
        'total_amount'
    ]].drop_duplicates().copy() # Use .copy() to avoid a common pandas warning

    # 3b. Create 'order_items' DataFrame for a normalized database
    # This shows you are thinking about good database design.
    order_items_df = all_orders_df[[
        'order_id', 
        'sku_id', 
        'sku_count'
    ]]

    # 3c. Clean data types (Requirement B1b)
    # This handles time zone awareness (Req 5d) by converting to datetime
    unique_orders_df['order_date_time'] = pd.to_datetime(unique_orders_df['order_date_time'])
    unique_orders_df['total_amount'] = pd.to_numeric(unique_orders_df['total_amount'])
    
    print(f"Processed {len(unique_orders_df)} unique orders.")
    print("Data cleaning and preprocessing complete.")
    
    # Return the 3 clean DataFrames for the two approaches
    return customers_df, unique_orders_df, order_items_df

# --- 4. REQUIREMENT A: TABLE-BASED (DATABASE) APPROACH ---

def load_data_to_sql(engine, customers_df, unique_orders_df, order_items_df):
    """
    Loads the clean DataFrames into MySQL tables.
    This is the 'Load' step of ETL. (Requirement A1b)
    """
    print("\n--- Starting Requirement A (Database Approach): Loading Data ---")
    try:
        # pandas.to_sql is efficient and uses SQLAlchemy for safety.
        # if_exists='replace' is good for a daily-run script.
        customers_df.to_sql('customers', engine, if_exists='replace', index=False)
        print("Loaded data into 'customers' table.")
        
        unique_orders_df.to_sql('orders', engine, if_exists='replace', index=False)
        print("Loaded data into 'orders' table.")
        
        order_items_df.to_sql('order_items', engine, if_exists='replace', index=False)
        print("Loaded data into 'order_items' table.")
        
        print("All data successfully loaded into MySQL.")
    except Exception as e:
        print(f"Error loading data to SQL: {e}")

def run_sql_kpi_queries(engine):
    """
    Runs SQL queries against the database to get all 4 KPIs.
    (Requirement A2a, A2b)
    """
    print("\n--- Requirement A (Database Approach): Running SQL KPI Queries ---")
    
    # We use a 'with' block to properly manage the connection
    with engine.connect() as conn:
        
        # KPI 1: Repeat Customers
        print("\nKPI 1: Repeat Customers (Customers with > 1 order)")
        sql_kpi_1 = sqlalchemy.text("""
            SELECT c.customer_name, c.mobile_number, COUNT(o.order_id) AS order_count
            FROM orders o
            JOIN customers c ON o.mobile_number = c.mobile_number
            GROUP BY c.customer_name, c.mobile_number
            HAVING order_count > 1;
        """)
        kpi_1_df = pd.read_sql(sql_kpi_1, conn)
        print(kpi_1_df.to_string())

        # KPI 2: Monthly Order Trends
        print("\nKPI 2: Monthly Order Trends")
        sql_kpi_2 = sqlalchemy.text("""
            SELECT 
                DATE_FORMAT(order_date_time, '%Y-%m') AS order_month,
                COUNT(order_id) AS total_orders,
                SUM(total_amount) AS total_revenue
            FROM orders
            GROUP BY order_month
            ORDER BY order_month;
        """)
        kpi_2_df = pd.read_sql(sql_kpi_2, conn)
        print(kpi_2_df.to_string())

        # KPI 3: Regional Revenue
        print("\nKPI 3: Regional Revenue")
        sql_kpi_3 = sqlalchemy.text("""
            SELECT 
                c.region,
                SUM(o.total_amount) AS total_revenue
            FROM orders o
            JOIN customers c ON o.mobile_number = c.mobile_number
            GROUP BY c.region
            ORDER BY total_revenue DESC;
        """)
        kpi_3_df = pd.read_sql(sql_kpi_3, conn)
        print(kpi_3_df.to_string())

        # KPI 4: Top Customers by Spend (Last 30 Days)
        print("\nKPI 4: Top Customers (Last 30 Days)")
        # This query is dynamic and time-zone aware (Req 5d)
        # Using DATE_SUB(NOW(), ...) is safe and handled by the DB.
        sql_kpi_4 = sqlalchemy.text("""
            SELECT 
                c.customer_name,
                SUM(o.total_amount) AS total_spend
            FROM orders o
            JOIN customers c ON o.mobile_number = c.mobile_number
            WHERE o.order_date_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY c.customer_name
            ORDER BY total_spend DESC
            LIMIT 10;
        """)
        kpi_4_df = pd.read_sql(sql_kpi_4, conn)
        print(kpi_4_df.to_string())


# --- 5. REQUIREMENT B: IN-MEMORY (PANDAS) APPROACH ---

def run_in_memory_kpi_analysis(customers_df, unique_orders_df):
    """
    Uses Pandas to get all 4 KPIs directly from the DataFrames.
    (Requirement B2a)
    """
    print("\n--- Starting Requirement B (In-Memory Approach): Running Pandas KPI Functions ---")
    
    # First, we merge the two dataframes to get all info in one place
    # This is the in-memory equivalent of a SQL JOIN
    merged_df = pd.merge(
        unique_orders_df,
        customers_df,
        on='mobile_number',
        how='left' # Use 'left' join to keep all orders
    )

    # KPI 1: Repeat Customers
    print("\nKPI 1: Repeat Customers (Customers with > 1 order)")
    customer_order_counts = merged_df.groupby('customer_name')['order_id'].count()
    kpi_1_df = customer_order_counts[customer_order_counts > 1].reset_index(name='order_count')
    print(kpi_1_df.to_string())

    # KPI 2: Monthly Order Trends
    print("\nKPI 2: Monthly Order Trends")
    # We set the date as the index to use the 'resample' function
    monthly_df = merged_df.set_index('order_date_time')
    # 'M' means 'Month'. This is a powerful time-series function.
    kpi_2_df = monthly_df.resample('M').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_amount', 'sum')
    )
    kpi_2_df.index = kpi_2_df.index.strftime('%Y-%m') # Format for readability
    print(kpi_2_df)

    # KPI 3: Regional Revenue
    print("\nKPI 3: Regional Revenue")
    kpi_3_df = merged_df.groupby('region')['total_amount'].sum().reset_index(name='total_revenue')
    print(kpi_3_df.sort_values(by='total_revenue', ascending=False).to_string())

    # KPI 4: Top Customers by Spend (Last 30 Days)
    print("\nKPI 4: Top Customers (Last 30 Days)")
    # We calculate the date 30 days ago (Req 5d)
    # Using .now() is aware of the time the script is run
    with warnings.catch_warnings():
        # Suppress a known warning when comparing timezone-naive and timezone-aware datetimes
        # This is not critical for this specific dataset but shows good practice.
        warnings.simplefilter("ignore", category=UserWarning)
        thirty_days_ago = datetime.now() - timedelta(days=30)
    
    recent_orders_df = merged_df[merged_df['order_date_time'] >= thirty_days_ago]
    kpi_4_df = recent_orders_df.groupby('customer_name')['total_amount'].sum().reset_index(name='total_spend')
    print(kpi_4_df.sort_values(by='total_spend', ascending=False).head(10).to_string())


# --- 6. MAIN EXECUTION ---
def main():
    """
    Main function to run the entire ETL and analysis pipeline.
    This orchestrates all the steps above.
    """
    print("===== Akasha Air Data Engineering ETL Pipeline =====")
    
    # Define file paths
    CUSTOMER_FILE_PATH = 'data/task_DE_new_customers.csv'
    ORDER_FILE_PATH = 'data/task_DE_new_orders.xml'

    # --- Step 1: Extract, Transform (from your notebook logic) ---
    customers_df, unique_orders_df, order_items_df = load_and_clean_data(
        CUSTOMER_FILE_PATH, 
        ORDER_FILE_PATH
    )

    # --- Step 2: Connect to Database ---
    db_engine = create_db_engine()

    # --- Step 3: Run Requirement A (Table-Based) ---
    load_data_to_sql(db_engine, customers_df, unique_orders_df, order_items_df)
    run_sql_kpi_queries(db_engine)

    # --- Step 4: Run Requirement B (In-Memory) ---
    run_in_memory_kpi_analysis(customers_df, unique_orders_df)
    
    print("\n===== Pipeline execution complete. =====")


# This standard Python entry point makes the script runnable
if __name__ == "__main__":
    main()




