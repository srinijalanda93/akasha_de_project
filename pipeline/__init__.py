# --- 1. IMPORTS ---
# Import functions from our new modules
from pipeline.db_connector import create_db_engine
from pipeline.data_processor import load_and_clean_data
from pipeline.sql_analysis import load_data_to_sql, run_sql_kpi_queries
from pipeline.pandas_analysis import run_in_memory_kpi_analysis

# --- 2. CONSTANTS ---
# Define file paths in one central place
CUSTOMER_FILE_PATH = 'data/task_DE_new_customers.csv'
ORDER_FILE_PATH = 'data/task_DE_new_orders.xml'

# --- 3. MAIN EXECUTION ---
def main():
    """
    Main function to run the entire ETL and analysis pipeline.
    This function now just orchestrates the steps.
    """
    print("===== Akasha Air Data Engineering ETL Pipeline =====")
    
    # --- Step 1: Connect to Database ---
    # (We connect first to fail fast if DB is down)
    db_engine = create_db_engine()

    # --- Step 2: Extract, Transform (from your notebook logic) ---
    customers_df, unique_orders_df, order_items_df = load_and_clean_data(
        CUSTOMER_FILE_PATH, 
        ORDER_FILE_PATH
    )

    # --- Step 3: Run Requirement A (Table-Based) ---
    load_data_to_sql(db_engine, customers_df, unique_orders_df, order_items_df)
    run_sql_kpi_queries(db_engine)

    # --- Step 4: Run Requirement B (In-Memory) ---
    run_in_memory_kpi_analysis(customers_df, unique_orders_df)
    
    print("\n Extarct-transform-load--pipeline execution complete.")


# This standard Python entry point makes the script runnable
if __name__ == "__main__":
    main()