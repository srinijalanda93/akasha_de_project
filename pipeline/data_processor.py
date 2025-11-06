import pandas as pd
import sys

def load_and_clean_data(customer_file, order_file):
    """
    Loads data from CSV and XML (Extract).
    Cleans, transforms, and normalizes it (Transform).
    """
    print("\n--- Starting Data Extraction & Transformation ---")
    
    try:
        customers_df = pd.read_csv(customer_file)
        print(f"Loaded {len(customers_df)} customer records from CSV.")
    except FileNotFoundError:
        print(f"Error: Customer file not found at {customer_file}")
        sys.exit(1)

    try:
        all_orders_df = pd.read_xml(order_file, xpath='//order')
        print(f"Loaded {len(all_orders_df)} order *items* from XML.")
    except FileNotFoundError:
        print(f"Error: Order file not found at {order_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading XML (is 'lxml' installed?): {e}")
        sys.exit(1)

    # --- Data Transformation & Cleaning ---
    
    unique_orders_df = all_orders_df[[
        'order_id', 
        'mobile_number', 
        'order_date_time', 
        'total_amount'
    ]].drop_duplicates().copy()

    order_items_df = all_orders_df[[
        'order_id', 
        'sku_id', 
        'sku_count'
    ]]

    unique_orders_df['order_date_time'] = pd.to_datetime(unique_orders_df['order_date_time'])
    unique_orders_df['total_amount'] = pd.to_numeric(unique_orders_df['total_amount'])
    
    print(f"Processed {len(unique_orders_df)} unique orders.")
    print("Data cleaning and preprocessing complete.")
    
    return customers_df, unique_orders_df, order_items_df