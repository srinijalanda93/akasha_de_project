import pandas as pd
import sqlalchemy

def load_data_to_sql(engine, customers_df, unique_orders_df, order_items_df):
    """
    Loads the clean DataFrames into MySQL tables.
    This is the 'Load' step of ETL. (Requirement A1b)
    """
    print("\n--- Starting Requirement A (Database Approach): Loading Data ---")
    try:
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