import pandas as pd
from datetime import datetime, timedelta
import warnings

def run_in_memory_kpi_analysis(customers_df, unique_orders_df):
    """
    Uses Pandas to get all 4 KPIs directly from the DataFrames.
    (Requirement B2a)
    """
    print("\n--- Starting Requirement B (In-Memory Approach): Running Pandas KPI Functions ---")
    
    merged_df = pd.merge(
        unique_orders_df,
        customers_df,
        on='mobile_number',
        how='left' 
    )

    # KPI 1: Repeat Customers
    print("\nKPI 1: Repeat Customers (Customers with > 1 order)")
    customer_order_counts = merged_df.groupby('customer_name')['order_id'].count()
    kpi_1_df = customer_order_counts[customer_order_counts > 1].reset_index(name='order_count')
    print(kpi_1_df.to_string())

    # KPI 2: Monthly Order Trends
    print("\nKPI 2: Monthly Order Trends")
    monthly_df = merged_df.set_index('order_date_time')
    kpi_2_df = monthly_df.resample('M').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_amount', 'sum')
    )
    kpi_2_df.index = kpi_2_df.index.strftime('%Y-%m')
    print(kpi_2_df)

    # KPI 3: Regional Revenue
    print("\nKPI 3: Regional Revenue")
    kpi_3_df = merged_df.groupby('region')['total_amount'].sum().reset_index(name='total_revenue')
    print(kpi_3_df.sort_values(by='total_revenue', ascending=False).to_string())

    # KPI 4: Top Customers by Spend (Last 30 Days)
    print("\nKPI 4: Top Customers (Last 30 Days)")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        thirty_days_ago = datetime.now() - timedelta(days=30)
    
    recent_orders_df = merged_df[merged_df['order_date_time'] >= thirty_days_ago]
    kpi_4_df = recent_orders_df.groupby('customer_name')['total_amount'].sum().reset_index(name='total_spend')
    print(kpi_4_df.sort_values(by='total_spend', ascending=False).head(10).to_string())