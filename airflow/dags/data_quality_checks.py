from airflow.exceptions import AirflowFailException
import psycopg2

def check_click_data_quality():
    conn = psycopg2.connect(
        host="postgres",
        database="analytics",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM daily_product_clicks;")
    row_count = cur.fetchone()[0]

    if row_count == 0:
        raise AirflowFailException("Data quality failed: table is empty")

    cur.execute("""
        SELECT COUNT(*) FROM daily_product_clicks
        WHERE product_id IS NULL OR total_clicks IS NULL;
    """)
    null_count = cur.fetchone()[0]

    if null_count > 0:
        raise AirflowFailException("Data quality failed: NULL values found")

    cur.close()
    conn.close()

    print("Data quality checks passed")

def check_order_data_quality():
    conn = psycopg2.connect(
        host="postgres",
        database="analytics",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM daily_product_orders;")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("Orders table empty")

    cur.execute("""
        SELECT COUNT(*) FROM daily_product_orders
        WHERE order_id IS NULL OR total_revenue IS NULL;
    """)
    if cur.fetchone()[0] > 0:
        raise AirflowFailException("Invalid order data")

    cur.close()
    conn.close()
