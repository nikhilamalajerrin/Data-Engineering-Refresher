from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "mysql_connection_test",
    default_args=default_args,
    description="Test connection to MySQL and append the current date",
    schedule_interval=None,  # Run only when triggered manually
    catchup=False
) as dag:

    def test_mysql_connection():
        """Function to test connection and print 'Connected'."""
        mysql_hook = MySqlHook(mysql_conn_id="my_sql")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        print("✅ Connected to MySQL")
        
        cursor.close()
        conn.close()

    def append_current_date():
        """Function to insert current date into MySQL."""
        mysql_hook = MySqlHook(mysql_conn_id="my_sql")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # ✅ Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_dates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                inserted_at DATETIME
            );
        """)
        
        # ✅ Insert current timestamp
        cursor.execute("INSERT INTO test_dates (inserted_at) VALUES (NOW());")
        conn.commit()

        print("✅ Current date appended to MySQL table")
        
        cursor.close()
        conn.close()

    # Define tasks
    test_db_connection_task = PythonOperator(
        task_id="test_mysql_connection",
        python_callable=test_mysql_connection,
    )

    append_date_task = PythonOperator(
        task_id="append_current_date",
        python_callable=append_current_date,
    )

    # Task dependencies
    test_db_connection_task >> append_date_task