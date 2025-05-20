from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.settings import Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 0,
}

with DAG(
    dag_id="postgres_create_and_test_connection",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Create a Postgres connection and test it with SELECT NOW()",
) as dag:

    def create_pg_connection():
        """Creates a Postgres connection with a space in the username."""
        conn_id = "pg_database"
        conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host="192.168.0.145",     # Use your host IP
            schema="ais",             # Your DB name
            login=" root",            # ← With space!
            password="root",
            port=5431,
        )

        session = Session()
        existing = session.query(Connection).filter_by(conn_id=conn_id).first()
        if not existing:
            session.add(conn)
            session.commit()
            print("✅ Connection 'pg_database' created.")
        else:
            print("ℹ️ Connection 'pg_database' already exists.")

    def test_pg_connection():
        """Test the created Postgres connection."""
        pg_hook = PostgresHook(postgres_conn_id="pg_database")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        print(f"✅ Connected to PostgreSQL. Current time: {result}")
        cursor.close()
        conn.close()

    create_conn_task = PythonOperator(
        task_id="create_postgres_connection",
        python_callable=create_pg_connection,
    )

    test_connection_task = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_pg_connection,
    )

    create_conn_task >> test_connection_task
