from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define DAG
with DAG(
    "ais_etl_postgres",
    default_args=default_args,
    description="ETL pipeline for AIS data to PostgreSQL",
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True
) as dag:

    # Constants
    BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024"
    DOWNLOAD_PATH = "/tmp/ais_data"
    EXTRACT_PATH = "/tmp/ais_extracted"
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    os.makedirs(EXTRACT_PATH, exist_ok=True)

    def fetch_ais_data(ds, **kwargs):
        """Download AIS zip files."""
        file_names = ["AIS_2024_01_01.zip", "AIS_2024_01_02.zip"]
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        for file_name in file_names:
            url = f"{BASE_URL}/{file_name}"
            local_path = os.path.join(DOWNLOAD_PATH, file_name)
            try:
                with session.get(url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                print(f"✅ Downloaded {file_name}")
            except requests.exceptions.RequestException as e:
                print(f"❌ Failed to download {file_name}: {e}")
                raise

    def extract_data():
        """Extract zip files and trim to 100 rows."""
        for file in os.listdir(DOWNLOAD_PATH):
            if file.endswith(".zip"):
                with zipfile.ZipFile(os.path.join(DOWNLOAD_PATH, file), 'r') as zip_ref:
                    zip_ref.extractall(EXTRACT_PATH)
                    print(f"✅ Extracted {file}")

        for csv_file in os.listdir(EXTRACT_PATH):
            if csv_file.endswith(".csv"):
                full_path = os.path.join(EXTRACT_PATH, csv_file)
                df = pd.read_csv(full_path, nrows=100)
                df.to_csv(full_path, index=False)
                print(f"✅ Trimmed {csv_file} to 100 rows")

    def load_to_postgres():
        """Load extracted CSVs into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id="pg_database")
        engine = pg_hook.get_sqlalchemy_engine()

        table_mapping = {
            "AIS_2024_01_01.csv": "ais_staging",
            "AIS_2024_01_02.csv": "ais_stagnant",
        }

        for csv_file, table_name in table_mapping.items():
            path = os.path.join(EXTRACT_PATH, csv_file)
            if os.path.exists(path):
                df = pd.read_csv(path, nrows=100)
                df.to_sql(table_name, con=engine, if_exists="replace", index=False)
                print(f"✅ Loaded {csv_file} → {table_name}")

        print("🎯 Data loaded into PostgreSQL.")

    fetch_task = PythonOperator(
        task_id="fetch_ais_data",
        python_callable=fetch_ais_data,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    fetch_task >> extract_task >> load_task
