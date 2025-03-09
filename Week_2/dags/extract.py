from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
with DAG(
    "ais_etl",
    default_args=default_args,
    description="ETL pipeline for AIS data",
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True
) as dag:
    
    # Define variables
    BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024"
    DOWNLOAD_PATH = "/tmp/ais_data"
    EXTRACT_PATH = "/tmp/ais_extracted"
    STAGING_TABLE = "ais_staging"

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    os.makedirs(EXTRACT_PATH, exist_ok=True)
    
    import requests


    def fetch_ais_data(ds, **kwargs):
        file_name = f"AIS_2024_01_01.zip"
        file_url = f"{BASE_URL}/{file_name}"
        local_zip_path = os.path.join(DOWNLOAD_PATH, file_name)

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        try:
            with session.get(file_url, stream=True, timeout=30) as response:
                response.raise_for_status()  # Raise error for bad status codes

                with open(local_zip_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):  # ✅ Larger chunks reduce failures
                        file.write(chunk)

            print(f"✅ Successfully downloaded {file_name}")

        except requests.exceptions.RequestException as e:
            print(f"❌ Download failed: {e}")
            raise Exception(f"Download failed for {file_name}")



    def extract_data():
        for file in os.listdir(DOWNLOAD_PATH):
            if file.endswith(".zip"):
                with zipfile.ZipFile(os.path.join(DOWNLOAD_PATH, file), 'r') as zip_ref:
                    zip_ref.extractall(EXTRACT_PATH)
                    print(f"✅ Extracted {file}")

        # ✅ Limit data to first 100 rows after extraction
        for csv_file in os.listdir(EXTRACT_PATH):
            if csv_file.endswith(".csv"):
                file_path = os.path.join(EXTRACT_PATH, csv_file)

                # ✅ Read only the first 100 rows
                df = pd.read_csv(file_path, nrows=100)

                # ✅ Overwrite the extracted file with only 100 rows
                df.to_csv(file_path, index=False)

                print(f"✅ Filtered first 100 rows of {csv_file}")



    def load_to_staging():
        mysql_hook = MySqlHook(mysql_conn_id="my_sql")
        engine = mysql_hook.get_sqlalchemy_engine()

        for csv_file in os.listdir(EXTRACT_PATH):
            if csv_file.endswith(".csv"):
                file_path = os.path.join(EXTRACT_PATH, csv_file)
                df = pd.read_csv(file_path)

                # ✅ Load data without dropping table schema
                df.to_sql(STAGING_TABLE, con=engine, if_exists="append", index=False)

                print(f"✅ Loaded {csv_file} into {STAGING_TABLE}")

        print("🎯 Data successfully loaded into staging table!")

    # Define tasks
    fetch_task = PythonOperator(
        task_id="fetch_ais_data",
        python_callable=fetch_ais_data,
        op_kwargs={"ds": "{{ ds }}"},  # ✅ Pass execution date dynamically
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    load_staging_task = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    # Task dependencies
    fetch_task >> extract_task >> load_staging_task


