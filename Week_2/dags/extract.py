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
        """Fetch AIS data for both staging and stagnant tables."""
        file_names = ["AIS_2024_01_01.zip", "AIS_2024_01_02.zip"]  # ✅ Download both files

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        for file_name in file_names:
            file_url = f"{BASE_URL}/{file_name}"
            local_zip_path = os.path.join(DOWNLOAD_PATH, file_name)

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
        """Extracts and filters first 100 rows of each AIS dataset."""
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



    def load_to_database():
        """Loads first 100 rows of extracted AIS data into separate MySQL tables."""
        mysql_hook = MySqlHook(mysql_conn_id="my_sql")
        engine = mysql_hook.get_sqlalchemy_engine()

        table_mapping = {
            "AIS_2024_01_01.csv": "ais_staging",
            "AIS_2024_01_02.csv": "ais_stagnant",
        }

        for csv_file, table_name in table_mapping.items():
            file_path = os.path.join(EXTRACT_PATH, csv_file)
            
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, nrows=100)  # ✅ Ensure only 100 rows

                df.to_sql(table_name, con=engine, if_exists="replace", index=False)
                print(f"✅ Loaded first 100 rows from {csv_file} into {table_name}")

        print("🎯 Data successfully loaded into MySQL!")


    fetch_task = PythonOperator(
        task_id="fetch_ais_data",
        python_callable=fetch_ais_data,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    load_task = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
    )

    fetch_task >> extract_task >> load_task







