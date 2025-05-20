import os
import pandas as pd
import zipfile
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

# === Configuration ===
PROJECT_ID = "ais-astro"       # ✅ Your GCP project ID
DATASET = "ais_data"           # BigQuery dataset name
BUCKET_NAME = "aisda"          # ✅ Your GCS bucket

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024"
DOWNLOAD_PATH = "/tmp/ais_data"
EXTRACT_PATH = "/tmp/ais_extracted"

CSV_TABLES = {
    "AIS_2024_01_01.csv": "ais_staging",
    "AIS_2024_01_02.csv": "ais_stagnant",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="upload_ais_to_bigquery_via_gcs",
    default_args=default_args,
    description="ETL pipeline: Download AIS → Trim → GCS → BigQuery",
    schedule_interval=None,
    catchup=False,
) as dag:

    def setup_dirs():
        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
        os.makedirs(EXTRACT_PATH, exist_ok=True)
        print("✅ Local directories ready.")

    setup_dirs_task = PythonOperator(
        task_id="setup_dirs",
        python_callable=setup_dirs,
    )

    def fetch_ais_data():
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
                print(f"✅ Downloaded: {file_name}")
            except Exception as e:
                print(f"❌ Failed to download {file_name}: {e}")
                raise

    fetch_task = PythonOperator(
        task_id="fetch_ais_data",
        python_callable=fetch_ais_data,
    )

    def extract_and_trim():
        for file in os.listdir(DOWNLOAD_PATH):
            if file.endswith(".zip"):
                with zipfile.ZipFile(os.path.join(DOWNLOAD_PATH, file), 'r') as zip_ref:
                    zip_ref.extractall(EXTRACT_PATH)
                    print(f"✅ Extracted {file}")

        for csv_file in os.listdir(EXTRACT_PATH):
            if csv_file.endswith(".csv"):
                path = os.path.join(EXTRACT_PATH, csv_file)
                df = pd.read_csv(path, nrows=100)
                df.to_csv(path, index=False)
                print(f"✅ Trimmed {csv_file} to 100 rows")

    extract_task = PythonOperator(
        task_id="extract_and_trim",
        python_callable=extract_and_trim,
    )

    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        dataset_id=DATASET,
        project_id=PROJECT_ID,
        location="US",
        gcp_conn_id="google_cloud_default",  # ✅ Required for auth
    )

    for csv_file, table_name in CSV_TABLES.items():
        local_path = os.path.join(EXTRACT_PATH, csv_file)
        gcs_path = f"ais/{csv_file}"

        upload = LocalFilesystemToGCSOperator(
            task_id=f"upload_{table_name}_to_gcs",
            src=local_path,
            dst=gcs_path,
            bucket=BUCKET_NAME,
            gcp_conn_id="google_cloud_default",
        )

        load = BigQueryInsertJobOperator(
            task_id=f"load_{table_name}_to_bigquery",
            configuration={
                "load": {
                    "sourceUris": [f"gs://{BUCKET_NAME}/{gcs_path}"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET,
                        "tableId": table_name,
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            location="US",
            gcp_conn_id="google_cloud_default",
        )

        extract_task >> upload >> load

    setup_dirs_task >> fetch_task >> extract_task
    create_bq_dataset >> extract_task
