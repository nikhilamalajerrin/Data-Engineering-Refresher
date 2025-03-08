from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests



@dag(
    start_date=datetime(2025, 8, 1),
    schedule="@monthly",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)

def extract():