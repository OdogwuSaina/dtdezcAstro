import sys
import os
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task

# Dynamically add root folder to Python path (for Astro environment)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from pipelines.extract_bus_status import fetch_all_bus_statuses
from pipelines.transform_bus_metrics import transform_bus_metrics
from pipelines.load_to_postgres import load_bus_metrics_to_postgres

DATA_DIR = os.path.join(ROOT_DIR, "data")

@dag(
    dag_id="tfl_bus_metrics_pipeline",
    start_date=datetime(2025, 10, 19),
    schedule="@daily",
    catchup=False,
    tags=["tfl", "bus", "etl"]
)
def tfl_bus_metrics_pipeline():

    @task()
    def extract():
        file_path =  os.path.join(DATA_DIR, "bus_status_20251018_231406.csv") #fetch_all_bus_statuses(max_workers=5, save_csv=True)
        return file_path

    @task()
    def transform(file_path: str):
        # Static local paths for stop data
        stops_path = os.path.join(DATA_DIR, "Stops.csv")
        stop_points_path = os.path.join(DATA_DIR, "tfl_stop_points_20251019_082653.csv")

        metrics_df = transform_bus_metrics(file_path, stops_path, stop_points_path)
        metrics_path = file_path.replace("bus_status", "bus_metrics")
        metrics_df.to_csv(metrics_path, index=False)
        return metrics_path

    @task()
    def load(file_path: str):
        df = pd.read_csv(file_path)
        db_uri = "postgresql+psycopg2://postgres:postgres@postgres:5432/tfl_db"
        load_bus_metrics_to_postgres(df, "bus_performance_metrics",db_uri)

    raw_path = extract()
    metrics_path = transform(raw_path)
    load(metrics_path)

dag = tfl_bus_metrics_pipeline()
