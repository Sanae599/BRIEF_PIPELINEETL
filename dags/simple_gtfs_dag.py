from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import zipfile
import logging

load_dotenv()
import requests
from google.transit import gtfs_realtime_pb2

GTFS_STATIC_URL = os.getenv("GTFS_STATIC_URL")
GTFS_RT_TU_URL = os.getenv("GTFS_RT_TU_URL")
GTFS_RT_VP_URL = os.getenv("GTFS_RT_VP_URL")

#Dossiers dans le conteneur
DATA_DIR = "/opt/airflow/data"
EXPORTS_DIR = "/opt/airflow/exports"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORTS_DIR, exist_ok=True)

UA = {"User-Agent": "airflow-gtfs-demo/1.0"}

#Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_file(url: str, filename: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    resp = requests.get(url, timeout=30, headers=UA)
    resp.raise_for_status()
    path = os.path.join(DATA_DIR, filename)
    with open(path, "wb") as f:
        f.write(resp.content)
    print(f" Fichier téléchargé : {path}")
    return path


def download_gtfs_static():
    zip_path = download_file(GTFS_STATIC_URL, "gtfs_static.zip")
    os.makedirs(EXPORTS_DIR, exist_ok=True)

    # Extraction directement dans exports
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(EXPORTS_DIR)
    print(f"Contenu du GTFS static extrait dans {EXPORTS_DIR}")

    return EXPORTS_DIR


def export_trip_updates():
    pb_path = download_file(GTFS_RT_TU_URL, "trip_updates.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())
    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update) + "\n")
    print(f" Export TripUpdates : {out_txt}")
    return out_txt


def export_vehicle_positions():
    pb_path = download_file(GTFS_RT_VP_URL, "vehicle_positions.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())
    out_txt = os.path.join(EXPORTS_DIR, "vehicle_positions.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle) + "\n")
    print(f" Export VehiclePositions : {out_txt}")
    return out_txt


#Définition du DAG
with DAG(
    dag_id="gtfs_dag",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",
    catchup=False,
    tags=["GTFS", "demo", "Snowflake"],
) as dag:

    task_gtfs_static = PythonOperator(
        task_id="download_gtfs_static",
        python_callable=download_gtfs_static,
    )

    task_trip_updates = PythonOperator(
        task_id="export_trip_updates",
        python_callable=export_trip_updates,
    )

    task_vehicle_positions = PythonOperator(
        task_id="export_vehicle_positions",
        python_callable=export_vehicle_positions,
    )

    # Petit ping SQL pour valider la connexion Snowflake (si la Connection existe)
    snowflake_ping = SQLExecuteQueryOperator(
        task_id="snowflake_ping",
        conn_id="snowflake_conn",
        sql="""
            SELECT
              CURRENT_ACCOUNT(),
              CURRENT_REGION(),
              CURRENT_ROLE(),
              CURRENT_WAREHOUSE(),
              CURRENT_DATABASE(),
              CURRENT_SCHEMA();
        """,
        do_xcom_push=False, 
    )

    # Ordre entre les tâches
    (
        task_gtfs_static
        >> [task_trip_updates, task_vehicle_positions]
        >> snowflake_ping
    )