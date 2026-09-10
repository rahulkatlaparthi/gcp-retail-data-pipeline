from datetime import datetime
import glob
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


# ============================================================
# AIRFLOW VARIABLES
# Create these in Composer UI
# ============================================================

PROJECT_ID = Variable.get("gcp_project_id")
REGION = Variable.get("gcp_region")
BUCKET = Variable.get("gcs_bucket")
DATASET = Variable.get("bq_dataset")
TABLE = Variable.get("bq_table")


# ============================================================
# SQL DIRECTORY
# ============================================================

SQL_FOLDER = "/home/airflow/gcs/data/sql"


# ============================================================
# SHOW CONFIGURATION
# ============================================================

def show_configuration():

    print("==========================================")
    print("RETAIL DATA PIPELINE")
    print("==========================================")

    print(f"Project ID : {PROJECT_ID}")
    print(f"Region     : {REGION}")
    print(f"GCS Bucket : {BUCKET}")
    print(f"Dataset    : {DATASET}")
    print(f"Table      : {TABLE}")

    print("==========================================")


# ============================================================
# DYNAMIC SQL DISCOVERY
# Automatically finds every .sql file
# ============================================================

def discover_sql_files():

    sql_files = sorted(
        glob.glob(
            os.path.join(SQL_FOLDER, "*.sql")
        )
    )

    print("==========================================")
    print("SQL FILES")
    print("==========================================")

    if not sql_files:
        print("No SQL files found.")
        return

    for sql_file in sql_files:
        print(f"Found SQL file: {sql_file}")

    print(f"Total SQL files: {len(sql_files)}")
    print("==========================================")


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="retail_data_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "retail",
        "data-engineering",
        "dynamic"
    ],
) as dag:

    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=show_configuration,
    )

    discover_sql = PythonOperator(
        task_id="discover_sql_files",
        python_callable=discover_sql_files,
    )

    start_pipeline >> discover_sql
