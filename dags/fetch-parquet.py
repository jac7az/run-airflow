from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import duckdb
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
# PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
PARQUET_URL = "https://s3.amazonaws.com/uvasds-systems/data/taxi/yellow_tripdata_2025-01.parquet"
LOCAL_PARQUET_PATH = "/tmp/yellow_tripdata_2025-01.parquet"
DUCKDB_PATH = "/tmp/taxi_data.duckdb"
TABLE_NAME = "yellow_tripdata"


def download_parquet(**context):
    """Download the parquet file from the URL"""
    print(f"Downloading parquet file from {PARQUET_URL}")

    response = requests.get(PARQUET_URL, stream=True)
    response.raise_for_status()

    with open(LOCAL_PARQUET_PATH, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    file_size = os.path.getsize(LOCAL_PARQUET_PATH)
    print(f"Downloaded {file_size:,} bytes to {LOCAL_PARQUET_PATH}")

    # Push the file path to XCom for the next task
    return LOCAL_PARQUET_PATH


def load_to_duckdb(**context):
    """Load the parquet file into DuckDB"""
    # Pull the file path from XCom
    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='download_parquet')

    print(f"Loading data from {parquet_path} into DuckDB at {DUCKDB_PATH}")

    # Connect to DuckDB (creates the file if it doesn't exist)
    con = duckdb.connect(DUCKDB_PATH)

    # Drop table if it exists and create new one from parquet
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM read_parquet('{parquet_path}')")

    # Get row count
    result = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
    row_count = result[0]

    print(f"Successfully loaded {row_count:,} rows into {TABLE_NAME} table")

    # Show sample of the data
    sample = con.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5").fetchdf()
    print("\nSample data:")
    print(sample)

    con.close()

    return row_count


def cleanup(**context):
    """Clean up the temporary parquet file"""
    if os.path.exists(LOCAL_PARQUET_PATH):
        os.remove(LOCAL_PARQUET_PATH)
        print(f"Cleaned up temporary file: {LOCAL_PARQUET_PATH}")


# Define the DAG
with DAG(
    'load_taxi_data_to_duckdb',
    default_args=default_args,
    description='Download NYC taxi parquet data and load into DuckDB',
    schedule='@monthly',  # Run monthly for new data
    catchup=False,
    tags=['etl', 'duckdb', 'taxi'],
) as dag:

    # Task 1: Download the parquet file
    download_task = PythonOperator(
        task_id='download_parquet',
        python_callable=download_parquet,
    )

    # Task 2: Load data into DuckDB
    load_task = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
    )

    # Task 3: Clean up temporary files
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='all_done',  # Run even if previous tasks fail
    )

    # Define task dependencies
    download_task >> load_task >> cleanup_task
