from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
# import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'add_api_numbers',
    default_args=default_args,
    description='Fetch values from API 5 times and sum them',
    schedule=timedelta(days=1),
    catchup=False,
)

def fetch_and_sum_numbers():
    """
    Fetch values from the API 5 times, add them up, and log the result.
    """
    api_url = "https://ids.pods.uvarc.io/int/6"
    total_sum = 0
    fetched_values = []

    print("Starting to fetch values from API...")

    # Loop 5 times to fetch values
    for i in range(5):
        try:
            # Make HTTP GET request to the API
            response = requests.get(api_url)
            # response.raise_for_status()

            # Parse the JSON response
            data = response.json()

            # Extract the ID value and convert to integer
            id_value = int(data['id'])
            fetched_values.append(id_value)

            # Add to running total
            total_sum += id_value

            print(f"Iteration {i+1}: Fetched value = {id_value}, Running sum = {total_sum}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data on iteration {i+1}: {e}")
            raise
        except (KeyError, ValueError) as e:
            print(f"Error parsing data on iteration {i+1}: {e}")
            raise

    # Print the final results
    print("=" * 50)
    print(f"All fetched values: {fetched_values}")
    print(f"FINAL SUM: {total_sum}")
    print("=" * 50)

    return total_sum

# Create the task
fetch_and_sum_task = PythonOperator(
    task_id='fetch_and_sum_numbers',
    python_callable=fetch_and_sum_numbers,
    dag=dag,
)
