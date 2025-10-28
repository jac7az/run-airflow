from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import os

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
    'fetch_sqs',
    default_args=default_args,
    description='Fetch an SQS message and print its contents',
    schedule=timedelta(days=1),
    catchup=False,
)

def fetch_sqs_message():
    """
    Fetch a message from SQS and display it. Easy peasy.
    However, getting AWS credentials into the DAG must be done by Airflow ENV variables
    and not just simple OS env variables. See the SQS client below, which maps Airflow vars
    with the expected AWS credential varibles. Set these values in the Airflow UI:
        Admin --> Variables --> Add Variable
    """

    queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/nem2p"
    sqs = boto3.client(
        'sqs',
        region_name='us-east-1',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY")
    )

    # try to get any messages with message-attributes from SQS queue:
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageSystemAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=10
        )
#         receipt_handle = response['Messages'][0]['ReceiptHandle']
#         delete_message(queue_url, receipt_handle)

        print(f"{response}")

        # print the MessageAttributes:
#         print(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")

        # print the order_no:
#         print(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
        return response['Messages'][0]

    except Exception as e:
        print(f"Error getting message: {e}")
        raise e

# Create the task
fetch_sqs_task = PythonOperator(
    task_id='fetch_sqs_message',
    python_callable=fetch_sqs_message,
    dag=dag,
)
