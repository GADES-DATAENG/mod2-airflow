import uuid
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.datasets import Dataset


# Configuration
BUCKET_NAME = Variable.get("BRONZE_GCS_BUCKET_NAME")
LOCAL_CSV_PATH = f"{Variable.get("LOCAL_FILE_PATH")}/raw_customers.csv"
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BRONZE_DATASET = Variable.get("BRONZE_BIGQUERY_DATASET")
BIGQUERY_TABLE = "bronze_customers"
BIGQUERY_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.{BIGQUERY_TABLE}"
BIGQUERY_LOCATION = Variable.get("GCP_LOCATION")


def generate_unique_file_name(**kwargs):
    """Generate a unique file name using UUID and push it to XCom."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = uuid.uuid4().hex[:5]
    unique_file_name = f"jaffle_shop_customers/data_{unique_id}_{timestamp}.csv"
    kwargs['ti'].xcom_push(key='uploaded_file', value=unique_file_name)


with DAG(
    dag_id="bronze_customers",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "jaffle_shop", "bronze_layer", "customers"],
) as dag:

    generate_file_name_task = PythonOperator(
        task_id="generate_unique_file_name",
        python_callable=generate_unique_file_name,
        provide_context=True,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=LOCAL_CSV_PATH,
        dst="{{ ti.xcom_pull(task_ids='generate_unique_file_name', key='uploaded_file') }}",
        bucket=BUCKET_NAME,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_table",
        bucket=BUCKET_NAME,
        #source_objects=["{{ ti.xcom_pull(task_ids='generate_unique_file_name', key='uploaded_file') }}"],
        source_objects=["{{ ti.xcom_pull(task_ids='generate_unique_file_name', key='uploaded_file') }}"],
        source_format="csv",
        destination_project_dataset_table=BIGQUERY_TABLE,
        write_disposition="WRITE_APPEND",
        create_disposition='CREATE_IF_NEEDED',
        external_table=False,
        autodetect=True,
        skip_leading_rows=1,  # Skip the header row for column names
        schema_fields=[
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"}
        ],
        field_delimiter=",",
        quote_character="",
        outlets=[Dataset("bronze_customers_dataset_ready")] # This indicates that this task writes to the dataset
    )

    generate_file_name_task.set_downstream(upload_to_gcs_task)
    upload_to_gcs_task.set_downstream(load_to_bq)
