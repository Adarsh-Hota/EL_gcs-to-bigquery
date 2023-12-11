import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from urllib.request import urlretrieve
from datetime import datetime

# Environment variables set at the airflow workers level
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

dataset_file_name = "yellow_tripdata_2023-01.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file_name}"


def upload_to_gcs(bucket, target_file_path, dataset_file_name, dataset_url):
    """
    :param bucket: GCS bucket name
    :param target_file_path: target file path
    :param dataset_file_name: name to be set for the downloaded file
    :param dataset_url: url endpoint for downloading the file 
    :return:
    """

    # Download the file from the URL and save it in the working directoy with the filename 'dataset_file_name'
    # Return a tuple with the first value being the local_file_path and second being the headers object
    local_file_path, headers = urlretrieve(dataset_url, dataset_file_name)

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(target_file_path)
    blob.upload_from_filename(local_file_path)
    
    return


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 11, 13),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_bq_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['gcs-to-bq']
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "target_file_path": f"raw/{dataset_file_name}",
            "dataset_file_name": dataset_file_name,
            "dataset_url": dataset_url,
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file_name}"],
            },
        },
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )

    start_pipeline >> local_to_gcs_task >> bigquery_external_table_task >> end_pipeline 
