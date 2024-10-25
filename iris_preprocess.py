# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

# Other imports
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from io import StringIO


# ------------------------------------------------------------------------
# Section 1: Configuration dictionary and default arguments
# ------------------------------------------------------------------------
CONFIG = Variable.get("iris_preprocessing_config", deserialize_json=True, default_var={
  "bucket_name": "composer-demo-bucket",
  "iris_file_name": "iris.data",
  "project_id": os.environ.get('GCP_PROJECT'),
  "dataset_id": "mlops_lab",
  "table_id": "iris_dataset_processed",
  "data_quality_thresholds": {
    "min_rows": 100,
    "max_null_percentage": 0.01,
    "max_outliers": 20
  },
})

# Default arguments for the DAG
default_args = {
  'owner': 'airflow',
  'start_date': datetime(2024, 1, 1),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'execution_timeout': timedelta(minutes=30),
}


# ------------------------------------------------------------------------
# Section 2: Instantiate the DAG
# ------------------------------------------------------------------------
dag = DAG(
  'iris_preprocessing_pipeline',
  default_args=default_args,
  description='A pipeline for preprocessing the Iris dataset',
  schedule_interval=timedelta(days=1),
  catchup=False,
)


# ------------------------------------------------------------------------
# Section 3: Function to create an empty metrics table in BigQuery
# ------------------------------------------------------------------------
def create_metrics_table(**context) -> None:
  bq_hook = BigQueryHook()
  metrics_table_schema = [
    {"name": "execution_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "metric_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "metric_value", "type": "FLOAT", "mode": "REQUIRED"},
  ]
  
  bq_hook.create_empty_table(
    project_id=CONFIG['project_id'],
    dataset_id=CONFIG['dataset_id'],
    table_id="pipeline_metrics",
    schema_fields=metrics_table_schema,
    exists_ok=True,
  )


# ------------------------------------------------------------------------
# Section 4: Function to preprocess and validate the Iris dataset
# ------------------------------------------------------------------------
def validate_and_preprocess_data(**context) -> str:
  gcs_hook = GCSHook()
  metrics = {}
  
  # Fetching the file from GCS
  file_data = gcs_hook.download(bucket_name=CONFIG['bucket_name'], object_name=CONFIG['iris_file_name'])
  # Convert bytes to a file-like object
  file_data = StringIO(file_data.decode('utf-8'))
  
  df = pd.read_csv(file_data, header=None, 
                   names=['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species'])
  
  # Save metrics
  metrics["row_count"] = len(df)
  metrics["null_percentage"] = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
  
  numeric_columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
  metrics["outliers_count"] = 0
  
  for col in numeric_columns:
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
    metrics["outliers_count"] += len(outliers)
  
  # Save metrics to Xcom for later tasks
  ti = context['task_instance']
  ti.xcom_push(key='data_metrics', value=metrics)
  
  # Some simple preprocessing
  df[numeric_columns] = (df[numeric_columns] - df[numeric_columns].mean()) / df[numeric_columns].std()
  # Some simple feature engineering
  df['petal_area'] = df['petal_length'] * df['petal_width']
  df['sepal_area'] = df['sepal_length'] * df['sepal_width']
  df['sepal_petal_ratio'] = df['sepal_area'] / df['petal_area']
  
  processed_file_path = f"processed/iris_processed_{context['execution_date'].strftime('%Y%m%d')}.csv"
  csv_data = df.to_csv(index=False)
  gcs_hook.upload(bucket_name=CONFIG['bucket_name'], object_name=processed_file_path, data=csv_data)
  
  return processed_file_path


# ------------------------------------------------------------------------
# Section 5: Function to store metrics in a BigQuery table
# ------------------------------------------------------------------------
def store_metrics(**context) -> None:
  bq_hook = BigQueryHook()
  ti = context['task_instance']
  metrics = ti.xcom_pull(task_ids='validate_and_preprocess_data', key='data_metrics')
  execution_date = context['execution_date']
  
  # Save metrics as a list of dictionaries
  rows_to_insert = []
  for metric_name, metric_value in metrics.items():
    rows_to_insert.append({
      "execution_date": execution_date.isoformat(),
      "metric_name": metric_name,
      "metric_value": float(metric_value),
    })
    
  bq_hook.insert_all(
    project_id=CONFIG['project_id'],
    dataset_id=CONFIG['dataset_id'],
    table_id="pipeline_metrics",
    rows=rows_to_insert,
  )
  
# ----------------------------------------------------------------------------------
# Section 6: Function to check the null percentage and volume of data and outliers
# ----------------------------------------------------------------------------------
def check_data_quality(**context) -> str:
  ti = context['task_instance']
  metrics = ti.xcom_pull(task_ids='validate_and_preprocess_data', key='data_metrics')
  
  if metrics['row_count'] < CONFIG['data_quality_thresholds']['min_rows']:
    return 'data_quality_alerts.handle_low_volume'
  elif metrics['null_percentage'] > CONFIG['data_quality_thresholds']['max_null_percentage']:
    return 'data_quality_alerts.handle_null_values'
  elif metrics['outliers_count'] > CONFIG['data_quality_thresholds']['max_outliers']:
    return 'data_quality_alerts.handle_outliers'
  else:
    return 'load_to_bigquery'
  
# ----------------------------------------------------------------------------------
# Section 7: Defining the DAG tasks
# ----------------------------------------------------------------------------------
with dag:
  # A dummy start task
  start = DummyOperator(task_id='start')
  # A dummy end task
  end = DummyOperator(task_id='end',
                       trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
  
  # Task to create metrics table
  create_metrics_table = PythonOperator(
    task_id='create_metrics_table',
    python_callable=create_metrics_table,
  )
  
  # Task to wait for data to be available in GCS using a sensor
  wait_for_data = GCSObjectExistenceSensor(
    task_id='wait_for_data',
    bucket=CONFIG['bucket_name'],
    object=CONFIG['iris_file_name'],
    poke_interval=60,
    timeout=600,
    mode='reschedule',
  )
  
  # Task to validate and preprocess data
  validate_and_preprocess_data = PythonOperator(
    task_id='validate_and_preprocess_data',
    python_callable=validate_and_preprocess_data,
    provide_context=True,
  )
  
  # Task to store metrics in BigQuery
  store_metrics = PythonOperator(
    task_id='store_metrics',
    python_callable=store_metrics,
    provide_context=True,
  )
  
  # Task to check data quality
  check_data_quality = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
  )
  
  # Create a task group for data quality alerts
  with TaskGroup(group_id='data_quality_alerts') as data_quality_alerts:
    handle_null_values = DummyOperator(task_id='handle_null_values')
    handle_low_volume = DummyOperator(task_id='handle_low_volume')
    handle_outliers = DummyOperator(task_id='handle_outliers')
  
  # Define 'load_to_bigquery' task
  load_to_bigquery = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=CONFIG['bucket_name'],
    source_objects=["{{ task_instance.xcom_pull(task_ids='validate_and_preprocess_data') }}"],
    destination_project_dataset_table=f"{CONFIG['project_id']}.{CONFIG['dataset_id']}.{CONFIG['table_id']}",
    schema_fields=[
      {'name': 'sepal_length', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'sepal_width', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'petal_length', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'petal_width', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'species', 'type': 'STRING', 'mode': 'NULLABLE'},
      {'name': 'petal_area', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'sepal_area', 'type': 'FLOAT', 'mode': 'NULLABLE'},
      {'name': 'sepal_petal_ratio', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='CSV',
    autodetect=True,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
  )
    
    
# ----------------------------------------------------------------------------------
# Section 8: Task dependencies
# ----------------------------------------------------------------------------------
start >> create_metrics_table >> wait_for_data >> validate_and_preprocess_data
validate_and_preprocess_data >> [store_metrics, check_data_quality]

# Define downstream tasks for 'check_data_quality'
check_data_quality >> [
  handle_null_values,
  handle_low_volume,
  handle_outliers,
  load_to_bigquery
]

# Each data quality alert task leads to 'end'
handle_null_values >> end
handle_low_volume >> end
handle_outliers >> end

# 'store_metrics' leads to 'end'
store_metrics >> end

# 'load_to_bigquery' leads to 'end'
load_to_bigquery >> end
  
