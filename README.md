# Airflow DAG for Iris Dataset Preprocessing
### Introduction
This lab focuses on building an Airflow DAG that preprocesses the Iris dataset. The pipeline involves validating and preprocessing data, performing data quality checks, storing metrics, and loading the processed data into BigQuery. The DAG demonstrates best practices in structuring tasks, handling dependencies, and utilizing Airflow's features for efficient workflow management.

### Learning Objectives
In this lab, you will learn how to:
- Use Airflow Variables and the `get` function to define and manage environment variables in a DAG.
- Create an empty table in BigQuery using the BigQuery Hook for storing metrics.
- Understand the context argument passed by Airflow to functions and identify useful parameters within it.
- Utilize Task Instances, XComs, and the push and pull methods for between task communication.
- Implement sensors like GCSObjectExistenceSensor in Airflow and understand their role in DAG structures.
- Use TaskGroup for better organization within a DAG.
- Design a DAG with proper task dependencies, ensuring all execution paths conclude appropriately.

-----
### Using Airflow Variables
We use Variable.get to retrieve configuration parameters stored as Airflow Variables. This approach allows us to define environment variables programmatically.
```python
CONFIG = Variable.get("iris_preprocessing_config", deserialize_json=True, default_var={
  "bucket_name": "your-bucket-name",
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
```
### Creating an Empty Metrics Table in BigQuery
The `create_metrics_table` function utilizes the `BigQueryHook` to create an empty table for storing metrics. This table will hold information like execution date, metric names, and metric values.
```python
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
```
### Understanding the `context` Argument
Airflow passes a `context` dictionary to tasks by default when `provide_context=True` is set. This dictionary contains useful information such as:

`execution_date`: The logical date and time of DAG execution.
`task_instance (ti)`: An instance of the task being executed, which allows for XCom operations.
By accessing context, we can interact with the task's execution environment programmatically.
### Function Roles
`validate_and_preprocess_data`:
This function performs the following:

- Downloads the Iris dataset from GCS.
- Calculates metrics like row count, null percentage, and outlier count.
- Pushes metrics to XCom for downstream tasks.
- Preprocesses the data (normalization and feature engineering).
- Uploads the processed data back to GCS.

`store_metrics`:
Stores the metrics collected during validation into the BigQuery `pipeline_metrics` table.

`check_data_quality`:
Checks if the data meets some predefined thresholds and decides the next task to execute based on the results.

### Using Task Instance and XCom
We use the task instance `(ti)` from the `context` to push and pull data between tasks using XCom:
- **Push Metrics:**
```python
ti = context['task_instance']
ti.xcom_push(key='data_metrics', value=metrics)
```
- **Pull Metrics:**
```python
metrics = ti.xcom_pull(task_ids='validate_and_preprocess_data', key='data_metrics')
```
This mechanism enables us to share data between tasks without external save of the data.

### Sensors in Airflow
We use the `GCSObjectExistenceSensor` to wait for the Iris dataset file to be available in `GCS` before proceeding:
```python
wait_for_data = GCSObjectExistenceSensor(
  task_id='wait_for_data',
  bucket=CONFIG['bucket_name'],
  object=CONFIG['iris_file_name'],
  poke_interval=60,
  timeout=600,
  mode='reschedule',
)
```
Sensors are essential for synchronizing tasks with external events or data availability.

### DAG Structure
The DAG is structured to handle data ingestion, preprocessing, validation, and loading:

1. Start: Begins the DAG execution.
2. Create Metrics Table: Ensures the metrics table exists in BigQuery.
3. Wait for Data: Uses a sensor to wait for the dataset in GCS.
4. Validate and Preprocess Data: Performs data validation and preprocessing.
5. Store Metrics: Saves metrics to BigQuery.
6. Check Data Quality: Determines the next steps based on data quality.
7. Data Quality Alerts: Handles any data quality issues.
8. Load to BigQuery: Loads the processed data into BigQuery.
9. End: Marks the end of the DAG execution.

### TaskGroup Usage
We use `TaskGroup` to group data quality alert tasks:
```python
with TaskGroup(group_id='data_quality_alerts') as data_quality_alerts:
  handle_null_values = DummyOperator(task_id='handle_null_values')
  handle_low_volume = DummyOperator(task_id='handle_low_volume')
  handle_outliers = DummyOperator(task_id='handle_outliers')
```
This grouping improves readability and organization of related tasks in the DAG.

### Task Dependencies
We carefully manage task dependencies to ensure proper execution flow:

- `start` leads to Create Metrics Table.
- `create_metrics_table` leads to `wait_for_data`.
- `wait_for_data` leads to `validate_and_preprocess_data`.
- `validate_and_preprocess_data` leads to both `store_metrics` and `check_data_quality`.
- `check_data_quality` branches to either `data_quality_alerts` tasks or `load_to_bigquery`.
- All paths eventually lead to `end`, ensuring that every execution path concludes properly.