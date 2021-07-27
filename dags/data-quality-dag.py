from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup

import hashlib
import json
import logging

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
# CSV_FILE_NAME = "forestfires.csv"
CSV_FILE_NAME = "forestfires_invalid.csv"
CSV_FILE_PATH = f"include/sample_data/{CSV_FILE_NAME}"


default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": True
}

with DAG("simple_el_dag_3",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3 and then Redshift, with data integrity and quality checks.",
         schedule_interval=None,
         template_searchpath='/usr/local/airflow/include',
         catchup=False) as dag:

    @task
    def upload_to_s3():
        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        aws_configs = Variable.get("aws_configs", deserialize_json=True)
        s3_bucket = aws_configs.get("s3_bucket")
        s3_key = aws_configs.get("s3_key_prefix") + "/" + CSV_FILE_PATH
        s3 = S3Hook()
        s3.load_file(CSV_FILE_PATH, s3_key, bucket_name=s3_bucket, replace=True)
        return { "s3_bucket": s3_bucket, "s3_key": s3_key }

    @task
    def validate_etag(s3_data):
        """
        #### Validation task
        Check the destination ETag against the local MD5 hash to ensure the file
        was uploaded without errors.
        """
        s3 = S3Hook()
        obj = s3.get_key(key=s3_data.get("s3_key"), bucket_name=s3_data.get("s3_bucket"))
        obj_etag = obj.e_tag.strip('"')
        file_hash = hashlib.md5(open(CSV_FILE_PATH).read().encode("utf-8")).hexdigest()
        if obj_etag != file_hash:
            raise AirflowException(f"Upload Error: Object ETag in S3 did not match hash of local file.")
        return s3_data

    upload_file = upload_to_s3()
    validate_file = validate_etag(upload_file)

    create_redshift_table = PostgresOperator(
        task_id='create_table',
        sql="sql/create_forestfire_table.sql",
        postgres_conn_id="redshift_default",
        params={"redshift_table": Variable.get("aws_configs", deserialize_json=True).get("redshift_table")}
    )

    """
    #### Second load task
    Loads the S3 data from the previous load to a Redshift table (specified
    in the Airflow Variables backend).
    """
    load_to_redshift = S3ToRedshiftOperator(
        s3_bucket=Variable.get("aws_configs", deserialize_json=True).get("s3_bucket"),
        s3_key=f'{Variable.get("aws_configs", deserialize_json=True).get("s3_key_prefix")}/{CSV_FILE_PATH}',
        schema="PUBLIC",
        table=Variable.get("aws_configs", deserialize_json=True).get("redshift_table"),
        copy_options=['csv'],
        task_id='load_to_redshift',
    )

    """
    #### Redshift row validation task
    Ensure that data was copied to Redshift from S3 correctly. A SQLCheckOperator is
    used here to check for any files in the stl_load_errors table.
    """
    validate_redshift = SQLCheckOperator(
        task_id="validate_redshift",
        conn_id="redshift_default",
        sql="sql/validate_forestfire_redshift_load.sql",
        params={"filename": CSV_FILE_NAME},
    )

    """
    #### Row-level data quality check
    Run a data quality check on a few rows, ensuring that the data in Redshift
    matches the ground truth in the correspoding JSON file.
    """
    with open("include/validation/forestfire_validation.json") as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                values["id"] = id
                values["redshift_table"] = Variable.get("aws_configs", deserialize_json=True).get("redshift_table")
                SQLCheckOperator(
                    task_id=f"forestfire_row_quality_check_{id}",
                    conn_id="redshift_default",
                    sql="sql/forestfire_row_quality_check.sql",
                    params=values,
                )

    done = DummyOperator(task_id="done")

    validate_file >> create_redshift_table
    create_redshift_table >> load_to_redshift
    load_to_redshift >> validate_redshift
    validate_redshift >> quality_check_group
    quality_check_group >> done