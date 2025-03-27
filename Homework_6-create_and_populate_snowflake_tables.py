# -*- coding: utf-8 -*-
"""import_tables_to_snowflake.py - Create and Populate Snowflake Tables using Airflow DAG"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define Snowflake connection parameters
SNOWFLAKE_CONN_ID = 'snowflake_default'

with DAG(
    dag_id='create_and_populate_snowflake_tables',
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=['snowflake', 'airflow', 'etl']
) as dag:

    # Task 1: Create user_session_channel table in USER_DB_GATOR.raw
    create_user_session_channel = SnowflakeOperator(
        task_id='create_user_session_channel',
        sql="""
        CREATE TABLE IF NOT EXISTS USER_DB_GATOR.raw.user_session_channel (
            userId int NOT NULL,
            sessionId varchar(32) PRIMARY KEY,
            channel varchar(32) DEFAULT 'direct'
        );
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # Task 2: Create session_timestamp table in USER_DB_GATOR.raw
    create_session_timestamp = SnowflakeOperator(
        task_id='create_session_timestamp',
        sql="""
        CREATE TABLE IF NOT EXISTS USER_DB_GATOR.raw.session_timestamp (
            sessionId varchar(32) PRIMARY KEY,
            ts timestamp
        );
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # Task 3: Create blob_stage to access S3 bucket in USER_DB_GATOR.raw
    create_blob_stage = SnowflakeOperator(
        task_id='create_blob_stage',
        sql="""
        -- Ensure the S3 bucket has LIST/READ privileges for everyone
        CREATE OR REPLACE STAGE USER_DB_GATOR.raw.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # Task 4: Load user_session_channel data from S3 to Snowflake
    load_user_session_channel = SnowflakeOperator(
        task_id='load_user_session_channel',
        sql="""
        COPY INTO USER_DB_GATOR.raw.user_session_channel
        FROM @USER_DB_GATOR.raw.blob_stage/user_session_channel.csv;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # Task 5: Load session_timestamp data from S3 to Snowflake
    load_session_timestamp = SnowflakeOperator(
        task_id='load_session_timestamp',
        sql="""
        COPY INTO USER_DB_GATOR.raw.session_timestamp
        FROM @USER_DB_GATOR.raw.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # Define task execution order
    [create_user_session_channel, create_session_timestamp] >> create_blob_stage >> [load_user_session_channel, load_session_timestamp]
