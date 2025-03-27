from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import logging
import snowflake.connector

def return_snowflake_conn():
    # Initialize the SnowflakeHook using your connection ID: snowflake_default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, dest_schema, table, select_sql, primary_key=None):

    logging.info(f"Creating table: {table} in {database}.{dest_schema}")
    logging.info(f"Select SQL: {select_sql}")

    cur = return_snowflake_conn()

    try:
        # Create a temporary table with the joined results
        sql = f"CREATE OR REPLACE TABLE {database}.{dest_schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Primary key uniqueness check on the temporary table
        if primary_key is not None:
            sql_pk = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{dest_schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1
            """
            logging.info(sql_pk)
            cur.execute(sql_pk)
            result = cur.fetchone()
            logging.info(f"Primary key check result: {result}")
            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Create the main table if it doesn't exist (structure only)
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{dest_schema}.{table} AS
            SELECT * FROM {database}.{dest_schema}.temp_{table} WHERE 1=0;
        """
        cur.execute(main_table_creation_if_not_exists_sql)

        # Swap the main table with the temporary table
        swap_sql = f"""ALTER TABLE {database}.{dest_schema}.{table} SWAP WITH {database}.{dest_schema}.temp_{table};"""
        cur.execute(swap_sql)

        # Additional duplicate records check on the final table
        if primary_key is not None:
            dup_sql = f"""
              SELECT COUNT(*) - COUNT(DISTINCT {primary_key}) AS duplicate_count
              FROM {database}.{dest_schema}.{table};
            """
            logging.info(dup_sql)
            cur.execute(dup_sql)
            dup_result = cur.fetchone()
            logging.info(f"Duplicate check result: {dup_result}")
            if int(dup_result[0]) > 0:
                raise Exception(f"Duplicate records found in {table}: {dup_result[0]} duplicates")

    except Exception as e:
        logging.error(f"Error in run_ctas task: {e}")
        raise

with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *'
) as dag:

    database = "USER_DB_GATOR"
    # Destination schema for the joined table is analytics
    dest_schema = "analytics"
    table = "session_summary"
    # Source tables remain in the raw schema
    select_sql = """SELECT u.*, s.ts
    FROM USER_DB_GATOR.raw.user_session_channel u
    JOIN USER_DB_GATOR.raw.session_timestamp s ON u.sessionId = s.sessionId
    """

    run_ctas(database, dest_schema, table, select_sql, primary_key='sessionId')
