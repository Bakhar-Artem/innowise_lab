from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from snowflake.connector.pandas_tools import pd_writer

# start sql scripts
sql_create_raw_table = """create or replace TABLE TASK6.PUBLIC.RAW_TABLE (
	ID VARCHAR(16777216),
	IOS_APP_ID NUMBER(38,0),
	TITLE VARCHAR(16777216),
	DEVELOPER_NAME VARCHAR(16777216),
	DEVELOPER_IOS_ID FLOAT,
	IOS_STORE_URL VARCHAR(16777216),
	SELLER_OFFICIAL_WEBSITE VARCHAR(16777216),
	AGE_RATING VARCHAR(16777216),
	TOTAL_AVERAGE_RATING FLOAT,
	TOTAL_NUMBER_OF_RATINGS NUMBER(38,0),
	AVERAGE_RATING_FOR_VERSION FLOAT,
	NUMBER_OF_RATINGS_FOR_VERSION NUMBER(38,0),
	ORIGINAL_RELEASE_DATE VARCHAR(16777216),
	CURRENT_VERSION_RELEASE_DATE VARCHAR(16777216),
	PRICE_USD FLOAT,
	PRIMARY_GENRE VARCHAR(16777216),
	ALL_GENRES VARCHAR(16777216),
	LANGUAGES VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216)
);"""
sql_create_raw_stream = """create or replace stream TASK6.PUBLIC.RAW_STREAM on table RAW_TABLE;"""
sql_create_stage_table = """create or replace TABLE TASK6.PUBLIC.STAGE_TABLE (
	ID VARCHAR(16777216),
	IOS_APP_ID NUMBER(38,0),
	TITLE VARCHAR(16777216),
	DEVELOPER_NAME VARCHAR(16777216),
	DEVELOPER_IOS_ID FLOAT,
	IOS_STORE_URL VARCHAR(16777216),
	SELLER_OFFICIAL_WEBSITE VARCHAR(16777216),
	AGE_RATING VARCHAR(16777216),
	TOTAL_AVERAGE_RATING FLOAT,
	TOTAL_NUMBER_OF_RATINGS NUMBER(38,0),
	AVERAGE_RATING_FOR_VERSION FLOAT,
	NUMBER_OF_RATINGS_FOR_VERSION NUMBER(38,0),
	ORIGINAL_RELEASE_DATE VARCHAR(16777216),
	CURRENT_VERSION_RELEASE_DATE VARCHAR(16777216),
	PRICE_USD FLOAT,
	PRIMARY_GENRE VARCHAR(16777216),
	ALL_GENRES VARCHAR(16777216),
	LANGUAGES VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	METADATA_ACTION_RAW VARCHAR(6),
	METADATA_ISUPDATE_RAW BOOLEAN,
	METADATA_ROW_ID_RAW VARCHAR(40)
);"""
sql_create_stage_stream = """create or replace stream TASK6.PUBLIC.STAGE_STREAM on table STAGE_TABLE;"""
sql_get_copy_raw_stream = """select * from raw_stream"""
sql_create_master_table = """create or replace TABLE TASK6.PUBLIC.MASTER_TABLE (ID VARCHAR(16777216));"""
sql_copy_stream_to_master = """create or replace table master_table as select * from stage_stream;"""
# end sql scripts

default_args = {
    'start_date': datetime(2022, 10, 3, hour=11, minute=14),
    'owner': 'airflow'
}
dag = DAG('snowflake', default_args=default_args,
          schedule=timedelta(days=1))


def get_engine():
    """
    Method is used to get sqlalchemy engine for python connector
    :return: engine
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    engine = hook.get_sqlalchemy_engine()
    return engine


def read_by_part():
    """
    Method reads dataframe by chunk and yield read chunks
    :return: pandas dataframe
    """
    chunksize = 10 ** 5
    for chunk in pd.read_csv('/home/artem/python/task6/data/763K_plus_IOS_Apps_Info.csv', chunksize=chunksize):
        yield chunk


def read_from_sql_by_part():
    """
    Method reads date from sql by chunk and yield read chunks
    :return: pandas dataframe
    """
    chunksize = 10 ** 5
    for chunk in pd.read_sql(sql=sql_get_copy_raw_stream, con=get_engine(), chunksize=chunksize):
        yield chunk


def push_dataframe():
    """
    Method is used to push date to snowflake table using sqlalchemy transaction
    :return:none
    """
    with get_engine().begin() as con:
        for frame in read_by_part():
            dataframe = frame.rename(columns={'_id': 'id'})
            dataframe.columns = dataframe.columns.str.upper()
            dataframe.to_sql(name='raw_table', con=con, method=pd_writer, index=False, if_exists='append')


def push_raw_stream():
    """
    Method is used to push date to snowflake table using sqlalchemy transaction
    :return: none
    """
    with get_engine().begin() as con:
        for frame in read_from_sql_by_part():
            frame.columns = frame.columns.str.upper()
            frame = frame.rename(
                columns={'METADATA$ACTION': 'METADATA_ACTION_RAW', 'METADATA$ISUPDATE': 'METADATA_ISUPDATE_RAW',
                         'METADATA$ROW_ID': 'METADATA_ROW_ID_RAW'})
            frame.to_sql(name='stage_table', con=con, method=pd_writer, index=False, if_exists='append')


# creating tables
with TaskGroup(group_id='create_tables', dag=dag) as tg1:
    task_create_raw_table = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_create_raw_table,
                                              dag=dag,
                                              task_id='raw_table')
    task_create_stage_table = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_create_stage_table,
                                                dag=dag, task_id='stage_table')
    task_create_master_table = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_create_master_table,
                                                 dag=dag, task_id='master_table')
# creating streams
with TaskGroup(group_id='create_streams', dag=dag) as tg2:
    task_create_raw_stream = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_create_raw_stream,
                                               dag=dag, task_id='raw_stream')

    task_create_stage_stream = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_create_stage_stream,
                                                 dag=dag, task_id='stage_stream')

# push date to tables
task_push_dataframe = PythonOperator(task_id='push_dataframe', dag=dag, python_callable=push_dataframe)
task_push_stream = PythonOperator(task_id='push_stream', dag=dag, python_callable=push_raw_stream)

# copy from stream to table
task_copy_to_master = SnowflakeOperator(snowflake_conn_id='snowflake_connection', sql=sql_copy_stream_to_master,
                                        dag=dag, task_id='master')

# dependencies
tg1 >> tg2 >> task_push_dataframe >> task_push_stream >> task_copy_to_master
