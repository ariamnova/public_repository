#!/usr/bin/env python
# coding: utf-8


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag
from airflow.models import Variable
from airflow.models.xcom import XCom

import boto3
import pendulum
from datetime import datetime
import logging
from typing import List, Dict
from typing import Optional
import sqlalchemy as sa
from sqlalchemy import types
import pandas as pd
import pendulum
import vertica_python
import contextlib

#get vars
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
VERTICA_HOST  = Variable.get("VERTICA_HOST")
VERTICA_PORT  = Variable.get("VERTICA_PORT")
VERTICA_USER  = Variable.get("VERTICA_USER")
VERTICA_PASSWORD  = Variable.get("VERTICA_PASSWORD")
DB_SCHEMA = Variable.get("DB_SCHEMA")
VERTICA_STAGING = Variable.get("VERTICA_STAGING")

bucket = 'final-project'

#get file names - вот тут не очень понятно, как решить проблему: я получаю полный список файлов
#ну и их всегда 0... 10 - то есть другие не добавляются (они только за одну дату!!!). по-хорошему (при обновлении) сохранять
#название последнего загруженного файла, и по итеративному номеру брать все с него
#но сколько бы раз я не запрашивала данные - там всего все те же 10 файлов
def get_file_folders(bucket_name, prefix=""):
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    
    file_names = []

    default_kwargs = {
        "Bucket": bucket_name,
        "Prefix": prefix
    }
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**updated_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names

#load files from s3
def fetch_s3_file(bucket: str, key: str, filename: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    s3_client.download_file(
        Bucket=bucket, #'sprint6',
        Key=key, #'groups.csv',
        Filename=f'{filename}'
)


def get_files():
    bucket = 'final-project'
    bucket_files = get_file_folders(bucket)
    for filename in bucket_files:
        key = filename
        print('FILENAME IS', filename)
        logging.info(f'filename: {filename}')
        fetch_s3_file(bucket, key, filename)
    return bucket_files

#load currencies to vertica
def load_currencies_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,):

    df = pd.read_csv(dataset_path, dtype=type_override)
    
    num_rows = len(df)
    conn_usr = {'host': VERTICA_HOST ,
                'port': VERTICA_PORT,
                'user': VERTICA_USER,
                'password': VERTICA_PASSWORD,
                'database': DB_SCHEMA,
                'autocommit': True
        }
    vertica_conn = vertica_python.connect(**conn_usr)
    cur = vertica_conn.cursor()
    #get the max uploaded date from currencies
    select_exp = f"""
        SELECT max(date_update) FROM {schema}.{table}
    """
    cur.execute(select_exp)
    result = (cur.fetchone())[0].strftime("%Y-%m-%d %H:%M:%S")
    filtered_df = df.query(f"date_update > '{result}'")
    
    if len(filtered_df) == 0:
        print(f'No new rows to upload to {schema}.{table}')
    else:
        df = filtered_df.copy()
        columns = ', '.join(columns)
        copy_expr = f"""
        COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
        """

        chunk_size = num_rows // 100
        with contextlib.closing(vertica_conn.cursor()) as cur:
            start = 0
            while start <= num_rows:
                end = min(start + chunk_size, num_rows)
                if end == num_rows:
                    end-=1
                print('START AND END', start, end)
                print(f"loading rows {start}-{end}")
                df_chunk=df.iloc[start:end]
                cur.copy(copy_expr, df_chunk.to_csv(header=None, index=False))   
                print("loaded")
                start += chunk_size
    vertica_conn.close()
    

#New part - for transactions
def load_transactions_to_vertica(
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,):
    
    bucket_files = ti.xcom_pull(task_ids='get_files')
    #get vertica connection
    conn_usr = {'host': VERTICA_HOST ,
                'port': VERTICA_PORT,
                'user': VERTICA_USER,
                'password': VERTICA_PASSWORD,
                'database': DB_SCHEMA,
                'autocommit': True
    }
    vertica_conn = vertica_python.connect(**conn_usr)
    cur = vertica_conn.cursor()
    #get the max uploaded date from transactions
    select_exp = f"""
        SELECT max(transaction_dt) FROM {schema}.{table}
    """
    cur.execute(select_exp)
    result = (cur.fetchone())[0].strftime("%Y-%m-%d %H:%M:%S")
    
    columns = ', '.join(columns)
    
    #loaded only new rows: there are always 10 files, but data in them should be uploaded daily
    #=> same files, but new data
    #ПРОБЛЕМА: в S3 данные только за часть одного дня - куратор пытается разобраться с авторами,
    #но пока ничего не выходит, поэтому данных на загрузку преступно мало
    
    for dataset_path in bucket_files:
        df = pd.read_csv(dataset_path, dtype=type_override)
        filtered_df = df.query(f"date_update > '{result}'")
        
        if len(filtered_df) == 0:
            print(f'No new rows to upload to {schema}.{table}')
        else:
            df = filtered_df.copy()
            num_rows = len(df)
            copy_expr = f"""
            COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
            """

            chunk_size = num_rows // 100
            with contextlib.closing(vertica_conn.cursor()) as cur:
                start = 0
                while start <= num_rows:
                    end = min(start + chunk_size, num_rows)
                    if end == num_rows:
                        end-=1
                    print('START AND END', start, end)
                    print(f"loading rows {start}-{end}")
                    df_chunk=df.iloc[start:end]
                    cur.copy(copy_expr, df_chunk.to_csv(header=None, index=False))   
                    print("loaded")
                    start += chunk_size
            vertica_conn.close()
        

default_args = {
    "start_date": datetime(2023, 3, 21),
    "owner": "airflow",
    "conn_id": "postgres_default"
}



with DAG(dag_id = "1_data_import",
         schedule_interval = "@daily",
         default_args = default_args,
         catchup = False) as dag:

    begin = DummyOperator(task_id="begin")
    
    get_files = PythonOperator(
        task_id='get_files',
        python_callable=get_files,
        do_xcom_push = True
    )
    
    load_currencies_to_vertica = PythonOperator(
            task_id='load_currencies_to_vertica',
            python_callable=load_currencies_to_vertica,
            op_kwargs={
                'dataset_path': 'currencies_history.csv',  
                'schema': VERTICA_STAGING,
                'table': 'currencies', 
                'columns': ['currency_code', 'currency_code_with', 'date_update', 'currency_code_div'],
            },
    )
    
    load_transactions_to_vertica = PythonOperator(
            task_id='load_transactions_to_vertica',
            python_callable=load_transactions_to_vertica,
            op_kwargs={
                'schema': VERTICA_STAGING,
                'table': 'transactions', 
                'columns': ['operation_id', 'account_number_from', 'account_number_to', 'currency_code', 'country', 'status', 'transaction_type', 'amount', 'transaction_dt'],
            },
    )
    
    end = DummyOperator(task_id="end")

    begin >> get_files >> load_currencies_to_vertica >> load_transactions_to_vertica >> end
