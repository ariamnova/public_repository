#!/usr/bin/env python
# coding: utf-8

import pendulum
import psycopg2
import datetime
import logging
import pandas as pd
import pendulum
import vertica_python
import contextlib
import os

from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


#vars
VERTICA_HOST  = Variable.get("VERTICA_HOST")
VERTICA_PORT  = Variable.get("VERTICA_PORT")
VERTICA_USER  = Variable.get("VERTICA_USER")
VERTICA_PASSWORD  = Variable.get("VERTICA_PASSWORD")
DB_SCHEMA = Variable.get("DB_SCHEMA")
VERTICA_STAGING = Variable.get("VERTICA_STAGING")

log = logging.getLogger(__name__)

#update dwh
def dwh_updates():

    conn_usr = {'host': VERTICA_HOST ,
                'port': VERTICA_PORT,
                'user': VERTICA_USER,
                'password': VERTICA_PASSWORD,
                'database': DB_SCHEMA,
                'autocommit': True
        }
    vertica_conn = vertica_python.connect(**conn_usr)
    
    with open(os.path.join(os.getcwd(), '/lessons/dags/sql', 'global_metrics_view.sql'), 'r') as f:
        expr_merge = f.read()
    
    cur = vertica_conn.cursor()
    cur.execute(expr_merge)
    logging.info(f'The view was updated')
    vertica_conn.close()


default_args = {
    "start_date": datetime(2023, 3, 21),
    "owner": "airflow",
    "conn_id": "postgres_default"
}



with DAG(dag_id = "2_datamart_update",
         schedule_interval = "@daily",
         default_args = default_args,
         catchup = False) as dag:
    
    #waiting for the data uploads
    externalsensor = ExternalTaskSensor(
        task_id='download_s3_files',
        external_dag_id='get_raw_data',
        external_task_id=None,  # wait for whole DAG to complete
        check_existence=True,
        timeout=240)
    
    
    
    dwh_updates = PythonOperator(
            task_id='dwh_updates',
            python_callable=dwh_updates
    )
    
    (
        externalsensor >> 
        dwh_updates
    )

