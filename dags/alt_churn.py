# dags/alt_churn.py

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pandas as pd
with DAG(
    dag_id='alt_churn',
    schedule='@once',
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Test"]) as dag:
    
    step1 = PythonOperator(task_id='1', python_callable=create_table)
    step2 = PythonOperator(task_id='2', python_callable=extract)
    step3 = PythonOperator(task_id='3', python_callable=transform)
    step4 = PythonOperator(task_id='4', python_callable=load)

    step1 >> step2 >> step3 >> step4
