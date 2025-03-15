# dags/outflow.py

import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id='outflow',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_outflow_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint

        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        users_outflow_table = Table(
            'users_outflow',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('dependents', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='unique_custome_id')
        )
        if not sqlalchemy.inspect(conn).has_table(users_outflow_table.name): 
            metadata.create_all(conn)

    @task()
    def extract(**kwargs):

        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.dependents,
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="users_outflow",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

prepare_outflow_dataset()
