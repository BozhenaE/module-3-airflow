from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

username = 'bektova'
default_args = {
    'owner': username,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    username + '.data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks BEktova',
    schedule_interval="0 0 1 1 *",
)
tasks = ('billing', 'issue', 'traffic', 'payment')
ods = []
for task in tasks:
    if task == 'billing':
        query = """
               insert overwrite table bektova.ods_billing partition (year='{{ execution_date.year }}') 
               select user_id, billing_period, service, tariff, cast(sum as DECIMAL(10,2)), cast(created_at as TIMESTAMP) 
                      from bektova.stg_billing where year(created_at) = {{ execution_date.year }};   
            """

    elif task == 'issue':
        query = """
               insert overwrite table bektova.ods_issue partition (year='{{ execution_date.year }}') 
               select cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
                      from bektova.stg_issue where year(start_time) = {{ execution_date.year }};   
            """

    elif task == 'traffic':
        query = """
               insert overwrite table bektova.ods_traffic partition (year='{{ execution_date.year }}') 
               select user_id, from_unixtime(cast(`timestamp`/1000 as int)), device_id, device_ip_addr, bytes_sent, bytes_received 
                      from bektova.stg_traffic where year(from_unixtime(cast(`timestamp`/1000 as int))) = {{ execution_date.year }};   
            """
 
    elif task == 'payment':
        query = """
               insert overwrite table bektova.ods_payment partition (year='{{ execution_date.year }}') 
               select user_id, pay_doc_type, pay_doc_num, account, phone, billing_period, cast(pay_date as DATE), cast(sum as DECIMAL(10,2))
                 from bektova.stg_payment where year(pay_date) = {{ execution_date.year }};   
            """
        
    ods.append(DataProcHiveOperator(
        task_id='ods_' + task,
        dag=dag,
        query=query,
        cluster_name='cluster-dataproc',
        job_name=username + '_{{ execution_date.year }}_ods_' + task + '_{{ params.job_suffix }}',
        params={"job_suffix": randint(0, 100000)},
        region='europe-west3',
    ))
    
dm = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query= """
        insert overwrite table bektova.dm_traffic partition (year='{{ execution_date.year }}')  
        select user_id, max(bytes_received), min(bytes_received), round(avg(bytes_received)) as avg_bytes_received
        from bektova.ods_traffic where year = {{ execution_date.year }} group by user_id order by avg_bytes_received;    
        """,
    cluster_name='cluster-dataproc',
    job_name=username + '_{{ execution_date.year }}_dm_traffic_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
    )
ods >> dm
