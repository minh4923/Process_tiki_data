import json
import redis 
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(20255, 10, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),   
}
dag = DAG("tutorial_dag", default_args=default_args, schedule_interval= timedelta(30)) # 30 lan 1 ngay 

def create_tree_data(**kwargs):
    f = open('/opt/airflow/dags/data.json', 'r')
    data=json.load(f)
    f.close()
    tree = [[0 for i in range(150) ] for j in range(50)]
    tree[49][0] = len(data)
    
    for i in range(len(data)):
        tree[i][0] = data[i]["catid"]
        tree[i][1] = data[i]["name"]
        tree[i][2] = data[i]["image"]
        tree[i][3] = data[i]["aasd"]