"""DAG Lakehouse Medallion: 1 Play corre Bronze->Silver->Gold."""
import sys, os
sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../scripts"))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _bronze():
    from bronze_ingest import run_bronze
    return run_bronze(year=2021, limit=100000)
def _silver():
    from silver_transform import run_silver
    return run_silver()
def _gold():
    from gold_agg import run_gold
    return run_gold()

with DAG("lakehouse_medallion", start_date=datetime(2026,1,1),
         schedule=None, catchup=False, tags=["lakehouse"]) as dag:
    b = PythonOperator(task_id="bronze_users_2021", python_callable=_bronze)
    s = PythonOperator(task_id="silver_users_hist", python_callable=_silver)
    g = PythonOperator(task_id="gold_metrics", python_callable=_gold)
    b >> s >> g
