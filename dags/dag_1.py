from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests


def fetch_pro_matches():
    url = "https://api.opendota.com/api/proMatches"
    response = requests.get(url)
    if response.status_code == 200:
        matches = response.json()
        # Here you can process the matches or save them to a database
        print(f"Fetched {len(matches)} matches.")
    else:
        print("Failed to fetch matches")
    return matches


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_pro_matches_dag',
          default_args=default_args,
          description='A simple DAG to fetch pro matches from OpenDota',
          schedule_interval=timedelta(days=1),
          )

t1 = PythonOperator(
    task_id='fetch_pro_matches',
    python_callable=fetch_pro_matches,
    dag=dag,
)
