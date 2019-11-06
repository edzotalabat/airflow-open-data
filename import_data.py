"""
DAG pulls a user's profile information from the Moves API.
"""
from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import urllib.request, json

def get_data(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    data = []
    with urllib.request.urlopen("http://opd.it-t.nl/data/amsterdam/ParkingLocation.json") as url:
        parkingData = json.loads(url.read().decode())['features']
    for x in parkingData:
        data.append(x['properties'])
    for x in data:
        name = str(x['Name'])
        date = str(x['PubDate'])
        status = str(x['State'])
        available = str(x['FreeSpaceShort'])
        capacity = str(x['ShortCapacity'])
        insert_cmd = """INSERT INTO parking_data(name, date, status, available, capacity) VALUES(%s, %s, %s, %s, %s);"""

        pg_hook.run(insert_cmd, parameters=([name, date, status, available, capacity]))

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('import_data', default_args=default_args, schedule_interval=timedelta(minutes=10))


get_data_task = \
    PythonOperator(task_id='get_data',
                   provide_context=True,
                   python_callable=get_data,
                   dag=dag)
