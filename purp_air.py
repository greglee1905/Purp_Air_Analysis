import datetime as dt
import pandas as pd
import requests
import json
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def purp_air():

    #Get the Date
    now = dt.datetime.now()
    t = now.strftime("%m_%d_%H_%M")

    #Legacy Data
    url = "https://www.purpleair.com/json"
    data = json.load(urlopen(url))
    query = pd.DataFrame(data)
    features = query.results.apply(pd.Series)
    raw_data = query[['mapVersion','baseVersion','mapVersionString']]
    query = pd.concat([raw_data,features],axis =1)
    query.to_pickle('/home/boogie2/external/tiny_tower_2/purpleair_data/legacy_' + str(t) + '.pkl')

    return 'legacy data acquired'

def purp_air_exp():

    #Get the Date
    now = dt.datetime.now()
    t = now.strftime("%m_%d_%H_%M")

    #Experimental Data
    url = "https://www.purpleair.com/data.json"
    exp = json.load(urlopen(url))

    exp_df = pd.DataFrame(exp['data'],columns = exp['fields'])
    exp_df['version'] = exp['version']
    exp_df['count']= exp['count']
    exp_df.to_pickle('/home/boogie2/external/tiny_tower_2/purpleair_data/experimental_' + str(t) + '.pkl')

    return 'experimental data acquired'

#Airflow stuff
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 4, 6, 21, 18, 00),
    'concurrency': 1,
    'retries': 3,
    'retry_delay': dt.timedelta(seconds = 60)
}

with DAG('purp_air',
         catchup=False,
         default_args=default_args,
         schedule_interval='@hourly'
         ) as dag:

    opr_purp_air = PythonOperator(task_id='legacy',
                               python_callable=purp_air)

    opr_purp_air_exp = PythonOperator(task_id='experimental',
                               python_callable=purp_air_exp)
