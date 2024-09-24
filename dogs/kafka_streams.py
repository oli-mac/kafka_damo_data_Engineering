from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    # 'depends_on_past': False,
    'start_date': datetime(2024, 9, 24, 10, 00, 00),
    # 'email': [''],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

def get_data():
    import json
    import requests
    
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=4))
    
    return res
    
def format_data(res):
    data = {}
    location = res['location']
    data['name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{res['location']['street']['number']}, {res['location']['street']['name']},{res['location']['city'] }, { res['location']['country'] }, {res['location']['state']}"
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['registered_date'] = res['registered']['date']
    data['picture'] = res['picture']['large']
    
    return data
    
    

def streaming_data():
    import json
    from kafka import KafkaProducer
    import time
    
    
    res= get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=4))
    
    producer  = KafkaProducer( bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    
    producer.send('kafka_damo_data_Engineering', json.dumps(res).encode('utf-8'))
    

with DAG(
        'kafka_damo_data_Engineering',
        default_args=default_args,
        description='kafka_damo_data_Engineering',
        schedule_interval='@daily',
        catchup=False,
        tags=['kafka_damo_data_Engineering'],
) as dag:
    streaming_task = PythonOperator(
        task_id='kafka_damo_data_Engineering',
        python_callable=streaming_data,
    )
    
streaming_data();