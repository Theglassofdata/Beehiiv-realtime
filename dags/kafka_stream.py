import uuid
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

default_args = {
    'owner': 'Rahul',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def get_data():
    """Fetch random user data from API"""
    res = requests.get("https://randomuser.me/api/")
    res.raise_for_status()
    return res.json()['results'][0]

def format_data(res):
    """Format the user data into the desired structure"""
    location = res['location']
    return {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'post_code': str(location['postcode']),
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

def test_kafka_connection():
    """Verify Kafka connection inside Docker"""
    try:
        admin = KafkaAdminClient(bootstrap_servers='broker:29092', request_timeout_ms=5000)
        topics = admin.list_topics()
        logging.info(f"Kafka topics found: {topics}")
        return True
    except Exception as e:
        logging.error(f"Kafka connection failed: {e}")
        return False

def create_topic():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers='broker:29092', request_timeout_ms=5000)
        existing_topics = admin_client.list_topics()
        topic_name = "beehiiv_users_activity"

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
            logging.info(f"Topic '{topic_name}' created successfully")
        else:
            logging.info(f"Topic '{topic_name}' already exists")
    except Exception as e:
        logging.error(f"Kafka topic creation failed: {e}")
        raise

def stream_data():
    """Stream formatted user data to Kafka"""
    if not test_kafka_connection():
        raise Exception("Cannot connect to Kafka broker")
    
    create_topic()
    
    producer = KafkaProducer(
        bootstrap_servers='broker:29092',
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    curr_time = time.time()
    message_count = 0

    while time.time() < curr_time + 60:  # Run for 1 minute
        try:
            res = get_data()
            formatted_data = format_data(res)
            producer.send('beehiiv_users_activity', formatted_data).get(timeout=10)
            message_count += 1
            logging.info(f"Sent message {message_count}: {formatted_data['id']}")
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"Error while streaming data: {e}")
            continue
    
    producer.flush()
    logging.info(f"Completed streaming {message_count} messages to Kafka")

with DAG(
    'beehiiv_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Stream random user data to Kafka',
    tags=['kafka', 'streaming']
) as dag:

    connection_test_task = PythonOperator(
        task_id='test_kafka_connection',
        python_callable=test_kafka_connection
    )
    
    topic_creation_task = PythonOperator(
        task_id='create_kafka_topic',
        python_callable=create_topic
    )
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    connection_test_task >> topic_creation_task >> streaming_task
