import json
import logging
import time
from kafka import KafkaConsumer
from clickhouse_driver import Client
from uuid import UUID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_clickhouse_client():
    """Create and return a ClickHouse client."""
    try:
        # When running locally on MacOS, use localhost
        client = Client(host='localhost', port=9000, user='default', password='password', database='default')
        logging.info("ClickHouse connection established successfully!")
        return client
    except Exception as e:
        logging.error(f"Could not create ClickHouse connection: {e}")
        return None

def setup_clickhouse(client):
    """Set up the ClickHouse database and table with post_code as String."""
    try:
        client.execute("CREATE DATABASE IF NOT EXISTS beehiiv_db")
        logging.info("Database 'beehiiv_db' created successfully!")
        
        # Drop existing table if it exists (to change schema)
        client.execute("DROP TABLE IF EXISTS beehiiv_db.beehiiv_users_activity")
        
        # Create table with post_code as String instead of Int32
        client.execute("""
            CREATE TABLE beehiiv_db.beehiiv_users_activity (
                id UUID,
                first_name String,
                last_name String,
                gender String,
                address String,
                post_code String,
                email String,
                username String,
                registered_date String,
                phone String,
                picture String
            ) ENGINE = MergeTree()
            ORDER BY id
        """)
        logging.info("Table 'beehiiv_users_activity' created successfully with post_code as String!")
    except Exception as e:
        logging.error(f"Failed to set up ClickHouse: {e}")
        raise

def create_kafka_consumer():
    """Create and return a Kafka consumer."""
    try:
        consumer = KafkaConsumer(
            'beehiiv_users_activity',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='beehiiv-clickhouse-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info("Kafka consumer created successfully!")
        return consumer
    except Exception as e:
        logging.error(f"Could not create Kafka consumer: {e}")
        return None

def validate_and_convert_record(record):
    """Validate and convert record values to the correct types."""
    try:
        # Ensure id is a valid UUID string
        if 'id' not in record or not record['id']:
            logging.warning("Record missing id, skipping")
            return None

        # Keep post_code as string (no conversion needed)
        if 'post_code' not in record or record['post_code'] is None:
            record['post_code'] = ''  # Empty string if missing or None

        # Ensure all string fields are not None
        string_fields = ['first_name', 'last_name', 'gender', 'address', 
                         'email', 'username', 'registered_date', 'phone', 'picture']
        for field in string_fields:
            if field not in record or record[field] is None:
                record[field] = ''  # Empty string for missing string fields
                
        return record
    except Exception as e:
        logging.error(f"Error validating record: {e}, record: {record}")
        return None

def write_to_clickhouse(client, records):
    """Write records to ClickHouse."""
    if not records:
        return
    
    try:
        # Validate and convert each record
        valid_records = []
        for record in records:
            validated_record = validate_and_convert_record(record)
            if validated_record:
                valid_records.append(validated_record)
        
        if not valid_records:
            logging.warning("No valid records to insert")
            return
            
        # Prepare data for insertion
        data = [
            (
                UUID(record['id']),
                record['first_name'],
                record['last_name'],
                record['gender'],
                record['address'],
                record['post_code'],
                record['email'],
                record['username'],
                record['registered_date'],
                record['phone'],
                record['picture']
            )
            for record in valid_records
        ]
        
        client.execute(
            "INSERT INTO beehiiv_db.beehiiv_users_activity "
            "(id, first_name, last_name, gender, address, post_code, "
            "email, username, registered_date, phone, picture) VALUES",
            data
        )
        logging.info(f"Batch of {len(data)} records written to ClickHouse successfully!")
    except Exception as e:
        logging.error(f"Error writing batch to ClickHouse: {e}")
        if data and len(data) > 0:
            sample_data = data[0]
            logging.error(f"Sample data: {sample_data}")

def main():
    # Create ClickHouse connection and set up database/table
    client = create_clickhouse_client()
    if not client:
        return

    setup_clickhouse(client)

    # Create Kafka consumer
    consumer = create_kafka_consumer()
    if not consumer:
        client.disconnect()
        return

    # Start consuming messages
    logging.info("Starting Kafka consumer...")
    batch = []
    batch_size = 100
    last_commit_time = time.time()
    commit_interval = 10  # seconds
    
    try:
        while True:
            # Poll for messages with a timeout
            message_batch = consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    try:
                        record = message.value
                        batch.append(record)
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
            
            current_time = time.time()
            time_elapsed = current_time - last_commit_time
            
            # Write batch if it's full or if commit interval has passed
            if len(batch) >= batch_size or (batch and time_elapsed >= commit_interval):
                write_to_clickhouse(client, batch)
                batch = []
                last_commit_time = current_time
            
            # Sleep a bit to avoid high CPU usage in case of no messages
            if not message_batch:
                time.sleep(0.1)
                
    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user.")
    except Exception as e:
        logging.error(f"Error in Kafka consumer: {e}")
    finally:
        # Write any remaining records
        if batch:
            write_to_clickhouse(client, batch)
            
        if consumer:
            consumer.close()
        if client:
            client.disconnect()
        logging.info("Resources cleaned up.")

if __name__ == "__main__":
    main()