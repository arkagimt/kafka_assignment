#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)

# Config for Kafka consumer
kafka_config = {
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '3HE4XSQSKBDEMSWM',
    'sasl.password': 'S0XzrM4rTHH5iFoA3urOipNjOVx6JX0S29l9g9gjw9rFoWcp+th9vxmhX71kGZBf',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
}

# Config for Cassandra cluster
cloud_config = {
    'secure_connect_bundle': 'C:/Users/arkag/3D Objects/secure-connect-kafka-assignment.zip'
}
auth_provider = PlainTextAuthProvider('arkagimt@gmail.com', '@A9232695645g')

# Create Kafka consumer
try:
    c = Consumer(kafka_config)
except Exception as e:
    logging.error(f"Failed to create Kafka consumer: {e}")
    exit(1)

# Subscribe to Kafka topic
try:
    c.subscribe(['kafka_assignment'])
except Exception as e:
    logging.error(f"Failed to subscribe to Kafka topic: {e}")
    exit(1)

# Create Cassandra cluster and connect
try:
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
except Exception as e:
    logging.error(f"Failed to connect to Cassandra: {e}")
    exit(1)

# Process messages
while True:
    try:
        msg = c.poll(1.0)  # Wait for up to 1 second for a message

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Kafka error: {msg.error()}")
                continue

        # Deserialize message value from bytes to string
        message_value_str = msg.value().decode('utf-8')

        # Parse JSON into Python object
        message_value = json.loads(message_value_str)

        try:
            user_id = message_value.get('id', 'default_id')
            name = message_value.get('name', 'default_name')
            email = message_value.get('email', 'default_email')
            created_at = message_value.get('created_at', 'default_created_at')
            if created_at != 'default_created_at':
                created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

            query = "INSERT INTO kafka_keyspace.transformed_user_data (id, name, email, created_at) VALUES (%s, %s, %s, %s)"
            session.execute(query, (user_id, name, email, created_at))
            logging.info(f"Successfully inserted data: {message_value}")
        except Exception as e:
            logging.error(f"Failed to insert data: {message_value}")
            logging.error(f"Error: {e}")
    except KeyboardInterrupt:
        break
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

# Close Kafka consumer
c.close()

# Close Cassandra session
session.shutdown()

