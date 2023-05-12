#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector
import time
import json
from confluent_kafka import Producer

# Create a MySQL connection
db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="@A9232695645g",
  database="kafkaassignments"
)

# Create a Kafka producer
p = Producer({
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '3HE4XSQSKBDEMSWM',
    'sasl.password': 'S0XzrM4rTHH5iFoA3urOipNjOVx6JX0S29l9g9gjw9rFoWcp+th9vxmhX71kGZBf'
})

# Keep track of the last ID we processed
last_id = 0

while True:
    # Create a cursor
    cursor = db.cursor()

    # Execute a query to fetch all records with an ID greater than the last one we processed
    cursor.execute(f"SELECT * FROM user_data WHERE id > {last_id}")

    # Fetch all the rows
    rows = cursor.fetchall()

    for row in rows:
        # Convert the row to a JSON string
        message = {
            "id": row[0],
            "name": row[1],
            "email": row[2],
            "created_at": row[3].isoformat()  # convert datetime to string
        }

        # Convert the dictionary to a JSON string
        message_str = json.dumps(message)

        # Send the message to Kafka
        p.produce('kafka_assignment', value=message_str)

        # Update the last ID we processed
        last_id = row[0]

    # Sleep for a bit before checking for new records
    time.sleep(10)

