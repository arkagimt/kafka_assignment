#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from confluent_kafka import Consumer, KafkaError
import json

# Create a Kafka consumer
c = Consumer({
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '3HE4XSQSKBDEMSWM',
    'sasl.password': 'S0XzrM4rTHH5iFoA3urOipNjOVx6JX0S29l9g9gjw9rFoWcp+th9vxmhX71kGZBf',
    'group.id': 'mygroup',
    'default.topic.config': {'auto.offset.reset': 'earliest'}
})

# Subscribe to the Kafka topic
c.subscribe(['kafka_assignment'])

while True:
    # Try to consume a message
    msg = c.poll(1.0)

    # If there's no message, try again
    if msg is None:
        continue

    # If there's an error (like end of partition), print it and continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Otherwise, print the message
    print('Received message: {}'.format(json.loads(msg.value().decode('utf-8'))))

# Close the consumer when you're done with it
c.close()

