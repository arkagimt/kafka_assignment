#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector
import time
import random
import string

def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

while True:
    connection = mysql.connector.connect(host='localhost',
                                         database='kafkaassignments',
                                         user='root',
                                         password='@A9232695645g')

    cursor = connection.cursor()

    sql_insert_query = """ INSERT INTO `user_data` (`name`, `email`) VALUES (%s,%s)"""
    record_to_insert = (random_string(5), random_string(5) + "@gmail.com")

    cursor.execute(sql_insert_query, record_to_insert)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into user_data table")

    cursor.close()
    connection.close()

    time.sleep(5)  # sleep for 5 seconds before inserting next record

