#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install cassandra-driver


# In[2]:


import cassandra
print(cassandra.__version__)


# In[9]:


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
  'secure_connect_bundle': 'C:/Users/arkag/3D Objects/secure-connect-kafka-assignment.zip'
}
auth_provider = PlainTextAuthProvider('wXKXCEYwZPObYjhmewGJMGWx', 'GIbKaT.1Q.Pk,cEj4ZwpaQk9S3UcQ2DmeIo2BcDm+zTznTPBpm,Wzp6UkeBl6,skt+liAOpQJ+7LvgYHUak,MUa.wQvEZ+SoC3uj6Z6Esj45kGMwxNQ-PBiDQEz+6_ur')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")


# In[10]:


# Command to use a keyspace
try:
    query = "use kafka_keyspace"
    session.execute(query)
    print("Inside the kafka_keyspace")
except Exception as err:
    print("Exception Occured while using Keyspace : ",err)


# In[11]:


# Command to create a table inside a KEyspace
try:
    query = """CREATE TABLE transformed_user_data (
    id int PRIMARY KEY,
    name text,
    email text,
    created_at text,
    extra text
             )
            """
    session.execute(query)
    print("Table created inside the keyspace")
except Exception as err:
    print("Exception Occured while creating the table : ",err)


# In[13]:


# Select query on cassandra table
try:
    query = "select * from transformed_user_data"
    result = session.execute(query)
    for row in result:
        print(row)
except Exception as err:
    print("Exception Occured while selecting the data from table: ",err)


# In[ ]:




