#!/usr/bin/env python
# coding: utf-8

# In[13]:
# all comments are explained clearly from the PDF file

from pyspark.sql.functions import when


# In[14]:


sc.stop()
conf = SparkConf().setAppName("HW3 Q2")
sc = SparkContext(conf=conf)

# Load DataFrame
sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv')                    .options(header='true')                    .load('yellow_tripdata_2017-09.csv')

# Show Schema
df.printSchema()


# In[15]:


df.count()


# In[16]:


df.groupby('passenger_count').count().sort(desc('count')).show()


# In[17]:


df = df.withColumn("passenger_count",                    when((df["passenger_count"] == "0"), 1)                    .otherwise(df["passenger_count"]))


# In[18]:


df.groupby('passenger_count').count().sort(desc('count')).show()


# In[7]:


df_credit = df.filter(df["payment_type"] == "1")         .select("passenger_count", "total_amount", "trip_distance")


# In[10]:


mean_amt = df_credit.groupby('passenger_count')         .agg({"total_amount": "average","trip_distance": "average"})


# In[11]:


mean_amt.sort("passenger_count").show(4)

