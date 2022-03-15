#!/usr/bin/env python
# coding: utf-8

# In[1]:
# all comments are explained clearly from the PDF file

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *


# In[2]:


conf = SparkConf().setAppName("HW3 Q1")
sc = SparkContext.getOrCreate(conf=conf)
text_file = sc.textFile("hdfs://master/user/HW3/Youvegottofindwhatyoulove.txt")
sents = text_file.filter(lambda line: len(line)>0)
N = sents.count()


# In[3]:


counts = text_file.flatMap(lambda line: line.split(' ')) .filter(lambda word: len(word)>0 and word != '\x00') .map(lambda word: (word.lower(), 1)) .reduceByKey(lambda a, b: a + b) .map(lambda x: (x[1], x[0])) .sortByKey(ascending=False) .map(lambda x: (x[1], x[0], x[0]/N))


# In[4]:


conuts = text_file.flatMap(lambda line: line.split(' ')) .filter(lambda word: len(word)>0) .map(lambda word: (word.lower(), 1)) .reduceByKey(lambda a, b: a + b) .map(lambda x: (x[1], x[0])) .sortByKey(ascending=False) .map(lambda x: (x[1], x[0], x[0]/N))


# In[5]:


print("Word\tOccurrence\tOccurrence per Sentence")
print("----\t-----------\t----------------------")
for word, time, per in counts.take(30):
    print("{}\t{}\t\t\t{}".format(word, time, per))

