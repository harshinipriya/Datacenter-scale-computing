from pyspark.sql import SparkSession
import sys 
from operator import add
import re

spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

def wordCount(wordListRDD):
    return wordListRDD.map(lambda x:(x,1)).reduceByKey(add).sortByKey()

lines = spark.read.text('input/').rdd.map(lambda r: r[0])
textRDD = lines.flatMap(lambda x: x.split())
WordsRDD = textRDD.filter(lambda x:x!=" ")

Highest2kFrequency = wordCount(WordsRDD)
count = Highest2kFrequency.count()
Highfreq = Highest2kFrequency.takeOrdered(count, key=lambda x: -x[1])


#Writing output to the file
with open('wordcount.txt', 'w') as f:
    for (word,count) in Highfreq:
        f.write("%s\t%i\n" % (word,count))

spark.stop()
