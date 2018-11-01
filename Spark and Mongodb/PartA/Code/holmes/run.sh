#!/bin/bash

#remove existing(old) input folder from hdfs 
hadoop fs -rm -r -f input

#copy input file to HDFS
hadoop fs -copyFromLocal input/
 
#Move to the location of .py file
cd ..

#remove locally existing output directory from previous runs
rm -f output.txt

#run the spark job
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 partA_holmes.py

#output file exists in PartA/Code