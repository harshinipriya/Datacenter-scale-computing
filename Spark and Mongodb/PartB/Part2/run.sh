#!/bin/bash


#remove locally existing output directory from previous runs
rm -f Bucket1/output.txt
rm -f Bucket2/output.txt
rm -f Bucket3/output.txt
rm -f Bucket4/output.txt
rm -f Bucket5/output.txt

#run the spark job
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 partBpart2.py