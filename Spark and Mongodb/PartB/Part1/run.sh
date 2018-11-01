#!/bin/bash


#remove locally existing output directory from previous runs
rm -f output.txt

#run the spark job
spark-submit --master local[3] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 partBpart1.py
