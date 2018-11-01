#!/bin/bash

#remove locally existing output directory from previous runs
rm -rf output/ 

#remove existing(old) input folder from hdfs 
hadoop fs -rm -r -f input

#remove existing(old) output folder from hdfs 
hadoop fs -rm -r -f output

#move up one directory to run the job
cd .. 

#run the map reduce job
hadoop jar wordcount_top2000.jar wordcount.wordcount.wordcount s3://csci5253-gutenberg-dataset/ output

#move back to run.sh's folder
cd gutenberg

#copy the results back to local file system
hadoop fs -copyToLocal output/

#display the output file
printf "\n\n\n\t THE OUTPUT FILES ARE\t\n\n" 
ls output/
printf "\n PLEASE 'CD' INTO THE PART FILES LOCATED INSIDE THE OUTPUT DIRECTORY TO VIEW THE RESULTS\t\n\n"