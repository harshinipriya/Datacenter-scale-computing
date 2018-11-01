#!/bin/bash

#remove locally existing output directory from previous runs
rm -rf output/ 

#remove existing(old) input folder from hdfs 
hadoop fs -rm -r -f input

#remove existing(old) output folder from hdfs 
hadoop fs -rm -r -f output

#copy the input to hdfs
hadoop fs -copyFromLocal input/


#run the map reduce job
hadoop jar TimeBlocks.jar TimeBlocks.TimeBlocks.TimeBlocks input/ output


#copy the results back to local file system
hadoop fs -copyToLocal output/

#display the output file
printf "\n\n\n\t THE OUTPUT FILES ARE\t\n\n" 
ls output/
printf "\n PLEASE 'CD' INTO THE PART FILES LOCATED INSIDE THE OUTPUT DIRECTORY TO VIEW THE RESULTS\t\n\n"