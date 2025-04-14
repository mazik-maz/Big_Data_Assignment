#!/bin/bash

INPUT_PATH="$1"
if [ -z "$INPUT_PATH" ]; then
  INPUT_PATH="/data/sample.txt"
fi

if [[ "$INPUT_PATH" != /* ]]; then
  INPUT_PATH="/$INPUT_PATH"
fi

OUTPUT_PATH="/index/output" 

hdfs dfs -rm -r -f "$OUTPUT_PATH"

echo "Running Hadoop MapReduce indexing job on $INPUT_PATH ..."

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -D mapreduce.job.name="BuildInvertedIndex" \
    -D mapreduce.job.reduces=1 \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH"

echo "MapReduce job finished. Output stored in HDFS at $OUTPUT_PATH."

hdfs dfs -get -f "$OUTPUT_PATH/part-00000" index_result.txt

echo "Index results retrieved to local file index_result.txt."

echo "Storing index in Cassandra..."
python3 app.py index_result.txt

echo "Indexing pipeline completed. The inverted index and stats are now in Cassandra."
