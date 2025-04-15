#!/bin/bash
source .venv/bin/activate

# Use the Python from our virtualenv for PySpark driver
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Upload the Parquet file to HDFS..."
hdfs dfs -put -f a.parquet /a.parquet

echo "Running data preparation PySpark job..."
spark-submit prepare_data.py

echo "Uploading prepared data to HDFS..."
hdfs dfs -put -f data /

# Step 4: Verify files in HDFS
hdfs dfs -ls /data
hdfs dfs -cat /data/sample.txt | head -5   # Display first 5 lines of sample.txt in HDFS as a preview and check in logs of proper working
echo "Data preparation step completed."
