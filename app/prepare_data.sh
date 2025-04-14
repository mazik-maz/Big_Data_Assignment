#!/bin/bash
source .venv/bin/activate

# Use the Python from our virtualenv for PySpark driver
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON  # Ensure executors use system default (we'll use archives for venv in cluster mode)

# Step 1: Upload the Parquet file to HDFS (optional, for completeness)
echo "Upload the Parquet file to HDFS..."
hdfs dfs -put -f a.parquet /a.parquet

# Step 2: Run the PySpark data preparation job to generate documents
echo "Running data preparation PySpark job..."
spark-submit prepare_data.py

# Step 3: Upload the prepared data files to HDFS
echo "Uploading prepared data to HDFS..."
hdfs dfs -put -f data /    # This copies the local 'data' folder to HDFS root as /data

# Step 4: Verify files in HDFS
hdfs dfs -ls /data
hdfs dfs -cat /data/sample.txt | head -5   # Display first 5 lines of sample.txt in HDFS as a preview
echo "Data preparation step completed."
