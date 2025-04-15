from pyspark.sql import SparkSession
from pathvalidate import sanitize_filename
from tqdm import tqdm

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("data_preparation") \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Read the Parquet file
df = spark.read.parquet("/a.parquet")

# Select relevant columns and sample 1000 documents
n = 1000
df = df.select("id", "title", "text")
total_docs = df.count()
if total_docs >= n:
    df = df.limit(n)
else:
    # Fewer than 1000, so we just take all
    n = total_docs

print(f"Preparing {n} documents out of {total_docs} available...")

# Collect the sampled DataFrame to driver
docs = df.collect()

# Use tqdm for progress indication
for row in tqdm(docs, desc="Writing documents", unit="doc"):
    doc_id = str(row['id'])
    title = row['title'] if row['title'] is not None else ""
    text = row['text'] if row['text'] is not None else ""
    # Replace any newlines in text to spaces for consistency
    text_single_line = text.replace("\n", " ")
    # Sanitize filename to avoid illegal characters and combine id and title
    filename = sanitize_filename(f"{doc_id}_{title}")[:100]  # limit length for safety (saw in different works of other people in internet)
    filepath = f"data/{filename}.txt".replace(" ", "_") # replace spaces with _
    # Write document text to a file
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)
    # Append the document to the combined sample.txt (tab-separated format)
    with open("data/sample.txt", "a", encoding="utf-8") as f_combined:
        f_combined.write(f"{doc_id}\t{title}\t{text_single_line}\n")

print("Document files and combined sample.txt created successfully.")
