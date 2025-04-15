import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Check for input file argument
input_file = None
if len(sys.argv) > 1:
    input_file = sys.argv[1]
else:
    input_file = "index_result.txt"

# Connect to cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# Create keyspace with replication_factor = 1 becasuse we run for 1 node
KEYSPACE = "search_engine"
print("Creating keyspace (if not exists)...")
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
""")
session.set_keyspace(KEYSPACE)

# Create tables for vocabulary, inverted index, and document stats
print("Creating tables (if not exists)...")
session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        doc_freq int
    )
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        term_freq int,
        PRIMARY KEY (term, doc_id)
    )
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id text PRIMARY KEY,
        title text,
        doc_length int
    )
""")

# Dictionaries for doc titles and lengths
doc_titles = {}
doc_lengths = {}

# Load document info from the combined data file (sample.txt) if available
try:
    with open("data/sample.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t", 2)
            if len(parts) < 3:
                continue
            doc_id, title, text = parts[0], parts[1], parts[2]
            doc_titles[doc_id] = title
            # Compute doc length as number of terms (same way as mapper to be consistent)
            import re
            terms = re.findall(r"\w+", text.lower())
            doc_lengths[doc_id] = len([term for term in terms if term and not term.isdigit()])
except FileNotFoundError:
    print("Warning: data/sample.txt not found. Titles will not be available.")


print(f"Length of doc_titlse is {len(doc_titles)}. Just for checking proper work of code")
print(f"Reading index output from {input_file} ...")
try:
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            tag = parts[0]
            if tag == "VOCAB":
                # Format: VOCAB, term, df
                if len(parts) >= 3:
                    term = parts[1]
                    df = int(parts[2])
                    session.execute(
                        "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
                        (term, df)
                    )
            elif tag == "POST":
                # Format: POST, term, doc_id, tf
                if len(parts) >= 4:
                    term = parts[1]
                    doc_id = parts[2]
                    tf = int(parts[3]) if parts[3].isdigit() else 0
                    session.execute(
                        "INSERT INTO inverted_index (term, doc_id, term_freq) VALUES (%s, %s, %s)",
                        (term, doc_id, tf)
                    )
                    # If doc length not already known and tf is numeric, add to doc_lengths sum
                    if doc_id not in doc_lengths:
                        doc_lengths[doc_id] = 0
                    doc_lengths[doc_id] += tf
                    # If we didn't capture title earlier, we at least ensure the key exists
                    if doc_id not in doc_titles:
                        doc_titles[doc_id] = ""
except FileNotFoundError as e:
    print(f"Error: Index output file {input_file} not found.")
    sys.exit(1)

print("Inserting document stats into Cassandra...")
for doc_id, length in doc_lengths.items():
    title = doc_titles[doc_id]
    session.execute(
        "INSERT INTO doc_stats (doc_id, title, doc_length) VALUES (%s, %s, %s)",
        (doc_id, title, length)
    )

print("Index data has been successfully stored in Cassandra.")
