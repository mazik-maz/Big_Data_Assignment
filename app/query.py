import sys
from math import log

from cassandra.cluster import Cluster

# Connect to Cassandra keyspace
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

# Read query from arguments or stdin
if len(sys.argv) > 1:
    # If arguments are provided (search.sh passes the query as one arg)
    query_text = " ".join(sys.argv[1:])
else:
    # If no arg, read from stdin (interactive use)
    query_text = sys.stdin.read().strip()

if not query_text:
    print("No query provided.")
    sys.exit(0)

print(f"Query: {query_text}") # check proper work in logs

# Simple preprocessing of query: lowercase and split into terms
query_terms = [t for t in query_text.lower().split() if t]
if not query_terms:
    print("No valid query terms.")
    sys.exit(0)

# Remove duplicate terms in query for efficiency (this point was not easy for me, 
# i studied different approaches and decided that  iwill use this simplified 
# implementation without counting the frequency of repetition of terms in query too)
unique_query_terms = set(query_terms)

# Get total number of documents (N) and average document length (avgdl) from doc_stats table
rows = session.execute("SELECT doc_length FROM doc_stats")
doc_lengths_list = [row.doc_length for row in rows]
N = len(doc_lengths_list)
avgdl = sum(doc_lengths_list) / N if N > 0 else 0.0

# Dictionary to accumulate scores: doc_id -> score
scores = {}

# For each term in the query, get postings and compute partial scores
k1 = 1.2
b = 0.75
for term in unique_query_terms:
    # Get document frequency for the term
    df_row = session.execute("SELECT doc_freq FROM vocabulary WHERE term = %s", (term, )).one()
    if df_row is None:
        # Term not in index, skip
        continue
    df = df_row.doc_freq
    if df == 0:
        continue

    # Compute IDF for the term using BM25 idf formula
    idf = log((N - df + 0.5) / (df + 0.5) + 1)
    # Retrieve all postings for this term
    postings = session.execute("SELECT doc_id, term_freq FROM inverted_index WHERE term = %s", (term, ))
    for row in postings:
        doc_id = row.doc_id
        tf = row.term_freq
        # Get document length from doc_stats in Cassandra
        if doc_id is None:
            continue
        if doc_id not in scores:
            scores[doc_id] = 0.0
        
        length_row = session.execute("SELECT doc_length FROM doc_stats WHERE doc_id = %s", (doc_id, )).one()
        dl = length_row.doc_length if length_row else avgdl  # avgdl if not found (just for save work)

        # Compute BM25 score contribution for this term and document
        score_term = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
        scores[doc_id] += score_term

# Get top 10 docs by score
top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 Results:")
for rank, (doc_id, score) in enumerate(top_docs, start=1):
    # Fetch title from doc_stats for display
    title_row = session.execute("SELECT title FROM doc_stats WHERE doc_id = %s", (doc_id, )).one()
    print(title_row)
    title = title_row.title if title_row and title_row.title is not None else ""
    print(f"{rank}. DocID={doc_id}\tScore={score:.4f}\tTitle={title}")
