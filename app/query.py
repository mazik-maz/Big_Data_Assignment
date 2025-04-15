# app/query.py
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

print(f"Query: {query_text}")

# Simple preprocessing of query: lowercase and split into terms
query_terms = [t for t in query_text.lower().split() if t]
if not query_terms:
    print("No valid query terms.")
    sys.exit(0)

# Remove duplicate terms in query for efficiency (but keep for scoring if term appears multiple times in query,
# though usually IDF and doc stats don't change; BM25 for multiple occurrences in query could weight more,
# but a simple approach is to treat query term frequency as 1 for BM25 as an approximation.)
unique_query_terms = set(query_terms)

# Get total number of documents (N) and average document length (avgdl) from doc_stats table
# We can do this by selecting all doc_length or using COUNT and AVG in a loop.
rows = session.execute("SELECT doc_length FROM doc_stats")
doc_lengths_list = [row.doc_length for row in rows]
N = len(doc_lengths_list)
avgdl = sum(doc_lengths_list) / N if N > 0 else 0.0

# Prepare a dict to accumulate scores: doc_id -> score
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

    # Compute IDF for the term. Using BM25 idf formula:
    # idf = log((N - df + 0.5) / (df + 0.5) + 1)
    idf = log((N - df + 0.5) / (df + 0.5) + 1)
    # Retrieve all postings for this term
    postings = session.execute("SELECT doc_id, term_freq FROM inverted_index WHERE term = %s", (term, ))
    for row in postings:
        doc_id = row.doc_id
        tf = row.term_freq
        # Get document length from our cached list (we have doc_lengths_list but not a direct mapping).
        # Better, we can fetch from doc_stats table for this doc (since small number of docs typically).
        # We'll use a quick way: we have doc_stats in Cassandra, let's query it.
        # (Alternatively, we could have built a dict of doc_id->length from earlier selection.)
        if doc_id is None:
            continue
        if doc_id not in scores:
            scores[doc_id] = 0.0

        # If we had doc_lengths dict from earlier:
        # We didn't build doc_id->length dict above, but we can fetch each needed doc's length from session
        # to avoid scanning entire list:
        length_row = session.execute("SELECT doc_length FROM doc_stats WHERE doc_id = %s", (doc_id, )).one()
        dl = length_row.doc_length if length_row else avgdl  # fallback to avgdl if not found (should not happen)

        # Compute BM25 score contribution for this term and document
        # tf weight:
        score_term = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
        scores[doc_id] += score_term

# After processing all terms, we have a score for each doc that had at least one query term.
# If query had duplicate terms, note we treated each term once (this assumes query term frequency doesn't significantly change weighting, 
# which is a simplification; a more complete approach would incorporate term frequency in query as well).

# Get top 10 docs by score
top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 Results:")
for rank, (doc_id, score) in enumerate(top_docs, start=1):
    # Fetch title from doc_stats for display
    title_row = session.execute("SELECT title FROM doc_stats WHERE doc_id = %s", (doc_id, )).one()
    print(title_row)
    title = title_row.title if title_row and title_row.title is not None else ""
    print(f"{rank}. DocID={doc_id}\tScore={score:.4f}\tTitle={title}")
