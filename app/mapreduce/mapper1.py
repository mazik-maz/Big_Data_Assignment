import sys
import re

# Pattern to split by any non-alphabetical character and get words
token_pattern = re.compile(r"\w+")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    # Each input line should be like: doc_id \t title \t text
    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue
    doc_id, title, text = parts[0], parts[1], parts[2]
    text_lower = text.lower()
    terms = token_pattern.findall(text_lower)
    if not terms:
        continue

    # Count term frequencies in this document
    term_counts = {}
    for term in terms:
        # Optionally, skip very short terms or purely numeric terms
        if term == "" or term.isdigit():
            continue
        term_counts[term] = term_counts.get(term, 0) + 1

    for term, tf in term_counts.items():
        # Using print will write to sys.stdout which Hadoop Streaming captures.
        print(f"{term}\t{doc_id}:{tf}")
