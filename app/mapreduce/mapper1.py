#!/usr/bin/env python3
# app/mapreduce/mapper1.py
import sys
import re

# Define a simple tokenizer to split text into terms.
# Here we split on any non-alphanumeric character to get words.
token_pattern = re.compile(r"\w+")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    # Each input line is: doc_id \t title \t text
    parts = line.split("\t", 2)
    if len(parts) < 3:
        # If the line doesn't have 3 parts, skip it (malformed line)
        continue
    doc_id, title, text = parts[0], parts[1], parts[2]
    # Convert text to lowercase and find all words
    text_lower = text.lower()
    terms = token_pattern.findall(text_lower)
    if not terms:
        continue

    # Count term frequencies in this document
    term_counts = {}
    for term in terms:
        # Optionally, skip very short terms or purely numeric terms if needed.
        if term == "" or term.isdigit():
            continue
        term_counts[term] = term_counts.get(term, 0) + 1

    # Emit term frequencies
    for term, tf in term_counts.items():
        # Output format: term \t doc_id:tf
        # Using print will write to sys.stdout which Hadoop Streaming captures.
        print(f"{term}\t{doc_id}:{tf}")
