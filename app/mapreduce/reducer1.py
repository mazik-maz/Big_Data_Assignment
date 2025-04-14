import sys

current_term = None
postings = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    term, doc_tf = line.split("\t", 1)

    if current_term is not None and term != current_term:
        df = len(postings)

        print(f"VOCAB\t{current_term}\t{df}")
        for doc_id, tf in postings:
            print(f"POST\t{current_term}\t{doc_id}\t{tf}")

        postings = []

    current_term = term
    if ":" in doc_tf:
        doc_id, tf = doc_tf.split(":", 1)
        try:
            tf = int(tf)
        except:
            tf = 1
        postings.append((doc_id, tf))
    else:
        doc_id = doc_tf
        postings.append((doc_id, 1))

if current_term is not None:
    df = len(postings)
    print(f"VOCAB\t{current_term}\t{df}")
    for doc_id, tf in postings:
        print(f"POST\t{current_term}\t{doc_id}\t{tf}")
