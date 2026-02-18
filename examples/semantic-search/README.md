# semantic search

an e-commerce product catalog where users search by meaning, not keywords. "warm gear for winter camping" surfaces sleeping bags and down jackets even when those exact words don't appear in the product name.

## how it works

each product is stored in two places:

- **HSET** `product:{id}` — name, category, price, description
- **VADD** `products:vecs` — 128-dimensional embedding of the product text

on a search query the demo:

1. checks a **string cache** (`SET` with 60-second TTL) for a recent identical query
2. on a cache miss, runs **VSIM** to find the 5 nearest embeddings
3. fetches full metadata with **HGETALL** for each match
4. stores the result in the cache before returning

the second identical query hits the string cache and returns in microseconds rather than going through the HNSW index.

> in a real system the embeddings would come from sentence-transformers or the openai embeddings api. here they're deterministic pseudo-embeddings seeded from the text so the same product always maps to the same vector.

## run it

```sh
cd examples/semantic-search
pip install -r requirements.txt
python main.py
```

start ember with the vector feature enabled:

```sh
cargo build --release --features grpc,vector
./target/release/ember-server
```

## what to look for

the first run of a query shows the vsim search time (typically a few milliseconds). repeat the same query and the timing drops to microseconds — the cache line shows `cache hit`.

```
query: "warm gear for winter camping"  [4.31ms — vsim search, cached for 60s]
  1. Alpine sleeping bag (camping) — $199.99  similarity=0.847
  2. Emergency bivy sack (camping) — $199.99  similarity=0.821
  ...

--- cache hit demo (repeat query) ---
query: "warm gear for winter camping"  [0.18ms — cache hit]
  1. Alpine sleeping bag (camping) — $199.99  similarity=0.847
```
