#!/usr/bin/env python3
"""
e-commerce product catalog with semantic search.

products are stored as HSET metadata alongside HNSW vector embeddings.
a user query surfaces relevant products even when keywords don't match —
"i need something warm for winter camping" finds sleeping bags and bivies.
"""

import hashlib
import json
import time

import numpy as np
from ember import EmberClient

VECTOR_INDEX = "products:vecs"
DIM = 128
CACHE_TTL = 60  # seconds

PRODUCTS = [
    {
        "id": "p001",
        "name": "Alpine sleeping bag",
        "category": "camping",
        "price": 199.99,
        "description": "rated to -20°F, ultralight ripstop shell, 650-fill down",
    },
    {
        "id": "p002",
        "name": "Trail running shoes",
        "category": "footwear",
        "price": 129.99,
        "description": "breathable mesh upper, Vibram outsole, 4mm drop",
    },
    {
        "id": "p003",
        "name": "Merino wool base layer",
        "category": "apparel",
        "price": 89.99,
        "description": "270gsm merino, moisture-wicking, odor-resistant",
    },
    {
        "id": "p004",
        "name": "Trekking poles carbon",
        "category": "hiking",
        "price": 149.99,
        "description": "collapsible, flick-lock, cork grips, ultralight",
    },
    {
        "id": "p005",
        "name": "Insulated water bottle",
        "category": "hydration",
        "price": 39.99,
        "description": "32oz double-wall vacuum, keeps cold 24h, hot 12h",
    },
    {
        "id": "p006",
        "name": "Down puffy jacket",
        "category": "apparel",
        "price": 249.99,
        "description": "650-fill duck down, packable to its own pocket, wind-resistant",
    },
    {
        "id": "p007",
        "name": "Ultralight tent 2-person",
        "category": "camping",
        "price": 389.99,
        "description": "freestanding, 1.1kg total weight, silnylon fly",
    },
    {
        "id": "p008",
        "name": "Headlamp 400 lumen",
        "category": "lighting",
        "price": 49.99,
        "description": "USB-C rechargeable, IPX4 waterproof, red night-vision mode",
    },
    {
        "id": "p009",
        "name": "Wool mitten liners",
        "category": "apparel",
        "price": 29.99,
        "description": "100% boiled wool, thin enough to layer under shells",
    },
    {
        "id": "p010",
        "name": "Camp cook system",
        "category": "cooking",
        "price": 99.99,
        "description": "750ml titanium pot, integrated stove, windscreen, nests flat",
    },
    {
        "id": "p011",
        "name": "Waterproof rain jacket",
        "category": "apparel",
        "price": 279.99,
        "description": "Gore-Tex 3L, pit zips, packable hood, fully seam-sealed",
    },
    {
        "id": "p012",
        "name": "Foam sleeping pad",
        "category": "camping",
        "price": 59.99,
        "description": "closed-cell foam, R-value 2.0, accordion fold, indestructible",
    },
    {
        "id": "p013",
        "name": "Rock climbing harness",
        "category": "climbing",
        "price": 79.99,
        "description": "adjustable leg loops, 4 gear loops, CE EN12277 certified",
    },
    {
        "id": "p014",
        "name": "Ski goggles OTG",
        "category": "snow sports",
        "price": 109.99,
        "description": "over-glasses fit, triple-layer foam, anti-fog spherical lens",
    },
    {
        "id": "p015",
        "name": "Compression packing cubes",
        "category": "travel",
        "price": 34.99,
        "description": "set of 3, reduces packed volume by 60%, ripstop nylon",
    },
    {
        "id": "p016",
        "name": "Thermal balaclava",
        "category": "apparel",
        "price": 24.99,
        "description": "fleece-lined neoprene, full face and neck coverage for cold weather",
    },
    {
        "id": "p017",
        "name": "Bear canister BV500",
        "category": "camping",
        "price": 89.99,
        "description": "2.9L capacity, IGBC approved, polycarbonate, 1.9lbs",
    },
    {
        "id": "p018",
        "name": "Snow gaiters",
        "category": "footwear",
        "price": 44.99,
        "description": "low-cut, ripstop nylon, instep strap, velcro closure for deep snow",
    },
    {
        "id": "p019",
        "name": "Portable water filter",
        "category": "hydration",
        "price": 74.99,
        "description": "filters to 0.1 micron, 1,000L capacity, backflush-cleanable",
    },
    {
        "id": "p020",
        "name": "Emergency bivy sack",
        "category": "camping",
        "price": 19.99,
        "description": "reflective mylar, 340g, retains body heat, fits any sleeping bag",
    },
]


def embed(text: str) -> list[float]:
    """generate a deterministic pseudo-embedding for demo purposes.

    in production, swap this for sentence-transformers or the openai embeddings api.
    the sha256 seed ensures the same text always maps to the same vector.
    """
    seed = int(hashlib.sha256(text.encode()).hexdigest(), 16) % (2**31)
    rng = np.random.default_rng(seed)
    vec = rng.standard_normal(DIM).astype(np.float32)
    return (vec / np.linalg.norm(vec)).tolist()


def query_cache_key(query: str) -> str:
    digest = hashlib.sha256(query.encode()).hexdigest()[:16]
    return f"search:cache:{digest}"


def seed_catalog(client: EmberClient) -> None:
    print(f"seeding {len(PRODUCTS)} products...")

    entries: dict[str, list[float]] = {}
    for p in PRODUCTS:
        client.hset(
            f"product:{p['id']}",
            {
                "name": p["name"].encode(),
                "category": p["category"].encode(),
                "price": str(p["price"]).encode(),
                "description": p["description"].encode(),
            },
        )
        entries[p["id"]] = embed(f"{p['name']} {p['description']}")

    added = client.vadd_batch(VECTOR_INDEX, entries)
    print(f"  indexed {added} product embeddings (dim={DIM})\n")


def search(client: EmberClient, query: str, top_k: int = 5) -> None:
    cache_key = query_cache_key(query)

    t0 = time.perf_counter()
    cached = client.get(cache_key)
    if cached is not None:
        elapsed = (time.perf_counter() - t0) * 1000
        results = json.loads(cached)
        print(f'query: "{query}"  [{elapsed:.2f}ms — cache hit]')
        for rank, r in enumerate(results, 1):
            print(f"  {rank}. {r['name']} ({r['category']}) — ${r['price']}")
        return

    hits = client.vsim(VECTOR_INDEX, embed(query), count=top_k)

    results = []
    for element, distance in hits:
        meta = client.hgetall(f"product:{element}")
        results.append(
            {
                "id": element,
                "name": meta[b"name"].decode(),
                "category": meta[b"category"].decode(),
                "price": meta[b"price"].decode(),
                "similarity": round(1 - distance, 3),
            }
        )

    client.set(cache_key, json.dumps(results).encode(), ex=CACHE_TTL)

    elapsed = (time.perf_counter() - t0) * 1000
    print(
        f'query: "{query}"  [{elapsed:.2f}ms — vsim search, cached for {CACHE_TTL}s]'
    )
    for rank, r in enumerate(results, 1):
        print(
            f"  {rank}. {r['name']} ({r['category']}) — ${r['price']}"
            f"  similarity={r['similarity']}"
        )


def main() -> None:
    with EmberClient("localhost:6380") as client:
        client.flushdb()
        seed_catalog(client)

        print("--- semantic search demo ---")
        search(client, "warm gear for winter camping")
        print()
        search(client, "lightweight layers for cold weather hiking")
        print()
        search(client, "protection from rain on the trail")

        print("\n--- cache hit demo (repeat query) ---")
        search(client, "warm gear for winter camping")


if __name__ == "__main__":
    main()
