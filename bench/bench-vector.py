#!/usr/bin/env python3
"""
vector similarity benchmark harness.

benchmarks vector insert and kNN query performance across ember, chromadb,
and pgvector. called by bench-vector.sh with appropriate arguments.

usage:
    python3 bench/bench-vector.py --system ember --mode random --dim 128 --count 100000
    python3 bench/bench-vector.py --system chromadb --mode sift --sift-dir bench/vector_data
"""

import argparse
import json
import time
import sys
import os
import numpy as np
from abc import ABC, abstractmethod


# ---------------------------------------------------------------------------
# vector generation
# ---------------------------------------------------------------------------

def generate_vectors(count: int, dim: int, seed: int = 42) -> np.ndarray:
    """generate random unit vectors for benchmarking."""
    rng = np.random.RandomState(seed)
    vectors = rng.randn(count, dim).astype(np.float32)
    # normalize to unit vectors for cosine similarity
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    vectors /= norms
    return vectors


# ---------------------------------------------------------------------------
# client abstractions
# ---------------------------------------------------------------------------

class VectorClient(ABC):
    """base class for vector database clients."""

    @abstractmethod
    def setup(self, dim: int, metric: str = "cosine"):
        """create collection/index. called once before inserts."""

    @abstractmethod
    def insert_batch(self, ids: list, vectors: np.ndarray):
        """insert a batch of vectors."""

    @abstractmethod
    def query(self, vector: np.ndarray, k: int) -> list:
        """return top-k nearest neighbor ids."""

    @abstractmethod
    def teardown(self):
        """clean up collection/index."""

    @abstractmethod
    def name(self) -> str:
        """display name for this system."""


class EmberClient(VectorClient):
    """ember vector client using redis-py for RESP command execution."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        import redis
        self.conn = redis.Redis(host=host, port=port, decode_responses=True)
        self.key = "bench_vectors"

    def setup(self, dim: int, metric: str = "cosine"):
        # clear any previous data
        self.conn.delete(self.key)

    def insert_batch(self, ids: list, vectors: np.ndarray):
        pipe = self.conn.pipeline(transaction=False)
        for i, vid in enumerate(ids):
            vec = vectors[i]
            # VADD key element v1 v2 ... METRIC COSINE M 16 EF 64
            args = [self.key, vid] + [str(float(v)) for v in vec]
            args += ["METRIC", "COSINE", "M", "16", "EF", "64"]
            pipe.execute_command("VADD", *args)
        pipe.execute()

    def query(self, vector: np.ndarray, k: int) -> list:
        args = [self.key] + [str(float(v)) for v in vector]
        args += ["COUNT", str(k)]
        result = self.conn.execute_command("VSIM", *args)
        if result is None:
            return []
        return [r.decode() if isinstance(r, bytes) else str(r) for r in result]

    def teardown(self):
        self.conn.delete(self.key)
        self.conn.close()

    def name(self) -> str:
        return "ember"


class EmberGrpcClient(VectorClient):
    """ember vector client using the python gRPC client.

    sends vectors as packed floats — no string parsing overhead.
    requires the ember-py package to be installed.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6380):
        from ember import EmberClient
        self.client = EmberClient(f"{host}:{port}")
        self.key = "bench_vectors"

    def setup(self, dim: int, metric: str = "cosine"):
        self.client.flushdb()

    def insert_batch(self, ids: list, vectors: np.ndarray):
        for i, vid in enumerate(ids):
            vec = vectors[i].tolist()
            self.client.vadd(self.key, vid, vec, metric="cosine", m=16, ef=64)

    def query(self, vector: np.ndarray, k: int) -> list:
        results = self.client.vsim(self.key, vector.tolist(), count=k)
        return [r[0] for r in results]

    def teardown(self):
        self.client.flushdb()
        self.client.close()

    def name(self) -> str:
        return "ember-grpc"


class ChromaClient(VectorClient):
    """chromadb client via HTTP API."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        import chromadb
        self.client = chromadb.HttpClient(host=host, port=port)
        self.collection = None
        self.collection_name = "bench_vectors"

    def setup(self, dim: int, metric: str = "cosine"):
        # delete if exists
        try:
            self.client.delete_collection(self.collection_name)
        except Exception:
            pass
        self.collection = self.client.create_collection(
            name=self.collection_name,
            metadata={
                "hnsw:space": "cosine",
                "hnsw:M": 16,
                "hnsw:construction_ef": 64,
            },
        )

    def insert_batch(self, ids: list, vectors: np.ndarray):
        self.collection.add(
            ids=ids,
            embeddings=vectors.tolist(),
        )

    def query(self, vector: np.ndarray, k: int) -> list:
        results = self.collection.query(
            query_embeddings=[vector.tolist()],
            n_results=k,
        )
        return results["ids"][0] if results["ids"] else []

    def teardown(self):
        try:
            self.client.delete_collection(self.collection_name)
        except Exception:
            pass

    def name(self) -> str:
        return "chromadb"


class PgVectorClient(VectorClient):
    """pgvector client via psycopg2."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5432,
                 user: str = "postgres", password: str = "postgres",
                 dbname: str = "vectordb"):
        import psycopg2
        self.conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname,
        )
        self.conn.autocommit = True
        self.dim = None

    def setup(self, dim: int, metric: str = "cosine"):
        self.dim = dim
        cur = self.conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("DROP TABLE IF EXISTS bench_vectors")
        cur.execute(f"CREATE TABLE bench_vectors (id text PRIMARY KEY, embedding vector({dim}))")
        cur.close()

    def _create_index(self):
        """create HNSW index after all inserts (faster than incremental)."""
        cur = self.conn.cursor()
        cur.execute(
            "CREATE INDEX ON bench_vectors USING hnsw (embedding vector_cosine_ops) "
            "WITH (m = 16, ef_construction = 64)"
        )
        cur.close()

    def insert_batch(self, ids: list, vectors: np.ndarray):
        cur = self.conn.cursor()
        # format vectors as pgvector string literals: "[0.1,0.2,0.3]"
        data = [
            (vid, "[" + ",".join(str(float(v)) for v in vec) + "]")
            for vid, vec in zip(ids, vectors)
        ]
        cur.executemany(
            "INSERT INTO bench_vectors (id, embedding) VALUES (%s, %s::vector) "
            "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding",
            data,
        )
        cur.close()

    def query(self, vector: np.ndarray, k: int) -> list:
        cur = self.conn.cursor()
        # explicit cast to vector type since psycopg2 sends lists as numeric[]
        vec_str = "[" + ",".join(str(float(v)) for v in vector) + "]"
        cur.execute(
            "SELECT id FROM bench_vectors ORDER BY embedding <=> %s::vector LIMIT %s",
            (vec_str, k),
        )
        results = [row[0] for row in cur.fetchall()]
        cur.close()
        return results

    def teardown(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS bench_vectors")
        cur.close()
        self.conn.close()

    def name(self) -> str:
        return "pgvector"


class QdrantClient_(VectorClient):
    """qdrant client via the official python SDK."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6333):
        from qdrant_client import QdrantClient as _QC
        from qdrant_client.models import (
            Distance, VectorParams, PointStruct, HnswConfigDiff,
        )
        self._qc = _QC(host=host, port=port)
        self._PointStruct = PointStruct
        self._Distance = Distance
        self._VectorParams = VectorParams
        self._HnswConfigDiff = HnswConfigDiff
        self.collection_name = "bench_vectors"

    def setup(self, dim: int, metric: str = "cosine"):
        # delete if exists
        try:
            self._qc.delete_collection(self.collection_name)
        except Exception:
            pass
        self._qc.create_collection(
            collection_name=self.collection_name,
            vectors_config=self._VectorParams(
                size=dim,
                distance=self._Distance.COSINE,
            ),
            hnsw_config=self._HnswConfigDiff(
                m=16,
                ef_construct=64,
            ),
        )

    def insert_batch(self, ids: list, vectors: np.ndarray):
        points = [
            self._PointStruct(
                id=idx,
                vector=vectors[idx].tolist(),
                payload={"name": vid},
            )
            for idx, vid in enumerate(ids)
        ]
        self._qc.upsert(
            collection_name=self.collection_name,
            points=points,
        )

    def query(self, vector: np.ndarray, k: int) -> list:
        response = self._qc.query_points(
            collection_name=self.collection_name,
            query=vector.tolist(),
            limit=k,
        )
        return [
            r.payload.get("name", str(r.id)) if r.payload else str(r.id)
            for r in response.points
        ]

    def teardown(self):
        try:
            self._qc.delete_collection(self.collection_name)
        except Exception:
            pass

    def name(self) -> str:
        return "qdrant"


# ---------------------------------------------------------------------------
# benchmark functions
# ---------------------------------------------------------------------------

def benchmark_insert(client: VectorClient, vectors: np.ndarray,
                     batch_size: int = 500) -> dict:
    """measure insert throughput. returns vectors/sec."""
    n = len(vectors)
    ids = [f"vec_{i}" for i in range(n)]

    start = time.perf_counter()
    for i in range(0, n, batch_size):
        end_idx = min(i + batch_size, n)
        client.insert_batch(ids[i:end_idx], vectors[i:end_idx])
    elapsed = time.perf_counter() - start

    # for pgvector, create the HNSW index after all inserts
    index_time = 0.0
    if isinstance(client, PgVectorClient):
        idx_start = time.perf_counter()
        client._create_index()
        index_time = time.perf_counter() - idx_start

    throughput = n / elapsed if elapsed > 0 else 0

    return {
        "vectors": n,
        "elapsed_sec": round(elapsed, 3),
        "index_time_sec": round(index_time, 3),
        "throughput": round(throughput, 1),
    }


def benchmark_query(client: VectorClient, queries: np.ndarray,
                    k: int = 10, warmup: int = 50) -> dict:
    """measure query latency and throughput."""
    n = len(queries)

    # warmup
    for i in range(min(warmup, n)):
        client.query(queries[i], k)

    # timed run — measure each query individually for latency percentiles
    latencies = []
    for i in range(n):
        start = time.perf_counter()
        client.query(queries[i], k)
        latencies.append(time.perf_counter() - start)

    latencies_ms = np.array(latencies) * 1000
    total_sec = sum(latencies)
    throughput = n / total_sec if total_sec > 0 else 0

    return {
        "queries": n,
        "elapsed_sec": round(total_sec, 3),
        "throughput": round(throughput, 1),
        "p50_ms": round(float(np.percentile(latencies_ms, 50)), 3),
        "p95_ms": round(float(np.percentile(latencies_ms, 95)), 3),
        "p99_ms": round(float(np.percentile(latencies_ms, 99)), 3),
    }


def compute_recall(client: VectorClient, queries: np.ndarray,
                   ground_truth: np.ndarray, k: int = 10) -> dict:
    """compute recall@k against ground truth nearest neighbors.

    ground_truth contains integer indices into the base vector array.
    we use "vec_{index}" as the ID format matching our insert convention.
    """
    n = len(queries)
    hits = 0
    total = 0

    for i in range(n):
        predicted = set(client.query(queries[i], k))
        # ground truth gives integer indices; our IDs are "vec_{index}"
        true_neighbors = set(f"vec_{gt}" for gt in ground_truth[i][:k])
        hits += len(predicted & true_neighbors)
        total += k

    recall = hits / total if total > 0 else 0.0
    return {
        "recall_at_k": round(recall, 4),
        "k": k,
        "queries": n,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="vector similarity benchmark")
    parser.add_argument("--system", required=True, choices=["ember", "ember-grpc", "chromadb", "pgvector", "qdrant"])
    parser.add_argument("--mode", default="random", choices=["random", "sift"])
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--ember-port", type=int, default=6379)
    parser.add_argument("--ember-grpc-port", type=int, default=6380)
    parser.add_argument("--chroma-port", type=int, default=8000)
    parser.add_argument("--pgvector-port", type=int, default=5432)
    parser.add_argument("--qdrant-port", type=int, default=6333)
    parser.add_argument("--sift-dir", default="bench/vector_data")
    parser.add_argument("--output", help="JSON output file")
    args = parser.parse_args()

    # create client
    if args.system == "ember":
        client = EmberClient(port=args.ember_port)
    elif args.system == "ember-grpc":
        client = EmberGrpcClient(port=args.ember_grpc_port)
    elif args.system == "chromadb":
        client = ChromaClient(port=args.chroma_port)
    elif args.system == "pgvector":
        client = PgVectorClient(port=args.pgvector_port)
    elif args.system == "qdrant":
        client = QdrantClient_(port=args.qdrant_port)

    # load or generate vectors
    if args.mode == "sift":
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vector_data"))
        from sift_loader import load_sift1m
        base_vectors, query_vectors, ground_truth = load_sift1m(args.sift_dir)
        dim = base_vectors.shape[1]
        print(f"loaded SIFT1M: {len(base_vectors)} base, {len(query_vectors)} queries, dim={dim}", file=sys.stderr)
    else:
        dim = args.dim
        print(f"generating {args.count} random vectors (dim={dim})...", file=sys.stderr)
        all_vectors = generate_vectors(args.count + args.queries, dim)
        base_vectors = all_vectors[:args.count]
        query_vectors = all_vectors[args.count:]
        ground_truth = None

    # setup
    client.setup(dim)

    # run benchmarks
    print(f"benchmarking {client.name()} insert...", file=sys.stderr)
    insert_result = benchmark_insert(client, base_vectors, batch_size=args.batch_size)
    print(f"  {insert_result['throughput']:.0f} vectors/sec", file=sys.stderr)

    print(f"benchmarking {client.name()} query (k={args.k})...", file=sys.stderr)
    query_result = benchmark_query(client, query_vectors, k=args.k)
    print(f"  {query_result['throughput']:.0f} queries/sec, p99={query_result['p99_ms']:.2f}ms", file=sys.stderr)

    # recall (sift mode only)
    recall_result = None
    if args.mode == "sift" and ground_truth is not None:
        print(f"computing recall@{args.k}...", file=sys.stderr)
        recall_result = compute_recall(client, query_vectors, ground_truth, k=args.k)
        print(f"  recall@{args.k} = {recall_result['recall_at_k']:.4f}", file=sys.stderr)

    # cleanup
    client.teardown()

    # output
    result = {
        "system": client.name(),
        "mode": args.mode,
        "dim": dim,
        "insert": insert_result,
        "query": query_result,
    }
    if recall_result:
        result["recall"] = recall_result

    output_json = json.dumps(result, indent=2)

    if args.output:
        with open(args.output, "w") as f:
            f.write(output_json + "\n")
        print(f"results written to {args.output}", file=sys.stderr)
    else:
        print(output_json)


if __name__ == "__main__":
    main()
