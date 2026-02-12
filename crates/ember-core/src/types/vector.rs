//! HNSW-backed vector sets for similarity search.
//!
//! Each vector set owns a usearch `Index` with fixed dimensionality and
//! distance metric. Elements are named strings mapped to dense float vectors,
//! analogous to how sorted set members have scores.
