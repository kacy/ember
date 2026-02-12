//! HNSW-backed vector sets for similarity search.
//!
//! Each vector set owns a usearch `Index` with fixed dimensionality and
//! distance metric. Elements are named strings mapped to dense float vectors,
//! analogous to how sorted set members have scores.

use std::collections::HashMap;
use std::fmt;

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

/// Distance metric for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

impl DistanceMetric {
    fn to_metric_kind(self) -> MetricKind {
        match self {
            DistanceMetric::Cosine => MetricKind::Cos,
            DistanceMetric::L2 => MetricKind::L2sq,
            DistanceMetric::InnerProduct => MetricKind::IP,
        }
    }

    /// Returns the string name used in VINFO output and protocol parsing.
    pub fn as_str(self) -> &'static str {
        match self {
            DistanceMetric::Cosine => "cosine",
            DistanceMetric::L2 => "l2",
            DistanceMetric::InnerProduct => "ip",
        }
    }
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<DistanceMetric> for u8 {
    fn from(m: DistanceMetric) -> u8 {
        match m {
            DistanceMetric::Cosine => 0,
            DistanceMetric::L2 => 1,
            DistanceMetric::InnerProduct => 2,
        }
    }
}

impl DistanceMetric {
    /// Converts a wire-format byte to a distance metric.
    /// Defaults to `Cosine` for unknown values (forward compatibility).
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => DistanceMetric::L2,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        }
    }
}

/// Quantization type for stored vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationType {
    F32,
    F16,
    I8,
}

impl QuantizationType {
    fn to_scalar_kind(self) -> ScalarKind {
        match self {
            QuantizationType::F32 => ScalarKind::F32,
            QuantizationType::F16 => ScalarKind::F16,
            QuantizationType::I8 => ScalarKind::I8,
        }
    }

    /// Bytes per element for this quantization level.
    pub fn bytes_per_element(self) -> usize {
        match self {
            QuantizationType::F32 => 4,
            QuantizationType::F16 => 2,
            QuantizationType::I8 => 1,
        }
    }

    /// Returns the string name used in VINFO output and protocol parsing.
    pub fn as_str(self) -> &'static str {
        match self {
            QuantizationType::F32 => "f32",
            QuantizationType::F16 => "f16",
            QuantizationType::I8 => "i8",
        }
    }
}

impl fmt::Display for QuantizationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<QuantizationType> for u8 {
    fn from(q: QuantizationType) -> u8 {
        match q {
            QuantizationType::F32 => 0,
            QuantizationType::F16 => 1,
            QuantizationType::I8 => 2,
        }
    }
}

impl QuantizationType {
    /// Converts a wire-format byte to a quantization type.
    /// Defaults to `F32` for unknown values (forward compatibility).
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => QuantizationType::F16,
            2 => QuantizationType::I8,
            _ => QuantizationType::F32,
        }
    }
}

/// Metadata about a vector set, returned by VINFO.
#[derive(Debug, Clone)]
pub struct VectorSetInfo {
    pub dim: usize,
    pub count: usize,
    pub metric: DistanceMetric,
    pub quantization: QuantizationType,
    pub connectivity: usize,
    pub expansion_add: usize,
}

/// A single search result: element name + distance.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub element: String,
    pub distance: f32,
}

/// Error type for vector operations.
#[derive(Debug, thiserror::Error)]
pub enum VectorError {
    #[error("dimension mismatch: index has {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("usearch error: {0}")]
    Index(String),
}

/// A set of named vectors backed by a usearch HNSW index.
///
/// Each element is a string name mapped to a dense f32 vector. The index
/// is configured on first insert and its parameters (dim, metric, quant,
/// connectivity) are immutable after that.
///
/// Analogous to Redis sorted sets where members have scores, here elements
/// have vectors.
pub struct VectorSet {
    index: Index,
    /// element name → usearch key
    elements: HashMap<String, u64>,
    /// usearch key → element name (for translating search results)
    names: HashMap<u64, String>,
    /// monotonic key counter for usearch
    next_key: u64,
    /// vector dimensionality, locked after first insert
    dim: usize,
    /// distance metric
    metric: DistanceMetric,
    /// quantization level
    quantization: QuantizationType,
    /// HNSW connectivity parameter (M)
    connectivity: usize,
    /// HNSW construction beam width (ef_construction)
    expansion_add: usize,
}

impl VectorSet {
    /// Creates a new vector set with the given configuration.
    ///
    /// `dim` is the fixed dimensionality for all vectors in this set.
    /// The usearch index is created eagerly with initial capacity.
    pub fn new(
        dim: usize,
        metric: DistanceMetric,
        quantization: QuantizationType,
        connectivity: usize,
        expansion_add: usize,
    ) -> Result<Self, VectorError> {
        let options = IndexOptions {
            dimensions: dim,
            metric: metric.to_metric_kind(),
            quantization: quantization.to_scalar_kind(),
            connectivity,
            expansion_add,
            expansion_search: 0, // use default at search time
            multi: false,
        };

        let index = Index::new(&options).map_err(|e| VectorError::Index(e.to_string()))?;
        index
            .reserve(64)
            .map_err(|e| VectorError::Index(e.to_string()))?;

        Ok(Self {
            index,
            elements: HashMap::new(),
            names: HashMap::new(),
            next_key: 0,
            dim,
            metric,
            quantization,
            connectivity,
            expansion_add,
        })
    }

    /// Adds or replaces a vector for the given element name.
    ///
    /// Returns `true` if a new element was added, `false` if an existing
    /// element was updated.
    pub fn add(&mut self, element: String, vector: &[f32]) -> Result<bool, VectorError> {
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        // ensure capacity (double when full, amortized O(1))
        if self.index.size() >= self.index.capacity() {
            let new_cap = (self.index.capacity() * 2).max(64);
            self.index
                .reserve(new_cap)
                .map_err(|e| VectorError::Index(e.to_string()))?;
        }

        let is_new = if let Some(&existing_key) = self.elements.get(&element) {
            // remove old vector, then re-insert with same key
            let _ = self.index.remove(existing_key);
            self.index
                .add(existing_key, vector)
                .map_err(|e| VectorError::Index(e.to_string()))?;
            false
        } else {
            let key = self.next_key;
            self.next_key += 1;
            self.index
                .add(key, vector)
                .map_err(|e| VectorError::Index(e.to_string()))?;
            self.elements.insert(element.clone(), key);
            self.names.insert(key, element);
            true
        };

        Ok(is_new)
    }

    /// Removes an element from the vector set.
    ///
    /// Returns `true` if the element existed and was removed.
    pub fn remove(&mut self, element: &str) -> bool {
        if let Some(key) = self.elements.remove(element) {
            self.names.remove(&key);
            // usearch remove marks the entry as deleted (lazy tombstone).
            // the space is reclaimed on subsequent adds.
            let _ = self.index.remove(key);
            true
        } else {
            false
        }
    }

    /// Searches for the k nearest neighbors of the given query vector.
    ///
    /// `ef_search` controls the search beam width (higher = more accurate,
    /// slower). Pass 0 to use usearch's default.
    pub fn search(
        &self,
        query: &[f32],
        count: usize,
        ef_search: usize,
    ) -> Result<Vec<SearchResult>, VectorError> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        if self.elements.is_empty() {
            return Ok(Vec::new());
        }

        // temporarily adjust search expansion if requested
        if ef_search > 0 {
            self.index.change_expansion_search(ef_search);
        }

        let matches = self
            .index
            .search(query, count)
            .map_err(|e| VectorError::Index(e.to_string()))?;

        let mut results = Vec::with_capacity(matches.keys.len());
        for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            if let Some(name) = self.names.get(key) {
                results.push(SearchResult {
                    element: name.clone(),
                    distance: *distance,
                });
            }
        }

        Ok(results)
    }

    /// Retrieves the stored vector for an element.
    ///
    /// Returns `None` if the element doesn't exist.
    pub fn get(&self, element: &str) -> Option<Vec<f32>> {
        let &key = self.elements.get(element)?;
        let mut buffer = vec![0.0f32; self.dim];
        match self.index.get(key, &mut buffer) {
            Ok(found) if found > 0 => Some(buffer),
            _ => None,
        }
    }

    /// Returns the number of elements in the vector set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns `true` if the vector set has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the dimensionality of vectors in this set.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Returns the distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Returns the quantization type.
    pub fn quantization(&self) -> QuantizationType {
        self.quantization
    }

    /// Returns metadata about this vector set.
    pub fn info(&self) -> VectorSetInfo {
        VectorSetInfo {
            dim: self.dim,
            count: self.elements.len(),
            metric: self.metric,
            quantization: self.quantization,
            connectivity: self.connectivity,
            expansion_add: self.expansion_add,
        }
    }

    /// Returns an iterator over all element names.
    ///
    /// Used for snapshot serialization — the caller retrieves each vector
    /// via `get()`.
    pub fn elements(&self) -> impl Iterator<Item = &str> {
        self.elements.keys().map(String::as_str)
    }

    /// Returns the HNSW connectivity parameter.
    pub fn connectivity(&self) -> usize {
        self.connectivity
    }

    /// Returns the HNSW construction beam width.
    pub fn expansion_add(&self) -> usize {
        self.expansion_add
    }

    /// Estimates memory usage in bytes.
    ///
    /// Accounts for: usearch index storage (vectors + HNSW graph),
    /// element↔key hashmaps, and string names.
    pub fn memory_usage(&self) -> usize {
        let count = self.elements.len();

        // usearch internal: vector storage + HNSW graph edges
        let vector_bytes = count * self.dim * self.quantization.bytes_per_element();
        let graph_bytes = count * self.connectivity * 2 * 8; // each edge is a u64 key

        // rust-side hashmaps: elements + names
        let name_bytes: usize = self
            .elements
            .keys()
            .map(|name| name.len() + 80) // String + HashMap entry overhead for both maps
            .sum();

        Self::BASE_OVERHEAD + vector_bytes + graph_bytes + name_bytes
    }

    /// Base overhead of an empty VectorSet (usearch index shell + two HashMaps).
    pub const BASE_OVERHEAD: usize = 128;
}

impl fmt::Debug for VectorSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSet")
            .field("dim", &self.dim)
            .field("count", &self.elements.len())
            .field("metric", &self.metric)
            .field("quantization", &self.quantization)
            .finish()
    }
}

impl Clone for VectorSet {
    fn clone(&self) -> Self {
        // rebuild the index from scratch — usearch Index doesn't implement Clone
        let mut new = Self::new(
            self.dim,
            self.metric,
            self.quantization,
            self.connectivity,
            self.expansion_add,
        )
        .expect("clone: failed to create index with same config");

        for (name, &key) in &self.elements {
            let mut buffer = vec![0.0f32; self.dim];
            if self.index.get(key, &mut buffer).is_ok() {
                let _ = new.add(name.clone(), &buffer);
            }
        }

        new
    }
}

impl PartialEq for VectorSet {
    fn eq(&self, other: &Self) -> bool {
        if self.dim != other.dim
            || self.metric != other.metric
            || self.quantization != other.quantization
            || self.elements.len() != other.elements.len()
        {
            return false;
        }

        // compare all element names and their vectors
        for (name, &key) in &self.elements {
            match other.elements.get(name) {
                Some(&other_key) => {
                    let mut buf_a = vec![0.0f32; self.dim];
                    let mut buf_b = vec![0.0f32; self.dim];
                    let ok_a = self.index.get(key, &mut buf_a).is_ok();
                    let ok_b = other.index.get(other_key, &mut buf_b).is_ok();
                    if ok_a != ok_b || buf_a != buf_b {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_set(dim: usize) -> VectorSet {
        VectorSet::new(dim, DistanceMetric::Cosine, QuantizationType::F32, 16, 64).unwrap()
    }

    #[test]
    fn add_and_get() {
        let mut vs = make_set(3);
        let added = vs.add("a".into(), &[1.0, 0.0, 0.0]).unwrap();
        assert!(added);
        assert_eq!(vs.len(), 1);

        let vec = vs.get("a").unwrap();
        assert_eq!(vec, vec![1.0, 0.0, 0.0]);
    }

    #[test]
    fn add_update_existing() {
        let mut vs = make_set(3);
        vs.add("a".into(), &[1.0, 0.0, 0.0]).unwrap();

        let added = vs.add("a".into(), &[0.0, 1.0, 0.0]).unwrap();
        assert!(!added); // update, not new
        assert_eq!(vs.len(), 1);

        let vec = vs.get("a").unwrap();
        assert_eq!(vec, vec![0.0, 1.0, 0.0]);
    }

    #[test]
    fn dimension_mismatch() {
        let mut vs = make_set(3);
        let err = vs.add("a".into(), &[1.0, 0.0]).unwrap_err();
        assert!(matches!(
            err,
            VectorError::DimensionMismatch {
                expected: 3,
                got: 2
            }
        ));
    }

    #[test]
    fn remove_element() {
        let mut vs = make_set(3);
        vs.add("a".into(), &[1.0, 0.0, 0.0]).unwrap();

        assert!(vs.remove("a"));
        assert_eq!(vs.len(), 0);
        assert!(vs.get("a").is_none());
        assert!(!vs.remove("a")); // already gone
    }

    #[test]
    fn search_basic() {
        let mut vs = make_set(3);
        vs.add("x-axis".into(), &[1.0, 0.0, 0.0]).unwrap();
        vs.add("y-axis".into(), &[0.0, 1.0, 0.0]).unwrap();
        vs.add("z-axis".into(), &[0.0, 0.0, 1.0]).unwrap();

        // searching near x-axis should return x-axis first
        let results = vs.search(&[0.9, 0.1, 0.0], 2, 0).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].element, "x-axis");
    }

    #[test]
    fn search_empty_set() {
        let vs = make_set(3);
        let results = vs.search(&[1.0, 0.0, 0.0], 5, 0).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_dimension_mismatch() {
        let vs = make_set(3);
        let err = vs.search(&[1.0, 0.0], 5, 0).unwrap_err();
        assert!(matches!(err, VectorError::DimensionMismatch { .. }));
    }

    #[test]
    fn get_nonexistent() {
        let vs = make_set(3);
        assert!(vs.get("nope").is_none());
    }

    #[test]
    fn info() {
        let vs = make_set(4);
        let info = vs.info();
        assert_eq!(info.dim, 4);
        assert_eq!(info.count, 0);
        assert_eq!(info.metric, DistanceMetric::Cosine);
        assert_eq!(info.quantization, QuantizationType::F32);
        assert_eq!(info.connectivity, 16);
        assert_eq!(info.expansion_add, 64);
    }

    #[test]
    fn memory_usage_grows() {
        let mut vs = make_set(128);
        let base = vs.memory_usage();

        vs.add("a".into(), &vec![0.0; 128]).unwrap();
        let with_one = vs.memory_usage();
        assert!(with_one > base);

        vs.add("b".into(), &vec![0.0; 128]).unwrap();
        assert!(vs.memory_usage() > with_one);
    }

    #[test]
    fn clone_preserves_data() {
        let mut vs = make_set(3);
        vs.add("a".into(), &[1.0, 2.0, 3.0]).unwrap();
        vs.add("b".into(), &[4.0, 5.0, 6.0]).unwrap();

        let cloned = vs.clone();
        assert_eq!(cloned.len(), 2);
        assert_eq!(cloned.get("a").unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(cloned.get("b").unwrap(), vec![4.0, 5.0, 6.0]);
    }

    #[test]
    fn partial_eq() {
        let mut a = make_set(3);
        a.add("x".into(), &[1.0, 0.0, 0.0]).unwrap();

        let mut b = make_set(3);
        b.add("x".into(), &[1.0, 0.0, 0.0]).unwrap();

        assert_eq!(a, b);
    }

    #[test]
    fn l2_metric() {
        let mut vs = VectorSet::new(2, DistanceMetric::L2, QuantizationType::F32, 16, 64).unwrap();
        vs.add("origin".into(), &[0.0, 0.0]).unwrap();
        vs.add("far".into(), &[10.0, 10.0]).unwrap();

        let results = vs.search(&[0.1, 0.1], 1, 0).unwrap();
        assert_eq!(results[0].element, "origin");
    }

    #[test]
    fn different_quantization() {
        // F16 should work, though values may lose precision
        let mut vs =
            VectorSet::new(3, DistanceMetric::Cosine, QuantizationType::F16, 16, 64).unwrap();
        vs.add("a".into(), &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(vs.len(), 1);

        // we can still get the vector back (with possible precision loss)
        let vec = vs.get("a").unwrap();
        assert!((vec[0] - 1.0).abs() < 0.01);
    }
}
