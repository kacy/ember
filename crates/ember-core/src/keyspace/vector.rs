#[cfg(feature = "vector")]
use super::*;

#[cfg(feature = "vector")]
impl Keyspace {
    /// Adds a vector to a vector set, creating the set if it doesn't exist.
    ///
    /// On first insert, the set's configuration (dim, metric, quantization,
    /// connectivity, expansion_add) is locked. Subsequent inserts must match
    /// the established dimensionality.
    ///
    /// Returns a `VAddResult` with the element name, vector, and whether it
    /// was newly added (for AOF recording).
    #[allow(clippy::too_many_arguments)]
    pub fn vadd(
        &mut self,
        key: &str,
        element: String,
        vector: Vec<f32>,
        metric: crate::types::vector::DistanceMetric,
        quantization: crate::types::vector::QuantizationType,
        connectivity: usize,
        expansion_add: usize,
    ) -> Result<VAddResult, VectorWriteError> {
        use crate::types::vector::VectorSet;

        self.remove_if_expired(key);

        let is_new = match self.entries.get(key) {
            None => true,
            Some(e) if matches!(e.value, Value::Vector(_)) => false,
            Some(_) => return Err(VectorWriteError::WrongType),
        };

        // estimate memory for the new vector (saturating to avoid overflow)
        let dim = vector.len();
        let per_vector = dim
            .saturating_mul(quantization.bytes_per_element())
            .saturating_add(connectivity.saturating_mul(16))
            .saturating_add(element.len())
            .saturating_add(80);
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + VectorSet::BASE_OVERHEAD + per_vector
        } else {
            per_vector
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(VectorWriteError::OutOfMemory);
        }

        if is_new {
            let vs = VectorSet::new(dim, metric, quantization, connectivity, expansion_add)
                .map_err(|e| VectorWriteError::IndexError(e.to_string()))?;
            let value = Value::Vector(vs);
            self.memory.add(key, &value);
            self.entries
                .insert(CompactString::from(key), Entry::new(value, None));
        }

        let entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Err(VectorWriteError::IndexError("entry missing".into())),
        };
        let old_entry_size = entry.entry_size(key);

        let added = match entry.value {
            Value::Vector(ref mut vs) => vs
                .add(element.clone(), &vector)
                .map_err(|e| VectorWriteError::IndexError(e.to_string()))?,
            _ => return Err(VectorWriteError::WrongType),
        };
        entry.touch();

        let new_value_size = memory::value_size(&entry.value);
        entry.cached_value_size = new_value_size;
        let new_entry_size = key.len() + new_value_size + memory::ENTRY_OVERHEAD;
        self.memory.adjust(old_entry_size, new_entry_size);

        Ok(VAddResult {
            element,
            vector,
            added,
        })
    }

    /// Adds multiple vectors to a vector set in a single operation.
    ///
    /// All vectors are validated upfront (NaN/inf check) before any are inserted.
    /// Memory is estimated for the entire batch with one `enforce_memory_limit` call.
    /// On usearch error mid-batch, returns the error but already-applied vectors
    /// are included in the result for AOF persistence.
    #[allow(clippy::too_many_arguments)]
    pub fn vadd_batch(
        &mut self,
        key: &str,
        entries: Vec<(String, Vec<f32>)>,
        metric: crate::types::vector::DistanceMetric,
        quantization: crate::types::vector::QuantizationType,
        connectivity: usize,
        expansion_add: usize,
    ) -> Result<VAddBatchResult, VectorWriteError> {
        use crate::types::vector::VectorSet;

        if entries.is_empty() {
            return Ok(VAddBatchResult {
                added_count: 0,
                applied: Vec::new(),
            });
        }

        self.remove_if_expired(key);

        // type check
        let is_new = match self.entries.get(key) {
            None => true,
            Some(e) if matches!(e.value, Value::Vector(_)) => false,
            Some(_) => return Err(VectorWriteError::WrongType),
        };

        // validate all vectors upfront — reject entire batch on NaN/inf
        let dim = entries[0].1.len();
        for (elem, vec) in &entries {
            if vec.len() != dim {
                return Err(VectorWriteError::IndexError(format!(
                    "dimension mismatch: expected {dim}, element '{elem}' has {}",
                    vec.len()
                )));
            }
            for &v in vec {
                if v.is_nan() || v.is_infinite() {
                    return Err(VectorWriteError::IndexError(format!(
                        "element '{elem}' contains NaN or infinity"
                    )));
                }
            }
        }

        // estimate total memory for all vectors
        let per_vector = dim
            .saturating_mul(quantization.bytes_per_element())
            .saturating_add(connectivity.saturating_mul(16))
            .saturating_add(80);
        let total_elem_names: usize = entries.iter().map(|(e, _)| e.len()).sum();
        let vectors_cost = entries
            .len()
            .saturating_mul(per_vector)
            .saturating_add(total_elem_names);
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + VectorSet::BASE_OVERHEAD + vectors_cost
        } else {
            vectors_cost
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(VectorWriteError::OutOfMemory);
        }

        // create vector set if new
        if is_new {
            let vs = VectorSet::new(dim, metric, quantization, connectivity, expansion_add)
                .map_err(|e| VectorWriteError::IndexError(e.to_string()))?;
            let value = Value::Vector(vs);
            self.memory.add(key, &value);
            self.entries
                .insert(CompactString::from(key), Entry::new(value, None));
        }

        let entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Err(VectorWriteError::IndexError("entry missing".into())),
        };
        let old_entry_size = entry.entry_size(key);

        let mut added_count = 0;
        let mut applied = Vec::with_capacity(entries.len());

        match entry.value {
            Value::Vector(ref mut vs) => {
                // pre-allocate index capacity for the entire batch to avoid
                // incremental resizes during insertion
                if let Err(e) = vs.reserve(entries.len()) {
                    return Err(VectorWriteError::IndexError(e.to_string()));
                }

                for (element, vector) in entries {
                    // clone element for the index (which needs ownership),
                    // then move both element and vector into applied
                    match vs.add(element.clone(), &vector) {
                        Ok(added) => {
                            if added {
                                added_count += 1;
                            }
                            applied.push((element, vector));
                        }
                        Err(e) => {
                            // partial insert: return applied vectors so they can
                            // be persisted to AOF despite the error
                            entry.touch();
                            let new_vs = memory::value_size(&entry.value);
                            entry.cached_value_size = new_vs;
                            let new_entry_size = key.len() + new_vs + memory::ENTRY_OVERHEAD;
                            self.memory.adjust(old_entry_size, new_entry_size);
                            return Err(VectorWriteError::PartialBatch {
                                message: format!(
                                    "error at element '{}': {e} ({} vectors applied before failure)",
                                    element,
                                    applied.len()
                                ),
                                applied,
                            });
                        }
                    }
                }
            }
            _ => return Err(VectorWriteError::WrongType),
        }

        entry.touch();
        let new_vs = memory::value_size(&entry.value);
        entry.cached_value_size = new_vs;
        let new_entry_size = key.len() + new_vs + memory::ENTRY_OVERHEAD;
        self.memory.adjust(old_entry_size, new_entry_size);

        Ok(VAddBatchResult {
            added_count,
            applied,
        })
    }

    /// Searches for the k nearest neighbors in a vector set.
    pub fn vsim(
        &mut self,
        key: &str,
        query: &[f32],
        count: usize,
        ef_search: usize,
    ) -> Result<Vec<crate::types::vector::SearchResult>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(Vec::new());
        }

        let entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };

        entry.touch();

        match entry.value {
            Value::Vector(ref vs) => vs.search(query, count, ef_search).map_err(|_| WrongType),
            _ => Err(WrongType),
        }
    }

    /// Removes an element from a vector set. Returns `true` if the element
    /// existed. Deletes the key if the set becomes empty.
    pub fn vrem(&mut self, key: &str, element: &str) -> Result<bool, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(false);
        }

        let entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Ok(false),
        };

        if !matches!(entry.value, Value::Vector(_)) {
            return Err(WrongType);
        }

        let old_size = entry.entry_size(key);

        let removed = match entry.value {
            Value::Vector(ref mut vs) => vs.remove(element),
            _ => return Err(WrongType),
        };

        if removed {
            entry.touch();
            let is_empty = matches!(entry.value, Value::Vector(ref vs) if vs.is_empty());
            let new_vs = memory::value_size(&entry.value);
            entry.cached_value_size = new_vs;
            let new_size = key.len() + new_vs + memory::ENTRY_OVERHEAD;
            self.memory.adjust(old_size, new_size);

            if is_empty {
                self.memory.remove_with_size(new_size);
                self.entries.remove(key);
            }
        }

        Ok(removed)
    }

    /// Retrieves the stored vector for an element.
    pub fn vget(&mut self, key: &str, element: &str) -> Result<Option<Vec<f32>>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

        let entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        entry.touch();

        match entry.value {
            Value::Vector(ref vs) => Ok(vs.get(element)),
            _ => Err(WrongType),
        }
    }

    /// Returns the number of elements in a vector set.
    pub fn vcard(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }

        match self.entries.get(key) {
            None => Ok(0),
            Some(e) => match e.value {
                Value::Vector(ref vs) => Ok(vs.len()),
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the dimensionality of a vector set, or 0 if the key doesn't exist.
    pub fn vdim(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }

        match self.entries.get(key) {
            None => Ok(0),
            Some(e) => match e.value {
                Value::Vector(ref vs) => Ok(vs.dim()),
                _ => Err(WrongType),
            },
        }
    }

    /// Returns metadata about a vector set.
    pub fn vinfo(
        &mut self,
        key: &str,
    ) -> Result<Option<crate::types::vector::VectorSetInfo>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

        match self.entries.get(key) {
            None => Ok(None),
            Some(e) => match e.value {
                Value::Vector(ref vs) => Ok(Some(vs.info())),
                _ => Err(WrongType),
            },
        }
    }
}
