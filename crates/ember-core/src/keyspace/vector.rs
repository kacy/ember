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
        if !self.enforce_memory_limit(key, estimated_increase) {
            return Err(VectorWriteError::OutOfMemory);
        }

        if is_new {
            let vs = VectorSet::new(dim, metric, quantization, connectivity, expansion_add)
                .map_err(|e| VectorWriteError::IndexError(e.to_string()))?;
            let value = Value::Vector(Box::new(vs));
            self.memory.add(key, &value);
            let entry = Entry::new(value, None);
            self.entries.insert(CompactString::from(key), entry);
            self.bump_version(key);
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
        entry.touch(self.track_access);

        let new_entry_size = entry.entry_size(key);
        self.memory.adjust(old_entry_size, new_entry_size);
        self.bump_version(key);

        Ok(VAddResult {
            element,
            vector,
            added,
        })
    }

    /// Adds multiple vectors to a vector set in a single operation.
    ///
    /// All vectors are validated upfront (dimension, NaN and infinity)
    /// before any is inserted. An element named more than once takes its
    /// last vector. If some vectors fail to go into the index, the others
    /// stay, and the result lists what was applied and which existing
    /// elements were lost, so the AOF and replicas can follow.
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

        let entries = last_of_each(entries);
        if entries.is_empty() {
            return Ok(VAddBatchResult::default());
        }

        self.remove_if_expired(key);

        // type check, and the dimension an existing set requires
        let existing_dim = match self.entries.get(key) {
            None => None,
            Some(Entry {
                value: Value::Vector(vs),
                ..
            }) => Some(vs.dim()),
            Some(_) => return Err(VectorWriteError::WrongType),
        };
        let is_new = existing_dim.is_none();

        // validate all vectors upfront — reject entire batch on NaN/inf
        let dim = existing_dim.unwrap_or(entries[0].1.len());
        for (elem, vec) in &entries {
            if vec.len() != dim {
                return Err(VectorWriteError::IndexError(format!(
                    "dimension mismatch: expected {dim}, element '{elem}' has {}",
                    vec.len()
                )));
            }
            for &v in vec {
                if !v.is_finite() {
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
        if !self.enforce_memory_limit(key, estimated_increase) {
            return Err(VectorWriteError::OutOfMemory);
        }

        // create vector set if new
        if is_new {
            let vs = VectorSet::new(dim, metric, quantization, connectivity, expansion_add)
                .map_err(|e| VectorWriteError::IndexError(e.to_string()))?;
            let value = Value::Vector(Box::new(vs));
            self.memory.add(key, &value);
            let new_entry = Entry::new(value, None);
            self.entries.insert(CompactString::from(key), new_entry);
            self.bump_version(key);
        }

        let track_access = self.track_access;
        let Some(entry) = self.entries.get_mut(key) else {
            return Err(VectorWriteError::IndexError("entry missing".into()));
        };
        let old_entry_size = entry.entry_size(key);
        let Value::Vector(ref mut vs) = entry.value else {
            return Err(VectorWriteError::WrongType);
        };
        // updates that fail lose their old vector, so note which elements
        // existed before
        let existed: Vec<bool> = entries.iter().map(|(e, _)| vs.contains(e)).collect();
        let batch = vs
            .add_batch_parallel(&entries)
            .map_err(|e| VectorWriteError::IndexError(e.to_string()))?;
        entry.touch(track_access);
        let new_entry_size = entry.entry_size(key);
        self.memory.adjust(old_entry_size, new_entry_size);
        self.bump_version(key);

        let failed: std::collections::HashSet<&str> =
            batch.failed.iter().map(String::as_str).collect();
        let mut result = VAddBatchResult {
            added_count: batch.added_count,
            error: batch.error.map(|e| e.to_string()),
            ..VAddBatchResult::default()
        };
        for ((element, vector), existed) in entries.into_iter().zip(existed) {
            if !failed.contains(element.as_str()) {
                result.applied.push((element, vector));
            } else if existed {
                result.removed.push(element);
            }
        }
        Ok(result)
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

        entry.touch(self.track_access);

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
            entry.touch(self.track_access);
            let is_empty = matches!(entry.value, Value::Vector(ref vs) if vs.is_empty());
            let new_size = entry.entry_size(key);
            self.memory.adjust(old_size, new_size);
            self.bump_version(key);

            if is_empty {
                self.memory.remove_with_size(new_size);
                if let Some(removed) = self.entries.swap_remove(key) {
                    self.untrack_expiry(key, &removed);
                }
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

        entry.touch(self.track_access);

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

/// Keeps the last vector given for each element, in the order the
/// elements last appear.
#[cfg(feature = "vector")]
fn last_of_each(entries: Vec<(String, Vec<f32>)>) -> Vec<(String, Vec<f32>)> {
    let mut seen = std::collections::HashSet::new();
    let mut kept: Vec<_> = entries
        .into_iter()
        .rev()
        .filter(|(element, _)| seen.insert(element.clone()))
        .collect();
    kept.reverse();
    kept
}

#[cfg(all(test, feature = "vector"))]
mod tests {
    use super::*;
    use crate::types::vector::{DistanceMetric, QuantizationType};

    fn add(
        ks: &mut Keyspace,
        entries: &[(&str, &[f32])],
    ) -> Result<VAddBatchResult, VectorWriteError> {
        let entries = entries
            .iter()
            .map(|(e, v)| (e.to_string(), v.to_vec()))
            .collect();
        ks.vadd_batch(
            "v",
            entries,
            DistanceMetric::from_u8(0),
            QuantizationType::from_u8(0),
            16,
            64,
        )
    }

    fn set_len(ks: &Keyspace) -> usize {
        match &ks.entries.get("v").unwrap().value {
            Value::Vector(vs) => vs.len(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn a_batch_must_match_the_dimension_of_an_existing_set() {
        let mut ks = Keyspace::new();
        add(&mut ks, &[("a", &[1.0, 2.0])]).unwrap();
        let result = add(&mut ks, &[("b", &[1.0, 2.0, 3.0])]);
        assert!(matches!(result, Err(VectorWriteError::IndexError(_))));
        assert_eq!(set_len(&ks), 1);
    }

    #[test]
    fn an_element_named_twice_in_a_batch_takes_its_last_vector() {
        let mut ks = Keyspace::new();
        let result = add(
            &mut ks,
            &[("a", &[1.0, 0.0]), ("b", &[0.0, 1.0]), ("a", &[2.0, 2.0])],
        )
        .unwrap();
        assert_eq!(result.added_count, 2);
        assert!(result.error.is_none());
        assert_eq!(
            result.applied,
            [
                ("b".to_owned(), vec![0.0, 1.0]),
                ("a".to_owned(), vec![2.0, 2.0])
            ]
        );
        assert_eq!(set_len(&ks), 2);
    }
}
