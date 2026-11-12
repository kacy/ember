//! SCAN over the keyspace.

use super::*;

impl Keyspace {
    /// Scans proto keys starting from a cursor position.
    ///
    /// Returns only keys holding `Value::Proto` values. If `type_name` is
    /// provided, further restricts to keys whose message type matches exactly.
    /// Pattern matching follows the same glob rules as `scan_keys`.
    #[cfg(feature = "protobuf")]
    pub fn scan_proto_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
        type_name: Option<&str>,
    ) -> (u64, Vec<String>) {
        self.scan_entries(cursor, count, |key, entry| {
            let Value::Proto { type_name: t, .. } = &entry.value else {
                return false;
            };
            type_name.is_none_or(|wanted| t.as_str() == wanted)
                && pattern.is_none_or(|pat| glob_match(pat, key))
        })
    }

    /// Scans keys starting from a cursor position.
    ///
    /// Returns the next cursor (0 if scan complete) and a batch of keys.
    /// The `pattern` argument uses the glob rules in [`crate::glob`].
    pub fn scan_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> (u64, Vec<String>) {
        self.scan_entries(cursor, count, |key, _| {
            pattern.is_none_or(|pat| glob_match(pat, key))
        })
    }

    /// Walks entries down from the cursor, collecting live keys that pass
    /// `keep`, until `count` are found or the walk reaches the start.
    ///
    /// The cursor is the number of positions left to visit: 0 starts a
    /// scan, and a returned 0 means it is done. Each call costs O(count)
    /// plus the entries skipped. Walking down from the end keeps Redis's
    /// guarantee that a key present for the whole scan is returned:
    /// `swap_remove` only moves the last entry, which the walk has already
    /// passed, into a lower slot. It may be returned twice. Keys added
    /// during the scan go at the end and may be missed, as Redis allows.
    fn scan_entries(
        &self,
        cursor: u64,
        count: usize,
        keep: impl Fn(&str, &Entry) -> bool,
    ) -> (u64, Vec<String>) {
        let target = if count == 0 { 10 } else { count };
        let len = self.entries.len();
        let mut position = match cursor {
            0 => len,
            c => usize::try_from(c).map_or(len, |c| c.min(len)),
        };
        let mut keys = Vec::with_capacity(target.min(position));
        while position > 0 && keys.len() < target {
            position -= 1;
            let (key, entry) = &self
                .entries
                .get_index(position)
                .expect("position is below len");
            if !entry.is_expired() && keep(key, entry) {
                keys.push(key.to_string());
            }
        }
        (position as u64, keys)
    }
}
