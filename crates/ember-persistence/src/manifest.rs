//! Records how many shards wrote a data directory.
//!
//! Keys are routed to shards by hash modulo the shard count, and each shard
//! keeps its own AOF and snapshot. Starting with a different count would
//! leave some shard files unread and route recovered keys to shards that do
//! not hold them, so the count is saved next to the data and checked at
//! startup.

use std::fs;
use std::io;
use std::path::Path;

const MANIFEST_FILE: &str = "shards";

/// Checks that `data_dir` was written with `shard_count` shards, then
/// records that count.
///
/// Uses the saved count when there is one. A directory from before the
/// count was saved falls back to the highest shard number among its files.
/// Returns a message for the operator when the counts differ.
pub fn check_shard_count(data_dir: &Path, shard_count: usize) -> Result<(), String> {
    let io_error =
        |e: io::Error| format!("cannot check shard count in {}: {e}", data_dir.display());
    fs::create_dir_all(data_dir).map_err(io_error)?;

    let manifest = data_dir.join(MANIFEST_FILE);
    let previous = match fs::read_to_string(&manifest) {
        Ok(contents) => Some(
            contents
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("{} does not hold a shard count", manifest.display()))?,
        ),
        Err(e) if e.kind() == io::ErrorKind::NotFound => shard_count_from_files(data_dir)?,
        Err(e) => return Err(io_error(e)),
    };

    if let Some(previous) = previous {
        if previous != shard_count {
            return Err(format!(
                "{} holds data for {previous} shards, but the server is configured for \
                 {shard_count}. set shards = {previous} to use this data",
                data_dir.display()
            ));
        }
    }

    let tmp = data_dir.join(format!("{MANIFEST_FILE}.tmp"));
    fs::write(&tmp, format!("{shard_count}\n")).map_err(io_error)?;
    fs::rename(&tmp, &manifest).map_err(io_error)
}

/// Infers the shard count from `shard-<id>.aof` and `shard-<id>.snap` files.
/// Returns `None` for a directory with no shard files.
fn shard_count_from_files(data_dir: &Path) -> Result<Option<usize>, String> {
    let entries =
        fs::read_dir(data_dir).map_err(|e| format!("cannot read {}: {e}", data_dir.display()))?;
    let highest = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter_map(|name| {
            let id = name
                .strip_prefix("shard-")?
                .strip_suffix(".aof")
                .or_else(|| name.strip_prefix("shard-")?.strip_suffix(".snap"))?;
            id.parse::<usize>().ok()
        })
        .max();
    Ok(highest.map(|id| id + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_the_count_and_rejects_a_different_one() {
        let dir = tempfile::tempdir().unwrap();
        check_shard_count(dir.path(), 4).unwrap();
        check_shard_count(dir.path(), 4).unwrap();

        let err = check_shard_count(dir.path(), 2).unwrap_err();
        assert!(err.contains("4 shards"), "{err}");
    }

    #[test]
    fn infers_the_count_from_older_data() {
        let dir = tempfile::tempdir().unwrap();
        for file in ["shard-0.aof", "shard-1.snap", "shard-2.aof", "other.txt"] {
            fs::write(dir.path().join(file), b"").unwrap();
        }
        let err = check_shard_count(dir.path(), 8).unwrap_err();
        assert!(err.contains("3 shards"), "{err}");

        check_shard_count(dir.path(), 3).unwrap();
        assert_eq!(
            fs::read_to_string(dir.path().join(MANIFEST_FILE)).unwrap(),
            "3\n"
        );
    }
}
