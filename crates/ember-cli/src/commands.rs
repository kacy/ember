//! Static command metadata for autocomplete and inline help.
//!
//! This is a thin client — we never validate commands ourselves.
//! This table only drives tab-completion and the `help` display.

use std::collections::BTreeMap;

/// Metadata for a single ember/redis command.
pub struct CommandInfo {
    /// Uppercase command name (e.g. "SET").
    pub name: &'static str,
    /// Argument synopsis (e.g. "key value [EX seconds] [NX|XX]").
    pub args: &'static str,
    /// Functional group for help display.
    pub group: &'static str,
    /// One-line summary.
    pub summary: &'static str,
}

/// All known commands, sorted alphabetically.
///
/// Keeping this as a flat array makes autocomplete fast (linear scan
/// over ~80 entries is plenty fast for interactive use) and avoids
/// any initialization cost.
pub static COMMANDS: &[CommandInfo] = &[
    // --- connection ---
    CommandInfo {
        name: "AUTH",
        args: "[username] password",
        group: "connection",
        summary: "authenticate to the server",
    },
    CommandInfo {
        name: "ECHO",
        args: "message",
        group: "connection",
        summary: "echo the given string",
    },
    CommandInfo {
        name: "PING",
        args: "[message]",
        group: "connection",
        summary: "ping the server",
    },
    CommandInfo {
        name: "QUIT",
        args: "",
        group: "connection",
        summary: "close the connection",
    },
    // --- string ---
    CommandInfo {
        name: "APPEND",
        args: "key value",
        group: "string",
        summary: "append a value to a key",
    },
    CommandInfo {
        name: "DECR",
        args: "key",
        group: "string",
        summary: "decrement the integer value of a key by one",
    },
    CommandInfo {
        name: "DECRBY",
        args: "key decrement",
        group: "string",
        summary: "decrement the integer value of a key by the given number",
    },
    CommandInfo {
        name: "GET",
        args: "key",
        group: "string",
        summary: "get the value of a key",
    },
    CommandInfo {
        name: "INCR",
        args: "key",
        group: "string",
        summary: "increment the integer value of a key by one",
    },
    CommandInfo {
        name: "INCRBY",
        args: "key increment",
        group: "string",
        summary: "increment the integer value of a key by the given amount",
    },
    CommandInfo {
        name: "INCRBYFLOAT",
        args: "key increment",
        group: "string",
        summary: "increment the float value of a key by the given amount",
    },
    CommandInfo {
        name: "MGET",
        args: "key [key ...]",
        group: "string",
        summary: "get the values of all the given keys",
    },
    CommandInfo {
        name: "MSET",
        args: "key value [key value ...]",
        group: "string",
        summary: "set multiple keys to multiple values",
    },
    CommandInfo {
        name: "SET",
        args: "key value [EX seconds | PX milliseconds] [NX|XX]",
        group: "string",
        summary: "set the string value of a key",
    },
    CommandInfo {
        name: "STRLEN",
        args: "key",
        group: "string",
        summary: "get the length of the value stored at a key",
    },
    // --- generic ---
    CommandInfo {
        name: "DEL",
        args: "key [key ...]",
        group: "generic",
        summary: "delete one or more keys",
    },
    CommandInfo {
        name: "EXISTS",
        args: "key [key ...]",
        group: "generic",
        summary: "determine if a key exists",
    },
    CommandInfo {
        name: "EXPIRE",
        args: "key seconds",
        group: "generic",
        summary: "set a key's time to live in seconds",
    },
    CommandInfo {
        name: "KEYS",
        args: "pattern",
        group: "generic",
        summary: "find all keys matching the given pattern",
    },
    CommandInfo {
        name: "PERSIST",
        args: "key",
        group: "generic",
        summary: "remove the expiration from a key",
    },
    CommandInfo {
        name: "PEXPIRE",
        args: "key milliseconds",
        group: "generic",
        summary: "set a key's time to live in milliseconds",
    },
    CommandInfo {
        name: "PTTL",
        args: "key",
        group: "generic",
        summary: "get the time to live for a key in milliseconds",
    },
    CommandInfo {
        name: "RENAME",
        args: "key newkey",
        group: "generic",
        summary: "rename a key",
    },
    CommandInfo {
        name: "SCAN",
        args: "cursor [MATCH pattern] [COUNT count]",
        group: "generic",
        summary: "incrementally iterate the keys space",
    },
    CommandInfo {
        name: "TTL",
        args: "key",
        group: "generic",
        summary: "get the time to live for a key in seconds",
    },
    CommandInfo {
        name: "TYPE",
        args: "key",
        group: "generic",
        summary: "determine the type stored at key",
    },
    CommandInfo {
        name: "UNLINK",
        args: "key [key ...]",
        group: "generic",
        summary: "delete keys asynchronously",
    },
    // --- list ---
    CommandInfo {
        name: "LLEN",
        args: "key",
        group: "list",
        summary: "get the length of a list",
    },
    CommandInfo {
        name: "LPOP",
        args: "key",
        group: "list",
        summary: "remove and return the first element of a list",
    },
    CommandInfo {
        name: "LPUSH",
        args: "key value [value ...]",
        group: "list",
        summary: "prepend one or more values to a list",
    },
    CommandInfo {
        name: "LRANGE",
        args: "key start stop",
        group: "list",
        summary: "get a range of elements from a list",
    },
    CommandInfo {
        name: "RPOP",
        args: "key",
        group: "list",
        summary: "remove and return the last element of a list",
    },
    CommandInfo {
        name: "RPUSH",
        args: "key value [value ...]",
        group: "list",
        summary: "append one or more values to a list",
    },
    // --- hash ---
    CommandInfo {
        name: "HDEL",
        args: "key field [field ...]",
        group: "hash",
        summary: "delete one or more hash fields",
    },
    CommandInfo {
        name: "HEXISTS",
        args: "key field",
        group: "hash",
        summary: "determine if a hash field exists",
    },
    CommandInfo {
        name: "HGET",
        args: "key field",
        group: "hash",
        summary: "get the value of a hash field",
    },
    CommandInfo {
        name: "HGETALL",
        args: "key",
        group: "hash",
        summary: "get all fields and values in a hash",
    },
    CommandInfo {
        name: "HINCRBY",
        args: "key field increment",
        group: "hash",
        summary: "increment the integer value of a hash field",
    },
    CommandInfo {
        name: "HKEYS",
        args: "key",
        group: "hash",
        summary: "get all field names in a hash",
    },
    CommandInfo {
        name: "HLEN",
        args: "key",
        group: "hash",
        summary: "get the number of fields in a hash",
    },
    CommandInfo {
        name: "HMGET",
        args: "key field [field ...]",
        group: "hash",
        summary: "get the values of multiple hash fields",
    },
    CommandInfo {
        name: "HSET",
        args: "key field value [field value ...]",
        group: "hash",
        summary: "set the value of a hash field",
    },
    CommandInfo {
        name: "HVALS",
        args: "key",
        group: "hash",
        summary: "get all values in a hash",
    },
    // --- set ---
    CommandInfo {
        name: "SADD",
        args: "key member [member ...]",
        group: "set",
        summary: "add one or more members to a set",
    },
    CommandInfo {
        name: "SCARD",
        args: "key",
        group: "set",
        summary: "get the number of members in a set",
    },
    CommandInfo {
        name: "SISMEMBER",
        args: "key member",
        group: "set",
        summary: "determine if a value is a member of a set",
    },
    CommandInfo {
        name: "SMEMBERS",
        args: "key",
        group: "set",
        summary: "get all members in a set",
    },
    CommandInfo {
        name: "SREM",
        args: "key member [member ...]",
        group: "set",
        summary: "remove one or more members from a set",
    },
    // --- sorted set ---
    CommandInfo {
        name: "ZADD",
        args: "key [NX|XX] [GT|LT] [CH] score member [score member ...]",
        group: "sorted_set",
        summary: "add one or more members to a sorted set",
    },
    CommandInfo {
        name: "ZCARD",
        args: "key",
        group: "sorted_set",
        summary: "get the number of members in a sorted set",
    },
    CommandInfo {
        name: "ZRANGE",
        args: "key start stop [WITHSCORES]",
        group: "sorted_set",
        summary: "return a range of members by index",
    },
    CommandInfo {
        name: "ZRANK",
        args: "key member",
        group: "sorted_set",
        summary: "determine the index of a member in a sorted set",
    },
    CommandInfo {
        name: "ZREM",
        args: "key member [member ...]",
        group: "sorted_set",
        summary: "remove one or more members from a sorted set",
    },
    CommandInfo {
        name: "ZSCORE",
        args: "key member",
        group: "sorted_set",
        summary: "get the score of a member in a sorted set",
    },
    // --- server ---
    CommandInfo {
        name: "BGREWRITEAOF",
        args: "",
        group: "server",
        summary: "asynchronously rewrite the append-only file",
    },
    CommandInfo {
        name: "BGSAVE",
        args: "",
        group: "server",
        summary: "asynchronously save the dataset to disk",
    },
    CommandInfo {
        name: "DBSIZE",
        args: "",
        group: "server",
        summary: "return the number of keys in the database",
    },
    CommandInfo {
        name: "FLUSHDB",
        args: "[ASYNC]",
        group: "server",
        summary: "remove all keys from the current database",
    },
    CommandInfo {
        name: "INFO",
        args: "[section]",
        group: "server",
        summary: "get information and statistics about the server",
    },
    CommandInfo {
        name: "SLOWLOG",
        args: "GET [count] | LEN | RESET",
        group: "server",
        summary: "manage the slow log",
    },
    // --- pubsub ---
    CommandInfo {
        name: "PSUBSCRIBE",
        args: "pattern [pattern ...]",
        group: "pubsub",
        summary: "listen for messages published to channels matching patterns",
    },
    CommandInfo {
        name: "PUBLISH",
        args: "channel message",
        group: "pubsub",
        summary: "post a message to a channel",
    },
    CommandInfo {
        name: "PUBSUB",
        args: "CHANNELS [pattern] | NUMSUB [channel ...] | NUMPAT",
        group: "pubsub",
        summary: "inspect the pub/sub subsystem",
    },
    CommandInfo {
        name: "PUNSUBSCRIBE",
        args: "[pattern [pattern ...]]",
        group: "pubsub",
        summary: "stop listening for messages matching patterns",
    },
    CommandInfo {
        name: "SUBSCRIBE",
        args: "channel [channel ...]",
        group: "pubsub",
        summary: "listen for messages published to channels",
    },
    CommandInfo {
        name: "UNSUBSCRIBE",
        args: "[channel [channel ...]]",
        group: "pubsub",
        summary: "stop listening for messages on channels",
    },
    // --- cluster ---
    CommandInfo {
        name: "ASKING",
        args: "",
        group: "cluster",
        summary: "signal that the next command is for a migrating slot",
    },
    CommandInfo {
        name: "CLUSTER",
        args: "subcommand [args...]",
        group: "cluster",
        summary: "cluster management commands",
    },
    CommandInfo {
        name: "MIGRATE",
        args: "host port key db timeout [COPY] [REPLACE]",
        group: "cluster",
        summary: "atomically transfer a key to another server",
    },
];

/// Look up a command by name (case-insensitive).
pub fn find_command(name: &str) -> Option<&'static CommandInfo> {
    let upper = name.to_uppercase();
    COMMANDS.iter().find(|c| c.name == upper)
}

/// Returns all known command names for autocomplete.
pub fn command_names() -> Vec<&'static str> {
    COMMANDS.iter().map(|c| c.name).collect()
}

/// Groups commands by their functional group for help display.
pub fn commands_by_group() -> BTreeMap<&'static str, Vec<&'static CommandInfo>> {
    let mut groups = BTreeMap::new();
    for cmd in COMMANDS {
        groups.entry(cmd.group).or_insert_with(Vec::new).push(cmd);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_command_case_insensitive() {
        assert!(find_command("set").is_some());
        assert!(find_command("SET").is_some());
        assert!(find_command("Set").is_some());
        assert_eq!(find_command("set").map(|c| c.name), Some("SET"));
    }

    #[test]
    fn find_unknown_command() {
        assert!(find_command("NOTACOMMAND").is_none());
    }

    #[test]
    fn command_names_not_empty() {
        let names = command_names();
        assert!(!names.is_empty());
        assert!(names.contains(&"GET"));
        assert!(names.contains(&"SET"));
    }

    #[test]
    fn groups_cover_all_commands() {
        let groups = commands_by_group();
        let total: usize = groups.values().map(|v| v.len()).sum();
        assert_eq!(total, COMMANDS.len());
    }

    #[test]
    fn commands_sorted_within_groups() {
        // each group's commands should be alphabetically sorted
        let groups = commands_by_group();
        for (group_name, cmds) in &groups {
            for i in 1..cmds.len() {
                assert!(
                    cmds[i - 1].name <= cmds[i].name,
                    "commands in group '{group_name}' not sorted: {} > {}",
                    cmds[i - 1].name,
                    cmds[i].name,
                );
            }
        }
    }
}
