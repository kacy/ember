//! Per-user access control (ACL) for ember.
//!
//! Provides fine-grained command and key permissions on a per-user basis.
//! When no ACL users are configured, the system falls back to legacy
//! `requirepass` behavior with zero overhead — permission checks are
//! branch-predicted away behind two boolean fast paths.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use ember_protocol::types::Frame;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// category bitmask constants
// ---------------------------------------------------------------------------

pub const CAT_READ: u64 = 1 << 0;
pub const CAT_WRITE: u64 = 1 << 1;
pub const CAT_STRING: u64 = 1 << 2;
pub const CAT_LIST: u64 = 1 << 3;
pub const CAT_SET: u64 = 1 << 4;
pub const CAT_SORTEDSET: u64 = 1 << 5;
pub const CAT_HASH: u64 = 1 << 6;
pub const CAT_KEYSPACE: u64 = 1 << 7;
pub const CAT_SERVER: u64 = 1 << 8;
pub const CAT_CONNECTION: u64 = 1 << 9;
pub const CAT_TRANSACTION: u64 = 1 << 10;
pub const CAT_PUBSUB: u64 = 1 << 11;
pub const CAT_FAST: u64 = 1 << 12;
pub const CAT_SLOW: u64 = 1 << 13;
pub const CAT_ADMIN: u64 = 1 << 14;
pub const CAT_DANGEROUS: u64 = 1 << 15;
pub const CAT_CLUSTER: u64 = 1 << 16;

/// All category flags OR'd together.
const ALL_CATEGORIES: u64 = CAT_READ
    | CAT_WRITE
    | CAT_STRING
    | CAT_LIST
    | CAT_SET
    | CAT_SORTEDSET
    | CAT_HASH
    | CAT_KEYSPACE
    | CAT_SERVER
    | CAT_CONNECTION
    | CAT_TRANSACTION
    | CAT_PUBSUB
    | CAT_FAST
    | CAT_SLOW
    | CAT_ADMIN
    | CAT_DANGEROUS
    | CAT_CLUSTER;

/// All known category names, sorted alphabetically.
const CATEGORY_NAMES: &[(&str, u64)] = &[
    ("admin", CAT_ADMIN),
    ("cluster", CAT_CLUSTER),
    ("connection", CAT_CONNECTION),
    ("dangerous", CAT_DANGEROUS),
    ("fast", CAT_FAST),
    ("hash", CAT_HASH),
    ("keyspace", CAT_KEYSPACE),
    ("list", CAT_LIST),
    ("pubsub", CAT_PUBSUB),
    ("read", CAT_READ),
    ("server", CAT_SERVER),
    ("set", CAT_SET),
    ("slow", CAT_SLOW),
    ("sortedset", CAT_SORTEDSET),
    ("string", CAT_STRING),
    ("transaction", CAT_TRANSACTION),
    ("write", CAT_WRITE),
];

/// Resolves a category name (case-insensitive) to its bitmask.
pub fn category_from_name(name: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    CATEGORY_NAMES
        .iter()
        .find(|(n, _)| *n == lower)
        .map(|(_, v)| *v)
}

/// Returns the display name for a single-bit category flag.
#[allow(dead_code)]
pub fn category_name(flag: u64) -> &'static str {
    for &(name, val) in CATEGORY_NAMES {
        if val == flag {
            return name;
        }
    }
    "unknown"
}

/// Returns all known category names.
pub fn all_category_names() -> Vec<&'static str> {
    CATEGORY_NAMES.iter().map(|(name, _)| *name).collect()
}

// ---------------------------------------------------------------------------
// AclUser
// ---------------------------------------------------------------------------

/// A single ACL user with command and key permissions.
#[derive(Debug, Clone)]
pub struct AclUser {
    /// Whether this user can authenticate.
    pub enabled: bool,
    /// SHA-256 hashed passwords. Multiple passwords supported for rotation.
    pub password_hashes: Vec<[u8; 32]>,
    /// When true, authentication succeeds without a password.
    pub nopass: bool,
    /// Bitmask of allowed categories.
    pub allowed_categories: u64,
    /// Bitmask of denied categories (applied after allowed).
    pub denied_categories: u64,
    /// Explicitly allowed commands (overrides category deny).
    pub allowed_commands: HashSet<String>,
    /// Explicitly denied commands (overrides category allow).
    pub denied_commands: HashSet<String>,
    /// Key glob patterns. Empty means no keys allowed (unless `allkeys`).
    pub key_patterns: Vec<String>,
    /// Fast path: skip key pattern matching.
    pub allkeys: bool,
    /// Fast path: skip command/category checks.
    pub allcommands: bool,
}

impl AclUser {
    /// Creates a new disabled user with no permissions.
    fn new() -> Self {
        Self {
            enabled: false,
            password_hashes: Vec::new(),
            nopass: false,
            allowed_categories: 0,
            denied_categories: 0,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            key_patterns: Vec::new(),
            allkeys: false,
            allcommands: false,
        }
    }

    /// Creates the default "unrestricted" user — full access, no password required.
    pub fn unrestricted() -> Self {
        Self {
            enabled: true,
            password_hashes: Vec::new(),
            nopass: true,
            allowed_categories: ALL_CATEGORIES,
            denied_categories: 0,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            key_patterns: Vec::new(),
            allkeys: true,
            allcommands: true,
        }
    }

    /// Verifies a password against this user's stored hashes using
    /// constant-time comparison to prevent timing attacks.
    pub fn verify_password(&self, password: &str) -> bool {
        if self.nopass {
            return true;
        }
        let hash = sha256_hash(password);
        self.password_hashes
            .iter()
            .any(|stored| bool::from(hash.ct_eq(stored)))
    }
}

/// Computes SHA-256 of a password string.
fn sha256_hash(password: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

// ---------------------------------------------------------------------------
// rule parsing
// ---------------------------------------------------------------------------

/// Applies a single SETUSER rule token to a user.
///
/// Rules follow Redis ACL syntax:
/// - `on`/`off` — enable/disable the user
/// - `>password` — add a hashed password
/// - `<password` — remove a hashed password
/// - `nopass` — allow auth without a password
/// - `resetpass` — clear all passwords and disable nopass
/// - `+cmd` — allow a specific command
/// - `-cmd` — deny a specific command
/// - `+@category` — allow all commands in a category
/// - `-@category` — deny all commands in a category
/// - `allcommands` / `nocommands` — allow/deny everything
/// - `~pattern` — add a key pattern
/// - `allkeys` — allow access to all keys
/// - `resetkeys` — clear all key patterns
/// - `reset` — reset the user to defaults
pub fn apply_rule(user: &mut AclUser, rule: &str) -> Result<(), String> {
    match rule {
        "on" => {
            user.enabled = true;
        }
        "off" => {
            user.enabled = false;
        }
        "nopass" => {
            user.nopass = true;
            user.password_hashes.clear();
        }
        "resetpass" => {
            user.nopass = false;
            user.password_hashes.clear();
        }
        "allcommands" => {
            user.allcommands = true;
            user.allowed_categories = ALL_CATEGORIES;
            user.denied_categories = 0;
            user.allowed_commands.clear();
            user.denied_commands.clear();
        }
        "nocommands" => {
            user.allcommands = false;
            user.allowed_categories = 0;
            user.denied_categories = ALL_CATEGORIES;
            user.allowed_commands.clear();
            user.denied_commands.clear();
        }
        "allkeys" => {
            user.allkeys = true;
            user.key_patterns.clear();
        }
        "resetkeys" => {
            user.allkeys = false;
            user.key_patterns.clear();
        }
        "reset" => {
            *user = AclUser::new();
        }
        _ if rule.starts_with('>') => {
            let password = &rule[1..];
            if password.is_empty() {
                return Err("ERR empty password is not allowed".into());
            }
            let hash = sha256_hash(password);
            if !user.password_hashes.iter().any(|h| h == &hash) {
                user.password_hashes.push(hash);
            }
            // adding a password implicitly clears nopass
            user.nopass = false;
        }
        _ if rule.starts_with('<') => {
            let password = &rule[1..];
            let hash = sha256_hash(password);
            user.password_hashes.retain(|h| h != &hash);
        }
        _ if rule.starts_with("+@") => {
            let cat_name = &rule[2..];
            if cat_name == "all" {
                user.allowed_categories = ALL_CATEGORIES;
                user.denied_categories = 0;
                user.allcommands = true;
            } else {
                let flag = category_from_name(cat_name)
                    .ok_or_else(|| format!("ERR unknown ACL category '{cat_name}'"))?;
                user.allowed_categories |= flag;
                user.denied_categories &= !flag;
            }
        }
        _ if rule.starts_with("-@") => {
            let cat_name = &rule[2..];
            if cat_name == "all" {
                user.denied_categories = ALL_CATEGORIES;
                user.allowed_categories = 0;
                user.allcommands = false;
            } else {
                let flag = category_from_name(cat_name)
                    .ok_or_else(|| format!("ERR unknown ACL category '{cat_name}'"))?;
                user.denied_categories |= flag;
                user.allowed_categories &= !flag;
            }
        }
        _ if rule.starts_with('+') => {
            let cmd = rule[1..].to_ascii_lowercase();
            if cmd.is_empty() {
                return Err("ERR empty command name".into());
            }
            user.denied_commands.remove(&cmd);
            user.allowed_commands.insert(cmd);
        }
        _ if rule.starts_with('-') => {
            let cmd = rule[1..].to_ascii_lowercase();
            if cmd.is_empty() {
                return Err("ERR empty command name".into());
            }
            user.allowed_commands.remove(&cmd);
            user.denied_commands.insert(cmd);
        }
        _ if rule.starts_with('~') => {
            let pattern = &rule[1..];
            if pattern.is_empty() {
                return Err("ERR empty key pattern".into());
            }
            user.allkeys = false;
            user.key_patterns.push(pattern.to_string());
        }
        _ => {
            return Err(format!("ERR unrecognized ACL rule '{rule}'"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// permission check
// ---------------------------------------------------------------------------

/// Checks whether a user has permission to execute a command.
///
/// Returns `None` if allowed, or `Some(Frame::Error(...))` if denied.
/// The caller should check `allcommands && allkeys` before calling this
/// function as a fast path — this function handles the detailed checks.
pub fn check_permission(
    user: &AclUser,
    cmd: &ember_protocol::Command,
    cmd_name: &str,
    categories: u64,
) -> Option<Frame> {
    // check command permission
    if !user.allcommands {
        let lower_name = cmd_name.to_ascii_lowercase();

        // explicit deny takes priority
        if user.denied_commands.contains(&lower_name) {
            return Some(Frame::Error(format!(
                "NOPERM this user has no permissions to run the '{lower_name}' command"
            )));
        }

        // explicit allow overrides category
        if !user.allowed_commands.contains(&lower_name) {
            // check categories: denied bits take priority
            if categories & user.denied_categories != 0 {
                return Some(Frame::Error(format!(
                    "NOPERM this user has no permissions to run the '{lower_name}' command"
                )));
            }
            if categories != 0 && categories & user.allowed_categories == 0 {
                return Some(Frame::Error(format!(
                    "NOPERM this user has no permissions to run the '{lower_name}' command"
                )));
            }
        }
    }

    // check key permission
    if !user.allkeys {
        let keys = extract_keys(cmd);
        for key in keys {
            if !user.key_patterns.iter().any(|pat| glob_match(pat, key)) {
                return Some(Frame::Error(format!(
                    "NOPERM this user has no permissions to access the '{key}' key"
                )));
            }
        }
    }

    None
}

/// Extracts all keys referenced by a command.
///
/// Returns references to the key strings inside the command. Multi-key
/// commands return all keys; keyless commands return an empty vec.
fn extract_keys(cmd: &ember_protocol::Command) -> Vec<&str> {
    use ember_protocol::Command;
    match cmd {
        // single-key commands
        Command::Get { key }
        | Command::Set { key, .. }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. }
        | Command::IncrByFloat { key, .. }
        | Command::Append { key, .. }
        | Command::Strlen { key }
        | Command::Expire { key, .. }
        | Command::Pexpire { key, .. }
        | Command::Ttl { key }
        | Command::Pttl { key }
        | Command::Persist { key }
        | Command::Type { key }
        | Command::ObjectEncoding { key }
        | Command::ObjectRefcount { key }
        | Command::LPush { key, .. }
        | Command::RPush { key, .. }
        | Command::LPop { key }
        | Command::RPop { key }
        | Command::LRange { key, .. }
        | Command::LLen { key }
        | Command::ZAdd { key, .. }
        | Command::ZRem { key, .. }
        | Command::ZScore { key, .. }
        | Command::ZRank { key, .. }
        | Command::ZRange { key, .. }
        | Command::ZCard { key }
        | Command::HSet { key, .. }
        | Command::HGet { key, .. }
        | Command::HGetAll { key }
        | Command::HDel { key, .. }
        | Command::HExists { key, .. }
        | Command::HLen { key }
        | Command::HIncrBy { key, .. }
        | Command::HKeys { key }
        | Command::HVals { key }
        | Command::HMGet { key, .. }
        | Command::SAdd { key, .. }
        | Command::SRem { key, .. }
        | Command::SMembers { key }
        | Command::SIsMember { key, .. }
        | Command::SCard { key }
        | Command::SScan { key, .. }
        | Command::HScan { key, .. }
        | Command::ZScan { key, .. }
        | Command::Restore { key, .. }
        | Command::VAdd { key, .. }
        | Command::VAddBatch { key, .. }
        | Command::VSim { key, .. }
        | Command::VRem { key, .. }
        | Command::VGet { key, .. }
        | Command::VCard { key }
        | Command::VDim { key }
        | Command::VInfo { key }
        | Command::ProtoSet { key, .. }
        | Command::ProtoGet { key }
        | Command::ProtoType { key }
        | Command::ProtoGetField { key, .. }
        | Command::ProtoSetField { key, .. }
        | Command::ProtoDelField { key, .. } => vec![key.as_str()],

        // two-key commands
        Command::Rename { key, newkey } => vec![key.as_str(), newkey.as_str()],
        Command::Copy {
            source,
            destination,
            ..
        } => vec![source.as_str(), destination.as_str()],

        // multi-key commands
        Command::Del { keys }
        | Command::Unlink { keys }
        | Command::Exists { keys }
        | Command::MGet { keys }
        | Command::BLPop { keys, .. }
        | Command::BRPop { keys, .. }
        | Command::Watch { keys } => keys.iter().map(String::as_str).collect(),

        Command::MSet { pairs } => pairs.iter().map(|(k, _)| k.as_str()).collect(),

        // keyless commands
        _ => Vec::new(),
    }
}

/// Simple glob pattern matcher supporting `*` and `?`.
///
/// Case-sensitive, matching Redis ACL key pattern behavior.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pat = pattern.as_bytes();
    let txt = text.as_bytes();
    glob_match_bytes(pat, txt)
}

fn glob_match_bytes(pat: &[u8], txt: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < txt.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == txt[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
}

// ---------------------------------------------------------------------------
// ACL state
// ---------------------------------------------------------------------------

/// Shared ACL state. Protected by RwLock — writes happen only on
/// ACL SETUSER/DELUSER (admin operations), reads on AUTH.
#[derive(Debug)]
pub struct AclState {
    users: HashMap<String, AclUser>,
}

impl AclState {
    /// Creates a new ACL state with just the default user.
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".into(), AclUser::unrestricted());
        Self { users }
    }

    /// Returns a reference to a user by name.
    pub fn get_user(&self, name: &str) -> Option<&AclUser> {
        self.users.get(name)
    }

    /// Returns a clone of a user wrapped in Arc for caching on connections.
    #[allow(dead_code)]
    pub fn get_user_arc(&self, name: &str) -> Option<Arc<AclUser>> {
        self.users.get(name).cloned().map(Arc::new)
    }

    /// Creates or updates a user by applying rules.
    pub fn set_user(&mut self, name: &str, rules: &[String]) -> Result<(), String> {
        let user = self
            .users
            .entry(name.to_string())
            .or_insert_with(AclUser::new);
        for rule in rules {
            apply_rule(user, rule)?;
        }
        Ok(())
    }

    /// Deletes users by name. Returns the number deleted.
    /// The "default" user cannot be deleted.
    pub fn del_users(&mut self, names: &[String]) -> Result<usize, String> {
        let mut count = 0;
        for name in names {
            if name == "default" {
                return Err("ERR The 'default' user cannot be removed".into());
            }
            if self.users.remove(name).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Returns all usernames.
    pub fn usernames(&self) -> Vec<String> {
        let mut names: Vec<_> = self.users.keys().cloned().collect();
        names.sort();
        names
    }

    /// Formats all users for ACL LIST output.
    pub fn list(&self) -> Vec<String> {
        let mut result = Vec::new();
        let mut names: Vec<_> = self.users.keys().collect();
        names.sort();
        for name in names {
            if let Some(user) = self.users.get(name) {
                result.push(format_user_line(name, user));
            }
        }
        result
    }

    /// Formats a single user for ACL GETUSER output.
    pub fn get_user_detail(&self, name: &str) -> Option<Frame> {
        self.users.get(name).map(format_user_detail)
    }
}

// ---------------------------------------------------------------------------
// formatting
// ---------------------------------------------------------------------------

/// Formats a user as a single-line ACL LIST entry.
fn format_user_line(name: &str, user: &AclUser) -> String {
    let mut parts = Vec::new();

    parts.push(format!("user {name}"));

    if user.enabled {
        parts.push("on".into());
    } else {
        parts.push("off".into());
    }

    if user.nopass {
        parts.push("nopass".into());
    } else if user.password_hashes.is_empty() {
        // no passwords and not nopass = effectively locked out
        parts.push("resetpass".into());
    } else {
        for hash in &user.password_hashes {
            parts.push(format!("#{}", hex_encode(hash)));
        }
    }

    if user.allcommands {
        parts.push("+@all".into());
    } else {
        for &(cat_name, flag) in CATEGORY_NAMES {
            if user.allowed_categories & flag != 0 && user.denied_categories & flag == 0 {
                parts.push(format!("+@{cat_name}"));
            }
        }
        for &(cat_name, flag) in CATEGORY_NAMES {
            if user.denied_categories & flag != 0 {
                parts.push(format!("-@{cat_name}"));
            }
        }
        for cmd in sorted_set(&user.allowed_commands) {
            parts.push(format!("+{cmd}"));
        }
        for cmd in sorted_set(&user.denied_commands) {
            parts.push(format!("-{cmd}"));
        }
    }

    if user.allkeys {
        parts.push("~*".into());
    } else if user.key_patterns.is_empty() {
        // no key patterns = no key access
    } else {
        for pat in &user.key_patterns {
            parts.push(format!("~{pat}"));
        }
    }

    parts.join(" ")
}

/// Formats a user as a detail array for ACL GETUSER.
fn format_user_detail(user: &AclUser) -> Frame {
    let mut fields = Vec::new();

    // flags
    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"flags")));
    let mut flags = Vec::new();
    if user.enabled {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"on")));
    } else {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"off")));
    }
    if user.allkeys {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"allkeys")));
    }
    if user.allcommands {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"allcommands")));
    }
    if user.nopass {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"nopass")));
    }
    fields.push(Frame::Array(flags));

    // passwords (hex-encoded hashes)
    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"passwords")));
    let passwords: Vec<Frame> = user
        .password_hashes
        .iter()
        .map(|h| Frame::Bulk(bytes::Bytes::from(hex_encode(h))))
        .collect();
    fields.push(Frame::Array(passwords));

    // commands
    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"commands")));
    let cmd_str = if user.allcommands {
        "+@all".to_string()
    } else {
        let mut parts = Vec::new();
        for &(cat_name, flag) in CATEGORY_NAMES {
            if user.allowed_categories & flag != 0 && user.denied_categories & flag == 0 {
                parts.push(format!("+@{cat_name}"));
            }
        }
        for &(cat_name, flag) in CATEGORY_NAMES {
            if user.denied_categories & flag != 0 {
                parts.push(format!("-@{cat_name}"));
            }
        }
        for cmd in sorted_set(&user.allowed_commands) {
            parts.push(format!("+{cmd}"));
        }
        for cmd in sorted_set(&user.denied_commands) {
            parts.push(format!("-{cmd}"));
        }
        if parts.is_empty() {
            "-@all".to_string()
        } else {
            parts.join(" ")
        }
    };
    fields.push(Frame::Bulk(bytes::Bytes::from(cmd_str)));

    // keys
    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"keys")));
    let keys_str = if user.allkeys {
        "~*".to_string()
    } else if user.key_patterns.is_empty() {
        String::new()
    } else {
        user.key_patterns
            .iter()
            .map(|p| format!("~{p}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    fields.push(Frame::Bulk(bytes::Bytes::from(keys_str)));

    Frame::Array(fields)
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn sorted_set(set: &HashSet<String>) -> Vec<&str> {
    let mut v: Vec<&str> = set.iter().map(String::as_str).collect();
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// convenience type alias
// ---------------------------------------------------------------------------

/// The shared ACL state, wrapped for concurrent access.
///
/// `None` = legacy mode (no ACL configured, zero overhead).
/// `Some(...)` = ACL is active, read lock on AUTH, write lock on SETUSER/DELUSER.
pub type SharedAclState = Option<Arc<RwLock<AclState>>>;

// ---------------------------------------------------------------------------
// ACL CAT handler
// ---------------------------------------------------------------------------

/// Handles the ACL CAT command.
///
/// Without arguments: returns all category names.
/// With a category name: returns all commands in that category.
pub fn handle_acl_cat(category: Option<&str>) -> Frame {
    match category {
        None => {
            // list all category names
            let names = all_category_names();
            Frame::Array(
                names
                    .into_iter()
                    .map(|n| Frame::Bulk(bytes::Bytes::from(n)))
                    .collect(),
            )
        }
        Some(cat_name) => {
            let flag = match category_from_name(cat_name) {
                Some(f) => f,
                None => {
                    return Frame::Error(format!("ERR unknown ACL category '{cat_name}'"));
                }
            };
            // collect all commands that have this category flag
            let commands = commands_in_category(flag);
            Frame::Array(
                commands
                    .into_iter()
                    .map(|n| Frame::Bulk(bytes::Bytes::from(n)))
                    .collect(),
            )
        }
    }
}

/// Returns the names of all commands belonging to a given category flag.
fn commands_in_category(flag: u64) -> Vec<&'static str> {
    use ember_protocol::Command;

    // representative command instances to iterate over
    let samples: Vec<Command> = vec![
        Command::Ping(None),
        Command::Echo(bytes::Bytes::new()),
        Command::Get { key: String::new() },
        Command::Set {
            key: String::new(),
            value: bytes::Bytes::new(),
            expire: None,
            nx: false,
            xx: false,
        },
        Command::Incr { key: String::new() },
        Command::Decr { key: String::new() },
        Command::IncrBy {
            key: String::new(),
            delta: 0,
        },
        Command::DecrBy {
            key: String::new(),
            delta: 0,
        },
        Command::IncrByFloat {
            key: String::new(),
            delta: 0.0,
        },
        Command::Append {
            key: String::new(),
            value: bytes::Bytes::new(),
        },
        Command::Strlen { key: String::new() },
        Command::Del { keys: vec![] },
        Command::Unlink { keys: vec![] },
        Command::Exists { keys: vec![] },
        Command::MGet { keys: vec![] },
        Command::MSet { pairs: vec![] },
        Command::Keys {
            pattern: String::new(),
        },
        Command::Rename {
            key: String::new(),
            newkey: String::new(),
        },
        Command::Copy {
            source: String::new(),
            destination: String::new(),
            replace: false,
        },
        Command::Expire {
            key: String::new(),
            seconds: 0,
        },
        Command::Pexpire {
            key: String::new(),
            milliseconds: 0,
        },
        Command::Ttl { key: String::new() },
        Command::Pttl { key: String::new() },
        Command::Persist { key: String::new() },
        Command::Type { key: String::new() },
        Command::ObjectEncoding { key: String::new() },
        Command::ObjectRefcount { key: String::new() },
        Command::Scan {
            cursor: 0,
            pattern: None,
            count: None,
        },
        Command::DbSize,
        Command::Info { section: None },
        Command::Time,
        Command::LastSave,
        Command::Role,
        Command::BgSave,
        Command::BgRewriteAof,
        Command::FlushDb { async_mode: false },
        Command::ConfigGet {
            pattern: String::new(),
        },
        Command::ConfigSet {
            param: String::new(),
            value: String::new(),
        },
        Command::ConfigRewrite,
        Command::Multi,
        Command::Exec,
        Command::Discard,
        Command::Watch { keys: vec![] },
        Command::Unwatch,
        Command::LPush {
            key: String::new(),
            values: vec![],
        },
        Command::RPush {
            key: String::new(),
            values: vec![],
        },
        Command::LPop { key: String::new() },
        Command::RPop { key: String::new() },
        Command::LRange {
            key: String::new(),
            start: 0,
            stop: 0,
        },
        Command::LLen { key: String::new() },
        Command::BLPop {
            keys: vec![],
            timeout_secs: 0.0,
        },
        Command::BRPop {
            keys: vec![],
            timeout_secs: 0.0,
        },
        Command::ZAdd {
            key: String::new(),
            flags: ember_protocol::ZAddFlags::default(),
            members: vec![],
        },
        Command::ZRem {
            key: String::new(),
            members: vec![],
        },
        Command::ZScore {
            key: String::new(),
            member: String::new(),
        },
        Command::ZRank {
            key: String::new(),
            member: String::new(),
        },
        Command::ZCard { key: String::new() },
        Command::ZRange {
            key: String::new(),
            start: 0,
            stop: 0,
            with_scores: false,
        },
        Command::HSet {
            key: String::new(),
            fields: vec![],
        },
        Command::HGet {
            key: String::new(),
            field: String::new(),
        },
        Command::HGetAll { key: String::new() },
        Command::HDel {
            key: String::new(),
            fields: vec![],
        },
        Command::HExists {
            key: String::new(),
            field: String::new(),
        },
        Command::HLen { key: String::new() },
        Command::HIncrBy {
            key: String::new(),
            field: String::new(),
            delta: 0,
        },
        Command::HKeys { key: String::new() },
        Command::HVals { key: String::new() },
        Command::HMGet {
            key: String::new(),
            fields: vec![],
        },
        Command::SAdd {
            key: String::new(),
            members: vec![],
        },
        Command::SRem {
            key: String::new(),
            members: vec![],
        },
        Command::SMembers { key: String::new() },
        Command::SIsMember {
            key: String::new(),
            member: String::new(),
        },
        Command::SCard { key: String::new() },
        Command::Subscribe { channels: vec![] },
        Command::Unsubscribe { channels: vec![] },
        Command::PSubscribe { patterns: vec![] },
        Command::PUnsubscribe { patterns: vec![] },
        Command::Publish {
            channel: String::new(),
            message: bytes::Bytes::new(),
        },
        Command::SlowLogGet { count: None },
        Command::SlowLogLen,
        Command::SlowLogReset,
        Command::Auth {
            username: None,
            password: String::new(),
        },
        Command::Quit,
        Command::Monitor,
        Command::ClientId,
        Command::ClientGetName,
        Command::ClientSetName {
            name: String::new(),
        },
        Command::ClientList,
        Command::ClusterInfo,
        Command::AclWhoAmI,
        Command::AclList,
        Command::AclCat { category: None },
    ];

    let mut names: Vec<&'static str> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for sample in &samples {
        let cats = sample.acl_categories();
        if cats & flag != 0 {
            let name = sample.command_name();
            if seen.insert(name) {
                names.push(name);
            }
        }
    }
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_is_unrestricted() {
        let state = AclState::new();
        let user = state.get_user("default").expect("default user must exist");
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.allcommands);
        assert!(user.allkeys);
    }

    #[test]
    fn cannot_delete_default_user() {
        let mut state = AclState::new();
        let result = state.del_users(&["default".into()]);
        assert!(result.is_err());
        assert!(state.get_user("default").is_some());
    }

    #[test]
    fn rule_on_off() {
        let mut user = AclUser::new();
        assert!(!user.enabled);
        apply_rule(&mut user, "on").unwrap();
        assert!(user.enabled);
        apply_rule(&mut user, "off").unwrap();
        assert!(!user.enabled);
    }

    #[test]
    fn rule_password_add_remove() {
        let mut user = AclUser::new();
        apply_rule(&mut user, ">mypassword").unwrap();
        assert_eq!(user.password_hashes.len(), 1);
        assert!(!user.nopass);

        // duplicate password not added twice
        apply_rule(&mut user, ">mypassword").unwrap();
        assert_eq!(user.password_hashes.len(), 1);

        // verify password
        assert!(user.verify_password("mypassword"));
        assert!(!user.verify_password("wrong"));

        // remove password
        apply_rule(&mut user, "<mypassword").unwrap();
        assert!(user.password_hashes.is_empty());
    }

    #[test]
    fn rule_nopass_and_resetpass() {
        let mut user = AclUser::new();
        apply_rule(&mut user, ">secret").unwrap();
        apply_rule(&mut user, "nopass").unwrap();
        assert!(user.nopass);
        assert!(user.password_hashes.is_empty());
        assert!(user.verify_password("anything"));

        apply_rule(&mut user, "resetpass").unwrap();
        assert!(!user.nopass);
        assert!(!user.verify_password("anything"));
    }

    #[test]
    fn rule_categories() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "+@read").unwrap();
        assert_eq!(user.allowed_categories & CAT_READ, CAT_READ);

        apply_rule(&mut user, "-@read").unwrap();
        assert_eq!(user.allowed_categories & CAT_READ, 0);
        assert_eq!(user.denied_categories & CAT_READ, CAT_READ);
    }

    #[test]
    fn rule_all_categories() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "+@all").unwrap();
        assert!(user.allcommands);

        apply_rule(&mut user, "-@all").unwrap();
        assert!(!user.allcommands);
        assert_eq!(user.denied_categories, ALL_CATEGORIES);
    }

    #[test]
    fn rule_specific_commands() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "+get").unwrap();
        assert!(user.allowed_commands.contains("get"));

        apply_rule(&mut user, "-get").unwrap();
        assert!(!user.allowed_commands.contains("get"));
        assert!(user.denied_commands.contains("get"));
    }

    #[test]
    fn rule_key_patterns() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "~user:*").unwrap();
        assert_eq!(user.key_patterns, vec!["user:*"]);
        assert!(!user.allkeys);

        apply_rule(&mut user, "allkeys").unwrap();
        assert!(user.allkeys);
        assert!(user.key_patterns.is_empty());

        apply_rule(&mut user, "resetkeys").unwrap();
        assert!(!user.allkeys);
        assert!(user.key_patterns.is_empty());
    }

    #[test]
    fn rule_allcommands_nocommands() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "allcommands").unwrap();
        assert!(user.allcommands);
        assert_eq!(user.allowed_categories, ALL_CATEGORIES);

        apply_rule(&mut user, "nocommands").unwrap();
        assert!(!user.allcommands);
        assert_eq!(user.denied_categories, ALL_CATEGORIES);
    }

    #[test]
    fn rule_reset() {
        let mut user = AclUser::unrestricted();
        apply_rule(&mut user, "reset").unwrap();
        assert!(!user.enabled);
        assert!(!user.allcommands);
        assert!(!user.allkeys);
        assert!(user.password_hashes.is_empty());
    }

    #[test]
    fn rule_unknown_category_errors() {
        let mut user = AclUser::new();
        assert!(apply_rule(&mut user, "+@nonexistent").is_err());
    }

    #[test]
    fn rule_empty_password_errors() {
        let mut user = AclUser::new();
        assert!(apply_rule(&mut user, ">").is_err());
    }

    #[test]
    fn rule_unknown_errors() {
        let mut user = AclUser::new();
        assert!(apply_rule(&mut user, "nonsense").is_err());
    }

    #[test]
    fn glob_match_star() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*", "user:"));
        assert!(!glob_match("user:*", "session:123"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn glob_match_question_mark() {
        assert!(glob_match("user:?", "user:a"));
        assert!(!glob_match("user:?", "user:ab"));
    }

    #[test]
    fn glob_match_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn glob_match_complex() {
        assert!(glob_match("*:*", "user:123"));
        assert!(glob_match("cache:*:data", "cache:session:data"));
    }

    #[test]
    fn permission_check_unrestricted() {
        let user = AclUser::unrestricted();
        let cmd = ember_protocol::Command::Get { key: "foo".into() };
        assert!(check_permission(&user, &cmd, "get", CAT_READ | CAT_STRING | CAT_FAST).is_none());
    }

    #[test]
    fn permission_check_denied_command() {
        let mut user = AclUser::unrestricted();
        user.allcommands = false;
        user.denied_commands.insert("set".into());

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        let result = check_permission(&user, &cmd, "set", CAT_WRITE | CAT_STRING | CAT_SLOW);
        assert!(result.is_some());
        if let Some(Frame::Error(msg)) = result {
            assert!(msg.contains("NOPERM"));
        }
    }

    #[test]
    fn permission_check_denied_category() {
        let mut user = AclUser::unrestricted();
        user.allcommands = false;
        user.allowed_categories = CAT_READ | CAT_STRING | CAT_FAST;
        user.denied_categories = CAT_WRITE;

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        // SET has WRITE category — should be denied
        let result = check_permission(&user, &cmd, "set", CAT_WRITE | CAT_STRING | CAT_SLOW);
        assert!(result.is_some());
    }

    #[test]
    fn permission_check_allowed_command_overrides_denied_category() {
        let mut user = AclUser::unrestricted();
        user.allcommands = false;
        user.denied_categories = CAT_WRITE;
        user.allowed_commands.insert("set".into());

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        // SET is explicitly allowed even though WRITE category is denied
        let result = check_permission(&user, &cmd, "set", CAT_WRITE | CAT_STRING | CAT_SLOW);
        assert!(result.is_none());
    }

    #[test]
    fn permission_check_key_pattern() {
        let mut user = AclUser::unrestricted();
        user.allkeys = false;
        user.key_patterns = vec!["user:*".into()];

        let allowed = ember_protocol::Command::Get {
            key: "user:123".into(),
        };
        assert!(check_permission(&user, &allowed, "get", CAT_READ).is_none());

        let denied = ember_protocol::Command::Get {
            key: "session:abc".into(),
        };
        let result = check_permission(&user, &denied, "get", CAT_READ);
        assert!(result.is_some());
        if let Some(Frame::Error(msg)) = result {
            assert!(msg.contains("NOPERM"));
            assert!(msg.contains("session:abc"));
        }
    }

    #[test]
    fn setuser_and_list() {
        let mut state = AclState::new();
        state
            .set_user(
                "alice",
                &[
                    "on".into(),
                    ">pass".into(),
                    "+@read".into(),
                    "~user:*".into(),
                ],
            )
            .unwrap();

        let names = state.usernames();
        assert!(names.contains(&"alice".into()));
        assert!(names.contains(&"default".into()));

        let alice = state.get_user("alice").unwrap();
        assert!(alice.enabled);
        assert!(alice.verify_password("pass"));
        assert!(!alice.verify_password("wrong"));
        assert_eq!(alice.allowed_categories & CAT_READ, CAT_READ);
        assert_eq!(alice.key_patterns, vec!["user:*"]);
    }

    #[test]
    fn deluser() {
        let mut state = AclState::new();
        state.set_user("bob", &["on".into()]).unwrap();
        assert!(state.get_user("bob").is_some());

        let deleted = state.del_users(&["bob".into()]).unwrap();
        assert_eq!(deleted, 1);
        assert!(state.get_user("bob").is_none());
    }

    #[test]
    fn format_user_round_trip() {
        let mut state = AclState::new();
        state
            .set_user(
                "test",
                &[
                    "on".into(),
                    "nopass".into(),
                    "allcommands".into(),
                    "allkeys".into(),
                ],
            )
            .unwrap();
        let list = state.list();
        assert!(list
            .iter()
            .any(|l| l.contains("user test") && l.contains("on") && l.contains("nopass")));
    }

    #[test]
    fn sha256_constant_time() {
        let hash1 = sha256_hash("password");
        let hash2 = sha256_hash("password");
        let hash3 = sha256_hash("different");
        assert!(bool::from(hash1.ct_eq(&hash2)));
        assert!(!bool::from(hash1.ct_eq(&hash3)));
    }

    #[test]
    fn all_category_names_returned() {
        let names = all_category_names();
        assert!(names.contains(&"read"));
        assert!(names.contains(&"write"));
        assert!(names.contains(&"admin"));
        assert!(names.contains(&"cluster"));
        assert_eq!(names.len(), 17);
    }

    #[test]
    fn category_from_name_case_insensitive() {
        assert_eq!(category_from_name("READ"), Some(CAT_READ));
        assert_eq!(category_from_name("Read"), Some(CAT_READ));
        assert_eq!(category_from_name("nope"), None);
    }
}
