//! Per-user access control (ACL) for ember.
//!
//! Provides command and key permissions per user. ACL is always active: the
//! `default` user takes its password from `requirepass` (or has none), and
//! `--aclfile` or ACL SETUSER add more users. A user with access to every
//! command and key skips the detailed checks through two boolean flags.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard};

use ember_core::glob_match;
use ember_protocol::command::category as cat;
use ember_protocol::types::Frame;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// categories
// ---------------------------------------------------------------------------

/// All category flags OR'd together.
const ALL_CATEGORIES: u64 = cat::READ
    | cat::WRITE
    | cat::STRING
    | cat::LIST
    | cat::SET
    | cat::SORTEDSET
    | cat::HASH
    | cat::KEYSPACE
    | cat::SERVER
    | cat::CONNECTION
    | cat::TRANSACTION
    | cat::PUBSUB
    | cat::FAST
    | cat::SLOW
    | cat::ADMIN
    | cat::DANGEROUS
    | cat::CLUSTER;

/// All known category names, sorted alphabetically.
const CATEGORY_NAMES: &[(&str, u64)] = &[
    ("admin", cat::ADMIN),
    ("cluster", cat::CLUSTER),
    ("connection", cat::CONNECTION),
    ("dangerous", cat::DANGEROUS),
    ("fast", cat::FAST),
    ("hash", cat::HASH),
    ("keyspace", cat::KEYSPACE),
    ("list", cat::LIST),
    ("pubsub", cat::PUBSUB),
    ("read", cat::READ),
    ("server", cat::SERVER),
    ("set", cat::SET),
    ("slow", cat::SLOW),
    ("sortedset", cat::SORTEDSET),
    ("string", cat::STRING),
    ("transaction", cat::TRANSACTION),
    ("write", cat::WRITE),
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
    /// Command rules in the order they were given. The last rule that
    /// matches a command decides whether the user may run it, as in Redis.
    /// No rules means no commands.
    command_rules: Vec<CommandRule>,
    /// Key glob patterns. Empty means no keys allowed (unless `allkeys`).
    pub key_patterns: Vec<String>,
    /// Fast path: skip key pattern matching.
    pub allkeys: bool,
    /// Pub/sub channel glob patterns, from `&pattern` rules. Empty means
    /// no channels, unless `allchannels`.
    pub channel_patterns: Vec<String>,
    /// Fast path: skip channel pattern matching.
    pub allchannels: bool,
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
            command_rules: Vec::new(),
            key_patterns: Vec::new(),
            allkeys: false,
            // as in Redis 7, a new user may use no channels until given some
            channel_patterns: Vec::new(),
            allchannels: false,
            allcommands: false,
        }
    }

    /// Creates the default "unrestricted" user — full access, no password required.
    pub fn unrestricted() -> Self {
        Self {
            enabled: true,
            password_hashes: Vec::new(),
            nopass: true,
            command_rules: vec![CommandRule::Category {
                flags: ALL_CATEGORIES,
                allow: true,
            }],
            key_patterns: Vec::new(),
            allkeys: true,
            channel_patterns: Vec::new(),
            allchannels: true,
            allcommands: true,
        }
    }

    /// Adds a command rule, dropping an earlier rule for the same command
    /// or category, which the new one overrides.
    fn push_rule(&mut self, rule: CommandRule) {
        if matches!(
            rule,
            CommandRule::Category {
                flags: ALL_CATEGORIES,
                ..
            }
        ) {
            // +@all and -@all override every earlier rule
            self.command_rules.clear();
            if rule
                == (CommandRule::Category {
                    flags: ALL_CATEGORIES,
                    allow: false,
                })
            {
                self.allcommands = false;
                return;
            }
        } else {
            self.command_rules.retain(|r| !r.same_target(&rule));
        }
        self.command_rules.push(rule);
        self.allcommands = matches!(
            self.command_rules.as_slice(),
            [CommandRule::Category {
                flags: ALL_CATEGORIES,
                allow: true
            }]
        );
    }

    /// Whether the user may run the command `name` (lowercase), which
    /// belongs to `categories`.
    fn may_run(&self, name: &str, categories: u64) -> bool {
        self.command_rules
            .iter()
            .rev()
            .find_map(|rule| match rule {
                CommandRule::Category { flags, allow } if flags & categories != 0 => Some(*allow),
                CommandRule::Command { name: n, allow } if n == name => Some(*allow),
                _ => None,
            })
            .unwrap_or(false)
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

/// One `+`/`-` rule for a command or category.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CommandRule {
    /// `+@category` or `-@category`. `flags` is one category, or
    /// [`ALL_CATEGORIES`] for `@all`.
    Category { flags: u64, allow: bool },
    /// `+command` or `-command`, lowercase.
    Command { name: String, allow: bool },
}

impl CommandRule {
    /// Whether both rules are about the same command or category.
    fn same_target(&self, other: &CommandRule) -> bool {
        match (self, other) {
            (Self::Category { flags: a, .. }, Self::Category { flags: b, .. }) => a == b,
            (Self::Command { name: a, .. }, Self::Command { name: b, .. }) => a == b,
            _ => false,
        }
    }

    /// The rule as ACL SETUSER takes it.
    fn to_rule_string(&self) -> String {
        match self {
            Self::Category { flags, allow } => {
                let name = if *flags == ALL_CATEGORIES {
                    "all"
                } else {
                    category_name(*flags)
                };
                format!("{}@{name}", if *allow { '+' } else { '-' })
            }
            Self::Command { name, allow } => format!("{}{name}", if *allow { '+' } else { '-' }),
        }
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
/// - `&pattern` — add a pub/sub channel pattern
/// - `allchannels` / `&*` — allow every channel
/// - `resetchannels` — clear all channel patterns
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
        "allcommands" => user.push_rule(CommandRule::Category {
            flags: ALL_CATEGORIES,
            allow: true,
        }),
        "nocommands" => user.push_rule(CommandRule::Category {
            flags: ALL_CATEGORIES,
            allow: false,
        }),
        "allkeys" | "~*" => {
            user.allkeys = true;
            user.key_patterns.clear();
        }
        "resetkeys" => {
            user.allkeys = false;
            user.key_patterns.clear();
        }
        "allchannels" | "&*" => {
            user.allchannels = true;
            user.channel_patterns.clear();
        }
        "resetchannels" => {
            user.allchannels = false;
            user.channel_patterns.clear();
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
        // a password given by its SHA-256 hash, as ACL LIST prints it
        _ if rule.starts_with('#') => {
            let hash = hex_decode(&rule[1..])
                .ok_or("ERR the password hash must be 64 hexadecimal characters")?;
            if !user.password_hashes.contains(&hash) {
                user.password_hashes.push(hash);
            }
            user.nopass = false;
        }
        _ if rule.starts_with('!') => {
            let hash = hex_decode(&rule[1..])
                .ok_or("ERR the password hash must be 64 hexadecimal characters")?;
            user.password_hashes.retain(|h| h != &hash);
        }
        _ if rule.starts_with("+@") || rule.starts_with("-@") => {
            let cat_name = &rule[2..];
            let flags = if cat_name.eq_ignore_ascii_case("all") {
                ALL_CATEGORIES
            } else {
                category_from_name(cat_name)
                    .ok_or_else(|| format!("ERR unknown ACL category '{cat_name}'"))?
            };
            user.push_rule(CommandRule::Category {
                flags,
                allow: rule.starts_with('+'),
            });
        }
        _ if rule.starts_with('+') || rule.starts_with('-') => {
            let name = rule[1..].to_ascii_lowercase();
            if name.is_empty() {
                return Err("ERR empty command name".into());
            }
            user.push_rule(CommandRule::Command {
                name,
                allow: rule.starts_with('+'),
            });
        }
        _ if rule.starts_with('&') => {
            let pattern = &rule[1..];
            if pattern.is_empty() {
                return Err("ERR empty channel pattern".into());
            }
            // `allchannels` already matches every channel
            if !user.allchannels && !user.channel_patterns.iter().any(|p| p == pattern) {
                user.channel_patterns.push(pattern.to_string());
            }
        }
        _ if rule.starts_with('~') => {
            let pattern = &rule[1..];
            if pattern.is_empty() {
                return Err("ERR empty key pattern".into());
            }
            // `allkeys` already matches every key. adding a pattern after it
            // must not narrow access down to that one pattern.
            if !user.allkeys {
                user.key_patterns.push(pattern.to_string());
            }
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
/// The caller should check `allcommands && allkeys && allchannels` before
/// calling this function as a fast path — this function handles the
/// detailed checks.
pub fn check_permission(
    user: &AclUser,
    cmd: &ember_protocol::Command,
    cmd_name: &str,
    categories: u64,
) -> Option<Frame> {
    // check command permission
    if !user.allcommands {
        let lower_name = cmd_name.to_ascii_lowercase();
        if !user.may_run(&lower_name, categories) {
            return Some(Frame::Error(format!(
                "NOPERM this user has no permissions to run the '{lower_name}' command"
            )));
        }
    }

    // check key permission
    if !user.allkeys {
        for key in cmd.keys().iter() {
            if !user.key_patterns.iter().any(|pat| glob_match(pat, key)) {
                return Some(Frame::Error(format!(
                    "NOPERM this user has no permissions to access the '{key}' key"
                )));
            }
        }
    }

    check_channels(user, cmd)
}

/// Checks the channels a pub/sub command uses against the user's `&`
/// patterns. As in Redis, a channel must match one of the patterns, and a
/// pattern given to PSUBSCRIBE must be one of them exactly, since a broader
/// pattern would reach channels the user may not read.
fn check_channels(user: &AclUser, cmd: &ember_protocol::Command) -> Option<Frame> {
    use ember_protocol::Command;
    if user.allchannels {
        return None;
    }
    let denied = |channel: &str| {
        Some(Frame::Error(format!(
            "NOPERM this user has no permissions to access the '{channel}' channel"
        )))
    };
    let matches = |channel: &str| user.channel_patterns.iter().any(|p| glob_match(p, channel));
    match cmd {
        Command::Subscribe { channels } => channels
            .iter()
            .find(|c| !matches(c))
            .and_then(|c| denied(c)),
        Command::Publish { channel, .. } if !matches(channel) => denied(channel),
        Command::PSubscribe { patterns } => patterns
            .iter()
            .find(|p| !user.channel_patterns.contains(p))
            .and_then(|p| denied(p)),
        _ => None,
    }
}

/// Formats patterns with their rule prefix: `~` for keys, `&` for channels.
/// `all` prints as the pattern `*`.
fn format_patterns(prefix: char, all: bool, patterns: &[String]) -> Vec<String> {
    if all {
        vec![format!("{prefix}*")]
    } else {
        patterns.iter().map(|p| format!("{prefix}{p}")).collect()
    }
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
    /// Creates the ACL state with only the `default` user.
    ///
    /// The default user can run every command on every key. With a
    /// `requirepass` it must authenticate with that password; without one
    /// it needs no password, so new connections start authenticated.
    pub fn new(requirepass: Option<&str>) -> Self {
        let mut default = AclUser::unrestricted();
        if let Some(password) = requirepass {
            default.nopass = false;
            default.password_hashes.push(sha256_hash(password));
        }
        let mut users = HashMap::new();
        users.insert("default".into(), default);
        Self { users }
    }

    /// Loads users from an ACL file in the Redis format: one
    /// `user <name> <rule> ...` line per user. Blank lines and lines
    /// starting with `#` are skipped. Each line's rules apply on top of any
    /// existing user with that name, including `default`.
    pub fn load_file(&mut self, contents: &str) -> Result<(), String> {
        for (i, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut words = line.split_whitespace();
            let (Some("user"), Some(name)) = (words.next(), words.next()) else {
                return Err(format!("line {}: expected 'user <name> <rules...>'", i + 1));
            };
            let rules: Vec<String> = words.map(str::to_owned).collect();
            self.set_user(name, &rules)
                .map_err(|e| format!("line {}: {e}", i + 1))?;
        }
        Ok(())
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

    /// Creates or updates a user by applying rules. The rules apply all
    /// together or, if one is invalid, not at all.
    pub fn set_user(&mut self, name: &str, rules: &[String]) -> Result<(), String> {
        let mut user = self.users.get(name).cloned().unwrap_or_else(AclUser::new);
        for rule in rules {
            apply_rule(&mut user, rule)?;
        }
        self.users.insert(name.to_string(), user);
        Ok(())
    }

    /// Deletes users by name. Returns the number deleted.
    /// The "default" user cannot be deleted.
    pub fn del_users(&mut self, names: &[String]) -> Result<usize, String> {
        // check first, so a refused call deletes nobody
        if names.iter().any(|name| name == "default") {
            return Err("ERR The 'default' user cannot be removed".into());
        }
        Ok(names
            .iter()
            .filter(|name| self.users.remove(name.as_str()).is_some())
            .count())
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

    parts.push(format_commands(user));
    parts.extend(format_patterns('~', user.allkeys, &user.key_patterns));
    // a user without channels gets no channel rule, which ACL SETUSER reads
    // back the same way, since new users start without channels
    let channels = format_patterns('&', user.allchannels, &user.channel_patterns);
    if channels.is_empty() {
        parts.push("resetchannels".into());
    }
    parts.extend(channels);

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
    if user.allchannels {
        flags.push(Frame::Bulk(bytes::Bytes::from_static(b"allchannels")));
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
    fields.push(Frame::Bulk(bytes::Bytes::from(format_commands(user))));

    // keys
    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"keys")));
    let keys = format_patterns('~', user.allkeys, &user.key_patterns).join(" ");
    fields.push(Frame::Bulk(bytes::Bytes::from(keys)));

    fields.push(Frame::Bulk(bytes::Bytes::from_static(b"channels")));
    let channels = format_patterns('&', user.allchannels, &user.channel_patterns).join(" ");
    fields.push(Frame::Bulk(bytes::Bytes::from(channels)));

    Frame::Array(fields)
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parses 64 hex characters into a SHA-256 hash.
fn hex_decode(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (byte, pair) in out.iter_mut().zip(hex.as_bytes().chunks(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

/// The user's command rules in order, starting from `-@all`, which is
/// what a user with no rules has. ACL SETUSER reproduces them.
fn format_commands(user: &AclUser) -> String {
    let rules = user.command_rules.iter().map(CommandRule::to_rule_string);
    match user.command_rules.first() {
        Some(CommandRule::Category {
            flags: ALL_CATEGORIES,
            allow: true,
        }) => rules.collect::<Vec<_>>().join(" "),
        _ => std::iter::once("-@all".to_owned())
            .chain(rules)
            .collect::<Vec<_>>()
            .join(" "),
    }
}

// ---------------------------------------------------------------------------
// convenience type alias
// ---------------------------------------------------------------------------

/// The ACL users shared by every connection, plus a generation number that
/// changes whenever a user is added, changed or deleted. Connections compare
/// the generation, a single atomic load, to learn that their copy of their
/// user is stale.
#[derive(Debug)]
pub struct Acl {
    users: RwLock<AclState>,
    generation: AtomicU64,
}

impl Acl {
    pub fn new(users: AclState) -> Self {
        Self {
            users: RwLock::new(users),
            generation: AtomicU64::new(0),
        }
    }

    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, AclState>> {
        self.users.read()
    }

    /// Changes the users under the write lock and bumps the generation.
    /// Returns `None` if the lock is poisoned.
    fn modify<T>(&self, change: impl FnOnce(&mut AclState) -> T) -> Option<T> {
        let mut users = self.users.write().ok()?;
        let result = change(&mut users);
        self.generation.fetch_add(1, Ordering::Release);
        Some(result)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

pub type SharedAclState = Arc<Acl>;

const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";
const LOCK_POISONED: &str = "ERR ACL state lock poisoned";

/// Checks a username and password against the ACL users and returns a copy
/// of the user on success. Every AUTH path calls this.
pub fn authenticate(
    acl: &SharedAclState,
    username: &str,
    password: &str,
) -> Result<AclUser, String> {
    let state = acl.read().map_err(|_| LOCK_POISONED.to_string())?;
    match state.get_user(username) {
        Some(user) if username == "default" && user.nopass => {
            Err("ERR Client sent AUTH, but no password is set. \
             Did you mean ACL SETUSER with >password?"
                .into())
        }
        Some(user) if user.enabled && user.verify_password(password) => Ok(user.clone()),
        _ => Err(WRONGPASS.into()),
    }
}

/// Runs ACL LIST, USERS, GETUSER, SETUSER, DELUSER and CAT. Both connection
/// handlers call this. ACL WHOAMI needs the connection's user, so the
/// handlers answer it themselves.
pub fn run_admin_command(acl: &SharedAclState, cmd: ember_protocol::Command) -> Frame {
    use ember_protocol::Command;

    let bulk_array = |items: Vec<String>| {
        Frame::Array(items.into_iter().map(|i| Frame::Bulk(i.into())).collect())
    };
    let reply = match cmd {
        Command::AclList => acl.read().ok().map(|state| bulk_array(state.list())),
        Command::AclUsers => acl.read().ok().map(|state| bulk_array(state.usernames())),
        Command::AclGetUser { username } => acl.read().ok().map(|state| {
            state
                .get_user_detail(&username)
                .unwrap_or_else(|| Frame::Error(format!("ERR no such user '{username}'")))
        }),
        Command::AclSetUser { username, rules } => {
            acl.modify(|state| match state.set_user(&username, &rules) {
                Ok(()) => Frame::Simple("OK".into()),
                Err(msg) => Frame::Error(msg),
            })
        }
        Command::AclDelUser { usernames } => {
            acl.modify(|state| match state.del_users(&usernames) {
                Ok(count) => Frame::Integer(count as i64),
                Err(msg) => Frame::Error(msg),
            })
        }
        Command::AclCat { category } => Some(handle_acl_cat(category.as_deref())),
        other => Some(Frame::Error(format!(
            "ERR '{}' is not an ACL admin command",
            other.command_name()
        ))),
    };
    reply.unwrap_or_else(|| Frame::Error(LOCK_POISONED.into()))
}

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
///
/// Built from the example calls in the command table, which cover every
/// command and subcommand the server parses.
fn commands_in_category(flag: u64) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = ember_protocol::command::table::examples()
        .filter_map(|example| ember_protocol::command::table::parse_example(example).ok())
        .filter(|cmd| cmd.acl_categories() & flag != 0)
        .map(|cmd| cmd.command_name())
        .collect();
    names.sort_unstable();
    names.dedup();
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
        let state = AclState::new(None);
        let user = state.get_user("default").expect("default user must exist");
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.allcommands);
        assert!(user.allkeys);
    }

    #[test]
    fn cannot_delete_default_user() {
        let mut state = AclState::new(None);
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
        assert!(user.may_run("get", cat::READ | cat::STRING | cat::FAST));

        apply_rule(&mut user, "-@read").unwrap();
        assert!(!user.may_run("get", cat::READ | cat::STRING | cat::FAST));
    }

    #[test]
    fn the_last_matching_rule_decides() {
        let mut user = AclUser::new();
        for rule in ["-@all", "+@read"] {
            apply_rule(&mut user, rule).unwrap();
        }
        assert!(user.may_run("get", cat::READ | cat::STRING | cat::FAST));
        assert!(!user.may_run("set", cat::WRITE | cat::STRING | cat::SLOW));

        // a later category rule overrides an earlier command rule
        for rule in ["+set", "-@string"] {
            apply_rule(&mut user, rule).unwrap();
        }
        assert!(!user.may_run("set", cat::WRITE | cat::STRING | cat::SLOW));
        assert!(!user.may_run("get", cat::READ | cat::STRING | cat::FAST));
        apply_rule(&mut user, "+get").unwrap();
        assert!(user.may_run("get", cat::READ | cat::STRING | cat::FAST));
    }

    #[test]
    fn rule_all_categories() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "+@all").unwrap();
        assert!(user.allcommands);

        apply_rule(&mut user, "-@all").unwrap();
        assert!(!user.allcommands);
        assert!(!user.may_run("get", cat::READ | cat::STRING | cat::FAST));
    }

    #[test]
    fn rule_specific_commands() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "+get").unwrap();
        assert!(user.may_run("get", cat::READ | cat::STRING | cat::FAST));

        apply_rule(&mut user, "-get").unwrap();
        assert!(!user.may_run("get", cat::READ | cat::STRING | cat::FAST));
        assert_eq!(user.command_rules.len(), 1, "the old rule was replaced");
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

        apply_rule(&mut user, "nocommands").unwrap();
        assert!(!user.allcommands);
        assert!(!user.may_run("get", cat::READ | cat::STRING | cat::FAST));
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
    fn permission_check_unrestricted() {
        let user = AclUser::unrestricted();
        let cmd = ember_protocol::Command::Get { key: "foo".into() };
        assert!(
            check_permission(&user, &cmd, "get", cat::READ | cat::STRING | cat::FAST).is_none()
        );
    }

    #[test]
    fn permission_check_denied_command() {
        let mut user = AclUser::unrestricted();
        apply_rule(&mut user, "-set").unwrap();

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        let result = check_permission(&user, &cmd, "set", cat::WRITE | cat::STRING | cat::SLOW);
        assert!(result.is_some());
        if let Some(Frame::Error(msg)) = result {
            assert!(msg.contains("NOPERM"));
        }
    }

    #[test]
    fn permission_check_denied_category() {
        let mut user = AclUser::unrestricted();
        apply_rule(&mut user, "-@write").unwrap();

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        // SET has WRITE category — should be denied
        let result = check_permission(&user, &cmd, "set", cat::WRITE | cat::STRING | cat::SLOW);
        assert!(result.is_some());
    }

    #[test]
    fn permission_check_allowed_command_overrides_denied_category() {
        let mut user = AclUser::unrestricted();
        apply_rule(&mut user, "-@write").unwrap();
        apply_rule(&mut user, "+set").unwrap();

        let cmd = ember_protocol::Command::Set {
            key: "foo".into(),
            value: bytes::Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        // SET is explicitly allowed even though WRITE category is denied
        let result = check_permission(&user, &cmd, "set", cat::WRITE | cat::STRING | cat::SLOW);
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
        assert!(check_permission(&user, &allowed, "get", cat::READ).is_none());

        let denied = ember_protocol::Command::Get {
            key: "session:abc".into(),
        };
        let result = check_permission(&user, &denied, "get", cat::READ);
        assert!(result.is_some());
        if let Some(Frame::Error(msg)) = result {
            assert!(msg.contains("NOPERM"));
            assert!(msg.contains("session:abc"));
        }
    }

    #[test]
    fn permission_check_covers_keys_of_every_command() {
        // these commands used to skip the key check entirely
        let mut user = AclUser::unrestricted();
        user.allkeys = false;
        user.key_patterns = vec!["user:*".into()];

        let getset = ember_protocol::Command::GetSet {
            key: "secret:x".into(),
            value: bytes::Bytes::from("v"),
        };
        assert!(check_permission(&user, &getset, "getset", cat::WRITE).is_some());

        let store = ember_protocol::Command::SUnionStore {
            dest: "user:mine".into(),
            keys: vec!["secret:a".into()],
        };
        assert!(check_permission(&user, &store, "sunionstore", cat::WRITE).is_some());
    }

    #[test]
    fn deny_rules_after_allow_all_take_effect() {
        let mut user = AclUser::new();
        for rule in [
            "on",
            "nopass",
            "+@all",
            "-@dangerous",
            "-flushall",
            "allkeys",
        ] {
            apply_rule(&mut user, rule).unwrap();
        }
        let flushall = ember_protocol::Command::FlushAll { async_mode: false };
        let categories = flushall.acl_categories();
        assert!(check_permission(&user, &flushall, "flushall", categories).is_some());

        let get = ember_protocol::Command::Get { key: "k".into() };
        assert!(check_permission(&user, &get, "get", get.acl_categories()).is_none());
    }

    #[test]
    fn no_dangerous_blocks_admin_commands() {
        let mut user = AclUser::new();
        for rule in ["on", "nopass", "+@all", "-@dangerous", "allkeys"] {
            apply_rule(&mut user, rule).unwrap();
        }
        for args in [
            &["ACL", "SETUSER", "self", "allcommands"][..],
            &["CONFIG", "SET", "maxmemory", "0"],
            &["MONITOR"],
            &["CLIENT", "LIST"],
            &["SLOWLOG", "RESET"],
            &["BGSAVE"],
            &["CLUSTER", "FORGET", "abc"],
            &["MIGRATE", "h", "6379", "k", "0", "1000"],
            &["RESTORE", "k", "0", "v"],
        ] {
            let frame = Frame::Array(
                args.iter()
                    .map(|a| Frame::Bulk(bytes::Bytes::copy_from_slice(a.as_bytes())))
                    .collect(),
            );
            let cmd = ember_protocol::Command::from_frame(frame).unwrap();
            let denied = check_permission(&user, &cmd, cmd.command_name(), cmd.acl_categories());
            assert!(denied.is_some(), "{args:?} should need @dangerous");
        }
    }

    #[test]
    fn key_pattern_after_allkeys_keeps_every_key() {
        let mut user = AclUser::new();
        apply_rule(&mut user, "allkeys").unwrap();
        apply_rule(&mut user, "~user:*").unwrap();
        assert!(user.allkeys);
    }

    #[test]
    fn default_user_takes_its_password_from_requirepass() {
        let open = AclState::new(None);
        assert!(open.get_user("default").unwrap().nopass);

        let acl: SharedAclState = Arc::new(Acl::new(AclState::new(Some("secret"))));
        assert!(authenticate(&acl, "default", "secret").is_ok());
        assert!(authenticate(&acl, "default", "wrong").is_err());
    }

    #[test]
    fn acl_file_adds_users_and_reports_bad_lines() {
        let mut state = AclState::new(None);
        state
            .load_file("# comment\n\nuser app on >pw +get ~app:*\n")
            .unwrap();
        let acl: SharedAclState = Arc::new(Acl::new(state));
        let app = authenticate(&acl, "app", "pw").unwrap();
        assert_eq!(app.key_patterns, vec!["app:*"]);

        let err = AclState::new(None).load_file("user\n").unwrap_err();
        assert!(err.starts_with("line 1"));
        let err = AclState::new(None).load_file("user x bogus\n").unwrap_err();
        assert!(err.contains("bogus"));
    }

    #[test]
    fn setuser_applies_no_rule_when_one_is_invalid() {
        let mut acl = AclState::new(None);
        let rules = |r: &[&str]| r.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        acl.set_user("alice", &rules(&["on", ">pw"])).unwrap();

        assert!(acl.set_user("alice", &rules(&["off", "bogus"])).is_err());
        assert!(
            acl.get_user("alice").unwrap().enabled,
            "the valid rule applied"
        );
        assert!(acl.set_user("bob", &rules(&["on", "bogus"])).is_err());
        assert!(acl.get_user("bob").is_none(), "the user was created");
    }

    #[test]
    fn setuser_and_list() {
        let mut state = AclState::new(None);
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
        assert!(alice.may_run("get", cat::READ | cat::STRING | cat::FAST));
        assert_eq!(alice.key_patterns, vec!["user:*"]);
    }

    #[test]
    fn acl_list_output_loads_back_as_the_same_user() {
        let mut state = AclState::new(None);
        let rules: Vec<String> = ["on", ">secret", "-@all", "+@read", "-keys", "~app:*"]
            .map(String::from)
            .into();
        state.set_user("alice", &rules).unwrap();
        let line = state
            .list()
            .into_iter()
            .find(|l| l.starts_with("user alice "))
            .unwrap();
        let reloaded: Vec<String> = line.split(' ').skip(2).map(String::from).collect();
        state.set_user("copy", &reloaded).unwrap();
        let copy = state.get_user("copy").unwrap();
        assert!(copy.verify_password("secret"));
        assert!(copy.may_run("get", cat::READ | cat::STRING | cat::FAST));
        assert!(!copy.may_run("keys", cat::READ | cat::KEYSPACE | cat::SLOW));
        assert_eq!(copy.key_patterns, ["app:*"]);
    }

    #[test]
    fn channel_rules() {
        let mut user = AclUser::new();
        assert!(!user.allchannels && user.channel_patterns.is_empty());
        apply_rule(&mut user, "&news.*").unwrap();
        apply_rule(&mut user, "&news.*").unwrap();
        assert_eq!(user.channel_patterns, ["news.*"]);
        apply_rule(&mut user, "&*").unwrap();
        assert!(user.allchannels);
        apply_rule(&mut user, "resetchannels").unwrap();
        assert!(!user.allchannels && user.channel_patterns.is_empty());
        assert!(apply_rule(&mut user, "&").is_err());
        assert!(AclUser::unrestricted().allchannels);
    }

    #[test]
    fn channels_are_checked_for_publish_and_subscribe() {
        use ember_protocol::Command;
        let mut user = AclUser::unrestricted();
        apply_rule(&mut user, "resetchannels").unwrap();
        apply_rule(&mut user, "&news.*").unwrap();
        let publish = |channel: &str| Command::Publish {
            channel: channel.into(),
            message: bytes::Bytes::from_static(b"m"),
        };
        assert!(check_channels(&user, &publish("news.sport")).is_none());
        assert!(check_channels(&user, &publish("secret")).is_some());
        let psubscribe = |pattern: &str| Command::PSubscribe {
            patterns: vec![pattern.into()],
        };
        assert!(check_channels(&user, &psubscribe("news.*")).is_none());
        assert!(check_channels(&user, &psubscribe("news.s*")).is_some());
    }

    #[test]
    fn deluser_refusing_default_deletes_nobody() {
        let mut state = AclState::new(None);
        state.set_user("bob", &["on".into()]).unwrap();
        assert!(state.del_users(&["bob".into(), "default".into()]).is_err());
        assert!(state.get_user("bob").is_some());
    }

    #[test]
    fn deluser() {
        let mut state = AclState::new(None);
        state.set_user("bob", &["on".into()]).unwrap();
        assert!(state.get_user("bob").is_some());

        let deleted = state.del_users(&["bob".into()]).unwrap();
        assert_eq!(deleted, 1);
        assert!(state.get_user("bob").is_none());
    }

    #[test]
    fn format_user_round_trip() {
        let mut state = AclState::new(None);
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
    fn acl_cat_lists_every_command_in_the_category() {
        let writes = commands_in_category(category_from_name("write").unwrap());
        for name in [
            "set",
            "setrange",
            "msetnx",
            "migrate",
            "restore",
            "vadd",
            "proto.set",
        ] {
            assert!(writes.contains(&name), "write is missing {name}");
        }
        assert!(!writes.contains(&"get"));
        let dangerous = commands_in_category(category_from_name("dangerous").unwrap());
        assert!(dangerous.contains(&"cluster_setslot"));
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
        assert_eq!(category_from_name("READ"), Some(cat::READ));
        assert_eq!(category_from_name("Read"), Some(cat::READ));
        assert_eq!(category_from_name("nope"), None);
    }
}
