//! Interactive REPL for ember.
//!
//! Uses rustyline for readline editing, history, and tab-completion.
//! Commands are tokenized and sent as raw RESP3 arrays — no client-side
//! validation. The server handles everything.

use std::borrow::Cow;
use std::io::Write;
use std::path::PathBuf;

use colored::Colorize;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context, Editor, Helper};

use crate::commands::{
    command_names, commands_by_group, find_command, has_subcommands, subcommands,
};
use crate::connection::{Connection, ConnectionError};
use crate::format::format_response;

/// Runs the interactive REPL loop.
///
/// Blocks the calling thread. Uses `tokio::runtime::Runtime` internally
/// because rustyline needs the main thread for terminal I/O.
pub fn run_repl(host: &str, port: u16, password: Option<&str>, tls: bool) {
    if tls {
        eprintln!("{}", "tls is not yet supported".yellow());
        return;
    }

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{}", format!("failed to create runtime: {e}").red());
            return;
        }
    };

    // connect to server
    let mut conn = match rt.block_on(Connection::connect(host, port)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "{}",
                format!("could not connect to {host}:{port}: {e}").red()
            );
            return;
        }
    };

    // authenticate if needed
    if let Some(pw) = password {
        if let Err(e) = rt.block_on(conn.authenticate(pw)) {
            eprintln!("{}", format!("authentication failed: {e}").red());
            rt.block_on(conn.shutdown());
            return;
        }
    }

    // set up rustyline
    let config = Config::builder()
        .completion_type(CompletionType::List)
        .build();

    let mut rl = match Editor::with_config(config) {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("{}", format!("failed to create editor: {e}").red());
            rt.block_on(conn.shutdown());
            return;
        }
    };
    rl.set_helper(Some(EmberHelper));

    let history_path = history_file();
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    let prompt = format!("{host}:{port}> ");

    loop {
        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                // handle local commands
                let first_word = trimmed.split_whitespace().next().unwrap_or("");
                match first_word.to_lowercase().as_str() {
                    "quit" | "exit" => break,
                    "clear" => {
                        print!("\x1B[2J\x1B[1;1H");
                        let _ = std::io::stdout().flush();
                        continue;
                    }
                    "help" => {
                        handle_help(trimmed);
                        continue;
                    }
                    _ => {}
                }

                // tokenize and send to server
                let tokens = match tokenize(trimmed) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("{}", format!("parse error: {e}").red());
                        continue;
                    }
                };

                if tokens.is_empty() {
                    continue;
                }

                match rt.block_on(conn.send_command(&tokens)) {
                    Ok(frame) => {
                        println!("{}", format_response(&frame));
                    }
                    Err(ConnectionError::Disconnected) => {
                        eprintln!("{}", "server disconnected, reconnecting...".yellow());
                        match rt.block_on(reconnect(host, port, password)) {
                            Ok(new_conn) => {
                                conn = new_conn;
                                eprintln!("{}", "reconnected".green());
                            }
                            Err(e) => {
                                eprintln!("{}", format!("reconnection failed: {e}").red());
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", format!("error: {e}").red());
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C — just show a new prompt
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D — exit
                break;
            }
            Err(e) => {
                eprintln!("{}", format!("readline error: {e}").red());
                break;
            }
        }
    }

    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }

    // graceful shutdown — send QUIT and close the TCP stream
    rt.block_on(conn.shutdown());
}

/// Establishes a new connection, authenticating if a password is provided.
async fn reconnect(
    host: &str,
    port: u16,
    password: Option<&str>,
) -> Result<Connection, ConnectionError> {
    let mut conn = Connection::connect(host, port).await?;
    if let Some(pw) = password {
        conn.authenticate(pw).await?;
    }
    Ok(conn)
}

/// Handles the `help` local command.
fn handle_help(input: &str) {
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.len() >= 2 {
        // help for a specific command
        let name = parts[1];
        match find_command(name) {
            Some(cmd) => {
                println!(
                    "  {} {}\n  {}\n  group: {}",
                    cmd.name.bold(),
                    cmd.args.dimmed(),
                    cmd.summary,
                    cmd.group,
                );
            }
            None => {
                println!("unknown command '{}'. try just 'help' for a list.", name);
            }
        }
        return;
    }

    // full help listing
    println!("{}", "ember commands:".bold());
    println!();
    for (group, cmds) in commands_by_group() {
        println!("  {}:", group.bold());
        for cmd in cmds {
            println!("    {:<20} {}", cmd.name, cmd.summary.dimmed());
        }
        println!();
    }
    println!(
        "type {} for details on a specific command.",
        "help <command>".bold()
    );
}

/// Returns the path to the history file.
fn history_file() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".emberkv_history"))
}

// -----------------------------------------------------------------------
// tokenizer
// -----------------------------------------------------------------------

/// Tokenizes a command string into individual arguments.
///
/// Handles double-quoted and single-quoted strings with backslash
/// escapes inside double quotes. Unquoted tokens are split on whitespace.
///
/// # Errors
///
/// Returns an error on unmatched quotes.
pub fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
                chars.next();
            }
            '"' => {
                in_token = true;
                chars.next(); // consume opening quote
                loop {
                    match chars.next() {
                        None => return Err("unmatched double quote".into()),
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(escaped) => current.push(escaped),
                            None => return Err("trailing backslash".into()),
                        },
                        Some(c) => current.push(c),
                    }
                }
            }
            '\'' => {
                in_token = true;
                chars.next(); // consume opening quote
                loop {
                    match chars.next() {
                        None => return Err("unmatched single quote".into()),
                        Some('\'') => break,
                        Some(c) => current.push(c),
                    }
                }
            }
            _ => {
                in_token = true;
                current.push(ch);
                chars.next();
            }
        }
    }

    if in_token {
        tokens.push(current);
    }

    Ok(tokens)
}

// -----------------------------------------------------------------------
// rustyline helper (completer + stubs)
// -----------------------------------------------------------------------

struct EmberHelper;

impl Helper for EmberHelper {}

/// Local-only commands that the REPL handles without sending to the server.
const LOCAL_COMMANDS: &[&str] = &["HELP", "QUIT", "EXIT", "CLEAR"];

impl Completer for EmberHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];

        // check if we're completing a subcommand (second token)
        if let Some(space_idx) = prefix.find(' ') {
            let first = prefix[..space_idx].trim();
            if has_subcommands(first) {
                let rest = &prefix[space_idx + 1..];
                // don't complete if there's already a second space (third token)
                if !rest.contains(' ') {
                    let upper = rest.trim_start().to_uppercase();
                    let subs = subcommands(first);
                    let matches: Vec<Pair> = subs
                        .iter()
                        .filter(|s| s.starts_with(&upper))
                        .map(|s| Pair {
                            display: s.to_string(),
                            replacement: format!("{s} "),
                        })
                        .collect();
                    let start = space_idx + 1 + rest.len() - rest.trim_start().len();
                    return Ok((start, matches));
                }
            }
            return Ok((pos, vec![]));
        }

        // first token: complete command names + local commands
        let upper = prefix.to_uppercase();
        let mut matches: Vec<Pair> = command_names()
            .into_iter()
            .filter(|name| name.starts_with(&upper))
            .map(|name| Pair {
                display: name.to_string(),
                replacement: format!("{name} "),
            })
            .collect();

        // also complete local commands
        for &local in LOCAL_COMMANDS {
            if local.starts_with(&upper) {
                matches.push(Pair {
                    display: local.to_string(),
                    replacement: format!("{} ", local.to_lowercase()),
                });
            }
        }

        Ok((0, matches))
    }
}

impl Hinter for EmberHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        // only show hints when cursor is at the end of the line
        if pos != line.len() {
            return None;
        }

        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            return None;
        }

        // extract the first token
        let first_end = trimmed.find(' ').unwrap_or(trimmed.len());
        let first = &trimmed[..first_end];

        // if still typing the first token (no space yet), no hint
        if !trimmed.contains(' ') {
            return None;
        }

        // for multi-word commands, show subcommand list as hint
        if has_subcommands(first) {
            let after_space = &trimmed[first_end + 1..];
            // only hint if subcommand token is not started or partial
            if after_space.trim().is_empty() || !after_space.contains(' ') {
                let subs = subcommands(first);
                let upper = after_space.trim().to_uppercase();
                if upper.is_empty() {
                    return Some(format!(" {}", subs.join("|")));
                }
                // show matching subcommand's full name
                for &sub in subs {
                    if sub.starts_with(&upper) && sub != upper {
                        return Some(sub[upper.len()..].to_string());
                    }
                }
            }
            return None;
        }

        // show args hint for known commands
        if let Some(cmd) = find_command(first) {
            if !cmd.args.is_empty() {
                let after_space = &trimmed[first_end + 1..];
                // only show hint if user hasn't typed any arguments yet
                if after_space.trim().is_empty() {
                    return Some(cmd.args.to_string());
                }
            }
        }

        None
    }
}

impl Highlighter for EmberHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if line.is_empty() {
            return Cow::Borrowed(line);
        }

        let trimmed_start = line.len() - line.trim_start().len();
        let trimmed = &line[trimmed_start..];
        let first_end = trimmed.find(' ').unwrap_or(trimmed.len());
        let first = &trimmed[..first_end];
        let rest = &trimmed[first_end..];

        let is_known = find_command(first).is_some()
            || LOCAL_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(first));

        let highlighted_cmd = if is_known {
            format!("\x1b[1;36m{first}\x1b[0m") // bold cyan
        } else {
            format!("\x1b[31m{first}\x1b[0m") // red
        };

        // highlight quoted strings in rest
        let highlighted_rest = highlight_quotes(rest);

        Cow::Owned(format!(
            "{}{}{}",
            &line[..trimmed_start],
            highlighted_cmd,
            highlighted_rest,
        ))
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("\x1b[2m{hint}\x1b[0m")) // dim
    }

    fn highlight_char(
        &self,
        _line: &str,
        _pos: usize,
        _kind: rustyline::highlight::CmdKind,
    ) -> bool {
        true // re-highlight on every keystroke
    }
}

/// Highlights quoted strings in green within the argument portion.
fn highlight_quotes(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 32);
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                out.push_str("\x1b[32m\""); // green
                loop {
                    match chars.next() {
                        None => break,
                        Some('"') => {
                            out.push('"');
                            break;
                        }
                        Some('\\') => {
                            out.push('\\');
                            if let Some(esc) = chars.next() {
                                out.push(esc);
                            }
                        }
                        Some(c) => out.push(c),
                    }
                }
                out.push_str("\x1b[0m"); // reset
            }
            '\'' => {
                out.push_str("\x1b[32m'"); // green
                loop {
                    match chars.next() {
                        None => break,
                        Some('\'') => {
                            out.push('\'');
                            break;
                        }
                        Some(c) => out.push(c),
                    }
                }
                out.push_str("\x1b[0m"); // reset
            }
            _ => out.push(ch),
        }
    }

    out
}

impl Validator for EmberHelper {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("SET foo bar").unwrap(), vec!["SET", "foo", "bar"],);
    }

    #[test]
    fn tokenize_extra_whitespace() {
        assert_eq!(tokenize("  GET   key  ").unwrap(), vec!["GET", "key"],);
    }

    #[test]
    fn tokenize_double_quotes() {
        assert_eq!(
            tokenize(r#"SET key "hello world""#).unwrap(),
            vec!["SET", "key", "hello world"],
        );
    }

    #[test]
    fn tokenize_single_quotes() {
        assert_eq!(
            tokenize("SET key 'hello world'").unwrap(),
            vec!["SET", "key", "hello world"],
        );
    }

    #[test]
    fn tokenize_escaped_quote() {
        assert_eq!(
            tokenize(r#"SET key "say \"hi\"""#).unwrap(),
            vec!["SET", "key", r#"say "hi""#],
        );
    }

    #[test]
    fn tokenize_backslash_in_double_quotes() {
        assert_eq!(
            tokenize(r#"SET key "a\\b""#).unwrap(),
            vec!["SET", "key", r"a\b"],
        );
    }

    #[test]
    fn tokenize_single_quotes_no_escaping() {
        // single quotes are literal — no backslash processing
        assert_eq!(
            tokenize(r"SET key 'a\b'").unwrap(),
            vec!["SET", "key", r"a\b"],
        );
    }

    #[test]
    fn tokenize_empty_quoted_string() {
        assert_eq!(tokenize(r#"SET key """#).unwrap(), vec!["SET", "key", ""],);
    }

    #[test]
    fn tokenize_unmatched_double_quote() {
        assert!(tokenize(r#"SET key "hello"#).is_err());
    }

    #[test]
    fn tokenize_unmatched_single_quote() {
        assert!(tokenize("SET key 'hello").is_err());
    }

    #[test]
    fn tokenize_empty_input() {
        assert_eq!(tokenize("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn tokenize_whitespace_only() {
        assert_eq!(tokenize("   ").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn tokenize_adjacent_quoted_and_unquoted() {
        // "foo"bar should produce "foobar" — quotes don't force token boundaries
        assert_eq!(tokenize(r#""foo"bar"#).unwrap(), vec!["foobar"],);
    }

    #[test]
    fn tokenize_tabs() {
        assert_eq!(tokenize("GET\tkey").unwrap(), vec!["GET", "key"],);
    }

    // -- highlighter tests --

    #[test]
    fn highlight_known_command_bold_cyan() {
        let h = EmberHelper;
        let result = h.highlight("SET key val", 0);
        // known command should be bold cyan
        assert!(result.contains("\x1b[1;36m"));
        assert!(result.contains("SET"));
    }

    #[test]
    fn highlight_unknown_command_red() {
        let h = EmberHelper;
        let result = h.highlight("FOOBAR key", 0);
        assert!(result.contains("\x1b[31m"));
        assert!(result.contains("FOOBAR"));
    }

    #[test]
    fn highlight_local_command_bold_cyan() {
        let h = EmberHelper;
        let result = h.highlight("help SET", 0);
        assert!(result.contains("\x1b[1;36m"));
    }

    #[test]
    fn highlight_empty_line() {
        let h = EmberHelper;
        let result = h.highlight("", 0);
        assert_eq!(result.as_ref(), "");
    }

    #[test]
    fn highlight_quoted_strings_green() {
        let result = highlight_quotes(r#" "hello world""#);
        assert!(result.contains("\x1b[32m"));
    }

    // -- hinter tests --

    #[test]
    fn hint_shows_args_for_known_command() {
        let h = EmberHelper;
        let hint = h.hint(
            "SET ",
            4,
            &Context::new(&rustyline::history::DefaultHistory::new()),
        );
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert!(hint.contains("key"));
    }

    #[test]
    fn hint_shows_subcommands_for_cluster() {
        let h = EmberHelper;
        let hint = h.hint(
            "CLUSTER ",
            8,
            &Context::new(&rustyline::history::DefaultHistory::new()),
        );
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert!(hint.contains("INFO"));
    }

    #[test]
    fn hint_none_for_unknown_command() {
        let h = EmberHelper;
        let hint = h.hint(
            "FOOBAR ",
            7,
            &Context::new(&rustyline::history::DefaultHistory::new()),
        );
        assert!(hint.is_none());
    }

    #[test]
    fn hint_none_when_cursor_not_at_end() {
        let h = EmberHelper;
        let hint = h.hint(
            "SET key",
            3,
            &Context::new(&rustyline::history::DefaultHistory::new()),
        );
        assert!(hint.is_none());
    }
}
