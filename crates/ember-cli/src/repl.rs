//! Interactive REPL for ember.
//!
//! Uses rustyline for readline editing, history, and tab-completion.
//! Commands are tokenized and sent as raw RESP3 arrays — no client-side
//! validation. The server handles everything.

use std::borrow::Cow;
use std::path::PathBuf;

use colored::Colorize;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context, Editor, Helper};

use crate::commands::{command_names, commands_by_group, find_command};
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

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

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
            return;
        }
    }

    // set up rustyline
    let config = Config::builder()
        .completion_type(CompletionType::List)
        .build();

    let mut rl = Editor::with_config(config).expect("failed to create editor");
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
                        match rt.block_on(Connection::connect(host, port)) {
                            Ok(mut new_conn) => {
                                // re-authenticate if needed
                                if let Some(pw) = password {
                                    if let Err(e) = rt.block_on(new_conn.authenticate(pw)) {
                                        eprintln!(
                                            "{}",
                                            format!("re-authentication failed: {e}").red()
                                        );
                                        break;
                                    }
                                }
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

/// Returns the path to the history file, creating the parent directory if needed.
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

impl Completer for EmberHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // only complete the first token (command name)
        let prefix = &line[..pos];
        if prefix.contains(' ') {
            return Ok((pos, vec![]));
        }

        let upper = prefix.to_uppercase();
        let matches: Vec<Pair> = command_names()
            .into_iter()
            .filter(|name| name.starts_with(&upper))
            .map(|name| Pair {
                display: name.to_string(),
                replacement: format!("{name} "),
            })
            .collect();

        Ok((0, matches))
    }
}

impl Hinter for EmberHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Highlighter for EmberHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }
}

impl Validator for EmberHelper {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        assert_eq!(
            tokenize("SET foo bar").unwrap(),
            vec!["SET", "foo", "bar"],
        );
    }

    #[test]
    fn tokenize_extra_whitespace() {
        assert_eq!(
            tokenize("  GET   key  ").unwrap(),
            vec!["GET", "key"],
        );
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
        assert_eq!(
            tokenize(r#"SET key """#).unwrap(),
            vec!["SET", "key", ""],
        );
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
        assert_eq!(
            tokenize(r#""foo"bar"#).unwrap(),
            vec!["foobar"],
        );
    }

    #[test]
    fn tokenize_tabs() {
        assert_eq!(
            tokenize("GET\tkey").unwrap(),
            vec!["GET", "key"],
        );
    }
}
