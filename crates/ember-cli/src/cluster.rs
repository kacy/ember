//! Cluster management subcommands.
//!
//! Provides typed CLI subcommands for cluster operations. Each variant
//! maps to a `CLUSTER <subcommand>` wire command that gets sent to the
//! server as a RESP3 array.

use std::process::ExitCode;

use clap::Subcommand;
use colored::Colorize;

use crate::connection::Connection;
use crate::format::format_response;
use crate::tls::TlsClientConfig;

/// Cluster management actions.
#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Show cluster info (state, slots, known nodes).
    Info,

    /// List all known nodes in the cluster.
    Nodes,

    /// Show slot-to-node mapping.
    Slots,

    /// Return the hash slot for a given key.
    Keyslot {
        /// The key to hash.
        key: String,
    },

    /// Return this node's unique ID.
    Myid,

    /// Introduce a new node to the cluster.
    Meet {
        /// IP address of the node to meet.
        ip: String,
        /// Port of the node to meet.
        port: u16,
    },

    /// Remove a node from the cluster.
    Forget {
        /// Node ID to remove.
        node_id: String,
    },

    /// Assign hash slots to this node.
    Addslots {
        /// Slot numbers to assign.
        #[arg(required = true, num_args = 1..)]
        slots: Vec<u16>,
    },

    /// Assign a contiguous range of hash slots to this node.
    Addslotsrange {
        /// First slot in the range (inclusive).
        start: u16,
        /// Last slot in the range (inclusive).
        end: u16,
    },

    /// Remove hash slots from this node.
    Delslots {
        /// Slot numbers to remove.
        #[arg(required = true, num_args = 1..)]
        slots: Vec<u16>,
    },

    /// Configure a specific hash slot.
    Setslot {
        /// The slot number to configure.
        slot: u16,

        /// The action to take on the slot.
        #[command(subcommand)]
        action: SetslotAction,
    },

    /// Make this node a replica of the given node.
    Replicate {
        /// Node ID to replicate.
        node_id: String,
    },

    /// Trigger a manual failover.
    Failover {
        /// Force failover without agreement from the master.
        #[arg(long)]
        force: bool,

        /// Take over without waiting for the cluster to agree.
        #[arg(long)]
        takeover: bool,
    },

    /// Return the number of keys in a hash slot.
    Countkeysinslot {
        /// The slot number to count.
        slot: u16,
    },

    /// Return keys in a hash slot.
    Getkeysinslot {
        /// The slot number to query.
        slot: u16,
        /// Maximum number of keys to return.
        count: u32,
    },
}

/// Actions for the `CLUSTER SETSLOT` command.
#[derive(Debug, Subcommand)]
pub enum SetslotAction {
    /// Mark slot as importing from another node.
    Importing {
        /// Source node ID.
        node_id: String,
    },
    /// Mark slot as migrating to another node.
    Migrating {
        /// Destination node ID.
        node_id: String,
    },
    /// Assign slot to a specific node.
    Node {
        /// Target node ID.
        node_id: String,
    },
    /// Clear any importing/migrating state for the slot.
    Stable,
}

/// Maximum valid hash slot index.
const MAX_SLOT: u16 = 16383;

impl ClusterCommand {
    /// Validates command arguments before sending to the server.
    ///
    /// Catches obvious input errors (slot numbers ≥ 16384) early so the CLI
    /// can show a clear message instead of a server-side ERR response.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Addslots { slots } | Self::Delslots { slots } => {
                for &slot in slots {
                    if slot > MAX_SLOT {
                        return Err(format!(
                            "invalid slot {slot}: hash slots must be in the range 0-{MAX_SLOT}"
                        ));
                    }
                }
            }
            Self::Addslotsrange { start, end } => {
                for (label, &slot) in [("start", start), ("end", end)] {
                    if slot > MAX_SLOT {
                        return Err(format!(
                            "invalid {label} slot {slot}: hash slots must be in the range 0-{MAX_SLOT}"
                        ));
                    }
                }
                if start > end {
                    return Err(format!("start slot {start} must not exceed end slot {end}"));
                }
            }
            Self::Setslot { slot, .. } if *slot > MAX_SLOT => {
                return Err(format!(
                    "invalid slot {slot}: hash slots must be in the range 0-{MAX_SLOT}"
                ));
            }
            _ => {}
        }
        Ok(())
    }

    /// Converts the typed command into RESP3 command tokens.
    ///
    /// Returns a vec like `["CLUSTER", "MEET", "10.0.0.1", "6379"]` that
    /// can be sent directly to the server.
    pub fn to_tokens(&self) -> Vec<String> {
        match self {
            Self::Info => vec!["CLUSTER".into(), "INFO".into()],
            Self::Nodes => vec!["CLUSTER".into(), "NODES".into()],
            Self::Slots => vec!["CLUSTER".into(), "SLOTS".into()],
            Self::Keyslot { key } => vec!["CLUSTER".into(), "KEYSLOT".into(), key.clone()],
            Self::Myid => vec!["CLUSTER".into(), "MYID".into()],
            Self::Meet { ip, port } => {
                vec![
                    "CLUSTER".into(),
                    "MEET".into(),
                    ip.clone(),
                    port.to_string(),
                ]
            }
            Self::Forget { node_id } => {
                vec!["CLUSTER".into(), "FORGET".into(), node_id.clone()]
            }
            Self::Addslots { slots } => {
                let mut tokens = vec!["CLUSTER".into(), "ADDSLOTS".into()];
                tokens.extend(slots.iter().map(|s| s.to_string()));
                tokens
            }
            Self::Addslotsrange { start, end } => vec![
                "CLUSTER".into(),
                "ADDSLOTSRANGE".into(),
                start.to_string(),
                end.to_string(),
            ],
            Self::Delslots { slots } => {
                let mut tokens = vec!["CLUSTER".into(), "DELSLOTS".into()];
                tokens.extend(slots.iter().map(|s| s.to_string()));
                tokens
            }
            Self::Setslot { slot, action } => {
                let mut tokens = vec!["CLUSTER".into(), "SETSLOT".into(), slot.to_string()];
                match action {
                    SetslotAction::Importing { node_id } => {
                        tokens.push("IMPORTING".into());
                        tokens.push(node_id.clone());
                    }
                    SetslotAction::Migrating { node_id } => {
                        tokens.push("MIGRATING".into());
                        tokens.push(node_id.clone());
                    }
                    SetslotAction::Node { node_id } => {
                        tokens.push("NODE".into());
                        tokens.push(node_id.clone());
                    }
                    SetslotAction::Stable => {
                        tokens.push("STABLE".into());
                    }
                }
                tokens
            }
            Self::Replicate { node_id } => {
                vec!["CLUSTER".into(), "REPLICATE".into(), node_id.clone()]
            }
            Self::Failover { force, takeover } => {
                let mut tokens = vec!["CLUSTER".into(), "FAILOVER".into()];
                if *force {
                    tokens.push("FORCE".into());
                }
                if *takeover {
                    tokens.push("TAKEOVER".into());
                }
                tokens
            }
            Self::Countkeysinslot { slot } => {
                vec!["CLUSTER".into(), "COUNTKEYSINSLOT".into(), slot.to_string()]
            }
            Self::Getkeysinslot { slot, count } => vec![
                "CLUSTER".into(),
                "GETKEYSINSLOT".into(),
                slot.to_string(),
                count.to_string(),
            ],
        }
    }
}

/// Connects to the server, sends a cluster command, and prints the response.
pub fn run_cluster(
    cmd: &ClusterCommand,
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{}", format!("failed to create runtime: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    if let Err(msg) = cmd.validate() {
        eprintln!("{}", format!("error: {msg}").red());
        return ExitCode::FAILURE;
    }

    rt.block_on(async {
        let mut conn = match Connection::connect(host, port, tls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "{}",
                    format!("could not connect to {host}:{port}: {e}").red()
                );
                return ExitCode::FAILURE;
            }
        };

        if let Some(pw) = password {
            if let Err(e) = conn.authenticate(pw).await {
                eprintln!("{}", format!("authentication failed: {e}").red());
                conn.shutdown().await;
                return ExitCode::FAILURE;
            }
        }

        let tokens = cmd.to_tokens();
        let exit_code = match conn.send_command(&tokens).await {
            Ok(frame) => {
                println!("{}", format_response(&frame));
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{}", format!("error: {e}").red());
                ExitCode::FAILURE
            }
        };

        conn.shutdown().await;
        exit_code
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_tokens() {
        let cmd = ClusterCommand::Info;
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "INFO"]);
    }

    #[test]
    fn nodes_tokens() {
        let cmd = ClusterCommand::Nodes;
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "NODES"]);
    }

    #[test]
    fn slots_tokens() {
        let cmd = ClusterCommand::Slots;
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "SLOTS"]);
    }

    #[test]
    fn keyslot_tokens() {
        let cmd = ClusterCommand::Keyslot {
            key: "mykey".into(),
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "KEYSLOT", "mykey"]);
    }

    #[test]
    fn myid_tokens() {
        let cmd = ClusterCommand::Myid;
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "MYID"]);
    }

    #[test]
    fn meet_tokens() {
        let cmd = ClusterCommand::Meet {
            ip: "10.0.0.1".into(),
            port: 6379,
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "MEET", "10.0.0.1", "6379"]);
    }

    #[test]
    fn forget_tokens() {
        let cmd = ClusterCommand::Forget {
            node_id: "abc123".into(),
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "FORGET", "abc123"]);
    }

    #[test]
    fn addslots_tokens() {
        let cmd = ClusterCommand::Addslots {
            slots: vec![0, 1, 2],
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "ADDSLOTS", "0", "1", "2"]);
    }

    #[test]
    fn addslotsrange_tokens() {
        let cmd = ClusterCommand::Addslotsrange {
            start: 0,
            end: 5460,
        };
        assert_eq!(
            cmd.to_tokens(),
            vec!["CLUSTER", "ADDSLOTSRANGE", "0", "5460"]
        );
    }

    #[test]
    fn delslots_tokens() {
        let cmd = ClusterCommand::Delslots {
            slots: vec![100, 200],
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "DELSLOTS", "100", "200"]);
    }

    #[test]
    fn setslot_importing_tokens() {
        let cmd = ClusterCommand::Setslot {
            slot: 42,
            action: SetslotAction::Importing {
                node_id: "node-a".into(),
            },
        };
        assert_eq!(
            cmd.to_tokens(),
            vec!["CLUSTER", "SETSLOT", "42", "IMPORTING", "node-a"]
        );
    }

    #[test]
    fn setslot_migrating_tokens() {
        let cmd = ClusterCommand::Setslot {
            slot: 42,
            action: SetslotAction::Migrating {
                node_id: "node-b".into(),
            },
        };
        assert_eq!(
            cmd.to_tokens(),
            vec!["CLUSTER", "SETSLOT", "42", "MIGRATING", "node-b"]
        );
    }

    #[test]
    fn setslot_node_tokens() {
        let cmd = ClusterCommand::Setslot {
            slot: 42,
            action: SetslotAction::Node {
                node_id: "node-c".into(),
            },
        };
        assert_eq!(
            cmd.to_tokens(),
            vec!["CLUSTER", "SETSLOT", "42", "NODE", "node-c"]
        );
    }

    #[test]
    fn setslot_stable_tokens() {
        let cmd = ClusterCommand::Setslot {
            slot: 42,
            action: SetslotAction::Stable,
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "SETSLOT", "42", "STABLE"]);
    }

    #[test]
    fn replicate_tokens() {
        let cmd = ClusterCommand::Replicate {
            node_id: "master-1".into(),
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "REPLICATE", "master-1"]);
    }

    #[test]
    fn failover_no_flags_tokens() {
        let cmd = ClusterCommand::Failover {
            force: false,
            takeover: false,
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "FAILOVER"]);
    }

    #[test]
    fn failover_force_tokens() {
        let cmd = ClusterCommand::Failover {
            force: true,
            takeover: false,
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "FAILOVER", "FORCE"]);
    }

    #[test]
    fn failover_takeover_tokens() {
        let cmd = ClusterCommand::Failover {
            force: false,
            takeover: true,
        };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "FAILOVER", "TAKEOVER"]);
    }

    #[test]
    fn countkeysinslot_tokens() {
        let cmd = ClusterCommand::Countkeysinslot { slot: 999 };
        assert_eq!(cmd.to_tokens(), vec!["CLUSTER", "COUNTKEYSINSLOT", "999"]);
    }

    #[test]
    fn getkeysinslot_tokens() {
        let cmd = ClusterCommand::Getkeysinslot {
            slot: 500,
            count: 10,
        };
        assert_eq!(
            cmd.to_tokens(),
            vec!["CLUSTER", "GETKEYSINSLOT", "500", "10"]
        );
    }
}
