//! Cluster management subcommands.
//!
//! Provides typed CLI subcommands for cluster operations. Individual variants
//! map to `CLUSTER <subcommand>` wire commands. The `Create` and `Check`
//! commands are multi-step orchestrations that connect to multiple nodes.

use std::process::ExitCode;

use clap::Subcommand;
use colored::Colorize;

use crate::connection::Connection;
use crate::format::format_response;
use crate::tls::TlsClientConfig;

/// Total number of hash slots in a Redis/Ember cluster.
const SLOT_COUNT: u16 = 16384;

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

    /// Create a new cluster from a list of nodes.
    ///
    /// Connects to each node, assigns slots evenly, and issues CLUSTER MEET
    /// to form the cluster. All nodes must be in cluster mode and have no
    /// slots assigned.
    Create {
        /// Node addresses in host:port format.
        #[arg(required = true, num_args = 1..)]
        nodes: Vec<String>,

        /// Number of replicas per primary (default: 0).
        #[arg(long, default_value_t = 0)]
        replicas: usize,
    },

    /// Check cluster health and slot coverage.
    ///
    /// Connects to a node, fetches CLUSTER NODES, and verifies that all
    /// 16384 slots are covered with no FAIL/PFAIL nodes.
    Check {
        /// Node address to query (default: 127.0.0.1:6379).
        #[arg(default_value = "127.0.0.1:6379")]
        node: String,
    },

    /// Move hash slots from one node to another.
    ///
    /// Migrates the specified number of slots (and their keys) from the
    /// source node to the target node using the standard import/migrate
    /// protocol.
    Reshard {
        /// Node address to connect to (default: 127.0.0.1:6379).
        #[arg(default_value = "127.0.0.1:6379")]
        node: String,

        /// Number of slots to move.
        #[arg(long)]
        slots: u16,

        /// Source node ID.
        #[arg(long)]
        from: String,

        /// Target node ID.
        #[arg(long)]
        to: String,
    },

    /// Rebalance slots across all primaries.
    ///
    /// Computes the ideal distribution (16384 / primaries) and moves
    /// slots from over-provisioned to under-provisioned nodes.
    Rebalance {
        /// Node address to connect to (default: 127.0.0.1:6379).
        #[arg(default_value = "127.0.0.1:6379")]
        node: String,
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
            Self::Create { nodes, replicas } => {
                if nodes.len() < 3 {
                    return Err("cluster create requires at least 3 nodes".into());
                }
                let min_nodes = 3 * (1 + replicas);
                if nodes.len() < min_nodes {
                    return Err(format!(
                        "not enough nodes for {replicas} replica(s): need at least {min_nodes}, got {}",
                        nodes.len()
                    ));
                }
                for addr in nodes {
                    parse_host_port(addr).map_err(|e| format!("invalid address '{addr}': {e}"))?;
                }
            }
            Self::Reshard {
                slots, from, to, ..
            } => {
                if *slots == 0 {
                    return Err("--slots must be at least 1".into());
                }
                if *slots > SLOT_COUNT {
                    return Err(format!("--slots cannot exceed {SLOT_COUNT}"));
                }
                if from == to {
                    return Err("--from and --to must be different nodes".into());
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Converts the typed command into RESP3 command tokens.
    ///
    /// Returns a vec like `["CLUSTER", "MEET", "10.0.0.1", "6379"]` that
    /// can be sent directly to the server.
    ///
    /// Returns `None` for orchestrated commands (Create, Check) that
    /// handle their own multi-step communication.
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
            // orchestrated commands don't use tokens
            Self::Create { .. }
            | Self::Check { .. }
            | Self::Reshard { .. }
            | Self::Rebalance { .. } => vec![],
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

    // orchestrated commands handle their own connections
    match cmd {
        ClusterCommand::Create { nodes, replicas } => {
            return rt.block_on(run_cluster_create(nodes, *replicas, password, tls));
        }
        ClusterCommand::Check { node } => {
            return rt.block_on(run_cluster_check(node, password, tls));
        }
        ClusterCommand::Reshard {
            node,
            slots,
            from,
            to,
        } => {
            return rt.block_on(run_cluster_reshard(node, *slots, from, to, password, tls));
        }
        ClusterCommand::Rebalance { node } => {
            return rt.block_on(run_cluster_rebalance(node, password, tls));
        }
        _ => {}
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

// ---------------------------------------------------------------------------
// cluster create
// ---------------------------------------------------------------------------

/// Orchestrates creating a new cluster from a list of node addresses.
///
/// Steps:
/// 1. Connect to each node and verify it's in cluster mode with no slots
/// 2. Get each node's ID via CLUSTER MYID
/// 3. Assign 16384 slots evenly across primary nodes
/// 4. CLUSTER MEET from node[0] to every other node
/// 5. Wait for all nodes to see each other (convergence)
/// 6. If replicas > 0, assign replicas to primaries
async fn run_cluster_create(
    addrs: &[String],
    replicas: usize,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let primary_count = addrs.len() / (1 + replicas);
    let replica_count = addrs.len() - primary_count;

    println!(">>> creating cluster with {primary_count} primaries and {replica_count} replicas");

    // connect to all nodes and collect their IDs
    let mut connections: Vec<(String, Connection, String)> = Vec::new(); // (addr, conn, node_id)

    for addr in addrs {
        let (host, port) = match parse_host_port(addr) {
            Ok(hp) => hp,
            Err(e) => {
                eprintln!("{}", format!("invalid address '{addr}': {e}").red());
                return ExitCode::FAILURE;
            }
        };

        let mut conn = match Connection::connect(&host, port, tls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{}", format!("could not connect to {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        };

        if let Some(pw) = password {
            if let Err(e) = conn.authenticate(pw).await {
                eprintln!("{}", format!("auth failed on {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        }

        // verify node is in cluster mode and has no slots assigned
        match conn.send_command_strs(&["CLUSTER", "INFO"]).await {
            Ok(frame) => {
                let info = frame_to_string(&frame);
                if !info.contains("cluster_state:") {
                    eprintln!(
                        "{}",
                        format!("{addr} does not appear to be in cluster mode").red()
                    );
                    return ExitCode::FAILURE;
                }
                // check for already assigned slots
                if let Some(assigned) = parse_cluster_info_field(&info, "cluster_slots_assigned") {
                    if assigned != "0" {
                        eprintln!(
                            "{}",
                            format!(
                                "{addr} already has {assigned} slots assigned — node must be empty"
                            )
                            .red()
                        );
                        return ExitCode::FAILURE;
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", format!("CLUSTER INFO failed on {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        }

        // get node ID
        let node_id = match conn.send_command_strs(&["CLUSTER", "MYID"]).await {
            Ok(frame) => frame_to_string(&frame),
            Err(e) => {
                eprintln!("{}", format!("CLUSTER MYID failed on {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        };

        println!("  {} {}", addr, node_id.dimmed());
        connections.push((addr.clone(), conn, node_id));
    }

    // assign slots evenly across primaries using ADDSLOTSRANGE
    let slots_per_primary = SLOT_COUNT / primary_count as u16;
    let remainder = SLOT_COUNT % primary_count as u16;

    println!(">>> assigning {SLOT_COUNT} slots across {primary_count} primaries");

    let mut cursor: u16 = 0;
    for (i, (addr, conn, _)) in connections.iter_mut().enumerate().take(primary_count) {
        let count = slots_per_primary + if (i as u16) < remainder { 1 } else { 0 };
        let start = cursor;
        let end = cursor + count - 1;
        cursor += count;
        let start_s = start.to_string();
        let end_s = end.to_string();
        match conn
            .send_command_strs(&["CLUSTER", "ADDSLOTSRANGE", &start_s, &end_s])
            .await
        {
            Ok(frame) if is_ok(&frame) => {
                println!("  {addr}: slots {start}-{end} ({count} slots)");
            }
            Ok(frame) => {
                eprintln!(
                    "{}",
                    format!(
                        "ADDSLOTSRANGE failed on {addr}: {}",
                        frame_to_string(&frame)
                    )
                    .red()
                );
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("{}", format!("ADDSLOTSRANGE failed on {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        }
    }

    // CLUSTER MEET from node[0] to every other node
    let (ref first_host, first_port) = match parse_host_port(&connections[0].0) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!(
                "{}",
                format!("bad address '{}': {e}", connections[0].0).red()
            );
            return ExitCode::FAILURE;
        }
    };

    println!(">>> sending CLUSTER MEET commands");

    for i in 1..connections.len() {
        let addr = connections[i].0.clone();
        let (host, port) = match parse_host_port(&addr) {
            Ok(hp) => hp,
            Err(e) => {
                eprintln!("{}", format!("bad address '{addr}': {e}").red());
                return ExitCode::FAILURE;
            }
        };
        let first_addr = connections[0].0.clone();

        match connections[0]
            .1
            .send_command_strs(&["CLUSTER", "MEET", &host, &port.to_string()])
            .await
        {
            Ok(frame) if is_ok(&frame) => {
                println!("  {first_addr} -> {addr}");
            }
            Ok(frame) => {
                eprintln!(
                    "{}",
                    format!(
                        "CLUSTER MEET failed for {addr}: {}",
                        frame_to_string(&frame)
                    )
                    .red()
                );
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("{}", format!("CLUSTER MEET failed for {addr}: {e}").red());
                return ExitCode::FAILURE;
            }
        }
    }

    // also meet from other nodes back to the first node to speed convergence
    for (_, conn, _) in connections.iter_mut().skip(1) {
        let _ = conn
            .send_command_strs(&["CLUSTER", "MEET", first_host, &first_port.to_string()])
            .await;
    }

    // wait for convergence: poll until all nodes see the expected count
    let expected_nodes = connections.len();
    println!(">>> waiting for cluster convergence ({expected_nodes} nodes)");

    for attempt in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let mut converged = true;
        for (addr, conn, _) in &mut connections {
            match conn.send_command_strs(&["CLUSTER", "INFO"]).await {
                Ok(frame) => {
                    let info = frame_to_string(&frame);
                    let known = parse_cluster_info_field(&info, "cluster_known_nodes")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    if known < expected_nodes {
                        converged = false;
                        if attempt % 5 == 4 {
                            println!("  {} sees {known}/{expected_nodes} nodes...", addr.dimmed());
                        }
                        break;
                    }
                }
                Err(_) => {
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            println!("  {} all {expected_nodes} nodes converged", "✓".green());
            break;
        }

        if attempt == 29 {
            eprintln!(
                "{}",
                "warning: convergence timed out after 30s — some nodes may not see all peers yet"
                    .yellow()
            );
        }
    }

    // assign replicas if requested
    if replicas > 0 && replica_count > 0 {
        println!(">>> assigning replicas");

        for (i, replica_idx) in (primary_count..connections.len()).enumerate() {
            let primary_idx = i % primary_count;
            let primary_id = connections[primary_idx].2.clone();
            let primary_addr = connections[primary_idx].0.clone();
            let replica_addr = connections[replica_idx].0.clone();

            match connections[replica_idx]
                .1
                .send_command_strs(&["CLUSTER", "REPLICATE", &primary_id])
                .await
            {
                Ok(frame) if is_ok(&frame) => {
                    println!(
                        "  {} -> replica of {} ({})",
                        replica_addr,
                        primary_addr,
                        truncate_id(&primary_id, 8)
                    );
                }
                Ok(frame) => {
                    eprintln!(
                        "{}",
                        format!(
                            "CLUSTER REPLICATE failed on {replica_addr}: {}",
                            frame_to_string(&frame)
                        )
                        .red()
                    );
                    return ExitCode::FAILURE;
                }
                Err(e) => {
                    eprintln!(
                        "{}",
                        format!("CLUSTER REPLICATE failed on {replica_addr}: {e}").red()
                    );
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    // clean up connections
    for (_, mut conn, _) in connections {
        conn.shutdown().await;
    }

    println!("{}", ">>> cluster created successfully".green());
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// cluster check
// ---------------------------------------------------------------------------

/// Connects to a node and checks cluster health.
///
/// Fetches CLUSTER NODES, verifies all 16384 slots are covered, and
/// reports any FAIL or PFAIL nodes.
async fn run_cluster_check(
    addr: &str,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let (host, port) = match parse_host_port(addr) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!("{}", format!("invalid address '{addr}': {e}").red());
            return ExitCode::FAILURE;
        }
    };

    let mut conn = match Connection::connect(&host, port, tls).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}", format!("could not connect to {addr}: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    if let Some(pw) = password {
        if let Err(e) = conn.authenticate(pw).await {
            eprintln!("{}", format!("auth failed: {e}").red());
            return ExitCode::FAILURE;
        }
    }

    // get cluster info
    let cluster_info = match conn.send_command_strs(&["CLUSTER", "INFO"]).await {
        Ok(frame) => frame_to_string(&frame),
        Err(e) => {
            eprintln!("{}", format!("CLUSTER INFO failed: {e}").red());
            conn.shutdown().await;
            return ExitCode::FAILURE;
        }
    };

    // get cluster nodes
    let nodes_output = match conn.send_command_strs(&["CLUSTER", "NODES"]).await {
        Ok(frame) => frame_to_string(&frame),
        Err(e) => {
            eprintln!("{}", format!("CLUSTER NODES failed: {e}").red());
            conn.shutdown().await;
            return ExitCode::FAILURE;
        }
    };

    conn.shutdown().await;

    // parse nodes
    let mut errors: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut slot_coverage = vec![false; SLOT_COUNT as usize];
    let mut node_count = 0;
    let mut primary_count = 0;
    let mut fail_nodes: Vec<String> = Vec::new();

    for line in nodes_output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }

        node_count += 1;
        let node_id = truncate_id(parts[0], 8);
        let addr_part = parts[1];
        let flags = parts[2];

        // check for failure flags
        if flags.contains("fail") && !flags.contains("pfail") {
            fail_nodes.push(format!("{node_id} ({addr_part})"));
            errors.push(format!("node {node_id} ({addr_part}) is in FAIL state"));
        } else if flags.contains("pfail") {
            warnings.push(format!(
                "node {node_id} ({addr_part}) is in PFAIL state (possible failure)"
            ));
        }

        // parse slots (fields 8+)
        let is_primary = flags.contains("master") || !flags.contains("slave");
        if is_primary {
            primary_count += 1;
        }

        for slot_spec in parts.iter().skip(8) {
            // slots can be: "0-5460" or "5461" or "[5461-<-importing_id]" etc
            if slot_spec.starts_with('[') {
                continue; // skip migration markers
            }

            if let Some((start_s, end_s)) = slot_spec.split_once('-') {
                if let (Ok(start), Ok(end)) = (start_s.parse::<u16>(), end_s.parse::<u16>()) {
                    for s in start..=end {
                        if (s as usize) < slot_coverage.len() {
                            slot_coverage[s as usize] = true;
                        }
                    }
                }
            } else if let Ok(s) = slot_spec.parse::<u16>() {
                if (s as usize) < slot_coverage.len() {
                    slot_coverage[s as usize] = true;
                }
            }
        }
    }

    // check slot coverage
    let covered = slot_coverage.iter().filter(|&&c| c).count();
    let uncovered = SLOT_COUNT as usize - covered;

    if uncovered > 0 {
        // find uncovered ranges for a readable message
        let ranges = find_uncovered_ranges(&slot_coverage);
        errors.push(format!(
            "{uncovered} slots not covered: {}",
            format_slot_ranges(&ranges)
        ));
    }

    // parse cluster state from CLUSTER INFO
    let state = parse_cluster_info_field(&cluster_info, "cluster_state")
        .unwrap_or_else(|| "unknown".into());

    // print report
    println!("{}", "=== cluster check ===".bold());
    println!("connected to: {addr}");
    println!(
        "cluster state: {}",
        if state == "ok" {
            state.green().to_string()
        } else {
            state.red().to_string()
        }
    );
    println!("nodes: {node_count} ({primary_count} primaries)");
    println!(
        "slot coverage: {covered}/{} ({:.1}%)",
        SLOT_COUNT,
        covered as f64 / SLOT_COUNT as f64 * 100.0
    );

    if !warnings.is_empty() {
        println!();
        for w in &warnings {
            println!("{} {w}", "[WARN]".yellow());
        }
    }

    if !errors.is_empty() {
        println!();
        for e in &errors {
            println!("{} {e}", "[ERR]".red());
        }
        return ExitCode::FAILURE;
    }

    if warnings.is_empty() {
        println!("\n{}", "all checks passed".green());
    }

    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// cluster reshard
// ---------------------------------------------------------------------------

/// Moves `slot_count` slots from one node to another.
///
/// For each slot: SETSLOT IMPORTING on target, SETSLOT MIGRATING on source,
/// loop GETKEYSINSLOT + MIGRATE per key, then SETSLOT NODE on all nodes.
async fn run_cluster_reshard(
    addr: &str,
    slot_count: u16,
    from_id: &str,
    to_id: &str,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let (host, port) = match parse_host_port(addr) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!("{}", format!("invalid address '{addr}': {e}").red());
            return ExitCode::FAILURE;
        }
    };

    let mut conn = match connect_and_auth(&host, port, password, tls).await {
        Ok(c) => c,
        Err(code) => return code,
    };

    // get cluster nodes to find addresses and slot ownership
    let nodes_output = match conn.send_command_strs(&["CLUSTER", "NODES"]).await {
        Ok(frame) => frame_to_string(&frame),
        Err(e) => {
            eprintln!("{}", format!("CLUSTER NODES failed: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    // parse nodes to find source, target, and their addresses
    let node_map = parse_nodes_output(&nodes_output);

    let source = match node_map.iter().find(|n| n.id.starts_with(from_id)) {
        Some(n) => n.clone(),
        None => {
            eprintln!(
                "{}",
                format!("source node '{from_id}' not found in cluster").red()
            );
            return ExitCode::FAILURE;
        }
    };

    let target = match node_map.iter().find(|n| n.id.starts_with(to_id)) {
        Some(n) => n.clone(),
        None => {
            eprintln!(
                "{}",
                format!("target node '{to_id}' not found in cluster").red()
            );
            return ExitCode::FAILURE;
        }
    };

    if source.slots.is_empty() {
        eprintln!("{}", "source node has no slots to move".red());
        return ExitCode::FAILURE;
    }

    let slots_to_move: Vec<u16> = source
        .slots
        .iter()
        .copied()
        .take(slot_count as usize)
        .collect();
    if slots_to_move.len() < slot_count as usize {
        eprintln!(
            "{}",
            format!(
                "source only has {} slots, requested {slot_count}",
                slots_to_move.len()
            )
            .yellow()
        );
    }

    println!(
        ">>> resharding {} slots from {} to {}",
        slots_to_move.len(),
        truncate_id(&source.id, 8),
        truncate_id(&target.id, 8)
    );

    // connect to source and target
    let (source_host, source_port) = match parse_host_port(&source.addr) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!(
                "{}",
                format!("invalid source address '{}': {e}", source.addr).red()
            );
            return ExitCode::FAILURE;
        }
    };
    let (target_host, target_port) = match parse_host_port(&target.addr) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!(
                "{}",
                format!("invalid target address '{}': {e}", target.addr).red()
            );
            return ExitCode::FAILURE;
        }
    };

    let mut source_conn = match connect_and_auth(&source_host, source_port, password, tls).await {
        Ok(c) => c,
        Err(code) => return code,
    };
    let mut target_conn = match connect_and_auth(&target_host, target_port, password, tls).await {
        Ok(c) => c,
        Err(code) => return code,
    };

    let mut moved = 0u16;
    for &slot in &slots_to_move {
        // 1. SETSLOT IMPORTING on target
        let r = target_conn
            .send_command_strs(&[
                "CLUSTER",
                "SETSLOT",
                &slot.to_string(),
                "IMPORTING",
                &source.id,
            ])
            .await;
        if !matches!(&r, Ok(f) if is_ok(f)) {
            eprintln!(
                "{}",
                format!(
                    "SETSLOT IMPORTING failed for slot {slot}: {}",
                    result_msg(&r)
                )
                .red()
            );
            break;
        }

        // 2. SETSLOT MIGRATING on source
        let r = source_conn
            .send_command_strs(&[
                "CLUSTER",
                "SETSLOT",
                &slot.to_string(),
                "MIGRATING",
                &target.id,
            ])
            .await;
        if !matches!(&r, Ok(f) if is_ok(f)) {
            eprintln!(
                "{}",
                format!(
                    "SETSLOT MIGRATING failed for slot {slot}: {}",
                    result_msg(&r)
                )
                .red()
            );
            break;
        }

        // 3. migrate keys
        loop {
            let keys_frame = match source_conn
                .send_command_strs(&["CLUSTER", "GETKEYSINSLOT", &slot.to_string(), "100"])
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!(
                        "{}",
                        format!("GETKEYSINSLOT failed for slot {slot}: {e}").red()
                    );
                    break;
                }
            };

            let keys = frame_to_string_list(&keys_frame);
            if keys.is_empty() {
                break;
            }

            for key in &keys {
                let r = source_conn
                    .send_command_strs(&[
                        "MIGRATE",
                        &target_host,
                        &target_port.to_string(),
                        key,
                        "0",
                        "5000",
                        "REPLACE",
                    ])
                    .await;
                if !matches!(&r, Ok(f) if is_ok(f)) {
                    eprintln!(
                        "{}",
                        format!(
                            "MIGRATE failed for key '{key}' in slot {slot}: {}",
                            result_msg(&r)
                        )
                        .red()
                    );
                    // continue with other keys
                }
            }
        }

        // 4. SETSLOT NODE on both source and target (and the node we're connected to)
        let slot_s = slot.to_string();
        for c in [&mut source_conn, &mut target_conn, &mut conn] {
            let _ = c
                .send_command_strs(&["CLUSTER", "SETSLOT", &slot_s, "NODE", &target.id])
                .await;
        }

        moved += 1;
        if moved.is_multiple_of(100) || moved == slots_to_move.len() as u16 {
            println!("  moved {moved}/{} slots", slots_to_move.len());
        }
    }

    source_conn.shutdown().await;
    target_conn.shutdown().await;
    conn.shutdown().await;

    if moved == slots_to_move.len() as u16 {
        println!(
            "{}",
            format!(">>> reshard complete: {moved} slots moved").green()
        );
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "{}",
            format!(
                "reshard incomplete: {moved}/{} slots moved",
                slots_to_move.len()
            )
            .yellow()
        );
        ExitCode::FAILURE
    }
}

// ---------------------------------------------------------------------------
// cluster rebalance
// ---------------------------------------------------------------------------

/// Rebalances slots across all primary nodes.
///
/// Computes the ideal distribution (16384 / primaries), then generates
/// migrations from over-provisioned to under-provisioned nodes.
async fn run_cluster_rebalance(
    addr: &str,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let (host, port) = match parse_host_port(addr) {
        Ok(hp) => hp,
        Err(e) => {
            eprintln!("{}", format!("invalid address '{addr}': {e}").red());
            return ExitCode::FAILURE;
        }
    };

    let mut conn = match connect_and_auth(&host, port, password, tls).await {
        Ok(c) => c,
        Err(code) => return code,
    };

    let nodes_output = match conn.send_command_strs(&["CLUSTER", "NODES"]).await {
        Ok(frame) => frame_to_string(&frame),
        Err(e) => {
            eprintln!("{}", format!("CLUSTER NODES failed: {e}").red());
            return ExitCode::FAILURE;
        }
    };
    conn.shutdown().await;

    let all_nodes = parse_nodes_output(&nodes_output);
    let primaries: Vec<&NodeInfo> = all_nodes.iter().filter(|n| n.is_primary).collect();

    if primaries.is_empty() {
        eprintln!("{}", "no primary nodes found".red());
        return ExitCode::FAILURE;
    }

    let ideal = SLOT_COUNT as usize / primaries.len();
    let remainder = SLOT_COUNT as usize % primaries.len();

    // print current distribution
    println!("{}", "=== current distribution ===".bold());
    for p in &primaries {
        let id_short = truncate_id(&p.id, 8);
        let count = p.slots.len();
        let diff = count as i64 - ideal as i64;
        let diff_str = if diff > 0 {
            format!("+{diff}").yellow().to_string()
        } else if diff < 0 {
            format!("{diff}").yellow().to_string()
        } else {
            "=".dimmed().to_string()
        };
        println!("  {id_short} ({}) : {} slots ({diff_str})", p.addr, count);
    }
    println!("  ideal: {ideal} slots per primary (+{remainder} remainder)");

    // compute migrations: over-provisioned give to under-provisioned
    let mut donors: Vec<(&NodeInfo, Vec<u16>)> = Vec::new();
    let mut receivers: Vec<(&NodeInfo, usize)> = Vec::new();

    for (i, p) in primaries.iter().enumerate() {
        let target_count = ideal + if i < remainder { 1 } else { 0 };
        if p.slots.len() > target_count {
            let excess: Vec<u16> = p.slots[target_count..].to_vec();
            donors.push((p, excess));
        } else if p.slots.len() < target_count {
            let deficit = target_count - p.slots.len();
            receivers.push((p, deficit));
        }
    }

    if donors.is_empty() && receivers.is_empty() {
        println!("\n{}", "cluster is already balanced".green());
        return ExitCode::SUCCESS;
    }

    // generate migration plan
    let mut plan: Vec<(String, String, u16)> = Vec::new(); // (from_id, to_id, slot)
    let mut donor_idx = 0;
    let mut donor_slot_idx = 0;

    for (receiver, deficit) in &receivers {
        for _ in 0..*deficit {
            // find next available donor slot
            while donor_idx < donors.len() && donor_slot_idx >= donors[donor_idx].1.len() {
                donor_idx += 1;
                donor_slot_idx = 0;
            }
            if donor_idx >= donors.len() {
                break;
            }

            let slot = donors[donor_idx].1[donor_slot_idx];
            plan.push((donors[donor_idx].0.id.clone(), receiver.id.clone(), slot));
            donor_slot_idx += 1;
        }
    }

    println!("\n>>> rebalancing: {} slot migrations planned", plan.len());

    if plan.is_empty() {
        println!("{}", "nothing to do".green());
        return ExitCode::SUCCESS;
    }

    // execute migrations by grouping by (from, to) pair
    let mut grouped: std::collections::BTreeMap<(String, String), Vec<u16>> =
        std::collections::BTreeMap::new();
    for (from, to, slot) in &plan {
        grouped
            .entry((from.clone(), to.clone()))
            .or_default()
            .push(*slot);
    }

    for ((from_id, to_id), slots) in &grouped {
        let from_short = truncate_id(from_id, 8);
        let to_short = truncate_id(to_id, 8);
        println!("  {from_short} -> {to_short}: {} slots", slots.len());

        // execute as a reshard operation
        let result =
            run_cluster_reshard(addr, slots.len() as u16, from_id, to_id, password, tls).await;
        if result != ExitCode::SUCCESS {
            return result;
        }
    }

    // print final distribution
    println!("\n{}", "=== rebalance complete ===".green());

    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Connects to a node and authenticates. Returns the connection or an error exit code.
async fn connect_and_auth(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<Connection, ExitCode> {
    let mut conn = Connection::connect(host, port, tls).await.map_err(|e| {
        eprintln!(
            "{}",
            format!("could not connect to {host}:{port}: {e}").red()
        );
        ExitCode::FAILURE
    })?;

    if let Some(pw) = password {
        conn.authenticate(pw).await.map_err(|e| {
            eprintln!("{}", format!("auth failed on {host}:{port}: {e}").red());
            ExitCode::FAILURE
        })?;
    }

    Ok(conn)
}

/// Parsed node info from CLUSTER NODES output.
#[derive(Clone, Debug)]
struct NodeInfo {
    id: String,
    addr: String,
    is_primary: bool,
    slots: Vec<u16>,
}

/// Parses CLUSTER NODES output into a list of NodeInfo.
fn parse_nodes_output(output: &str) -> Vec<NodeInfo> {
    let mut nodes = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }

        let id = parts[0].to_string();
        // address format: host:port@gossip_port or host:port
        let addr = parts[1].split('@').next().unwrap_or(parts[1]).to_string();
        let flags = parts[2];
        let is_primary = flags.contains("master") || !flags.contains("slave");

        let mut slots = Vec::new();
        for slot_spec in parts.iter().skip(8) {
            if slot_spec.starts_with('[') {
                continue;
            }
            if let Some((start_s, end_s)) = slot_spec.split_once('-') {
                if let (Ok(start), Ok(end)) = (start_s.parse::<u16>(), end_s.parse::<u16>()) {
                    for s in start..=end {
                        slots.push(s);
                    }
                }
            } else if let Ok(s) = slot_spec.parse::<u16>() {
                slots.push(s);
            }
        }

        nodes.push(NodeInfo {
            id,
            addr,
            is_primary,
            slots,
        });
    }

    nodes
}

/// Extracts a list of strings from a Frame::Array response.
fn frame_to_string_list(frame: &ember_protocol::types::Frame) -> Vec<String> {
    match frame {
        ember_protocol::types::Frame::Array(items) => items
            .iter()
            .filter_map(|f| match f {
                ember_protocol::types::Frame::Bulk(b) => {
                    Some(String::from_utf8_lossy(b).into_owned())
                }
                ember_protocol::types::Frame::Simple(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Extracts a human-readable message from a connection result.
fn result_msg(
    r: &Result<ember_protocol::types::Frame, crate::connection::ConnectionError>,
) -> String {
    match r {
        Ok(f) => frame_to_string(f),
        Err(e) => e.to_string(),
    }
}

/// Truncates a string to at most `max_len` bytes on a char boundary.
///
/// Node IDs are always hex UUIDs (ASCII), but this function is safe
/// for any UTF-8 string — it will never split a multi-byte character.
fn truncate_id(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        return s;
    }
    // find the largest char boundary <= max_len
    let mut end = max_len;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Parses a "host:port" string into (host, port).
fn parse_host_port(addr: &str) -> Result<(String, u16), String> {
    // handle IPv6 addresses like [::1]:6379
    if let Some(bracket_end) = addr.find("]:") {
        let host = &addr[..bracket_end + 1];
        let port_str = &addr[bracket_end + 2..];
        let port: u16 = port_str
            .parse()
            .map_err(|_| format!("invalid port '{port_str}'"))?;
        return Ok((host.to_string(), port));
    }

    let (host, port_str) = addr
        .rsplit_once(':')
        .ok_or_else(|| "expected host:port format".to_string())?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| format!("invalid port '{port_str}'"))?;
    Ok((host.to_string(), port))
}

/// Extracts the string content from a Frame (Bulk or Simple).
fn frame_to_string(frame: &ember_protocol::types::Frame) -> String {
    match frame {
        ember_protocol::types::Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        ember_protocol::types::Frame::Simple(s) => s.clone(),
        ember_protocol::types::Frame::Error(e) => format!("ERR {e}"),
        ember_protocol::types::Frame::Integer(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Returns true if the frame is a Simple "OK" response.
fn is_ok(frame: &ember_protocol::types::Frame) -> bool {
    matches!(frame, ember_protocol::types::Frame::Simple(s) if s == "OK")
}

/// Parses a field value from CLUSTER INFO output.
///
/// CLUSTER INFO returns lines like "cluster_state:ok\r\n".
fn parse_cluster_info_field(info: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}:");
    for line in info.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix(&prefix) {
            return Some(value.trim().to_string());
        }
    }
    None
}

/// Finds ranges of uncovered slots for readable error messages.
fn find_uncovered_ranges(coverage: &[bool]) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut start: Option<u16> = None;

    for (i, &covered) in coverage.iter().enumerate() {
        if !covered {
            if start.is_none() {
                start = Some(i as u16);
            }
        } else if let Some(s) = start {
            ranges.push((s, i as u16 - 1));
            start = None;
        }
    }
    if let Some(s) = start {
        ranges.push((s, coverage.len() as u16 - 1));
    }
    ranges
}

/// Formats slot ranges for display (e.g. "0-5460, 10923-16383").
fn format_slot_ranges(ranges: &[(u16, u16)]) -> String {
    let strs: Vec<String> = ranges
        .iter()
        .map(|(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect();
    strs.join(", ")
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

    // --- new tests for create/check helpers ---

    #[test]
    fn parse_host_port_simple() {
        assert_eq!(
            parse_host_port("127.0.0.1:7001").unwrap(),
            ("127.0.0.1".into(), 7001)
        );
    }

    #[test]
    fn parse_host_port_hostname() {
        assert_eq!(
            parse_host_port("node1.example.com:6379").unwrap(),
            ("node1.example.com".into(), 6379)
        );
    }

    #[test]
    fn parse_host_port_invalid() {
        assert!(parse_host_port("no-port").is_err());
        assert!(parse_host_port("host:abc").is_err());
    }

    #[test]
    fn parse_cluster_info_field_found() {
        let info = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:3\r\n";
        assert_eq!(
            parse_cluster_info_field(info, "cluster_state"),
            Some("ok".into())
        );
        assert_eq!(
            parse_cluster_info_field(info, "cluster_slots_assigned"),
            Some("16384".into())
        );
        assert_eq!(
            parse_cluster_info_field(info, "cluster_known_nodes"),
            Some("3".into())
        );
    }

    #[test]
    fn parse_cluster_info_field_missing() {
        let info = "cluster_state:ok\r\n";
        assert_eq!(parse_cluster_info_field(info, "nonexistent"), None);
    }

    #[test]
    fn find_uncovered_ranges_all_covered() {
        let coverage = vec![true; SLOT_COUNT as usize];
        assert!(find_uncovered_ranges(&coverage).is_empty());
    }

    #[test]
    fn find_uncovered_ranges_none_covered() {
        let coverage = vec![false; 10];
        assert_eq!(find_uncovered_ranges(&coverage), vec![(0, 9)]);
    }

    #[test]
    fn find_uncovered_ranges_gap_in_middle() {
        let mut coverage = vec![true; 10];
        coverage[3] = false;
        coverage[4] = false;
        assert_eq!(find_uncovered_ranges(&coverage), vec![(3, 4)]);
    }

    #[test]
    fn format_slot_ranges_display() {
        assert_eq!(format_slot_ranges(&[(0, 5460)]), "0-5460");
        assert_eq!(
            format_slot_ranges(&[(0, 5460), (10923, 16383)]),
            "0-5460, 10923-16383"
        );
        assert_eq!(format_slot_ranges(&[(42, 42)]), "42");
    }

    #[test]
    fn create_validates_minimum_nodes() {
        let cmd = ClusterCommand::Create {
            nodes: vec!["a:1".into(), "b:2".into()],
            replicas: 0,
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn create_validates_replica_count() {
        let cmd = ClusterCommand::Create {
            nodes: vec!["a:1".into(), "b:2".into(), "c:3".into()],
            replicas: 1,
        };
        // 3 nodes with 1 replica requires 6 nodes minimum
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn create_validates_addresses() {
        let cmd = ClusterCommand::Create {
            nodes: vec!["a:1".into(), "b:2".into(), "no-port".into()],
            replicas: 0,
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn create_valid_three_nodes() {
        let cmd = ClusterCommand::Create {
            nodes: vec!["a:1".into(), "b:2".into(), "c:3".into()],
            replicas: 0,
        };
        assert!(cmd.validate().is_ok());
    }

    #[test]
    fn reshard_validates_zero_slots() {
        let cmd = ClusterCommand::Reshard {
            node: "127.0.0.1:6379".into(),
            slots: 0,
            from: "aaa".into(),
            to: "bbb".into(),
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn reshard_validates_same_node() {
        let cmd = ClusterCommand::Reshard {
            node: "127.0.0.1:6379".into(),
            slots: 100,
            from: "aaa".into(),
            to: "aaa".into(),
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn reshard_validates_too_many_slots() {
        let cmd = ClusterCommand::Reshard {
            node: "127.0.0.1:6379".into(),
            slots: 16385,
            from: "aaa".into(),
            to: "bbb".into(),
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn reshard_valid() {
        let cmd = ClusterCommand::Reshard {
            node: "127.0.0.1:6379".into(),
            slots: 100,
            from: "aaa".into(),
            to: "bbb".into(),
        };
        assert!(cmd.validate().is_ok());
    }

    #[test]
    fn parse_nodes_output_basic() {
        let output = "\
abc123def456 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
bbb222ccc333 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922\n\
ddd444eee555 127.0.0.1:7003@17003 slave bbb222ccc333 0 0 2 connected\n";

        let nodes = parse_nodes_output(output);
        assert_eq!(nodes.len(), 3);

        assert!(nodes[0].is_primary);
        assert_eq!(nodes[0].addr, "127.0.0.1:7001");
        assert_eq!(nodes[0].slots.len(), 5461); // 0-5460

        assert!(nodes[1].is_primary);
        assert_eq!(nodes[1].slots.len(), 5462); // 5461-10922

        assert!(!nodes[2].is_primary);
        assert!(nodes[2].slots.is_empty());
    }

    #[test]
    fn parse_nodes_output_empty() {
        assert!(parse_nodes_output("").is_empty());
        assert!(parse_nodes_output("  \n  \n").is_empty());
    }

    #[test]
    fn frame_to_string_list_extracts_bulk() {
        use ember_protocol::types::Frame;

        let frame = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from("key1")),
            Frame::Bulk(bytes::Bytes::from("key2")),
        ]);
        assert_eq!(frame_to_string_list(&frame), vec!["key1", "key2"]);
    }

    #[test]
    fn frame_to_string_list_empty_array() {
        use ember_protocol::types::Frame;
        assert!(frame_to_string_list(&Frame::Array(vec![])).is_empty());
    }

    #[test]
    fn frame_to_string_list_non_array() {
        use ember_protocol::types::Frame;
        assert!(frame_to_string_list(&Frame::Simple("OK".into())).is_empty());
    }

    #[test]
    fn reshard_and_rebalance_return_empty_tokens() {
        let reshard = ClusterCommand::Reshard {
            node: "a:1".into(),
            slots: 10,
            from: "x".into(),
            to: "y".into(),
        };
        assert!(reshard.to_tokens().is_empty());

        let rebalance = ClusterCommand::Rebalance { node: "a:1".into() };
        assert!(rebalance.to_tokens().is_empty());
    }

    #[test]
    fn truncate_id_short_string() {
        assert_eq!(truncate_id("abc", 8), "abc");
    }

    #[test]
    fn truncate_id_exact_length() {
        assert_eq!(truncate_id("abcdefgh", 8), "abcdefgh");
    }

    #[test]
    fn truncate_id_long_string() {
        assert_eq!(truncate_id("abcdefghijklmnop", 8), "abcdefgh");
    }

    #[test]
    fn truncate_id_multibyte_boundary() {
        // 2-byte char "é" straddles the boundary — should not panic
        let s = "abcdefé"; // 'é' is 2 bytes at position 6-7
        let t = truncate_id(s, 7);
        assert_eq!(t, "abcdef"); // truncates before the multi-byte char
    }
}
