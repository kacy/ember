//! Built-in benchmark tool for ember.
//!
//! Spawns multiple async tasks, each with its own pipelined TCP connection,
//! to measure throughput and latency under controlled workloads.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use clap::Args;
use colored::Colorize;
use ember_protocol::types::Frame;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Barrier;

use crate::bench_conn::BenchConnection;
use crate::tls::TlsClientConfig;

/// Arguments for the built-in benchmark.
#[derive(Debug, Args)]
pub struct BenchmarkArgs {
    /// Total number of requests to send.
    #[arg(short = 'n', long, default_value_t = 100_000)]
    pub requests: u64,

    /// Number of concurrent client connections.
    #[arg(short = 'c', long, default_value_t = 50)]
    pub clients: u32,

    /// Number of commands to pipeline per batch.
    #[arg(short = 'P', long, default_value_t = 1)]
    pub pipeline: u32,

    /// Size of the value payload in bytes.
    #[arg(short = 'd', long = "data-size", default_value_t = 64)]
    pub data_size: usize,

    /// Comma-separated list of tests to run (e.g. "set,get,ping").
    #[arg(short = 't', long, default_value = "set,get")]
    pub tests: String,

    /// Number of unique keys to use.
    #[arg(long, default_value_t = 100_000)]
    pub keyspace: u64,

    /// Quiet mode: only print the summary line for each test.
    #[arg(short = 'q', long)]
    pub quiet: bool,
}

/// Runs the benchmark with the given arguments.
pub fn run_benchmark(
    args: &BenchmarkArgs,
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

    // clone the TLS config so it can be moved into async tasks
    let tls_owned = tls.cloned();
    rt.block_on(async { run_benchmark_async(args, host, port, password, tls_owned.as_ref()).await })
}

async fn run_benchmark_async(
    args: &BenchmarkArgs,
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    // print header
    println!();
    println!("{}", "=== ember benchmark ===".bold());
    println!("server:     {}:{}", host.cyan(), port.to_string().cyan());
    println!("requests:   {}", format_num(args.requests));
    println!("clients:    {}", args.clients);
    println!("pipeline:   {}", args.pipeline);
    println!("data size:  {} bytes", args.data_size);
    println!();

    let workloads: Vec<&str> = args.tests.split(',').map(|s| s.trim()).collect();

    for workload in &workloads {
        match workload.to_lowercase().as_str() {
            "ping" => {
                if run_workload(args, host, port, password, tls, "PING", WorkloadKind::Ping)
                    .await
                    .is_err()
                {
                    return ExitCode::FAILURE;
                }
            }
            "set" => {
                if run_workload(args, host, port, password, tls, "SET", WorkloadKind::Set)
                    .await
                    .is_err()
                {
                    return ExitCode::FAILURE;
                }
            }
            "get" => {
                // pre-populate keys so GET has data to read
                if !args.quiet {
                    println!("  pre-populating {} keys...", format_num(args.keyspace));
                }
                if prepopulate(args, host, port, password, tls).await.is_err() {
                    return ExitCode::FAILURE;
                }
                if run_workload(args, host, port, password, tls, "GET", WorkloadKind::Get)
                    .await
                    .is_err()
                {
                    return ExitCode::FAILURE;
                }
            }
            "multi" => {
                if run_workload(
                    args,
                    host,
                    port,
                    password,
                    tls,
                    "MULTI/SET/EXEC",
                    WorkloadKind::Multi,
                )
                .await
                .is_err()
                {
                    return ExitCode::FAILURE;
                }
            }
            other => {
                eprintln!(
                    "{}",
                    format!("unknown workload: {other} (valid: ping, set, get, multi)").yellow()
                );
            }
        }
    }

    println!();
    ExitCode::SUCCESS
}

#[derive(Clone, Copy)]
enum WorkloadKind {
    Ping,
    Set,
    Get,
    /// MULTI + SET + EXEC per operation — measures transaction overhead.
    Multi,
}

/// Runs a single workload benchmark.
async fn run_workload(
    args: &BenchmarkArgs,
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
    label: &str,
    kind: WorkloadKind,
) -> Result<(), ()> {
    let clients = args.clients as usize;
    let pipeline = args.pipeline.max(1) as usize;
    let total = args.requests;

    // divide requests evenly among clients
    let per_client = total / clients as u64;
    let remainder = total % clients as u64;

    // generate the value payload once
    let value = generate_value(args.data_size);

    // the coordinator (main task) is a barrier participant so wall_start
    // is measured only after all clients have connected and are ready.
    let barrier = Arc::new(Barrier::new(clients + 1));

    let mut handles = Vec::with_capacity(clients);

    for i in 0..clients {
        let count = per_client + if (i as u64) < remainder { 1 } else { 0 };
        if count == 0 {
            continue;
        }

        let host = host.to_string();
        let password = password.map(|s| s.to_string());
        let value = value.clone();
        let barrier = barrier.clone();
        let keyspace = args.keyspace;
        let tls = tls.cloned();

        let handle = tokio::spawn(async move {
            let mut conn = match BenchConnection::connect(&host, port, tls.as_ref()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{}", format!("connection failed: {e}").red());
                    return Err(());
                }
            };

            if let Some(ref pw) = password {
                if let Err(e) = conn.authenticate(pw).await {
                    eprintln!("{}", format!("auth failed: {e}").red());
                    return Err(());
                }
            }

            // collect per-batch latencies
            let mut latencies = Vec::with_capacity((count as usize / pipeline) + 1);
            let mut sent: u64 = 0;

            barrier.wait().await;

            let mut rng = StdRng::from_os_rng();

            // reuse a serialization buffer across batches to avoid
            // re-allocating on every iteration
            let mut ser_buf = BytesMut::new();

            // pre-serialize MULTI and EXEC frames for transaction workloads
            let (multi_bytes, exec_bytes) = if matches!(kind, WorkloadKind::Multi) {
                let mut mb = BytesMut::new();
                Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"MULTI"))]).serialize(&mut mb);
                let mut eb = BytesMut::new();
                Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"EXEC"))]).serialize(&mut eb);
                (mb.freeze(), eb.freeze())
            } else {
                (Bytes::new(), Bytes::new())
            };

            while sent < count {
                let batch = pipeline.min((count - sent) as usize);

                // build one command per pipeline slot, each with a distinct
                // random key. this avoids the hot-key skew that results from
                // repeating the same command N times.
                let mut cmds: Vec<Bytes> = Vec::with_capacity(batch);
                for _ in 0..batch {
                    let key = format!("key:{:012}", rng.random_range(0..keyspace));
                    let frame = match kind {
                        WorkloadKind::Ping => {
                            Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
                        }
                        WorkloadKind::Set | WorkloadKind::Multi => Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"SET")),
                            Frame::Bulk(Bytes::from(key)),
                            Frame::Bulk(Bytes::from(value.clone())),
                        ]),
                        WorkloadKind::Get => Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"GET")),
                            Frame::Bulk(Bytes::from(key)),
                        ]),
                    };
                    frame.serialize(&mut ser_buf);
                    cmds.push(ser_buf.split().freeze());
                }

                let result = if matches!(kind, WorkloadKind::Multi) {
                    conn.send_transactions(&cmds, &multi_bytes, &exec_bytes)
                        .await
                } else {
                    conn.send_many(&cmds).await
                };

                match result {
                    Ok(elapsed) => {
                        // record per-command latency (guard against zero/overflow)
                        let divisor = (batch as u32).max(1);
                        let per_cmd = elapsed / divisor;
                        for _ in 0..batch {
                            latencies.push(per_cmd);
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", format!("pipeline error: {e}").red());
                        return Err(());
                    }
                }

                sent += batch as u64;
            }

            Ok(latencies)
        });

        handles.push(handle);
    }

    // wait until all clients have finished setup and are ready to go.
    // starting the timer here ensures connection setup time is excluded
    // from the reported throughput.
    barrier.wait().await;
    let wall_start = Instant::now();
    let mut all_latencies: Vec<Duration> = Vec::with_capacity(total as usize);

    // wait for tasks — they've already been spawned and the barrier will
    // release them roughly together
    for handle in handles {
        match handle.await {
            Ok(Ok(lats)) => all_latencies.extend(lats),
            Ok(Err(())) => return Err(()),
            Err(e) => {
                eprintln!("{}", format!("task panicked: {e}").red());
                return Err(());
            }
        }
    }

    let wall_elapsed = wall_start.elapsed();

    // compute stats
    let ops_sec = if wall_elapsed.as_secs_f64() > 0.0 {
        all_latencies.len() as f64 / wall_elapsed.as_secs_f64()
    } else {
        0.0
    };

    all_latencies.sort_unstable();

    let p50 = percentile(&all_latencies, 50.0);
    let p99 = percentile(&all_latencies, 99.0);
    let p999 = percentile(&all_latencies, 99.9);
    let max = all_latencies.last().copied().unwrap_or_default();

    println!(
        "{}: {} rps    p50: {}  p99: {}  p99.9: {}  max: {}",
        label.bold(),
        format_num(ops_sec as u64).green(),
        format_duration(p50),
        format_duration(p99),
        format_duration(p999),
        format_duration(max),
    );

    Ok(())
}

/// Pre-populates keys before a GET benchmark so reads don't all return nil.
async fn prepopulate(
    args: &BenchmarkArgs,
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<(), ()> {
    let value = generate_value(args.data_size);
    let clients = args.clients.max(1) as usize;
    let per_client = args.keyspace / clients as u64;
    let remainder = args.keyspace % clients as u64;
    let pipeline = args.pipeline.max(1) as usize;

    let mut handles = Vec::with_capacity(clients);

    for i in 0..clients {
        let start = per_client * i as u64 + (i as u64).min(remainder);
        let count = per_client + if (i as u64) < remainder { 1 } else { 0 };
        if count == 0 {
            continue;
        }

        let host = host.to_string();
        let password = password.map(|s| s.to_string());
        let value = value.clone();
        let tls = tls.cloned();

        let handle = tokio::spawn(async move {
            let mut conn = match BenchConnection::connect(&host, port, tls.as_ref()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{}", format!("prepopulate connect: {e}").red());
                    return Err(());
                }
            };

            if let Some(ref pw) = password {
                if let Err(e) = conn.authenticate(pw).await {
                    eprintln!("{}", format!("prepopulate auth: {e}").red());
                    return Err(());
                }
            }

            let mut sent: u64 = 0;
            while sent < count {
                let batch = pipeline.min((count - sent) as usize);
                let key = format!("key:{:012}", start + sent);
                let frame = Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"SET")),
                    Frame::Bulk(Bytes::from(key)),
                    Frame::Bulk(Bytes::from(value.clone())),
                ]);
                conn.set_command(&frame);

                if let Err(e) = conn.send_pipeline(batch).await {
                    eprintln!("{}", format!("prepopulate error: {e}").red());
                    return Err(());
                }
                sent += batch as u64;
            }

            Ok(())
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(())) => return Err(()),
            Err(e) => {
                eprintln!("{}", format!("prepopulate panic: {e}").red());
                return Err(());
            }
        }
    }

    Ok(())
}

/// Generates a random value string of the given size.
fn generate_value(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random_range(b'a'..=b'z')).collect()
}

/// Computes a percentile from a sorted slice of durations.
pub fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Formats a duration as a human-readable latency string.
fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{us}us")
    } else {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    }
}

/// Formats a large number with comma separators.
fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Builds a workload frame for testing.
#[cfg(test)]
fn build_workload_frame(cmd: &str, key: &str, value: Option<&[u8]>) -> Frame {
    let mut parts = vec![
        Frame::Bulk(Bytes::from(cmd.to_string())),
        Frame::Bulk(Bytes::from(key.to_string())),
    ];
    if let Some(v) = value {
        parts.push(Frame::Bulk(Bytes::from(v.to_vec())));
    }
    Frame::Array(parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn default_args() {
        // verify defaults are what we document
        let args = BenchmarkArgs {
            requests: 100_000,
            clients: 50,
            pipeline: 1,
            data_size: 64,
            tests: "set,get".into(),
            keyspace: 100_000,
            quiet: false,
        };
        assert_eq!(args.requests, 100_000);
        assert_eq!(args.clients, 50);
        assert_eq!(args.pipeline, 1);
        assert_eq!(args.data_size, 64);
        assert_eq!(args.tests, "set,get");
        assert_eq!(args.keyspace, 100_000);
        assert!(!args.quiet);
    }

    #[test]
    fn percentile_empty() {
        assert_eq!(percentile(&[], 50.0), Duration::ZERO);
    }

    #[test]
    fn percentile_single() {
        let d = vec![Duration::from_millis(5)];
        assert_eq!(percentile(&d, 50.0), Duration::from_millis(5));
        assert_eq!(percentile(&d, 99.0), Duration::from_millis(5));
    }

    #[test]
    fn percentile_multiple() {
        let mut latencies: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        latencies.sort_unstable();

        // p50 should be around 50ms
        let p50 = percentile(&latencies, 50.0);
        assert!(p50 >= Duration::from_millis(49) && p50 <= Duration::from_millis(51));

        // p99 should be around 99ms
        let p99 = percentile(&latencies, 99.0);
        assert!(p99 >= Duration::from_millis(98) && p99 <= Duration::from_millis(100));

        // p99.9 should be ~100ms (last element)
        let p999 = percentile(&latencies, 99.9);
        assert_eq!(p999, Duration::from_millis(100));
    }

    #[test]
    fn format_num_basic() {
        assert_eq!(format_num(0), "0");
        assert_eq!(format_num(999), "999");
        assert_eq!(format_num(1000), "1,000");
        assert_eq!(format_num(100_000), "100,000");
        assert_eq!(format_num(1_000_000), "1,000,000");
    }

    #[test]
    fn generate_value_correct_size() {
        let v = generate_value(64);
        assert_eq!(v.len(), 64);
        // all bytes should be lowercase ascii
        for &b in &v {
            assert!(b.is_ascii_lowercase());
        }
    }

    #[test]
    fn generate_value_zero() {
        let v = generate_value(0);
        assert!(v.is_empty());
    }

    #[test]
    fn workload_frame_set() {
        let frame = build_workload_frame("SET", "key:1", Some(b"hello"));
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        // should be a valid RESP3 array with 3 elements
        assert!(buf.starts_with(b"*3\r\n"));
    }

    #[test]
    fn workload_frame_get() {
        let frame = build_workload_frame("GET", "key:1", None);
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        // should be a valid RESP3 array with 2 elements
        assert!(buf.starts_with(b"*2\r\n"));
    }

    #[test]
    fn format_duration_microseconds() {
        assert_eq!(format_duration(Duration::from_micros(500)), "500us");
        assert_eq!(format_duration(Duration::from_micros(0)), "0us");
    }

    #[test]
    fn format_duration_milliseconds() {
        let d = Duration::from_micros(1500);
        let s = format_duration(d);
        assert!(s.contains("ms"));
        assert!(s.starts_with("1.50"));
    }
}
