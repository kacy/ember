//! Prometheus metrics and health check HTTP server.
//!
//! When `--metrics-port` is set, installs a prometheus recorder and starts
//! a custom HTTP server serving both `/metrics` and `/health`. The hot path
//! records per-command counters and latency histograms through the `metrics`
//! crate's global recorder. A background stats poller periodically broadcasts
//! `ShardRequest::Stats` and updates gauge values.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse};
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::server::ServerContext;

/// Histogram buckets tuned for cache latency (10µs to 100ms).
const HISTOGRAM_BUCKETS: &[f64] = &[
    0.000_01,  // 10µs
    0.000_025, // 25µs
    0.000_05,  // 50µs
    0.000_1,   // 100µs
    0.000_25,  // 250µs
    0.000_5,   // 500µs
    0.001,     // 1ms
    0.002_5,   // 2.5ms
    0.005,     // 5ms
    0.01,      // 10ms
    0.025,     // 25ms
    0.05,      // 50ms
    0.1,       // 100ms
];

/// Atomic counter for memory used bytes, updated by the stats poller.
///
/// Exposed on `ServerContext` so the /health endpoint can read it without
/// querying shards on every HTTP request.
pub struct MemoryUsedBytes(AtomicU64);

impl MemoryUsedBytes {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn store(&self, bytes: u64) {
        self.0.store(bytes, Ordering::Relaxed);
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for MemoryUsedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MemoryUsedBytes")
            .field(&self.load())
            .finish()
    }
}

/// Installs the prometheus recorder without starting an HTTP server.
///
/// Returns a handle that can render metrics on demand. The caller is
/// responsible for spawning both the upkeep task and the HTTP server.
pub fn install_recorder() -> Result<PrometheusHandle, Box<dyn std::error::Error>> {
    let handle = PrometheusBuilder::new()
        .set_buckets(HISTOGRAM_BUCKETS)
        .map_err(|e| format!("failed to set histogram buckets: {e}"))?
        .install_recorder()
        .map_err(|e| format!("failed to install prometheus recorder: {e}"))?;

    Ok(handle)
}

/// Spawns the HTTP server for `/metrics` and `/health` on the given address.
///
/// Also spawns the prometheus upkeep task (required when using `install_recorder`
/// instead of `install`).
pub fn spawn_http_server(addr: SocketAddr, handle: PrometheusHandle, ctx: Arc<ServerContext>) {
    // spawn periodic upkeep for histogram aggregation
    let upkeep_handle = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            upkeep_handle.run_upkeep();
        }
    });

    tokio::spawn(async move {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                warn!("failed to bind metrics/health server on {addr}: {e}");
                return;
            }
        };

        info!("metrics and health endpoint on http://{addr}");

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("metrics listener accept error: {e}");
                    continue;
                }
            };

            let handle = handle.clone();
            let ctx = Arc::clone(&ctx);

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let handle = handle.clone();
                    let ctx = Arc::clone(&ctx);
                    async move { handle_request(req, &handle, &ctx).await }
                });

                if let Err(e) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    // connection reset / client gone — not worth logging at warn
                    tracing::debug!("http connection error: {e}");
                }
            });
        }
    });
}

/// Routes HTTP requests to the appropriate handler.
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    handle: &PrometheusHandle,
    ctx: &ServerContext,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let response = match req.uri().path() {
        "/metrics" => {
            let body = handle.render();
            Response::builder()
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .expect("static builder never fails")
        }
        "/health" => build_health_response(ctx).await,
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found")))
            .expect("static builder never fails"),
    };

    Ok(response)
}

/// Builds the /health JSON response.
///
/// Returns 200 for healthy servers, 503 for unhealthy. A standalone
/// (non-cluster) node is healthy if it's running. A cluster node is
/// healthy only when all 16384 slots are covered (cluster_state:ok).
async fn build_health_response(ctx: &ServerContext) -> Response<Full<Bytes>> {
    let uptime = ctx.start_time.elapsed().as_secs();
    let used_bytes = ctx.memory_used_bytes.load();
    let max_bytes = ctx.max_memory.unwrap_or(0) as u64;

    let (cluster_json, is_healthy) = if let Some(ref coordinator) = ctx.cluster {
        let summary = coordinator.health_summary().await;
        let healthy = summary.state == "ok";
        let json = serde_json::json!({
            "enabled": true,
            "state": summary.state,
            "known_nodes": summary.known_nodes,
            "slots_assigned": summary.slots_assigned,
        });
        (Some(json), healthy)
    } else {
        (None, true)
    };

    let status = if is_healthy { "healthy" } else { "unhealthy" };
    let code = if is_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::json!({
        "status": status,
        "version": ctx.version,
        "uptime_secs": uptime,
        "memory": {
            "used_bytes": used_bytes,
            "max_bytes": max_bytes,
        },
        "shards": ctx.shard_count,
        "cluster": cluster_json,
    });

    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("static builder never fails")
}

/// Spawns a background task that polls shard stats and publishes them
/// as prometheus gauges.
///
/// Keeps `ember-core` free of metrics dependencies — the poller
/// pulls stats through the existing `ShardRequest::Stats` broadcast.
pub fn spawn_stats_poller(engine: Engine, ctx: Arc<ServerContext>, poll_interval: Duration) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            match engine.broadcast(|| ShardRequest::Stats).await {
                Ok(responses) => {
                    let mut total = KeyspaceStats {
                        key_count: 0,
                        used_bytes: 0,
                        keys_with_expiry: 0,
                        keys_expired: 0,
                        keys_evicted: 0,
                    };
                    for r in &responses {
                        if let ShardResponse::Stats(stats) = r {
                            total.key_count += stats.key_count;
                            total.used_bytes += stats.used_bytes;
                            total.keys_with_expiry += stats.keys_with_expiry;
                            total.keys_expired += stats.keys_expired;
                            total.keys_evicted += stats.keys_evicted;
                        }
                    }

                    gauge!("ember_keys_total").set(total.key_count as f64);
                    gauge!("ember_memory_used_bytes").set(total.used_bytes as f64);
                    gauge!("ember_keys_expired_total").set(total.keys_expired as f64);
                    gauge!("ember_keys_evicted_total").set(total.keys_evicted as f64);

                    // update atomic for /health endpoint
                    ctx.memory_used_bytes.store(total.used_bytes as u64);
                }
                Err(e) => {
                    warn!("stats poller broadcast failed: {e}");
                }
            }
        }
    });
}

/// Records a command execution in prometheus metrics.
///
/// Called from the connection handler after each command completes.
#[inline]
pub fn record_command(cmd_name: &'static str, duration: Duration, is_error: bool) {
    let labels = [("cmd", cmd_name)];
    counter!("ember_commands_total", &labels).increment(1);
    histogram!("ember_commands_duration_seconds", &labels).record(duration.as_secs_f64());
    if is_error {
        counter!("ember_commands_errors_total", &labels).increment(1);
    }
}

/// Increments the active connection gauge and total counter.
#[inline]
pub fn on_connection_accepted() {
    gauge!("ember_connections_active").increment(1.0);
    counter!("ember_connections_total").increment(1);
}

/// Decrements the active connection gauge.
#[inline]
pub fn on_connection_closed() {
    gauge!("ember_connections_active").decrement(1.0);
}

/// Records a rejected connection (limit reached).
#[inline]
pub fn on_connection_rejected() {
    counter!("ember_connections_rejected").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_used_bytes_store_and_load() {
        let m = MemoryUsedBytes::new();
        assert_eq!(m.load(), 0);
        m.store(42_000);
        assert_eq!(m.load(), 42_000);
    }

    #[test]
    fn memory_used_bytes_debug() {
        let m = MemoryUsedBytes::new();
        m.store(1024);
        let s = format!("{m:?}");
        assert!(s.contains("1024"));
    }
}
