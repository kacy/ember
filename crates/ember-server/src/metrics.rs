//! Prometheus metrics exposition.
//!
//! When `--metrics-port` is set, installs a prometheus exporter that
//! serves `/metrics` on a separate HTTP port. The hot path records
//! per-command counters and latency histograms through the `metrics`
//! crate's global recorder. A background stats poller periodically
//! broadcasts `ShardRequest::Stats` and updates gauge values.

use std::net::SocketAddr;
use std::time::Duration;

use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::{info, warn};

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


/// Installs the prometheus exporter and starts the HTTP listener.
///
/// Returns an error if the listener can't bind to the address.
pub fn install_exporter(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets(HISTOGRAM_BUCKETS)
        .map_err(|e| format!("failed to set histogram buckets: {e}"))?
        .install()
        .map_err(|e| format!("failed to install prometheus exporter: {e}"))?;

    info!("prometheus metrics available on http://{addr}/metrics");
    Ok(())
}

/// Spawns a background task that polls shard stats and publishes them
/// as prometheus gauges.
///
/// Keeps `ember-core` free of metrics dependencies — the poller
/// pulls stats through the existing `ShardRequest::Stats` broadcast.
pub fn spawn_stats_poller(engine: Engine, poll_interval: Duration) {
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
