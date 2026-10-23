//! Starting and stopping replication between a primary and its replicas.

use super::*;

/// TCP port offset from the data port to the replication stream port.
const REPLICATION_PORT_OFFSET: u16 = 2;

/// Snapshot of replication status for the `INFO replication` command.
#[derive(Debug)]
pub struct ReplicationInfo {
    pub role: NodeRole,
    /// Address of the primary this node replicates from (replica only).
    pub primary_addr: Option<std::net::SocketAddr>,
    /// Number of connected replicas (primary only).
    pub replica_count: usize,
}

impl ClusterCoordinator {
    /// Derives the replication TCP port from a data-plane port.
    ///
    /// The formula is `data_port + gossip_port_offset + REPLICATION_PORT_OFFSET`,
    /// which keeps replication off both the data port and the gossip port.
    pub(super) fn replication_port(&self, data_port: u16) -> Option<u16> {
        data_port
            .checked_add(self.gossip_port_offset)
            .and_then(|p| p.checked_add(REPLICATION_PORT_OFFSET))
    }

    /// Attaches the engine so replication can start on demand.
    ///
    /// Must be called once after the engine is built, before any `CLUSTER REPLICATE`
    /// commands are processed.
    pub fn set_engine(&self, engine: Arc<Engine>) {
        let _ = self.engine.set(engine);
    }

    /// Starts the replication server for this node.
    ///
    /// Binds a TCP listener on `bind_addr.port() + gossip_port_offset + 2`
    /// and accepts replica connections indefinitely. This is a no-op if
    /// no engine has been attached via `set_engine`.
    pub async fn start_replication_server(
        self: &Arc<Self>,
        tracker: Arc<crate::replication::ReplicaTracker>,
    ) {
        let Some(engine) = self.engine.get() else {
            warn!("start_replication_server called before set_engine; skipping");
            return;
        };

        let repl_port = match self.replication_port(self.bind_addr.port()) {
            Some(p) => p,
            None => {
                error!("replication port overflows u16; not starting replication server");
                return;
            }
        };

        let local_id = self.local_id.to_string();
        if let Err(e) = crate::replication::ReplicationServer::start(
            Arc::clone(engine),
            local_id,
            SocketAddr::new(self.bind_addr.ip(), repl_port),
            tracker,
            self.secret.clone(),
        )
        .await
        {
            error!("failed to start replication server on port {repl_port}: {e}");
        }
    }

    /// Starts the replication client, connecting to the primary's replication port.
    ///
    /// The replication port of the primary is derived from its data-plane address
    /// by adding `gossip_port_offset + 2`.
    pub(super) async fn start_replication_client(&self, primary_id: NodeId) {
        let Some(engine) = self.engine.get() else {
            warn!("start_replication_client called before set_engine; skipping");
            return;
        };

        let primary_addr = {
            let state = self.state.read().await;
            state.nodes.get(&primary_id).map(|n| n.addr)
        };

        let Some(addr) = primary_addr else {
            warn!(%primary_id, "cannot start replication client: primary not found in state");
            return;
        };

        let repl_port = match self.replication_port(addr.port()) {
            Some(p) => p,
            None => {
                error!(%primary_id, "primary replication port overflows u16; not connecting");
                return;
            }
        };

        let repl_addr = std::net::SocketAddr::new(addr.ip(), repl_port);
        info!(%primary_id, %repl_addr, "starting replication client");
        let task = crate::replication::ReplicationClient::start(
            Arc::clone(engine),
            repl_addr,
            self.secret.clone(),
        );
        // a second REPLICATE replaces the first; two streams would mix data
        if let Some(previous) = self.replication_task.lock().await.replace(task) {
            previous.abort();
        }
    }

    /// Stops pulling from the primary, if this node was replicating one.
    pub(super) async fn stop_replication_client(&self) {
        if let Some(task) = self.replication_task.lock().await.take() {
            task.abort();
        }
    }

    /// Starts replicating from our primary if the saved cluster state says
    /// this node is a replica. Called once at startup, after the engine is
    /// attached, so a restarted replica picks its stream back up.
    pub async fn resume_replication(&self) {
        let primary = {
            let state = self.state.read().await;
            state
                .nodes
                .get(&self.local_id)
                .filter(|node| node.role == NodeRole::Replica)
                .and_then(|node| node.replicates)
        };
        if let Some(primary_id) = primary {
            self.start_replication_client(primary_id).await;
        }
    }

    /// Returns replication status for the `INFO replication` section.
    ///
    /// Returns `(role, Option<primary_addr>)`.
    pub async fn replication_info(&self) -> ReplicationInfo {
        let state = self.state.read().await;
        let local = state.nodes.get(&self.local_id);
        let role = local.map(|n| n.role).unwrap_or(NodeRole::Primary);
        let primary_addr = if role == NodeRole::Replica {
            local
                .and_then(|n| n.replicates)
                .and_then(|id| state.nodes.get(&id))
                .map(|n| n.addr)
        } else {
            None
        };
        let replica_count = if role == NodeRole::Primary {
            local.map(|n| n.replicas.len()).unwrap_or(0)
        } else {
            0
        };
        ReplicationInfo {
            role,
            primary_addr,
            replica_count,
        }
    }
}
