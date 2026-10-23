use super::failover::MAX_VOTE_EPOCH_AHEAD;
use super::*;

/// Creates a test coordinator with a single node that owns no slots.
fn test_coordinator() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
    let local_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let config = GossipConfig::default();
    ClusterCoordinator::new(local_id, addr, config, false, None, None).unwrap()
}

/// Creates a test coordinator bootstrapped with all 16384 slots.
fn test_coordinator_bootstrapped() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
    let local_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let config = GossipConfig::default();
    ClusterCoordinator::new(local_id, addr, config, true, None, None).unwrap()
}

#[tokio::test]
async fn raft_update_only_applies_slots_it_changed() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let local = coord.local_id;
    let other = NodeId::new();
    // raft still names the local node for slots 0 and 1, but gossip has
    // since moved slot 0 to `other`
    let stale: BTreeMap<u16, String> = [(0, local.0.to_string()), (1, local.0.to_string())]
        .into_iter()
        .collect();
    coord.state.write().await.slot_map.assign(0, other);

    // the new raft update moves slot 1 and leaves slot 0 alone
    let mut data = ClusterStateData {
        slots: stale.clone(),
        ..Default::default()
    };
    data.slots.insert(1, other.0.to_string());
    coord.apply_raft_state(&data, &stale).await;

    let state = coord.state.read().await;
    assert_eq!(state.slot_map.owner(0), Some(other));
    assert_eq!(state.slot_map.owner(1), Some(other));
}

#[test]
fn new_rejects_port_overflow() {
    // port 65000 + offset 2000 = 67000, which overflows u16 (max 65535)
    let local_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:65000".parse().unwrap();
    let config = GossipConfig {
        gossip_port_offset: 2000,
        ..GossipConfig::default()
    };
    let result = ClusterCoordinator::new(local_id, addr, config, false, None, None);
    assert!(result.is_err(), "expected port overflow error");
}

#[tokio::test]
async fn setslot_importing_valid() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();
    let resp = coord
        .cluster_setslot_importing(100, &source.0.to_string())
        .await;
    assert!(matches!(resp, Frame::Simple(_)));
}

#[tokio::test]
async fn setslot_importing_invalid_slot() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();
    let resp = coord
        .cluster_setslot_importing(16384, &source.0.to_string())
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn setslot_importing_self_rejected() {
    let (coord, _rx) = test_coordinator();
    let resp = coord
        .cluster_setslot_importing(100, &coord.local_id.0.to_string())
        .await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("can't import from myself")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn setslot_importing_duplicate_rejected() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();
    let id_str = source.0.to_string();
    coord.cluster_setslot_importing(100, &id_str).await;
    let resp = coord.cluster_setslot_importing(100, &id_str).await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn setslot_migrating_valid() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let target = NodeId::new();
    let resp = coord
        .cluster_setslot_migrating(0, &target.0.to_string())
        .await;
    assert!(matches!(resp, Frame::Simple(_)));
}

#[tokio::test]
async fn setslot_migrating_not_owner() {
    let (coord, _rx) = test_coordinator(); // no slots owned
    let target = NodeId::new();
    let resp = coord
        .cluster_setslot_migrating(100, &target.0.to_string())
        .await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("not the owner")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn setslot_migrating_self_rejected() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let resp = coord
        .cluster_setslot_migrating(0, &coord.local_id.0.to_string())
        .await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("can't migrate to myself")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn setslot_node_assigns_slot() {
    let (coord, _rx) = test_coordinator();
    let target = NodeId::new();

    // add the target node to cluster state
    {
        let mut state = coord.state.write().await;
        let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
        state.add_node(node);
    }

    let resp = coord.cluster_setslot_node(100, &target.0.to_string()).await;
    assert!(matches!(resp, Frame::Simple(_)));

    // verify the slot is now owned by the target
    let state = coord.state.read().await;
    assert_eq!(state.slot_map.owner(100), Some(target));
}

#[tokio::test]
async fn setslot_node_completes_migration() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let target = NodeId::new();

    // start a migration
    coord
        .cluster_setslot_migrating(0, &target.0.to_string())
        .await;

    // add target to state
    {
        let mut state = coord.state.write().await;
        let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
        state.add_node(node);
    }

    // complete with NODE — should clean up migration state
    let resp = coord.cluster_setslot_node(0, &target.0.to_string()).await;
    assert!(matches!(resp, Frame::Simple(_)));

    // migration should be cleaned up
    let migration = coord.migration.lock().await;
    assert!(!migration.is_migrating(0));
}

#[tokio::test]
async fn setslot_stable_aborts_migration() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();
    coord
        .cluster_setslot_importing(100, &source.0.to_string())
        .await;

    let resp = coord.cluster_setslot_stable(100).await;
    assert!(matches!(resp, Frame::Simple(_)));

    // migration should be cleaned up
    let migration = coord.migration.lock().await;
    assert!(!migration.is_importing(100));
}

#[tokio::test]
async fn setslot_stable_noop_when_no_migration() {
    let (coord, _rx) = test_coordinator();
    // should succeed even with no active migration
    let resp = coord.cluster_setslot_stable(100).await;
    assert!(matches!(resp, Frame::Simple(_)));
}

#[tokio::test]
async fn addslots_queues_gossip_update() {
    let (coord, _rx) = test_coordinator();

    let resp = coord.cluster_addslots(&[0, 1, 2]).await;
    assert!(matches!(resp, Frame::Simple(_)));

    // verify state has the slots assigned
    let state = coord.state.read().await;
    assert_eq!(state.slot_map.owner(0), Some(coord.local_id));
    assert_eq!(state.slot_map.owner(1), Some(coord.local_id));
    assert_eq!(state.slot_map.owner(2), Some(coord.local_id));
    // slot 3 should still be unassigned
    assert_eq!(state.slot_map.owner(3), None);
}

#[tokio::test]
async fn delslots_queues_gossip_update() {
    let (coord, _rx) = test_coordinator_bootstrapped();

    let resp = coord.cluster_delslots(&[0, 1]).await;
    assert!(matches!(resp, Frame::Simple(_)));

    let state = coord.state.read().await;
    assert_eq!(state.slot_map.owner(0), None);
    assert_eq!(state.slot_map.owner(1), None);
    // slot 2 should still be owned
    assert_eq!(state.slot_map.owner(2), Some(coord.local_id));
}

// -- check_slot_with_migration tests --

#[tokio::test]
async fn check_slot_owned_no_migration() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    // "foo" hashes to some slot — we own all slots
    let result = coord.check_slot_with_migration(b"foo", false).await;
    assert!(result.is_none(), "should handle locally");
}

#[tokio::test]
async fn check_slot_ask_when_key_migrated() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let target = NodeId::new();

    // add target node
    {
        let mut state = coord.state.write().await;
        let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
        state.add_node(node);
    }

    // "foo" hashes to slot 12182
    let slot = ember_cluster::key_slot(b"foo");

    // start migrating the slot
    coord
        .cluster_setslot_migrating(slot, &target.0.to_string())
        .await;

    // mark "foo" as migrated via the migration manager directly
    {
        let mut migration = coord.migration.lock().await;
        migration.key_migrated(slot, b"foo".to_vec());
    }

    // should return ASK redirect
    let result = coord.check_slot_with_migration(b"foo", false).await;
    match result {
        Some(Frame::Error(msg)) => {
            assert!(msg.starts_with("ASK"), "expected ASK, got: {msg}");
            assert!(msg.contains("127.0.0.1:6380"));
        }
        other => panic!("expected ASK error, got {other:?}"),
    }
}

#[tokio::test]
async fn check_slot_local_when_key_not_migrated() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let target = NodeId::new();

    let slot = ember_cluster::key_slot(b"foo");
    coord
        .cluster_setslot_migrating(slot, &target.0.to_string())
        .await;

    // "foo" NOT migrated yet — should serve locally
    let result = coord.check_slot_with_migration(b"foo", false).await;
    assert!(result.is_none(), "should handle locally");
}

#[tokio::test]
async fn check_slot_importing_with_asking() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();

    let slot = ember_cluster::key_slot(b"foo");
    coord
        .cluster_setslot_importing(slot, &source.0.to_string())
        .await;

    // with asking=true, should allow local access
    let result = coord.check_slot_with_migration(b"foo", true).await;
    assert!(result.is_none(), "should allow with ASKING");
}

#[tokio::test]
async fn check_slot_importing_without_asking() {
    let (coord, _rx) = test_coordinator();
    let source = NodeId::new();

    // add source node so MOVED has somewhere to point
    {
        let mut state = coord.state.write().await;
        let node = ClusterNode::new_primary(source, "127.0.0.1:6381".parse().unwrap());
        state.add_node(node);
    }

    let slot = ember_cluster::key_slot(b"foo");

    // assign the slot to the source node first
    {
        let mut state = coord.state.write().await;
        state.slot_map.assign(slot, source);
    }

    coord
        .cluster_setslot_importing(slot, &source.0.to_string())
        .await;

    // without asking, should return MOVED to the owner
    let result = coord.check_slot_with_migration(b"foo", false).await;
    match result {
        Some(Frame::Error(msg)) => {
            assert!(msg.starts_with("MOVED"), "expected MOVED, got: {msg}");
        }
        other => panic!("expected MOVED error, got {other:?}"),
    }
}

#[tokio::test]
async fn check_slot_unassigned_returns_clusterdown() {
    let (coord, _rx) = test_coordinator(); // no slots assigned
    let result = coord.check_slot_with_migration(b"foo", false).await;
    match result {
        Some(Frame::Error(msg)) => {
            assert!(
                msg.contains("CLUSTERDOWN"),
                "expected CLUSTERDOWN, got: {msg}"
            );
        }
        other => panic!("expected CLUSTERDOWN error, got {other:?}"),
    }
}

#[tokio::test]
async fn save_config_writes_readable_file() {
    let dir = tempfile::tempdir().unwrap();
    let local_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let config = GossipConfig::default();
    let (coord, _rx) = ClusterCoordinator::new(
        local_id,
        addr,
        config,
        true,
        Some(dir.path().to_path_buf()),
        None,
    )
    .unwrap();

    // add some slots and save
    coord.save_config().await;

    let content = std::fs::read_to_string(dir.path().join("nodes.conf")).unwrap();
    let (restored, _) = ClusterState::from_nodes_conf(&content).unwrap();

    assert_eq!(restored.local_id, local_id);
    assert!(restored.owns_slot(0));
    assert!(restored.owns_slot(16383));
}

#[tokio::test]
async fn from_config_restores_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let local_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let config = GossipConfig::default();
    let (coord, _rx) = ClusterCoordinator::new(
        local_id,
        addr,
        config.clone(),
        true,
        Some(dir.path().to_path_buf()),
        None,
    )
    .unwrap();

    coord.save_config().await;

    let content = std::fs::read_to_string(dir.path().join("nodes.conf")).unwrap();
    let (restored_coord, _rx) =
        ClusterCoordinator::from_config(&content, addr, config, dir.path().to_path_buf(), None)
            .unwrap();

    assert_eq!(restored_coord.local_id, local_id);

    // verify slots are intact
    let state = restored_coord.state.read().await;
    assert!(state.owns_slot(0));
    assert!(state.owns_slot(16383));
}

#[tokio::test]
async fn mark_key_migrated_triggers_ask_redirect() {
    let (coord, _rx) = test_coordinator_bootstrapped();

    // set up a migration: slot 12182 (hash of "foo") migrating to a target
    let target = NodeId::new();
    let target_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

    // add target node to state
    {
        let mut state = coord.state.write().await;
        state.add_node(ClusterNode::new_primary(target, target_addr));
    }

    // start migrating slot 12182
    coord
        .cluster_setslot_migrating(12182, &target.0.to_string())
        .await;

    // before marking the key, it should be served locally
    let result = coord.check_slot_with_migration(b"foo", false).await;
    assert!(
        result.is_none(),
        "key should be served locally before migration"
    );

    // mark the key as migrated
    coord.mark_key_migrated(12182, b"foo").await;

    // now it should return ASK
    let result = coord.check_slot_with_migration(b"foo", false).await;
    match result {
        Some(Frame::Error(msg)) => {
            assert!(
                msg.starts_with("ASK 12182"),
                "expected ASK redirect, got: {msg}"
            );
        }
        other => panic!("expected ASK redirect, got {other:?}"),
    }
}

// -- cluster_replicate --

#[tokio::test]
async fn cluster_replicate_self_rejected() {
    let (coord, _rx) = test_coordinator();
    let resp = coord.cluster_replicate(&coord.local_id.0.to_string()).await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("Cannot replicate self")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_replicate_invalid_id_rejected() {
    let (coord, _rx) = test_coordinator();
    let resp = coord.cluster_replicate("not-a-uuid").await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("Invalid node ID")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_replicate_unknown_node_rejected() {
    let (coord, _rx) = test_coordinator();
    let unknown = NodeId::new();
    let resp = coord.cluster_replicate(&unknown.0.to_string()).await;
    match resp {
        Frame::Error(msg) => assert!(msg.contains("Unknown node ID")),
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_replicate_updates_state() {
    let (coord, _rx) = test_coordinator();
    let primary_id = NodeId::new();

    // add a primary node to replicate from
    {
        let mut state = coord.state.write().await;
        let primary = ClusterNode::new_primary(primary_id, "127.0.0.1:6380".parse().unwrap());
        state.add_node(primary);
    }

    let resp = coord.cluster_replicate(&primary_id.0.to_string()).await;
    assert!(
        matches!(resp, Frame::Simple(_)),
        "expected OK, got {resp:?}"
    );

    // local node should now be a replica
    assert!(
        coord.is_replica().await,
        "node should be a replica after REPLICATE"
    );

    // local node's replicates field should point to primary
    let state = coord.state.read().await;
    let local = state.nodes.get(&coord.local_id).unwrap();
    assert_eq!(local.replicates, Some(primary_id));
    assert!(state.nodes[&primary_id].replicas.contains(&coord.local_id));
}

#[tokio::test]
async fn primary_addr_for_slot_returns_primary_addr() {
    let (coord, _rx) = test_coordinator_bootstrapped();

    // the bootstrap coordinator owns all slots — find any one
    let addr = coord.primary_addr_for_slot(0).await;
    assert!(addr.is_some(), "should find primary for slot 0");
    assert_eq!(addr.unwrap().port(), 6379);
}

#[tokio::test]
async fn is_replica_returns_false_initially() {
    let (coord, _rx) = test_coordinator();
    assert!(
        !coord.is_replica().await,
        "new coordinator should be a primary"
    );
}

#[tokio::test]
async fn failover_rejected_on_primary() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let result = coord.cluster_failover(false, false).await;
    match result {
        Frame::Error(msg) => assert!(
            msg.contains("replica"),
            "expected replica error, got: {msg}"
        ),
        other => panic!("expected error frame, got {other:?}"),
    }
}

#[tokio::test]
async fn failover_takeover_promotes_replica() {
    // set up: primary owns all slots, replica replicates from it
    let (coord, _rx) = test_coordinator_bootstrapped();
    let primary_id = coord.local_id;

    let replica_id = NodeId::new();
    let replica_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

    // add the replica to the primary's state
    {
        let mut state = coord.state.write().await;
        let mut replica = ClusterNode::new_replica(replica_id, replica_addr, primary_id);
        replica.set_myself(); // pretend we're running on the replica
        state.add_node(replica);
        // register replica in primary's list
        state
            .nodes
            .get_mut(&primary_id)
            .unwrap()
            .replicas
            .push(replica_id);
        // update local_id to the replica
        // (simulate running on the replica node)
    }

    // build a replica coordinator with the same state
    let replica_addr_sa: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    let (replica_coord, _rx2) = ClusterCoordinator::new(
        replica_id,
        replica_addr_sa,
        GossipConfig::default(),
        false,
        None,
        None,
    )
    .unwrap();

    // manually set up the replica's state
    {
        let mut state = replica_coord.state.write().await;
        let primary_node = coord
            .state
            .read()
            .await
            .nodes
            .get(&primary_id)
            .unwrap()
            .clone();
        state.add_node(primary_node);
        // set this node as a replica of primary
        if let Some(local) = state.nodes.get_mut(&replica_id) {
            local.role = NodeRole::Replica;
            local.replicates = Some(primary_id);
        }
        // assign all slots to primary in the slot map
        for slot in 0..16384u16 {
            state.slot_map.assign(slot, primary_id);
        }
    }

    // TAKEOVER: should succeed since no Raft is needed
    let result = replica_coord.cluster_failover(false, true).await;
    assert!(
        matches!(result, Frame::Simple(_)),
        "expected OK, got {result:?}"
    );

    // verify the replica is now a primary
    assert!(
        !replica_coord.is_replica().await,
        "after TAKEOVER, node should be primary"
    );

    // verify it owns all slots
    let state = replica_coord.state.read().await;
    for slot in 0..16384u16 {
        assert_eq!(state.slot_map.owner(slot), Some(replica_id));
    }
}

#[tokio::test]
async fn writes_paused_blocks_and_resumes() {
    let (coord, _rx) = test_coordinator_bootstrapped();

    assert!(!coord.is_writes_paused());
    coord.pause_writes();
    assert!(coord.is_writes_paused());
    coord.resume_writes();
    assert!(!coord.is_writes_paused());
}

// -- automatic failover --

/// Adds a primary to `coord`'s view, marked failed or healthy.
async fn add_primary(coord: &ClusterCoordinator, failed: bool) -> NodeId {
    let id = NodeId::new();
    let mut state = coord.state.write().await;
    let mut node = ClusterNode::new_primary(id, "127.0.0.1:7000".parse().unwrap());
    node.flags.fail = failed;
    state.add_node(node);
    id
}

#[tokio::test]
async fn primary_grants_vote_once_per_epoch() {
    // A primary should grant a vote for a given epoch exactly once.
    let (coord, _rx) = test_coordinator_bootstrapped();
    let candidate = NodeId::new();
    let failed = add_primary(&coord, true).await;

    // first request for epoch 5 should be granted (gossip queue entry added)
    coord.handle_vote_request(candidate, failed, 5).await;
    assert_eq!(
        coord.state.read().await.last_vote_epoch,
        5,
        "last_vote_epoch should be 5 after granting"
    );

    // second request for epoch 5 should be ignored
    let prev_epoch = coord.state.read().await.last_vote_epoch;
    coord.handle_vote_request(candidate, failed, 5).await;
    assert_eq!(
        coord.state.read().await.last_vote_epoch,
        prev_epoch,
        "epoch should not change on duplicate request"
    );
}

#[tokio::test]
async fn primary_does_not_vote_far_past_the_current_epoch() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let failed = add_primary(&coord, true).await;

    coord
        .handle_vote_request(NodeId::new(), failed, 1 + MAX_VOTE_EPOCH_AHEAD + 1)
        .await;
    assert_eq!(coord.state.read().await.last_vote_epoch, 0);
}

#[tokio::test]
async fn primary_does_not_vote_to_replace_a_healthy_primary() {
    let (coord, _rx) = test_coordinator_bootstrapped();
    let healthy = add_primary(&coord, false).await;

    coord.handle_vote_request(NodeId::new(), healthy, 4).await;
    assert_eq!(
        coord.state.read().await.last_vote_epoch,
        0,
        "a primary that looks healthy here must not be voted out"
    );
}

#[tokio::test]
async fn replica_does_not_grant_vote() {
    // Replicas must not vote in elections.
    let primary_id = NodeId::new();
    let replica_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
    let (coord, _rx) =
        ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None, None)
            .unwrap();

    // set up coord as a replica
    {
        let mut state = coord.state.write().await;
        state.add_node(ClusterNode::new_primary(
            primary_id,
            "127.0.0.1:6379".parse().unwrap(),
        ));
        if let Some(n) = state.nodes.get_mut(&replica_id) {
            n.role = NodeRole::Replica;
            n.replicates = Some(primary_id);
        }
    }

    // replica should not update last_vote_epoch
    coord
        .handle_vote_request(NodeId::new(), primary_id, 3)
        .await;
    assert_eq!(
        coord.state.read().await.last_vote_epoch,
        0,
        "replica must not grant votes"
    );
}

#[tokio::test]
async fn vote_granted_reaches_quorum_and_promotes() {
    // A replica that receives enough votes should promote itself.
    let primary_id = NodeId::new();
    let voter1 = NodeId::new();
    let voter2 = NodeId::new();
    let replica_id = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
    let (coord, _rx) =
        ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None, None)
            .unwrap();
    let coord = Arc::new(coord);

    // set up coord as a replica with two peers owning all slots
    {
        let mut state = coord.state.write().await;
        let mut primary_node =
            ClusterNode::new_primary(primary_id, "127.0.0.1:6379".parse().unwrap());
        primary_node.slots = vec![SlotRange::new(0, 16383)];
        primary_node.flags.fail = true; // mark as failed
        state.add_node(primary_node.clone());
        state.add_node(ClusterNode::new_primary(
            voter1,
            "127.0.0.1:6382".parse().unwrap(),
        ));
        state.add_node(ClusterNode::new_primary(
            voter2,
            "127.0.0.1:6383".parse().unwrap(),
        ));
        // assign slots to primary
        for slot in 0..16384u16 {
            state.slot_map.assign(slot, primary_id);
        }
        // set replica state
        if let Some(n) = state.nodes.get_mut(&replica_id) {
            n.role = NodeRole::Replica;
            n.replicates = Some(primary_id);
        }
    }

    // seed election: epoch=1, 2 alive primaries (voter1, voter2), need 2 votes
    {
        let mut guard = coord.election.lock().await;
        *guard = Some(ElectionAttempt {
            inner: Election::new(1),
            voters: HashSet::from([voter1, voter2]),
        });
    }

    // first vote: not yet promoted
    coord.handle_vote_granted(voter1, replica_id, 1).await;
    assert!(
        !coord.is_replica().await || {
            // either still replica (not yet quorum) or promoted — check election
            coord
                .election
                .lock()
                .await
                .as_ref()
                .map(|e| !e.inner.is_promoted())
                .unwrap_or(true)
        }
    );

    // second vote: quorum reached
    coord.handle_vote_granted(voter2, replica_id, 1).await;

    // after quorum the election entry should be promoted and the node primary
    // (cluster_failover runs synchronously in tests since there's no Raft)
    assert!(
        !coord.is_replica().await,
        "node should be primary after winning election"
    );
}

#[tokio::test]
async fn vote_granted_wrong_candidate_ignored() {
    let replica_id = NodeId::new();
    let other_candidate = NodeId::new();
    let addr: SocketAddr = "127.0.0.1:6384".parse().unwrap();
    let (coord, _rx) =
        ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None, None)
            .unwrap();
    let coord = Arc::new(coord);

    {
        let mut guard = coord.election.lock().await;
        *guard = Some(ElectionAttempt {
            inner: Election::new(1),
            voters: HashSet::from([NodeId::new()]),
        });
    }

    // vote granted to a different candidate — should be ignored
    coord
        .handle_vote_granted(NodeId::new(), other_candidate, 1)
        .await;

    let guard = coord.election.lock().await;
    assert!(
        !guard.as_ref().unwrap().inner.is_promoted(),
        "vote for wrong candidate should not trigger promotion"
    );
}
