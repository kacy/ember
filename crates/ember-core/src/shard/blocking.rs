use super::*;

/// Handles a BLPOP or BRPOP request.
///
/// Pops immediately if the list has an element. If the list is empty or
/// missing, registers the waiter for a later LPush/RPush to wake.
pub(super) fn handle_blocking_pop(
    key: &str,
    waiter: mpsc::Sender<(String, Bytes)>,
    is_left: bool,
    reply: ReplySender,
    ctx: &mut ProcessCtx<'_>,
) {
    match ctx.keyspace.llen(key) {
        Err(_) => reply.send(ShardResponse::WrongType),
        Ok(0) => {
            let map = if is_left {
                &mut *ctx.lpop_waiters
            } else {
                &mut *ctx.rpop_waiters
            };
            map.entry(key.to_owned()).or_default().push_back(waiter);
            // drop reply — connection handler doesn't await it for blocking ops
            drop(reply);
        }
        Ok(_) => {
            // a client blocked on several keys takes one element in total,
            // and its channel holds one message. reserve that slot before
            // popping: if another shard already filled it, or the client is
            // gone, this list keeps its element.
            if let Ok(permit) = waiter.try_reserve() {
                pop_for(key, is_left, permit, ctx);
            }
            reply.send(ShardResponse::Ok);
        }
    }
}

/// Checks if any BLPOP/BRPOP waiters are blocked on a key after a push
/// operation. Pops elements from the list and sends them to waiters until
/// either no more waiters remain or the list is empty.
pub(super) fn wake_blocked_waiters(key: &str, ctx: &mut ProcessCtx<'_>) {
    // try BLPOP waiters first (left-pop), then BRPOP (right-pop)
    for is_left in [true, false] {
        let map = if is_left {
            &mut *ctx.lpop_waiters
        } else {
            &mut *ctx.rpop_waiters
        };
        let Some(mut waiters) = map.remove(key) else {
            continue;
        };

        while let Some(waiter) = waiters.pop_front() {
            // skip clients that left, and clients already served through
            // another key they were blocked on
            let Ok(permit) = waiter.try_reserve() else {
                continue;
            };
            if !pop_for(key, is_left, permit, ctx) {
                // the list ran out: this client is still waiting
                waiters.push_front(waiter);
                break;
            }
        }

        if !waiters.is_empty() {
            let map = if is_left {
                &mut *ctx.lpop_waiters
            } else {
                &mut *ctx.rpop_waiters
            };
            map.insert(key.to_owned(), waiters);
        }
    }
}

/// Pops one element into a blocked client's reserved slot and logs the pop
/// to the AOF and replication stream. Returns `false`, sending nothing,
/// when the list is empty or holds another type.
fn pop_for(
    key: &str,
    is_left: bool,
    permit: mpsc::Permit<'_, (String, Bytes)>,
    ctx: &mut ProcessCtx<'_>,
) -> bool {
    let popped = if is_left {
        ctx.keyspace.lpop(key)
    } else {
        ctx.keyspace.rpop(key)
    };
    let Ok(Some(data)) = popped else {
        return false;
    };
    permit.send((key.to_owned(), data));

    let key = key.to_owned();
    let record = if is_left {
        AofRecord::LPop { key }
    } else {
        AofRecord::RPop { key }
    };
    aof::write_aof_record(
        &record,
        ctx.aof_writer,
        ctx.fsync_policy,
        ctx.shard_id,
        ctx.aof_errors,
        ctx.disk_full,
    );
    aof::broadcast_replication(
        record,
        ctx.replication_tx,
        ctx.replication_offset,
        ctx.shard_id,
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn blpop_immediate_when_list_has_data() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        // push data first
        let resp = handle
            .send(ShardRequest::LPush {
                key: "mylist".into(),
                values: vec![Bytes::from("hello")],
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Len(1)));

        // BLPop should return immediately
        let (tx, mut rx) = mpsc::channel(1);
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "mylist".into(),
                waiter: tx,
            })
            .await;

        // recv with a generous timeout instead of a fixed sleep so a busy
        // runner can't flake the test
        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        let (key, data) = result.expect("timed out waiting for BLPop").unwrap();
        assert_eq!(key, "mylist");
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn blpop_on_two_full_lists_takes_one_element() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );
        for key in ["a", "b"] {
            handle
                .send(ShardRequest::LPush {
                    key: key.into(),
                    values: vec![Bytes::from("v")],
                })
                .await
                .unwrap();
        }

        // one client blocked on both keys shares one channel, as the
        // connection handler sets it up
        let (tx, mut rx) = mpsc::channel(1);
        for key in ["a", "b"] {
            let _ = handle
                .dispatch(ShardRequest::BLPop {
                    key: key.into(),
                    waiter: tx.clone(),
                })
                .await;
        }
        let (key, _) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for BLPop")
            .unwrap();
        assert_eq!(key, "a");

        // the second list was not popped into a full channel and lost
        let resp = handle
            .send(ShardRequest::LLen { key: "b".into() })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Len(1)), "{resp:?}");
    }

    #[tokio::test]
    async fn blpop_blocks_then_wakes_on_push() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        // BLPop on empty list — registers waiter
        let (tx, mut rx) = mpsc::channel(1);
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "q".into(),
                waiter: tx,
            })
            .await;

        // give time for the shard to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // nothing yet
        assert!(rx.try_recv().is_err());

        // push an element — should wake the waiter
        let resp = handle
            .send(ShardRequest::LPush {
                key: "q".into(),
                values: vec![Bytes::from("task1")],
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Len(1)));

        // waiter should have received the element
        let result = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
        assert!(result.is_ok());
        let (key, data) = result.unwrap().unwrap();
        assert_eq!(key, "q");
        assert_eq!(data, Bytes::from("task1"));
    }

    #[tokio::test]
    async fn brpop_immediate_when_list_has_data() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        // push data: list is [a, b]
        handle
            .send(ShardRequest::RPush {
                key: "mylist".into(),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            })
            .await
            .unwrap();

        // BRPop should pop from the tail (returns "b")
        let (tx, mut rx) = mpsc::channel(1);
        let _ = handle
            .dispatch(ShardRequest::BRPop {
                key: "mylist".into(),
                waiter: tx,
            })
            .await;

        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        let (key, data) = result.expect("timed out waiting for BRPop").unwrap();
        assert_eq!(key, "mylist");
        assert_eq!(data, Bytes::from("b"));
    }

    #[tokio::test]
    async fn blpop_waiter_dropped_on_timeout() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        // BLPop on empty list, then drop the receiver (simulating timeout)
        let (tx, rx) = mpsc::channel(1);
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "q".into(),
                waiter: tx,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(rx);

        // push should succeed without error (dead waiter is skipped)
        let resp = handle
            .send(ShardRequest::LPush {
                key: "q".into(),
                values: vec![Bytes::from("data")],
            })
            .await
            .unwrap();
        // the push adds 1 element. the waiter was dead so no pop happened.
        assert!(matches!(resp, ShardResponse::Len(1)));
    }

    #[tokio::test]
    async fn blpop_dead_waiter_cleanup() {
        // Alias: same test under a more descriptive name for the blocking module.
        // Re-use the blpop_waiter_dropped_on_timeout body inline.
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        let (tx, rx) = mpsc::channel(1);
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "cleanup".into(),
                waiter: tx,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(rx);

        // push should succeed; dead waiter is cleaned up silently
        let resp = handle
            .send(ShardRequest::LPush {
                key: "cleanup".into(),
                values: vec![Bytes::from("x")],
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Len(1)));
    }
}
