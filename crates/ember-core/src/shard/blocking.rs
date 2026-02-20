use super::*;

/// Handles a BLPOP or BRPOP request.
///
/// Tries to pop immediately. If the list has an element, sends the result
/// on the waiter oneshot and records the AOF mutation. If the list is empty
/// (or doesn't exist), registers the waiter in the appropriate map for
/// later wake-up by an LPush/RPush.
pub(super) fn handle_blocking_pop(
    key: &str,
    waiter: mpsc::Sender<(String, Bytes)>,
    is_left: bool,
    reply: oneshot::Sender<ShardResponse>,
    ctx: &mut ProcessCtx<'_>,
) {
    let fsync_policy = ctx.fsync_policy;
    let shard_id = ctx.shard_id;

    // try to pop immediately
    let result = if is_left {
        ctx.keyspace.lpop(key)
    } else {
        ctx.keyspace.rpop(key)
    };

    match result {
        Ok(Some(data)) => {
            // got an element — send to waiter and record the mutation
            let _ = waiter.try_send((key.to_owned(), data));
            let _ = reply.send(ShardResponse::Ok);

            // AOF + replication for the pop
            let record = if is_left {
                AofRecord::LPop {
                    key: key.to_owned(),
                }
            } else {
                AofRecord::RPop {
                    key: key.to_owned(),
                }
            };
            aof::write_aof_record(&record, ctx.aof_writer, fsync_policy, shard_id);
            aof::broadcast_replication(record, ctx.replication_tx, ctx.replication_offset, shard_id);
        }
        Ok(None) => {
            // list is empty or doesn't exist — register the waiter
            let map = if is_left {
                &mut *ctx.lpop_waiters
            } else {
                &mut *ctx.rpop_waiters
            };
            map.entry(key.to_owned()).or_default().push_back(waiter);
            // drop reply — connection handler doesn't await it for blocking ops
            drop(reply);
        }
        Err(_) => {
            // WRONGTYPE — drop the waiter (connection sees the receiver close)
            // and send an error on the reply channel
            let _ = reply.send(ShardResponse::WrongType);
        }
    }
}

/// Checks if any BLPOP/BRPOP waiters are blocked on a key after a push
/// operation. Pops elements from the list and sends them to waiters until
/// either no more waiters remain or the list is empty.
pub(super) fn wake_blocked_waiters(key: &str, ctx: &mut ProcessCtx<'_>) {
    let fsync_policy = ctx.fsync_policy;
    let shard_id = ctx.shard_id;

    // try BLPOP waiters first (left-pop), then BRPOP (right-pop)
    for is_left in [true, false] {
        let map = if is_left {
            &mut *ctx.lpop_waiters
        } else {
            &mut *ctx.rpop_waiters
        };

        if let Some(waiters) = map.get_mut(key) {
            while let Some(waiter) = waiters.pop_front() {
                // skip dead waiters (client disconnected / timed out)
                if waiter.is_closed() {
                    continue;
                }

                let result = if is_left {
                    ctx.keyspace.lpop(key)
                } else {
                    ctx.keyspace.rpop(key)
                };

                match result {
                    Ok(Some(data)) => {
                        let _ = waiter.try_send((key.to_owned(), data));

                        // record AOF + replication for the pop
                        let record = if is_left {
                            AofRecord::LPop {
                                key: key.to_owned(),
                            }
                        } else {
                            AofRecord::RPop {
                                key: key.to_owned(),
                            }
                        };
                        aof::write_aof_record(&record, ctx.aof_writer, fsync_policy, shard_id);
                        aof::broadcast_replication(
                            record,
                            ctx.replication_tx,
                            ctx.replication_offset,
                            shard_id,
                        );
                    }
                    _ => break, // list is empty or wrong type — stop waking
                }
            }

            // clean up empty waiter lists
            if waiters.is_empty() {
                map.remove(key);
            }
        }
    }
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

        // give the shard a moment to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = rx.try_recv();
        assert!(result.is_ok());
        let (key, data) = result.unwrap();
        assert_eq!(key, "mylist");
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn blpop_blocks_then_wakes_on_push() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
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

        tokio::time::sleep(Duration::from_millis(50)).await;
        let (key, data) = rx.try_recv().unwrap();
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
