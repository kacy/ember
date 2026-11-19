use std::sync::atomic::AtomicBool;

use super::*;

/// A client blocked in BLPOP or BRPOP, as each shard it waits on holds it.
///
/// The client takes at most one element in total. A shard claims the
/// client before popping, and the client claims itself when it stops
/// waiting, on a timeout or an error. Whoever claims first wins, so an
/// element is never popped for a client that has stopped waiting.
#[derive(Clone, Debug)]
pub struct PopWaiter {
    tx: mpsc::Sender<(String, Bytes)>,
    claimed: Arc<AtomicBool>,
}

impl PopWaiter {
    /// Creates a waiter and the receiver its element arrives on.
    pub fn new() -> (Self, mpsc::Receiver<(String, Bytes)>) {
        let (tx, rx) = mpsc::channel(1);
        let waiter = Self {
            tx,
            claimed: Arc::new(AtomicBool::new(false)),
        };
        (waiter, rx)
    }

    /// Claims the client. Returns true for the first caller only. After a
    /// shard's successful claim, the element is on its way.
    pub fn claim(&self) -> bool {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether the client still waits for an element.
    fn waiting(&self) -> bool {
        !self.claimed.load(Ordering::Acquire) && !self.tx.is_closed()
    }
}

/// Handles a BLPOP or BRPOP request.
///
/// Pops at once if the list has an element, and replies `Ok`. If the list
/// is empty or missing, registers the waiter for a later push to wake and
/// drops `reply`. A key holding another type replies `WrongType`.
pub(super) fn handle_blocking_pop(
    key: &str,
    waiter: PopWaiter,
    is_left: bool,
    reply: ReplySender,
    ctx: &mut ProcessCtx<'_>,
) {
    match ctx.keyspace.llen(key) {
        Err(_) => ctx.respond(reply, ShardResponse::WrongType),
        Ok(0) => {
            waiters(ctx, is_left)
                .entry(key.to_owned())
                .or_default()
                .push_back(waiter);
            drop(reply);
        }
        Ok(_) => {
            // a client served through another key, or gone, leaves this
            // list's element alone
            pop_for(key, is_left, &waiter, ctx);
            ctx.respond(reply, ShardResponse::Ok);
        }
    }
}

/// The BLPOP or BRPOP waiters of this shard.
fn waiters<'a>(
    ctx: &'a mut ProcessCtx<'_>,
    is_left: bool,
) -> &'a mut HashMap<String, VecDeque<PopWaiter>> {
    if is_left {
        ctx.lpop_waiters
    } else {
        ctx.rpop_waiters
    }
}

/// Checks if any BLPOP/BRPOP waiters are blocked on a key after a push
/// operation. Pops elements from the list and sends them to waiters until
/// either no more waiters remain or the list is empty.
pub(super) fn wake_blocked_waiters(key: &str, ctx: &mut ProcessCtx<'_>) {
    // try BLPOP waiters first (left-pop), then BRPOP (right-pop)
    for is_left in [true, false] {
        let Some(mut queue) = waiters(ctx, is_left).remove(key) else {
            continue;
        };

        while let Some(waiter) = queue.pop_front() {
            if !waiter.waiting() {
                continue;
            }
            if !ctx.keyspace.llen(key).is_ok_and(|n| n > 0) {
                // the list ran out: this client is still waiting
                queue.push_front(waiter);
                break;
            }
            pop_for(key, is_left, &waiter, ctx);
        }

        if !queue.is_empty() {
            waiters(ctx, is_left).insert(key.to_owned(), queue);
        }
    }
}

/// Drops waiters whose clients stopped waiting, and the keys left with
/// none. Waiters are otherwise only cleaned up when their key gets a push,
/// so clients blocking on keys that never receive one would pile up.
pub(super) fn prune_waiters(waiters: &mut HashMap<String, VecDeque<PopWaiter>>) {
    waiters.retain(|_, queue| {
        queue.retain(PopWaiter::waiting);
        !queue.is_empty()
    });
}

/// Claims `waiter`, pops one element from the list at `key` for it, logs
/// the pop to the AOF and replication stream, and then sends the element.
/// Does nothing if the client stopped waiting or another shard served it.
/// The caller checks that the list holds an element.
fn pop_for(key: &str, is_left: bool, waiter: &PopWaiter, ctx: &mut ProcessCtx<'_>) {
    // the client's channel holds one message, and only the claim's winner
    // sends, so the reserve fails only for a client that disconnected
    let Ok(permit) = waiter.tx.try_reserve() else {
        return;
    };
    if !waiter.claim() {
        return;
    }
    let popped = if is_left {
        ctx.keyspace.lpop(key)
    } else {
        ctx.keyspace.rpop(key)
    };
    let Ok(Some(data)) = popped else {
        return;
    };

    // log the pop before the client sees it. if the AOF fails, the element
    // is still delivered, since it left the list in memory, and the shard
    // rejects writes until the AOF works again
    let key = key.to_owned();
    let record = if is_left {
        AofRecord::LPop { key: key.clone() }
    } else {
        AofRecord::RPop { key: key.clone() }
    };
    if let Some(ref mut aof) = *ctx.aof {
        aof.append(std::slice::from_ref(&record));
    }
    if let Some(ref mut replication) = *ctx.replication {
        replication.push([record]);
    }
    permit.send((key, data));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_waiters_drops_closed_clients_and_empty_keys() {
        let mut waiters: HashMap<String, VecDeque<PopWaiter>> = HashMap::new();
        let (live, _live_rx) = PopWaiter::new();
        let (gone, gone_rx) = PopWaiter::new();
        drop(gone_rx);
        waiters
            .entry("kept".into())
            .or_default()
            .extend([live, gone.clone()]);
        waiters.entry("dropped".into()).or_default().push_back(gone);

        prune_waiters(&mut waiters);
        assert_eq!(waiters.len(), 1);
        assert_eq!(waiters["kept"].len(), 1);
    }

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
        let (tx, mut rx) = PopWaiter::new();
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
        let (tx, mut rx) = PopWaiter::new();
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
    async fn lmove_into_a_waited_key_wakes_the_client() {
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
        let (tx, mut rx) = PopWaiter::new();
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "dst".into(),
                waiter: tx,
            })
            .await;
        handle
            .send(ShardRequest::RPush {
                key: "src".into(),
                values: vec![Bytes::from("job")],
            })
            .await
            .unwrap();
        handle
            .send(ShardRequest::LMove {
                source: "src".into(),
                destination: "dst".into(),
                src_left: true,
                dst_left: false,
            })
            .await
            .unwrap();

        let (key, data) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("LMOVE did not wake the blocked client")
            .unwrap();
        assert_eq!((key.as_str(), data), ("dst", Bytes::from("job")));
    }

    fn test_shard() -> ShardHandle {
        spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        )
    }

    #[tokio::test]
    async fn a_claimed_waiter_leaves_pushed_elements_in_the_list() {
        let handle = test_shard();
        let (waiter, rx) = PopWaiter::new();
        let reply = handle
            .dispatch(ShardRequest::BLPop {
                key: "q".into(),
                waiter: waiter.clone(),
            })
            .await
            .unwrap();
        assert!(reply.await.is_err(), "the waiter was registered");

        // the client timed out, or another shard served it
        assert!(waiter.claim());
        handle
            .send(ShardRequest::LPush {
                key: "q".into(),
                values: vec![Bytes::from("kept")],
            })
            .await
            .unwrap();
        let len = handle
            .send(ShardRequest::LLen { key: "q".into() })
            .await
            .unwrap();
        assert!(matches!(len, ShardResponse::Len(1)), "{len:?}");
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn rename_into_a_waited_key_wakes_the_client() {
        let handle = test_shard();
        let (waiter, mut rx) = PopWaiter::new();
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "q".into(),
                waiter,
            })
            .await;
        handle
            .send(ShardRequest::RPush {
                key: "src".into(),
                values: vec![Bytes::from("x")],
            })
            .await
            .unwrap();
        let resp = handle
            .send(ShardRequest::Rename {
                key: "src".into(),
                newkey: "q".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok), "{resp:?}");
        let (key, data) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("not woken")
            .unwrap();
        assert_eq!((key.as_str(), &data[..]), ("q", &b"x"[..]));
    }

    #[tokio::test]
    async fn dropped_waiters_leave_pushes_in_the_list() {
        let handle = test_shard();
        let (waiter, rx) = PopWaiter::new();
        let _ = handle
            .dispatch(ShardRequest::BLPop {
                key: "q".into(),
                waiter,
            })
            .await;
        handle.send(ShardRequest::DropBlockedWaiters).await.unwrap();
        handle
            .send(ShardRequest::LPush {
                key: "q".into(),
                values: vec![Bytes::from("kept")],
            })
            .await
            .unwrap();
        let len = handle
            .send(ShardRequest::LLen { key: "q".into() })
            .await
            .unwrap();
        assert!(matches!(len, ShardResponse::Len(1)), "{len:?}");
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn blpop_on_another_type_replies_wrongtype() {
        let handle = test_shard();
        handle
            .send(ShardRequest::Set {
                key: "s".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: false,
                xx: false,
            })
            .await
            .unwrap();
        let (waiter, _rx) = PopWaiter::new();
        let reply = handle
            .send(ShardRequest::BLPop {
                key: "s".into(),
                waiter,
            })
            .await
            .unwrap();
        assert!(matches!(reply, ShardResponse::WrongType));
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
        let (tx, mut rx) = PopWaiter::new();
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
        let (tx, mut rx) = PopWaiter::new();
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
        let (tx, rx) = PopWaiter::new();
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

        let (tx, rx) = PopWaiter::new();
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
