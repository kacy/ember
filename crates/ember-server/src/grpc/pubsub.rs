//! Pub/sub command handlers for the gRPC service.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::proto::*;
use super::EmberService;

pub(super) async fn publish(
    svc: &EmberService,
    request: Request<PublishRequest>,
) -> Result<Response<IntResponse>, Status> {
    let req = request.into_inner();
    let count = svc.pubsub.publish(&req.channel, Bytes::from(req.message));
    Ok(Response::new(IntResponse {
        value: count as i64,
    }))
}

pub(super) async fn subscribe(
    svc: &EmberService,
    request: Request<SubscribeRequest>,
) -> Result<Response<ReceiverStream<Result<SubscribeEvent, Status>>>, Status> {
    let req = request.into_inner();
    if req.channels.is_empty() && req.patterns.is_empty() {
        return Err(Status::invalid_argument(
            "at least one channel or pattern required",
        ));
    }

    let total_subs = req.channels.len() + req.patterns.len();
    if total_subs > svc.ctx.limits.max_subscriptions_per_conn {
        return Err(Status::invalid_argument(format!(
            "too many subscriptions ({total_subs}), max {}",
            svc.ctx.limits.max_subscriptions_per_conn
        )));
    }

    for pat in &req.patterns {
        if pat.len() > svc.ctx.limits.max_pattern_len {
            return Err(Status::invalid_argument(format!(
                "pattern too long ({} bytes), max {}",
                pat.len(),
                svc.ctx.limits.max_pattern_len
            )));
        }
    }

    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let pubsub = Arc::clone(&svc.pubsub);

    // collect all broadcast receivers
    let mut channel_rxs: Vec<(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )> = Vec::new();
    let mut pattern_rxs: Vec<(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )> = Vec::new();

    for ch in &req.channels {
        channel_rxs.push((ch.clone(), pubsub.subscribe(ch)));
    }
    for pat in &req.patterns {
        if let Some(rx) = pubsub.psubscribe(pat) {
            pattern_rxs.push((pat.clone(), rx));
        }
    }

    tokio::spawn(async move {
        loop {
            // build a future that races all receivers
            let event = tokio::select! {
                biased;
                result = recv_any_channel(&mut channel_rxs) => result,
                result = recv_any_pattern(&mut pattern_rxs) => result,
            };

            match event {
                Some(evt) => {
                    if tx.send(Ok(evt)).await.is_err() {
                        break; // client disconnected
                    }
                }
                None => {
                    // all receivers closed
                    break;
                }
            }
        }

        // cleanup subscriptions
        for (ch, _) in &channel_rxs {
            pubsub.unsubscribe(ch);
        }
        for (pat, _) in &pattern_rxs {
            pubsub.punsubscribe(pat);
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

pub(super) async fn pub_sub_channels(
    svc: &EmberService,
    request: Request<PubSubChannelsRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let pattern = request.into_inner().pattern;
    let names = svc.pubsub.channel_names(pattern.as_deref());
    Ok(Response::new(KeysResponse { keys: names }))
}

pub(super) async fn pub_sub_num_sub(
    svc: &EmberService,
    request: Request<PubSubNumSubRequest>,
) -> Result<Response<PubSubNumSubResponse>, Status> {
    let channels = request.into_inner().channels;
    let pairs = svc.pubsub.numsub(&channels);
    Ok(Response::new(PubSubNumSubResponse {
        counts: pairs
            .into_iter()
            .map(|(channel, count)| ChannelCount {
                channel,
                count: count as i64,
            })
            .collect(),
    }))
}

pub(super) async fn pub_sub_num_pat(
    svc: &EmberService,
    _request: Request<PubSubNumPatRequest>,
) -> Result<Response<IntResponse>, Status> {
    Ok(Response::new(IntResponse {
        value: svc.pubsub.active_patterns() as i64,
    }))
}

/// Waits for the next message on any channel subscription receiver.
/// Returns None when all receivers are closed.
async fn recv_any_channel(
    rxs: &mut [(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )],
) -> Option<SubscribeEvent> {
    if rxs.is_empty() {
        // no channel subscriptions — park forever so pattern branch can drive
        std::future::pending::<()>().await;
        return None;
    }

    loop {
        // poll each receiver in round-robin (tokio::select! on a slice
        // requires a loop because we can't use select! with dynamic count).
        for (_, rx) in rxs.iter_mut() {
            match rx.try_recv() {
                Ok(msg) => {
                    return Some(SubscribeEvent {
                        kind: "message".to_string(),
                        channel: msg.channel.to_string(),
                        data: msg.data.to_vec(),
                        pattern: None,
                    });
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return None,
            }
        }
        // yield to avoid busy-spinning
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Waits for the next message on any pattern subscription receiver.
/// Returns None when all receivers are closed.
async fn recv_any_pattern(
    rxs: &mut [(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )],
) -> Option<SubscribeEvent> {
    if rxs.is_empty() {
        std::future::pending::<()>().await;
        return None;
    }

    loop {
        for (pat, rx) in rxs.iter_mut() {
            match rx.try_recv() {
                Ok(msg) => {
                    return Some(SubscribeEvent {
                        kind: "pmessage".to_string(),
                        channel: msg.channel.to_string(),
                        data: msg.data.to_vec(),
                        pattern: Some(pat.clone()),
                    });
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return None,
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
