//! Pub/sub command handlers for the gRPC service.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use ember_protocol::Command;

use super::proto::*;
use super::strings::int_response;
use super::{reply, EmberService};

pub(super) async fn publish(
    svc: &EmberService,
    request: Request<PublishRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Publish {
            channel: r.channel,
            message: Bytes::from(r.message),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn subscribe(
    svc: &EmberService,
    request: Request<SubscribeRequest>,
) -> Result<Response<ReceiverStream<Result<SubscribeEvent, Status>>>, Status> {
    let r = request.get_ref();
    if !r.channels.is_empty() {
        svc.authorize(
            &request,
            &Command::Subscribe {
                channels: r.channels.clone(),
            },
        )?;
    }
    if !r.patterns.is_empty() {
        svc.authorize(
            &request,
            &Command::PSubscribe {
                patterns: r.patterns.clone(),
            },
        )?;
    }
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
        for (ch, rx) in channel_rxs {
            pubsub.unsubscribe(&ch, rx);
        }
        for (pat, rx) in pattern_rxs {
            pubsub.punsubscribe(&pat, rx);
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

pub(super) async fn pub_sub_channels(
    svc: &EmberService,
    request: Request<PubSubChannelsRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::PubSubChannels { pattern: r.pattern })
        .await?;
    Ok(Response::new(KeysResponse {
        keys: reply::string_list(frame)?,
    }))
}

pub(super) async fn pub_sub_num_sub(
    svc: &EmberService,
    request: Request<PubSubNumSubRequest>,
) -> Result<Response<PubSubNumSubResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::PubSubNumSub {
            channels: r.channels,
        })
        .await?;
    let counts = reply::pairs(frame)?
        .into_iter()
        .map(|(channel, count)| {
            Ok(ChannelCount {
                channel: reply::string(channel)?,
                count: reply::int(count)?,
            })
        })
        .collect::<Result<_, Status>>()?;
    Ok(Response::new(PubSubNumSubResponse { counts }))
}

pub(super) async fn pub_sub_num_pat(
    svc: &EmberService,
    request: Request<PubSubNumPatRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |_| Command::PubSubNumPat).await?;
    int_response(frame)
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
