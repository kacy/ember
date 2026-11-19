//! Pub/sub command handlers for the gRPC service.

use std::sync::Arc;

use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use ember_protocol::Command;

use super::proto::*;
use super::strings::int_response;
use super::{reply, EmberService};
use crate::pubsub::Subscriber;

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

    let mut subs = Subscriber::new(Arc::clone(&svc.pubsub));
    for ch in &req.channels {
        subs.subscribe(ch);
    }
    for pat in &req.patterns {
        subs.psubscribe(pat);
    }

    tokio::spawn(async move {
        loop {
            // a client that disconnects ends the stream even on a channel
            // that never gets a message. dropping `subs` releases its
            // subscriptions
            let msg = tokio::select! {
                msg = subs.recv() => msg,
                _ = tx.closed() => break,
            };
            let event = SubscribeEvent {
                kind: if msg.pattern.is_some() {
                    "pmessage"
                } else {
                    "message"
                }
                .to_string(),
                channel: msg.channel.to_string(),
                data: msg.data.to_vec(),
                pattern: msg.pattern.as_deref().map(str::to_owned),
            };
            if tx.send(Ok(event)).await.is_err() {
                break;
            }
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
