//! String and bitmap command handlers for the gRPC service.

use bytes::Bytes;
use ember_protocol::command::{BitOpKind, BitRange, BitRangeUnit, SetExpire};
use ember_protocol::Command;
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{reply, EmberService};

pub(super) async fn get(
    svc: &EmberService,
    request: Request<GetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc.call(request, |r| Command::Get { key: r.key }).await?;
    get_response(frame)
}

pub(super) async fn set(
    svc: &EmberService,
    request: Request<SetRequest>,
) -> Result<Response<SetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Set {
            expire: set_expire(r.expire_seconds, r.expire_millis),
            key: r.key,
            value: Bytes::from(r.value),
            nx: r.nx,
            xx: r.xx,
        })
        .await?;
    Ok(Response::new(SetResponse {
        ok: reply::flag(frame)?,
    }))
}

pub(super) async fn m_get(
    svc: &EmberService,
    request: Request<MGetRequest>,
) -> Result<Response<MGetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::MGet { keys: r.keys })
        .await?;
    let values = reply::opt_byte_list(frame)?
        .into_iter()
        .map(|value| OptionalValue { value })
        .collect();
    Ok(Response::new(MGetResponse { values }))
}

pub(super) async fn m_set(
    svc: &EmberService,
    request: Request<MSetRequest>,
) -> Result<Response<MSetResponse>, Status> {
    svc.call(request, |r| Command::MSet {
        pairs: key_values(r.pairs),
    })
    .await?;
    Ok(Response::new(MSetResponse {}))
}

pub(super) async fn incr(
    svc: &EmberService,
    request: Request<IncrRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::Incr { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn incr_by(
    svc: &EmberService,
    request: Request<IncrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::IncrBy {
            key: r.key,
            delta: r.delta,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn decr_by(
    svc: &EmberService,
    request: Request<DecrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::DecrBy {
            key: r.key,
            delta: r.delta,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn incr_by_float(
    svc: &EmberService,
    request: Request<IncrByFloatRequest>,
) -> Result<Response<FloatResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::IncrByFloat {
            key: r.key,
            delta: r.delta,
        })
        .await?;
    Ok(Response::new(FloatResponse {
        value: reply::string(frame)?,
    }))
}

pub(super) async fn append(
    svc: &EmberService,
    request: Request<AppendRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Append {
            key: r.key,
            value: Bytes::from(r.value),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn strlen(
    svc: &EmberService,
    request: Request<StrlenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Strlen { key: r.key })
        .await?;
    int_response(frame)
}

pub(super) async fn decr(
    svc: &EmberService,
    request: Request<DecrRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::Decr { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn get_del(
    svc: &EmberService,
    request: Request<GetDelRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::GetDel { key: r.key })
        .await?;
    get_response(frame)
}

pub(super) async fn get_ex(
    svc: &EmberService,
    request: Request<GetExRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::GetEx {
            expire: getex_expiry(&r),
            key: r.key,
        })
        .await?;
    get_response(frame)
}

/// Maps the GETEX expiry fields to the command's convention: `None` leaves
/// the TTL alone, `Some(None)` removes it, and `Some(Some(_))` sets it.
fn getex_expiry(req: &GetExRequest) -> Option<Option<SetExpire>> {
    if req.persist {
        Some(None)
    } else {
        set_expire(req.expire_seconds, req.expire_millis).map(Some)
    }
}

/// The expiry for a pair of seconds and milliseconds fields, where 0 means
/// unset and milliseconds take precedence.
pub(super) fn set_expire(seconds: u64, millis: u64) -> Option<SetExpire> {
    if millis > 0 {
        Some(SetExpire::Px(millis))
    } else if seconds > 0 {
        Some(SetExpire::Ex(seconds))
    } else {
        None
    }
}

pub(super) async fn get_range(
    svc: &EmberService,
    request: Request<GetRangeRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::GetRange {
            key: r.key,
            start: r.start,
            end: r.end,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn set_range(
    svc: &EmberService,
    request: Request<SetRangeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            let offset = usize::try_from(r.offset)
                .map_err(|_| Status::invalid_argument("offset must not be negative"))?;
            Ok(Command::SetRange {
                key: r.key,
                offset,
                value: Bytes::from(r.value),
            })
        })
        .await?;
    int_response(frame)
}

pub(super) async fn getset(
    svc: &EmberService,
    request: Request<GetsetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::GetSet {
            key: r.key,
            value: Bytes::from(r.value),
        })
        .await?;
    get_response(frame)
}

pub(super) async fn msetnx(
    svc: &EmberService,
    request: Request<MsetnxRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            if r.pairs.is_empty() {
                return Err(Status::invalid_argument("at least one pair required"));
            }
            Ok(Command::MSetNx {
                pairs: key_values(r.pairs),
            })
        })
        .await?;
    Ok(Response::new(BoolResponse {
        value: reply::flag(frame)?,
    }))
}

pub(super) async fn getbit(
    svc: &EmberService,
    request: Request<GetbitRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::GetBit {
            key: r.key,
            offset: r.offset,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn setbit(
    svc: &EmberService,
    request: Request<SetbitRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::SetBit {
                key: r.key,
                offset: r.offset,
                value: bit(r.value, "bit value must be 0 or 1")?,
            })
        })
        .await?;
    int_response(frame)
}

pub(super) async fn bitcount(
    svc: &EmberService,
    request: Request<BitcountRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::BitCount {
            range: bit_range(r.has_range, r.start, r.end, &r.unit),
            key: r.key,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn bitpos(
    svc: &EmberService,
    request: Request<BitposRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::BitPos {
                bit: bit(r.bit, "bit argument must be 0 or 1")?,
                range: bit_range(r.has_range, r.start, r.end, &r.unit),
                key: r.key,
            })
        })
        .await?;
    int_response(frame)
}

pub(super) async fn bitop(
    svc: &EmberService,
    request: Request<BitopRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            if r.keys.is_empty() {
                return Err(Status::invalid_argument("at least one source key required"));
            }
            let op = match r.op.to_uppercase().as_str() {
                "AND" => BitOpKind::And,
                "OR" => BitOpKind::Or,
                "XOR" => BitOpKind::Xor,
                "NOT" => BitOpKind::Not,
                other => {
                    return Err(Status::invalid_argument(format!(
                        "unsupported BITOP operation: {other}"
                    )));
                }
            };
            Ok(Command::BitOp {
                op,
                dest: r.dest,
                keys: r.keys,
            })
        })
        .await?;
    int_response(frame)
}

fn bit(value: u32, msg: &'static str) -> Result<u8, Status> {
    match value {
        0 | 1 => Ok(value as u8),
        _ => Err(Status::invalid_argument(msg)),
    }
}

fn bit_range(has_range: bool, start: i64, end: i64, unit: &str) -> Option<BitRange> {
    has_range.then(|| BitRange {
        start,
        end,
        unit: if unit == "BIT" {
            BitRangeUnit::Bit
        } else {
            BitRangeUnit::Byte
        },
    })
}

fn key_values(pairs: Vec<KeyValue>) -> Vec<(String, Bytes)> {
    pairs
        .into_iter()
        .map(|kv| (kv.key, Bytes::from(kv.value)))
        .collect()
}

pub(super) fn int_response(frame: ember_protocol::Frame) -> Result<Response<IntResponse>, Status> {
    Ok(Response::new(IntResponse {
        value: reply::int(frame)?,
    }))
}

pub(super) fn get_response(frame: ember_protocol::Frame) -> Result<Response<GetResponse>, Status> {
    Ok(Response::new(GetResponse {
        value: reply::opt_bytes(frame)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getex_expiry_keeps_huge_seconds() {
        let req = GetExRequest {
            expire_seconds: u64::MAX,
            ..Default::default()
        };
        assert_eq!(getex_expiry(&req), Some(Some(SetExpire::Ex(u64::MAX))));
    }
}
