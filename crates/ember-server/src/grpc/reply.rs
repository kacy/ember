//! Conversions from the RESP reply of a command to gRPC response fields.
//!
//! Handlers run each request as a [`Frame`]-producing command (see
//! [`EmberService::run`](super::EmberService::run)), so these helpers are
//! the only place that knows how a reply is shaped. A reply of the wrong
//! shape is an internal error.

use ember_protocol::Frame;
use tonic::Status;

use super::proto::{FieldValue, ScoreMember};

fn unexpected(frame: &Frame) -> Status {
    Status::internal(format!("unexpected reply: {frame:?}"))
}

pub(super) fn int(frame: Frame) -> Result<i64, Status> {
    match frame {
        Frame::Integer(n) => Ok(n),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn opt_int(frame: Frame) -> Result<Option<i64>, Status> {
    match frame {
        Frame::Null => Ok(None),
        other => int(other).map(Some),
    }
}

/// An integer reply of 0 or 1, `+OK`, or null (false).
pub(super) fn flag(frame: Frame) -> Result<bool, Status> {
    match frame {
        Frame::Integer(n) => Ok(n != 0),
        Frame::Simple(_) => Ok(true),
        Frame::Null => Ok(false),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn opt_bytes(frame: Frame) -> Result<Option<Vec<u8>>, Status> {
    match frame {
        Frame::Bulk(b) => Ok(Some(b.to_vec())),
        Frame::Simple(s) => Ok(Some(s.into_bytes())),
        Frame::Null => Ok(None),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn bytes(frame: Frame) -> Result<Vec<u8>, Status> {
    opt_bytes(frame).map(Option::unwrap_or_default)
}

pub(super) fn string(frame: Frame) -> Result<String, Status> {
    bytes(frame).map(|b| String::from_utf8_lossy(&b).into_owned())
}

pub(super) fn opt_float(frame: Frame) -> Result<Option<f64>, Status> {
    match frame {
        Frame::Null => Ok(None),
        other => float(other).map(Some),
    }
}

pub(super) fn float(frame: Frame) -> Result<f64, Status> {
    let text = string(frame)?;
    text.parse()
        .map_err(|_| Status::internal(format!("not a number: {text}")))
}

/// The elements of an array reply. Null counts as empty.
pub(super) fn items(frame: Frame) -> Result<Vec<Frame>, Status> {
    match frame {
        Frame::Array(items) => Ok(items),
        Frame::Null => Ok(Vec::new()),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn byte_list(frame: Frame) -> Result<Vec<Vec<u8>>, Status> {
    items(frame)?.into_iter().map(bytes).collect()
}

/// A reply that is one bulk string without a count and an array with one,
/// as from SRANDMEMBER or HRANDFIELD.
pub(super) fn one_or_many(frame: Frame) -> Result<Vec<Vec<u8>>, Status> {
    match frame {
        Frame::Bulk(b) => Ok(vec![b.to_vec()]),
        other => byte_list(other),
    }
}

pub(super) fn opt_byte_list(frame: Frame) -> Result<Vec<Option<Vec<u8>>>, Status> {
    items(frame)?.into_iter().map(opt_bytes).collect()
}

pub(super) fn string_list(frame: Frame) -> Result<Vec<String>, Status> {
    items(frame)?.into_iter().map(string).collect()
}

/// A flat `[a1, b1, a2, b2, ...]` array, or a map, as pairs.
pub(super) fn pairs(frame: Frame) -> Result<Vec<(Frame, Frame)>, Status> {
    if let Frame::Map(pairs) = frame {
        return Ok(pairs);
    }
    let items = items(frame)?;
    if items.len() % 2 != 0 {
        return Err(Status::internal("reply has an odd number of elements"));
    }
    let mut iter = items.into_iter();
    let mut out = Vec::with_capacity(iter.len() / 2);
    while let (Some(a), Some(b)) = (iter.next(), iter.next()) {
        out.push((a, b));
    }
    Ok(out)
}

pub(super) fn field_values(frame: Frame) -> Result<Vec<FieldValue>, Status> {
    pairs(frame)?
        .into_iter()
        .map(|(field, value)| {
            Ok(FieldValue {
                field: string(field)?,
                value: bytes(value)?,
            })
        })
        .collect()
}

/// Members of a sorted-set reply. With scores the reply alternates member
/// and score; without them the scores are left at 0.
pub(super) fn score_members(frame: Frame, with_scores: bool) -> Result<Vec<ScoreMember>, Status> {
    if !with_scores {
        return Ok(string_list(frame)?
            .into_iter()
            .map(|member| ScoreMember { member, score: 0.0 })
            .collect());
    }
    pairs(frame)?
        .into_iter()
        .map(|(member, score)| {
            Ok(ScoreMember {
                member: string(member)?,
                score: float(score)?,
            })
        })
        .collect()
}

/// A SCAN-style `[cursor, [items...]]` reply.
pub(super) fn scan(frame: Frame) -> Result<(u64, Frame), Status> {
    let mut parts = items(frame)?.into_iter();
    let (Some(cursor), Some(page)) = (parts.next(), parts.next()) else {
        return Err(Status::internal("malformed scan reply"));
    };
    let cursor = string(cursor)?;
    let cursor = cursor
        .parse()
        .map_err(|_| Status::internal(format!("bad scan cursor: {cursor}")))?;
    Ok((cursor, page))
}

/// An LMPOP/ZMPOP-style `[key, elements]` reply, or null when nothing was
/// popped.
pub(super) fn keyed(frame: Frame) -> Result<Option<(String, Frame)>, Status> {
    if matches!(frame, Frame::Null) {
        return Ok(None);
    }
    let mut parts = items(frame)?.into_iter();
    let (Some(key), Some(elements)) = (parts.next(), parts.next()) else {
        return Err(Status::internal("malformed pop reply"));
    };
    Ok(Some((string(key)?, elements)))
}
