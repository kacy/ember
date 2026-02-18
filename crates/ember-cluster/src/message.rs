//! Binary wire format for cluster gossip messages.
//!
//! Uses a compact binary encoding for efficiency over the network.
//! All multi-byte integers are little-endian.

use std::io::{self, Read};
use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{NodeId, SlotRange};

/// Maximum number of members in a Welcome message or updates in a Ping/Ack.
/// Prevents allocation bombs from crafted messages.
const MAX_COLLECTION_COUNT: usize = 1024;

// Safe read helpers that return io::Error instead of panicking on truncated input.

fn safe_get_u8(buf: &mut &[u8]) -> io::Result<u8> {
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "need 1 byte"));
    }
    Ok(buf.get_u8())
}

fn safe_get_u16_le(buf: &mut &[u8]) -> io::Result<u16> {
    if buf.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "need 2 bytes"));
    }
    Ok(buf.get_u16_le())
}

fn safe_get_u64_le(buf: &mut &[u8]) -> io::Result<u64> {
    if buf.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "need 8 bytes"));
    }
    Ok(buf.get_u64_le())
}

/// Message types for the SWIM gossip protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum GossipMessage {
    /// Direct probe to check if a node is alive.
    Ping {
        seq: u64,
        sender: NodeId,
        /// Piggybacked state updates.
        updates: Vec<NodeUpdate>,
    },

    /// Request another node to probe a target on our behalf.
    PingReq {
        seq: u64,
        sender: NodeId,
        target: NodeId,
        target_addr: SocketAddr,
    },

    /// Response to a Ping or forwarded PingReq.
    Ack {
        seq: u64,
        sender: NodeId,
        /// Piggybacked state updates.
        updates: Vec<NodeUpdate>,
    },

    /// Join request from a new node.
    Join {
        sender: NodeId,
        sender_addr: SocketAddr,
    },

    /// Welcome response with current cluster state.
    Welcome {
        sender: NodeId,
        members: Vec<MemberInfo>,
    },
}

/// A state update about a node, piggybacked on protocol messages.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeUpdate {
    /// Node is alive with given incarnation number.
    Alive {
        node: NodeId,
        addr: SocketAddr,
        incarnation: u64,
    },
    /// Node is suspected to be failing.
    Suspect { node: NodeId, incarnation: u64 },
    /// Node has been confirmed dead.
    Dead { node: NodeId, incarnation: u64 },
    /// Node left the cluster gracefully.
    Left { node: NodeId },
    /// Node's slot ownership changed.
    SlotsChanged {
        node: NodeId,
        incarnation: u64,
        slots: Vec<SlotRange>,
    },
}

/// Information about a cluster member.
#[derive(Debug, Clone, PartialEq)]
pub struct MemberInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: u64,
    pub is_primary: bool,
    pub slots: Vec<SlotRange>,
}

// Wire format constants
const MSG_PING: u8 = 1;
const MSG_PING_REQ: u8 = 2;
const MSG_ACK: u8 = 3;
const MSG_JOIN: u8 = 4;
const MSG_WELCOME: u8 = 5;

const UPDATE_ALIVE: u8 = 1;
const UPDATE_SUSPECT: u8 = 2;
const UPDATE_DEAD: u8 = 3;
const UPDATE_LEFT: u8 = 4;
const UPDATE_SLOTS_CHANGED: u8 = 5;

impl GossipMessage {
    /// Serializes the message to bytes.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);
        self.encode_into(&mut buf);
        buf.freeze()
    }

    /// Serializes the message into the given buffer.
    pub fn encode_into(&self, buf: &mut BytesMut) {
        match self {
            GossipMessage::Ping {
                seq,
                sender,
                updates,
            } => {
                buf.put_u8(MSG_PING);
                buf.put_u64_le(*seq);
                encode_node_id(buf, sender);
                encode_updates(buf, updates);
            }
            GossipMessage::PingReq {
                seq,
                sender,
                target,
                target_addr,
            } => {
                buf.put_u8(MSG_PING_REQ);
                buf.put_u64_le(*seq);
                encode_node_id(buf, sender);
                encode_node_id(buf, target);
                encode_socket_addr(buf, target_addr);
            }
            GossipMessage::Ack {
                seq,
                sender,
                updates,
            } => {
                buf.put_u8(MSG_ACK);
                buf.put_u64_le(*seq);
                encode_node_id(buf, sender);
                encode_updates(buf, updates);
            }
            GossipMessage::Join {
                sender,
                sender_addr,
            } => {
                buf.put_u8(MSG_JOIN);
                encode_node_id(buf, sender);
                encode_socket_addr(buf, sender_addr);
            }
            GossipMessage::Welcome { sender, members } => {
                buf.put_u8(MSG_WELCOME);
                encode_node_id(buf, sender);
                let count = members.len().min(MAX_COLLECTION_COUNT);
                buf.put_u16_le(count as u16);
                for member in &members[..count] {
                    encode_member_info(buf, member);
                }
            }
        }
    }

    /// Deserializes a message from bytes.
    pub fn decode(mut buf: &[u8]) -> io::Result<Self> {
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "empty message",
            ));
        }

        let msg_type = safe_get_u8(&mut buf)?;
        match msg_type {
            MSG_PING => {
                let seq = safe_get_u64_le(&mut buf)?;
                let sender = decode_node_id(&mut buf)?;
                let updates = decode_updates(&mut buf)?;
                Ok(GossipMessage::Ping {
                    seq,
                    sender,
                    updates,
                })
            }
            MSG_PING_REQ => {
                let seq = safe_get_u64_le(&mut buf)?;
                let sender = decode_node_id(&mut buf)?;
                let target = decode_node_id(&mut buf)?;
                let target_addr = decode_socket_addr(&mut buf)?;
                Ok(GossipMessage::PingReq {
                    seq,
                    sender,
                    target,
                    target_addr,
                })
            }
            MSG_ACK => {
                let seq = safe_get_u64_le(&mut buf)?;
                let sender = decode_node_id(&mut buf)?;
                let updates = decode_updates(&mut buf)?;
                Ok(GossipMessage::Ack {
                    seq,
                    sender,
                    updates,
                })
            }
            MSG_JOIN => {
                let sender = decode_node_id(&mut buf)?;
                let sender_addr = decode_socket_addr(&mut buf)?;
                Ok(GossipMessage::Join {
                    sender,
                    sender_addr,
                })
            }
            MSG_WELCOME => {
                let sender = decode_node_id(&mut buf)?;
                let count = safe_get_u16_le(&mut buf)? as usize;
                if count > MAX_COLLECTION_COUNT {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("member count {count} exceeds limit"),
                    ));
                }
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    members.push(decode_member_info(&mut buf)?);
                }
                Ok(GossipMessage::Welcome { sender, members })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown message type: {other}"),
            )),
        }
    }
}

fn encode_node_id(buf: &mut BytesMut, id: &NodeId) {
    buf.put_slice(id.0.as_bytes());
}

fn decode_node_id(buf: &mut &[u8]) -> io::Result<NodeId> {
    if buf.len() < 16 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "not enough bytes for node id",
        ));
    }
    let mut bytes = [0u8; 16];
    buf.read_exact(&mut bytes)?;
    Ok(NodeId(uuid::Uuid::from_bytes(bytes)))
}

fn encode_socket_addr(buf: &mut BytesMut, addr: &SocketAddr) {
    match addr {
        SocketAddr::V4(v4) => {
            buf.put_u8(4);
            buf.put_slice(&v4.ip().octets());
            buf.put_u16_le(v4.port());
        }
        SocketAddr::V6(v6) => {
            buf.put_u8(6);
            buf.put_slice(&v6.ip().octets());
            buf.put_u16_le(v6.port());
        }
    }
}

fn decode_socket_addr(buf: &mut &[u8]) -> io::Result<SocketAddr> {
    if buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "not enough bytes for address type",
        ));
    }
    let addr_type = buf.get_u8();
    match addr_type {
        4 => {
            if buf.len() < 6 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "not enough bytes for ipv4 address",
                ));
            }
            let mut octets = [0u8; 4];
            buf.read_exact(&mut octets)?;
            let port = buf.get_u16_le();
            Ok(SocketAddr::from((octets, port)))
        }
        6 => {
            if buf.len() < 18 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "not enough bytes for ipv6 address",
                ));
            }
            let mut octets = [0u8; 16];
            buf.read_exact(&mut octets)?;
            let port = buf.get_u16_le();
            Ok(SocketAddr::from((octets, port)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown address type: {other}"),
        )),
    }
}

fn encode_updates(buf: &mut BytesMut, updates: &[NodeUpdate]) {
    let count = updates.len().min(MAX_COLLECTION_COUNT);
    buf.put_u16_le(count as u16);
    for update in &updates[..count] {
        encode_update(buf, update);
    }
}

fn encode_update(buf: &mut BytesMut, update: &NodeUpdate) {
    match update {
        NodeUpdate::Alive {
            node,
            addr,
            incarnation,
        } => {
            buf.put_u8(UPDATE_ALIVE);
            encode_node_id(buf, node);
            encode_socket_addr(buf, addr);
            buf.put_u64_le(*incarnation);
        }
        NodeUpdate::Suspect { node, incarnation } => {
            buf.put_u8(UPDATE_SUSPECT);
            encode_node_id(buf, node);
            buf.put_u64_le(*incarnation);
        }
        NodeUpdate::Dead { node, incarnation } => {
            buf.put_u8(UPDATE_DEAD);
            encode_node_id(buf, node);
            buf.put_u64_le(*incarnation);
        }
        NodeUpdate::Left { node } => {
            buf.put_u8(UPDATE_LEFT);
            encode_node_id(buf, node);
        }
        NodeUpdate::SlotsChanged {
            node,
            incarnation,
            slots,
        } => {
            buf.put_u8(UPDATE_SLOTS_CHANGED);
            encode_node_id(buf, node);
            buf.put_u64_le(*incarnation);
            let count = slots.len().min(MAX_COLLECTION_COUNT);
            buf.put_u16_le(count as u16);
            for slot in &slots[..count] {
                buf.put_u16_le(slot.start);
                buf.put_u16_le(slot.end);
            }
        }
    }
}

fn decode_updates(buf: &mut &[u8]) -> io::Result<Vec<NodeUpdate>> {
    let count = safe_get_u16_le(buf)? as usize;
    if count > MAX_COLLECTION_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("update count {count} exceeds limit"),
        ));
    }
    let mut updates = Vec::with_capacity(count);
    for _ in 0..count {
        updates.push(decode_update(buf)?);
    }
    Ok(updates)
}

fn decode_update(buf: &mut &[u8]) -> io::Result<NodeUpdate> {
    let update_type = safe_get_u8(buf)?;
    match update_type {
        UPDATE_ALIVE => {
            let node = decode_node_id(buf)?;
            let addr = decode_socket_addr(buf)?;
            let incarnation = safe_get_u64_le(buf)?;
            Ok(NodeUpdate::Alive {
                node,
                addr,
                incarnation,
            })
        }
        UPDATE_SUSPECT => {
            let node = decode_node_id(buf)?;
            let incarnation = safe_get_u64_le(buf)?;
            Ok(NodeUpdate::Suspect { node, incarnation })
        }
        UPDATE_DEAD => {
            let node = decode_node_id(buf)?;
            let incarnation = safe_get_u64_le(buf)?;
            Ok(NodeUpdate::Dead { node, incarnation })
        }
        UPDATE_LEFT => {
            let node = decode_node_id(buf)?;
            Ok(NodeUpdate::Left { node })
        }
        UPDATE_SLOTS_CHANGED => {
            let node = decode_node_id(buf)?;
            let incarnation = safe_get_u64_le(buf)?;
            let count = safe_get_u16_le(buf)? as usize;
            if count > MAX_COLLECTION_COUNT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("slot range count {count} exceeds limit"),
                ));
            }
            let mut slots = Vec::with_capacity(count);
            for _ in 0..count {
                let start = safe_get_u16_le(buf)?;
                let end = safe_get_u16_le(buf)?;
                slots.push(SlotRange::try_new(start, end)?);
            }
            Ok(NodeUpdate::SlotsChanged {
                node,
                incarnation,
                slots,
            })
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown update type: {other}"),
        )),
    }
}

fn encode_member_info(buf: &mut BytesMut, member: &MemberInfo) {
    encode_node_id(buf, &member.id);
    encode_socket_addr(buf, &member.addr);
    buf.put_u64_le(member.incarnation);
    buf.put_u8(if member.is_primary { 1 } else { 0 });
    let slot_count = member.slots.len().min(MAX_COLLECTION_COUNT);
    buf.put_u16_le(slot_count as u16);
    for slot in &member.slots[..slot_count] {
        buf.put_u16_le(slot.start);
        buf.put_u16_le(slot.end);
    }
}

fn decode_member_info(buf: &mut &[u8]) -> io::Result<MemberInfo> {
    let id = decode_node_id(buf)?;
    let addr = decode_socket_addr(buf)?;
    let incarnation = safe_get_u64_le(buf)?;
    let is_primary = safe_get_u8(buf)? != 0;
    let slot_count = safe_get_u16_le(buf)? as usize;
    if slot_count > MAX_COLLECTION_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("slot range count {slot_count} exceeds limit"),
        ));
    }
    let mut slots = Vec::with_capacity(slot_count);
    for _ in 0..slot_count {
        let start = safe_get_u16_le(buf)?;
        let end = safe_get_u16_le(buf)?;
        slots.push(SlotRange::try_new(start, end)?);
    }
    Ok(MemberInfo {
        id,
        addr,
        incarnation,
        is_primary,
        slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn test_addr() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), 6379))
    }

    fn test_addr_v6() -> SocketAddr {
        SocketAddr::from((Ipv6Addr::LOCALHOST, 6379))
    }

    #[test]
    fn ping_roundtrip() {
        let msg = GossipMessage::Ping {
            seq: 42,
            sender: NodeId::new(),
            updates: vec![],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn ping_with_updates() {
        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let msg = GossipMessage::Ping {
            seq: 100,
            sender: node1,
            updates: vec![
                NodeUpdate::Alive {
                    node: node2,
                    addr: test_addr(),
                    incarnation: 5,
                },
                NodeUpdate::Suspect {
                    node: node1,
                    incarnation: 3,
                },
            ],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn ping_req_roundtrip() {
        let msg = GossipMessage::PingReq {
            seq: 99,
            sender: NodeId::new(),
            target: NodeId::new(),
            target_addr: test_addr(),
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn ack_roundtrip() {
        let msg = GossipMessage::Ack {
            seq: 42,
            sender: NodeId::new(),
            updates: vec![NodeUpdate::Dead {
                node: NodeId::new(),
                incarnation: 10,
            }],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn join_roundtrip() {
        let msg = GossipMessage::Join {
            sender: NodeId::new(),
            sender_addr: test_addr(),
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn welcome_roundtrip() {
        let msg = GossipMessage::Welcome {
            sender: NodeId::new(),
            members: vec![
                MemberInfo {
                    id: NodeId::new(),
                    addr: test_addr(),
                    incarnation: 1,
                    is_primary: true,
                    slots: vec![SlotRange::new(0, 5460)],
                },
                MemberInfo {
                    id: NodeId::new(),
                    addr: test_addr(),
                    incarnation: 2,
                    is_primary: false,
                    slots: vec![],
                },
            ],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn ipv6_address() {
        let msg = GossipMessage::Join {
            sender: NodeId::new(),
            sender_addr: test_addr_v6(),
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn all_update_types() {
        let node = NodeId::new();
        let updates = vec![
            NodeUpdate::Alive {
                node,
                addr: test_addr(),
                incarnation: 1,
            },
            NodeUpdate::Suspect {
                node,
                incarnation: 2,
            },
            NodeUpdate::Dead {
                node,
                incarnation: 3,
            },
            NodeUpdate::Left { node },
            NodeUpdate::SlotsChanged {
                node,
                incarnation: 4,
                slots: vec![SlotRange::new(0, 5460)],
            },
        ];
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: node,
            updates,
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn slots_changed_empty_roundtrip() {
        let node = NodeId::new();
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: node,
            updates: vec![NodeUpdate::SlotsChanged {
                node,
                incarnation: 1,
                slots: vec![],
            }],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn slots_changed_multiple_ranges_roundtrip() {
        let node = NodeId::new();
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: node,
            updates: vec![NodeUpdate::SlotsChanged {
                node,
                incarnation: 5,
                slots: vec![
                    SlotRange::new(0, 5460),
                    SlotRange::new(5461, 10922),
                    SlotRange::new(10923, 16383),
                ],
            }],
        };
        let encoded = msg.encode();
        let decoded = GossipMessage::decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn empty_message_error() {
        let result = GossipMessage::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_message_type_error() {
        let result = GossipMessage::decode(&[255]);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_slot_range_in_welcome_rejected() {
        // craft a Welcome message with an invalid slot range (start > end)
        let mut buf = BytesMut::new();
        buf.put_u8(MSG_WELCOME);
        encode_node_id(&mut buf, &NodeId::new());
        buf.put_u16_le(1); // 1 member
        encode_node_id(&mut buf, &NodeId::new());
        encode_socket_addr(&mut buf, &test_addr());
        buf.put_u64_le(1); // incarnation
        buf.put_u8(1); // is_primary
        buf.put_u16_le(1); // 1 slot range
        buf.put_u16_le(5000); // start
        buf.put_u16_le(100); // end < start — invalid

        let result = GossipMessage::decode(&buf);
        assert!(result.is_err(), "should reject inverted slot range");
    }

    #[test]
    fn out_of_range_slot_in_welcome_rejected() {
        // craft a Welcome message with a slot >= 16384
        let mut buf = BytesMut::new();
        buf.put_u8(MSG_WELCOME);
        encode_node_id(&mut buf, &NodeId::new());
        buf.put_u16_le(1);
        encode_node_id(&mut buf, &NodeId::new());
        encode_socket_addr(&mut buf, &test_addr());
        buf.put_u64_le(1);
        buf.put_u8(1);
        buf.put_u16_le(1); // 1 slot range
        buf.put_u16_le(0);
        buf.put_u16_le(16384); // out of range

        let result = GossipMessage::decode(&buf);
        assert!(result.is_err(), "should reject slot >= 16384");
    }
}
