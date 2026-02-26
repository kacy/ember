//! Integration tests for bitmap commands: GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP.

use ember_protocol::Frame;

use crate::helpers::TestServer;

// --- GETBIT / SETBIT ---

#[tokio::test]
async fn setbit_and_getbit_round_trip() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // bit not set yet — should return 0
    assert_eq!(c.get_int(&["GETBIT", "bits", "7"]).await, 0);

    // set bit 7 → old value is 0
    assert_eq!(c.get_int(&["SETBIT", "bits", "7", "1"]).await, 0);

    // now bit 7 is 1
    assert_eq!(c.get_int(&["GETBIT", "bits", "7"]).await, 1);

    // clear it
    assert_eq!(c.get_int(&["SETBIT", "bits", "7", "0"]).await, 1);
    assert_eq!(c.get_int(&["GETBIT", "bits", "7"]).await, 0);
}

#[tokio::test]
async fn setbit_auto_extends_string() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // set bit 100 — the string has to grow to at least 13 bytes
    c.get_int(&["SETBIT", "bits", "100", "1"]).await;
    assert_eq!(c.get_int(&["GETBIT", "bits", "100"]).await, 1);

    // surrounding bits should be 0
    assert_eq!(c.get_int(&["GETBIT", "bits", "99"]).await, 0);
    assert_eq!(c.get_int(&["GETBIT", "bits", "101"]).await, 0);
}

#[tokio::test]
async fn getbit_on_missing_key_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["GETBIT", "nokey", "0"]).await, 0);
    assert_eq!(c.get_int(&["GETBIT", "nokey", "999"]).await, 0);
}

#[tokio::test]
async fn setbit_big_endian_ordering() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // set the MSB of byte 0 (bit 0)
    c.get_int(&["SETBIT", "bits", "0", "1"]).await;

    // the underlying string should be "\x80"
    match c.cmd(&["GET", "bits"]).await {
        Frame::Bulk(data) => assert_eq!(&data[..], &[0x80u8]),
        other => panic!("expected Bulk, got {other:?}"),
    }
}

// --- BITCOUNT ---

#[tokio::test]
async fn bitcount_all_bits() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // set all 8 bits via SETBIT to get a fully-set byte
    for i in 0..8 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }
    assert_eq!(c.get_int(&["BITCOUNT", "bits"]).await, 8);

    // a key with no bits set (auto-created, all zeros)
    c.get_int(&["SETBIT", "zero", "0", "0"]).await;
    assert_eq!(c.get_int(&["BITCOUNT", "zero"]).await, 0);
}

#[tokio::test]
async fn bitcount_byte_range() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // build two bytes: byte 0 = 0xFF, byte 1 = 0x00
    for i in 0..8 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }
    // byte 1 stays at 0x00 (auto-extended to accommodate bit 15)
    c.get_int(&["SETBIT", "bits", "15", "0"]).await;

    // byte 0 only: 8 set bits
    assert_eq!(c.get_int(&["BITCOUNT", "bits", "0", "0"]).await, 8);

    // byte 1 only: 0 set bits
    assert_eq!(c.get_int(&["BITCOUNT", "bits", "1", "1"]).await, 0);

    // both bytes: 8 set bits total
    assert_eq!(c.get_int(&["BITCOUNT", "bits", "0", "1"]).await, 8);
}

#[tokio::test]
async fn bitcount_bit_range_unit() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // set all 8 bits to get 0xFF
    for i in 0..8 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }

    // bits 0–3 → 4 set bits
    assert_eq!(c.get_int(&["BITCOUNT", "bits", "0", "3", "BIT"]).await, 4);
    // bits 0–7 → 8 set bits
    assert_eq!(c.get_int(&["BITCOUNT", "bits", "0", "7", "BIT"]).await, 8);
}

#[tokio::test]
async fn bitcount_missing_key_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["BITCOUNT", "nokey"]).await, 0);
}

// --- BITPOS ---

#[tokio::test]
async fn bitpos_first_set_bit() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // byte 0 = 0x00, byte 1 = 0xFF: first set bit is at position 8
    // extend to 2 bytes with byte1 bit 8 set
    c.get_int(&["SETBIT", "bits", "15", "0"]).await; // creates 2-byte string of zeros
    for i in 8..16 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }
    assert_eq!(c.get_int(&["BITPOS", "bits", "1"]).await, 8);
}

#[tokio::test]
async fn bitpos_first_clear_bit() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // byte 0 = 0xFF, byte 1 = 0x00: first clear bit is at position 8
    for i in 0..8 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }
    // extend to 2 bytes (byte 1 stays 0x00)
    c.get_int(&["SETBIT", "bits", "15", "0"]).await;
    assert_eq!(c.get_int(&["BITPOS", "bits", "0"]).await, 8);
}

#[tokio::test]
async fn bitpos_missing_key_clear() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // missing key: BITPOS 0 → 0
    assert_eq!(c.get_int(&["BITPOS", "nokey", "0"]).await, 0);
    // missing key: BITPOS 1 → -1
    let resp = c.cmd(&["BITPOS", "nokey", "1"]).await;
    match resp {
        Frame::Integer(n) => assert_eq!(n, -1),
        other => panic!("expected Integer(-1), got {other:?}"),
    }
}

#[tokio::test]
async fn bitpos_with_byte_range() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // bytes 0,1 = 0x00, byte 2 = 0xFF
    // extend to 3 bytes with first two bytes zero
    c.get_int(&["SETBIT", "bits", "23", "0"]).await; // creates 3-byte string of zeros
    for i in 16..24 {
        c.get_int(&["SETBIT", "bits", &i.to_string(), "1"]).await;
    }
    // first set bit in byte range [2, 2] is position 16
    assert_eq!(c.get_int(&["BITPOS", "bits", "1", "2", "2"]).await, 16);
}

// --- BITOP ---

#[tokio::test]
async fn bitop_and() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // a = 0xFF (all bits set)
    for i in 0..8 {
        c.get_int(&["SETBIT", "a", &i.to_string(), "1"]).await;
    }
    // b = 0x0F (lower nibble set: bits 4-7)
    for i in 4..8 {
        c.get_int(&["SETBIT", "b", &i.to_string(), "1"]).await;
    }
    c.get_int(&["SETBIT", "b", "0", "0"]).await; // ensure b is 1 byte

    // 0xFF AND 0x0F = 0x0F → 4 set bits
    let len = c.get_int(&["BITOP", "AND", "dest", "a", "b"]).await;
    assert_eq!(len, 1);
    assert_eq!(c.get_int(&["BITCOUNT", "dest"]).await, 4);
}

#[tokio::test]
async fn bitop_or() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // a = 0xF0 (upper nibble: bits 0-3)
    for i in 0..4 {
        c.get_int(&["SETBIT", "a", &i.to_string(), "1"]).await;
    }
    c.get_int(&["SETBIT", "a", "7", "0"]).await; // ensure a is 1 byte

    // b = 0x0F (lower nibble: bits 4-7)
    for i in 4..8 {
        c.get_int(&["SETBIT", "b", &i.to_string(), "1"]).await;
    }
    c.get_int(&["SETBIT", "b", "0", "0"]).await; // ensure b is 1 byte

    // 0xF0 OR 0x0F = 0xFF → 8 set bits
    let len = c.get_int(&["BITOP", "OR", "dest", "a", "b"]).await;
    assert_eq!(len, 1);
    assert_eq!(c.get_int(&["BITCOUNT", "dest"]).await, 8);
}

#[tokio::test]
async fn bitop_xor() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // both a and b = 0xFF
    for i in 0..8 {
        c.get_int(&["SETBIT", "a", &i.to_string(), "1"]).await;
        c.get_int(&["SETBIT", "b", &i.to_string(), "1"]).await;
    }

    // 0xFF XOR 0xFF = 0x00 → 0 set bits
    let len = c.get_int(&["BITOP", "XOR", "dest", "a", "b"]).await;
    assert_eq!(len, 1);
    assert_eq!(c.get_int(&["BITCOUNT", "dest"]).await, 0);
}

#[tokio::test]
async fn bitop_not() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // src = 0xFF
    for i in 0..8 {
        c.get_int(&["SETBIT", "src", &i.to_string(), "1"]).await;
    }

    // NOT 0xFF = 0x00 → 0 set bits
    let len = c.get_int(&["BITOP", "NOT", "dest", "src"]).await;
    assert_eq!(len, 1);
    assert_eq!(c.get_int(&["BITCOUNT", "dest"]).await, 0);
}

#[tokio::test]
async fn bitop_result_length_matches_longest_source() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // a is 3 bytes (bit 23 set), b is 1 byte
    c.get_int(&["SETBIT", "a", "0", "1"]).await;
    c.get_int(&["SETBIT", "a", "23", "1"]).await; // forces 3-byte string
    c.get_int(&["SETBIT", "b", "4", "1"]).await; // 1-byte string

    let len = c.get_int(&["BITOP", "AND", "dest", "a", "b"]).await;
    assert_eq!(len, 3);
}

#[tokio::test]
async fn bitop_missing_source_treated_as_zeros() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // a = 0xFF
    for i in 0..8 {
        c.get_int(&["SETBIT", "a", &i.to_string(), "1"]).await;
    }
    // "missing" key does not exist — treated as 0x00

    // AND with a zero-filled ghost → result is all zeros
    c.get_int(&["BITOP", "AND", "dest", "a", "missing"]).await;
    assert_eq!(c.get_int(&["BITCOUNT", "dest"]).await, 0);
}
