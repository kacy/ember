//! Vector set commands (`VADD`, `VSIM`, etc.).
//!
//! Enabled by the `vector` cargo feature. Requires the server to be built
//! with the same feature enabled.

use bytes::Bytes;
use ember_protocol::types::Frame;

use crate::connection::{Client, ClientError};

// --- public types ---

/// One result from a [`Client::vsim`] nearest-neighbour search.
#[derive(Debug, Clone)]
pub struct SimResult {
    /// The element name.
    pub element: Bytes,
    /// Distance from the query vector. Lower is closer (metric-dependent).
    pub distance: f32,
}

// --- decoders ---

fn vsim_results(frame: Frame, with_scores: bool) -> Result<Vec<SimResult>, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Null => return Ok(Vec::new()),
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for VSIM, got {other:?}"
            )))
        }
    };

    if !with_scores {
        return elems
            .into_iter()
            .map(|e| match e {
                Frame::Bulk(b) => Ok(SimResult {
                    element: b,
                    distance: 0.0,
                }),
                other => Err(ClientError::Protocol(format!(
                    "expected bulk element in VSIM, got {other:?}"
                ))),
            })
            .collect();
    }

    if elems.len() % 2 != 0 {
        return Err(ClientError::Protocol(format!(
            "VSIM WITHSCORES array has odd length ({})",
            elems.len()
        )));
    }

    let mut result = Vec::with_capacity(elems.len() / 2);
    let mut iter = elems.into_iter();
    while let (Some(elem_frame), Some(dist_frame)) = (iter.next(), iter.next()) {
        let element = match elem_frame {
            Frame::Bulk(b) => b,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk element in VSIM WITHSCORES, got {other:?}"
                )))
            }
        };
        let distance = match dist_frame {
            Frame::Bulk(b) => {
                let s = std::str::from_utf8(&b).map_err(|_| {
                    ClientError::Protocol("VSIM distance is not valid UTF-8".into())
                })?;
                s.parse::<f32>().map_err(|_| {
                    ClientError::Protocol(format!("VSIM distance is not a valid float: {s:?}"))
                })?
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk distance in VSIM WITHSCORES, got {other:?}"
                )))
            }
        };
        result.push(SimResult { element, distance });
    }
    Ok(result)
}

fn vget_vector(frame: Frame) -> Result<Option<Vec<f32>>, ClientError> {
    match frame {
        Frame::Array(elems) => {
            let floats = elems
                .into_iter()
                .map(|e| match e {
                    Frame::Bulk(b) => {
                        let s = std::str::from_utf8(&b).map_err(|_| {
                            ClientError::Protocol("vector component is not valid UTF-8".into())
                        })?;
                        s.parse::<f32>().map_err(|_| {
                            ClientError::Protocol(format!(
                                "vector component is not a valid float: {s:?}"
                            ))
                        })
                    }
                    other => Err(ClientError::Protocol(format!(
                        "expected bulk float in VGET response, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(floats))
        }
        Frame::Null => Ok(None),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected array or null for VGET, got {other:?}"
        ))),
    }
}

fn vinfo_pairs(frame: Frame) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
    match frame {
        Frame::Array(elems) => {
            if elems.len() % 2 != 0 {
                return Err(ClientError::Protocol(format!(
                    "VINFO array has odd length ({})",
                    elems.len()
                )));
            }
            let mut result = Vec::with_capacity(elems.len() / 2);
            let mut iter = elems.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                let key = match k {
                    Frame::Bulk(b) => b,
                    Frame::Simple(s) => Bytes::copy_from_slice(s.as_bytes()),
                    other => {
                        return Err(ClientError::Protocol(format!(
                            "expected bulk key in VINFO, got {other:?}"
                        )))
                    }
                };
                let val = match v {
                    Frame::Bulk(b) => b,
                    Frame::Integer(n) => Bytes::copy_from_slice(n.to_string().as_bytes()),
                    other => {
                        return Err(ClientError::Protocol(format!(
                            "expected bulk/integer value in VINFO, got {other:?}"
                        )))
                    }
                };
                result.push((key, val));
            }
            Ok(result)
        }
        Frame::Null => Ok(Vec::new()),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected array for VINFO, got {other:?}"
        ))),
    }
}

// --- Client methods ---

impl Client {
    /// Adds `element` with `vector` to the vector set at `key`. Returns `true`
    /// if the element was newly inserted, `false` if it already existed and
    /// was updated.
    ///
    /// Requires the server `vector` feature.
    pub async fn vadd(
        &mut self,
        key: &str,
        element: &str,
        vector: &[f32],
    ) -> Result<bool, ClientError> {
        let mut parts = Vec::with_capacity(3 + vector.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"VADD")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(element.as_bytes())));
        for &f in vector {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(
                f.to_string().as_bytes(),
            )));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected 0 or 1 for VADD, got {other:?}"
            ))),
        }
    }

    /// Adds multiple elements in a single round-trip. Returns the number of
    /// newly inserted vectors (updates do not count).
    ///
    /// The server requires all vectors in a batch to have the same
    /// dimensionality as the vector set (or creates it from the first batch).
    pub async fn vadd_batch(
        &mut self,
        key: &str,
        dim: usize,
        entries: &[(&str, &[f32])],
    ) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(5 + entries.len() * (1 + dim));
        parts.push(Frame::Bulk(Bytes::from_static(b"VADD_BATCH")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        parts.push(Frame::Bulk(Bytes::from_static(b"DIM")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(
            dim.to_string().as_bytes(),
        )));
        for (element, vector) in entries {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(element.as_bytes())));
            for &f in *vector {
                parts.push(Frame::Bulk(Bytes::copy_from_slice(
                    f.to_string().as_bytes(),
                )));
            }
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected integer for VADD_BATCH, got {other:?}"
            ))),
        }
    }

    /// Searches the vector set at `key` for the `count` nearest neighbours of
    /// `query`. Returns results sorted by distance (closest first).
    pub async fn vsim(
        &mut self,
        key: &str,
        query: &[f32],
        count: u32,
    ) -> Result<Vec<SimResult>, ClientError> {
        let mut parts = Vec::with_capacity(4 + query.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"VSIM")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        for &f in query {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(
                f.to_string().as_bytes(),
            )));
        }
        parts.push(Frame::Bulk(Bytes::from_static(b"COUNT")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(
            count.to_string().as_bytes(),
        )));
        parts.push(Frame::Bulk(Bytes::from_static(b"WITHSCORES")));
        let frame = self.send_frame(Frame::Array(parts)).await?;
        vsim_results(frame, true)
    }

    /// Removes `element` from the vector set at `key`. Returns `true` if the
    /// element was removed.
    pub async fn vrem(&mut self, key: &str, element: &str) -> Result<bool, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"VREM")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(element.as_bytes())),
            ]))
            .await?;
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected 0 or 1 for VREM, got {other:?}"
            ))),
        }
    }

    /// Returns the raw vector stored for `element` in the vector set at `key`,
    /// or `None` if not found.
    pub async fn vget(
        &mut self,
        key: &str,
        element: &str,
    ) -> Result<Option<Vec<f32>>, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"VGET")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(element.as_bytes())),
            ]))
            .await?;
        vget_vector(frame)
    }

    /// Returns the number of vectors in the vector set at `key`.
    pub async fn vcard(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"VCARD")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]))
            .await?;
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected integer for VCARD, got {other:?}"
            ))),
        }
    }

    /// Returns the dimensionality of vectors in the set at `key`.
    pub async fn vdim(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"VDIM")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]))
            .await?;
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected integer for VDIM, got {other:?}"
            ))),
        }
    }

    /// Returns metadata about the vector set at `key` as key-value pairs.
    pub async fn vinfo(&mut self, key: &str) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"VINFO")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]))
            .await?;
        vinfo_pairs(frame)
    }
}
