//! Parsers for vector commands.

use super::*;

/// Maximum number of dimensions in a vector. 65,536 is generous for any
/// real-world embedding model (OpenAI: 1536, Cohere: 4096) while preventing
/// memory abuse from absurdly large vectors.
const MAX_VECTOR_DIMS: usize = 65_536;

/// Maximum value for HNSW connectivity (M) and expansion parameters.
/// Values above 1024 give no practical benefit and waste memory.
const MAX_HNSW_PARAM: u64 = 1024;

/// Maximum number of results for VSIM. 10,000 is generous for any practical
/// similarity search while preventing OOM from unbounded result allocation.
const MAX_VSIM_COUNT: u64 = 10_000;

/// Maximum search beam width for VSIM. Same cap as MAX_HNSW_PARAM —
/// larger values cause worst-case O(n) graph traversal with no accuracy gain.
const MAX_VSIM_EF: u64 = MAX_HNSW_PARAM;

/// Maximum number of vectors in a single VADD_BATCH command. 10,000 keeps
/// per-command latency bounded while still being large enough to amortize
/// round-trip overhead for bulk inserts.
const MAX_VADD_BATCH_SIZE: usize = 10_000;

/// Parses METRIC / QUANT / M / EF flags from a slice of command arguments.
///
/// Returns `(metric, quantization, connectivity, expansion_add)`.
/// `cmd` is used in error messages (e.g., "VADD" or "VADD_BATCH").
pub(super) fn parse_vector_flags(
    args: &[Frame],
    cmd: &'static str,
) -> Result<(u8, u8, u32, u32), ProtocolError> {
    let mut metric: u8 = 0; // cosine default
    let mut quantization: u8 = 0; // f32 default
    let mut connectivity: u32 = 16;
    let mut expansion_add: u32 = 64;
    let mut idx = 0;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "METRIC" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: METRIC requires a value"
                    )));
                }
                let mut kw2 = [0u8; MAX_KEYWORD_LEN];
                let val = uppercase_arg(&args[idx], &mut kw2)?;
                metric = match val {
                    "COSINE" => 0,
                    "L2" => 1,
                    "IP" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "{cmd}: unknown metric '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "QUANT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: QUANT requires a value"
                    )));
                }
                let mut kw2 = [0u8; MAX_KEYWORD_LEN];
                let val = uppercase_arg(&args[idx], &mut kw2)?;
                quantization = match val {
                    "F32" => 0,
                    "F16" => 1,
                    "I8" | "Q8" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "{cmd}: unknown quantization '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "M" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: M requires a value"
                    )));
                }
                let m = parse_u64(&args[idx], cmd)?;
                if m > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: M value {m} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                connectivity = m as u32;
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: EF requires a value"
                    )));
                }
                let ef = parse_u64(&args[idx], cmd)?;
                if ef > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: EF value {ef} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                expansion_add = ef as u32;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "{cmd}: unexpected argument '{flag}'"
                )));
            }
        }
    }

    Ok((metric, quantization, connectivity, expansion_add))
}

/// VADD key element f32 [f32 ...] [METRIC COSINE|L2|IP] [QUANT F32|F16|I8] [M n] [EF n]
pub(super) fn parse_vadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + element + at least one float
    if args.len() < 3 {
        return Err(wrong_arity("VADD"));
    }

    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;

    // parse vector values until we hit a non-numeric argument, end, or dim limit
    let mut idx = 2;
    let mut vector = Vec::new();
    while idx < args.len() {
        if vector.len() >= MAX_VECTOR_DIMS {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "VADD: vector exceeds {MAX_VECTOR_DIMS} dimensions"
            )));
        }
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            if v.is_nan() || v.is_infinite() {
                return Err(ProtocolError::InvalidCommandFrame(
                    "VADD: vector components must be finite (no NaN/infinity)".into(),
                ));
            }
            vector.push(v);
            idx += 1;
        } else {
            break;
        }
    }

    if vector.is_empty() {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD: at least one vector dimension required".into(),
        ));
    }

    // parse optional flags
    let (metric, quantization, connectivity, expansion_add) =
        parse_vector_flags(&args[idx..], "VADD")?;

    Ok(Command::VAdd {
        key,
        element,
        vector,
        metric,
        quantization,
        connectivity,
        expansion_add,
    })
}

/// VADD_BATCH key DIM n [BINARY] element1 f32...|<blob> element2 f32...|<blob>
/// [METRIC COSINE|L2|IP] [QUANT F32|F16|I8] [M n] [EF n]
///
/// When BINARY is specified, each vector is a single bulk string of `dim * 4`
/// raw little-endian f32 bytes instead of `dim` separate text arguments.
/// This eliminates string serialization overhead on both client and server.
pub(super) fn parse_vadd_batch(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + DIM + n (even an empty batch needs the DIM declaration)
    if args.len() < 3 {
        return Err(wrong_arity("VADD_BATCH"));
    }

    let key = extract_string(&args[0])?;

    // require DIM keyword
    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let dim_kw = uppercase_arg(&args[1], &mut kw)?;
    if dim_kw != "DIM" {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD_BATCH: expected DIM keyword".into(),
        ));
    }

    let dim = parse_u64(&args[2], "VADD_BATCH")? as usize;
    if dim == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD_BATCH: DIM must be at least 1".into(),
        ));
    }
    if dim > MAX_VECTOR_DIMS {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "VADD_BATCH: DIM {dim} exceeds max {MAX_VECTOR_DIMS}"
        )));
    }

    // check for optional BINARY flag after DIM
    let mut idx = 3;
    let binary_mode = if idx < args.len() {
        let mut kw2 = [0u8; MAX_KEYWORD_LEN];
        matches!(uppercase_arg(&args[idx], &mut kw2), Ok("BINARY"))
    } else {
        false
    };
    if binary_mode {
        idx += 1;
    }

    let mut entries: Vec<(String, Vec<f32>)> = Vec::new();

    if binary_mode {
        // binary mode: each entry is element_name + single blob of dim*4 bytes
        let blob_len = dim * 4;
        let entry_len = 2; // element name + blob

        while idx < args.len() {
            if idx + entry_len > args.len() {
                break;
            }

            // peek: if the second arg isn't exactly blob_len bytes, we've
            // hit the flags section (flags are short text strings)
            let blob_bytes = extract_bytes(&args[idx + 1])?;
            if blob_bytes.len() != blob_len {
                break;
            }

            let element = extract_string(&args[idx])?;
            idx += 1;

            // skip extract_bytes again — reuse what we already have
            idx += 1;

            // reinterpret raw LE bytes as f32 slice
            let vector = bytes_to_f32_vec(&blob_bytes, dim)?;

            entries.push((element, vector));

            if entries.len() >= MAX_VADD_BATCH_SIZE {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VADD_BATCH: batch size exceeds max {MAX_VADD_BATCH_SIZE}"
                )));
            }
        }
    } else {
        // text mode: each entry is element_name followed by exactly `dim` floats.
        // we detect the end of entries by checking whether enough args remain
        // for a full entry (1 name + dim floats). this avoids misinterpreting
        // element names like "metric" as flags.
        let entry_len = 1 + dim; // element name + dim floats

        while idx < args.len() {
            // not enough remaining args for a full entry — must be flags
            if idx + entry_len > args.len() {
                break;
            }

            // peek: if the token after the element name isn't a valid float,
            // we've reached the flags section
            if dim > 0 {
                let peek = extract_string(&args[idx + 1])?;
                if peek.parse::<f32>().is_err() {
                    break;
                }
            }

            let element = extract_string(&args[idx])?;
            idx += 1;

            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                let s = extract_string(&args[idx])?;
                let v = s.parse::<f32>().map_err(|_| {
                    ProtocolError::InvalidCommandFrame(format!(
                        "VADD_BATCH: expected float, got '{s}'"
                    ))
                })?;
                if v.is_nan() || v.is_infinite() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD_BATCH: vector components must be finite (no NaN/infinity)".into(),
                    ));
                }
                vector.push(v);
                idx += 1;
            }

            entries.push((element, vector));

            if entries.len() >= MAX_VADD_BATCH_SIZE {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VADD_BATCH: batch size exceeds max {MAX_VADD_BATCH_SIZE}"
                )));
            }
        }
    }

    // parse optional flags (same logic as parse_vadd)
    let (metric, quantization, connectivity, expansion_add) =
        parse_vector_flags(&args[idx..], "VADD_BATCH")?;

    Ok(Command::VAddBatch {
        key,
        entries,
        dim,
        metric,
        quantization,
        connectivity,
        expansion_add,
    })
}

/// Converts a raw byte buffer of little-endian f32s into a Vec<f32>.
///
/// Validates that all values are finite (no NaN/infinity). The buffer
/// must be exactly `dim * 4` bytes.
pub(super) fn bytes_to_f32_vec(data: &[u8], dim: usize) -> Result<Vec<f32>, ProtocolError> {
    // compile-time endianness check — binary protocol assumes little-endian
    #[cfg(not(target_endian = "little"))]
    compile_error!("VADD_BATCH BINARY mode requires a little-endian target");

    debug_assert_eq!(data.len(), dim * 4);

    let mut vector = Vec::with_capacity(dim);
    for chunk in data.chunks_exact(4) {
        let v = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if !v.is_finite() {
            return Err(ProtocolError::InvalidCommandFrame(
                "VADD_BATCH BINARY: vector contains non-finite value (NaN/infinity)".into(),
            ));
        }
        vector.push(v);
    }
    Ok(vector)
}

/// VSIM key f32 [f32 ...] COUNT k [EF n] [WITHSCORES]
pub(super) fn parse_vsim(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + at least one float + COUNT + k
    if args.len() < 4 {
        return Err(wrong_arity("VSIM"));
    }

    let key = extract_string(&args[0])?;

    // parse query vector until we hit a non-numeric argument, end, or dim limit
    let mut idx = 1;
    let mut query = Vec::new();
    while idx < args.len() {
        if query.len() >= MAX_VECTOR_DIMS {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "VSIM: query exceeds {MAX_VECTOR_DIMS} dimensions"
            )));
        }
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            if v.is_nan() || v.is_infinite() {
                return Err(ProtocolError::InvalidCommandFrame(
                    "VSIM: query components must be finite (no NaN/infinity)".into(),
                ));
            }
            query.push(v);
            idx += 1;
        } else {
            break;
        }
    }

    if query.is_empty() {
        return Err(ProtocolError::InvalidCommandFrame(
            "VSIM: at least one query dimension required".into(),
        ));
    }

    // COUNT k is required
    let mut count: Option<usize> = None;
    let mut ef_search: usize = 0;
    let mut with_scores = false;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: COUNT requires a value".into(),
                    ));
                }
                let c = parse_u64(&args[idx], "VSIM")?;
                if c > MAX_VSIM_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VSIM: COUNT {c} exceeds max {MAX_VSIM_COUNT}"
                    )));
                }
                count = Some(c as usize);
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: EF requires a value".into(),
                    ));
                }
                let ef = parse_u64(&args[idx], "VSIM")?;
                if ef > MAX_VSIM_EF {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VSIM: EF {ef} exceeds max {MAX_VSIM_EF}"
                    )));
                }
                ef_search = ef as usize;
                idx += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VSIM: unexpected argument '{flag}'"
                )));
            }
        }
    }

    let count = count
        .ok_or_else(|| ProtocolError::InvalidCommandFrame("VSIM: COUNT is required".into()))?;

    Ok(Command::VSim {
        key,
        query,
        count,
        ef_search,
        with_scores,
    })
}

/// VREM key element
pub(super) fn parse_vrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("VREM"));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VRem { key, element })
}

/// VGET key element
pub(super) fn parse_vget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("VGET"));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VGet { key, element })
}

/// VCARD key
pub(super) fn parse_vcard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VCard { key })
}

/// VDIM key
pub(super) fn parse_vdim(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VDIM"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VDim { key })
}

/// VINFO key
pub(super) fn parse_vinfo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VINFO"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VInfo { key })
}
