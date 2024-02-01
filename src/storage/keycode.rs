//! Order-preserving encodings for use in keys.
//!
//! bool:    0x00 for false, 0x01 for true.
//! Vec<u8>: 0x00 is escaped with 0x00 0xff, terminated with 0x00 0x00.
//! String:  Like Vec<u8>.
//! u64:     Big-endian binary representation.
//! i64:     Big-endian binary representation, with sign bit flipped.
//! f64:     Big-endian binary representation, with sign bit flipped if +, all flipped if -.
//! Value:   Like above, with type prefix 0x00=Null 0x01=Boolean 0x02=Float 0x03=Integer 0x04=String

// use crate::sql::types::Value;
use std::convert::TryInto;

use super::mvcc::MVCCError;

/// Encodes a boolean, using 0x00 for false and 0x01 for true.
pub fn encode_boolean(bool: bool) -> u8 {
    match bool {
        true => 0x01,
        false => 0x00,
    }
}

/// Decodes a boolean. See encode_boolean() for format.
pub fn decode_boolean(byte: u8) -> Result<bool, MVCCError> {
    match byte {
        0x00 => Ok(false),
        0x01 => Ok(true),
        b => Err(MVCCError::Serialization(format!(
            "Invalid boolean value {:?}",
            b
        ))),
    }
}

/// Decodes a boolean from a slice and shrinks the slice.
pub fn take_boolean(bytes: &[u8]) -> Result<bool, MVCCError> {
    take_byte(bytes).and_then(decode_boolean)
}

/// Encodes a byte vector. 0x00 is escaped as 0x00 0xff, and 0x00 0x00 is used as a terminator.
/// See: https://activesphere.com/blog/2018/08/17/order-preserving-serialization
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    // flat_map() obscures Iterator.size_hint(), so we explicitly allocate.
    // See also: https://github.com/rust-lang/rust/issues/45840
    let mut encoded = Vec::with_capacity(bytes.len() + 2);
    encoded.extend(
        bytes
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]),
    );
    encoded
}

/// Takes a single byte from a slice and shortens it, without any escaping.
pub fn take_byte(bytes: &[u8]) -> Result<u8, MVCCError> {
    if bytes.is_empty() {
        return Err(MVCCError::Serialization("Unexpected end of bytes".into()));
    }
    let b = bytes[0];
    // *bytes = &bytes[1..];
    Ok(b)
}

/// Decodes a byte vector from a slice and shortens the slice. See encode_bytes() for format.
pub fn take_bytes(bytes: &[u8]) -> Result<Vec<u8>, MVCCError> {
    // Since we're generally decoding keys, and these are short, we begin allocating at half of
    // the byte size.
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut iter = bytes.iter().enumerate();
    loop {
        match iter.next().map(|(_, b)| b) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,        // 0x00 0x00 is terminator
                Some((_, 0xff)) => decoded.push(0x00), // 0x00 0xff is escape sequence for 0x00
                Some((_, b)) => {
                    return Err(MVCCError::Serialization(format!(
                        "Invalid byte escape {:?}",
                        b
                    )))
                }
                None => return Err(MVCCError::Serialization("Unexpected end of bytes".into())),
            },
            Some(b) => decoded.push(*b),
            None => return Err(MVCCError::Serialization("Unexpected end of bytes".into())),
        }
    };
    // *bytes = &bytes[taken..];
    Ok(decoded)
}

/// Encodes an f64. Uses big-endian form, and flip sign bit to 1 if 0, otherwise flip all bits.
/// This preserves the natural numerical ordering, with NaN at the end.
pub fn encode_f64(n: f64) -> [u8; 8] {
    let mut bytes = n.to_be_bytes();
    match (bytes[0] >> 7) & 1 {
        0 => bytes[0] ^= 1 << 7,
        _ => bytes.iter_mut().for_each(|b| *b = !*b),
    }
    bytes
}

/// Decodes an f64. See encode_f64() for format.
pub fn decode_f64(mut bytes: [u8; 8]) -> f64 {
    match (bytes[0] >> 7) & 1 {
        1 => bytes[0] ^= 1 << 7,
        _ => bytes.iter_mut().for_each(|b| *b = !*b),
    }
    f64::from_be_bytes(bytes)
}

/// Decodes an f64 from a slice and shrinks the slice.
pub fn take_f64(bytes: &mut &[u8]) -> Result<f64,MVCCError> {
    if bytes.len() < 8 {
        return Err(MVCCError::Serialization(format!(
            "Unable to decode f64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_f64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

/// Encodes an i64. Uses big-endian form, with the first bit flipped to order negative/positive
/// numbers correctly.
pub fn encode_i64(n: i64) -> [u8; 8] {
    let mut bytes = n.to_be_bytes();
    bytes[0] ^= 1 << 7; // Flip left-most bit in the first byte, i.e. sign bit.
    bytes
}

/// Decodes an i64. See encode_i64() for format.
pub fn decode_i64(mut bytes: [u8; 8]) -> i64 {
    bytes[0] ^= 1 << 7;
    i64::from_be_bytes(bytes)
}

/// Decodes a i64 from a slice and shrinks the slice.
pub fn take_i64(bytes: &mut &[u8]) -> Result<i64,MVCCError> {
    if bytes.len() < 8 {
        return Err(MVCCError::Serialization(format!(
            "Unable to decode i64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_i64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

/// Encodes a string. Simply converts to a byte vector and encodes that.
pub fn encode_string(string: &str) -> Vec<u8> {
    encode_bytes(string.as_bytes())
}

/// Decodes a string from a slice and shrinks the slice.
pub fn take_string(bytes: &mut &[u8]) -> Result<String, MVCCError> {
    Ok(String::from_utf8(take_bytes(bytes)?)?)
}

/// Encodes a u64. Simply uses the big-endian form, which preserves order. Does not attempt to
/// compress it, for now.
pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// Decodes a u64. See encode_u64() for format.
pub fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

/// Decodes a u64 from a slice and shrinks the slice.
pub fn take_u64(bytes: &[u8]) -> Result<u64, MVCCError> {
    if bytes.len() < 8 {
        return Err(MVCCError::Serialization(format!(
            "Unable to decode u64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_u64(bytes[0..8].try_into()?);
    // *bytes = &bytes[8..];
    Ok(n)
}


