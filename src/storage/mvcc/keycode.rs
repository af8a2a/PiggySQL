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

use serde::{Deserialize, Serialize};

use crate::errors::*;

use super::Version;


#[derive(Debug, Deserialize, Serialize)]
pub enum Key {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.   
    /// Write set for a transaction version.
    TxnWrite(Version, Vec<u8>),
    /// A versioned key/value pair.
    Version(Vec<u8>, Version),
    /// Unversioned non-transactional key/value pairs. These exist separately
    /// from versioned keys, i.e. the unversioned key "foo" is entirely
    /// independent of the versioned key "foo@7". These are mostly used
    /// for metadata.
    Unversioned(Vec<u8>),
}
/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
pub enum KeyPrefix {
    NextVersion,
    TxnActive,
    TxnActiveSnapshot,
    TxnWrite(Version),
    Version(Vec<u8>),
    Unversioned,
}



impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            KeyPrefix::NextVersion => Ok(vec![0x01]),
            KeyPrefix::TxnActive => Ok(vec![0x02]),
            KeyPrefix::TxnActiveSnapshot => Ok(vec![0x03]),
            KeyPrefix::TxnWrite(version) => Ok([&[0x04][..], &encode_u64(*version)].concat()),
            KeyPrefix::Version(key) => Ok([&[0x05][..], &encode_bytes(&key)].concat()),
            KeyPrefix::Unversioned => Ok(vec![0x06]),
        }
    }
}

impl Key {
    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        let bytes = &mut bytes;
        Ok(match take_byte(bytes)? {
            0x01 => Self::NextVersion,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnActiveSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnWrite(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Version(take_bytes(bytes)?.into(), take_u64(bytes)?),
            0x06 => Self::Unversioned(take_bytes(bytes)?.into()),
            _ => {
                return Err(DatabaseError::InternalError(format!(
                    "Invalid key prefix {:?}",
                    bytes[0]
                )))
            }
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Key::NextVersion => Ok(vec![0x01]),
            Key::TxnActive(version) => Ok([&[0x02][..], &encode_u64(*version)].concat()),
            Key::TxnActiveSnapshot(version) => Ok([&[0x03][..], &encode_u64(*version)].concat()),
            Key::TxnWrite(version, key) => {
                Ok([&[0x04][..], &encode_u64(*version), &encode_bytes(&key)].concat())
            }
            Key::Version(key, version) => {
                Ok([&[0x05][..], &encode_bytes(&key), &encode_u64(*version)].concat())
            }
            Key::Unversioned(key) => Ok([&[0x06][..], &encode_bytes(&key)].concat()),
        }
    }
}






/// Encodes a boolean, using 0x00 for false and 0x01 for true.
pub fn encode_boolean(bool: bool) -> u8 {
    match bool {
        true => 0x01,
        false => 0x00,
    }
}

/// Decodes a boolean. See encode_boolean() for format.
pub fn decode_boolean(byte: u8) -> Result<bool> {
    match byte {
        0x00 => Ok(false),
        0x01 => Ok(true),
        b => Err(DatabaseError::InternalError(format!(
            "Invalid boolean value {:?}",
            b
        ))),
    }
}

/// Decodes a boolean from a slice and shrinks the slice.
pub fn take_boolean(bytes: &mut &[u8]) -> Result<bool> {
    take_byte(bytes).and_then(decode_boolean)
}

/// Encodes a byte vector. 0x00 is escaped as 0x00 0xff, and 0x00 0x00 is used as a terminator.
/// See: https://activesphere.com/blog/2018/08/17/order-preserving-Internal
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
pub fn take_byte(bytes:&mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(DatabaseError::InternalError("Unexpected end of bytes".into()));
    }
    let b = bytes[0];
     *bytes = &bytes[1..];
    Ok(b)
}

/// Decodes a byte vector from a slice and shortens the slice. See encode_bytes() for format.
pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    // Since we're generally decoding keys, and these are short, we begin allocating at half of
    // the byte size.
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut iter = bytes.iter().enumerate();
    let taken=loop {
        match iter.next().map(|(_, b)| b) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,        // 0x00 0x00 is terminator
                Some((_, 0xff)) => decoded.push(0x00), // 0x00 0xff is escape sequence for 0x00
                Some((_, b)) => {
                    return Err(DatabaseError::InternalError(format!(
                        "Invalid byte escape {:?}",
                        b
                    )))
                }
                None => return Err(DatabaseError::InternalError("Unexpected end of bytes".into())),
            },
            Some(b) => decoded.push(*b),
            None => return Err(DatabaseError::InternalError("Unexpected end of bytes".into())),
        }
    };
     *bytes = &bytes[taken..];
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
pub fn take_f64(bytes: &mut &[u8]) -> Result<f64> {
    if bytes.len() < 8 {
        return Err(DatabaseError::InternalError(format!(
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
pub fn take_i64(bytes: &mut &[u8]) -> Result<i64> {
    if bytes.len() < 8 {
        return Err(DatabaseError::InternalError(format!(
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
pub fn take_string(bytes: &mut &[u8]) -> Result<String> {
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
pub fn take_u64(bytes:&mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(DatabaseError::InternalError(format!(
            "Unable to decode u64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_u64(bytes[0..8].try_into()?);
     *bytes = &bytes[8..];
    Ok(n)
}


#[cfg(test)]
pub mod tests {
    use super::*;
    #[test]
    fn encode_boolean() -> Result<()> {
        use super::encode_boolean;
        assert_eq!(encode_boolean(false), 0x00);
        assert_eq!(encode_boolean(true), 0x01);
        Ok(())
    }

    #[test]
    fn decode_boolean() -> Result<()> {
        use super::decode_boolean;
        assert_eq!(decode_boolean(0x00)?, false);
        assert_eq!(decode_boolean(0x01)?, true);
        assert!(decode_boolean(0x02).is_err());
        Ok(())
    }
    #[test]
    fn take_boolean() -> Result<()> {
        use super::take_boolean;
        let mut bytes: &[u8] = &[0x01, 0xaf];
        assert_eq!(take_boolean(&mut bytes)?, true);
        assert_eq!(bytes, &[0xaf]);
        Ok(())
    }

    #[test]
    fn encode_bytes() -> Result<()> {
        use super::encode_bytes;
        assert_eq!(encode_bytes(&[]), vec![0x00, 0x00]);
        assert_eq!(encode_bytes(&[0x01, 0x02, 0x03]), vec![0x01, 0x02, 0x03, 0x00, 0x00]);
        assert_eq!(encode_bytes(&[0x00, 0x01, 0x02]), vec![0x00, 0xff, 0x01, 0x02, 0x00, 0x00]);
        Ok(())
    }

    #[test]
    fn take_bytes() -> Result<()> {
        use super::take_bytes;

        let mut bytes: &[u8] = &[];
        assert!(take_bytes(&mut bytes).is_err());

        let mut bytes: &[u8] = &[0x00, 0x00];
        assert_eq!(take_bytes(&mut bytes)?, Vec::<u8>::new());
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x01, 0x02, 0x03, 0x00, 0x00, 0xa0, 0xb0];
        assert_eq!(take_bytes(&mut bytes)?, &[0x01, 0x02, 0x03]);
        assert_eq!(bytes, &[0xa0, 0xb0]);

        let mut bytes: &[u8] = &[0x00, 0xff, 0x01, 0x02, 0x00, 0x00];
        assert_eq!(take_bytes(&mut bytes)?, &[0x00, 0x01, 0x02]);
        assert!(bytes.is_empty());

        assert!(take_bytes(&mut &[0x00][..]).is_err());
        assert!(take_bytes(&mut &[0x01][..]).is_err());
        assert!(take_bytes(&mut &[0x00, 0x01, 0x00, 0x00][..]).is_err());

        Ok(())
    }

    #[test]
    fn encode_f64() -> Result<()> {
        use super::encode_f64;
        use std::f64;
        use std::f64::consts::PI;
        assert_eq!(encode_f64(f64::NEG_INFINITY), [0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(encode_f64(-PI * 1e100), [0x2b, 0x33, 0x46, 0x0a, 0x3c, 0x0d, 0x14, 0x7b]);
        assert_eq!(encode_f64(-PI * 1e2), [0x3f, 0x8c, 0x5d, 0x73, 0xa6, 0x2a, 0xbc, 0xc4]);
        assert_eq!(encode_f64(-0_f64), [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(encode_f64(0_f64), [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_f64(PI), [0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18]);
        assert_eq!(encode_f64(PI * 1e2), [0xc0, 0x73, 0xa2, 0x8c, 0x59, 0xd5, 0x43, 0x3b]);
        assert_eq!(encode_f64(PI * 1e100), [0xd4, 0xcc, 0xb9, 0xf5, 0xc3, 0xf2, 0xeb, 0x84]);
        assert_eq!(encode_f64(f64::INFINITY), [0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_f64(f64::NAN), [0xff, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        Ok(())
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn decode_f64() -> Result<()> {
        use super::{decode_f64, encode_f64};
        use std::f64;
        use std::f64::consts::PI;
        assert_eq!(decode_f64(encode_f64(f64::NEG_INFINITY)), f64::NEG_INFINITY);
        assert_eq!(decode_f64(encode_f64(-PI)), -PI);
        assert_eq!(decode_f64(encode_f64(-0.0)), -0.0);
        assert_eq!(decode_f64(encode_f64(0.0)), 0.0);
        assert_eq!(decode_f64(encode_f64(PI)), PI);
        assert_eq!(decode_f64(encode_f64(f64::INFINITY)), f64::INFINITY);
        assert!(decode_f64(encode_f64(f64::NAN)).is_nan());
        Ok(())
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn take_f64() -> Result<()> {
        use super::take_f64;

        let mut bytes: &[u8] = &[];
        assert!(take_f64(&mut bytes).is_err());

        let mut bytes: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        assert!(take_f64(&mut bytes).is_err());
        assert_eq!(bytes.len(), 7);

        let mut bytes: &[u8] = &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(take_f64(&mut bytes)?, 0.0);
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xaf];
        assert_eq!(take_f64(&mut bytes)?, -0.0);
        assert_eq!(bytes, &[0xaf]);

        Ok(())
    }

    #[test]
    fn encode_i64() -> Result<()> {
        use super::encode_i64;
        assert_eq!(encode_i64(std::i64::MIN), [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_i64(-1024), [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc, 0x00]);
        assert_eq!(encode_i64(-1), [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(encode_i64(0), [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_i64(1), [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(encode_i64(1024), [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00]);
        assert_eq!(encode_i64(std::i64::MAX), [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        Ok(())
    }

    #[test]
    fn decode_i64() -> Result<()> {
        use super::decode_i64;
        assert_eq!(decode_i64([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), std::i64::MIN);
        assert_eq!(decode_i64([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc, 0x00]), -1024);
        assert_eq!(decode_i64([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), -1);
        assert_eq!(decode_i64([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), 0);
        assert_eq!(decode_i64([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), 1);
        assert_eq!(decode_i64([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00]), 1024);
        assert_eq!(decode_i64([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), std::i64::MAX);
        Ok(())
    }

    #[test]
    fn take_i64() -> Result<()> {
        use super::take_i64;

        let mut bytes: &[u8] = &[];
        assert!(take_i64(&mut bytes).is_err());

        let mut bytes: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        assert!(take_i64(&mut bytes).is_err());
        assert_eq!(bytes.len(), 7);

        let mut bytes: &[u8] = &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00];
        assert_eq!(take_i64(&mut bytes)?, 1024);
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc, 0x00, 0xff];
        assert_eq!(take_i64(&mut bytes)?, -1024);
        assert_eq!(bytes, &[0xff]);

        Ok(())
    }
    #[test]
    fn encode_string() -> Result<()> {
        use super::encode_string;
        assert_eq!(encode_string(""), vec![0x00, 0x00]);
        assert_eq!(encode_string("abc"), vec![0x61, 0x62, 0x63, 0x00, 0x00]);
        assert_eq!(
            encode_string("x \u{0000} z"),
            vec![0x78, 0x20, 0x00, 0xff, 0x20, 0x7a, 0x00, 0x00]
        );
        assert_eq!(encode_string("aáåA"), vec![0x61, 0xc3, 0xa1, 0xc3, 0xa5, 0x41, 0x00, 0x00]);
        Ok(())
    }

    #[test]
    fn take_string() -> Result<()> {
        use super::take_string;

        let mut bytes: &[u8] = &[];
        assert!(take_string(&mut bytes).is_err());

        let mut bytes: &[u8] = &[0x00, 0x00];
        assert_eq!(take_string(&mut bytes)?, "".to_owned());
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x61, 0x62, 0x63, 0x00, 0x00];
        assert_eq!(take_string(&mut bytes)?, "abc".to_owned());
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x78, 0x20, 0x00, 0xff, 0x20, 0x7a, 0x00, 0x00, 0x01, 0x02, 0x03];
        assert_eq!(take_string(&mut bytes)?, "x \u{0000} z".to_owned());
        assert_eq!(bytes, &[0x01, 0x02, 0x03]);

        let mut bytes: &[u8] = &[0xff, 0x00, 0x00]; // invalid utf-8
        assert!(take_string(&mut bytes).is_err());

        Ok(())
    }

    #[test]
    fn encode_u64() -> Result<()> {
        use super::encode_u64;
        assert_eq!(encode_u64(0), [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(encode_u64(1), [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(encode_u64(1024), [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00]);
        assert_eq!(encode_u64(std::u64::MAX), [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        Ok(())
    }

    #[test]
    fn decode_u64() -> Result<()> {
        use super::decode_u64;
        assert_eq!(decode_u64([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), 0);
        assert_eq!(decode_u64([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), 1);
        assert_eq!(decode_u64([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00]), 1024);
        assert_eq!(decode_u64([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), std::u64::MAX);
        Ok(())
    }

    #[test]
    fn take_u64() -> Result<()> {
        use super::take_u64;

        let mut bytes: &[u8] = &[];
        assert!(take_u64(&mut bytes).is_err());

        let mut bytes: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        assert!(take_u64(&mut bytes).is_err());
        assert_eq!(bytes.len(), 7);

        let mut bytes: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(take_u64(&mut bytes)?, 1);
        assert!(bytes.is_empty());

        let mut bytes: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xff];
        assert_eq!(take_u64(&mut bytes)?, 1);
        assert_eq!(bytes, &[0xff]);

        Ok(())
    }


}