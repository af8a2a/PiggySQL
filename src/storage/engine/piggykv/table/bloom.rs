// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::errors::Result;

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let checksum = (&buf[buf.len() - 4..buf.len()]).get_u32();
        if checksum != crc32fast::hash(&buf[..buf.len() - 4]) {
            panic!("checksum mismatched for bloom filters");
        }
        let filter = &buf[..buf.len() - 5];
        let k = buf[buf.len() - 5];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let offset = buf.len();
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        let checksum = crc32fast::hash(&buf[offset..]);
        buf.put_u32(checksum);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get_bit(bit_pos as usize) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            true
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use crate::storage::engine::piggykv::{key::{KeySlice, TS_ENABLED}, table::{bloom::Bloom, FileObject, SsTable, SsTableBuilder}};
    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:010}", idx * 5).into_bytes()
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn num_of_keys() -> usize {
        100
    }
    #[test]
    fn test_task1_bloom_filter() {
        let mut key_hashes = Vec::new();
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            key_hashes.push(farmhash::fingerprint32(&key));
        }
        let bits_per_key = Bloom::bloom_bits_per_key(key_hashes.len(), 0.01);
        println!("bits per key: {}", bits_per_key);
        let bloom = Bloom::build_from_key_hashes(&key_hashes, bits_per_key);
        println!("bloom size: {}, k={}", bloom.filter.len(), bloom.k);
        assert!(bloom.k < 30);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            assert!(bloom.may_contain(farmhash::fingerprint32(&key)));
        }
        let mut x = 0;
        let mut cnt = 0;
        for idx in num_of_keys()..(num_of_keys() * 10) {
            let key = key_of(idx);
            if bloom.may_contain(farmhash::fingerprint32(&key)) {
                x += 1;
            }
            cnt += 1;
        }
        assert_ne!(x, cnt, "bloom filter not taking effect?");
        assert_ne!(x, 0, "bloom filter not taking effect?");
    }
    #[test]
    fn test_task2_sst_decode() {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let sst = builder.build_for_test(&path).unwrap();
        let sst2 = SsTable::open(0, None, FileObject::open(&path).unwrap()).unwrap();
        let bloom_1 = sst.bloom.as_ref().unwrap();
        let bloom_2 = sst2.bloom.as_ref().unwrap();
        assert_eq!(bloom_1.k, bloom_2.k);
        assert_eq!(bloom_1.filter, bloom_2.filter);
    }
    #[test]
fn test_task3_block_key_compression() {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    let sst = builder.build_for_test(path).unwrap();
    if TS_ENABLED {
        assert!(
            sst.block_meta.len() <= 34,
            "you have {} blocks, expect 34",
            sst.block_meta.len()
        );
    } else {
        assert!(
            sst.block_meta.len() <= 25,
            "you have {} blocks, expect 25",
            sst.block_meta.len()
        );
    }
}

}
