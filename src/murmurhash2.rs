const SEED: u32 = 3_242_157_231u32;
const M: u32 = 0x5bd1_e995;

#[inline]
pub fn murmurhash2(key: &[u8]) -> u32 {
    let mut h: u32 = SEED ^ (key.len() as u32);

    let mut four_bytes_chunks = key.chunks_exact(4);

    for chunk in four_bytes_chunks.by_ref() {
        let mut k: u32 = u32::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let remainder = four_bytes_chunks.remainder();

    // Handle the last few bytes of the input array
    match remainder.len() {
        3 => {
            h ^= u32::from(remainder[2]) << 16;
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^ (h >> 15)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmurhash() {
        assert_eq!(murmurhash2(b""), 3_632_506_080);
        assert_eq!(murmurhash2(b"hello happy tax payer"), 2_104_079_757);
    }
}
