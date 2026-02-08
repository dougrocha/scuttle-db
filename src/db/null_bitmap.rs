use miette::{Result, miette};

/// A Bitmap used to store null values for each row
#[derive(Debug, Clone)]
pub struct NullBitmap {
    pub bytes: Vec<u8>,
}

impl NullBitmap {
    pub fn new(num_columns: usize) -> Self {
        let bitmap_size = num_columns.div_ceil(8);
        Self {
            bytes: vec![0u8; bitmap_size],
        }
    }

    pub fn from_bytes(bytes: &[u8], num_columns: usize) -> Result<Self> {
        let expected_len = num_columns.div_ceil(8);
        if bytes.len() < expected_len {
            return Err(miette!(
                "Bitmap too short: expected {} byte(s), got {}",
                expected_len,
                bytes.len()
            ));
        }

        Ok(Self {
            bytes: bytes[0..expected_len].to_vec(),
        })
    }

    pub fn set_null(&mut self, index: usize) {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.bytes[byte_idx] |= 1 << bit_idx;
    }

    pub fn is_null(&self, index: usize) -> bool {
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        if byte_idx >= self.bytes.len() {
            return false;
        }
        (self.bytes[byte_idx] & (1 << bit_idx)) != 0
    }
}
