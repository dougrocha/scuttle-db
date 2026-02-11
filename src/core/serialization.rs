/// Internal trait for serializing fixed-size data structures to bytes.
///
/// Used by the storage layer for encoding data into pages.
pub(crate) trait Serializable<const N: usize>: Sized {
    /// Convert this value to a fixed-size byte array.
    fn to_bytes(&self) -> [u8; N];

    /// Reconstruct this value from a fixed-size byte array.
    fn from_bytes(data: [u8; N]) -> Self;
}
