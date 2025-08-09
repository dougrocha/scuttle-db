pub(crate) mod catalog;
pub(crate) mod common;
pub(crate) mod db;
pub(crate) mod sql;
pub(crate) mod storage;

pub use common::error::DatabaseError;
pub use db::database::Database;
pub use db::table::*;

pub const KEYWORDS: &[&str] = &["SELECT", "FROM", "WHERE", "INSERT", "CREATE", "TABLE"];

pub(crate) trait Serializable<const N: usize>: Sized {
    fn to_bytes(&self) -> [u8; N];
    fn from_bytes(data: [u8; N]) -> Self;
}
