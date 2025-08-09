pub(crate) mod btree;
pub(crate) mod buffer_manager;
pub mod database;
pub(crate) mod error;
pub(crate) mod lexer;
pub(crate) mod logical_planner;
pub(crate) mod page;
pub(crate) mod parser;
pub(crate) mod physical_planner;
pub(crate) mod planner_context;
pub(crate) mod predicate_evaluator;
pub(crate) mod system_catalog;
pub mod table;

pub use error::DatabaseError;

pub const KEYWORDS: &[&str] = &["SELECT", "FROM", "WHERE", "INSERT", "CREATE", "TABLE"];

pub(crate) trait Serializable<const N: usize>: Sized {
    fn to_bytes(&self) -> [u8; N];
    fn from_bytes(data: [u8; N]) -> Self;
}
