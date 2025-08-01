pub(crate) mod btree;
pub(crate) mod buffer_manager;
pub mod database;
pub(crate) mod error;
pub(crate) mod lexer;
pub(crate) mod page;
pub(crate) mod parser;
pub(crate) mod system_catalog;
pub mod table;

pub use error::DatabaseError;

pub const KEYWORDS: &[&str] = &["SELECT", "FROM", "WHERE", "INSERT", "CREATE", "TABLE"];
