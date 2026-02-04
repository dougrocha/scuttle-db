# Agent Guidelines for Scuttle DB

This document provides coding guidelines and conventions for AI coding agents working on the Scuttle DB codebase.

## Build, Lint, and Test Commands

### Building
```bash
# Build the entire project
cargo build

# Build with release optimizations
cargo build --release

# Build only the library (no binary)
cargo build --lib

# Build and run the interactive REPL
cargo run
```

### Testing
```bash
# Run all tests
cargo test

# Run a specific test by name (substring match)
cargo test test_parse_select

# Run tests in a specific module
cargo test parser::tests

# Run tests without capturing output (shows println! statements)
cargo test -- --nocapture

# Run tests quietly (one character per test)
cargo test --quiet

# Compile tests but don't run them
cargo test --no-run
```

### Linting and Formatting
```bash
# Check code with clippy (recommended before commits)
cargo clippy

# Format code with rustfmt
cargo fmt

# Check formatting without modifying files
cargo fmt -- --check
```

### Documentation
```bash
# Generate and open API documentation
cargo doc --open

# Generate docs with all features enabled
cargo doc --all-features
```

### Examples
```bash
# Run the basic operations example
cargo run --example 01_basic_operations
```

## Code Style Guidelines

### Module Organization

```rust
// 1. Standard library imports (alphabetical)
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

// 2. External crate imports (alphabetical)
use miette::{IntoDiagnostic, Result};
use thiserror::Error;

// 3. Internal crate imports (alphabetical, grouped by module)
use crate::common::error::DatabaseError;
use crate::db::table::{Row, Schema, Value};
use crate::sql::parser::SqlParser;
use crate::storage::page::{Page, PageId};
```

### Visibility and Module Structure

- Use `pub(crate)` for internal module declarations in `mod.rs` files
- Public API types should be re-exported in `lib.rs`
- Keep module files focused on a single concern
- Module structure:
  ```
  src/
  ├── lib.rs              # Public API surface
  ├── module/
  │   ├── mod.rs          # Module declarations (pub(crate) mod ...)
  │   ├── type1.rs        # Implementation file
  │   └── type2.rs        # Implementation file
  ```

### Naming Conventions

- **Types**: `PascalCase` (e.g., `Database`, `PageHeader`, `LogicalPlan`)
- **Functions/Methods**: `snake_case` (e.g., `execute_query`, `get_table`, `to_bytes`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `KEYWORDS`, `PAGE_SIZE`)
- **Type Aliases**: `PascalCase` (e.g., `PageId`, `ItemId`)
- **Modules**: `snake_case` (e.g., `buffer_pool`, `logical_planner`)

### Documentation

All public items MUST have documentation comments:

```rust
/// Brief one-line summary.
///
/// Detailed explanation of what this does, how it works,
/// and any important context.
///
/// # Arguments
///
/// * `param_name` - Description of parameter
///
/// # Returns
///
/// Description of return value
///
/// # Errors
///
/// When and why this function returns errors
///
/// # Example
///
/// ```no_run
/// use scuttle_db::*;
///
/// let db = Database::new("./data");
/// ```
pub fn example_function(param_name: &str) -> Result<()> {
    // Implementation
}
```

- Module-level docs use `//!` at the top of files
- Use `# Example` sections with `no_run` or actual runnable code
- Document panics with `# Panics` if applicable
- Document safety with `# Safety` for unsafe code

### Error Handling

- Use `miette::Result<T>` for functions that can fail
- Use `thiserror::Error` for custom error types
- Use `miette::Diagnostic` trait for rich error reporting
- Convert errors with `.into_diagnostic()` when needed
- Use descriptive error messages with context:

```rust
use miette::{miette, Result};
use crate::DatabaseError;

// Good error handling
pub fn get_table(&self, name: &str) -> Result<&Relation, DatabaseError> {
    self.tables
        .get(name)
        .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
}

// For immediate errors
return Err(miette!("Invalid query: {}", reason));
```

### Type Annotations

- Type annotations on function signatures are required
- Use type inference for local variables when obvious
- Prefer explicit return types over `impl Trait` for public APIs
- Use type aliases for clarity (e.g., `pub type PageId = u32`)

```rust
// Good: explicit return type
pub fn execute_query(&mut self, query: &str) -> Result<QueryResponse<'_>> { }

// Good: obvious local variable
let rows = Vec::new();

// Good: type alias for domain concepts
pub type PageId = u32;
```

### Formatting

- Line length: aim for 100 characters, hard limit at 120
- Indentation: 4 spaces (enforced by rustfmt)
- Trailing commas in multi-line lists
- Use `rustfmt` defaults (no custom configuration present)

### Comments

- Use `//` for inline comments
- Prefer doc comments (`///`) for public items
- Keep comments up-to-date with code changes
- Use TODO comments for future work: `// TODO: Description`
- Mark work-in-progress clearly: `// WIP: Description`

## Architecture Patterns

### Storage Layer

- Pages are 8KB fixed-size blocks
- All data serialization uses little-endian encoding
- Implement `Serializable<N>` trait for fixed-size types
- Use `encode_row`/`decode_row` for variable-size data

### Query Pipeline

Follow the established pipeline pattern:
```
SQL String → Lexer → Parser → Logical Plan → Physical Plan → Execution
```

- Each stage should be independent and testable
- Pass errors up the chain with proper context
- Keep stages loosely coupled

### Testing

- Place tests in the same file as the code using `#[cfg(test)]`
- Name test functions descriptively: `test_<what>_<scenario>`
- Test both success and failure cases
- Use meaningful test data

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_with_columns() {
        let mut parser = SqlParser::new("SELECT id, name FROM users");
        let stmt = parser.parse().unwrap();
        assert_eq!(stmt.columns(), vec!["id", "name"]);
    }
}
```

## Common Patterns

### Buffer Management
- Always use `buffer_manager.get_page()` to access pages
- Call `buffer_manager.save_page()` after modifications
- Pages are immutable once returned; request again for updates

### Schema Validation
- Always validate rows against schema before insertion
- Use `schema.encode_row()` for serialization
- Use `schema.decode_row()` for deserialization

### Error Propagation
```rust
// Use ? operator for propagation
let table = self.get_table(name)?;

// Add context with .map_err() when needed
parser.parse()
    .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {e}")))?;
```

## Version and Edition

- Rust Edition: **2024** (specified in Cargo.toml)
- Minimum supported features: stable Rust (no nightly features used)
- Dependencies: minimal (only `miette` and `thiserror`)

## Important Notes

- This is a **learning project** - code clarity is prioritized over performance
- Extensive inline documentation is encouraged
- Match the PostgreSQL naming conventions where applicable (e.g., "relation" for table)
- The project is work-in-progress: features marked as WIP may be incomplete
