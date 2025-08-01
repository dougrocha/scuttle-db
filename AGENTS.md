# Agent Guidelines for ScuttleDB

## Build/Test Commands
- Build: `cargo build`
- Test all: `cargo test`
- Test single: `cargo test test_name`
- Run: `cargo run`
- Check: `cargo check`
- Format: `cargo fmt`
- Lint: `cargo clippy`

## Code Style
- Use snake_case for functions, variables, modules
- Use PascalCase for types, structs, enums
- Prefer explicit types over inference when clarity improves
- Use `pub(crate)` for internal visibility, `pub` for external API
- Import std modules first, then external crates, then local modules
- Group imports with blank lines between categories
- Use `Result<T, DatabaseError>` for fallible operations
- Implement `From` traits for error conversions
- Use `miette::Result` for top-level error handling
- Prefer `BTreeMap` over `HashMap` for deterministic ordering
- Use descriptive variable names (e.g., `table_name` not `name`)
- Add TODO comments for incomplete implementations