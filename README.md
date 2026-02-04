# Scuttle DB

A learning project: building a relational database from scratch in Rust.

**Scuttle DB** is an educational database implementation modeled after PostgreSQL and SQLite, designed to understand database internals including SQL parsing, query planning, page-based storage, and buffer management.

## Purpose

This is a learning project to explore:
- **SQL Parsing** - Lexical analysis and recursive descent parsing
- **Query Planning** - Logical and physical query optimization
- **Storage Management** - Page-based storage with buffer pools
- **B-tree Indexing** - Tree-based data structures for fast lookups (in progress)
- **Transactions** - MVCC and concurrency control (planned)

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
scuttle-db = { path = "path/to/scuttle_db" }
```

### Basic Usage

```rust
use scuttle_db::*;

fn main() -> miette::Result<()> {
    // Create a database
    let mut db = Database::new("./my_data");
    db.initialize()?;

    // Define a schema
    let schema = Schema::new(vec![
        ColumnDefinition::new("id", DataType::Integer, false),
        ColumnDefinition::new("name", DataType::VarChar(100), false),
        ColumnDefinition::new("age", DataType::Integer, false),
    ]);

    // Create a table
    db.create_table("users", schema)?;

    // Insert data
    let row = Row::new(vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Integer(30),
    ]);
    db.insert_row("users", row)?;

    // Query data
    let result = db.execute_query("SELECT * FROM users WHERE age > 25")?;
    for row in result.rows {
        println!("{:?}", row);
    }

    Ok(())
}
```

### Running the Interactive Shell

```bash
cargo run
```

This starts an interactive REPL where you can execute SQL queries.

### Running Examples

```bash
cargo run --example 01_basic_operations
```

## Architecture

### Query Pipeline

```
SQL String → Lexer → Parser → Logical Plan → Physical Plan → Execution → Results
```

1. **Lexer**: Tokenizes SQL string into keywords, identifiers, operators
2. **Parser**: Builds Abstract Syntax Tree (AST) using recursive descent
3. **Logical Planner**: Converts AST to logical query plan (table scans, filters, projections)
4. **Physical Planner**: Converts to executable physical plan with cost-based decisions
5. **Execution Engine**: Executes plan nodes and returns result rows

### Storage Layer

```
Database
  ├── Tables (Relations with schemas)
  ├── Buffer Pool (In-memory page cache)
  │     └── Pages (8KB fixed-size blocks)
  │           └── Rows (Encoded as bytes)
  └── B-Trees (Indexes - in progress)
```

- **Pages**: Fixed 8KB blocks containing multiple rows
- **Buffer Pool**: Manages page cache and disk I/O
- **Encoding**: Type-specific serialization (integers as little-endian, strings with length prefix)

## Current Features

- ✅ SQL lexer and parser (SELECT statements)
- ✅ WHERE clause filtering (`=`, `!=`, `>`, `<`)
- ✅ Multiple data types (Integer, Text, VarChar, Float, Boolean)
- ✅ Page-based storage with buffer pool
- ✅ Query planning (logical and physical)
- ✅ Multi-table support
- ✅ Column projection (SELECT specific columns)

## SQL Support

### Supported Syntax

```sql
-- Select all columns
SELECT * FROM users

-- Select specific columns
SELECT id, name FROM users

-- Filter with WHERE
SELECT * FROM users WHERE age > 25
SELECT * FROM users WHERE name = 'Alice'
```

### Supported Operators

- `=` - Equality
- `!=` - Inequality  
- `>` - Greater than
- `<` - Less than

### Limitations

- ❌ No JOINs yet
- ❌ No INSERT/UPDATE/DELETE via SQL (use API instead)
- ❌ No GROUP BY, ORDER BY, LIMIT
- ❌ No aggregate functions (COUNT, SUM, etc.)
- ❌ Table names must be unquoted in SQL
- ❌ String literals use single quotes only (`'text'`, not `"text"`)
- ❌ No AND/OR in WHERE clauses yet

## Documentation

- **API Docs**: Run `cargo doc --open` to view comprehensive API documentation
- **Examples**: See `examples/01_basic_operations.rs` for a complete walkthrough
- **Code**: Inline documentation throughout the codebase explains design decisions

## Development Roadmap

### Near-term
- [ ] INSERT/UPDATE/DELETE via SQL
- [ ] AND/OR logical operators in WHERE
- [ ] ORDER BY and LIMIT clauses
- [ ] Aggregate functions (COUNT, SUM, AVG, etc.)
- [ ] File persistence (save/load database)

### Mid-term
- [ ] B-tree indexes for fast lookups
- [ ] JOIN operations (INNER, LEFT, RIGHT)
- [ ] Query optimization (index selection, join order)
- [ ] Statistics collection (TableStats)

### Long-term
- [ ] Transactions with MVCC
- [ ] Write-Ahead Log (WAL) for durability
- [ ] Concurrent query execution
- [ ] Asynchronous I/O
- [ ] Network protocol (PostgreSQL wire format?)

## Project Structure

```
scuttle-db/
├── src/
│   ├── lib.rs              # Public API and module overview
│   ├── bin/
│   │   └── scuttle.rs      # Interactive REPL
│   ├── common/
│   │   ├── mod.rs          # Common utilities module
│   │   └── error.rs        # DatabaseError types
│   ├── db/
│   │   ├── mod.rs          # Database module declarations
│   │   ├── database.rs     # Core Database type
│   │   └── table.rs        # Schema, Row, Value types
│   ├── sql/
│   │   ├── mod.rs          # SQL module declarations
│   │   ├── lexer.rs        # SQL tokenization
│   │   ├── parser.rs       # AST construction
│   │   ├── logical_planner.rs   # Logical query plans
│   │   ├── physical_planner.rs  # Physical query plans
│   │   ├── planner_context.rs   # Context for query planning
│   │   └── predicate_evaluator.rs  # WHERE clause evaluation
│   ├── storage/
│   │   ├── mod.rs          # Storage module declarations
│   │   ├── page.rs         # Page layout and encoding
│   │   ├── buffer_pool.rs  # Page cache management
│   │   └── btree.rs        # B-tree implementation (WIP)
│   └── catalog/
│       ├── mod.rs          # Catalog module declarations
│       └── system_catalog.rs  # Metadata storage (WIP)
├── examples/
│   └── 01_basic_operations.rs  # Comprehensive example
├── AGENTS.md               # Coding guidelines for AI agents
└── README.md               # This file
```

## Learning Resources

This project was built to learn database internals. Key concepts explored:

- **Database Internals** by Alex Petrov
- **CMU 15-445 Database Systems** course materials
- **PostgreSQL source code** for inspiration
- **SQLite architecture** documentation

## License

MIT OR Apache-2.0
