# Scuttle DB

> A learning project: building a relational database from scratch in Rust

**Scuttle DB** is my side-project database implementation modeled after PostgreSQL and SQLite because I want to be better at Rust and also understand lower level programming.

## Purpose

Areas I've explored:

- **SQL Parsing** - Lexical analysis and recursive descent parsing
- **Query Planning** - Logical and physical query optimization
- **Storage Management** - Page-based storage with buffer pools
- **Data Encoding** - Type-safe serialization and row encoding
- **B-tree Indexing** - Tree-based data structures for fast lookups (in progress)
- **Transactions** - MVCC and concurrency control (planned)

The project follows PostgreSQL/SQLite/MySql conventions and architectural patterns where applicable (shamelessly copied from their repos).

## Quick Start

```bash
git clone <repository-url>
cd scuttle_db
cargo run
```

This starts an interactive REPL where you can execute SQL queries:

```
scuttle_db> SELECT * FROM users WHERE age > 25
scuttle_db> SELECT id, name FROM users
scuttle_db> SELECT id, name, age FROM users WHERE name = 'Alice' AND (10 + 10) < age;
```

## Architecture

### Query Pipeline

```
SQL String → Lexer → Parser → Logical Plan → Physical Plan → Execution → Results
```

1. **Lexer**: Tokenizes SQL string into keywords, identifiers, operators
2. **Parser**: Builds Abstract Syntax Tree (AST) using recursive descent
3. **Logical Planner**: Converts AST to logical query plan (table scans, filters, projections)
4. **Physical Planner**: Converts to executable physical plan with cost-based decisions (I lied, no costs yet)
5. **Execution Engine**: Executes plan nodes using iterators and returns result rows

### Storage Layer

```
Database
  ├── Tables (Relations with schemas)
  ├── Buffer Pool (In-memory page cache)
  │     └── Pages (8KB fixed-size blocks)
  │           └── Rows (Encoded as bytes)
  └── B-Trees (Indexes - in progress)
```

**Key Storage Concepts:**

- **Pages**: Fixed 8KB blocks containing headers and multiple rows
- **Buffer Pool**: Manages page cache and disk I/O (currently in-memory only)
- **Encoding**: Type-specific serialization
  - Integers: little-endian 4-byte encoding
  - Strings: length-prefixed variable-length encoding
  - Booleans: single byte (0 or 1)
- **Page Layout**: PostgreSQL-inspired with headers, item pointers, and tuple data

## Development Roadmap

- [ ] INSERT/UPDATE/DELETE via SQL
- [ ] AND/OR logical operators in WHERE
- [ ] ORDER BY and LIMIT clauses
- [ ] Aggregate functions (COUNT, SUM, AVG, etc.)
- [ ] File persistence (save/load database)

- [ ] B-tree indexes for fast lookups
- [ ] JOIN operations (INNER, LEFT, RIGHT)
- [ ] Query optimization (index selection, join order)
- [ ] Statistics collection (TableStats)

### Long-term

- [ ] Transactions with MVCC (Multi-Version Concurrency Control)
- [ ] Write-Ahead Log (WAL) for durability
- [ ] Concurrent query execution
- [ ] Asynchronous I/O
- [ ] Network protocol (PostgreSQL wire format)

## Learning Resources

This project was built to learn database internals. Key references:

- **Database Internals** by Alex Petrov - comprehensive guide to storage engines
- **CMU 15-445 Database Systems** course - query processing and optimization
- **PostgreSQL source code** - real-world implementation patterns
- **SQLite architecture** - simple and elegant design principles

Extra Resources/Inspirations:

- [Ben Dicken's Video on Database Internals Book](https://www.youtube.com/watch?v=HibHalGlIes)
- [Tony Saro: Writing My Own Database From Scratch](https://www.youtube.com/watch?v=5Pc18ge9ohI&t=1834s&pp=ygUcd3JpdGluZyBhdGFiYXNlIGZyb20gc2NyYXRjaA%3D%3D)
