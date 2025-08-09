# Scuttle DB

A scratch database modeled after PostgreSQL/SQLite.

## Current Features

- Basic SQL parsing and lexical analysis
- SELECT statements with WHERE clause support
- Table creation and management
- B-tree storage implementation
- Buffer pool for page management
- Physical and logical query planning

## TODOs

- [ ] Update parser to read INSERT statements
- [ ] Use current page implementation to store table information
- [ ] Implement index pages
- [x] Support WHERE clauses in SELECT statements
- [ ] Add primary key support with incremental IDs for now
- [ ] Build a REPL to accept and returns queries
- [ ] Implement a TableStats struct to help optimize the physical planner
- [ ] Fix loading up data from files. Primarily for database metadata
- [ ] Implement buffer pool with limited page capacity and LRU eviction
- [ ] Load config file at startup for database settings
- [ ] Use asynchronous I/O functions
- [ ] Implement a WAL to allow batching updates
- [ ] Implement async page reading and writing operations
- [ ] Add async table scan and index operations
