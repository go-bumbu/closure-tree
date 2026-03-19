# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # SQLite only (fast)
make test-full     # All DBs via Docker (MySQL, PostgreSQL, SQLite)
make lint          # golangci-lint
make verify        # test-full + lint + license-check + benchmark + coverage
make clean         # Remove generated .sqlite files
```

Run a single test:
```bash
go test -run TestTreeGetNode ./...
```

Inspect SQLite databases during development:
```bash
LOCAL_SQLITE=true go test -run TestTreeGetNode ./...
```

## Architecture

**Closure tree pattern**: two tables per tree — the user's node table (embedding `Node`) and a `closure_tree_rel_<nodetable>` table that stores every ancestor/descendant pair with a `depth` column, enabling O(1) subtree reads.

### Core files

- **`closuretree.go`** — `Tree` struct and all CRUD methods. All SQL is raw via `fmt.Sprintf` with table name substitution (never user input). `New` validates table names with a regex before any migration. Write operations are transactional via `db.Transaction(...)`.
- **`node.go`** — `Node` struct to embed in user types. `ParentId` is read-only (populated via LEFT JOIN on read, ignored on write). Reflection helpers `hasNode`/`hasNodeType`/`findNodeValue` support multi-level embedding.
- **`leaves.go`** — `Leaf` struct for many-to-many targets. `GetLeaves` auto-discovers the `many2many:` GORM tag, validates the m2m table name, then joins through the closure table.

### Key invariants

- **Multi-tenancy**: every operation requires a non-empty `tenant` string. Empty string returns `ErrEmptyTenant`. Use `closuretree.DefaultTenant` as the fallback constant.
- **Schema**: the closure table has a unique constraint on `(ancestor_id, descendant_id, tenant, depth)`. A reflexive self-row at `depth=0` exists for every node; root nodes also have an ancestor row with `ancestor_id=0, depth=1`.
- **`Add`/`Update` use reflection** to copy only non-`Node` fields, using a `map[string]any` for `Update` to preserve zero values. Fields with `gorm:"-"` (empty `DBName`) are excluded.
- **`TreeDescendants`/`TreeDescendantsIds`** use `WITH RECURSIVE` CTEs. The internal depth counter is named `cte_depth` (not `depth`) to avoid shadowing any `depth` column in the user struct.
- **MySQL 8.0+** is enforced at `New` time via `checkMySQLVersion`.

### `TreeDescendants` requirements

The items slice must be `[]*YourType` and `YourType` must have a `Children []*YourType` field. Scanning is done via `mapRowToStruct` using the `col2FieldMap` built at `New` time; unknown columns (like `cte_depth`, `ancestor_id`) are silently skipped.
