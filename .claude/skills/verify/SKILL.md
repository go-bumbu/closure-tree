---
name: verify
description: Use when the user runs /verify or asks to run make verify. Runs the full verification suite (tests, lint, coverage, benchmarks, license) and fixes every issue found.
---

# verify

Run `make verify` and fix all issues until it passes clean.

`make verify` runs in order: `lint` → `test-full` → `test-race` → `license-check` → `benchmark` → `coverage`

## How to run

```bash
make verify
```

Read ALL output carefully. Don't stop at the first failure — run through to the end to collect all issues, then fix them together.

## Fixing issues — The Iron Law

**Fix the code. Never silence the tool.**

| Forbidden | Why |
|-----------|-----|
| Adding `//nolint:...` directives | Hides the problem, ships broken code |
| Removing or skipping tests | Destroys the safety net |
| Lowering the coverage threshold | Treats the symptom |
| Commenting out failing assertions | Same as deleting the test |
| `//nolint` without a real reason | `nolintlint` requires specific linter + explanation anyway |

The only valid `//nolint` is when the linter is provably wrong for that exact line and you include a clear explanation. This should be rare.

## Linter quick reference

Config: `.golangci.yaml` — standard linters + `nolintlint`, `gocyclo` (≥20), `nestif` (≥5), `gosec`, `dupl`

| Linter | Common fix |
|--------|-----------|
| `errcheck` | Handle or explicitly discard the error: `_ = f()` only if truly safe |
| `staticcheck` | Follow the message — usually dead code, deprecated API, or impossible condition |
| `unused` | Delete the unused symbol, don't keep it for "future use" |
| `govet` | Fix the suspicious construct (printf verbs, mutex copies, etc.) |
| `ineffassign` | Remove the assignment or actually use the value |
| `gocyclo` / `nestif` | Refactor: extract helper functions, invert conditions, reduce nesting |
| `gosec` | Fix the security issue (weak random, unhandled error on Close, etc.) |
| `dupl` | Extract the duplicated block into a shared function |
| `nolintlint` | Remove invalid nolint or add specific linter name + explanation |

## Coverage

Threshold: **70%** for `./internal/...` and `./libs/...`

If coverage drops below 70%: write the missing tests. Do not lower the threshold.

## Step-by-step

1. Run `make verify`, capture full output
2. Group failures by type (test failures, lint issues, coverage gaps)
3. Fix all test failures first (they may affect coverage numbers)
4. Fix all lint issues by refactoring code
5. Add missing tests if coverage is below threshold
6. Run `make verify` again — repeat until it passes with zero errors
