# Roadmap: Fix Multi-Generation Path Collision in OpenMSIStream

## Problem Statement

When a file at the same path is uploaded more than once (e.g., an instrument
appends data incrementally), the Kafka topic accumulates chunks from ALL upload
generations. Each generation has a different `n_total_chunks` value baked into
every chunk message. The consumer locks in `n_total_chunks` from the first chunk
it sees, then crashes with a `ValueError` when it encounters a chunk from a
different generation. The crash-restart loop is infinite.

**Reproduced end-to-end:** Two uploads of the same filepath with 10 vs 15
chunks to a real Kafka broker triggers the exact production error.

## Fix Strategy: Four PRs

### PR 1: Consumer directional policy (CRITICAL — stops the crash loop)

**Branch:** `fix/consumer-generation-resilience`
**Prompt:** `fix-multigen/prompt_pr1_consumer_resilience.md`

Uses `file_hash` to detect generation boundaries and `n_total_chunks` as a
recency signal. Strictly higher chunk count wins; equal or lower is skipped.
Handles interleaved chunks without thrashing. Four unit tests.

### PR 4: Add file_mtime for equal-count tiebreaking (completes the fix)

**Branch:** `feat/chunk-mtime-tiebreaker`
**Prompt:** `fix-multigen/prompt_pr4_mtime_tiebreaker.md`

Adds file modification time as a 10th field in the msgpack wire format.
When two generations have the same chunk count but different hashes, mtime
determines which is newer. Backward compatible (deserializer accepts 9 or 10).
**Deploy consumers before producers.**

## IMPORTANT: Code Style

This project enforces `black` formatting. Configuration in `pyproject.toml`:
```
[tool.black]
line-length = 90
target-version = ['py39']
```

**ALL prompts MUST run `black` on changed files before committing.**

## Execution Order

```
PR 1  ─── CRITICAL: stops crash loops, handles growing-file case
  │
  └── PR 4  ─── completes the fix for equal-count modifications
                 (deploy consumers first, then producers)
```

## How to Execute

```bash
cd /Users/elbert/Documents/GitHub/openmsistream

# PR 1 — emergency fix
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr1_consumer_resilience.md and execute all changes. Show me the diffs when done."

# Review and merge PR 1, then:

# PR 4 — mtime tiebreaker
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr4_mtime_tiebreaker.md and execute all changes. Show me the diffs when done."
```
