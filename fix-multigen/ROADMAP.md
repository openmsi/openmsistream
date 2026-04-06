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
recency signal:

| Condition | Action |
|-----------|--------|
| Hash matches | Accept (same generation) |
| Hash differs, chunks strictly higher | Reset → adopt newer generation |
| Hash differs, chunks equal or lower | Skip (stale/indeterminate) |

Handles interleaved chunks without thrashing. Four unit tests.

### PR 2: Generation-aware message keys (debuggability)

**Branch:** `fix/generation-aware-msg-key`
**Prompt:** `fix-multigen/prompt_pr2_generation_aware_keys.md`

Appends file hash prefix to chunk message keys. Makes `kcat` output
human-readable for debugging. Backward compatible.

### PR 3: Suppress redundant uploads (producer optimization)

**Branch:** `fix/suppress-growing-file-reuploads`
**Prompt:** `fix-multigen/prompt_pr3_suppress_reuploads.md`

File stability check in producer, `file_hash` property exposed for
future duplicate suppression.

### PR 4: Add file_mtime for equal-count tiebreaking (completes the fix)

**Branch:** `feat/chunk-mtime-tiebreaker`
**Prompt:** `fix-multigen/prompt_pr4_mtime_tiebreaker.md`

Adds file modification time as a 10th field in the msgpack wire format.
When two generations have the same chunk count but different hashes (e.g.,
a CSV log gains one row without crossing a chunk boundary), mtime determines
which is newer.

Backward compatible: deserializer accepts 9 or 10 fields.
**Deploy consumers before producers.**

## Execution Order

```
PR 1  ─── CRITICAL: stops crash loops, handles growing-file case
  │
  ├── PR 2  ─── nice-to-have: better debugging
  │
  ├── PR 3  ─── optimization: less topic pollution
  │
  └── PR 4  ─── completes the fix for equal-count modifications
                 (deploy consumers first, then producers)
```

PR 1 is the only one that must be deployed urgently. PRs 2, 3, 4 are
independent of each other but all depend on PR 1.

## How to Execute

```bash
cd /Users/elbert/Documents/GitHub/openmsistream

# PR 1 — emergency fix
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr1_consumer_resilience.md and execute all changes. Show me the diffs when done."

# Review and merge PR 1, then any of:

# PR 4 — mtime tiebreaker (most important after PR 1)
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr4_mtime_tiebreaker.md and execute all changes. Show me the diffs when done."

# PR 2 — generation-aware keys
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr2_generation_aware_keys.md and execute all changes. Show me the diffs when done."

# PR 3 — suppress reuploads
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  "Read fix-multigen/prompt_pr3_suppress_reuploads.md and execute all changes. Show me the diffs when done."
```

## Verification After Deployment

1. Deploy updated consumers to SPHINX and HELIX containers
2. Reset consumer group offsets (or use a new consumer group ID)
3. Consumer should gracefully handle mixed-generation chunks
4. MAXPOLL rebalance errors should decrease (no more crash loops)
5. Verify the latest version of each file is reconstructed correctly
