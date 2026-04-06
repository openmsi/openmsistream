# Claude Code Prompt — PR 1: Consumer Generation Resilience (Directional Policy)

## Context

Read `fix-multigen/ROADMAP.md` for full background.

OpenMSIStream consumers crash with an infinite loop when a file is uploaded
more than once to the same Kafka topic with a different number of chunks.

Each chunk message already carries `file_hash` (SHA-512 of the full file
contents at the time it was chunked) and `n_total_chunks`. The consumer
currently ignores `file_hash` during reconstruction and crashes on any
`n_total_chunks` mismatch.

The fix must handle **interleaved chunks from multiple generations** on
the same topic. Because Kafka topics can have multiple partitions, and the
consumer uses multiple threads, chunks from Gen1 and Gen2 can arrive in
any order. A naive "reset on any mismatch" would thrash between generations.

### Directional Policy

The rule is: **strictly higher `n_total_chunks` wins; everything else is skipped.**

For each chunk arriving at `add_chunk()`:

1. **Hash matches current** → accept normally (same generation)
2. **Hash differs, `n_total_chunks` is strictly higher** → newer generation
   (file grew). Reset state. Adopt new hash and chunk count.
3. **Hash differs, `n_total_chunks` is equal or lower** → stale or
   indeterminate chunk. **Skip it.** Return `CHUNK_ALREADY_WRITTEN_CODE`.

Rule 3 covers two sub-cases:
- **Lower chunk count** = definitely an older, smaller generation → skip.
- **Equal chunk count, different hash** = file was modified in place without
  growing. We can't tell which generation is newer from the chunk alone, so
  we stick with whichever generation we committed to first. This prevents
  thrashing when equal-count generations are interleaved.

After a reset (rule 2), old-generation chunks continue arriving but hit
rule 3 and are silently skipped. The consumer makes steady forward progress
on the newest generation only.

Example with interleaving:
```
Gen1 chunk 1 (n=10, hash=AAA) → lock in AAA, n=10, ACCEPT
Gen2 chunk 3 (n=15, hash=BBB) → BBB≠AAA, 15>10 → RESET to BBB, n=15
Gen1 chunk 2 (n=10, hash=AAA) → AAA≠BBB, 10<15 → SKIP (stale)
Gen2 chunk 1 (n=15, hash=BBB) → matches BBB → ACCEPT
Gen1 chunk 5 (n=10, hash=AAA) → AAA≠BBB, 10<15 → SKIP (stale)
Gen2 chunk 2 (n=15, hash=BBB) → matches BBB → ACCEPT
...all Gen1 skipped, all Gen2 accepted...
Gen2 chunk 15 (n=15, hash=BBB) → ACCEPT, last chunk → COMPLETE ✓
```

Example with equal-count interleaving (no thrashing):
```
Gen1 chunk 1 (n=5, hash=AAA) → lock in AAA, n=5, ACCEPT
Gen2 chunk 1 (n=5, hash=BBB) → BBB≠AAA, 5==5 → SKIP (equal, stick with AAA)
Gen1 chunk 2 (n=5, hash=AAA) → matches AAA → ACCEPT
Gen2 chunk 2 (n=5, hash=BBB) → BBB≠AAA, 5==5 → SKIP
...Gen1 completes, Gen2 all skipped...
Gen1 chunk 5 (n=5, hash=AAA) → ACCEPT, last chunk → COMPLETE ✓
```

## Create Issue

```bash
gh issue create \
  --title "fix: consumer crashes on multi-generation chunk mismatch" \
  --body "## Problem

When a file at the same path is uploaded more than once (e.g., an instrument
writes data incrementally), the topic accumulates chunks from all upload
generations. Each generation has a different \`n_total_chunks\` and
\`file_hash\`. The consumer locks in \`n_total_chunks\` from the first chunk
it sees, then crashes with a \`ValueError\` when it encounters a chunk from
a different generation. The crash-restart loop is infinite because the
offset is never committed.

## Evidence

The SPHINX producer registry shows \`Orientation Grid 7x3.NMD\` uploaded 21
times in 40 minutes with monotonically increasing \`n_total_chunks\`
(8→16→24→...→163), ~115 seconds apart. The consumer error names the exact
values from two of these generations. Reproduced end-to-end with a test
script that uploads two generations to a real broker.

## Fix

Implement a directional generation policy in \`add_chunk()\`:
- Track \`_expected_file_hash\` alongside \`n_total_chunks\`
- When a chunk arrives with a different hash and strictly higher chunk
  count, reset the in-progress reconstruction (newer generation wins)
- When a chunk arrives with a different hash and equal or lower chunk
  count, skip it silently (stale or indeterminate generation)
- This handles interleaved chunks without thrashing: once committed to a
  generation, only a strictly larger generation can displace it

## Files

- \`openmsistream/data_file_io/config.py\`
- \`openmsistream/data_file_io/entity/download_data_file.py\`
- \`openmsistream/data_file_io/actor/data_file_chunk_handlers.py\`
- \`test/test_scripts/test_download_data_file.py\`"
```

## Branch

```bash
git checkout main
git pull
git checkout -b fix/consumer-generation-resilience
```

## Changes

### 1. `openmsistream/data_file_io/config.py`

Add a new constant. Find the class body:

```python
class DataFileHandlingConstants:
    """
    Constants for internally handling DataFileChunks
    """

    CHUNK_ALREADY_WRITTEN_CODE = 10
    FILE_HASH_MISMATCH_CODE = -1
    FILE_SUCCESSFULLY_RECONSTRUCTED_CODE = 3
    FILE_IN_PROGRESS = 2
```

Add after `FILE_IN_PROGRESS`:

```python
    GENERATION_RESET_CODE = 11
```

### 2. `openmsistream/data_file_io/entity/download_data_file.py`

#### 2a. In `DownloadDataFile.__init__`, add `_expected_file_hash`

Find:
```python
    def __init__(self, filepath, *args, **kwargs):
        super().__init__(filepath, *args, **kwargs)
        # start an empty set of this file's downloaded offsets
        self._chunk_offsets_downloaded = []
        self.full_filepath = None
        self.subdir_str = None
        self.n_total_chunks = None
```

Replace with:
```python
    def __init__(self, filepath, *args, **kwargs):
        super().__init__(filepath, *args, **kwargs)
        # start an empty set of this file's downloaded offsets
        self._chunk_offsets_downloaded = []
        self.full_filepath = None
        self.subdir_str = None
        self.n_total_chunks = None
        self._expected_file_hash = None
```

#### 2b. Replace the `n_total_chunks` check block in `add_chunk`

Find this exact block:
```python
        # set or check the total number of chunks expected
        if self.n_total_chunks is None:
            with thread_lock:
                self.n_total_chunks = dfc.n_total_chunks
        elif self.n_total_chunks != dfc.n_total_chunks:
            errmsg = (
                f"ERROR: {self.__class__.__name__} with filepath {self.full_filepath} "
                f"is expecting {self.n_total_chunks} chunks but found a chunk from a split "
                f"with {dfc.n_total_chunks} total chunks."
            )
            self.logger.error(errmsg, exc_type=ValueError)
```

Replace with:
```python
        # set or check the total number of chunks expected, applying the
        # directional generation policy when file_hash differs.
        #
        # Policy:
        #   hash matches          → accept (same generation)
        #   hash differs, n >     → reset and adopt (newer generation)
        #   hash differs, n <=    → skip (stale or indeterminate generation)
        #   hash same, n differs  → genuine corruption (ValueError)
        if self.n_total_chunks is None:
            with thread_lock:
                self.n_total_chunks = dfc.n_total_chunks
                self._expected_file_hash = dfc.file_hash
        elif self.n_total_chunks != dfc.n_total_chunks:
            # Chunk count mismatch — check if this is a different generation
            if (
                self._expected_file_hash is not None
                and dfc.file_hash != self._expected_file_hash
            ):
                if dfc.n_total_chunks > self.n_total_chunks:
                    # Strictly more chunks = newer generation (file grew).
                    # Reset and adopt.
                    warnmsg = (
                        f"WARNING: {self.__class__.__name__} with filepath "
                        f"{self.full_filepath} received a chunk from a newer "
                        f"file generation (had {self.n_total_chunks} chunks "
                        f"with hash {self._expected_file_hash[:4].hex()}, "
                        f"now {dfc.n_total_chunks} chunks with hash "
                        f"{dfc.file_hash[:4].hex()}). Discarding "
                        f"{len(self._chunk_offsets_downloaded)} chunks from "
                        f"the previous generation and starting fresh."
                    )
                    self.logger.warning(warnmsg)
                    with thread_lock:
                        self._reset_for_new_generation()
                        self.n_total_chunks = dfc.n_total_chunks
                        self._expected_file_hash = dfc.file_hash
                    return DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE
                else:
                    # Fewer chunks = older generation. Skip.
                    return DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
            else:
                # Same hash but different chunk count — genuine corruption
                errmsg = (
                    f"ERROR: {self.__class__.__name__} with filepath "
                    f"{self.full_filepath} is expecting {self.n_total_chunks} "
                    f"chunks but found a chunk from a split with "
                    f"{dfc.n_total_chunks} total chunks."
                )
                self.logger.error(errmsg, exc_type=ValueError)
        elif (
            self._expected_file_hash is not None
            and dfc.file_hash != self._expected_file_hash
        ):
            # Same chunk count but different hash — file was modified in place
            # without changing size. We can't determine which is newer, so
            # stick with the generation we already committed to (skip this
            # chunk) to avoid thrashing when interleaved.
            return DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
```

IMPORTANT: That last `elif` block catches the case where `n_total_chunks`
matches (so the `n_total_chunks !=` branch doesn't fire) but `file_hash`
differs. It SKIPS the chunk rather than resetting, to prevent thrashing
when equal-count generations are interleaved. It must come AFTER the
`elif self.n_total_chunks != dfc.n_total_chunks` block and BEFORE the
`with thread_lock:` block that calls `_on_add_chunk`.

#### 2c. Add `_reset_for_new_generation` method to `DownloadDataFile`

Add this method in the `PRIVATE HELPER FUNCTIONS` section, after `_on_add_chunk`:

```python
    def _reset_for_new_generation(self):
        """
        Reset internal state to accept chunks from a new upload generation.

        Called when a chunk arrives with a different file_hash and a strictly
        higher n_total_chunks, indicating a newer (larger) version of the
        same file. The directional policy ensures we only reset forward
        to a generation with more chunks, never backward or sideways.

        Subclasses should override to clean up their own state (e.g., delete
        partial files on disk, clear in-memory data dictionaries).
        """
        self._chunk_offsets_downloaded = []
        self.n_total_chunks = None
        self._expected_file_hash = None
```

#### 2d. Override `_reset_for_new_generation` in `DownloadDataFileToDisk`

Add after `_on_add_chunk` in the `DownloadDataFileToDisk` class:

```python
    def _reset_for_new_generation(self):
        """Delete the partially reconstructed file on disk, then reset."""
        if self.full_filepath is not None and self.full_filepath.is_file():
            self.full_filepath.unlink()
        super()._reset_for_new_generation()
```

#### 2e. Override `_reset_for_new_generation` in `DownloadDataFileToMemory`

Add after `_on_add_chunk` in the `DownloadDataFileToMemory` class:

```python
    def _reset_for_new_generation(self):
        """Clear the in-memory chunk data, then reset."""
        self.__chunk_data_by_offset = {}
        self.__bytestring = None
        super()._reset_for_new_generation()
```

### 3. `openmsistream/data_file_io/actor/data_file_chunk_handlers.py`

Find the return-value check block:

```python
        # If the file is just in progress, return True
        if retval in (
            DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
            DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE,
        ):
            return True
```

Replace with:
```python
        # If the file is just in progress (or was reset for a new generation), return True
        if retval in (
            DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
            DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE,
            DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE,
        ):
            return True
```

### 4. `test/test_scripts/test_download_data_file.py`

Add these imports at the top of the file if not already present:

```python
from openmsistream.data_file_io.config import DATA_FILE_HANDLING_CONST
```

Add four new test functions and one helper at the end of the file:

```python
def _make_test_chunks(data, filename, subdir, chunk_size, file_hash):
    """Helper to create DataFileChunk objects from raw data."""
    from hashlib import sha512

    chunks = []
    offset = 0
    chunk_i = 1
    n_total = -(-len(data) // chunk_size)  # ceiling division
    while offset < len(data):
        end = min(offset + chunk_size, len(data))
        chunk_data = data[offset:end]
        chunk_hash = sha512(chunk_data).digest()
        chunks.append(
            DataFileChunk(
                subdir / filename,
                filename,
                file_hash,
                chunk_hash,
                None,
                offset,
                len(chunk_data),
                chunk_i,
                n_total,
                data=chunk_data,
            )
        )
        offset = end
        chunk_i += 1
    return chunks


def test_generation_reset_to_memory(output_dir, logger):
    """
    Verify that DownloadDataFileToMemory resets when a newer (larger)
    generation arrives, and reconstructs the newer version correctly.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "growing_file.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # --- Generation 1: a small file (2 chunks) ---
    gen1_data = b"A" * int(chunk_size * 1.5)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # --- Generation 2: a larger file (3 chunks) ---
    gen2_data = b"B" * int(chunk_size * 2.5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Feed Gen1 chunk 1 — establishes n_total_chunks=2, hash=gen1
    result = dl.add_chunk(gen1_chunks[0])
    assert result == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Feed Gen2 chunk 1 — different hash, strictly higher n_total_chunks → RESET
    result = dl.add_chunk(gen2_chunks[0])
    assert result == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Feed all Gen2 chunks from the start
    for i, chunk in enumerate(gen2_chunks):
        result = dl.add_chunk(chunk)
        if i == 0:
            # Chunk 0 may be ALREADY_WRITTEN (was the trigger for reset)
            assert result in (
                DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS,
                DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE,
            )
        elif i < len(gen2_chunks) - 1:
            assert result == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert result == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen2_data


def test_interleaved_generations_to_memory(output_dir, logger):
    """
    Verify that interleaved chunks from two generations are handled correctly:
    newer generation wins, older generation chunks are skipped.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "interleaved.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: 3 chunks
    gen1_data = b"X" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # Gen2: 5 chunks (file grew)
    gen2_data = b"Y" * (chunk_size * 5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept, lock in Gen1
    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Gen2 chunk 1 → different hash, 5 > 3 → RESET to Gen2
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Gen1 chunk 2 → stale (n=3 < 5) → SKIP
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Gen2 chunk 1 again → already written
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Gen1 chunk 3 → stale (n=3 < 5) → SKIP
    r = dl.add_chunk(gen1_chunks[2])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Feed remaining Gen2 chunks 2-5 → all accepted
    for i in range(1, len(gen2_chunks)):
        r = dl.add_chunk(gen2_chunks[i])
        if i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Verify the reconstructed file is Gen2
    assert dl.bytestring == gen2_data


def test_equal_chunk_count_no_thrashing(output_dir, logger):
    """
    Verify that two generations with the SAME n_total_chunks but different
    hashes do NOT cause thrashing. The first generation committed to wins;
    chunks from the other generation are skipped.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "same_size.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1 and Gen2: same size (3 chunks each) but different content
    gen1_data = b"P" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    gen2_data = b"Q" * (chunk_size * 3)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    # Sanity: hashes must differ, chunk counts must be equal
    assert gen1_hash != gen2_hash
    assert gen1_chunks[0].n_total_chunks == gen2_chunks[0].n_total_chunks

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Interleave: Gen1[0], Gen2[0], Gen1[1], Gen2[1], Gen1[2], Gen2[2]
    # Gen1 should win (committed first); all Gen2 chunks should be skipped.

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE  # skipped, not reset

    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE  # skipped

    r = dl.add_chunk(gen1_chunks[2])
    assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Gen1 completed; Gen2 was entirely skipped
    assert dl.bytestring == gen1_data


def test_stale_chunks_skipped_after_reset_to_disk(output_dir, logger):
    """
    Verify that DownloadDataFileToDisk skips stale (older generation) chunks
    after resetting to a newer generation, and reconstructs correctly.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "growing_on_disk.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: 2 chunks
    gen1_data = b"M" * int(chunk_size * 1.5)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)

    # Gen2: 4 chunks
    gen2_data = b"N" * int(chunk_size * 3.5)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToDisk(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept
    dl.add_chunk(gen1_chunks[0])

    # Gen2 chunk 2 → reset (newer, 4 > 2)
    r = dl.add_chunk(gen2_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Gen1 chunk 2 → skip (stale, n_total_chunks=2 < 4)
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Feed all Gen2 chunks
    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i == 1:
            # Already added during reset
            assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
        elif i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    # Verify reconstructed file matches Gen2
    assert dl.bytestring == gen2_data
```

Make sure `DataFileChunk` is imported at the top of the test file. It already
is (check the existing imports). Also make sure `DownloadDataFileToMemory` is
imported — it should be from the existing tests but verify:

```python
from openmsistream.data_file_io.entity.download_data_file import (
    DownloadDataFileToDisk,
    DownloadDataFileToMemory,
)
```

## Verification

Run the unit tests (no Kafka broker needed):

```bash
pytest test/test_scripts/test_download_data_file.py -v
```

All tests must pass — the four new tests plus all existing tests. The
existing tests should not regress because the new logic only activates when
`file_hash` differs between chunks, which never happens in single-generation
scenarios.

Also verify no remaining lint errors:
```bash
python -m pylint openmsistream/data_file_io/entity/download_data_file.py \
  --rcfile=.pylintrc --disable=all --enable=E 2>/dev/null; echo "Exit: $?"
```

## Commit and PR

```bash
git add -A
git commit -m "fix: consumer handles multi-generation chunks with directional policy

When a file at the same path is uploaded more than once with different
content (e.g., an instrument appending data incrementally), the topic
accumulates chunks from all upload generations. Previously, the consumer
crashed with a ValueError on any n_total_chunks mismatch, entering an
infinite crash-restart loop.

Now, add_chunk() implements a directional generation policy using the
file_hash field (already present in every chunk message):

- Chunk with matching hash → accept (same generation)
- Chunk with different hash and STRICTLY HIGHER chunk count → reset
  the in-progress reconstruction and adopt the newer generation
- Chunk with different hash and EQUAL OR LOWER chunk count → skip
  silently (stale or indeterminate generation)

The strictly-higher rule prevents thrashing when equal-count generations
are interleaved: once committed to a generation, only a generation with
MORE chunks can displace it.

This handles interleaved chunks from multiple generations on
multi-partition topics correctly: the consumer always makes forward
progress toward reconstructing the newest (largest) generation.

Adds GENERATION_RESET_CODE to DataFileHandlingConstants.
Adds _reset_for_new_generation() to DownloadDataFile and subclasses.
Adds four tests: sequential reset, interleaved generations,
equal-count no-thrashing, and stale-chunk skipping on disk.

Fixes #<ISSUE_NUMBER>"
git push -u origin fix/consumer-generation-resilience

gh pr create \
  --title "fix: consumer handles multi-generation chunks with directional policy" \
  --body "## Problem

When a file is uploaded more than once to the same topic (different content,
different \`n_total_chunks\`), the consumer crashes with a \`ValueError\` in
\`add_chunk()\` and enters an infinite crash-restart loop.

Reproduced end-to-end: two uploads of the same filepath with 10 vs 15 chunks
to a real broker triggers the exact production error.

## Root Cause

\`add_chunk()\` locks in \`n_total_chunks\` from the first chunk and crashes
on any mismatch. It never checks \`file_hash\` to distinguish generations.
The \`file_hash\` field is already in every serialized chunk but was only used
as a post-reconstruction checksum, never for routing.

## Fix: Directional Generation Policy

Uses \`file_hash\` to detect generation boundaries and \`n_total_chunks\` as
a recency signal:

| Condition | Action |
|-----------|--------|
| Hash matches | Accept (same generation) |
| Hash differs, chunks strictly higher | Reset → adopt newer generation |
| Hash differs, chunks equal or lower | Skip (stale/indeterminate) |

The strictly-higher rule prevents thrashing when equal-count generations
are interleaved. Once committed to a generation, only a generation with
MORE chunks can displace it.

## Changes

- \`config.py\`: Added \`GENERATION_RESET_CODE = 11\`
- \`download_data_file.py\`: Directional policy in \`add_chunk()\`,
  \`_reset_for_new_generation()\` with subclass overrides
- \`data_file_chunk_handlers.py\`: Handle \`GENERATION_RESET_CODE\`
- \`test_download_data_file.py\`: Four tests — sequential reset,
  interleaved generations, equal-count no-thrashing, stale-chunk
  skipping on disk

## Testing

\`\`\`
pytest test/test_scripts/test_download_data_file.py -v
\`\`\`" \
  --base main
```

Replace `<ISSUE_NUMBER>` with the actual issue number from the
`gh issue create` output above.
