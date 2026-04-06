# Claude Code Prompt — PR 4: Add file_mtime to chunk messages for generation tiebreaking

## Context

Read `fix-multigen/ROADMAP.md` and `fix-multigen/prompt_pr1_consumer_resilience.md`
for full background.

PR 1 implemented a directional generation policy: when chunks from multiple
upload generations of the same file are interleaved on a topic, the consumer
resets to the generation with strictly more chunks and skips older generations.

**The gap PR 1 leaves:** when a file is modified with a small addition (e.g.,
one CSV row added to a log file), the hash changes but `n_total_chunks` may
stay the same. PR 1 handles this safely (no crash, no thrashing — it sticks
with whichever generation it committed to first) but may reconstruct the older
version instead of the newer one.

**This PR fixes the gap** by adding the file's modification time (`mtime`) to
each chunk message. When two generations have equal `n_total_chunks` but
different `file_hash`, the consumer uses `mtime` as a tiebreaker: newer mtime
wins, older mtime is skipped.

This requires a **backward-compatible change to the msgpack wire format**:
- Serializer sends 10 fields (was 9)
- Deserializer accepts both 9 fields (old messages, `file_mtime=None`) and
  10 fields (new messages)

**Deployment order matters:** deploy updated consumers FIRST (they accept both
formats), then deploy updated producers (they send the new format). If you
deploy producers first, old consumers will reject the 10-field messages.

## Create Issue

```bash
gh issue create \
  --title "feat: add file_mtime to chunk messages for generation tiebreaking" \
  --body "## Problem

When a file is modified with a small addition that doesn't change the chunk
count, PR 1's directional policy (strictly higher n_total_chunks wins) cannot
determine which generation is newer. It defaults to the first-seen generation,
which may be the older version.

## Fix

Add the file's modification time (mtime) as a 10th field in the serialized
chunk message. When two generations have the same chunk count but different
hashes, the consumer uses mtime as a tiebreaker: newer mtime wins.

The wire format change is backward compatible:
- New deserializer accepts 9 or 10 fields
- Old messages without mtime get \`file_mtime=None\`
- When mtime is unavailable, the existing behavior (skip indeterminate
  chunks) is preserved

**Deploy consumers before producers** to avoid old consumers rejecting the
new 10-field format.

## Files

- \`openmsistream/data_file_io/entity/data_file_chunk.py\`
- \`openmsistream/data_file_io/entity/upload_data_file.py\`
- \`openmsistream/data_file_io/entity/download_data_file.py\`
- \`openmsistream/kafka_wrapper/serialization.py\`
- \`test/test_scripts/test_download_data_file.py\`
- \`test/test_scripts/test_serialization.py\`"
```

## Branch

```bash
git checkout main
git pull
git checkout -b feat/chunk-mtime-tiebreaker
```

## Changes

### 1. `openmsistream/data_file_io/entity/data_file_chunk.py`

#### 1a. Add `file_mtime` parameter to `__init__`

Find the `__init__` method signature and body:

```python
    def __init__(
        self,
        filepath,
        filename,
        file_hash,
        chunk_hash,
        chunk_offset_read,
        chunk_offset_write,
        chunk_size,
        chunk_i,
        n_total_chunks,
        rootdir=None,
        filename_append="",
        data=None,
    ):
        self.__filepath = filepath
        self.filename = filename
        self.file_hash = file_hash
        self.chunk_hash = chunk_hash
        self.chunk_offset_read = chunk_offset_read
        self.chunk_offset_write = chunk_offset_write
        self.chunk_size = chunk_size
        self.chunk_i = chunk_i
        self.n_total_chunks = n_total_chunks
        self.__rootdir = rootdir
        if self.__rootdir is not None:
            self.__relative_filepath = self.__filepath.relative_to(self.__rootdir)
        else:
            self.__relative_filepath = self.__filepath
        self.filename_append = filename_append
        self.data = data
```

Add `file_mtime=None` parameter and store it:

```python
    def __init__(
        self,
        filepath,
        filename,
        file_hash,
        chunk_hash,
        chunk_offset_read,
        chunk_offset_write,
        chunk_size,
        chunk_i,
        n_total_chunks,
        rootdir=None,
        filename_append="",
        data=None,
        file_mtime=None,
    ):
        self.__filepath = filepath
        self.filename = filename
        self.file_hash = file_hash
        self.chunk_hash = chunk_hash
        self.chunk_offset_read = chunk_offset_read
        self.chunk_offset_write = chunk_offset_write
        self.chunk_size = chunk_size
        self.chunk_i = chunk_i
        self.n_total_chunks = n_total_chunks
        self.__rootdir = rootdir
        if self.__rootdir is not None:
            self.__relative_filepath = self.__filepath.relative_to(self.__rootdir)
        else:
            self.__relative_filepath = self.__filepath
        self.filename_append = filename_append
        self.data = data
        self.file_mtime = file_mtime
```

#### 1b. Add `file_mtime` to `callback_kwargs`

Find:

```python
    @property
    def callback_kwargs(self):
        """
        keyword arguments that should be sent to the producer callback function
        when the chunk is produced
        """
        return {
            "filepath": self.__filepath,
            "filename": self.filename,
            "n_total_chunks": self.n_total_chunks,
            "chunk_i": self.chunk_i,
        }
```

Add `file_mtime`:

```python
    @property
    def callback_kwargs(self):
        """
        keyword arguments that should be sent to the producer callback function
        when the chunk is produced
        """
        return {
            "filepath": self.__filepath,
            "filename": self.filename,
            "n_total_chunks": self.n_total_chunks,
            "chunk_i": self.chunk_i,
            "file_mtime": self.file_mtime,
        }
```

#### 1c. Add `file_mtime` to `__eq__`

Find the `__eq__` method. Add `file_mtime` comparison. After the line:

```python
        retval = retval and self.filename_append == other.filename_append
```

Add:

```python
        retval = retval and self.file_mtime == other.file_mtime
```

(before the `retval = retval and self.data == other.data` line)

#### 1d. Add `file_mtime` to `__str__`

Find the `__str__` method. Add `file_mtime` to the string representation.
After the line:

```python
            f"filename_append: {self.filename_append})"  # , '
```

Replace that closing `)` line with:

```python
            f"filename_append: {self.filename_append}, "
            f"file_mtime: {self.file_mtime})"
```

### 2. `openmsistream/data_file_io/entity/upload_data_file.py`

#### 2a. Capture mtime in `_build_list_of_file_chunks`

In the `_build_list_of_file_chunks` method, capture the file's mtime before
reading. Find the line:

```python
        # start a hash for the file and the lists of chunks
        file_hash = sha512()
```

Add BEFORE it:

```python
        # capture the file's modification time for generation tiebreaking
        file_mtime = self.filepath.stat().st_mtime
```

#### 2b. Store mtime and pass to chunks

Find where `self.__file_hash` is set (near the end of `_build_list_of_file_chunks`).
The exact lines will depend on whether PR 3's stability check was applied.
Look for:

```python
        # set the hash for the file
        self.__file_hash = file_hash
```

Add after it:

```python
        self.__file_mtime = file_mtime
```

Also make sure `__file_mtime` is initialized in `__init__`. Find the line in
`UploadDataFile.__init__`:

```python
        self.__file_hash = None
```

Add after it:

```python
        self.__file_mtime = None
```

#### 2c. Pass mtime to DataFileChunk constructor

In `add_chunks_to_upload`, find where `DataFileChunk` is constructed:

```python
                self.chunks_to_upload.append(
                    DataFileChunk(
                        self.filepath,
                        self.filename,
                        self.__file_hash,
                        chunk[0],
                        chunk[1],
                        chunk[2],
                        chunk[3],
                        ichunk,
                        self.__n_total_chunks,
                        rootdir=self.rootdir,
                        filename_append=self.__filename_append,
                    )
                )
```

Add `file_mtime`:

```python
                self.chunks_to_upload.append(
                    DataFileChunk(
                        self.filepath,
                        self.filename,
                        self.__file_hash,
                        chunk[0],
                        chunk[1],
                        chunk[2],
                        chunk[3],
                        ichunk,
                        self.__n_total_chunks,
                        rootdir=self.rootdir,
                        filename_append=self.__filename_append,
                        file_mtime=self.__file_mtime,
                    )
                )
```

### 3. `openmsistream/kafka_wrapper/serialization.py`

#### 3a. Serializer: add mtime as 10th field

In `DataFileChunkSerializer.__call__`, find the ordered_properties list.
After:

```python
            ordered_properties.append(file_chunk_obj.data)
```

Add:

```python
            ordered_properties.append(
                file_chunk_obj.file_mtime
                if file_chunk_obj.file_mtime is not None
                else 0.0
            )
```

We serialize `None` as `0.0` rather than msgpack `None` to keep the type
consistent (always a float). The deserializer will interpret `0.0` as "no
mtime available" since no real file has mtime=0.

#### 3b. Deserializer: accept 9 or 10 fields

In `DataFileChunkDeserializer.__call__`, find the length check:

```python
            if len(ordered_properties) != 9:
                errmsg = (
                    f"ERROR: unrecognized token passed to DataFileChunkDeserializer. "
                    f"Expected 9 properties but found {len(ordered_properties)}"
                )
                raise ValueError(errmsg)
```

Replace with:

```python
            if len(ordered_properties) not in (9, 10):
                errmsg = (
                    f"ERROR: unrecognized token passed to DataFileChunkDeserializer. "
                    f"Expected 9 or 10 properties but found {len(ordered_properties)}"
                )
                raise ValueError(errmsg)
```

Then after the existing field extraction block (after `data = ordered_properties[8]`),
add:

```python
                file_mtime = None
                if len(ordered_properties) > 9:
                    raw_mtime = float(ordered_properties[9])
                    file_mtime = raw_mtime if raw_mtime > 0.0 else None
```

And update the `DataFileChunk` constructor call at the end of the method.
Find:

```python
            return DataFileChunk(
                filepath,
                filename,
                file_hash,
                chunk_hash,
                chunk_offset_read,
                chunk_offset_write,
                len(data),
                chunk_i,
                n_total_chunks,
                data=data,
                filename_append=filename_append,
            )
```

Replace with:

```python
            return DataFileChunk(
                filepath,
                filename,
                file_hash,
                chunk_hash,
                chunk_offset_read,
                chunk_offset_write,
                len(data),
                chunk_i,
                n_total_chunks,
                data=data,
                filename_append=filename_append,
                file_mtime=file_mtime,
            )
```

### 4. `openmsistream/data_file_io/entity/download_data_file.py`

#### 4a. Add `_expected_file_mtime` to `__init__`

Find where `_expected_file_hash` is initialized (added by PR 1):

```python
        self._expected_file_hash = None
```

Add after it:

```python
        self._expected_file_mtime = None
```

#### 4b. Store mtime when locking in a generation

In `add_chunk`, find where `_expected_file_hash` is set (the `n_total_chunks is None`
branch, added by PR 1):

```python
        if self.n_total_chunks is None:
            with thread_lock:
                self.n_total_chunks = dfc.n_total_chunks
                self._expected_file_hash = dfc.file_hash
```

Add mtime:

```python
        if self.n_total_chunks is None:
            with thread_lock:
                self.n_total_chunks = dfc.n_total_chunks
                self._expected_file_hash = dfc.file_hash
                self._expected_file_mtime = dfc.file_mtime
```

Also add mtime storage in the RESET branch. Find the reset block (from PR 1)
where a newer generation is adopted:

```python
                    with thread_lock:
                        self._reset_for_new_generation()
                        self.n_total_chunks = dfc.n_total_chunks
                        self._expected_file_hash = dfc.file_hash
```

Add mtime:

```python
                    with thread_lock:
                        self._reset_for_new_generation()
                        self.n_total_chunks = dfc.n_total_chunks
                        self._expected_file_hash = dfc.file_hash
                        self._expected_file_mtime = dfc.file_mtime
```

#### 4c. Replace the equal-count different-hash skip with mtime tiebreaker

Find the `elif` block at the end of the generation policy (from PR 1) that
handles equal chunk count with different hash. It currently says:

```python
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

Replace with:

```python
        elif (
            self._expected_file_hash is not None
            and dfc.file_hash != self._expected_file_hash
        ):
            # Same chunk count but different hash — file was modified in place
            # without changing size. Use mtime as a tiebreaker if available:
            # newer mtime wins, older or unknown mtime is skipped.
            if (
                dfc.file_mtime is not None
                and self._expected_file_mtime is not None
                and dfc.file_mtime > self._expected_file_mtime
            ):
                # Incoming chunk has a strictly newer mtime — reset and adopt
                warnmsg = (
                    f"WARNING: {self.__class__.__name__} with filepath "
                    f"{self.full_filepath} received a chunk with same "
                    f"n_total_chunks={dfc.n_total_chunks} but newer mtime "
                    f"({dfc.file_mtime} > {self._expected_file_mtime}) and "
                    f"different hash. Resetting to newer generation."
                )
                self.logger.warning(warnmsg)
                with thread_lock:
                    self._reset_for_new_generation()
                    self.n_total_chunks = dfc.n_total_chunks
                    self._expected_file_hash = dfc.file_hash
                    self._expected_file_mtime = dfc.file_mtime
                return DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE
            else:
                # No mtime, same mtime, or older mtime — skip to avoid thrashing
                return DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
```

#### 4d. Reset mtime in `_reset_for_new_generation`

Find the `_reset_for_new_generation` method in `DownloadDataFile` (from PR 1):

```python
    def _reset_for_new_generation(self):
        self._chunk_offsets_downloaded = []
        self.n_total_chunks = None
        self._expected_file_hash = None
```

Add mtime reset:

```python
    def _reset_for_new_generation(self):
        self._chunk_offsets_downloaded = []
        self.n_total_chunks = None
        self._expected_file_hash = None
        self._expected_file_mtime = None
```

### 5. `test/test_scripts/test_download_data_file.py`

Add a new test at the end of the file that specifically tests the mtime
tiebreaker for equal-count generations:

```python
def test_equal_count_mtime_tiebreaker(output_dir, logger):
    """
    Verify that when two generations have the same n_total_chunks but
    different hashes and different mtimes, the newer mtime wins.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "log_file.csv"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1: 3 chunks, mtime=1000.0
    gen1_data = b"R" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)
    for c in gen1_chunks:
        c.file_mtime = 1000.0

    # Gen2: 3 chunks (same count!), different content, mtime=2000.0
    gen2_data = b"S" * (chunk_size * 3)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)
    for c in gen2_chunks:
        c.file_mtime = 2000.0

    assert gen1_hash != gen2_hash
    assert gen1_chunks[0].n_total_chunks == gen2_chunks[0].n_total_chunks

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept, lock in Gen1 (mtime=1000)
    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Gen2 chunk 1 → same n_total_chunks, different hash, newer mtime → RESET
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    # Gen1 chunk 2 → same n_total_chunks, different hash, OLDER mtime → SKIP
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Gen1 chunk 3 → skip (older mtime)
    r = dl.add_chunk(gen1_chunks[2])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Feed remaining Gen2 chunks
    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i == 0:
            assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
        elif i < len(gen2_chunks) - 1:
            assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
        else:
            assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen2_data


def test_equal_count_no_mtime_skips(output_dir, logger):
    """
    Verify that when mtime is unavailable (old producer), equal-count
    different-hash chunks are skipped (no thrashing, no crash).
    This is the backward-compatibility path.
    """
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "old_format.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    # Gen1 and Gen2: same chunk count, no mtime (simulating old producer)
    gen1_data = b"T" * (chunk_size * 2)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(gen1_data, filename, subdir, chunk_size, gen1_hash)
    # file_mtime defaults to None — simulates old 9-field messages

    gen2_data = b"U" * (chunk_size * 2)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(gen2_data, filename, subdir, chunk_size, gen2_hash)

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(gen1_chunks[0].filepath, logger=logger)

    # Gen1 chunk 1 → accept
    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    # Gen2 chunk 1 → same n, different hash, no mtime → SKIP (not reset)
    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE

    # Gen1 chunk 2 → accept, completes Gen1
    r = dl.add_chunk(gen1_chunks[1])
    assert r == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE

    assert dl.bytestring == gen1_data
```

### 6. `test/test_scripts/test_serialization.py`

Check this file for existing serialization round-trip tests. Add a test that
verifies 10-field messages round-trip correctly, and a test that verifies
9-field backward compatibility. Look at the existing test structure to match
the style. The tests may require Kafka (check for `@pytest.mark.kafka`).

If the existing tests are Kafka-dependent, add a non-Kafka test:

```python
def test_chunk_serialization_with_mtime(logger):
    """Verify DataFileChunk with file_mtime serializes and deserializes correctly."""
    from openmsistream.kafka_wrapper.serialization import (
        DataFileChunkSerializer,
        DataFileChunkDeserializer,
    )
    from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile

    udf = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    udf.add_chunks_to_upload()

    chunk = udf.chunks_to_upload[0]
    chunk.populate_with_file_data(logger=logger)

    # Chunk should have mtime set from the file
    assert chunk.file_mtime is not None
    assert chunk.file_mtime > 0.0

    # Serialize and deserialize
    serializer = DataFileChunkSerializer()
    deserializer = DataFileChunkDeserializer()

    packed = serializer(chunk)
    unpacked = deserializer(packed)

    assert unpacked.file_mtime == chunk.file_mtime
    assert unpacked.n_total_chunks == chunk.n_total_chunks
    assert unpacked.file_hash == chunk.file_hash
    assert unpacked.chunk_i == chunk.chunk_i


def test_chunk_deserialization_backward_compat(logger):
    """Verify that 9-field messages (no mtime) still deserialize correctly."""
    import msgpack
    from hashlib import sha512
    from openmsistream.kafka_wrapper.serialization import DataFileChunkDeserializer

    # Manually create a 9-field msgpack payload (old format)
    data = b"test data for backward compat"
    chunk_hash = sha512(data).digest()
    file_hash = sha512(data).digest()

    old_format = [
        b"testfile.dat",       # filename
        file_hash,             # file_hash
        chunk_hash,            # chunk_hash
        0,                     # chunk_offset_write
        1,                     # chunk_i
        1,                     # n_total_chunks
        b"subdir",             # subdir_str
        b"",                   # filename_append
        data,                  # data
    ]
    packed = msgpack.packb(old_format, use_bin_type=True)

    deserializer = DataFileChunkDeserializer()
    chunk = deserializer(packed)

    assert chunk.file_mtime is None  # old format has no mtime
    assert chunk.filename == "testfile.dat"
    assert chunk.n_total_chunks == 1
    assert chunk.data == data
```

## Verification

Run all affected tests:

```bash
# Download file tests (PR 1 tests + new mtime tests)
pytest test/test_scripts/test_download_data_file.py -v

# Serialization tests (round-trip + backward compat)
pytest test/test_scripts/test_serialization.py -v -k "mtime or backward_compat" 2>/dev/null || \
pytest test/test_scripts/test_serialization.py::test_chunk_serialization_with_mtime \
       test/test_scripts/test_serialization.py::test_chunk_deserialization_backward_compat -v

# Data file chunk tests (verify mtime in eq, str, props)
pytest test/test_scripts/test_data_file_chunk.py -v -k "not kafka"
```

Also verify existing tests don't regress:

```bash
pytest test/test_scripts/test_download_data_file.py test/test_scripts/test_upload_data_file.py -v
```

## Commit and PR

```bash
git add -A
git commit -m "feat: add file_mtime to chunk messages for generation tiebreaking

Adds the file's modification time (mtime) as a 10th field in the msgpack
serialization format. This provides a tiebreaker when two upload generations
have the same n_total_chunks but different file_hash (e.g., a CSV log file
that gains one row without crossing a chunk boundary).

Wire format change is backward compatible:
- Deserializer accepts both 9 fields (old) and 10 fields (new)
- Old messages get file_mtime=None
- When mtime is unavailable, the PR 1 behavior is preserved (skip
  indeterminate chunks to avoid thrashing)

Tiebreaker logic in add_chunk():
- Same chunk count, different hash, newer mtime → reset (adopt newer)
- Same chunk count, different hash, older/no mtime → skip

Deploy consumers before producers to avoid old consumers rejecting
the new 10-field format.

Fixes #<ISSUE_NUMBER>"
git push -u origin feat/chunk-mtime-tiebreaker

gh pr create \
  --title "feat: add file_mtime to chunk messages for generation tiebreaking" \
  --body "## Problem

PR 1 handles multi-generation chunks with a directional policy, but when
a file is modified without changing its chunk count (e.g., appending one
CSV row), it cannot determine which generation is newer and defaults to
the first-seen version.

## Fix

Adds \`file_mtime\` (float, seconds since epoch) as a 10th field in the
chunk message serialization:

\`\`\`
Old: [filename, file_hash, chunk_hash, offset, chunk_i, n_total, subdir, append, data]
New: [filename, file_hash, chunk_hash, offset, chunk_i, n_total, subdir, append, data, mtime]
\`\`\`

Backward compatible: deserializer accepts 9 or 10 fields.

| Condition | Action |
|-----------|--------|
| Same chunk count, different hash, newer mtime | Reset → adopt |
| Same chunk count, different hash, older/no mtime | Skip |

## Deployment

**Deploy consumers first** (accept both formats), then producers (send
new format). Old consumers reject 10-field messages.

## Changes

- \`data_file_chunk.py\`: Added \`file_mtime\` parameter
- \`upload_data_file.py\`: Captures \`stat().st_mtime\` during chunking
- \`serialization.py\`: 10-field serialize, 9-or-10 deserialize
- \`download_data_file.py\`: mtime tiebreaker in equal-count policy
- Tests: mtime tiebreaker, backward compat, serialization round-trip

## Testing

\`\`\`
pytest test/test_scripts/test_download_data_file.py test/test_scripts/test_serialization.py -v
\`\`\`" \
  --base main
```

Replace `<ISSUE_NUMBER>` with the actual issue number from `gh issue create`.
