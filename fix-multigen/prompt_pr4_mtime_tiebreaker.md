# Claude Code Prompt — PR 4: Add file_mtime to chunk messages for generation tiebreaking

## Context

Read `fix-multigen/ROADMAP.md` for full background. This PR depends on PR 1.

PR 1 implemented a directional generation policy: strictly higher `n_total_chunks`
wins. **The gap:** when a file is modified with a small addition (e.g., one CSV
row), the hash changes but `n_total_chunks` may stay the same. PR 1 handles
this safely (no crash, first-seen wins) but may reconstruct the older version.

This PR adds `file_mtime` (modification time) to each chunk message as a
tiebreaker. When two generations have equal chunk count but different hash,
the newer mtime wins.

This requires a **backward-compatible change to the msgpack wire format**:
- Serializer sends 10 fields (was 9)
- Deserializer accepts both 9 fields (old, `file_mtime=None`) and 10 fields

**Deploy consumers first, then producers.**

## IMPORTANT: Code Style

This project enforces `black` formatting (`line-length=90`, `target-version=['py39']`).
**You MUST run `black` on all changed files before committing.**

## Create Issue

```bash
gh issue create \
  --title "feat: add file_mtime to chunk messages for generation tiebreaking" \
  --body "## Problem

PR 1's directional policy cannot determine which generation is newer when
two have the same chunk count but different hashes. It defaults to first-seen,
which may be older.

## Fix

Add file mtime as a 10th field in the serialized chunk message. When chunk
count is equal but hash differs, newer mtime wins. Backward compatible:
deserializer accepts 9 or 10 fields. Deploy consumers before producers.

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

Add `file_mtime=None` as the last keyword parameter in `__init__`, and store
it as `self.file_mtime = file_mtime` in the body.

#### 1b. Add `file_mtime` to `callback_kwargs`

Add `"file_mtime": self.file_mtime` to the returned dict.

#### 1c. Add `file_mtime` to `__eq__`

Add `retval = retval and self.file_mtime == other.file_mtime` before the
`self.data` comparison.

#### 1d. Add `file_mtime` to `__str__`

Include `file_mtime: {self.file_mtime}` in the string representation.

### 2. `openmsistream/data_file_io/entity/upload_data_file.py`

#### 2a. Initialize `__file_mtime` in `__init__`

After `self.__file_hash = None`, add `self.__file_mtime = None`.

#### 2b. Capture mtime in `_build_list_of_file_chunks`

Before the `file_hash = sha512()` line, add:
```python
        file_mtime = self.filepath.stat().st_mtime
```

After `self.__file_hash = file_hash` (near the end of the method), add:
```python
        self.__file_mtime = file_mtime
```

#### 2c. Pass mtime to DataFileChunk constructor

In `add_chunks_to_upload`, add `file_mtime=self.__file_mtime` to the
`DataFileChunk(...)` constructor call.

### 3. `openmsistream/kafka_wrapper/serialization.py`

#### 3a. Serializer: add mtime as 10th field

In `DataFileChunkSerializer.__call__`, after
`ordered_properties.append(file_chunk_obj.data)`, add:

```python
            ordered_properties.append(
                file_chunk_obj.file_mtime
                if file_chunk_obj.file_mtime is not None
                else 0.0
            )
```

#### 3b. Deserializer: accept 9 or 10 fields

Change `if len(ordered_properties) != 9:` to
`if len(ordered_properties) not in (9, 10):` and update the error message.

After `data = ordered_properties[8]`, add:

```python
                file_mtime = None
                if len(ordered_properties) > 9:
                    raw_mtime = float(ordered_properties[9])
                    file_mtime = (
                        raw_mtime if raw_mtime > 0.0 else None
                    )
```

Add `file_mtime=file_mtime` to the `DataFileChunk(...)` constructor call
at the end of the method.

### 4. `openmsistream/data_file_io/entity/download_data_file.py`

#### 4a. Add `_expected_file_mtime` to `__init__`

After `self._expected_file_hash = None` (from PR 1), add:
```python
        self._expected_file_mtime = None
```

#### 4b. Store mtime when locking in a generation

In the `if self.n_total_chunks is None:` branch, add:
```python
                self._expected_file_mtime = dfc.file_mtime
```

In the RESET branch (strictly higher n_total_chunks), also add:
```python
                        self._expected_file_mtime = dfc.file_mtime
```

#### 4c. Replace the equal-count different-hash skip with mtime tiebreaker

Find the `elif` block from PR 1 that handles same-count different-hash:

```python
        elif (
            self._expected_file_hash is not None
            and dfc.file_hash != self._expected_file_hash
        ):
            # Same chunk count but different hash ...
            return DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
```

Replace with:

```python
        elif (
            self._expected_file_hash is not None
            and dfc.file_hash != self._expected_file_hash
        ):
            # Same chunk count but different hash — file modified in
            # place. Use mtime as tiebreaker if available.
            if (
                dfc.file_mtime is not None
                and self._expected_file_mtime is not None
                and dfc.file_mtime > self._expected_file_mtime
            ):
                warnmsg = (
                    f"WARNING: {self.__class__.__name__} with "
                    f"filepath {self.full_filepath} received a "
                    f"chunk with same n_total_chunks="
                    f"{dfc.n_total_chunks} but newer mtime "
                    f"({dfc.file_mtime} > "
                    f"{self._expected_file_mtime}) and different "
                    f"hash. Resetting to newer generation."
                )
                self.logger.warning(warnmsg)
                with thread_lock:
                    self._reset_for_new_generation()
                    self.n_total_chunks = dfc.n_total_chunks
                    self._expected_file_hash = dfc.file_hash
                    self._expected_file_mtime = dfc.file_mtime
                return (
                    DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE
                )
            else:
                return (
                    DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
                )
```

#### 4d. Reset mtime in `_reset_for_new_generation`

Add `self._expected_file_mtime = None` to the base class method.

### 5. Tests

Add to `test/test_scripts/test_download_data_file.py`:

```python
def test_equal_count_mtime_tiebreaker(output_dir, logger):
    """When two generations have the same chunk count but different
    hashes and mtimes, the newer mtime wins."""
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "log_file.csv"
    subdir = pathlib.PurePosixPath("test_subdir")

    gen1_data = b"R" * (chunk_size * 3)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(
        gen1_data, filename, subdir, chunk_size, gen1_hash
    )
    for c in gen1_chunks:
        c.file_mtime = 1000.0

    gen2_data = b"S" * (chunk_size * 3)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(
        gen2_data, filename, subdir, chunk_size, gen2_hash
    )
    for c in gen2_chunks:
        c.file_mtime = 2000.0

    assert gen1_hash != gen2_hash

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(
        gen1_chunks[0].filepath, logger=logger
    )

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.GENERATION_RESET_CODE

    r = dl.add_chunk(gen1_chunks[1])
    assert (
        r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
    )

    for i, chunk in enumerate(gen2_chunks):
        r = dl.add_chunk(chunk)
        if i == 0:
            assert (
                r
                == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
            )
        elif i < len(gen2_chunks) - 1:
            assert (
                r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
            )
        else:
            assert (
                r
                == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE
            )

    assert dl.bytestring == gen2_data


def test_equal_count_no_mtime_skips(output_dir, logger):
    """When mtime is unavailable (old producer), equal-count
    different-hash chunks are skipped (backward compat)."""
    from hashlib import sha512

    chunk_size = TEST_CONST.TEST_CHUNK_SIZE
    filename = "old_format.dat"
    subdir = pathlib.PurePosixPath("test_subdir")

    gen1_data = b"T" * (chunk_size * 2)
    gen1_hash = sha512(gen1_data).digest()
    gen1_chunks = _make_test_chunks(
        gen1_data, filename, subdir, chunk_size, gen1_hash
    )

    gen2_data = b"U" * (chunk_size * 2)
    gen2_hash = sha512(gen2_data).digest()
    gen2_chunks = _make_test_chunks(
        gen2_data, filename, subdir, chunk_size, gen2_hash
    )

    for c in gen1_chunks + gen2_chunks:
        c.rootdir = output_dir

    dl = DownloadDataFileToMemory(
        gen1_chunks[0].filepath, logger=logger
    )

    r = dl.add_chunk(gen1_chunks[0])
    assert r == DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS

    r = dl.add_chunk(gen2_chunks[0])
    assert (
        r == DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE
    )

    r = dl.add_chunk(gen1_chunks[1])
    assert (
        r
        == DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE
    )

    assert dl.bytestring == gen1_data
```

Add to `test/test_scripts/test_serialization.py`:

```python
def test_chunk_serialization_with_mtime(logger):
    """Verify DataFileChunk with file_mtime round-trips."""
    from openmsistream.kafka_wrapper.serialization import (
        DataFileChunkSerializer,
        DataFileChunkDeserializer,
    )
    from openmsistream.data_file_io.entity.upload_data_file import (
        UploadDataFile,
    )

    udf = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    udf.add_chunks_to_upload()

    chunk = udf.chunks_to_upload[0]
    chunk.populate_with_file_data(logger=logger)

    assert chunk.file_mtime is not None
    assert chunk.file_mtime > 0.0

    serializer = DataFileChunkSerializer()
    deserializer = DataFileChunkDeserializer()

    packed = serializer(chunk)
    unpacked = deserializer(packed)

    assert unpacked.file_mtime == chunk.file_mtime
    assert unpacked.n_total_chunks == chunk.n_total_chunks
    assert unpacked.file_hash == chunk.file_hash


def test_chunk_deserialization_backward_compat(logger):
    """Verify 9-field messages (no mtime) still deserialize."""
    import msgpack
    from hashlib import sha512
    from openmsistream.kafka_wrapper.serialization import (
        DataFileChunkDeserializer,
    )

    data = b"test data for backward compat"
    chunk_hash = sha512(data).digest()
    file_hash = sha512(data).digest()

    old_format = [
        b"testfile.dat",
        file_hash,
        chunk_hash,
        0,
        1,
        1,
        b"subdir",
        b"",
        data,
    ]
    packed = msgpack.packb(old_format, use_bin_type=True)

    deserializer = DataFileChunkDeserializer()
    chunk = deserializer(packed)

    assert chunk.file_mtime is None
    assert chunk.filename == "testfile.dat"
    assert chunk.n_total_chunks == 1
    assert chunk.data == data
```

## Verification

**Format with black FIRST:**

```bash
black --line-length 90 --target-version py39 \
  openmsistream/data_file_io/entity/data_file_chunk.py \
  openmsistream/data_file_io/entity/upload_data_file.py \
  openmsistream/data_file_io/entity/download_data_file.py \
  openmsistream/kafka_wrapper/serialization.py \
  test/test_scripts/test_download_data_file.py \
  test/test_scripts/test_serialization.py
```

Run tests:

```bash
pytest test/test_scripts/test_download_data_file.py \
       test/test_scripts/test_serialization.py -v \
       -k "not kafka"
```

Verify black:

```bash
black --check --line-length 90 --target-version py39 \
  openmsistream/data_file_io/entity/data_file_chunk.py \
  openmsistream/data_file_io/entity/upload_data_file.py \
  openmsistream/data_file_io/entity/download_data_file.py \
  openmsistream/kafka_wrapper/serialization.py \
  test/test_scripts/test_download_data_file.py \
  test/test_scripts/test_serialization.py
```

## Commit and PR

```bash
git add -A
git commit -m "feat: add file_mtime to chunk messages for generation tiebreaking

Adds the file's modification time (mtime) as a 10th field in the msgpack
serialization format. Provides a tiebreaker when two upload generations
have the same n_total_chunks but different file_hash.

Wire format is backward compatible:
- Deserializer accepts both 9 (old) and 10 (new) fields
- Old messages get file_mtime=None
- When mtime unavailable, PR 1 behavior preserved (skip indeterminate)

Deploy consumers before producers.

Fixes #<ISSUE_NUMBER>"
git push -u origin feat/chunk-mtime-tiebreaker

gh pr create \
  --title "feat: add file_mtime to chunk messages for generation tiebreaking" \
  --body "## Problem

PR 1 cannot determine which generation is newer when two have the same
chunk count but different hashes. Defaults to first-seen.

## Fix

Adds \`file_mtime\` as 10th msgpack field. Newer mtime wins tiebreak.
Backward compatible: deserializer accepts 9 or 10 fields.
**Deploy consumers first, then producers.**

## Changes

- \`data_file_chunk.py\`: Added \`file_mtime\` parameter
- \`upload_data_file.py\`: Captures \`stat().st_mtime\`
- \`serialization.py\`: 10-field serialize, 9-or-10 deserialize
- \`download_data_file.py\`: mtime tiebreaker in equal-count policy
- Tests: mtime tiebreaker, backward compat, serialization round-trip" \
  --base main
```
