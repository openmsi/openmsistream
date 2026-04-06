# Claude Code Prompt — PR 2: Generation-Aware Message Keys

## Context

Read `fix-multigen/ROADMAP.md` for full background.

Currently the Kafka message key for a DataFileChunk is:

```
{subdir}_{filename}_chunk_{i}_of_{n}
```

This format does not distinguish between different upload generations of the
same file. When inspecting a topic with `kcat`, all generations of the same
file look identical except for the `_of_{n}` suffix, which only tells you
the total chunk count — not WHICH upload produced it.

By adding the first 8 hex characters of `file_hash` to the key, chunks
from different generations become visually and programmatically distinct:

```
{subdir}_{filename}_chunk_{i}_of_{n}_{hash8}
```

This is **backward compatible** — consumers never parse the key format;
they use the deserialized `DataFileChunk.relative_filepath` for routing.

## Create Issue

```bash
gh issue create \
  --title "feat: include file hash prefix in chunk message keys" \
  --body "## Motivation

When a file is uploaded multiple times (multi-generation), all chunks from
all generations share the same message key pattern. This makes it impossible
to distinguish generations when inspecting the topic with \`kcat\` or similar
tools.

## Change

Append the first 8 hex characters of \`file_hash\` to the message key:

\`\`\`
Old: {subdir}_{filename}_chunk_{i}_of_{n}
New: {subdir}_{filename}_chunk_{i}_of_{n}_{hash8}
\`\`\`

Backward compatible — consumers never parse the key format.

## Files

- \`openmsistream/data_file_io/entity/data_file_chunk.py\`
- \`test/test_scripts/test_data_file_chunk.py\`"
```

## Branch

```bash
git checkout main
git pull
git checkout -b fix/generation-aware-msg-key
```

## Changes

### 1. `openmsistream/data_file_io/entity/data_file_chunk.py`

Find the `msg_key` property:

```python
    @property
    def msg_key(self):
        """
        string representing the key of the message this chunk will be produced as
        """
        key_pp = get_message_prepend(self.subdir_str, self.filename)
        return f"{key_pp}_{self.chunk_i}_of_{self.n_total_chunks}"
```

Replace with:

```python
    @property
    def msg_key(self):
        """
        string representing the key of the message this chunk will be produced as.
        Includes the first 8 hex characters of the file hash to distinguish
        chunks from different upload generations of the same file.
        """
        key_pp = get_message_prepend(self.subdir_str, self.filename)
        hash_prefix = self.file_hash[:4].hex() if self.file_hash else "nohash"
        return f"{key_pp}_{self.chunk_i}_of_{self.n_total_chunks}_{hash_prefix}"
```

Note: `self.file_hash` is a bytes object (SHA-512 digest). `[:4]` takes the
first 4 bytes, `.hex()` converts to 8 hex characters. This produces a compact
but sufficient prefix for visual disambiguation (4 billion possible values).

### 2. `test/test_scripts/test_data_file_chunk.py`

Find the `test_props` method and its assertion about `msg_key` (if any).
The existing test checks `subdir_str` but may not directly assert on `msg_key`
format. Add a test that verifies the new key format:

Add a new test method or a standalone test function at the end of the file.
If the tests use a class structure with `@pytest.mark.kafka`, add the test
as a standalone function that does NOT require Kafka:

```python
def test_msg_key_includes_hash_prefix(logger):
    """Verify that msg_key includes a file hash prefix for generation disambiguation."""
    udf = UploadDataFile(
        TEST_CONST.TEST_DATA_FILE_PATH,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        logger=logger,
    )
    udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    udf.add_chunks_to_upload()

    chunk = udf.chunks_to_upload[0]
    chunk.populate_with_file_data(logger=logger)

    key = chunk.msg_key
    # Key should end with _HASH8 (8 hex chars)
    parts = key.rsplit("_", 1)
    assert len(parts) == 2, f"Expected key to end with _hash, got: {key}"
    hash_part = parts[1]
    assert len(hash_part) == 8, f"Expected 8 hex chars, got {len(hash_part)}: {hash_part}"
    # Verify it's valid hex
    int(hash_part, 16)

    # Verify the hash prefix matches the actual file_hash
    expected_prefix = chunk.file_hash[:4].hex()
    assert hash_part == expected_prefix

    # Verify two chunks from the same file have the same hash prefix
    chunk2 = udf.chunks_to_upload[1]
    chunk2.populate_with_file_data(logger=logger)
    key2 = chunk2.msg_key
    assert key2.rsplit("_", 1)[1] == hash_part
```

## Verification

Run the unit tests:

```bash
pytest test/test_scripts/test_data_file_chunk.py::test_msg_key_includes_hash_prefix -v
```

Also run the download file tests to make sure nothing regresses:

```bash
pytest test/test_scripts/test_download_data_file.py -v
```

Also check that the serialization round-trip still works (the key is
serialized separately from the value, so this should be fine):

```bash
pytest test/test_scripts/test_serialization.py -v --no-header -q 2>/dev/null || true
```

(The serialization tests may need Kafka — if they fail due to missing broker,
that's OK. The key format change doesn't affect serialization.)

## Commit and PR

```bash
git add -A
git commit -m "feat: include file hash prefix in chunk message keys

Appends the first 8 hex characters of file_hash to the message key:

  Old: {subdir}_{filename}_chunk_{i}_of_{n}
  New: {subdir}_{filename}_chunk_{i}_of_{n}_{hash8}

This makes chunks from different upload generations visually and
programmatically distinct when inspecting topics with kcat or
similar tools.

Backward compatible — consumers do not parse the key format.

Fixes #<ISSUE_NUMBER>"
git push -u origin fix/generation-aware-msg-key

gh pr create \
  --title "feat: include file hash prefix in chunk message keys" \
  --body "## Change

Appends file hash prefix to chunk message keys for generation disambiguation.

\`\`\`
Old: test_subdir_file.dat_chunk_1_of_10
New: test_subdir_file.dat_chunk_1_of_10_a1b2c3d4
\`\`\`

Backward compatible — consumers never parse the key format.

## Files

- \`data_file_chunk.py\`: Modified \`msg_key\` property
- \`test_data_file_chunk.py\`: Added \`test_msg_key_includes_hash_prefix\`" \
  --base main
```

Replace `<ISSUE_NUMBER>` with the issue number from the `gh issue create` output.
