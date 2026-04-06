#!/usr/bin/env python3
"""
Reproduce the multi-generation path collision bug in OpenMSIStream.
No Kafka broker needed — feeds chunks directly to add_chunk().

Usage:
    cd /Users/elbert/Documents/GitHub/openmsistream
    python fix-multigen/reproduce_consumer_crash.py
"""

import logging
import pathlib
import tempfile
from hashlib import sha512

from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk
from openmsistream.data_file_io.entity.download_data_file import (
    DownloadDataFileToDisk,
)
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger

CHUNK_SIZE = 524288


def make_chunks(data, filename, subdir, chunk_size):
    file_hash = sha512(data).digest()
    chunks = []
    offset = 0
    chunk_i = 1
    n_total = -(-len(data) // chunk_size)
    while offset < len(data):
        end = min(offset + chunk_size, len(data))
        chunk_data = data[offset:end]
        chunk_hash = sha512(chunk_data).digest()
        chunks.append(
            DataFileChunk(
                pathlib.PurePosixPath(subdir) / filename,
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


def main():
    logger = OpenMSILogger("reproduce_bug", streamlevel=logging.WARNING)

    gen1_data = b"A" * (CHUNK_SIZE + 100)
    gen1_chunks = make_chunks(
        gen1_data,
        "Test1.NMD",
        "FS_500um/FS_500um_20260319_1",
        CHUNK_SIZE,
    )

    gen2_data = gen1_data + b"B" * CHUNK_SIZE
    gen2_chunks = make_chunks(
        gen2_data,
        "Test1.NMD",
        "FS_500um/FS_500um_20260319_1",
        CHUNK_SIZE,
    )

    print(f"Gen1: {len(gen1_data)} bytes, {len(gen1_chunks)} chunks")
    print(f"Gen2: {len(gen2_data)} bytes, {len(gen2_chunks)} chunks")
    print()

    all_messages = gen1_chunks + gen2_chunks
    print(f"Topic has {len(all_messages)} messages")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = pathlib.Path(tmpdir)
        for chunk in all_messages:
            chunk.rootdir = output_dir

        dl = DownloadDataFileToDisk(all_messages[0].filepath, logger=logger)

        print("Feeding chunks to consumer...")
        for i, chunk in enumerate(all_messages):
            gen = "Gen1" if i < len(gen1_chunks) else "Gen2"
            print(
                f"  Msg {i+1}/{len(all_messages)}: {gen} "
                f"chunk {chunk.chunk_i} of {chunk.n_total_chunks} ... ",
                end="",
            )
            try:
                result = dl.add_chunk(chunk)
                print(f"OK (code={result})")
            except ValueError as e:
                print(f"\n\n{'='*60}")
                print("REPRODUCED THE BUG!")
                print(f"{'='*60}")
                print(f"\nValueError: {e}")
                return True

    print("\nBug NOT reproduced.")
    return False


if __name__ == "__main__":
    exit(0 if main() else 1)
