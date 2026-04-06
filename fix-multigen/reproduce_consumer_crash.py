#!/usr/bin/env python3
"""
Reproduce the multi-generation path collision bug in OpenMSIStream.

This script creates two "generations" of the same file (different sizes,
different hashes) and feeds their chunks to DownloadDataFileToDisk.add_chunk()
exactly as a consumer would encounter them on a topic containing mixed
generations.

Expected output: the exact ValueError from the production logs.

Usage:
    cd /Users/elbert/Documents/GitHub/openmsistream
    python fix-multigen/reproduce_consumer_crash.py
"""

import logging
import pathlib
import tempfile
from hashlib import sha512

from openmsistream.data_file_io.entity.data_file_chunk import DataFileChunk
from openmsistream.data_file_io.entity.download_data_file import DownloadDataFileToDisk
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger

CHUNK_SIZE = 524288  # default OpenMSIStream chunk size


def make_chunks(data: bytes, filename: str, subdir: str, chunk_size: int):
    """Break data into DataFileChunks, mimicking what the producer does."""
    file_hash = sha512(data).digest()
    chunks = []
    offset = 0
    chunk_i = 1
    while offset < len(data):
        end = min(offset + chunk_size, len(data))
        chunk_data = data[offset:end]
        chunk_hash = sha512(chunk_data).digest()
        n_total = (len(data) + chunk_size - 1) // chunk_size
        chunks.append(
            DataFileChunk(
                pathlib.PurePosixPath(subdir) / filename,
                filename,
                file_hash,
                chunk_hash,
                None,           # chunk_offset_read (not needed for download)
                offset,         # chunk_offset_write
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

    # --- Simulate what the Keyence indenter does ---
    # Generation 1: instrument writes first test (small file)
    gen1_data = b"A" * (CHUNK_SIZE + 100)  # just over 1 chunk → 2 chunks
    gen1_chunks = make_chunks(gen1_data, "Test1.NMD", "FS_500um/FS_500um_20260319_1", CHUNK_SIZE)

    # Generation 2: instrument appends second test (file grows)
    gen2_data = gen1_data + b"B" * CHUNK_SIZE  # grows to 3 chunks
    gen2_chunks = make_chunks(gen2_data, "Test1.NMD", "FS_500um/FS_500um_20260319_1", CHUNK_SIZE)

    print(f"Generation 1: {len(gen1_data)} bytes → {len(gen1_chunks)} chunks (n_total_chunks={gen1_chunks[0].n_total_chunks})")
    print(f"Generation 2: {len(gen2_data)} bytes → {len(gen2_chunks)} chunks (n_total_chunks={gen2_chunks[0].n_total_chunks})")
    print(f"Gen1 file_hash: {gen1_chunks[0].file_hash[:8].hex()}")
    print(f"Gen2 file_hash: {gen2_chunks[0].file_hash[:8].hex()}")
    print()

    # --- Simulate what the consumer sees ---
    # The topic has Gen1 chunks followed by Gen2 chunks (append-only)
    all_messages = gen1_chunks + gen2_chunks
    print(f"Topic has {len(all_messages)} total messages for this filepath")
    print(f"  Gen1 chunks: {len(gen1_chunks)} messages with n_total_chunks={gen1_chunks[0].n_total_chunks}")
    print(f"  Gen2 chunks: {len(gen2_chunks)} messages with n_total_chunks={gen2_chunks[0].n_total_chunks}")
    print()

    # Create a temporary output directory for reconstruction
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = pathlib.Path(tmpdir)

        # Set rootdir on all chunks (mimics what data_file_chunk_handlers does)
        for chunk in all_messages:
            chunk.rootdir = output_dir

        # Create the DownloadDataFile (mimics what data_file_chunk_handlers does)
        dl = DownloadDataFileToDisk(all_messages[0].filepath, logger=logger)

        print("Feeding chunks to consumer in topic order...")
        print()
        for i, chunk in enumerate(all_messages):
            gen = "Gen1" if i < len(gen1_chunks) else "Gen2"
            print(f"  Message {i+1}/{len(all_messages)}: {gen} chunk {chunk.chunk_i} of {chunk.n_total_chunks} ... ", end="")
            try:
                result = dl.add_chunk(chunk)
                print(f"OK (code={result})")
            except ValueError as e:
                print(f"\n\n{'='*70}")
                print("REPRODUCED THE BUG!")
                print(f"{'='*70}")
                print(f"\nValueError: {e}")
                print(f"\nThis is the EXACT error from the production logs.")
                print(f"The consumer locked in n_total_chunks={gen1_chunks[0].n_total_chunks} from Gen1,")
                print(f"then crashed when it encountered a Gen2 chunk with n_total_chunks={gen2_chunks[0].n_total_chunks}.")
                print(f"\nIn production, this thread crashes, restarts, re-polls")
                print(f"the same message, and loops forever.")
                return True

    print("\nBug was NOT reproduced — something has changed.")
    return False


if __name__ == "__main__":
    reproduced = main()
    exit(0 if reproduced else 1)
