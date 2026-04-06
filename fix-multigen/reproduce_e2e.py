#!/usr/bin/env python3
"""
End-to-end reproduction of the multi-generation path collision bug.

This script:
1. Creates a file (1 chunk)
2. Produces it to a Kafka topic via UploadDataFile
3. Appends data so the file becomes 2 chunks
4. Produces it again (same filepath, different n_total_chunks)
5. Launches a consumer that will hit the ValueError

Prerequisites:
  - A Kafka broker accessible via a config file
  - pip install openmsistream (or editable install from this repo)

Usage:
    cd /Users/elbert/Documents/GitHub/openmsistream

    # Create a config file pointing to your broker, e.g.:
    #   [broker]
    #   bootstrap.servers = <your-broker>
    #   security.protocol = SASL_SSL
    #   sasl.mechanism = PLAIN
    #   sasl.username = <key>
    #   sasl.password = <secret>
    #   [producer]
    #   key.serializer = StringSerializer
    #   value.serializer = DataFileChunkSerializer
    #   [consumer]
    #   group.id = create_new
    #   auto.offset.reset = earliest
    #   enable.auto.commit = False
    #   key.deserializer = StringDeserializer
    #   value.deserializer = DataFileChunkDeserializer

    python fix-multigen/reproduce_e2e.py <config_file> <topic_name>

    # Example:
    python fix-multigen/reproduce_e2e.py my_broker.config test-multigen-repro
"""

import pathlib
import sys
import tempfile
import time

from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile
from openmsistream.data_file_io.actor.data_file_stream_processor import (
    DataFileStreamProcessor,
)
from openmsitoolbox.logging.openmsi_logger import OpenMSILogger
from openmsitoolbox.utilities.exception_tracking_thread import ExceptionTrackingThread

CHUNK_SIZE = 16384  # use small chunks for fast test


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <config_file> <topic_name>")
        print(f"  config_file: path to a .config file with [broker], [producer], [consumer] sections")
        print(f"  topic_name:  Kafka topic to use (will be polluted — use a disposable one)")
        sys.exit(1)

    config_path = pathlib.Path(sys.argv[1])
    topic_name = sys.argv[2]
    logger = OpenMSILogger("e2e_repro", streamlevel="INFO")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        rootdir = tmpdir / "upload"
        rootdir.mkdir()
        test_file = rootdir / "growing_file.dat"

        # ============================================================
        # Step 1: Create a small file (1 chunk)
        # ============================================================
        logger.info("Step 1: Creating file with 1 chunk worth of data...")
        test_file.write_bytes(b"A" * CHUNK_SIZE)
        logger.info(f"  File size: {test_file.stat().st_size} bytes (1 chunk)")

        # ============================================================
        # Step 2: Upload it (Generation 1)
        # ============================================================
        logger.info("Step 2: Uploading Generation 1...")
        ul1 = UploadDataFile(test_file, rootdir=rootdir, logger=logger)
        ul1.upload_whole_file(config_path, topic_name, chunk_size=CHUNK_SIZE)
        logger.info(f"  Gen1 uploaded: n_total_chunks=1")

        # ============================================================
        # Step 3: Append data so the file becomes 2 chunks
        # ============================================================
        logger.info("Step 3: Appending data to file...")
        with open(test_file, "ab") as f:
            f.write(b"B" * CHUNK_SIZE)
        logger.info(f"  File size: {test_file.stat().st_size} bytes (2 chunks)")

        # ============================================================
        # Step 4: Upload it again (Generation 2)
        # ============================================================
        logger.info("Step 4: Uploading Generation 2...")
        ul2 = UploadDataFile(test_file, rootdir=rootdir, logger=logger)
        ul2.upload_whole_file(config_path, topic_name, chunk_size=CHUNK_SIZE)
        logger.info(f"  Gen2 uploaded: n_total_chunks=2")

        # ============================================================
        # Step 5: Consume — should crash with ValueError
        # ============================================================
        logger.info("Step 5: Starting consumer (should crash on generation mismatch)...")
        logger.info("  Watch for the ValueError in the output below.")
        logger.info("  Press Ctrl+C to stop if the crash loop starts.")
        print()

        output_dir = tmpdir / "consumer_output"
        output_dir.mkdir()

        try:
            processor = DataFileStreamProcessor(
                config_path,
                topic_name,
                output_dir=output_dir,
                n_threads=1,
                consumer_group_id="create_new",
                logger=logger,
            )

            # Run the consumer in a thread so we can stop it
            thread = ExceptionTrackingThread(
                target=processor.process_files_as_read
            )
            thread.start()

            # Wait for the crash (or success, which shouldn't happen)
            timeout = 30
            logger.info(f"  Waiting up to {timeout}s for the consumer to process...")
            time.sleep(timeout)

            # If we get here without a crash, something unexpected happened
            processor.control_command_queue.put("q")
            thread.join(timeout=10)

            if thread.caught_exception:
                print(f"\n{'='*70}")
                print("Consumer thread exception:")
                print(f"{'='*70}")
                print(thread.caught_exception)
            else:
                logger.info("Consumer did not crash — check the log output above for WARNING/ERROR lines.")

        except KeyboardInterrupt:
            logger.info("Interrupted by user.")
            try:
                processor.control_command_queue.put("q")
                thread.join(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    main()
