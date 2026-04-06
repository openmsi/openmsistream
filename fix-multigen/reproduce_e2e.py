#!/usr/bin/env python3
"""
End-to-end reproduction of the multi-generation path collision bug.

This script:
1. Creates a file large enough for multiple chunks
2. Produces it to a Kafka topic via UploadDataFile (Generation 1)
3. Appends data so the file grows by several chunks
4. Produces it again (Generation 2 — same filepath, different n_total_chunks)
5. Launches a consumer that reads from the beginning

The topic MUST have multiple partitions so that Gen1 and Gen2 chunks are
interleaved when the consumer polls across partitions. With a single
partition, Gen1 completes before Gen2 starts and the bug is not triggered.

Prerequisites:
  - A Kafka broker accessible via a config file
  - The topic must be created BEFORE running this script with >= 3 partitions:
      kafka-topics --create --topic test-multigen-repro \
        --partitions 3 --replication-factor 1 \
        --bootstrap-server localhost:9092
  - pip install openmsistream (or editable install from this repo)

Usage:
    cd /Users/elbert/Documents/GitHub/openmsistream
    python fix-multigen/reproduce_e2e.py fix-multigen/local-broker.config test-multigen-repro
"""

import logging
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


class SimpleStreamProcessor(DataFileStreamProcessor):
    """Minimal concrete subclass for testing — just logs completed files."""

    def _process_downloaded_data_file(self, datafile, lock):
        self.logger.info(f"  *** File reconstructed: {datafile.filename} ***")
        return None

    @classmethod
    def run_from_command_line(cls, args=None):
        pass


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <config_file> <topic_name>")
        print()
        print("IMPORTANT: The topic must exist with >= 3 partitions BEFORE running.")
        print("Create it with:")
        print("  kafka-topics --create --topic <topic_name> \\")
        print("    --partitions 3 --replication-factor 1 \\")
        print("    --bootstrap-server <broker>")
        sys.exit(1)

    config_path = pathlib.Path(sys.argv[1])
    topic_name = sys.argv[2]
    logger = OpenMSILogger("e2e_repro", streamlevel=logging.INFO)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        rootdir = tmpdir / "upload"
        rootdir.mkdir()
        test_file = rootdir / "growing_file.dat"

        # ============================================================
        # Step 1: Create a file large enough for MANY chunks
        # ============================================================
        n_gen1_chunks = 10
        logger.info(f"Step 1: Creating file with {n_gen1_chunks} chunks of data...")
        test_file.write_bytes(b"A" * (CHUNK_SIZE * n_gen1_chunks))
        logger.info(
            f"  File size: {test_file.stat().st_size} bytes ({n_gen1_chunks} chunks)"
        )

        # ============================================================
        # Step 2: Upload it (Generation 1)
        # ============================================================
        logger.info("Step 2: Uploading Generation 1...")
        ul1 = UploadDataFile(test_file, rootdir=rootdir, logger=logger)
        ul1.upload_whole_file(config_path, topic_name, chunk_size=CHUNK_SIZE)
        logger.info(f"  Gen1 uploaded: n_total_chunks={n_gen1_chunks}")

        # ============================================================
        # Step 3: Append data so the file grows substantially
        # ============================================================
        n_extra_chunks = 5
        logger.info(f"Step 3: Appending {n_extra_chunks} more chunks of data...")
        with open(test_file, "ab") as f:
            f.write(b"B" * (CHUNK_SIZE * n_extra_chunks))
        n_gen2_chunks = n_gen1_chunks + n_extra_chunks
        logger.info(
            f"  File size: {test_file.stat().st_size} bytes ({n_gen2_chunks} chunks)"
        )

        # ============================================================
        # Step 4: Upload it again (Generation 2)
        # ============================================================
        logger.info("Step 4: Uploading Generation 2...")
        ul2 = UploadDataFile(test_file, rootdir=rootdir, logger=logger)
        ul2.upload_whole_file(config_path, topic_name, chunk_size=CHUNK_SIZE)
        logger.info(f"  Gen2 uploaded: n_total_chunks={n_gen2_chunks}")

        logger.info("")
        logger.info(f"Topic now has {n_gen1_chunks + n_gen2_chunks} total messages:")
        logger.info(
            f"  Gen1: {n_gen1_chunks} messages with n_total_chunks={n_gen1_chunks}"
        )
        logger.info(
            f"  Gen2: {n_gen2_chunks} messages with n_total_chunks={n_gen2_chunks}"
        )
        logger.info("")
        logger.info("If the topic has multiple partitions, Gen1 and Gen2 chunks will be")
        logger.info("interleaved when the consumer polls across partitions, triggering")
        logger.info("the ValueError. With a single partition, Gen1 may complete before")
        logger.info("Gen2 starts (no crash).")

        # ============================================================
        # Step 5: Consume — should crash with ValueError
        # ============================================================
        logger.info("")
        logger.info("Step 5: Starting consumer...")
        logger.info(
            "  Watch for 'expecting N chunks but found a chunk from a split with M' error."
        )
        logger.info(
            "  Press Ctrl+C after seeing it (the thread restarts in a crash loop)."
        )
        print()

        output_dir = tmpdir / "consumer_output"
        output_dir.mkdir()

        try:
            processor = SimpleStreamProcessor(
                config_path,
                topic_name,
                output_dir=output_dir,
                n_threads=2,
                consumer_group_id="create_new",
                logger=logger,
            )

            thread = ExceptionTrackingThread(target=processor.process_files_as_read)
            thread.start()

            timeout = 60
            logger.info(f"  Waiting up to {timeout}s for the consumer to process...")

            start = time.time()
            while time.time() - start < timeout:
                time.sleep(2)
                if hasattr(processor, "n_msgs_read"):
                    logger.info(f"  ... {processor.n_msgs_read} messages read so far")

            processor.control_command_queue.put("q")
            thread.join(timeout=10)

            if thread.caught_exception:
                print(f"\n{'='*70}")
                print("Consumer thread exception:")
                print(f"{'='*70}")
                print(thread.caught_exception)
            else:
                logger.info("")
                logger.info("Consumer did not crash within the timeout.")
                logger.info("This likely means the topic has only 1 partition,")
                logger.info("so Gen1 completed before Gen2 started.")
                logger.info("")
                logger.info(
                    "To reproduce via Kafka, recreate the topic with >= 3 partitions:"
                )
                logger.info(
                    f"  kafka-topics --delete --topic {topic_name} --bootstrap-server <broker>"
                )
                logger.info(
                    f"  kafka-topics --create --topic {topic_name} --partitions 3 "
                    f"--replication-factor 1 --bootstrap-server <broker>"
                )
                logger.info("")
                logger.info("Or use fix-multigen/reproduce_consumer_crash.py for a")
                logger.info("direct (no-Kafka) reproduction that always works.")

        except KeyboardInterrupt:
            logger.info("Interrupted by user.")
            try:
                processor.control_command_queue.put("q")
                thread.join(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    main()
