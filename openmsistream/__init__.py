"""OpenMSIStream library for simplified data streaming using Apache Kafka"""

# Before anything else, add hooks to capture all logs (does not interfere
# with later added handlers, etc).
import warnings, logging
from .utilities.log_handler import LoggingHandler

warnings.showwarning = LoggingHandler.showwarning
logging.getLogger().addHandler(LoggingHandler)

# imports
# we diable pylint warnings here because we need to have enabled logging
# before any of these imports
# pylint: disable=C0413
import os  # pylint: disable=C0411
from .data_file_io.entity.upload_data_file import UploadDataFile
from .data_file_io.actor.data_file_upload_directory import DataFileUploadDirectory
from .data_file_io.actor.data_file_download_directory import DataFileDownloadDirectory
from .data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from .data_file_io.actor.data_file_stream_reproducer import DataFileStreamReproducer
from .metadata_extraction.metadata_json_reproducer import MetadataJSONReproducer
from .s3_buckets.s3_transfer_stream_processor import S3TransferStreamProcessor
from .girder.girder_upload_stream_processor import GirderUploadStreamProcessor
from .version import __version__

__all__ = [
    "__version__",
    "UploadDataFile",
    "DataFileUploadDirectory",
    "DataFileDownloadDirectory",
    "DataFileStreamProcessor",
    "DataFileStreamReproducer",
    "GirderUploadStreamProcessor",
    "MetadataJSONReproducer",
    "S3TransferStreamProcessor",
]

# If running on Windows, try to pre-load the librdkafka.dll file
if os.name == "nt":
    try:
        import confluent_kafka
    except Exception:
        import traceback

        reason = None
        import sys, pathlib

        dll_dir = (
            pathlib.Path(sys.executable).parent
            / "Lib"
            / "site-packages"
            / "confluent_kafka.libs"
        )
        if not dll_dir.is_dir():
            reason = f"expected DLL directory {dll_dir} does not exist!"
        from ctypes import CDLL

        fps = []
        for fp in dll_dir.glob("*.dll"):
            if fp.name.startswith("librdkafka"):
                fps.append(fp)
                CDLL(str(fp))
        if len(fps) < 1:
            reason = f"{dll_dir} does not contain any librdkafka DLL files to preload!"
        if reason is not None:
            print(f"WARNING: Failed to preload librdkafka DLLs. Reason: {reason}")
        try:
            import confluent_kafka
        except Exception as exc:
            errmsg = "ERROR: Preloading librdkafka DLLs ("
            errmsg += ", ".join([str(_) for _ in fps])
            errmsg += (
                f"{errmsg}) did not allow confluent_kafka to be imported! "
                f"Exception (will be re-raised): {traceback.format_exc()}"
            )
            print(errmsg)
            raise exc
    _ = confluent_kafka.Producer  # appease pyflakes
