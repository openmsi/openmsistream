"""OpenMSIStream library for simplified data streaming using Apache Kafka"""

#imports
import os
from .data_file_io.entity.upload_data_file import UploadDataFile
from .data_file_io.actor.data_file_upload_directory import DataFileUploadDirectory
from .data_file_io.actor.data_file_download_directory import DataFileDownloadDirectory
from .data_file_io.actor.data_file_stream_processor import DataFileStreamProcessor
from .data_file_io.actor.data_file_stream_reproducer import DataFileStreamReproducer
from .s3_buckets.s3_transfer_stream_processor import S3TransferStreamProcessor

__all__ = [
    'UploadDataFile',
    'DataFileUploadDirectory',
    'DataFileDownloadDirectory',
    'DataFileStreamProcessor',
    'DataFileStreamReproducer',
    'S3TransferStreamProcessor',
]

#If running on Windows, try to pre-load the librdkafka.dll file
if os.name=='nt' :
    try :
        import confluent_kafka
    except Exception :
        import traceback
        reason = None
        import sys, pathlib
        dll_dir = pathlib.Path(sys.executable).parent/'Lib'/'site-packages'/'confluent_kafka.libs'
        if not dll_dir.is_dir() :
            reason = f'expected DLL directory {dll_dir} does not exist!'
        from ctypes import CDLL
        fps = []
        for fp in dll_dir.glob('*.dll') :
            if fp.name.startswith('librdkafka') :
                fps.append(fp)
                CDLL(str(fp))
        if len(fps)<1 :
            reason = f'{dll_dir} does not contain any librdkafka DLL files to preload!'
        if reason is not None :
            print(f'WARNING: Failed to preload librdkafka DLLs. Reason: {reason}')
        try :
            import confluent_kafka
        except Exception as e :
            errmsg = 'ERROR: Preloading librdkafka DLLs ('
            for fp in fps :
                errmsg+=f'{fp}, '
            errmsg = f'{errmsg[:-2]}) did not allow confluent_kafka to be imported! Exception (will be re-raised): '
            errmsg+= f'{traceback.format_exc()}'
            print(errmsg)
            raise e
    _ = confluent_kafka.Producer #appease pyflakes
