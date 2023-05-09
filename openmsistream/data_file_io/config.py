"""Constants used for internally processing file chunks and for some default parameter values"""


class DataFileHandlingConstants:
    """
    Constants for internally handling DataFileChunks
    """

    CHUNK_ALREADY_WRITTEN_CODE = 10
    FILE_HASH_MISMATCH_CODE = -1
    FILE_SUCCESSFULLY_RECONSTRUCTED_CODE = 3
    FILE_IN_PROGRESS = 2


DATA_FILE_HANDLING_CONST = DataFileHandlingConstants()
