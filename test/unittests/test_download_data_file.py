#imports
import unittest, pathlib, logging, filecmp, shutil
from hashlib import sha512
from openmsistream.shared.logging import Logger
from openmsistream.data_file_io.config import RUN_OPT_CONST, DATA_FILE_HANDLING_CONST
from openmsistream.data_file_io.data_file_chunk import DataFileChunk
from openmsistream.data_file_io.upload_data_file import UploadDataFile
from openmsistream.data_file_io.download_data_file import DownloadDataFileToDisk, DownloadDataFileToMemory
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

class TestDownloadDataFile(unittest.TestCase) :
    """
    Class for testing DownloadDataFile functions (without interacting with the Kafka broker)
    """

    def setUp(self) :
        self.ul_datafile = UploadDataFile(TEST_CONST.TEST_DATA_FILE_PATH,
                                          rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,logger=LOGGER)
        self.ul_datafile._build_list_of_file_chunks(RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        self.ul_datafile.add_chunks_to_upload()

    def run_download_chunks(self,disk_or_memory) :
        """
        Helper function run by both tests below
        disk_or_memory variable determines which objects are used in the test
        """
        dl_datafile = None
        if disk_or_memory=='disk' :
            TEST_CONST.TEST_RECO_DIR_PATH.mkdir()
        try :
            #add all of the chunks from an upload file, checking that the return codes are correct
            for ic,dfc in enumerate(self.ul_datafile.chunks_to_upload) :
                dfc.populate_with_file_data(logger=LOGGER)
                subdir_as_path = pathlib.Path('').joinpath(*(pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).parts))
                dfc_as_dl = DataFileChunk(subdir_as_path/dfc.filename,dfc.filename,
                                          dfc.file_hash,dfc.chunk_hash,
                                          None,dfc.chunk_offset_write,
                                          dfc.chunk_size,
                                          dfc.chunk_i,dfc.n_total_chunks,data=dfc.data)
                dfc_as_dl.rootdir = TEST_CONST.TEST_RECO_DIR_PATH
                if dl_datafile is None :
                    if disk_or_memory=='disk' :
                        dl_datafile = DownloadDataFileToDisk(dfc_as_dl.filepath,logger=LOGGER)
                    elif disk_or_memory=='memory' :
                        dl_datafile = DownloadDataFileToMemory(dfc_as_dl.filepath,logger=LOGGER)
                check = dl_datafile.add_chunk(dfc_as_dl)
                #try writing every tenth chunk twice; should return "chunk already added"
                if ic%10==0 :
                    check2 = dl_datafile.add_chunk(dfc_as_dl)
                    self.assertEqual(check2,DATA_FILE_HANDLING_CONST.CHUNK_ALREADY_WRITTEN_CODE)
                expected_check_value = DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
                if ic==len(self.ul_datafile.chunks_to_upload)-1 :
                    expected_check_value = DATA_FILE_HANDLING_CONST.FILE_SUCCESSFULLY_RECONSTRUCTED_CODE 
                self.assertEqual(check,expected_check_value)
            #make sure that the reconstructed contents match the original contents
            if disk_or_memory=='disk' :
                fp = TEST_CONST.TEST_RECO_DIR_PATH/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME/dl_datafile.filename
                if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH,fp,shallow=False) :
                    raise RuntimeError('ERROR: files are not the same after reconstruction!')
                (TEST_CONST.TEST_RECO_DIR_PATH/TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME/dl_datafile.filename).unlink()
            elif disk_or_memory=='memory' :
                with open(TEST_CONST.TEST_DATA_FILE_PATH,'rb') as fp :
                    ref_data = fp.read()
                if not dl_datafile.bytestring==ref_data :
                    raise RuntimeError('ERROR: files are not the same after reconstruction!')
            #make sure the hashes are mismatched if some chunks are missing
            dl_datafile._chunk_offsets_downloaded=[]
            hash_missing_some_chunks = sha512()
            for ic,dfc in enumerate(self.ul_datafile.chunks_to_upload) :
                if ic%3==0 :
                    hash_missing_some_chunks.update(dfc.data)
            hash_missing_some_chunks.digest()
            for ic,dfc in enumerate(self.ul_datafile.chunks_to_upload) :
                subdir_as_path = pathlib.Path('').joinpath(*(pathlib.PurePosixPath(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME).parts))
                dfc_as_dl = DataFileChunk(subdir_as_path/dfc.filename,dfc.filename,
                                          dfc.file_hash,dfc.chunk_hash,
                                          None,dfc.chunk_offset_write,
                                          dfc.chunk_size,
                                          dfc.chunk_i,dfc.n_total_chunks,data=dfc.data)
                dfc_as_dl.rootdir = TEST_CONST.TEST_RECO_DIR_PATH
                if ic==len(self.ul_datafile.chunks_to_upload)-1 :
                    dfc_as_dl.file_hash=hash_missing_some_chunks
                check = dl_datafile.add_chunk(dfc_as_dl)
                expected_check_value = DATA_FILE_HANDLING_CONST.FILE_IN_PROGRESS
                if ic==len(self.ul_datafile.chunks_to_upload)-1 :
                    expected_check_value = DATA_FILE_HANDLING_CONST.FILE_HASH_MISMATCH_CODE 
                self.assertEqual(check,expected_check_value)
        except Exception as e :
            raise e
        finally :
            if disk_or_memory=='disk' :
                shutil.rmtree(TEST_CONST.TEST_RECO_DIR_PATH)

    def test_download_chunks_to_disk(self) :
        self.run_download_chunks('disk')

    def test_download_chunks_to_memory(self) :
        self.run_download_chunks('memory')
