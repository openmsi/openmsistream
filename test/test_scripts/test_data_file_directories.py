# imports
import pathlib, shutil, filecmp, re
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)
from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

# pylint: disable=import-error,wrong-import-order
from test_base_classes import (
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
    TestWithEnvVars,
)

# constants
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]]


class TestDataFileDirectories(
    TestWithDataFileUploadDirectory, TestWithDataFileDownloadDirectory, TestWithEnvVars
):
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    def run_data_file_upload_directory(self, upload_file_dict, **create_kwargs):
        """
        Called by the test method below to run an upload directory from start to finish
        """
        # create the upload directory with the default config file
        self.create_upload_directory(**create_kwargs)
        # start it running in a new thread
        self.start_upload_thread(topic_name=TOPIC_NAME)
        try:
            # put the test file(s) in the watched directory
            for filepath, filedict in upload_file_dict.items():
                rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                self.copy_file_to_watched_dir(filepath, filepath.relative_to(rootdir))
            # put the "check" command into the input queue a couple times to test it
            self.upload_directory.control_command_queue.put("c")
            self.upload_directory.control_command_queue.put("check")
            # shut down the upload thread
            self.stop_upload_thread()
            # make sure that the ProducerFileRegistry files were created
            # and they list the file(s) as completely uploaded
            log_subdir = self.watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME
            in_prog_filepath = log_subdir / f"upload_to_{TOPIC_NAME}_in_progress.csv"
            completed_filepath = log_subdir / f"uploaded_to_{TOPIC_NAME}.csv"
            self.assertTrue(in_prog_filepath.is_file())
            in_prog_table = DataclassTableReadOnly(
                RegistryLineInProgress,
                filepath=in_prog_filepath,
                logger=self.logger,
            )
            self.assertEqual(in_prog_table.obj_addresses_by_key_attr("filename"), {})
            self.assertTrue(completed_filepath.is_file())
            completed_table = DataclassTableReadOnly(
                RegistryLineCompleted,
                filepath=completed_filepath,
                logger=self.logger,
            )
            addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")
            for filepath, filedict in upload_file_dict.items():
                if filedict["upload_expected"]:
                    rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                    if rootdir:
                        rel_path = filepath.relative_to(rootdir)
                    else:
                        rel_path = pathlib.Path(filepath.name)
                    self.assertTrue(rel_path in addrs_by_fp)
        except Exception as exc:
            raise exc

    def run_data_file_download_directory(self, download_file_dict, **other_create_kwargs):
        """
        Called by the test method below to run a download directory from start to finish
        """
        # make a list of relative filepaths we'll be waiting for
        relevant_files = {}
        for filepath, filedict in download_file_dict.items():
            if filedict["download_expected"]:
                rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                if rootdir:
                    rel_path = filepath.relative_to(rootdir)
                else:
                    rel_path = pathlib.Path(filepath.name)
                relevant_files[filepath] = rel_path
        # create the download directory
        self.create_download_directory(topic_name=TOPIC_NAME,**other_create_kwargs)
        # start reconstruct in a separate thread so we can time it out
        self.start_download_thread()
        try:
            # put the "check" command into the input queue a couple times
            self.download_directory.control_command_queue.put("c")
            self.download_directory.control_command_queue.put("check")
            # wait for the timeout for the test file(s) to be completely reconstructed
            self.wait_for_files_to_reconstruct(relevant_files.values())
            # make sure the reconstructed file(s) exists with the same content as the original
            for orig_fp, rel_fp in relevant_files.items():
                reco_fp = self.reco_dir / rel_fp
                self.assertTrue(reco_fp.is_file())
                if not filecmp.cmp(orig_fp, reco_fp, shallow=False):
                    errmsg = (
                        "ERROR: files are not the same after reconstruction! "
                        "(This may also be due to the timeout being too short)"
                    )
                    raise RuntimeError(errmsg)
        except Exception as exc:
            raise exc

    def test_upload_and_download_directories_kafka(self):
        """
        Test the upload and download directories while applying regular expressions
        """
        files_roots = {
            TEST_CONST.TEST_DATA_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
                "upload_expected": True,
                "download_expected": True,
            },
            TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
                "upload_expected": True,
                "download_expected": False,
            },
            TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE: {
                "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
                "upload_expected": False,
                "download_expected": False,
            },
        }
        upload_regex = re.compile(r'^.*\.(dat|config)$')
        download_regex = re.compile(r'^.*\.dat$')
        self.run_data_file_upload_directory(files_roots,upload_regex=upload_regex)
        self.run_data_file_download_directory(
            files_roots,
            consumer_group_id="run_data_file_download_directory_with_regexes",
            filepath_regex=download_regex,
        )
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_filepath_should_be_uploaded(self):
        """
        Test the function that says whether or not a file should be uploaded
        """
        # create an upload directory
        self.create_upload_directory(TEST_CONST.TEST_DATA_DIR_PATH)
        # test the "filepath_should_be_uploaded" function
        self.log_at_info("\nExpecting three errors below:")
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(None)
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(5)
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(
                "this is a string not a path!"
            )
        self.assertFalse(
            self.upload_directory.filepath_should_be_uploaded(
                TEST_CONST.TEST_DATA_DIR_PATH / ".this_file_is_hidden"
            )
        )
        self.assertFalse(
            self.upload_directory.filepath_should_be_uploaded(
                TEST_CONST.TEST_DATA_DIR_PATH / "this_file_is_a_log_file.log"
            )
        )
        for fp in TEST_CONST.TEST_DATA_DIR_PATH.rglob("*"):
            check = True
            if fp.is_dir():
                check = False
            elif fp.name.startswith(".") or fp.name.endswith(".log"):
                check = False
            self.assertEqual(
                self.upload_directory.filepath_should_be_uploaded(fp.resolve()), check
            )
        subdir_path = (
            TEST_CONST.TEST_DATA_DIR_PATH / "this_subdirectory_should_not_be_uploaded"
        )
        subdir_path.mkdir()
        try:
            self.assertFalse(
                self.upload_directory.filepath_should_be_uploaded(subdir_path)
            )
        except Exception as exc:
            raise exc
        finally:
            shutil.rmtree(subdir_path)
        self.success = True  # pylint: disable=attribute-defined-outside-init
