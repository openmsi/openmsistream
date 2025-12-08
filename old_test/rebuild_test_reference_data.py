# imports
import pathlib, logging, shutil, filecmp, urllib, importlib, pickle
from openmsitoolbox.logging import OpenMSILogger
from openmsistream.data_file_io.entity.upload_data_file import UploadDataFile
from openmsistream.data_file_io.entity.download_data_file import DownloadDataFileToMemory
from openmsistream.kafka_wrapper.serialization import DataFileChunkSerializer
from openmsistream.services.windows_service_manager import WindowsServiceManager
from openmsistream.services.config import SERVICE_CONST

# pylint: disable=wrong-import-order,import-error
from test_scripts.config import TEST_CONST

# constants
EXISTING_TEST_DATA_DIR = (pathlib.Path(__file__).parent / "data").resolve()
NEW_TEST_DATA_DIR = (pathlib.Path(__file__).parent / "new_test_data").resolve()
LOGGER = OpenMSILogger(pathlib.Path(__file__).name.split(".")[0], logging.INFO)

#################### OTHER HELPER FUNCTIONS ####################


def prompt_to_remove(rel_filepath, prompt):
    """
    Prompt a user about two different versions of a file and potentially
    remove the file if they're not alright with it

    rel_filepath = the path to the file relevant to the new/existing test data root directory
    prompt = the prompt to give to the user to ask them whether some difference is OK
    """
    check = input(prompt)
    if check.lower() in ("n", "no"):
        LOGGER.debug(f"\tremoving file {rel_filepath}")
        (NEW_TEST_DATA_DIR / rel_filepath).unlink()


def compare_and_check_old_and_new_files(filename, subdir_path=""):
    """
    Compare a newly created file with its potentially already existing counterpart
    and double check that adding or replacing it is alright with the user

    filename = the name of the file
    subdir_path = the path to the subdirectory containing the file within the
                  new/existing test data root directory
    """
    rel_filepath = pathlib.Path(subdir_path) / filename
    # if it's a new file
    if not (EXISTING_TEST_DATA_DIR / rel_filepath).is_file():
        prompt_to_remove(
            rel_filepath,
            f"File {rel_filepath} would be new test data. Is that alright? [(y)/n]: ",
        )
        return
    # if it's a different size than the older file
    old_size = (EXISTING_TEST_DATA_DIR / rel_filepath).stat().st_size
    new_size = (NEW_TEST_DATA_DIR / rel_filepath).stat().st_size
    if old_size != new_size:
        msg = (
            f"File {rel_filepath} has {new_size} bytes but the existing file has "
            f"{old_size} bytes. Is that alright? [(y)/n]: "
        )
        prompt_to_remove(rel_filepath, msg)
        return
    # if it's different than what exists
    if not filecmp.cmp(
        EXISTING_TEST_DATA_DIR / rel_filepath,
        NEW_TEST_DATA_DIR / rel_filepath,
        shallow=False,
    ):
        msg = (
            f"File {rel_filepath} has different content than the existing file. "
            "Is that alright? [(y)/n]: "
        )
        prompt_to_remove(rel_filepath, msg)
        return


def relocate_files(dirpath):
    """
    Move any files in the given directory into the existing test data directory
    Any directories found result in calling this function again recursively
    """
    for fp in dirpath.rglob("*"):
        if fp.is_dir():
            relocate_files(fp)
        elif fp.is_file():
            newpath = EXISTING_TEST_DATA_DIR / (fp.relative_to(NEW_TEST_DATA_DIR))
            if not newpath.parent.is_dir():
                newpath.parent.mkdir()
            fp.rename(EXISTING_TEST_DATA_DIR / (fp.relative_to(NEW_TEST_DATA_DIR)))


#################### INDIVIDUAL DATA CREATION FUNCTIONS ####################


def rebuild_binary_file_chunks_for_serialization_reference():
    """
    Rebuild the binary file chunks to reference for serialization/deserialization tests
    """
    # path to the test data file
    test_data_fp = (
        EXISTING_TEST_DATA_DIR
        / TEST_CONST.TEST_DATA_FILE_ROOT_DIR_NAME
        / TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
    )
    test_data_fp = test_data_fp / TEST_CONST.TEST_DATA_FILE_NAME
    # make the data file and build its list of chunks
    udf = UploadDataFile(
        test_data_fp,
        rootdir=EXISTING_TEST_DATA_DIR / TEST_CONST.TEST_DATA_FILE_ROOT_DIR_NAME,
        logger=LOGGER,
    )
    # pylint: disable=protected-access
    udf._build_list_of_file_chunks(TEST_CONST.TEST_CHUNK_SIZE)
    udf.add_chunks_to_upload()
    # populate and serialize a few chunks and save them as binary data
    dfcs = DataFileChunkSerializer()
    for i in range(3):
        udf.chunks_to_upload[i].populate_with_file_data(LOGGER)
        binary_data = dfcs(udf.chunks_to_upload[i])
        fn = f"{TEST_CONST.TEST_DATA_FILE_PATH.stem}_test_chunk_{i}.bin"
        with open(NEW_TEST_DATA_DIR / fn, "wb") as fp:
            fp.write(binary_data)
        compare_and_check_old_and_new_files(fn)


def rebuild_test_services_executable():
    """
    Rebuild the executable file used to double-check Services behavior
    """
    # some constants
    test_service_class_name = "DataFileUploadDirectory"
    test_service_name = "testing_service"
    test_service_executable_argslist = ["test_upload"]
    # create the file using the function supplied
    manager = WindowsServiceManager(
        test_service_name,
        service_spec_string=test_service_class_name,
        argslist=test_service_executable_argslist,
    )
    manager._write_executable_file()  # pylint: disable=protected-access
    # move it to the new test data folder
    exec_fp = (
        pathlib.Path(__file__).parent.parent
        / "openmsistream"
        / "services"
        / "working_dir"
    )
    exec_fp = exec_fp / f"{test_service_name}{SERVICE_CONST.SERVICE_EXECUTABLE_NAME_STEM}"
    exec_fp.replace(NEW_TEST_DATA_DIR / exec_fp.name)
    compare_and_check_old_and_new_files(exec_fp.name)


def rebuild_test_metadata_dict():
    """
    Rebuild the pickle file used to test the metadata extraction/production
    """
    # download the test file
    urllib.request.urlretrieve(
        TEST_CONST.TUTORIAL_TEST_FILE_URL, "metadata_test_file.csv"
    )
    # create a DownloadDataFile from the test file and set its bytestring
    datafile = DownloadDataFileToMemory(pathlib.Path("metadata_test_file.csv"))
    with open("metadata_test_file.csv", "rb") as fp:
        datafile.bytestring = fp.read()
    # import the XRDCSVMetadataReproducer from the examples directory
    class_path = (
        TEST_CONST.EXAMPLES_DIR_PATH
        / "extracting_metadata"
        / "xrd_csv_metadata_reproducer.py"
    )
    module_name = class_path.name[: -len(".py")]
    loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
    # pylint: disable=deprecated-method,no-value-for-parameter
    module = loader.load_module()
    # get the metadata dictionary from the file
    metadata_reproducer = module.XRDCSVMetadataReproducer(
        TEST_CONST.EXAMPLES_DIR_PATH
        / "extracting_metadata"
        / "test_xrd_csv_metadata_reproducer.config",
        "test_metadata_extractor_source",
        "test_metadata_extractor_dest",
        output_dir=pathlib.Path("./REMOVE_ME"),
    )
    # pylint: disable=protected-access
    metadata_dict = metadata_reproducer._get_metadata_dict_for_file(datafile)
    metadata_dict.pop("metadata_message_generated_at")  # get rid of the timestamp
    # pickle up the file
    with open(
        NEW_TEST_DATA_DIR / TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE.name, "wb"
    ) as fp:
        pickle.dump(metadata_dict, fp, protocol=0)
    # close and delete stuff
    metadata_reproducer.close()
    if pathlib.Path("./REMOVE_ME").is_dir():
        shutil.rmtree(pathlib.Path("./REMOVE_ME"))
    if pathlib.Path("metadata_test_file.csv").is_file():
        pathlib.Path("metadata_test_file.csv").unlink()
    compare_and_check_old_and_new_files(TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE.name)


#################### MAIN SCRIPT ####################


def main():
    """
    Main method to run the whole script
    """
    # make the directory to hold the new test data
    NEW_TEST_DATA_DIR.mkdir()
    # try populating it with all of the necessary new data, checking with the user along the way
    try:
        LOGGER.info("Rebuilding reference binary file chunks....")
        rebuild_binary_file_chunks_for_serialization_reference()
        LOGGER.info("Rebuilding reference Service executable file....")
        rebuild_test_services_executable()
        LOGGER.info("Rebuilding reference metadata message pickle file....")
        rebuild_test_metadata_dict()
        LOGGER.info(f"Moving new files into {EXISTING_TEST_DATA_DIR}...")
        relocate_files(NEW_TEST_DATA_DIR)
    except Exception as exc:
        raise exc
    finally:
        shutil.rmtree(NEW_TEST_DATA_DIR)


if __name__ == "__main__":
    main()
