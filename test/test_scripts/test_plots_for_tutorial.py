# test_plots_for_tutorial.py
import pathlib
import urllib.request
import importlib.machinery
import importlib.util
import pytest

from .config import TEST_CONST

# ---------------------------------------------------------------------
# Load the XRDCSVPlotter dynamically from examples directory
# ---------------------------------------------------------------------
class_path = TEST_CONST.EXAMPLES_DIR_PATH / "creating_plots" / "xrd_csv_plotter.py"
module_name = class_path.name[:-3]  # strip ".py"

loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()  # noqa: W1510  (deprecated-method, ignore)

# constants
TIMEOUT_SECS = 90
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / "creating_plots" / "SC001_XRR.csv"
CONSUMER_GROUP_ID = f"test_plots_for_tutorial_{TEST_CONST.PY_VERSION}"

TOPIC_NAME = "test_plots_for_tutorial"
TOPICS = {TOPIC_NAME: {}}

# ---------------------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------------------


@pytest.fixture
def downloaded_file():
    """Downloads the CSV test file before test and deletes it afterward."""
    urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL, UPLOAD_FILE)
    yield UPLOAD_FILE
    if UPLOAD_FILE.exists():
        UPLOAD_FILE.unlink()


# ---------------------------------------------------------------------
# TEST
# ---------------------------------------------------------------------

TOPIC_NAME = "test_plots_for_tutorial"

TOPICS = {TOPIC_NAME: {}}


@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("kafka_topics", "stream_processor_helper")
def test_plots_for_tutorial_kafka(
    upload_file_helper,
    stream_processor_helper,
    downloaded_file,
):
    """
    Pytest version of: Test that uploading a file → Kafka → stream processor → plot.
    """

    # ------------------------------------------------------------
    # Start the stream processor using pytest helper
    # ------------------------------------------------------------
    stream_processor_helper["create_stream_processor"](
        module.XRDCSVPlotter,
        topic_name=TOPIC_NAME,
        consumer_group_id=CONSUMER_GROUP_ID,
    )

    stream_processor_helper["start_stream_processor_thread"]()

    # ------------------------------------------------------------
    # Upload file to Kafka using pytest helper
    # ------------------------------------------------------------
    upload_file_helper(downloaded_file, topic_name=TOPIC_NAME)

    # Kafka reconstructs file under the processor
    rel_path = pathlib.Path(downloaded_file.name)
    stream_processor_helper["wait_for_files_to_be_processed"](
        rel_path, timeout_secs=TIMEOUT_SECS
    )

    # ------------------------------------------------------------
    # Assert on registry tables
    # ------------------------------------------------------------
    sp = stream_processor_helper["state"]["stream_processor"]

    sp.file_registry.in_progress_table.dump_to_file()
    sp.file_registry.succeeded_table.dump_to_file()

    # nothing should be pending re-run
    assert len(sp.file_registry.filepaths_to_rerun) == 0

    in_prog_table = sp.file_registry.in_progress_table
    succeeded_table = sp.file_registry.succeeded_table

    succeeded_entries = succeeded_table.obj_addresses
    assert len(succeeded_entries) >= 1, "Expected at least one succeeded entry"

    # Ensure we have no failures in in-progress table
    in_prog_by_status = in_prog_table.obj_addresses_by_key_attr("status")
    assert sp.file_registry.FAILED not in in_prog_by_status

    # Verify succeeded entry matches uploaded file
    entry_attrs = succeeded_table.get_entry_attrs(succeeded_entries[0])
    assert entry_attrs["filename"] == downloaded_file.name

    # ------------------------------------------------------------
    # Check that the plot file exists
    # ------------------------------------------------------------
    out_plot = stream_processor_helper["state"]["output_dir"] / (
        downloaded_file.stem + "_xrd_plot.png"
    )
    assert out_plot.is_file(), f"Missing output plot: {out_plot}"
