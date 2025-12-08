# test_argument_parsing.py
import os
import pathlib
import pytest
from openmsistream.utilities.config import RUN_CONST
from openmsistream.utilities.argument_parsing import (
    OpenMSIStreamArgumentParser,
    config_path,
)
from .config import TEST_CONST




def test_my_argument_parser(output_dir):
    """
    Test OpenMSIStreamArgumentParser by just adding a bunch of arguments.
    """
    parser = OpenMSIStreamArgumentParser()
    parser.add_arguments(
        "filepath",
        "output_dir",
        "upload_dir",
        "config",
        "topic_name",
        "queue_max_size",
        "upload_existing",
        "consumer_group_id",
        "optional_output_dir",
        n_threads=5,
        chunk_size=128,
        update_seconds=60,
    )

    args = [
        os.fspath(TEST_CONST.TEST_DATA_FILE_PATH),
        os.fspath(output_dir / "TEST_OUTPUT"),
        os.fspath(TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH),
        "--n_threads",
        "100",
    ]

    args = parser.parse_args(args=args)
    assert args.n_threads == 100
    assert (output_dir / "TEST_OUTPUT").is_dir()

    # Invalid argument name raises ValueError
    with pytest.raises(ValueError):
        parser = OpenMSIStreamArgumentParser()
        parser.add_arguments("never_name_a_command_line_arg_this")


def test_config_path():
    """
    Test the config_path argument parser callback.
    """
    cfg_file_name = f"{RUN_CONST.DEFAULT_CONFIG_FILE}{RUN_CONST.CONFIG_FILE_EXT}"
    default_config_file_path = (RUN_CONST.CONFIG_FILE_DIR / cfg_file_name).resolve()

    assert config_path(RUN_CONST.DEFAULT_CONFIG_FILE) == default_config_file_path
    assert config_path(str(default_config_file_path)) == default_config_file_path

    prod_config_file_path = (
        RUN_CONST.CONFIG_FILE_DIR / f"prod{RUN_CONST.CONFIG_FILE_EXT}"
    ).resolve()
    assert config_path("prod") == prod_config_file_path
    assert config_path(str(prod_config_file_path)) == prod_config_file_path

    does_not_exist_config_file_name = "never_make_a_file_called_this.fake_file_ext"
    assert not (pathlib.Path() / does_not_exist_config_file_name).is_file()
    assert not (RUN_CONST.CONFIG_FILE_DIR / does_not_exist_config_file_name).is_file()

    with pytest.raises(ValueError):
        config_path(does_not_exist_config_file_name)
    with pytest.raises(TypeError):
        config_path(None)
