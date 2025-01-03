# imports
import pathlib, os
from openmsistream.utilities.config import RUN_CONST
from openmsistream.utilities.argument_parsing import (
    OpenMSIStreamArgumentParser,
    config_path,
)

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import TestWithOpenMSIStreamOutputLocation
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import TestWithOpenMSIStreamOutputLocation


class TestArgumentParsing(TestWithOpenMSIStreamOutputLocation):
    """
    Class for testing functions in utilities/argument_parsing.py
    """

    def test_my_argument_parser(self):
        """
        Test OpenMSIStreamArgumentParser by just adding a bunch of arguments
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
            os.fspath(self.output_dir / "TEST_OUTPUT"),
            os.fspath(TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH),
            "--n_threads",
            "100",
        ]
        args = parser.parse_args(args=args)
        self.assertEqual(args.n_threads, 100)
        self.assertTrue((self.output_dir / "TEST_OUTPUT").is_dir())
        with self.assertRaises(ValueError):
            parser = OpenMSIStreamArgumentParser()
            parser.add_arguments("never_name_a_command_line_arg_this")
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_config_path(self):
        """
        Test the config_path argument parser callback
        """
        cfg_file_name = f"{RUN_CONST.DEFAULT_CONFIG_FILE}{RUN_CONST.CONFIG_FILE_EXT}"
        default_config_file_path = (RUN_CONST.CONFIG_FILE_DIR / cfg_file_name).resolve()
        self.assertEqual(
            config_path(RUN_CONST.DEFAULT_CONFIG_FILE), default_config_file_path
        )
        self.assertEqual(
            config_path(str(default_config_file_path)), default_config_file_path
        )
        prod_config_file_path = (
            RUN_CONST.CONFIG_FILE_DIR / f"prod{RUN_CONST.CONFIG_FILE_EXT}"
        ).resolve()
        self.assertEqual(config_path("prod"), prod_config_file_path)
        self.assertEqual(config_path(str(prod_config_file_path)), prod_config_file_path)
        does_not_exist_config_file_name = "never_make_a_file_called_this.fake_file_ext"
        self.assertFalse((pathlib.Path() / does_not_exist_config_file_name).is_file())
        self.assertFalse(
            (RUN_CONST.CONFIG_FILE_DIR / does_not_exist_config_file_name).is_file()
        )
        with self.assertRaises(ValueError):
            _ = config_path(does_not_exist_config_file_name)
        with self.assertRaises(TypeError):
            _ = config_path(None)
        self.success = True  # pylint: disable=attribute-defined-outside-init
