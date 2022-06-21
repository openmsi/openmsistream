#imports
import unittest, pathlib, shutil, os
from openmsistream.shared.config import UTIL_CONST
from openmsistream.shared.argument_parsing import MyArgumentParser, existing_file, existing_dir, create_dir
from openmsistream.shared.argument_parsing import config_path, int_power_of_two, positive_int
from openmsistream.data_file_io.config import RUN_OPT_CONST
from config import TEST_CONST

class TestArgumentParsing(unittest.TestCase) :
    """
    Class for testing functions in utilities/argument_parsing.py
    """

    #test MyArgumentParser by just adding a bunch of arguments
    def test_my_argument_parser(self) :
        parser = MyArgumentParser()
        parser.add_arguments('filepath','output_dir','upload_dir','config','topic_name','queue_max_size',
                             'upload_existing','consumer_group_ID','optional_output_dir',
                             n_threads=5,chunk_size=128,update_seconds=60)
        args = [os.fspath(TEST_CONST.TEST_DATA_FILE_PATH), 
                'TEST_OUTPUT',
                os.fspath(TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH),
                '--n_threads','100'
            ]
        args = parser.parse_args(args=args)
        self.assertEqual(args.n_threads,100)
        self.assertTrue((pathlib.Path() / 'TEST_OUTPUT').is_dir())
        shutil.rmtree(pathlib.Path() / 'TEST_OUTPUT')
        with self.assertRaises(ValueError) :
            parser = MyArgumentParser()
            parser.add_arguments('never_name_a_command_line_arg_this')

    #test the existing_file argument parser callback
    def test_existing_file(self) :
        this_file_path = pathlib.Path(__file__).resolve()
        self.assertTrue(this_file_path.is_file())
        self.assertEqual(existing_file(this_file_path),this_file_path)
        this_file_path_str = str(this_file_path)
        self.assertEqual(existing_file(this_file_path_str),this_file_path)
        does_not_exist_file_path = pathlib.Path(__file__).parent 
        does_not_exist_file_path = does_not_exist_file_path / 'never_make_a_directory_called_this' 
        does_not_exist_file_path = does_not_exist_file_path / 'nor_a_file_called_this.fake_file_ext'
        does_not_exist_file_path = does_not_exist_file_path.resolve()
        self.assertFalse(does_not_exist_file_path.is_file())
        with self.assertRaises(FileNotFoundError) :
            _ = existing_file(does_not_exist_file_path)
        does_not_exist_file_path_str = str(does_not_exist_file_path)
        with self.assertRaises(FileNotFoundError) :
            _ = existing_file(does_not_exist_file_path_str)
        with self.assertRaises(TypeError) :
            _ = existing_file(None)

    #test the existing_dir argument parser callback
    def test_existing_dir(self) :
        this_file_dir_path = pathlib.Path(__file__).parent.resolve()
        self.assertTrue(this_file_dir_path.is_dir())
        self.assertEqual(existing_dir(this_file_dir_path),this_file_dir_path)
        this_file_dir_path_str = str(this_file_dir_path)
        self.assertEqual(existing_dir(this_file_dir_path_str),this_file_dir_path)
        does_not_exist_dir_path = (pathlib.Path(__file__).parent / 'never_make_a_directory_called_this').resolve()
        self.assertFalse(does_not_exist_dir_path.is_dir())
        with self.assertRaises(FileNotFoundError) :
            _ = existing_dir(does_not_exist_dir_path)
        does_not_exist_dir_path_str = str(does_not_exist_dir_path)
        with self.assertRaises(FileNotFoundError) :
            _ = existing_dir(does_not_exist_dir_path_str)
        with self.assertRaises(TypeError) :
            _ = existing_dir(None)

    #test the create_dir argument parser callback
    def test_create_dir(self) :
        this_file_dir_path = pathlib.Path(__file__).parent.resolve()
        self.assertTrue(this_file_dir_path.is_dir())
        self.assertEqual(create_dir(this_file_dir_path),this_file_dir_path)
        this_file_dir_path_str = str(this_file_dir_path)
        self.assertEqual(create_dir(this_file_dir_path_str),this_file_dir_path)
        self.assertTrue(this_file_dir_path.is_dir())
        create_dir_path = (pathlib.Path(__file__).parent / 'test_create_directory').resolve()
        self.assertFalse(create_dir_path.is_dir())
        try :
            self.assertEqual(create_dir(create_dir_path),create_dir_path)
            self.assertTrue(create_dir_path.is_dir())
            create_dir_path.rmdir()
            self.assertFalse(create_dir_path.is_dir())
            create_dir_path_str = str(create_dir_path)
            self.assertEqual(create_dir(create_dir_path_str),create_dir_path)
            self.assertTrue(create_dir_path.is_dir())
        except Exception as e :
            raise e
        finally :
            if create_dir_path.is_dir() :
                create_dir_path.rmdir()
        with self.assertRaises(TypeError) :
            _ = existing_file(None)

    #test the config_path argument parser callback
    def test_config_path(self) :
        default_config_file_path = UTIL_CONST.CONFIG_FILE_DIR 
        default_config_file_path = default_config_file_path / f'{RUN_OPT_CONST.DEFAULT_CONFIG_FILE}{UTIL_CONST.CONFIG_FILE_EXT}'
        default_config_file_path = default_config_file_path.resolve()
        self.assertEqual(config_path(RUN_OPT_CONST.DEFAULT_CONFIG_FILE),default_config_file_path)
        self.assertEqual(config_path(str(default_config_file_path)),default_config_file_path)
        prod_config_file_path = (UTIL_CONST.CONFIG_FILE_DIR / f'prod{UTIL_CONST.CONFIG_FILE_EXT}').resolve()
        self.assertEqual(config_path('prod'),prod_config_file_path)
        self.assertEqual(config_path(str(prod_config_file_path)),prod_config_file_path)
        does_not_exist_config_file_name = 'never_make_a_file_called_this.fake_file_ext'
        self.assertFalse((pathlib.Path() / does_not_exist_config_file_name).is_file())
        self.assertFalse((UTIL_CONST.CONFIG_FILE_DIR / does_not_exist_config_file_name).is_file())
        with self.assertRaises(ValueError) :
            _ = config_path(does_not_exist_config_file_name)
        with self.assertRaises(TypeError) :
            _ = config_path(None)

    #test the int_power_of_two argument parser callback
    def test_int_power_of_two(self) :
        self.assertEqual(int_power_of_two(RUN_OPT_CONST.DEFAULT_CHUNK_SIZE),RUN_OPT_CONST.DEFAULT_CHUNK_SIZE)
        self.assertEqual(int_power_of_two(4),4)
        self.assertEqual(int_power_of_two('8'),8)
        self.assertEqual(int_power_of_two(16.0),16)
        with self.assertRaises(ValueError) :
            _ = int_power_of_two('hello : )')
        with self.assertRaises(ValueError) :
            _ = int_power_of_two('-2')
        with self.assertRaises(ValueError) :
            _ = int_power_of_two(-4)
        with self.assertRaises(ValueError) :
            _ = int_power_of_two(None)

    #test the positive_int argument parser callback
    def test_positive_int(self) :
        self.assertEqual(positive_int(3),3)
        self.assertEqual(positive_int('5'),5)
        self.assertEqual(positive_int(22.0),22)
        with self.assertRaises(ValueError) :
            _ = positive_int('hello : )')
        with self.assertRaises(ValueError) :
            _ = positive_int('-3')
        with self.assertRaises(ValueError) :
            _ = positive_int(-5)
        with self.assertRaises(ValueError) :
            _ = positive_int(None)
