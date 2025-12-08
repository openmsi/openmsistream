# imports
import pathlib, datetime
from typing import List, Set, Tuple
from dataclasses import dataclass, fields
from openmsistream.utilities.dataclass_table import DataclassTable

try:
    # pylint: disable=import-error,wrong-import-order
    from .base_classes import TestWithOpenMSIStreamOutputLocation
except ImportError:
    # pylint: disable=import-error,wrong-import-order
    from base_classes import TestWithOpenMSIStreamOutputLocation


@dataclass
class TableLineForTesting:
    """
    A dataclass to use as an example of what can be held in DataclassTables
    """

    str_field: str
    int_field: int
    float_field: float
    complex_field: complex
    bool_field: bool
    list_strs_field: List[str]
    set_ints_field: Set[int]
    tuple_floats_field: Tuple[float]
    path_field: pathlib.Path
    timestamp_field: datetime.datetime


class TestDataclassTable(TestWithOpenMSIStreamOutputLocation):
    """
    Class for testing DataclassTable utility
    """

    def test_dataclass_table_functions(self):
        """
        Test the DataclassTable functions
        """
        entry_1 = TableLineForTesting(
            "test",
            1,
            2.3,
            4 + 5j,
            True,
            ["str 1", "str 2", "str 3"],
            set([1, 1, 2, 3]),
            (6.7, 8.9),
            pathlib.Path(__file__),
            datetime.datetime.now(),
        )
        entry_2 = TableLineForTesting(
            "test",
            2,
            11.12,
            13 - 14j,
            False,
            ["str 15", "str 16"],
            set([17, 18, 18, 19]),
            (20.21, 22.23),
            pathlib.Path(__file__).parent,
            datetime.datetime.now(),
        )
        entry_3 = TableLineForTesting(
            "test",
            3,
            24.34,
            6 + 2j,
            True,
            ["hello", "hi", "hey"],
            set([25, 35, 35, 35, 43]),
            (72.0, 84.6),
            pathlib.Path(__file__).parent.parent,
            datetime.datetime.now(),
        )
        entry_1_addr = hex(id(entry_1))
        entry_2_addr = hex(id(entry_2))
        entry_3_addr = hex(id(entry_3))
        # make sure the table doesn't already exist
        self.assertFalse((self.output_dir / "test_table.csv").is_file())
        # create the empty table
        test_table = DataclassTable(
            dataclass_type=TableLineForTesting,
            filepath=self.output_dir / "test_table.csv",
            logger=self.logger,
        )
        # add only the first entry (and remove it and add it again)
        test_table.add_entries(entry_1)
        test_table.remove_entries(entry_1_addr)
        test_table.add_entries(entry_1)
        # get the attributes of the entry
        entry_1_attrs_read = test_table.get_entry_attrs(entry_1_addr)
        for field in fields(entry_1):
            self.assertEqual(getattr(entry_1, field.name), entry_1_attrs_read[field.name])
        self.log_at_info("\nExpecting five errors below:")
        # adding the same entry again should give an error
        with self.assertRaises(ValueError):
            test_table.add_entries(entry_1)
        # removing a nonexistent entry should give an error
        with self.assertRaises(ValueError):
            test_table.remove_entries(entry_2_addr)
        # trying to get attrs for a nonexistent entry should give an error
        with self.assertRaises(ValueError):
            _ = test_table.get_entry_attrs(entry_2_addr)
        # same thing for setting attrs
        with self.assertRaises(ValueError):
            test_table.set_entry_attrs(entry_2_addr, str_field="this should break")
        # should also get an error if trying to get a view keyed by a nonexistent attr
        with self.assertRaises(ValueError):
            _ = test_table.obj_addresses_by_key_attr("not_a_dataclass_attribute_name")
        # dumping the file to write it out to be re-read
        test_table.dump_to_file()
        del test_table
        test_table = DataclassTable(
            dataclass_type=TableLineForTesting,
            filepath=self.output_dir / "test_table.csv",
            logger=self.logger,
        )
        # it should have the same one entry in it
        self.assertEqual(
            1, len(test_table.obj_addresses_by_key_attr("str_field")["test"])
        )
        # after adding the other two entries the len of that list should be 3
        test_table.add_entries([entry_2, entry_3])
        self.assertEqual(
            3, len(test_table.obj_addresses_by_key_attr("str_field")["test"])
        )
        # and there should be three unique integer fields
        self.assertEqual(3, len(test_table.obj_addresses_by_key_attr("int_field").keys()))
        # set one of the entries' str field to be something different
        test_table.set_entry_attrs(entry_2_addr, str_field="testing")
        by_str_field = test_table.obj_addresses_by_key_attr("str_field")
        self.assertTrue("test" in by_str_field)
        self.assertEqual(2, len(by_str_field["test"]))
        self.assertTrue(entry_3_addr in by_str_field["test"])
        self.assertTrue("testing" in by_str_field)
        self.assertEqual(1, len(by_str_field["testing"]))
        self.assertTrue(entry_2_addr in by_str_field["testing"])
        # dump and delete the test table
        test_table.dump_to_file()
        del test_table
        self.success = True  # pylint: disable=attribute-defined-outside-init
