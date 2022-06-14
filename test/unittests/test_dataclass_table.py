#imports
import unittest, pathlib, datetime, logging, shutil
from typing import List, Set, Tuple
from dataclasses import dataclass, fields
from openmsistream.shared.logging import Logger
from openmsistream.shared.dataclass_table import DataclassTable

#some constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

@dataclass
class TableLineForTesting :
    str_field : str
    int_field : int
    float_field : float
    complex_field : complex
    bool_field : bool
    list_strs_field : List[str]
    set_ints_field : Set[int]
    tuple_floats_field : Tuple[float]
    path_field : pathlib.Path
    timestamp_field : datetime.datetime

class TestDataclassTable(unittest.TestCase) :
    """
    Class for testing DataclassTable utility
    """

    def test_dataclass_table_functions(self) :
        e1 = TableLineForTesting('test',1,2.3,4+5j,True,['str 1','str 2','str 3'],set([1,1,2,3]),(6.7,8.9),
                                 pathlib.Path(__file__),datetime.datetime.now())
        e2 = TableLineForTesting('test',2,11.12,13-14j,False,['str 15','str 16'],set([17,18,18,19]),(20.21,22.23),
                                 pathlib.Path(__file__).parent,datetime.datetime.now())
        e3 = TableLineForTesting('test',3,24.34,6+2j,True,['hello','hi','hey'],set([25,35,35,35,43]),(72.0,84.6),
                                 pathlib.Path(__file__).parent.parent,datetime.datetime.now())
        e1_addr = hex(id(e1))
        e2_addr = hex(id(e2))
        e3_addr = hex(id(e3))
        testing_location = pathlib.Path(__file__).parent.parent / 'test_dataclass_table'
        #make sure the directory and table don't already exist
        self.assertFalse(testing_location.is_dir())
        self.assertFalse((testing_location/'test_table.csv').is_file())
        #make the directory to work in
        if not testing_location.is_dir() :
            testing_location.mkdir()
        #create the empty table
        test_table = DataclassTable(dataclass_type=TableLineForTesting,
                                    filepath=testing_location/'test_table.csv',logger=LOGGER)
        #add only the first entry (and remove it and add it again)
        test_table.add_entries(e1)
        test_table.remove_entries(e1_addr)
        test_table.add_entries(e1)
        #get the attributes of the entry
        e1_attrs_read = test_table.get_entry_attrs(e1_addr)
        for field in fields(e1) :
            self.assertEqual(getattr(e1,field.name),e1_attrs_read[field.name])
        LOGGER.set_stream_level(logging.INFO)
        LOGGER.info('\nExpecting five errors below:')
        LOGGER.set_stream_level(logging.ERROR)
        #adding the same entry again should give an error
        with self.assertRaises(ValueError) :
            test_table.add_entries(e1)
        #removing a nonexistent entry should give an error
        with self.assertRaises(ValueError) :
            test_table.remove_entries(e2_addr)
        #trying to get attrs for a nonexistent entry should give an error
        with self.assertRaises(ValueError) :
            _ = test_table.get_entry_attrs(e2_addr)
        #same thing for setting attrs
        with self.assertRaises(ValueError) :
            test_table.set_entry_attrs(e2_addr,str_field='this should break')
        #should also get an error if trying to get a view keyed by a nonexistent attr
        with self.assertRaises(ValueError) :
            _ = test_table.obj_addresses_by_key_attr('never_name_a_dataclass_attribute_this')
        #dumping the file to write it out to be re-read
        test_table.dump_to_file()
        del test_table
        test_table = DataclassTable(dataclass_type=TableLineForTesting,
                                    filepath=testing_location/'test_table.csv',logger=LOGGER)
        #it should have the same one entry in it
        self.assertEqual(1,len(test_table.obj_addresses_by_key_attr('str_field')['test']))
        #after adding the other two entries the len of that list should be 3
        test_table.add_entries([e2,e3])
        self.assertEqual(3,len(test_table.obj_addresses_by_key_attr('str_field')['test']))
        #and there should be three unique integer fields
        self.assertEqual(3,len(test_table.obj_addresses_by_key_attr('int_field').keys()))
        #set one of the entries' str field to be something different
        test_table.set_entry_attrs(e2_addr,str_field='testing')
        by_str_field = test_table.obj_addresses_by_key_attr('str_field')
        self.assertTrue('test' in by_str_field.keys())
        self.assertEqual(2,len(by_str_field['test']))
        self.assertTrue(e3_addr in by_str_field['test'])
        self.assertTrue('testing' in by_str_field.keys())
        self.assertEqual(1,len(by_str_field['testing']))
        self.assertTrue(e2_addr in by_str_field['testing'])
        #dump and delete the test table
        test_table.dump_to_file()
        del test_table
        #remove the testing directory
        shutil.rmtree(testing_location)
