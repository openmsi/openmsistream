"""
A utility class to represent a set of (relatively arbitrary) dataclass objects serialized to/deseralized from
a corresponding CSV file in an atomic and (relatively) thread-safe way
"""

#imports
import pathlib, functools, datetime, typing, copy, os
from threading import Lock
from dataclasses import fields, is_dataclass
import methodtools
from atomicwrites import atomic_write
from .logging import LogOwner

def get_nested_types() :
    """
    Helper function to generate the class constant that holds the supported nested types for dataclass tables
    """
    simple_types = [str,int,float,complex,bool]
    container_types = [(typing.List,list,'[]'),(typing.Tuple,tuple,'()'),(typing.Set,set,'set()')]
    nested_types_dict = {}
    for c_t in container_types :
        for s_t in simple_types :
            nested_types_dict[c_t[0][s_t]] = (c_t[1],s_t,c_t[2])
    return nested_types_dict

class DataclassTable(LogOwner) :
    """
    A class to work with an atomic csv file that's holding dataclass entries in a thread-safe way

    :param dataclass_type: The :class:`dataclasses.dataclass` defining the entries in the table/csv file
    :type dataclass_type: :class:`dataclasses.dataclass`
    :param filepath: The path to the .csv file that should be created (or read from) on startup.
        The default is a file named after the dataclass type in the current directory.
    :type filepath: :class:`pathlib.Path` or None, optional
    """

    #################### PROPERTIES AND CONSTANTS ####################

    DELIMETER = ';' #can't use a comma or containers would display incorrectly
    DATETIME_FORMAT = '%a %b %d, %Y at %H:%M:%S'
    THREAD_LOCK = Lock()
    UPDATE_FILE_EVERY = 5 #only update the .csv file automatically every 5 seconds to make updates less expensive

    NESTED_TYPES = get_nested_types()

    @methodtools.lru_cache()
    @property
    def csv_header_line(self) :
        """
        The first line in the CSV file (OS-dependent)
        """
        header_line = ''
        #add an extra line when running on Windows to seamlessly open the file in Excel
        if os.name=='nt' :
            header_line+=f'sep={DataclassTable.DELIMETER}\n'
        for fieldname in self.__field_names :
            header_line+=f'{fieldname}{DataclassTable.DELIMETER}'
        return header_line[:-1]

    @property
    def obj_addresses(self) :
        """
        All recognized object hex addresses
        """
        return list(self.__entry_objs.keys())

    @property
    def filepath(self) :
        """
        Path to the CSV file
        """
        return self.__filepath

    @property
    def lock(self) :
        """
        A thread lock to use for ensuring only one thread is interacting with the :class:`~DataclassTable` at a time
        """
        return DataclassTable.THREAD_LOCK

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,dataclass_type,*,filepath=None,**kwargs) :
        """
        Constructor method
        """
        #init the LogOwner
        super().__init__(**kwargs)
        #figure out what type of objects the table/file will be describing
        self.__dataclass_type = dataclass_type
        if not is_dataclass(self.__dataclass_type) :
            self.logger.error(f'ERROR: "{self.__dataclass_type}" is not a dataclass!',TypeError)
        if len(fields(self.__dataclass_type)) <= 0 :
            errmsg = f'ERROR: dataclass type {self.__dataclass_type} does not have any fields '
            errmsg+= f'and so cannot be used in a {self.__class__.__name__}!'
            self.logger.error(errmsg,ValueError)
        self.__field_names = [field.name for field in fields(self.__dataclass_type)]
        self.__field_types = [field.type for field in fields(self.__dataclass_type)]
        #figure out where the csv file should go
        self.__filepath = filepath if filepath is not None else pathlib.Path() / f'{self.__dataclass_type.__name__}.csv'
        #set some other variables
        self.__entry_objs = {}
        self.__entry_lines = {}
        #read or create the file to finish setting up the table
        self.__file_last_updated = datetime.datetime.now()
        if self.__filepath.is_file() :
            self.__read_csv_file()
        else :
            msg = f'Creating new {self.__class__.__name__} csv file at {self.__filepath} '
            msg+= f'to hold {self.__dataclass_type.__name__} entries'
            self.logger.info(msg)
            self.dump_to_file()

    def __del__(self) :
        self.dump_to_file(reraise_exc=False)

    def add_entries(self,new_entries) :
        """
        Add a new set of entries to the table.

        :param new_entries: the new entry or entries to add to the table
        :type new_entries: :class:`dataclasses.dataclass` or list(:class:`dataclasses.dataclass`)

        :raises ValueError: if any of the objects in `new_entries` already exists in the table
        """
        if is_dataclass(new_entries) :
            new_entries = [new_entries]
        for entry in new_entries :
            entry_addr = hex(id(entry))
            if entry_addr in self.__entry_objs or entry_addr in self.__entry_lines :
                self.logger.error('ERROR: address of object sent to add_entries is already registered!',ValueError)
            with DataclassTable.THREAD_LOCK :
                self.__entry_objs[entry_addr] = entry
                self.__entry_lines[entry_addr] = self.__line_from_obj(entry)
                self.obj_addresses_by_key_attr.cache_clear()
        if (datetime.datetime.now()-self.__file_last_updated).total_seconds()>DataclassTable.UPDATE_FILE_EVERY :
            self.dump_to_file()

    def remove_entries(self,entry_obj_addresses) :
        """
        Remove an entry or entries from the table

        :param entry_obj_addresses: a single value or container of entry addresses (object IDs in hex form) to remove
            from the table
        :type entry_obj_addresses: hex(id(object)) or list(hex(id(object)))

        :raises ValueError: if any of the hex addresses in `entry_obj_addresses` is not present in the table
        """
        if isinstance(entry_obj_addresses,str) :
            entry_obj_addresses = [entry_obj_addresses]
        for entry_addr in entry_obj_addresses :
            if (entry_addr not in self.__entry_objs) or (entry_addr not in self.__entry_lines) :
                self.logger.error(f'ERROR: address {entry_addr} sent to remove_entries is not registered!',ValueError)
            with DataclassTable.THREAD_LOCK :
                self.__entry_objs.pop(entry_addr)
                self.__entry_lines.pop(entry_addr)
                self.obj_addresses_by_key_attr.cache_clear()
        if (datetime.datetime.now()-self.__file_last_updated).total_seconds()>DataclassTable.UPDATE_FILE_EVERY :
            self.dump_to_file()

    def get_entry_attrs(self,entry_obj_address,*args) :
        """
        Return copies of all or some of the current attributes of an entry in the table.
        Returning copies ensures the original objects cannot be modified by accident.

        Use `args` to get a dictionary of desired attribute values returned.
        If only one arg is given the return value is just that single attribute.
        The default (no additional arguments) returns a dictionary of all attributes for the entry

        :param entry_obj_address: the address in memory of the object to return copies of attributes for
        :type entry_obj_address: hex(id(object))
        :param args: Add other arguments that are names of attributes to get only those specific attributes of the entry
        :type args: str, optional

        :return: copies of some or all attributes for an entry in the table
        :rtype: depends on arguments, or dict

        :raises ValueError: if `entry_obj_address` doesn't correspond to an object listed in the table
        """
        if entry_obj_address not in self.__entry_objs :
            errmsg = f'ERROR: address {entry_obj_address} sent to get_entry_attrs is not registered!'
            self.logger.error(errmsg,ValueError)
        obj = self.__entry_objs[entry_obj_address]
        if len(args)==1 :
            return copy.deepcopy(getattr(obj,args[0]))
        to_return = {}
        for fname in self.__field_names :
            if (not args) or (fname in args) :
                to_return[fname] = copy.deepcopy(getattr(obj,fname))
        if args :
            for arg in args :
                if arg not in to_return :
                    errmsg = f'WARNING: attribute name {arg} is not a name of a {self.__dataclass_type} '
                    errmsg+= 'Field and will not be returned from get_entry_attrs!'
                    self.logger.warning(errmsg)
        return to_return

    def set_entry_attrs(self,entry_obj_address,**kwargs) :
        """
        Modify attributes of an entry that already exists in the table

        :param entry_obj_address: The address in memory of the entry object to modify
        :type entry_obj_address: hex(id(object))
        :param kwargs: Attributes to set (keys are names, values are values for those named attrs)
        :type kwargs: dict

        :raises ValueError: if `entry_obj_address` doesn't correspond to an object listed in the table
        """
        if entry_obj_address not in self.__entry_objs :
            errmsg = f'ERROR: address {entry_obj_address} sent to set_entry_attrs is not registered!'
            self.logger.error(errmsg,ValueError)
        with DataclassTable.THREAD_LOCK :
            obj = self.__entry_objs[entry_obj_address]
            for fname,fval in kwargs.items() :
                setattr(obj,fname,fval)
            self.__entry_lines[entry_obj_address] = self.__line_from_obj(obj)
            self.obj_addresses_by_key_attr.cache_clear()
        if (datetime.datetime.now()-self.__file_last_updated).total_seconds()>DataclassTable.UPDATE_FILE_EVERY :
            self.dump_to_file()

    @functools.lru_cache(maxsize=8)
    def obj_addresses_by_key_attr(self,key_attr_name) :
        """
        Return a dictionary whose keys are the values of some given attribute for each object
        and whose values are lists of the addresses in memory of the objects in the table
        that have each value of the requested attribute.

        Useful to find objects in the table by attribute values so they can be efficiently updated
        without compromising the integrity of the objects in the table and their attributes.

        Up to five calls are cached so if nothing changes this happens a little faster.

        :param key_attr_name: the name of the attribute whose values should be used as keys in the returned dictionary
        :type key_attr_name: str

        :return: A dictionary listing all objects in the table, keyed by their values of `key_attr_name`
        :rtype: dict

        :raises ValueError: if `key_attr_name` is not recognized as the name of an attribute for entries in the table
        """
        if key_attr_name not in self.__field_names :
            errmsg = f'ERROR: {key_attr_name} is not a name of a Field for {self.__dataclass_type} objects!'
            self.logger.error(errmsg,ValueError)
        to_return = {}
        with DataclassTable.THREAD_LOCK :
            for addr,obj in self.__entry_objs.items() :
                rkey = getattr(obj,key_attr_name)
                if rkey not in to_return :
                    to_return[rkey] = []
                to_return[rkey].append(addr)
        return to_return

    def dump_to_file(self,reraise_exc=True) :
        """
        Dump the contents of the table to a csv file.
        Call this to force the file to update and reflect the current state of objects.
        Automatically called in several contexts.
        """
        lines_to_write = [self.csv_header_line]
        if len(self.__entry_lines)>0 :
            lines_to_write+=list(self.__entry_lines.values())
        with DataclassTable.THREAD_LOCK :
            self.__write_lines(lines_to_write,reraise_exc=reraise_exc)

    #################### PRIVATE HELPER FUNCTIONS ####################

    def __read_csv_file(self) :
        """
        Read in a csv file with lines of the expected dataclass type
        """
        #read the file
        with open(self.__filepath,'r') as fp :
            lines_as_read = fp.readlines()
        #first make sure the header line matches what we'd expect for the dataclass
        n_header_lines = len(self.csv_header_line.split('\n'))
        for ihl in range(n_header_lines) :
            if lines_as_read[ihl].strip()!=(self.csv_header_line.split('\n'))[ihl] :
                errmsg = f'ERROR: header line in {self.__filepath} ({lines_as_read[0]}) does not match expectation for '
                errmsg+= f'{self.__dataclass_type} ({self.csv_header_line})!'
                self.logger.error(errmsg,RuntimeError)
        #add entry lines and objects
        for line in lines_as_read[n_header_lines:] :
            obj = self.__obj_from_line(line)
            #key both dictionaries by the address of the object in memory
            dkey = hex(id(obj))
            self.__entry_objs[dkey] = obj
            self.__entry_lines[dkey] = line
        msg = f'Found {len(self.__entry_objs)} {self.__dataclass_type.__name__} entries in {self.__filepath}'
        self.logger.info(msg)

    def __write_lines(self,lines,overwrite=True,reraise_exc=True) :
        """
        Write a line or container of lines to the csv file, in a thread-safe and atomic way
        """
        if isinstance(lines,str) :
            lines = [lines]
        lines_string = ''
        for line in lines :
            lines_string+=f'{line.strip()}\n'
        try :
            with atomic_write(self.__filepath,overwrite=overwrite) as fp :
                fp.write(lines_string)
        except Exception as exc :
            if not isinstance(exc,FileNotFoundError) : #This is common to see and okay when running multithreaded
                if reraise_exc :
                    errmsg = f'ERROR: failed to write to {self.__class__.__name__} csv file at {self.__filepath}! '
                    errmsg+=  'Will reraise exception.'
                    self.logger.error(errmsg,exc_obj=exc)
                else :
                    msg = f'WARNING: failed an attempt to write to {self.__class__.__name__} csv file at '
                    msg+= f'{self.__filepath}! Exception ({type(exc)}): {exc}'
                    self.logger.warning(msg)
        self.__file_last_updated = datetime.datetime.now()

    def __line_from_obj(self,obj) :
        """
        Return the csv file line for a given object
        """
        if obj.__class__ != self.__dataclass_type :
            self.logger.error(f'ERROR: "{obj}" is mismatched to type {self.__dataclass_type}!',TypeError)
        obj_line = ''
        for fname,ftype in zip(self.__field_names,self.__field_types) :
            obj_line+=f'{self.__get_str_from_attribute(getattr(obj,fname),ftype)}{DataclassTable.DELIMETER}'
        return obj_line[:-1]

    def __obj_from_line(self,line) :
        """
        Return the dataclass instance for a given csv file line string
        """
        args = []
        for attrtype,attrstr in zip(self.__field_types,(line.strip().split(DataclassTable.DELIMETER))) :
            args.append(self.__get_attribute_from_str(attrstr,attrtype))
        return self.__dataclass_type(*args)

    def __get_str_from_attribute(self,attrobj,attrtype) :
        """
        Given an object and the type it is in the dataclass,
        return the string representation of it that should go in the file
        """
        if attrtype==datetime.datetime :
            return repr(attrobj.strftime(DataclassTable.DATETIME_FORMAT))
        if attrtype==pathlib.Path :
            return repr(str(attrobj))
        return repr(attrobj)

    def __get_attribute_from_str(self,attrstr,attrtype) :
        """
        Given the string of the repr() output of some object and the datatype it represents, return the actual object
        """
        #datetime objects are handled in a custom way
        if attrtype==datetime.datetime :
            return datetime.datetime.strptime(attrstr[1:-1],DataclassTable.DATETIME_FORMAT)
        #so are path objects
        if attrtype==pathlib.Path :
            return pathlib.Path(attrstr[1:-1])
        #strings have extra quotes on either end
        if attrtype==str :
            return attrtype(attrstr[1:-1])
        #int, float, complex, and bool can all be directly re-casted
        if attrtype in (int,float,complex,bool) :
            return attrtype(attrstr)
        #some simply-nested container types can be casted in two steps
        if attrtype in self.NESTED_TYPES :
            #empty collections
            if attrstr==self.NESTED_TYPES[attrtype][2] :
                return self.NESTED_TYPES[attrtype][0]()
            to_cast = []
            for vstr in attrstr[1:-1].split(',') :
                to_cast.append(self.NESTED_TYPES[attrtype][1](vstr))
            return self.NESTED_TYPES[attrtype][0](to_cast)
        errmsg = f'ERROR: attribute type "{attrtype}" is not recognized for a {self.__class__.__name__}!'
        self.logger.error(errmsg,ValueError)
        return None
