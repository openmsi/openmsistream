"""
A couple utility classes (and a helper function) to represent a set of (relatively arbitrary) dataclass objects
serialized to/deseralized from a corresponding CSV file in an atomic and (relatively) thread-safe way
"""

#imports
import pathlib, functools, datetime, typing, copy, os, time
from abc import ABC
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

class DataclassTableBase(LogOwner,ABC) :
    """
    A base class for DataclassTable objects

    :param dataclass_type: The :class:`dataclasses.dataclass` defining the entries in the table/csv file
    :type dataclass_type: :class:`dataclasses.dataclass`
    :param filepath: The path to the .csv file that should be created (or read from) on startup.
        The default is a file named after the dataclass type in the current directory.
    :type filepath: :class:`pathlib.Path` or None, optional
    :param create_if_missing: If True, the file at the given path will be created as an empty file
        if it doesn't already exist
    :type create_if_missing: bool
    """

    #################### CONSTANTS ####################

    DELIMETER = ';' #can't use a comma or containers would display incorrectly
    DATETIME_FORMAT = '%a %b %d, %Y at %H:%M:%S'
    UPDATE_FILE_EVERY = 5 #only update the .csv file automatically every 5 seconds to make updates less expensive
    NESTED_TYPES = get_nested_types()

    #################### PUBLIC FUNCTIONS ####################

    def __init__(self,dataclass_type,*,filepath=None,create_if_missing=True,**kwargs) :
        """
        Constructor method
        """
        #init the LogOwner
        super().__init__(**kwargs)
        #create the thread lock for the table
        self.lock = Lock()
        #figure out what type of objects the table/file will be describing
        self.__dataclass_type = dataclass_type
        if not is_dataclass(self.__dataclass_type) :
            self.logger.error(f'ERROR: "{self.__dataclass_type}" is not a dataclass!',exc_type=TypeError)
        if len(fields(self.__dataclass_type)) <= 0 :
            errmsg = f'ERROR: dataclass type {self.__dataclass_type} does not have any fields '
            errmsg+= f'and so cannot be used in a {self.__class__.__name__}!'
            self.logger.error(errmsg,exc_type=ValueError)
        self.__field_names = [field.name for field in fields(self.__dataclass_type)]
        self.__field_types = [field.type for field in fields(self.__dataclass_type)]
        #figure out where the csv file should go
        self.__filepath = filepath if filepath is not None else pathlib.Path() / f'{self.__dataclass_type.__name__}.csv'
        #set some other variables
        self._entry_objs = {}
        self._entry_lines = {}
        #read or create the file to finish setting up the table
        self._file_last_updated = datetime.datetime.now()
        if self.__filepath.is_file() :
            self.__read_csv_file()
        elif create_if_missing :
            msg = f'Creating new {self.__class__.__name__} csv file at {self.__filepath} '
            msg+= f'to hold {self.__dataclass_type.__name__} entries'
            self.logger.debug(msg)
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
        if entry_obj_address not in self._entry_objs :
            errmsg = f'ERROR: address {entry_obj_address} sent to get_entry_attrs is not registered!'
            self.logger.error(errmsg,exc_type=ValueError)
        obj = self._entry_objs[entry_obj_address]
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
            self.logger.error(errmsg,exc_type=ValueError)
        to_return = {}
        locked_internally = False
        if not self.lock.locked() :
            locked_internally = True
            self.lock.acquire()
        for addr,obj in self._entry_objs.items() :
            rkey = getattr(obj,key_attr_name)
            if rkey not in to_return :
                to_return[rkey] = []
            to_return[rkey].append(addr)
        if locked_internally :
            self.lock.release()
        return to_return

    def dump_to_file(self,reraise_exc=True,retries=2) :
        """
        Dump the contents of the table to a csv file.
        Call this to force the file to update and reflect the current state of objects.
        Automatically called in several contexts.

        :param reraise_exc: if True, Exceptions raised when writing out to the file will be re-raised
        :type reraise_exc: bool, optional
        :param retries: how many times to retry writing out the file
        :type retries: int, optional
        """
        lines_to_write = [self.csv_header_line]
        if len(self._entry_lines)>0 :
            lines_to_write+=list(self._entry_lines.values())
        locked_internally = False
        if not self.lock.locked() :
            locked_internally = True
            self.lock.acquire()
        self.__write_lines(lines_to_write,reraise_exc=reraise_exc,retries=retries)
        if locked_internally :
            self.lock.release()

    #################### PROPERTIES ####################

    @methodtools.lru_cache()
    @property
    def csv_header_line(self) :
        """
        The first line in the CSV file (OS-dependent)
        """
        header_line = ''
        #add an extra line when running on Windows to seamlessly open the file in Excel
        if os.name=='nt' :
            header_line+=f'sep={self.DELIMETER}\n'
        for fieldname in self.__field_names :
            header_line+=f'{fieldname}{self.DELIMETER}'
        return header_line[:-1]

    @property
    def obj_addresses(self) :
        """
        All recognized object hex addresses
        """
        return list(self._entry_objs.keys())

    @property
    def filepath(self) :
        """
        Path to the CSV file
        """
        return self.__filepath

    @property
    def n_entries(self) :
        """
        The number of entries in the file
        """
        return len(self._entry_objs)

    @property
    def dataclass_type(self) :
        """
        The type of the Dataclass objects contained in the table
        """
        return self.__dataclass_type

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
                self.logger.error(errmsg,exc_type=RuntimeError)
        #add entry lines and objects
        for line in lines_as_read[n_header_lines:] :
            obj = self.__obj_from_line(line)
            #key both dictionaries by the address of the object in memory
            dkey = hex(id(obj))
            self._entry_objs[dkey] = obj
            self._entry_lines[dkey] = line
        msg = f'Found {len(self._entry_objs)} {self.__dataclass_type.__name__} entries in {self.__filepath}'
        self.logger.debug(msg)

    def __write_lines(self,lines,overwrite=True,reraise_exc=True,retries=2) :
        """
        Write a line or container of lines to the csv file, in a thread-safe and atomic way
        """
        if isinstance(lines,str) :
            lines = [lines]
        lines_string = ''
        for line in lines :
            lines_string+=f'{line.strip()}\n'
        n_retries_left = retries
        caught_exc = None
        while n_retries_left>0 :
            try :
                with atomic_write(self.__filepath,overwrite=overwrite) as fp :
                    fp.write(lines_string)
                self._file_last_updated = datetime.datetime.now()
                return
            except Exception as exc :
                caught_exc = exc
                n_retries_left-=1
                time.sleep(0.1)
                continue
        if caught_exc is not None :
            if not isinstance(caught_exc,FileNotFoundError) : #This is common to see and okay when running multithreaded
                if reraise_exc :
                    errmsg = f'ERROR: failed to write to {self.__class__.__name__} csv file at {self.__filepath} '
                    errmsg+= f'after {retries-n_retries_left+1} attempts! Will reraise exception.'
                    self.logger.error(errmsg,exc_info=caught_exc,reraise=True)
                else :
                    msg = f'WARNING: failed in {retries-n_retries_left+1} attempts to write to '
                    msg+= f'{self.__class__.__name__} csv file at {self.__filepath}! '
                    msg+= f'Exception ({type(caught_exc)}): {caught_exc}'
                    self.logger.warning(msg)

    def _line_from_obj(self,obj) :
        """
        Return the csv file line for a given object
        """
        if obj.__class__ != self.__dataclass_type :
            self.logger.error(f'ERROR: "{obj}" is mismatched to type {self.__dataclass_type}!',exc_type=TypeError)
        obj_line = ''
        for fname,ftype in zip(self.__field_names,self.__field_types) :
            obj_line+=f'{self.__get_str_from_attribute(getattr(obj,fname),ftype)}{self.DELIMETER}'
        return obj_line[:-1]

    def __obj_from_line(self,line) :
        """
        Return the dataclass instance for a given csv file line string
        """
        args = []
        for attrtype,attrstr in zip(self.__field_types,(line.strip().split(self.DELIMETER))) :
            args.append(self.__get_attribute_from_str(attrstr,attrtype))
        return self.__dataclass_type(*args)

    def __get_str_from_attribute(self,attrobj,attrtype) :
        """
        Given an object and the type it is in the dataclass,
        return the string representation of it that should go in the file
        """
        if attrtype==datetime.datetime :
            return repr(attrobj.strftime(self.DATETIME_FORMAT))
        if attrtype==pathlib.Path :
            return repr(str(attrobj))
        return repr(attrobj)

    def __get_attribute_from_str(self,attrstr,attrtype) :
        """
        Given the string of the repr() output of some object and the datatype it represents, return the actual object
        """
        #datetime objects are handled in a custom way
        if attrtype==datetime.datetime :
            return datetime.datetime.strptime(attrstr[1:-1],self.DATETIME_FORMAT)
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
        self.logger.error(errmsg,exc_type=ValueError)
        return None

class DataclassTableReadOnly(DataclassTableBase) :
    """
    A class to read dataclass objects stored in a csv file
    """

    def __init__(self,dataclass_type,*,filepath=None,**kwargs) :
        """
        Signature duplicated here for documentation
        """
        super().__init__(dataclass_type,filepath=filepath,create_if_missing=False,**kwargs)

    @property
    def objects(self) :
        """
        A list of all the dataclass objects in the file
        """
        return self._entry_objs.values()

    @property
    def lines(self) :
        """
        A list of all the text lines in the file
        """
        return self._entry_lines.values()

class DataclassTableAppendOnly(DataclassTableBase) :
    """
    A class to work with an atomic csv file that's holding dataclass entries. Only includes methods to add lines
    to the file like a log and not to edit the objects themselves, which can be much more efficient.
    """

    def __init__(self,dataclass_type,*,filepath=None,**kwargs) :
        """
        Signature duplicated here for documentation
        """
        super().__init__(dataclass_type,filepath=filepath,**kwargs)

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
            if entry_addr in self._entry_objs or entry_addr in self._entry_lines :
                errmsg = 'ERROR: address of object sent to add_entries is already registered!'
                self.logger.error(errmsg,exc_type=ValueError)
            locked_internally = False
            if not self.lock.locked() :
                locked_internally = True
                self.lock.acquire()
            self._entry_objs[entry_addr] = entry
            self._entry_lines[entry_addr] = self._line_from_obj(entry)
            self.obj_addresses_by_key_attr.cache_clear()
            if locked_internally :
                self.lock.release()
        if (datetime.datetime.now()-self._file_last_updated).total_seconds()>self.UPDATE_FILE_EVERY :
            self.dump_to_file()

    def as_read_only(self) :
        """
        Returns a "read only" version of the table
        """
        return DataclassTableReadOnly(self.dataclass_type,filepath=self.filepath,logger=self.logger)

class DataclassTable(DataclassTableAppendOnly) :
    """
    A class to work with an atomic csv file that's holding dataclass entries in a thread-safe way

    :param dataclass_type: The :class:`dataclasses.dataclass` defining the entries in the table/csv file
    :type dataclass_type: :class:`dataclasses.dataclass`
    :param filepath: The path to the .csv file that should be created (or read from) on startup.
        The default is a file named after the dataclass type in the current directory.
    :type filepath: :class:`pathlib.Path` or None, optional
    """

    def __init__(self,dataclass_type,*,filepath=None,**kwargs) :
        """
        Signature duplicated here for documentation
        """
        super().__init__(dataclass_type,filepath=filepath,**kwargs)

    def __del__(self) :
        self.dump_to_file(reraise_exc=False)

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
            if (entry_addr not in self._entry_objs) or (entry_addr not in self._entry_lines) :
                errmsg = f'ERROR: address {entry_addr} sent to remove_entries is not registered!'
                self.logger.error(errmsg,exc_type=ValueError)
            locked_internally = False
            if not self.lock.locked() :
                locked_internally = True
                self.lock.acquire()
            self._entry_objs.pop(entry_addr)
            self._entry_lines.pop(entry_addr)
            self.obj_addresses_by_key_attr.cache_clear()
            if locked_internally :
                self.lock.release()
        if (datetime.datetime.now()-self._file_last_updated).total_seconds()>self.UPDATE_FILE_EVERY :
            self.dump_to_file()

    def set_entry_attrs(self,entry_obj_address,**kwargs) :
        """
        Modify attributes of an entry that already exists in the table

        :param entry_obj_address: The address in memory of the entry object to modify
        :type entry_obj_address: hex(id(object))
        :param kwargs: Attributes to set (keys are names, values are values for those named attrs)
        :type kwargs: dict

        :raises ValueError: if `entry_obj_address` doesn't correspond to an object listed in the table
        """
        if entry_obj_address not in self._entry_objs :
            errmsg = f'ERROR: address {entry_obj_address} sent to set_entry_attrs is not registered!'
            self.logger.error(errmsg,exc_type=ValueError)
        locked_internally = False
        if not self.lock.locked() :
            locked_internally = True
            self.lock.acquire()
        obj = self._entry_objs[entry_obj_address]
        for fname,fval in kwargs.items() :
            setattr(obj,fname,fval)
        self._entry_lines[entry_obj_address] = self._line_from_obj(obj)
        self.obj_addresses_by_key_attr.cache_clear()
        if locked_internally :
            self.lock.release()
        if (datetime.datetime.now()-self._file_last_updated).total_seconds()>self.UPDATE_FILE_EVERY :
            self.dump_to_file()
