"""Some miscellaneous functions that are universally internally available"""

#imports
import os, sys, inspect, time, contextlib

def add_user_input(input_queue) :
    """
    Listen for and add user input to a queue at one second intervals
    """
    while True :
        time.sleep(1)
        input_queue.put((sys.stdin.read(1)).strip())

def raise_err_with_optional_logger(logger,errmsg,exc_type) :
    """
    If logger is not None, log the error message with the given exception type, otherwise raise the error
    """
    if logger is not None :
        logger.error(errmsg,exc_type)
    else :
        raise exc_type(errmsg)

def debug_msg_with_optional_logger(logger,msg) :
    """
    Log a given message at debug level if logger is not None, otherwise print it
    """
    if logger is not None :
        logger.debug(msg)
    else :
        print(msg)

def _ensure_classes_or_types_match_for_key(key,test,options,logger=None) :
    """
    Raise errors if "test" is not a subclass of the possibilities in "options" if "options" holds classes,
    or if the type of "test" is not in "options" if "options" holds objects
    """
    #if the options are classes
    if inspect.isclass(options[0] if isinstance(options,tuple) else options) :
        #make sure the test argument is also a class
        if not inspect.isclass(test) :
            errmsg = f'ERROR: Argument "{key}" expected to be a class, not an object!'
            raise_err_with_optional_logger(logger,errmsg,RuntimeError)
        #make sure the test argument is a subtype of any of the classes specified in the defaults
        if not issubclass(test,options[1:] if isinstance(options,tuple) else options) :
            errmsg = f'ERROR: Class type mismatch for argument "{key}", got {test} '
            errmsg+= f'but expected {options[1:] if isinstance(options,tuple) else options}'
            raise_err_with_optional_logger(logger,errmsg,RuntimeError)
    #if the options are objects instead, make sure the type of the test argument matches
    #one of the possible types in the defaults
    elif ( (isinstance(options,tuple) and type(test) not in options[1:]) or
           (not isinstance(options,tuple) and not isinstance(test,type(options))) ) :
        errmsg = f'ERROR: Type mismatch replacing argument "{key}" with {test} '
        errmsg+= '(expected '
        if isinstance(options,tuple) :
            for typestring in options[1:] :
                errmsg+=f'{typestring}, '
        else :
            errmsg+=f'{type(options)}'
        errmsg+=f'but got {type(test)})'
        raise_err_with_optional_logger(logger,errmsg,TypeError)

def populated_kwargs(given_kwargs,defaults,logger=None) :
    """
    Return a kwargs dictionary where every possible entry from the defaults has a valid value
    Can use this to make sure certain entries are present in kwargs
    """
    #for all the needed keys
    for key in defaults.keys() :
        #if it was given and is not None
        if key in given_kwargs.keys() and given_kwargs[key] is not None :
            # if there's just the default option it doesn't need to be a tuple
            if isinstance(defaults[key],tuple) :
                if len(defaults[key])==1 :
                    defaults[key] = (defaults[key])[0]
            #check the options for what the argument is allowed to be
            _ensure_classes_or_types_match_for_key(key,given_kwargs[key],defaults[key],logger)
        #if it wasn't given, then just add it to the dictionary as the default
        else :
            if isinstance(defaults[key],tuple) :
                given_kwargs[key] = (defaults[key])[0]
            else :
                given_kwargs[key] = defaults[key]
    return given_kwargs

@contextlib.contextmanager
def change_dir(dirpath):
    """
    Change the current working directory to a different directory,
    and go back when leaving the context manager.
    """
    cdminus = os.getcwd()
    try:
        yield os.chdir(dirpath)
    finally:
        os.chdir(cdminus)
