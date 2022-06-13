#imports
import os, sys, inspect, time, contextlib

def add_user_input(input_queue) :
    """
    Listen for and add user input to a queue at one second intervals
    """
    while True :
        time.sleep(1)
        input_queue.put((sys.stdin.read(1)).strip())

def populated_kwargs(given_kwargs,defaults,logger=None) :
    """
    Return a kwargs dictionary where every possible entry from the defaults has a valid value
    Can use this to make sure certain entries are present in kwargs
    """
    #for all the needed keys
    for key in defaults.keys() :
        #if it was given and is not None
        if key in given_kwargs.keys() and given_kwargs[key] is not None :
            #if there are several options for what the argument can be, check them all
            if isinstance(defaults[key],tuple) :
                #if there is a default and at least one specified class or type that the argument must be
                if len(defaults[key])>1 :
                    #if the default argument is a class
                    if inspect.isclass((defaults[key])[0]) :
                        #make sure the given argument is also a class
                        if (not inspect.isclass(given_kwargs[key])) :
                            errmsg = f'ERROR: Argument "{key}" expected to be a class, not an object!'
                            if logger is not None :
                                logger.error(errmsg,RuntimeError)
                            else :
                                raise RuntimeError(errmsg)
                        #make sure the given argument is a subtype of any of the classes specified in the defaults
                        isgood=False
                        for poss_class in (defaults[key])[1:] :
                            if issubclass(given_kwargs[key],poss_class) :
                                isgood=True
                                break
                        if not isgood :
                            errmsg = f'ERROR: Class type mismatch for argument "{key}", got {given_kwargs[key]} '
                            errmsg+= f'but expected one of {(defaults[key])[1:]}'
                            if logger is not None :
                                logger.error(errmsg,RuntimeError)
                            else :
                                raise RuntimeError(errmsg)
                    #if the default is an object, make sure the that type of the given argument matches 
                    #one of the possible types in the defaults
                    elif type(given_kwargs[key]) not in (defaults[key])[1:] :
                        errmsg = f'ERROR: Type mismatch replacing argument "{key}" with {given_kwargs[key]} '
                        errmsg+= '(expected one of '
                        for t in (defaults[key])[1:] :
                            errmsg+=f'{t}, '
                        errmsg+=f'but got {type(given_kwargs[key])})'
                        if logger is not None :
                            logger.error(errmsg,TypeError)
                        else :
                            raise TypeError(errmsg)
                #otherwise if there's just the default option it doesn't need to be a tuple
                else :
                    defaults[key] = (defaults[key])[0]
            #if the single default option is a class
            elif inspect.isclass(defaults[key]) :
                #make sure the given argument is also a class
                if (not inspect.isclass(given_kwargs[key])) :
                    errmsg = f'ERROR: Argument "{key}" expected to be a class, not an object!'
                    if logger is not None :
                        logger.error(errmsg,RuntimeError)
                    else :
                        raise RuntimeError(errmsg)
                #make sure the given argument is a subtype of the default class
                if not issubclass(given_kwargs[key],defaults[key]) :
                    errmsg = f'ERROR: Class type mismatch for argument "{key}", got {given_kwargs[key]} '
                    errmsg+= f'but expected {defaults[key]}'
                    if logger is not None :
                        logger.error(errmsg,RuntimeError)
                    else :
                        raise RuntimeError(errmsg)
            #if the single default option is just an object, make sure the argument is of the same type as the default
            elif defaults[key] is not None and type(defaults[key]) is not type(given_kwargs[key]) :
                errmsg = f'ERROR: Type mismatch replacing argument "{key}" with {given_kwargs[key]} '
                errmsg+= f'(expected type {type(defaults[key])})'
                if logger is not None :
                    logger.error(errmsg,TypeError)
                else :
                    raise TypeError(errmsg)
        #if it wasn't given, then just add it to the dictionary as the default
        else :
            if isinstance(defaults[key],tuple) :
                given_kwargs[key] = (defaults[key])[0]
            else :
                given_kwargs[key] = defaults[key]
    return given_kwargs

@contextlib.contextmanager
def cd(dir):
    """
    Change the current working directory to a different directory,
    and go back when leaving the context manager.
    """
    cdminus = os.getcwd()
    try:
        yield os.chdir(dir)
    finally:
        os.chdir(cdminus)
