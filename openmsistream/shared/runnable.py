#imports
from abc import ABC, abstractmethod
from .argument_parsing import MyArgumentParser
from .has_argument_parser import HasArgumentParser

class Runnable(HasArgumentParser,ABC) :
    """
    Class for any child classes that can be run on their own from the command line
    """

    ARGUMENT_PARSER_TYPE = MyArgumentParser

    @classmethod
    @abstractmethod
    def get_command_line_arguments(cls) :
        """
        child classes should implement this function to return a list of argument names 
        and a dictionary of argument names/default values to add to the argument parser
        """
        return [],{}

    @classmethod
    def get_argument_parser(cls,*args,**kwargs) :
        """
        Return the argument parser used to run the code
        """
        parser = cls.ARGUMENT_PARSER_TYPE(*args,**kwargs)
        cl_args, cl_kwargs = cls.get_command_line_arguments()
        parser.add_arguments(*cl_args,**cl_kwargs)
        return parser

    @classmethod
    @abstractmethod
    def run_from_command_line(cls,args=None) :
        """
        child classes should implement this function to do whatever it is they do when they run from the command line
        """
        pass 
