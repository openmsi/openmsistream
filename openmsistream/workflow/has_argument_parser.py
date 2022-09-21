"""Anything that has a classmethod to create an argument parser"""

#imports
from abc import ABC, abstractmethod

class HasArgumentParser(ABC) :
    """
    Base class for objects that have argument parsers
    """

    @classmethod
    @abstractmethod
    def get_argument_parser(cls) :
        """
        Return the argument parser that objects of this class use.

        Not implemented in the base class
        """
        raise NotImplementedError
