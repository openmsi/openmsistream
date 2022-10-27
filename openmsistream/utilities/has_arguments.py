"""
A class for anything that should have associated command line arguments
if anything extending it also extends Runnable
"""

#imports
from abc import ABC, abstractmethod

class HasArguments(ABC) :
    """
    A small utility class for anything that should have associated command line arguments
    """

    @classmethod
    @abstractmethod
    def get_command_line_arguments(cls) :
        """
        Get the list of argument names and the dictionary of argument names/default values to add to the argument parser

        :return: args, a list of argument names recognized by the argument parser
        :rtype: list(str)
        :return: kwargs, a dictionary of default argument values keyed by argument names
            recognized by the argument parser
        :rtype: dict
        """
        return [],{}
