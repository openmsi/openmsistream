"""Example of a general Runnable class that can be installed as a Service/daemon"""

#imports
import datetime, time
from openmsistream.workflow import Runnable

class RunnableExample(Runnable) :
    """
    A class that extends Runnable as an example of installing a Runnable's
    "run_from_command_line" method as a Service/daemon
    """

    @classmethod
    def get_command_line_arguments(cls) :
        return ['output_dir'], {}

    @classmethod
    def run_from_command_line(cls,args=None) :
        parser = cls.get_argument_parser()
        args = parser.parse_args(args)
        test_file_name = 'runnable_example_service_test.txt'
        if not (args.output_dir/test_file_name).is_file() :
            with open(args.output_dir/test_file_name,'w') as fp :
                msg = "This file was created to test running a generic Runnable's 'run_from_command_line' "
                msg+= "method as a Service/daemon"
                fp.write(msg)
        with open(args.output_dir/test_file_name,'a') as fp :
            fp.write(f'\nService rerun {(datetime.datetime.now()).strftime("on %Y-%m-%d at %H:%M:%S")}')
        time.sleep(30)
