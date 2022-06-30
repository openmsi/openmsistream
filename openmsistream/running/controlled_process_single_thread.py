#imports
from abc import ABC, abstractmethod
from .controlled_process import ControlledProcess

class ControlledProcessSingleThread(ControlledProcess,ABC) :
    """
    A class for running a process in a single thread in a loop until it's explicitly shut down
    """

    def run(self) :
        """
        Start the process and call run_iteration until the process is shut down
        """
        super().run()
        while self.alive :
            self._run_iteration()
            self._print_still_alive()
            self._check_control_command_queue()

    @abstractmethod
    def _run_iteration(self) :
        """
        The function that is run in an infinite loop while the process is alive
        Not implemented in the base class.
        """
        pass
