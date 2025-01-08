" Global handler of log messages, to allow self-production of own (complete) logs "

# imports
from logging import Handler
from warnings import formatwarning
from sys import stderr
from threading import Lock

class LoggingHandlerClass(Handler):
    """A thread-safe, global, handler for log messages.
       Implemented as a ring buffer to avoid unbounded resource usage
       in case of non-production or rapid log message generation rate.
    """

    def __init__(self, max_messages = 128):
        # we set max_messages small here, and increase when a log user appears
        # this should be at least as large as the number of log messages
        # expected from program start until the first log producer is created
        super().__init__()
        self.__buffer = [None] * max_messages
        self.__buffer_size = max_messages
        self.__write_pointer = 0
        self.__read_pointers = {}
        self.__write_lock = Lock() # protects write_pointer and buffer
        self.__read_lock = Lock() # protects read_pointers

    def set_max_messages(self, max_messages):
        with self.__read_lock:
            with self.__write_lock:
                if max_messages > self.__buffer_size:
                    self.__buffer = self.__buffer + [None] * (max_messages - self.__buffer_size)
                else:
                    self.__buffer = self.__buffer[0:max_messages]
                self.__buffer_size = max_messages

    def emit(self, record):
        with self.__write_lock:
            self.__buffer[self.__write_pointer] = self.format(record)
            self.__write_pointer += 1

    def showwarning(self, message, category, filename, lineno, file=None, line=None):
        msg = formatwarning(message, category, filename, lineno, line)
        with self.__write_lock:
            self.__buffer[self.__write_pointer] = msg
            self.__write_pointer += 1
        # still do output as default showwarning would
        if file is None:
            file = stderr
        print(msg, file=file, flush=True)

    def get_messages(self, reader_id="default"):
        start_pos = None
        end_pos = None
        rv = []
        with self.__read_lock:
            if reader_id not in self.__read_pointers:
                self.__read_pointers[reader_id] = 0
            start_pos = self.__read_pointers[reader_id]
        with self.__write_lock:
            end_pos = self.__write_pointer
            if start_pos < end_pos:
                rv = self.__buffer[start_pos:end_pos].copy()
            elif start_pos > end_pos:
                rv = self.__buffer[start_pos:].copy() + self.__buffer[0:end_pos].copy()
        with self.__read_lock:
            self.__read_pointers[reader_id] = end_pos
        return '\n'.join(rv)

    def close(self):
        super().close()

# The actual, single, global instance of LoggingHandlerClass
LoggingHandler = LoggingHandlerClass()
