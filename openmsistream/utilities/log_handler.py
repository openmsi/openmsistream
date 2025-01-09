" Global handler of log messages, to allow self-production of own (complete) logs "

# imports
from logging import Handler
from warnings import formatwarning
from threading import Lock

# from sys import stderr


class LoggingHandlerClass(Handler):
    """A thread-safe, global, handler for log messages.
    Implemented as a ring buffer to avoid unbounded resource usage
    in case of non-production or rapid log message generation rate.
    """

    def __init__(self, max_messages=128):
        # we set max_messages small here, and increase when a log user appears
        # this should be at least as large as the number of log messages
        # expected from program start until the first log producer is created
        super().__init__()
        self.__buffer = [None] * max_messages
        self.__buffer_size = max_messages
        self.__write_pointer = [0, 0]  # index, generation (# buffer wraps)
        self.__read_pointers = {}
        self.__write_lock = Lock()  # protects write_pointer and buffer
        self.__read_lock = Lock()  # protects read_pointers

    def set_max_messages(self, max_messages):
        "Change log ring buffer size"
        with self.__read_lock:
            with self.__write_lock:
                # Try to preserve logs even when shrinking
                lag_read_pointer = self.__write_pointer[0]
                for rpn in self.__read_pointers:  # pylint: disable=C0206
                    if self.__read_pointers[rpn][0] <= self.__write_pointer[0]:
                        if self.__read_pointers[rpn][1] == self.__write_pointer[1]:
                            nlrp = self.__read_pointers[rpn][0]
                        else:
                            # This reader is really far behind and already wrapped
                            nlrp = (self.__write_pointer[0] + 1) - max_messages
                    else:
                        if self.__read_pointers[rpn][1] == (self.__write_pointer[1] - 1):
                            nlrp = self.__read_pointers[rpn][0] - max_messages
                        else:
                            # This reader is really far behind and already wrapped
                            nlrp = (self.__write_pointer[0] + 1) - max_messages
                    # store offset for later computation of new start point
                    self.__read_pointers[rpn] = [nlrp, 0]
                    if nlrp < lag_read_pointer:
                        lag_read_pointer = nlrp
                # New buffer covers past X (partly) unread events,
                # with X <= max_messsages, and max_messages-X new
                # slots.
                lag_read_pointer = max(
                    self.__write_pointer[0] - max_messages, lag_read_pointer
                )
                old_buffer = self.__buffer
                self.__buffer = []
                if lag_read_pointer < 0:
                    self.__buffer += old_buffer[lag_read_pointer:]
                if lag_read_pointer < self.__write_pointer[0]:
                    self.__buffer += old_buffer[
                        max(lag_read_pointer, 0) : self.__write_pointer[0]
                    ]
                self.__write_pointer = [len(self.__buffer), 0]
                if self.__write_pointer[0] < max_messages:
                    self.__buffer += [None] * (max_messages - self.__write_pointer[0])
                else:
                    # map index max_messages back to first slot
                    self.__write_pointer = [0, 0]
                self.__buffer_size = max_messages
                # update read pointers
                for rpn in self.__read_pointers:  # pylint: disable=C0206
                    self.__read_pointers[rpn][0] -= lag_read_pointer
                    self.__read_pointers[rpn][1] = 0
                    if self.__read_pointers[rpn][0] < 0:
                        # This reader will have missed messages
                        self.__read_pointers[rpn][0] = 0

    def emit(self, record):
        with self.__write_lock:
            self.__buffer[self.__write_pointer[0]] = self.format(record)
            self.__write_pointer[0] += 1
            if self.__write_pointer[0] == self.__buffer_size:
                self.__write_pointer[1] += 1
                self.__write_pointer[0] = 0

    # We disable pylint here due as we must match specified showwarning call signature
    def showwarning(
        self, message, category, filename, lineno, file=None, line=None
    ):  # pylint: disable=W0613
        "Python warnings showwarning implementation"
        msg = formatwarning(message, category, filename, lineno, line)
        with self.__write_lock:
            self.__buffer[self.__write_pointer[0]] = msg
            self.__write_pointer[0] += 1
            if self.__write_pointer[0] == self.__buffer_size:
                self.__write_pointer[1] += 1
                self.__write_pointer[0] = 0
        # should we still do output as default showwarning would?
        # if file is None:
        #     file = stderr
        # print(msg, file=file, flush=True)

    def get_messages(self, reader_id="default"):
        "Get unread messages for given reader"
        start_pos = None
        end_pos = None
        rv = []
        with self.__read_lock:
            if reader_id not in self.__read_pointers:
                self.__read_pointers[reader_id] = [
                    0,
                    -1,
                ]  # index, generation=-1 (= current writer)
            start_pos = self.__read_pointers[reader_id]
        with self.__write_lock:
            end_pos = self.__write_pointer
            if start_pos[1] == -1:
                start_pos[1] = end_pos[1]
            if start_pos[0] < end_pos[0] and start_pos[1] == end_pos[1]:
                rv = self.__buffer[start_pos[0] : end_pos[0]].copy()
            elif start_pos[0] > end_pos[0] and start_pos[1] == (end_pos[1] - 1):
                rv = (
                    self.__buffer[start_pos[0] :].copy()
                    + self.__buffer[0 : end_pos[0]].copy()
                )
            elif start_pos[1] < (end_pos[1] - 1):
                rv = (
                    self.__buffer[end_pos[0] :].copy()
                    + self.__buffer[0 : end_pos[0]].copy()
                )
            # start_pos = end_pos is empty list
        with self.__read_lock:
            self.__read_pointers[reader_id] = end_pos.copy()
        return rv

    def close(self):
        super().close()


# The actual, single, global instance of LoggingHandlerClass
LoggingHandler = LoggingHandlerClass()
