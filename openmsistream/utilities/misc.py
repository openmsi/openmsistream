"""Some miscellaneous functions that are universally internally available"""

# imports
import sys, time


def add_user_input(input_queue):
    """
    Listen for and add user input to a queue at one second intervals
    """
    while True:
        time.sleep(1)
        input_queue.put((sys.stdin.read(1)).strip())
