"""Watchdog testing script from the PyPI page (https://pypi.org/project/watchdog/), 
except using a PollingObserver instead of the default 
"""

import sys
import time
import logging
from watchdog.observers.polling import PollingObserver
from watchdog.events import LoggingEventHandler

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    path = sys.argv[1] if len(sys.argv) > 1 else "."
    event_handler = LoggingEventHandler()
    observer = PollingObserver()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
