"""Shared test subclasses for ControlledProcess tests."""
from openmsistream.utilities.controlled_processes_heartbeats_logs import (
    ControlledProcessSingleThreadHeartbeatsLogs,
    ControlledProcessMultiThreadedHeartbeatsLogs,
)


class ControlledProcessSingleThreadHeartbeatsForTesting(
    ControlledProcessSingleThreadHeartbeatsLogs
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_heartbeat_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_iteration(self):
        if self.counter < 5:
            self.counter += 1


class ControlledProcessMultiThreadedHeartbeatsForTesting(
    ControlledProcessMultiThreadedHeartbeatsLogs
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_heartbeat_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_worker(self):
        while self.alive:
            if self.counter < 5:
                with self.lock:
                    self.counter += 1


class ControlledProcessSingleThreadLogsForTesting(
    ControlledProcessSingleThreadHeartbeatsLogs
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_log_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_iteration(self):
        if self.counter < 5:
            self.counter += 1


class ControlledProcessMultiThreadedLogsForTesting(
    ControlledProcessMultiThreadedHeartbeatsLogs
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_log_producer("Separate")
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self):
        self.checked = True

    def _on_shutdown(self):
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_worker(self):
        while self.alive:
            if self.counter < 5:
                with self.lock:
                    self.counter += 1
