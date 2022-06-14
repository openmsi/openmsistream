#imports
import unittest, time
from openmsistream.shared.my_thread import MyThread
from openmsistream.shared.controlled_process import ControlledProcessSingleThread, ControlledProcessMultiThreaded

#some constants
TIMEOUT_SECS = 10
N_THREADS = 3

class ControlledProcessSingleThreadForTesting(ControlledProcessSingleThread) :
    """
    Class to use in testing ControlledProcessSingleThread
    """

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self) :
        self.checked = True

    def _on_shutdown(self) :
        self.on_shutdown_called = True

    def _run_iteration(self) :
        if self.counter<5 :
            self.counter+=1

class ControlledProcessMultiThreadedForTesting(ControlledProcessMultiThreaded) :
    """
    Class to use in test ControlledProcessMultiThreaded
    """

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.counter = 0
        self.checked = False
        self.on_shutdown_called = False

    def _on_check(self) :
        self.checked = True

    def _on_shutdown(self) :
        super()._on_shutdown()
        self.on_shutdown_called = True

    def _run_worker(self,thread_lock) :
        while self.alive :
            if self.counter<5 :
                with thread_lock :
                    self.counter+=1

class TestControlledProcess(unittest.TestCase) :
    """
    Class for testing ControlledProcess utility classes
    """

    def test_controlled_process_single_thread(self) :
        cpst = ControlledProcessSingleThreadForTesting(update_secs=5)
        self.assertEqual(cpst.counter,0)
        run_thread = MyThread(target=cpst.run)
        run_thread.start()
        try :
            self.assertFalse(cpst.checked)
            time.sleep(1.0)
            cpst.control_command_queue.put('c')
            cpst.control_command_queue.put('check')
            time.sleep(1.0)
            self.assertTrue(cpst.checked)
            self.assertFalse(cpst.on_shutdown_called)
            cpst.control_command_queue.put('q')
            time.sleep(2.0)
            self.assertTrue(cpst.on_shutdown_called)
            run_thread.join(timeout=TIMEOUT_SECS)
            time.sleep(2.0)
            if run_thread.is_alive() :
                errmsg = 'ERROR: running thread in test_controlled_process_single_thread '
                errmsg+= f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            self.assertEqual(cpst.counter,5)
        except Exception as e :
            raise e
        finally :
            if run_thread.is_alive() :
                try :
                    cpst.shutdown()
                    run_thread.join(timeout=5)
                    if run_thread.is_alive() :
                        errmsg = 'ERROR: running thread in test_controlled_process_single_thread timed out '
                        errmsg+= 'after 5 seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e

    def test_controlled_process_multi_threaded(self) :
        cpmt = ControlledProcessMultiThreadedForTesting(n_threads=N_THREADS,update_secs=5)
        self.assertEqual(cpmt.counter,0)
        run_thread = MyThread(target=cpmt.run)
        run_thread.start()
        try :
            self.assertFalse(cpmt.checked)
            time.sleep(0.5)
            cpmt.control_command_queue.put('c')
            cpmt.control_command_queue.put('check')
            time.sleep(0.5)
            self.assertTrue(cpmt.checked)
            self.assertFalse(cpmt.on_shutdown_called)
            cpmt.control_command_queue.put('q')
            time.sleep(1.0)
            self.assertTrue(cpmt.on_shutdown_called)
            run_thread.join(timeout=TIMEOUT_SECS)
            if run_thread.is_alive() :
                errmsg = 'ERROR: running thread in test_controlled_process_multi_threaded '
                errmsg+= f'timed out after {TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            self.assertEqual(cpmt.counter,5)
        except Exception as e :
            raise e
        finally :
            if run_thread.is_alive() :
                try :
                    cpmt.shutdown()
                    run_thread.join(timeout=5)
                    if run_thread.is_alive() :
                        errmsg = 'ERROR: running thread in test_controlled_process_multi_threaded '
                        errmsg+= 'timed out after 5 seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e

